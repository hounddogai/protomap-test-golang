package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"

	budgetingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/budgeting/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// CategoryCalculationServer implements `acme.catalog.v1.CategoryCalculationService`.
// Computes the figures of a category, such as totals, taxes, and estimates.
type CategoryCalculationServer struct {
	catalogv1.UnimplementedCategoryCalculationServiceServer
	store                         *Store
	log                           *slog.Logger
	conn                          *grpc.ClientConn
	productCalculationClient      catalogv1.ProductCalculationServiceClient
	productCalculationPort        productCalculationPort
	budgetClient                  budgetingv1.BudgetServiceClient
	giftCardBatchQueryClient      giftcardsv1.GiftCardBatchQueryServiceClient
	sessionNotificationClient     identityv1.SessionNotificationServiceClient
	sessionHistoryClient          identityv1.SessionHistoryServiceClient
	paymentIntentValidationClient checkoutv1.PaymentIntentValidationServiceClient
	simulateProductFn             func(context.Context, *catalogv1.SimulateProductRequest, ...grpc.CallOption) (*catalogv1.SimulateProductResponse, error)
	synonymNotificationClient     searchv1.SynonymNotificationServiceClient
	clients                       *registry.Registry
}

// NewCategoryCalculationServer wires a CategoryCalculationServer.
func NewCategoryCalculationServer(deps platform.Deps) *CategoryCalculationServer {
	productCalculation := catalogv1.NewProductCalculationServiceClient(deps.Conn)
	return &CategoryCalculationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.catalog.v1.CategoryCalculationService"),
		conn:                          deps.Conn,
		productCalculationClient:      catalogv1.NewProductCalculationServiceClient(deps.Conn),
		productCalculationPort:        newProductCalculationPort(deps),
		budgetClient:                  budgetingv1.NewBudgetServiceClient(deps.Conn),
		giftCardBatchQueryClient:      giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn),
		sessionNotificationClient:     identityv1.NewSessionNotificationServiceClient(deps.Conn),
		sessionHistoryClient:          identityv1.NewSessionHistoryServiceClient(deps.Conn),
		paymentIntentValidationClient: checkoutv1.NewPaymentIntentValidationServiceClient(deps.Conn),
		simulateProductFn:             productCalculation.SimulateProduct,
		synonymNotificationClient:     searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		clients:                       deps.Clients,
	}
}

// CalculateCategory calculates a category.
func (s *CategoryCalculationServer) CalculateCategory(ctx context.Context, req *catalogv1.CalculateCategoryRequest) (*catalogv1.CalculateCategoryResponse, error) {
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "calculate category", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	detailsCache := cache.New[string, *catalogv1.CalculateCategoryRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	ctx = tenancy.With(ctx, "created_by", req.GetCategory().GetAudit().GetCreatedBy())
	source := req.GetDetails().GetDestination().ProtoReflect()
	locality := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("locality"))).String()
	recalculateProductsRequest := &catalogv1.RecalculateProductsRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				Region:    cachedDetails.GetDestination().GetRegion(),
				Recipient: req.GetCategory().GetLocation().GetRecipient(),
				Locality:  locality,
			}},
			Audit: &commonv1.AuditInfo{CreatedBy: tenancy.Value(ctx, "created_by")},
		},
	}
	recalculateProductsReply := new(catalogv1.RecalculateProductsResponse)
	if err := grpcx.Invoke(ctx, s.conn, catalogv1.ProductCalculationService_RecalculateProducts_FullMethodName, recalculateProductsRequest, recalculateProductsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/RecalculateProducts")
	}
	names := collectCategoryNames(req.GetCategory(), nil)
	compareProductOptionsRequest := &catalogv1.CompareProductOptionsRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Dimensions: &catalogv1.ProductDimensions{
				Weight: recalculateProductsReply.GetProduct().GetDimensions().GetWeight(),
			},
			CategoryPath: names,
		},
		Details: &catalogv1.CompareProductOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:    recalculateProductsReply.GetProduct().GetAddress().GetRegion(),
				Locality:  recalculateProductsReply.GetProduct().GetAddress().GetLocality(),
				Recipient: req.GetCategory().GetLocation().GetRecipient(),
			},
		},
	}
	type compareProductOptionsResult struct {
		reply *catalogv1.CompareProductOptionsResponse
		err   error
	}
	compareProductOptionsResults := make(chan compareProductOptionsResult, 1)
	go func() {
		reply, err := s.productCalculationClient.CompareProductOptions(ctx, compareProductOptionsRequest)
		compareProductOptionsResults <- compareProductOptionsResult{reply, err}
	}()
	var compareProductOptionsReply *catalogv1.CompareProductOptionsResponse
	select {
	case result := <-compareProductOptionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.catalog.v1.ProductCalculationService/CompareProductOptions")
		}
		compareProductOptionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	names2 := collectCategoryNames(req.GetCategory(), nil)
	explainProductCalculationRequest := &catalogv1.ExplainProductCalculationRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				DisplayName: compareProductOptionsReply.GetProduct().GetOwner().GetDisplayName(),
			},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			}},
			CategoryPath: names2,
		},
	}
	send := s.explainProductCalculationSender(explainProductCalculationRequest)
	explainProductCalculationReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/ExplainProductCalculation")
	}
	return &catalogv1.CalculateCategoryResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Manager: &identityv1.UserRef{
					DisplayName: recalculateProductsReply.GetProduct().GetOwner().GetDisplayName(),
				},
				Location: &commonv1.PostalAddress{
					Recipient: recalculateProductsReply.GetProduct().GetAddress().GetRecipient(),
				},
				Children: []*catalogv1.Category{&catalogv1.Category{
					Name: compareProductOptionsReply.GetProduct().GetName(),
					Manager: &identityv1.UserRef{
						Email: compareProductOptionsReply.GetProduct().GetOwner().GetEmail(),
					},
				}},
			}},
			Audit: &commonv1.AuditInfo{
				Etag: explainProductCalculationReply.GetProduct().GetAudit().GetEtag(),
			},
			CategoryId: req.GetCategoryId(),
			Name:       req.GetCategory().GetName(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: explainProductCalculationReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// EstimateCategory estimates a category.
func (s *CategoryCalculationServer) EstimateCategory(ctx context.Context, req *catalogv1.EstimateCategoryRequest) (*catalogv1.EstimateCategoryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimateCategory", "error", err)
		return nil, err
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "estimate category", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	quoteProductRequest := &catalogv1.QuoteProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetCategory().GetManager().GetDisplayName(),
			},
		},
	}
	quoteProductReply, err := s.productCalculationPort.QuoteProduct(ctx, quoteProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/QuoteProduct")
	}
	calculateProductRequest := buildCalculateProductRequestForEstimateCategory(req, quoteProductReply)
	calculateProductReply, err := s.productCalculationClient.CalculateProduct(ctx, calculateProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/CalculateProduct")
	}
	simulateProductRequest := &catalogv1.SimulateProductRequest{
		Context: req.GetContext(),
		Details: &catalogv1.SimulateProductRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:      quoteProductReply.GetProduct().GetAddress().GetRegion(),
				CountryCode: calculateProductReply.GetProduct().GetAddress().GetCountryCode(),
			},
		},
		Product: &catalogv1.Product{
			Dimensions: &catalogv1.ProductDimensions{
				Height: calculateProductReply.GetProduct().GetDimensions().GetHeight(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: quoteProductReply.GetProduct().GetAudit().GetUpdatedBy(),
			},
		},
	}
	simulateProductReply, err := simulateProductCall(ctx, simulateProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/SimulateProduct")
	}
	batchCreateBudgetsStream, err := s.budgetClient.BatchCreateBudgets(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.budgeting.v1.BudgetService/BatchCreateBudgets")
	}
	if err := batchCreateBudgetsStream.Send(&budgetingv1.BatchCreateBudgetsRequest{
		Context: req.GetContext(),
		Chunk: &budgetingv1.BatchCreateBudgetsRequest_Record{Record: &budgetingv1.Budget{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulateProductReply.GetProduct().GetAudit().GetUpdatedAt(),
				CreatedAt: quoteProductReply.GetProduct().GetAudit().GetCreatedAt(),
			},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.budgeting.v1.BudgetService/BatchCreateBudgets")
	}
	if _, err := batchCreateBudgetsStream.CloseAndRecv(); err != nil {
		return nil, grpcx.Wrap(err, "acme.budgeting.v1.BudgetService/BatchCreateBudgets")
	}
	return &catalogv1.EstimateCategoryResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Audit: &commonv1.AuditInfo{
					UpdatedAt: quoteProductReply.GetProduct().GetAudit().GetUpdatedAt(),
				},
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						CreatedBy: quoteProductReply.GetProduct().GetAudit().GetCreatedBy(),
					},
				}},
				Location: &commonv1.PostalAddress{
					Recipient: calculateProductReply.GetProduct().GetAddress().GetRecipient(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				Etag: simulateProductReply.GetProduct().GetAudit().GetEtag(),
			},
			CategoryId: req.GetCategoryId(),
			Name:       req.GetCategory().GetName(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: calculateProductReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// QuoteCategory quotes a category.
func (s *CategoryCalculationServer) QuoteCategory(ctx context.Context, req *catalogv1.QuoteCategoryRequest) (*catalogv1.QuoteCategoryResponse, error) {
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "quote category", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	names := collectCategoryNames(req.GetCategory(), nil)
	simulateProductRequest := &catalogv1.SimulateProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				Region:   req.GetDetails().GetDestination().GetRegion(),
				Locality: req.GetDetails().GetDestination().GetLocality(),
			}},
			Audit:        &commonv1.AuditInfo{Etag: req.GetCategory().GetAudit().GetEtag()},
			CategoryPath: names,
		},
		Details: &catalogv1.SimulateProductRequest_Details{
			Destination: &commonv1.PostalAddress{PostalCode: req.GetCategory().GetLocation().GetPostalCode()},
		},
	}
	simulateProductReply, err := s.productCalculationPort.SimulateProduct(ctx, simulateProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/SimulateProduct")
	}
	cloned := proto.Clone(req.GetCategory()).(*catalogv1.Category)
	quoteProductRequest := &catalogv1.QuoteProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Components: map[string]*catalogv1.ProductComponent{"primary": &catalogv1.ProductComponent{
				Description: simulateProductReply.GetProduct().GetComponents()["primary"].GetDescription(),
			}},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{Locality: req.GetCategory().GetLocation().GetLocality()}},
			Audit:   &commonv1.AuditInfo{CreatedAt: cloned.GetAudit().GetCreatedAt()},
			Dimensions: &catalogv1.ProductDimensions{
				Length: simulateProductReply.GetProduct().GetDimensions().GetLength(),
			},
		},
	}
	quoteProductReply, err := catalogv1.ProductCalculationServiceClient.QuoteProduct(s.productCalculationClient, ctx, quoteProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/QuoteProduct")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := categoryCalculationServerFindGiftCardBatchesSteps[mode]
	cloned2 := proto.Clone(req.GetCategory()).(*catalogv1.Category)
	findGiftCardBatchesRequest := &giftcardsv1.FindGiftCardBatchesRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.FindGiftCardBatchesRequest_Filter{
			Region:       cloned2.GetLocation().GetRegion(),
			CreatedAfter: simulateProductReply.GetProduct().GetAudit().GetCreatedAt(),
		},
	}
	findGiftCardBatchesReply, err := run(s, ctx, findGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/FindGiftCardBatches")
	}
	streamSessionNoticesStream, err := s.sessionNotificationClient.StreamSessionNotices(ctx, &identityv1.StreamSessionNoticesRequest{
		Context:    req.GetContext(),
		Filter:     &identityv1.StreamSessionNoticesRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
		Subscriber: &commonv1.ContactInfo{Email: quoteProductReply.GetProduct().GetOwner().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
	}
	for {
		received, err := streamSessionNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
		}
		_ = received
	}
	return &catalogv1.QuoteCategoryResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						DisplayName: simulateProductReply.GetProduct().GetOwner().GetDisplayName(),
					},
				}},
				Location: &commonv1.PostalAddress{
					PostalCode: quoteProductReply.GetProduct().GetAddress().GetPostalCode(),
				},
			}},
			Name:       quoteProductReply.GetProduct().GetName(),
			CategoryId: req.GetCategoryId(),
			Status:     req.GetCategory().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: findGiftCardBatchesReply.GetResult().GetCorrelationId(),
			Message:       findGiftCardBatchesReply.GetResult().GetMessage(),
			Status:        "ok",
		},
	}, nil
}

// CompareCategoryOptions compares a category.
func (s *CategoryCalculationServer) CompareCategoryOptions(ctx context.Context, req *catalogv1.CompareCategoryOptionsRequest) (*catalogv1.CompareCategoryOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "compare category options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetCategory())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	names := collectCategoryNames(req.GetCategory(), nil)
	annotateSessionVersionRequest := &identityv1.AnnotateSessionVersionRequest{}
	annotateSessionVersionRequest.Context = req.GetContext()
	annotateSessionVersionRequest.Session = &identityv1.Session{
		Audit: &commonv1.AuditInfo{
			CreatedAt: req.GetCategory().GetAudit().GetCreatedAt(),
			Etag:      req.GetCategory().GetAudit().GetEtag(),
		},
		Assignee: &identityv1.UserRef{
			DisplayName: req.GetCategory().GetManager().GetDisplayName(),
		},
		Tags: names,
	}
	annotateSessionVersionRequest.Details = &identityv1.AnnotateSessionVersionRequest_Details{Snapshot: snapshot}
	annotateSessionVersionReply, err := s.sessionHistoryClient.AnnotateSessionVersion(ctx, annotateSessionVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/AnnotateSessionVersion")
	}
	names2 := collectCategoryNames(req.GetCategory(), nil)
	explainProductCalculationReply, err := s.productCalculationClient.ExplainProductCalculation(ctx, &catalogv1.ExplainProductCalculationRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				DisplayName: annotateSessionVersionReply.GetSession().GetAssignee().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: annotateSessionVersionReply.GetSession().GetAudit().GetEtag(),
			},
			CategoryPath: names2,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/ExplainProductCalculation")
	}
	return &catalogv1.CompareCategoryOptionsResponse{
		Category: &catalogv1.Category{
			Manager: &identityv1.UserRef{
				DisplayName: annotateSessionVersionReply.GetSession().GetCustomer().GetDisplayName(),
			},
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Location: &commonv1.PostalAddress{
						Locality: explainProductCalculationReply.GetProduct().GetAddress().GetLocality(),
					},
				}},
			}},
			CategoryId: req.GetCategoryId(),
			Status:     req.GetCategory().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculateCategories recalculates a category.
func (s *CategoryCalculationServer) RecalculateCategories(ctx context.Context, req *catalogv1.RecalculateCategoriesRequest) (*catalogv1.RecalculateCategoriesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "recalculate categories", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	categoryCache := cache.New[string, *catalogv1.Category](1)
	categoryCache.Put(req.GetContext().GetTraceId(), req.GetCategory())
	cachedCategory, _ := categoryCache.Get(req.GetContext().GetTraceId())
	checkPaymentIntentEligibilityRequest := &checkoutv1.CheckPaymentIntentEligibilityRequest{}
	checkPaymentIntentEligibilityRequest.Context = req.GetContext()
	checkPaymentIntentEligibilityRequest.PaymentIntent = &checkoutv1.PaymentIntent{
		Assignee: &identityv1.UserRef{
			Email:  req.GetCategory().GetManager().GetEmail(),
			UserId: req.GetCategory().GetManager().GetUserId(),
		},
		Audit: &commonv1.AuditInfo{Etag: cachedCategory.GetAudit().GetEtag()},
	}
	checkPaymentIntentEligibilityReply, err := s.paymentIntentValidationClient.CheckPaymentIntentEligibility(ctx, checkPaymentIntentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/CheckPaymentIntentEligibility")
	}
	names := collectCategoryNames(req.GetCategory(), nil)
	quoteProductRequest := &catalogv1.QuoteProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{PostalCode: req.GetCategory().GetLocation().GetPostalCode()}},
			Owner: &identityv1.UserRef{
				DisplayName: checkPaymentIntentEligibilityReply.GetPaymentIntent().GetCustomer().GetDisplayName(),
				Email:       checkPaymentIntentEligibilityReply.GetPaymentIntent().GetAssignee().GetEmail(),
				UserId:      req.GetCategory().GetManager().GetUserId(),
			},
			CategoryPath: names,
		},
	}
	quoteProductReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*catalogv1.QuoteProductResponse, error) {
		return s.productCalculationClient.QuoteProduct(ctx, quoteProductRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/QuoteProduct")
	}
	simulateProductRequest := &catalogv1.SimulateProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				Etag: checkPaymentIntentEligibilityReply.GetPaymentIntent().GetAudit().GetEtag(),
			},
		},
	}
	simulateProductReply, err := s.simulateProductFn(ctx, simulateProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/SimulateProduct")
	}
	return &catalogv1.RecalculateCategoriesResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Audit: &commonv1.AuditInfo{
					CreatedBy: checkPaymentIntentEligibilityReply.GetPaymentIntent().GetAudit().GetCreatedBy(),
				},
				Manager: &identityv1.UserRef{
					DisplayName: simulateProductReply.GetProduct().GetOwner().GetDisplayName(),
				},
			}},
			Location: &commonv1.PostalAddress{
				Lines: quoteProductReply.GetProduct().GetAddress().GetLines(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: simulateProductReply.GetProduct().GetAudit().GetCreatedBy(),
			},
			CategoryId: req.GetCategoryId(),
			Status:     req.GetCategory().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SimulateCategory simulates a category.
func (s *CategoryCalculationServer) SimulateCategory(ctx context.Context, req *catalogv1.SimulateCategoryRequest) (*catalogv1.SimulateCategoryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "simulate category", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	names := collectCategoryNames(req.GetCategory(), nil)
	explainProductCalculationReply, err := s.productCalculationClient.ExplainProductCalculation(ctx, &catalogv1.ExplainProductCalculationRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner:        &identityv1.UserRef{Email: req.GetCategory().GetManager().GetEmail()},
			CategoryPath: names,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/ExplainProductCalculation")
	}
	cloned := proto.Clone(req.GetCategory()).(*catalogv1.Category)
	packed, err := anypb.New(req.GetCategory())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendSynonymTestNoticeRequest := &searchv1.SendSynonymTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSynonymTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: cloned.GetManager().GetEmail()},
			Payload:   packed,
		},
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
	sendSynonymTestNoticeReply, err := searchv1.SynonymNotificationServiceClient.SendSynonymTestNotice(s.synonymNotificationClient, ctx, sendSynonymTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/SendSynonymTestNotice")
	}
	encoded, err := protojson.Marshal(req.GetCategory())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.Category
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	categoryCache := cache.New[string, *catalogv1.Category](1)
	categoryCache.Put(req.GetContext().GetTraceId(), req.GetCategory())
	cachedCategory, _ := categoryCache.Get(req.GetContext().GetTraceId())
	simulateProductRequest := &catalogv1.SimulateProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				CreatedAt: sendSynonymTestNoticeReply.GetSynonym().GetAudit().GetCreatedAt(),
			},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				Region: (&decoded).GetLocation().GetRegion(),
				Lines:  explainProductCalculationReply.GetProduct().GetAddress().GetLines(),
			}},
		},
		Details: &catalogv1.SimulateProductRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: cachedCategory.GetLocation().GetLines()},
		},
	}
	simulateProductReply, err := s.simulateProductFn(ctx, simulateProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/SimulateProduct")
	}
	manifestValidation, err := registry.Lookup[shippingv1.ManifestValidationServiceClient](s.clients, "acme.shipping.v1.ManifestValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	names2 := collectCategoryNames(req.GetCategory(), nil)
	validateManifestRequest := &shippingv1.ValidateManifestRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Lines: []*shippingv1.ManifestLine{&shippingv1.ManifestLine{
				Tax: &shippingv1.ManifestLineTax{
					Amount: sendSynonymTestNoticeReply.GetSynonym().GetDiscount().GetAmount(),
				},
				Description: explainProductCalculationReply.GetProduct().GetComponents()["primary"].GetDescription(),
			}},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetCategory().GetAudit().GetCreatedBy()},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: explainProductCalculationReply.GetProduct().GetOwner().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{Lines: names2},
		},
	}
	validateManifestReply, err := manifestValidation.ValidateManifest(ctx, validateManifestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestValidationService/ValidateManifest")
	}
	return &catalogv1.SimulateCategoryResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Audit: &commonv1.AuditInfo{
					UpdatedBy: explainProductCalculationReply.GetProduct().GetAudit().GetUpdatedBy(),
				},
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						CreatedBy: explainProductCalculationReply.GetProduct().GetAudit().GetCreatedBy(),
					},
					Manager: &identityv1.UserRef{
						DisplayName: validateManifestReply.GetManifest().GetCounterparty().GetDisplayName(),
					},
				}},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulateProductReply.GetProduct().GetAudit().GetUpdatedAt(),
			},
			CategoryId: req.GetCategoryId(),
			ParentId:   req.GetCategory().GetParentId(),
		},
		Result: &commonv1.OperationResult{
			Message:      sendSynonymTestNoticeReply.GetOutcome().GetMessage(),
			ReferenceIds: simulateProductReply.GetResult().GetReferenceIds(),
			Status:       validateManifestReply.GetResult().GetStatus(),
		},
	}, nil
}

// ExplainCategoryCalculation explains a category.
func (s *CategoryCalculationServer) ExplainCategoryCalculation(ctx context.Context, req *catalogv1.ExplainCategoryCalculationRequest) (*catalogv1.ExplainCategoryCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "explain category calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	names := collectCategoryNames(req.GetCategory(), nil)
	compareProductOptionsReply, err := s.productCalculationClient.CompareProductOptions(ctx, &catalogv1.CompareProductOptionsRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner:        &identityv1.UserRef{UserId: req.GetCategory().GetManager().GetUserId()},
			CategoryPath: names,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/CompareProductOptions")
	}
	unsubscribeSearchQueryUpdatesRequest := &searchv1.UnsubscribeSearchQueryUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSearchQueryUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            compareProductOptionsReply.GetProduct().GetOwner().GetEmail(),
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Name:             req.GetDetails().GetContact().GetName(),
			},
		},
	}
	unsubscribeSearchQueryUpdatesReply := new(searchv1.UnsubscribeSearchQueryUpdatesResponse)
	if err := grpcx.Invoke(ctx, s.conn, searchv1.SearchQueryNotificationService_UnsubscribeSearchQueryUpdates_FullMethodName, unsubscribeSearchQueryUpdatesRequest, unsubscribeSearchQueryUpdatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/UnsubscribeSearchQueryUpdates")
	}
	cloned := proto.Clone(req.GetDetails()).(*catalogv1.ExplainCategoryCalculationRequest_Details)
	validateFulfillmentExceptionRequest := &fulfillmentv1.ValidateFulfillmentExceptionRequest{
		Context: req.GetContext(),
		Details: &fulfillmentv1.ValidateFulfillmentExceptionRequest_Details{
			Contact: &commonv1.ContactInfo{PreferredChannel: cloned.GetContact().GetPreferredChannel()},
		},
	}
	validateFulfillmentExceptionReply := new(fulfillmentv1.ValidateFulfillmentExceptionResponse)
	if err := s.conn.Invoke(ctx, fulfillmentFulfillmentExceptionValidationServiceValidateFulfillmentExceptionMethod, validateFulfillmentExceptionRequest, validateFulfillmentExceptionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ValidateFulfillmentException")
	}
	return &catalogv1.ExplainCategoryCalculationResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Location: &commonv1.PostalAddress{
					Region: compareProductOptionsReply.GetProduct().GetAddress().GetRegion(),
				},
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: validateFulfillmentExceptionReply.GetFulfillmentException().GetAudit().GetUpdatedBy(),
					},
				}},
			}},
			CategoryId: req.GetCategoryId(),
			Attributes: req.GetCategory().GetAttributes(),
		},
		Outcome: &catalogv1.ExplainCategoryCalculationResponse_Outcome{
			ReferenceId: unsubscribeSearchQueryUpdatesReply.GetOutcome().GetReferenceId(),
			NextSteps:   validateFulfillmentExceptionReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			Message: unsubscribeSearchQueryUpdatesReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetCategoryRates returns a category.
func (s *CategoryCalculationServer) GetCategoryRates(ctx context.Context, req *catalogv1.GetCategoryRatesRequest) (*catalogv1.GetCategoryRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetCategoryRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "category", req.GetCategoryId(), "get_category_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get category rates", "tenant", req.GetContext().GetTenantId())
	createBudgetRequest := buildCreateBudgetRequestForGetCategoryRates(req)
	createBudgetReply, err := s.budgetClient.CreateBudget(ctx, createBudgetRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.budgeting.v1.BudgetService/CreateBudget")
	}
	return &catalogv1.GetCategoryRatesResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{Etag: createBudgetReply.GetBudget().GetAudit().GetEtag()},
				}},
			}},
			CategoryId: req.GetCategoryId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// explainProductCalculationSender returns a function that sends one request to
// acme.catalog.v1.ProductCalculationService/ExplainProductCalculation.
func (s *CategoryCalculationServer) explainProductCalculationSender(request *catalogv1.ExplainProductCalculationRequest) func(context.Context) (*catalogv1.ExplainProductCalculationResponse, error) {
	return func(ctx context.Context) (*catalogv1.ExplainProductCalculationResponse, error) {
		return s.productCalculationClient.ExplainProductCalculation(ctx, request)
	}
}

// categoryCalculationServerFindGiftCardBatchesSteps are the ways to run
// acme.giftcards.v1.GiftCardBatchQueryService/FindGiftCardBatches, by name.
var categoryCalculationServerFindGiftCardBatchesSteps = map[string]func(*CategoryCalculationServer, context.Context, *giftcardsv1.FindGiftCardBatchesRequest) (*giftcardsv1.FindGiftCardBatchesResponse, error){
	"default": (*CategoryCalculationServer).findGiftCardBatchesStep,
	"audited": (*CategoryCalculationServer).findGiftCardBatchesAuditedStep,
}

func (s *CategoryCalculationServer) findGiftCardBatchesStep(ctx context.Context, request *giftcardsv1.FindGiftCardBatchesRequest) (*giftcardsv1.FindGiftCardBatchesResponse, error) {
	return s.giftCardBatchQueryClient.FindGiftCardBatches(ctx, request)
}

func (s *CategoryCalculationServer) findGiftCardBatchesAuditedStep(ctx context.Context, request *giftcardsv1.FindGiftCardBatchesRequest) (*giftcardsv1.FindGiftCardBatchesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.giftcards.v1.GiftCardBatchQueryService/FindGiftCardBatches")
	return s.giftCardBatchQueryClient.FindGiftCardBatches(ctx, request)
}
