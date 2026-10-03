package catalog

import (
	"context"
	"fmt"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	insurancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/insurance/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CategoryQueryServer implements `acme.catalog.v1.CategoryQueryService`.
// Answers read-only questions about categories, for screens and reports.
type CategoryQueryServer struct {
	catalogv1.UnimplementedCategoryQueryServiceServer
	store                            *Store
	log                              *slog.Logger
	conn                             *grpc.ClientConn
	productVariantCalculationClient  catalogv1.ProductVariantCalculationServiceClient
	productCalculationClient         catalogv1.ProductCalculationServiceClient
	productCalculationClientFallback catalogv1.ProductCalculationServiceClient
	policyQueryClient                insurancev1.PolicyQueryServiceClient
	notificationValidationClient     notificationsv1.NotificationValidationServiceClient
	simulateGiftCardTransactionFn    func(context.Context, *giftcardsv1.SimulateGiftCardTransactionRequest, ...grpc.CallOption) (*giftcardsv1.SimulateGiftCardTransactionResponse, error)
	productVariantValidationPort     productVariantValidationPort
	categoryCalculationClient        catalogv1.CategoryCalculationServiceClient
	productQueryClient               catalogv1.ProductQueryServiceClient
	documentValidationClient         documentsv1.DocumentValidationServiceClient
	documentValidationClientFallback documentsv1.DocumentValidationServiceClient
}

// NewCategoryQueryServer wires a CategoryQueryServer.
func NewCategoryQueryServer(deps platform.Deps) CategoryQueryServer {
	giftCardTransactionCalculation := giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn)
	return CategoryQueryServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.catalog.v1.CategoryQueryService"),
		conn:                             deps.Conn,
		productVariantCalculationClient:  catalogv1.NewProductVariantCalculationServiceClient(deps.Conn),
		productCalculationClient:         catalogv1.NewProductCalculationServiceClient(deps.Conn),
		productCalculationClientFallback: catalogv1.NewProductCalculationServiceClient(deps.Conn),
		policyQueryClient:                insurancev1.NewPolicyQueryServiceClient(deps.Conn),
		notificationValidationClient:     notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		simulateGiftCardTransactionFn:    giftCardTransactionCalculation.SimulateGiftCardTransaction,
		productVariantValidationPort:     newProductVariantValidationPort(deps),
		categoryCalculationClient:        catalogv1.NewCategoryCalculationServiceClient(deps.Conn),
		productQueryClient:               catalogv1.NewProductQueryServiceClient(deps.Conn),
		documentValidationClient:         documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		documentValidationClientFallback: documentsv1.NewDocumentValidationServiceClient(deps.Conn),
	}
}

// LookupCategory returns a category.
func (s CategoryQueryServer) LookupCategory(ctx context.Context, req *catalogv1.LookupCategoryRequest) (*catalogv1.LookupCategoryResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "category", req.GetCategoryId(), "lookup_category"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup category", "tenant", req.GetContext().GetTenantId())
	lintProductVariantsRequest := &catalogv1.LintProductVariantsRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	}
	lintProductVariantsReply, err := grpcx.NewClient(s.conn, catalogv1.NewProductVariantValidationServiceClient).LintProductVariants(ctx, lintProductVariantsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/LintProductVariants")
	}
	estimateProductVariantRequest := &catalogv1.EstimateProductVariantRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Locator: &catalogv1.ProductVariant_Address{Address: &commonv1.PostalAddress{
				Recipient: lintProductVariantsReply.GetProductVariant().GetAddress().GetRecipient(),
			}},
			Dimensions: &catalogv1.ProductVariantDimensions{
				Height: lintProductVariantsReply.GetProductVariant().GetDimensions().GetHeight(),
				Unit:   lintProductVariantsReply.GetProductVariant().GetDimensions().GetUnit(),
			},
			Owner: &identityv1.UserRef{
				Email: lintProductVariantsReply.GetProductVariant().GetOwner().GetEmail(),
			},
		},
	}
	type estimateProductVariantResult struct {
		reply *catalogv1.EstimateProductVariantResponse
		err   error
	}
	estimateProductVariantResults := make(chan estimateProductVariantResult, 1)
	go func() {
		reply, err := s.productVariantCalculationClient.EstimateProductVariant(ctx, estimateProductVariantRequest)
		estimateProductVariantResults <- estimateProductVariantResult{reply, err}
	}()
	var estimateProductVariantReply *catalogv1.EstimateProductVariantResponse
	select {
	case result := <-estimateProductVariantResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.catalog.v1.ProductVariantCalculationService/EstimateProductVariant")
		}
		estimateProductVariantReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &catalogv1.LookupCategoryResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						DisplayName: lintProductVariantsReply.GetProductVariant().GetOwner().GetDisplayName(),
					},
					Location: &commonv1.PostalAddress{
						PostalCode: estimateProductVariantReply.GetProductVariant().GetAddress().GetPostalCode(),
					},
				}},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: estimateProductVariantReply.GetProductVariant().GetAudit().GetCreatedBy(),
			},
			CategoryId: req.GetCategoryId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseCategories lists categories a page at a time.
func (s CategoryQueryServer) BrowseCategories(ctx context.Context, req *catalogv1.BrowseCategoriesRequest) (*catalogv1.BrowseCategoriesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse categories", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *catalogv1.QuoteProductRequest, ...grpc.CallOption) (*catalogv1.QuoteProductResponse, error){
		"primary":  s.productCalculationClient.QuoteProduct,
		"fallback": s.productCalculationClientFallback.QuoteProduct,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	quoteProductRequest := &catalogv1.QuoteProductRequest{
		Context: req.GetContext(),
		Details: &catalogv1.QuoteProductRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	quoteProductReply, err := route(ctx, quoteProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/QuoteProduct")
	}
	return &catalogv1.BrowseCategoriesResponse{
		Categories: []*catalogv1.Category{&catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Location: &commonv1.PostalAddress{
						Region: quoteProductReply.GetProduct().GetAddress().GetRegion(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedAt: quoteProductReply.GetProduct().GetAudit().GetCreatedAt(),
					},
				}},
			}},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// FindCategories lists categories a page at a time.
func (s CategoryQueryServer) FindCategories(ctx context.Context, req *catalogv1.FindCategoriesRequest) (*catalogv1.FindCategoriesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "find categories", "tenant", req.GetContext().GetTenantId())
	summarizePoliciesReply, err := s.policyQueryClient.SummarizePolicies(ctx, &insurancev1.SummarizePoliciesRequest{
		Context: req.GetContext(),
		Details: &insurancev1.SummarizePoliciesRequest_Details{Filters: req.GetFilter().GetLabels()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.insurance.v1.PolicyQueryService/SummarizePolicies")
	}
	return &catalogv1.FindCategoriesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizePoliciesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// CountCategories counts categories by status.
func (s CategoryQueryServer) CountCategories(ctx context.Context, req *catalogv1.CountCategoriesRequest) (*catalogv1.CountCategoriesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountCategories", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count categories", "tenant", req.GetContext().GetTenantId())
	return &catalogv1.CountCategoriesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ResolveCategories returns several categories at once.
func (s CategoryQueryServer) ResolveCategories(ctx context.Context, req *catalogv1.ResolveCategoriesRequest) (*catalogv1.ResolveCategoriesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve categories", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	validateNotificationRequest := &notificationsv1.ValidateNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Assignee: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Comments: []*notificationsv1.NotificationComment{&notificationsv1.NotificationComment{CreatedAt: req.GetContext().GetDeadline()}},
		},
		Details: &notificationsv1.ValidateNotificationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  cloned.GetLocale(),
		},
	}
	validateNotificationReply, err := notificationsv1.NotificationValidationServiceClient.ValidateNotification(s.notificationValidationClient, ctx, validateNotificationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ValidateNotification")
	}
	countFeatureFlagsRequest := &featureflagsv1.CountFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.CountFeatureFlagsRequest_Filter{
			CreatedAfter: validateNotificationReply.GetNotification().GetAudit().GetUpdatedAt(),
		},
	}
	countFeatureFlagsReply := new(featureflagsv1.CountFeatureFlagsResponse)
	if err := grpc.Invoke(ctx, "/acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags", countFeatureFlagsRequest, countFeatureFlagsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags")
	}
	return &catalogv1.ResolveCategoriesResponse{
		Categories: []*catalogv1.Category{&catalogv1.Category{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: validateNotificationReply.GetNotification().GetAudit().GetUpdatedBy(),
			},
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						Etag: validateNotificationReply.GetNotification().GetAudit().GetEtag(),
					},
				}},
			}},
		}},
		Result: &commonv1.OperationResult{Status: countFeatureFlagsReply.GetResult().GetStatus()},
	}, nil
}

// ExportCategories streams every matching category.
func (s CategoryQueryServer) ExportCategories(req *catalogv1.ExportCategoriesRequest, stream grpc.ServerStreamingServer[catalogv1.ExportCategoriesResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "export categories", "tenant", req.GetContext().GetTenantId())
	previewRenewalScheduleRequest := &subscriptionsv1.PreviewRenewalScheduleRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Details: &subscriptionsv1.PreviewRenewalScheduleRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	previewRenewalScheduleReply, err := previewRenewalScheduleCall(ctx, previewRenewalScheduleRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/PreviewRenewalSchedule")
	}
	if err := stream.Send(&catalogv1.ExportCategoriesResponse{
		Category: &catalogv1.Category{
			Audit: &commonv1.AuditInfo{
				CreatedAt: previewRenewalScheduleReply.GetRenewalSchedule().GetAudit().GetCreatedAt(),
			},
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeCategories summarizes a category.
func (s CategoryQueryServer) SummarizeCategories(ctx context.Context, req *catalogv1.SummarizeCategoriesRequest) (*catalogv1.SummarizeCategoriesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "category", req.GetCategoryId(), "summarize_categories"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize categories", "tenant", req.GetContext().GetTenantId())
	simulateGiftCardTransactionRequest := &giftcardsv1.SimulateGiftCardTransactionRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
	}
	simulateGiftCardTransactionReply, err := s.simulateGiftCardTransactionFn(ctx, simulateGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/SimulateGiftCardTransaction")
	}
	lintProductVariantsRequest := &catalogv1.LintProductVariantsRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				DisplayName: simulateGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
				Email:       req.GetContext().GetActor().GetEmail(),
			},
		},
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
	lintProductVariantsReply, err := s.productVariantValidationPort.LintProductVariants(ctx, lintProductVariantsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/LintProductVariants")
	}
	quoteCategoryReply, err := s.categoryCalculationClient.QuoteCategory(ctx, &catalogv1.QuoteCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						Email: lintProductVariantsReply.GetProductVariant().GetOwner().GetEmail(),
					},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/QuoteCategory")
	}
	annotateSynonymVersionRequest := &searchv1.AnnotateSynonymVersionRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				Etag:      quoteCategoryReply.GetCategory().GetAudit().GetEtag(),
				UpdatedAt: quoteCategoryReply.GetCategory().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: quoteCategoryReply.GetCategory().GetManager().GetDisplayName(),
			},
			Description: lintProductVariantsReply.GetProductVariant().GetComponents()["primary"].GetDescription(),
		},
	}
	annotateSynonymVersionReply := new(searchv1.AnnotateSynonymVersionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", searchSynonymHistoryServiceName, "AnnotateSynonymVersion"), annotateSynonymVersionRequest, annotateSynonymVersionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/AnnotateSynonymVersion")
	}
	return &catalogv1.SummarizeCategoriesResponse{
		Result: &commonv1.OperationResult{
			Details:       simulateGiftCardTransactionReply.GetResult().GetDetails(),
			ReferenceIds:  lintProductVariantsReply.GetResult().GetReferenceIds(),
			Message:       lintProductVariantsReply.GetOutcome().GetMessage(),
			CorrelationId: quoteCategoryReply.GetResult().GetCorrelationId(),
			Status:        quoteCategoryReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestCategories lists categories a page at a time.
func (s CategoryQueryServer) SuggestCategories(ctx context.Context, req *catalogv1.SuggestCategoriesRequest) (*catalogv1.SuggestCategoriesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestCategories", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest categories", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "page_token", req.GetPage().GetPageToken())
	suggestProductsRequest := &catalogv1.SuggestProductsRequest{}
	suggestProductsRequest.Context = req.GetContext()
	suggestProductsRequest.Page = &commonv1.PageRequest{
		PageSize:  req.GetPage().GetPageSize(),
		PageToken: tenancy.Value(ctx, "page_token"),
	}
	suggestProductsReply, err := s.productQueryClient.SuggestProducts(ctx, suggestProductsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductQueryService/SuggestProducts")
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	estimateProductVariantReply, err := s.productVariantCalculationClient.EstimateProductVariant(ctx, &catalogv1.EstimateProductVariantRequest{
		Context: req.GetContext(),
		Details: &catalogv1.EstimateProductVariantRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: cloned.GetActor().GetUserId(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/EstimateProductVariant")
	}
	return &catalogv1.SuggestCategoriesResponse{
		PageInfo: &commonv1.PageInfo{
			TotalSize: suggestProductsReply.GetPageInfo().GetTotalSize(),
		},
		Result: &commonv1.OperationResult{
			Details: suggestProductsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Categories: []*catalogv1.Category{&catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Location: &commonv1.PostalAddress{
					Recipient: estimateProductVariantReply.GetProductVariant().GetAddress().GetRecipient(),
				},
			}},
		}},
	}, nil
}

// PeekCategory returns a category.
func (s CategoryQueryServer) PeekCategory(ctx context.Context, req *catalogv1.PeekCategoryRequest) (*catalogv1.PeekCategoryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PeekCategory", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "category", req.GetCategoryId(), "peek_category"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek category", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *documentsv1.PreviewDocumentRequest, ...grpc.CallOption) (*documentsv1.PreviewDocumentResponse, error){
		"primary":  s.documentValidationClient.PreviewDocument,
		"fallback": s.documentValidationClientFallback.PreviewDocument,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	previewDocumentRequest := &documentsv1.PreviewDocumentRequest{
		Context: req.GetContext(),
		Details: &documentsv1.PreviewDocumentRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		Document: &documentsv1.Document{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	}
	previewDocumentReply, err := route(ctx, previewDocumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/PreviewDocument")
	}
	return &catalogv1.PeekCategoryResponse{
		Category: &catalogv1.Category{
			Location: &commonv1.PostalAddress{
				CountryCode: previewDocumentReply.GetDocument().GetBillingAddress().GetCountryCode(),
			},
			CategoryId: req.GetCategoryId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
