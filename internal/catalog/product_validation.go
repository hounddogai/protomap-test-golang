package catalog

import (
	"context"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// ProductValidationServer implements `acme.catalog.v1.ProductValidationService`.
// Checks products against the rules before anything acts on them.
type ProductValidationServer struct {
	catalogv1.UnsafeProductValidationServiceServer
	store                         *Store
	log                           *slog.Logger
	pointsLedgerValidationClient  loyaltyv1.PointsLedgerValidationServiceClient
	categoryCalculationClient     catalogv1.CategoryCalculationServiceClient
	apiCredentialValidationClient identityv1.ApiCredentialValidationServiceClient
	productCalculationClient      catalogv1.ProductCalculationServiceClient
	clients                       *registry.Registry
	paymentValidationClient       paymentsv1.PaymentValidationServiceClient
	usageRecordValidationClient   subscriptionsv1.UsageRecordValidationServiceClient
	conn                          *grpc.ClientConn
	searchIndexValidationClient   searchv1.SearchIndexValidationServiceClient
}

// NewProductValidationServer wires a ProductValidationServer.
func NewProductValidationServer(deps platform.Deps) *ProductValidationServer {
	return &ProductValidationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.catalog.v1.ProductValidationService"),
		pointsLedgerValidationClient:  loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn),
		categoryCalculationClient:     catalogv1.NewCategoryCalculationServiceClient(deps.Conn),
		apiCredentialValidationClient: identityv1.NewApiCredentialValidationServiceClient(deps.Conn),
		productCalculationClient:      catalogv1.NewProductCalculationServiceClient(deps.Conn),
		clients:                       deps.Clients,
		paymentValidationClient:       paymentsv1.NewPaymentValidationServiceClient(deps.Conn),
		usageRecordValidationClient:   subscriptionsv1.NewUsageRecordValidationServiceClient(deps.Conn),
		conn:                          deps.Conn,
		searchIndexValidationClient:   searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
	}
}

// ValidateProduct validates a product.
func (s *ProductValidationServer) ValidateProduct(ctx context.Context, req *catalogv1.ValidateProductRequest) (*catalogv1.ValidateProductResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "validate product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.ValidateProductRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	explainPointsLedgerDecisionReply, err := s.pointsLedgerValidationClient.ExplainPointsLedgerDecision(ctx, &loyaltyv1.ExplainPointsLedgerDecisionRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.ExplainPointsLedgerDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: (&decoded).GetContact().GetPreferredChannel(),
				Email:            req.GetDetails().GetContact().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ExplainPointsLedgerDecision")
	}
	return &catalogv1.ValidateProductResponse{
		Outcome: &catalogv1.ValidateProductResponse_Outcome{
			Status:    explainPointsLedgerDecisionReply.GetResult().GetStatus(),
			NextSteps: explainPointsLedgerDecisionReply.GetOutcome().GetNextSteps(),
		},
		Product: &catalogv1.Product{
			ProductId: req.GetProductId(),
			Code:      req.GetProduct().GetCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewProduct previews a product.
func (s *ProductValidationServer) PreviewProduct(ctx context.Context, req *catalogv1.PreviewProductRequest) (*catalogv1.PreviewProductResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewProduct", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "preview product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	calculateCategoryReply, err := s.categoryCalculationClient.CalculateCategory(ctx, &catalogv1.CalculateCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Location: &commonv1.PostalAddress{Region: req.GetProduct().GetAddress().GetRegion()},
					Manager:  &identityv1.UserRef{UserId: req.GetProduct().GetOwner().GetUserId()},
					Audit:    &commonv1.AuditInfo{Etag: req.GetProduct().GetAudit().GetEtag()},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/CalculateCategory")
	}
	previewApiCredentialReply, err := s.apiCredentialValidationClient.PreviewApiCredential(ctx, &identityv1.PreviewApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetProduct().GetAudit().GetUpdatedBy()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/PreviewApiCredential")
	}
	return &catalogv1.PreviewProductResponse{
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculateCategoryReply.GetCategory().GetAudit().GetCreatedAt(),
			},
			Name: calculateCategoryReply.GetCategory().GetName(),
			Owner: &identityv1.UserRef{
				DisplayName: previewApiCredentialReply.GetApiCredential().GetHolder().GetDisplayName(),
			},
			ProductId: req.GetProductId(),
			Code:      req.GetProduct().GetCode(),
		},
		Outcome: &catalogv1.PreviewProductResponse_Outcome{
			ReferenceId: previewApiCredentialReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckProductEligibility checks a product.
func (s *ProductValidationServer) CheckProductEligibility(ctx context.Context, req *catalogv1.CheckProductEligibilityRequest) (*catalogv1.CheckProductEligibilityResponse, error) {
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "check product eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	productCache := cache.New[string, *catalogv1.Product](1)
	productCache.Put(req.GetContext().GetTraceId(), req.GetProduct())
	cachedProduct, _ := productCache.Get(req.GetContext().GetTraceId())
	productCache2 := cache.New[string, *catalogv1.Product](1)
	productCache2.Put(req.GetContext().GetTraceId(), req.GetProduct())
	cachedProduct2, _ := productCache2.Get(req.GetContext().GetTraceId())
	calculateProductRequest := &catalogv1.CalculateProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{CreatedBy: cachedProduct.GetAudit().GetCreatedBy()},
			Owner: &identityv1.UserRef{UserId: cachedProduct2.GetOwner().GetUserId()},
		},
	}
	type calculateProductResult struct {
		reply *catalogv1.CalculateProductResponse
		err   error
	}
	calculateProductResults := make(chan calculateProductResult, 1)
	go func() {
		reply, err := s.productCalculationClient.CalculateProduct(ctx, calculateProductRequest)
		calculateProductResults <- calculateProductResult{reply, err}
	}()
	var calculateProductReply *catalogv1.CalculateProductResponse
	select {
	case result := <-calculateProductResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.catalog.v1.ProductCalculationService/CalculateProduct")
		}
		calculateProductReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	explainProductCalculationRequest := &catalogv1.ExplainProductCalculationRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				Email: calculateProductReply.GetProduct().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: calculateProductReply.GetProduct().GetAudit().GetEtag(),
			},
			Dimensions: &catalogv1.ProductDimensions{Unit: req.GetProduct().GetDimensions().GetUnit()},
		},
	}
	explainProductCalculationReply, err := grpcx.Call(ctx, s.productCalculationClient.ExplainProductCalculation, explainProductCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/ExplainProductCalculation")
	}
	checkApiCredentialEligibilityRequest := &identityv1.CheckApiCredentialEligibilityRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainProductCalculationReply.GetProduct().GetAudit().GetUpdatedAt(),
				CreatedAt: explainProductCalculationReply.GetProduct().GetAudit().GetCreatedAt(),
				Etag:      calculateProductReply.GetProduct().GetAudit().GetEtag(),
			},
		},
	}
	checkApiCredentialEligibilityReply, err := grpcx.Call(ctx, s.apiCredentialValidationClient.CheckApiCredentialEligibility, checkApiCredentialEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/CheckApiCredentialEligibility")
	}
	_ = checkApiCredentialEligibilityReply
	return &catalogv1.CheckProductEligibilityResponse{
		Product: &catalogv1.Product{
			Dimensions: &catalogv1.ProductDimensions{
				Height: calculateProductReply.GetProduct().GetDimensions().GetHeight(),
			},
			Components: map[string]*catalogv1.ProductComponent{"primary": &catalogv1.ProductComponent{
				Description: calculateProductReply.GetProduct().GetComponents()["primary"].GetDescription(),
			}},
			Capacity:  explainProductCalculationReply.GetProduct().GetCapacity(),
			ProductId: req.GetProductId(),
			Specs:     req.GetProduct().GetSpecs(),
		},
		Result: &commonv1.OperationResult{
			Details: explainProductCalculationReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ExplainProductDecision explains a product.
func (s *ProductValidationServer) ExplainProductDecision(ctx context.Context, req *catalogv1.ExplainProductDecisionRequest) (*catalogv1.ExplainProductDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "explain product decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	ledgerAccountValidation, ok := s.clients.Get("acme.ledger.v1.LedgerAccountValidationService").(ledgerv1.LedgerAccountValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.ledger.v1.LedgerAccountValidationService")
	}
	ctx = tenancy.With(ctx, "email", req.GetProduct().GetOwner().GetEmail())
	var holder struct{ PreferredChannel string }
	if record := req.GetDetails().GetContact(); record != nil {
		if err := mapper.CopyFields(&holder, record, "PreferredChannel"); err != nil {
			s.log.WarnContext(ctx, "copy preferred_channel", "error", err)
		}
	}
	validateLedgerAccountRequest := &ledgerv1.ValidateLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetContact().GetName(),
				Email: tenancy.Value(ctx, "email"),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetProduct().GetAudit().GetEtag()},
		},
		Details: &ledgerv1.ValidateLedgerAccountRequest_Details{
			Contact: &commonv1.ContactInfo{PreferredChannel: holder.PreferredChannel},
		},
	}
	validateLedgerAccountReply, err := ledgerAccountValidation.ValidateLedgerAccount(ctx, validateLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ValidateLedgerAccount")
	}
	quoteProductReply, err := s.productCalculationClient.QuoteProduct(ctx, &catalogv1.QuoteProductRequest{
		Context: req.GetContext(),
		Details: &catalogv1.QuoteProductRequest_Details{
			Destination: &commonv1.PostalAddress{PostalCode: req.GetProduct().GetAddress().GetPostalCode()},
		},
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{UserId: req.GetProduct().GetOwner().GetUserId()},
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateLedgerAccountReply.GetLedgerAccount().GetAudit().GetCreatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/QuoteProduct")
	}
	scoreApiCredentialRequest := &identityv1.ScoreApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Holder: &identityv1.SessionRef{
				DisplayName: validateLedgerAccountReply.GetLedgerAccount().GetHolder().GetDisplayName(),
			},
		},
	}
	scoreApiCredentialReply, err := grpcx.Call(ctx, s.apiCredentialValidationClient.ScoreApiCredential, scoreApiCredentialRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/ScoreApiCredential")
	}
	return &catalogv1.ExplainProductDecisionResponse{
		Result: &commonv1.OperationResult{
			Message: validateLedgerAccountReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &catalogv1.ExplainProductDecisionResponse_Outcome{Status: validateLedgerAccountReply.GetResult().GetStatus()},
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				CreatedAt: quoteProductReply.GetProduct().GetAudit().GetCreatedAt(),
				CreatedBy: scoreApiCredentialReply.GetApiCredential().GetAudit().GetCreatedBy(),
			},
			CategoryPath: quoteProductReply.GetProduct().GetCategoryPath(),
			ProductId:    req.GetProductId(),
			Specs:        req.GetProduct().GetSpecs(),
		},
	}, nil
}

// ScoreProduct scores a product.
func (s *ProductValidationServer) ScoreProduct(ctx context.Context, req *catalogv1.ScoreProductRequest) (*catalogv1.ScoreProductResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreProduct", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "score product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	previewPaymentRequest := buildPreviewPaymentRequestForScoreProduct(req)
	previewPaymentReply, err := s.paymentValidationClient.PreviewPayment(ctx, previewPaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/PreviewPayment")
	}
	packed, err := anypb.New(req.GetProduct())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreUsageRecordRequest := &subscriptionsv1.ScoreUsageRecordRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.ScoreUsageRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: previewPaymentReply.GetPayment().GetBillingAddress().GetRecipient(),
			},
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{Email: req.GetProduct().GetOwner().GetEmail()},
			Detail:  packed,
		},
	}
	scoreUsageRecordReply, err := grpcx.Call(ctx, s.usageRecordValidationClient.ScoreUsageRecord, scoreUsageRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordValidationService/ScoreUsageRecord")
	}
	simulateCategoryReply, err := s.categoryCalculationClient.SimulateCategory(ctx, &catalogv1.SimulateCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{DisplayName: req.GetProduct().GetOwner().GetDisplayName()},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/SimulateCategory")
	}
	return &catalogv1.ScoreProductResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: previewPaymentReply.GetResult().GetReferenceIds(),
			Status:       scoreUsageRecordReply.GetResult().GetStatus(),
		},
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				DisplayName: scoreUsageRecordReply.GetUsageRecord().GetSubject().GetDisplayName(),
			},
			ProductId: req.GetProductId(),
			Capacity:  req.GetProduct().GetCapacity(),
		},
		Breakdown: &catalogv1.ScoreProductResponse_Breakdown{
			CurrencyCode: simulateCategoryReply.GetBreakdown().GetCurrencyCode(),
		},
	}, nil
}

// LintProducts lints a product.
func (s *ProductValidationServer) LintProducts(ctx context.Context, req *catalogv1.LintProductsRequest) (*catalogv1.LintProductsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintProducts", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "lint products", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	var countryCode string
	switch choice := req.GetProduct().GetLocator().(type) {
	case *catalogv1.Product_Address:
		countryCode = choice.Address.GetCountryCode()
	case *catalogv1.Product_FacilityCode:
		countryCode = choice.FacilityCode
	}
	simulateProductReply, err := s.productCalculationClient.SimulateProduct(ctx, &catalogv1.SimulateProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetProduct().GetAudit().GetCreatedAt()},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{CountryCode: countryCode}},
			Dimensions: &catalogv1.ProductDimensions{
				Weight: req.GetProduct().GetDimensions().GetWeight(),
				Length: req.GetProduct().GetDimensions().GetLength(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/SimulateProduct")
	}
	compareProductOptionsRequest := &catalogv1.CompareProductOptionsRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{Email: req.GetProduct().GetOwner().GetEmail()},
			Dimensions: &catalogv1.ProductDimensions{
				Unit:   req.GetProduct().GetDimensions().GetUnit(),
				Length: simulateProductReply.GetProduct().GetDimensions().GetLength(),
			},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				Region: simulateProductReply.GetProduct().GetAddress().GetRegion(),
			}},
		},
	}
	compareProductOptionsReply := new(catalogv1.CompareProductOptionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, catalogv1.ProductCalculationService_CompareProductOptions_FullMethodName, compareProductOptionsRequest, compareProductOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/CompareProductOptions")
	}
	return &catalogv1.LintProductsResponse{
		Product: &catalogv1.Product{
			Components: map[string]*catalogv1.ProductComponent{"primary": &catalogv1.ProductComponent{
				Description: simulateProductReply.GetProduct().GetComponents()["primary"].GetDescription(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: simulateProductReply.GetProduct().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				Email: compareProductOptionsReply.GetProduct().GetOwner().GetEmail(),
			},
			ProductId: req.GetProductId(),
			Specs:     req.GetProduct().GetSpecs(),
		},
		Outcome: &catalogv1.LintProductsResponse_Outcome{Status: compareProductOptionsReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetProductRules returns a product.
func (s *ProductValidationServer) GetProductRules(ctx context.Context, req *catalogv1.GetProductRulesRequest) (*catalogv1.GetProductRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product", req.GetProductId(), "get_product_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get product rules", "tenant", req.GetContext().GetTenantId())
	estimateCategoryRequest := &catalogv1.EstimateCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						UserId: req.GetContext().GetActor().GetUserId(),
						Email:  req.GetContext().GetActor().GetEmail(),
					},
					Children: []*catalogv1.Category{&catalogv1.Category{ParentId: req.GetProductId()}},
					Audit:    &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
				}},
			}},
		},
	}
	estimateCategoryReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*catalogv1.EstimateCategoryResponse, error) {
		return s.categoryCalculationClient.EstimateCategory(ctx, estimateCategoryRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/EstimateCategory")
	}
	explainSearchIndexDecisionRequest := newExplainSearchIndexDecisionRequest(withExplainSearchIndexDecisionContext(req.GetContext()), withExplainSearchIndexDecisionSearchIndex(&searchv1.SearchIndex{
		Audit: &commonv1.AuditInfo{
			CreatedBy: estimateCategoryReply.GetCategory().GetAudit().GetCreatedBy(),
		},
		Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{
			CountryCode: estimateCategoryReply.GetCategory().GetLocation().GetCountryCode(),
		}},
	}))
	explainSearchIndexDecisionReply, err := s.searchIndexValidationClient.ExplainSearchIndexDecision(ctx, explainSearchIndexDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/ExplainSearchIndexDecision")
	}
	return &catalogv1.GetProductRulesResponse{
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				UserId: estimateCategoryReply.GetCategory().GetManager().GetUserId(),
			},
			Specs: explainSearchIndexDecisionReply.GetSearchIndex().GetSpecs(),
			Dimensions: &catalogv1.ProductDimensions{
				Length: explainSearchIndexDecisionReply.GetSearchIndex().GetDimensions().GetLength(),
			},
			ProductId: req.GetProductId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
