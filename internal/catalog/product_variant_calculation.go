package catalog

import (
	"context"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	seatingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/seating/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ProductVariantCalculationServer implements `acme.catalog.v1.ProductVariantCalculationService`.
// Computes the figures of a product variant, such as totals, taxes, and estimates.
type ProductVariantCalculationServer struct {
	*productVariantCalculationHandlers
}

// productVariantCalculationHandlers holds the handlers of ProductVariantCalculationServer, with what they use.
type productVariantCalculationHandlers struct {
	catalogv1.UnimplementedProductVariantCalculationServiceServer
	store                       *Store
	log                         *slog.Logger
	fraudRuleCalculationClient  fraudv1.FraudRuleCalculationServiceClient
	categoryCalculationClient   catalogv1.CategoryCalculationServiceClient
	usageRecordValidationClient subscriptionsv1.UsageRecordValidationServiceClient
	conn                        *grpc.ClientConn
	tenantNotificationClient    identityv1.TenantNotificationServiceClient
	seatMapClient               seatingv1.SeatMapServiceClient
	subscriptionQueryClient     subscriptionsv1.SubscriptionQueryServiceClient
	compareCategoryOptionsFn    func(context.Context, *catalogv1.CompareCategoryOptionsRequest, ...grpc.CallOption) (*catalogv1.CompareCategoryOptionsResponse, error)
}

// NewProductVariantCalculationServer wires a ProductVariantCalculationServer.
func NewProductVariantCalculationServer(deps platform.Deps) *ProductVariantCalculationServer {
	categoryCalculation := catalogv1.NewCategoryCalculationServiceClient(deps.Conn)
	return &ProductVariantCalculationServer{productVariantCalculationHandlers: &productVariantCalculationHandlers{
		store:                       NewStore(deps.DB),
		log:                         deps.Logger.With("service", "acme.catalog.v1.ProductVariantCalculationService"),
		fraudRuleCalculationClient:  fraudv1.NewFraudRuleCalculationServiceClient(deps.Conn),
		categoryCalculationClient:   catalogv1.NewCategoryCalculationServiceClient(deps.Conn),
		usageRecordValidationClient: subscriptionsv1.NewUsageRecordValidationServiceClient(deps.Conn),
		conn:                        deps.Conn,
		tenantNotificationClient:    identityv1.NewTenantNotificationServiceClient(deps.Conn),
		seatMapClient:               seatingv1.NewSeatMapServiceClient(deps.Conn),
		subscriptionQueryClient:     subscriptionsv1.NewSubscriptionQueryServiceClient(deps.Conn),
		compareCategoryOptionsFn:    categoryCalculation.CompareCategoryOptions,
	}}
}

// CalculateProductVariant calculates a product variant.
func (s *productVariantCalculationHandlers) CalculateProductVariant(ctx context.Context, req *catalogv1.CalculateProductVariantRequest) (*catalogv1.CalculateProductVariantResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateProductVariant", "error", err)
		return nil, err
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "calculate product variant", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetProductVariant())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.ProductVariant
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	quoteFraudRuleRequest := &fraudv1.QuoteFraudRuleRequest{
		Context: req.GetContext(),
		Details: &fraudv1.QuoteFraudRuleRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetProductVariant().GetAddress().GetRegion()},
		},
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetProductVariant().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: (&decoded).GetAudit().GetCreatedBy()},
		},
	}
	var quoteFraudRuleReply *fraudv1.QuoteFraudRuleResponse
	if err := pipeline.Run(ctx, "acme.fraud.v1.FraudRuleCalculationService/QuoteFraudRule", func(ctx context.Context) error {
		answer, err := s.fraudRuleCalculationClient.QuoteFraudRule(ctx, quoteFraudRuleRequest)
		quoteFraudRuleReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleCalculationService/QuoteFraudRule")
	}
	return &catalogv1.CalculateProductVariantResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: quoteFraudRuleReply.GetFraudRule().GetAudit().GetUpdatedAt(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Specs:            req.GetProductVariant().GetSpecs(),
		},
		Result: &commonv1.OperationResult{
			Message: quoteFraudRuleReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// EstimateProductVariant estimates a product variant.
func (s *productVariantCalculationHandlers) EstimateProductVariant(ctx context.Context, req *catalogv1.EstimateProductVariantRequest) (*catalogv1.EstimateProductVariantResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "estimate product variant", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	calculateCategoryRequest := &catalogv1.CalculateCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						CreatedAt: req.GetProductVariant().GetAudit().GetCreatedAt(),
					},
					Location: &commonv1.PostalAddress{
						CountryCode: req.GetProductVariant().GetAddress().GetCountryCode(),
						PostalCode:  req.GetProductVariant().GetAddress().GetPostalCode(),
					},
				}},
			}},
		},
	}
	calculateCategoryReply, err := calculateCategoryCall(ctx, calculateCategoryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/CalculateCategory")
	}
	quoteCategoryRequest := buildQuoteCategoryRequestForEstimateProductVariant(req, calculateCategoryReply)
	quoteCategoryReply, err := s.categoryCalculationClient.QuoteCategory(ctx, quoteCategoryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/QuoteCategory")
	}
	return &catalogv1.EstimateProductVariantResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Name: calculateCategoryReply.GetCategory().GetName(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: quoteCategoryReply.GetCategory().GetAudit().GetUpdatedAt(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Capacity:         req.GetProductVariant().GetCapacity(),
		},
		Result: &commonv1.OperationResult{
			Status:        calculateCategoryReply.GetResult().GetStatus(),
			CorrelationId: quoteCategoryReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// CompareProductVariantOptions compares a product variant.
func (s *productVariantCalculationHandlers) CompareProductVariantOptions(ctx context.Context, req *catalogv1.CompareProductVariantOptionsRequest) (*catalogv1.CompareProductVariantOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CompareProductVariantOptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "compare product variant options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := productVariantCalculationHandlersScoreUsageRecordSteps[mode]
	packed, err := anypb.New(req.GetProductVariant())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreUsageRecordRequest := &subscriptionsv1.ScoreUsageRecordRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.ScoreUsageRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: req.GetProductVariant().GetAddress().GetRecipient(),
			},
		},
		UsageRecord: &subscriptionsv1.UsageRecord{Detail: packed},
	}
	scoreUsageRecordReply, err := run(s, ctx, scoreUsageRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordValidationService/ScoreUsageRecord")
	}
	cloned := proto.Clone(req.GetDetails()).(*catalogv1.CompareProductVariantOptionsRequest_Details)
	lintGiftCardBatchesRequest := &giftcardsv1.LintGiftCardBatchesRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
				Recipient:   cloned.GetDestination().GetRecipient(),
			},
		},
	}
	lintGiftCardBatchesReply := new(giftcardsv1.LintGiftCardBatchesResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardBatchValidationService_LintGiftCardBatches_FullMethodName, lintGiftCardBatchesRequest, lintGiftCardBatchesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/LintGiftCardBatches")
	}
	var recipient string
	switch choice := req.GetProductVariant().GetLocator().(type) {
	case *catalogv1.ProductVariant_Address:
		recipient = choice.Address.GetRecipient()
	case *catalogv1.ProductVariant_FacilityCode:
		recipient = choice.FacilityCode
	}
	ctx = tenancy.With(ctx, "country_code", req.GetProductVariant().GetAddress().GetCountryCode())
	quoteCategoryRequest := &catalogv1.QuoteCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						CreatedBy: req.GetProductVariant().GetAudit().GetCreatedBy(),
					},
					Location: &commonv1.PostalAddress{
						Recipient:   recipient,
						CountryCode: tenancy.Value(ctx, "country_code"),
					},
				}},
			}},
		},
	}
	quoteCategoryReply, err := quoteCategoryCall(ctx, quoteCategoryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/QuoteCategory")
	}
	return &catalogv1.CompareProductVariantOptionsResponse{
		Breakdown: &catalogv1.CompareProductVariantOptionsResponse_Breakdown{
			CurrencyCode: scoreUsageRecordReply.GetBreakdown().GetCurrencyCode(),
		},
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				DisplayName: scoreUsageRecordReply.GetUsageRecord().GetSubject().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: lintGiftCardBatchesReply.GetGiftCardBatch().GetAudit().GetUpdatedBy(),
				CreatedBy: quoteCategoryReply.GetCategory().GetAudit().GetCreatedBy(),
				CreatedAt: quoteCategoryReply.GetCategory().GetAudit().GetCreatedAt(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Status:           req.GetProductVariant().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculateProductVariants recalculates a product variant.
func (s *productVariantCalculationHandlers) RecalculateProductVariants(ctx context.Context, req *catalogv1.RecalculateProductVariantsRequest) (*catalogv1.RecalculateProductVariantsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "recalculate product variants", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	recalculateCategoriesRequest := &catalogv1.RecalculateCategoriesRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Location: &commonv1.PostalAddress{
						PostalCode: req.GetProductVariant().GetAddress().GetPostalCode(),
					},
				}},
			}},
		},
	}
	recalculateCategoriesReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*catalogv1.RecalculateCategoriesResponse, error) {
		return s.categoryCalculationClient.RecalculateCategories(ctx, recalculateCategoriesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/RecalculateCategories")
	}
	compareCategoryOptionsRequest := &catalogv1.CompareCategoryOptionsRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Location: &commonv1.PostalAddress{
						PostalCode: req.GetProductVariant().GetAddress().GetPostalCode(),
					},
				}},
			}},
		},
	}
	send := s.compareCategoryOptionsSender(compareCategoryOptionsRequest)
	compareCategoryOptionsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/CompareCategoryOptions")
	}
	return &catalogv1.RecalculateProductVariantsResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Name: recalculateCategoriesReply.GetCategory().GetName(),
			Audit: &commonv1.AuditInfo{
				CreatedAt: compareCategoryOptionsReply.GetCategory().GetAudit().GetCreatedAt(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Specs:            req.GetProductVariant().GetSpecs(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SimulateProductVariant simulates a product variant.
func (s *productVariantCalculationHandlers) SimulateProductVariant(ctx context.Context, req *catalogv1.SimulateProductVariantRequest) (*catalogv1.SimulateProductVariantResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "simulate product variant", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	getTenantPreferencesRequest := buildGetTenantPreferencesRequestForSimulateProductVariant(req)
	getTenantPreferencesReply, err := s.tenantNotificationClient.GetTenantPreferences(ctx, getTenantPreferencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/GetTenantPreferences")
	}
	var region string
	switch choice := req.GetProductVariant().GetLocator().(type) {
	case *catalogv1.ProductVariant_Address:
		region = choice.Address.GetRegion()
	case *catalogv1.ProductVariant_FacilityCode:
		region = choice.FacilityCode
	}
	explainCategoryCalculationReply, err := s.categoryCalculationClient.ExplainCategoryCalculation(ctx, &catalogv1.ExplainCategoryCalculationRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Location: &commonv1.PostalAddress{
						Recipient: getTenantPreferencesReply.GetTenant().GetLocation().GetRecipient(),
						Region:    region,
					},
					Audit: &commonv1.AuditInfo{
						Etag: getTenantPreferencesReply.GetTenant().GetAudit().GetEtag(),
					},
					Manager: &identityv1.UserRef{
						UserId: getTenantPreferencesReply.GetTenant().GetManager().GetUserId(),
					},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/ExplainCategoryCalculation")
	}
	cloned := proto.Clone(req.GetDetails()).(*catalogv1.SimulateProductVariantRequest_Details)
	batchCreateSeatMapsStream, err := s.seatMapClient.BatchCreateSeatMaps(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.seating.v1.SeatMapService/BatchCreateSeatMaps")
	}
	if err := batchCreateSeatMapsStream.Send(&seatingv1.BatchCreateSeatMapsRequest{
		Context: req.GetContext(),
		Chunk: &seatingv1.BatchCreateSeatMapsRequest_Record{Record: &seatingv1.SeatMap{
			Children: []*seatingv1.SeatMap{&seatingv1.SeatMap{
				Children: []*seatingv1.SeatMap{&seatingv1.SeatMap{
					Location: &commonv1.PostalAddress{
						Lines:       cloned.GetDestination().GetLines(),
						CountryCode: explainCategoryCalculationReply.GetCategory().GetLocation().GetCountryCode(),
					},
					Audit: &commonv1.AuditInfo{
						Etag:      getTenantPreferencesReply.GetTenant().GetAudit().GetEtag(),
						UpdatedAt: req.GetProductVariant().GetAudit().GetUpdatedAt(),
					},
				}},
			}},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.seating.v1.SeatMapService/BatchCreateSeatMaps")
	}
	if _, err := batchCreateSeatMapsStream.CloseAndRecv(); err != nil {
		return nil, grpcx.Wrap(err, "acme.seating.v1.SeatMapService/BatchCreateSeatMaps")
	}
	return &catalogv1.SimulateProductVariantResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Audit: &commonv1.AuditInfo{
				CreatedAt: getTenantPreferencesReply.GetTenant().GetAudit().GetCreatedAt(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Name:             req.GetProductVariant().GetName(),
		},
		Result: &commonv1.OperationResult{
			Details: getTenantPreferencesReply.GetResult().GetDetails(),
			Status:  explainCategoryCalculationReply.GetResult().GetStatus(),
		},
	}, nil
}

// GetProductVariantRates returns a product variant.
func (s *productVariantCalculationHandlers) GetProductVariantRates(ctx context.Context, req *catalogv1.GetProductVariantRatesRequest) (*catalogv1.GetProductVariantRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetProductVariantRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "get_product_variant_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get product variant rates", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	findSubscriptionsRequest := &subscriptionsv1.FindSubscriptionsRequest{}
	findSubscriptionsRequest.Context = req.GetContext()
	findSubscriptionsRequest.Filter = &subscriptionsv1.FindSubscriptionsRequest_Filter{
		CreatedAfter: req.GetContext().GetDeadline(),
		OwnerId:      req.GetProductVariantId(),
		Labels:       baggage,
	}
	findSubscriptionsReply, err := s.subscriptionQueryClient.FindSubscriptions(ctx, findSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionQueryService/FindSubscriptions")
	}
	var holder struct{ UserId string }
	if record := req.GetContext().GetActor(); record != nil {
		if err := mapper.CopyFields(&holder, record, "UserId"); err != nil {
			s.log.WarnContext(ctx, "copy user_id", "error", err)
		}
	}
	compareCategoryOptionsRequest := &catalogv1.CompareCategoryOptionsRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						UserId: holder.UserId,
						Email:  req.GetContext().GetActor().GetEmail(),
					},
					Children: []*catalogv1.Category{&catalogv1.Category{
						Attributes: findSubscriptionsReply.GetResult().GetDetails(),
						ParentId:   findSubscriptionsReply.GetResult().GetCorrelationId(),
					}},
				}},
			}},
		},
	}
	compareCategoryOptionsReply, err := s.compareCategoryOptionsFn(ctx, compareCategoryOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/CompareCategoryOptions")
	}
	return &catalogv1.GetProductVariantRatesResponse{
		Result: &commonv1.OperationResult{Status: findSubscriptionsReply.GetResult().GetStatus()},
		ProductVariant: &catalogv1.ProductVariant{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: compareCategoryOptionsReply.GetCategory().GetAudit().GetUpdatedAt(),
			},
			ProductVariantId: req.GetProductVariantId(),
		},
	}, nil
}

// productVariantCalculationHandlersScoreUsageRecordSteps are the ways to run
// acme.subscriptions.v1.UsageRecordValidationService/ScoreUsageRecord, by name.
var productVariantCalculationHandlersScoreUsageRecordSteps = map[string]func(*productVariantCalculationHandlers, context.Context, *subscriptionsv1.ScoreUsageRecordRequest) (*subscriptionsv1.ScoreUsageRecordResponse, error){
	"default": (*productVariantCalculationHandlers).scoreUsageRecordStep,
	"audited": (*productVariantCalculationHandlers).scoreUsageRecordAuditedStep,
}

func (s *productVariantCalculationHandlers) scoreUsageRecordStep(ctx context.Context, request *subscriptionsv1.ScoreUsageRecordRequest) (*subscriptionsv1.ScoreUsageRecordResponse, error) {
	return s.usageRecordValidationClient.ScoreUsageRecord(ctx, request)
}

func (s *productVariantCalculationHandlers) scoreUsageRecordAuditedStep(ctx context.Context, request *subscriptionsv1.ScoreUsageRecordRequest) (*subscriptionsv1.ScoreUsageRecordResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.subscriptions.v1.UsageRecordValidationService/ScoreUsageRecord")
	return s.usageRecordValidationClient.ScoreUsageRecord(ctx, request)
}

// compareCategoryOptionsSender returns a function that sends one request to
// acme.catalog.v1.CategoryCalculationService/CompareCategoryOptions.
func (s *productVariantCalculationHandlers) compareCategoryOptionsSender(request *catalogv1.CompareCategoryOptionsRequest) func(context.Context) (*catalogv1.CompareCategoryOptionsResponse, error) {
	return func(ctx context.Context) (*catalogv1.CompareCategoryOptionsResponse, error) {
		return s.categoryCalculationClient.CompareCategoryOptions(ctx, request)
	}
}
