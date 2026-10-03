package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CustomerQueryServer implements `acme.customers.v1.CustomerQueryService`.
// Answers read-only questions about customers, for screens and reports.
type CustomerQueryServer struct {
	customersv1.UnimplementedCustomerQueryServiceServer
	store                            *Store
	log                              *slog.Logger
	searchQueryExportClient          searchv1.SearchQueryExportServiceClient
	customerSegmentValidationClient  customersv1.CustomerSegmentValidationServiceClient
	meteredPriceQuoteQuery           meteredPriceQuoteQuery
	productVariantValidationClient   catalogv1.ProductVariantValidationServiceClient
	clients                          *registry.Registry
	conn                             *grpc.ClientConn
	customerSegmentCalculationOnce   sync.Once
	customerSegmentCalculationCached customersv1.CustomerSegmentCalculationServiceClient
	meteredRewardCalculation         meteredRewardCalculation
}

// NewCustomerQueryServer wires a CustomerQueryServer.
func NewCustomerQueryServer(deps platform.Deps) *CustomerQueryServer {
	return &CustomerQueryServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.customers.v1.CustomerQueryService"),
		searchQueryExportClient:         searchv1.NewSearchQueryExportServiceClient(deps.Conn),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		meteredPriceQuoteQuery:          meteredPriceQuoteQuery{PriceQuoteQueryServiceClient: pricingv1.NewPriceQuoteQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		productVariantValidationClient:  catalogv1.NewProductVariantValidationServiceClient(deps.Conn),
		clients:                         deps.Clients,
		conn:                            deps.Conn,
		meteredRewardCalculation:        meteredRewardCalculation{RewardCalculationServiceClient: loyaltyv1.NewRewardCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// LookupCustomer returns a customer.
func (s *CustomerQueryServer) LookupCustomer(ctx context.Context, req *customersv1.LookupCustomerRequest) (*customersv1.LookupCustomerResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer", req.GetCustomerId(), "lookup_customer"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup customer", "tenant", req.GetContext().GetTenantId())
	return &customersv1.LookupCustomerResponse{
		Customer: &customersv1.Customer{CustomerId: req.GetCustomerId()},
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseCustomers lists customers a page at a time.
func (s *CustomerQueryServer) BrowseCustomers(ctx context.Context, req *customersv1.BrowseCustomersRequest) (*customersv1.BrowseCustomersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BrowseCustomers", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "browse customers", "tenant", req.GetContext().GetTenantId())
	return &customersv1.BrowseCustomersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindCustomers lists customers a page at a time.
func (s *CustomerQueryServer) FindCustomers(ctx context.Context, req *customersv1.FindCustomersRequest) (*customersv1.FindCustomersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "find customers", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	streamSearchQueryExportStream, err := s.searchQueryExportClient.StreamSearchQueryExport(ctx, &searchv1.StreamSearchQueryExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSearchQueryExportRequest_Filter{
			Region:       req.GetFilter().GetRegion(),
			Query:        req.GetFilter().GetQuery(),
			Labels:       labels,
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/StreamSearchQueryExport")
	}
	for {
		received, err := streamSearchQueryExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/StreamSearchQueryExport")
		}
		_ = received
	}
	return &customersv1.FindCustomersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountCustomers counts customers by status.
func (s *CustomerQueryServer) CountCustomers(ctx context.Context, req *customersv1.CountCustomersRequest) (*customersv1.CountCustomersResponse, error) {
	s.log.InfoContext(ctx, "count customers", "tenant", req.GetContext().GetTenantId())
	checkCustomerSegmentEligibilityRequest := &customersv1.CheckCustomerSegmentEligibilityRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &customersv1.CheckCustomerSegmentEligibilityRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	checkCustomerSegmentEligibilityReply, err := customersv1.CustomerSegmentValidationServiceClient.CheckCustomerSegmentEligibility(s.customerSegmentValidationClient, ctx, checkCustomerSegmentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/CheckCustomerSegmentEligibility")
	}
	return &customersv1.CountCustomersResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: checkCustomerSegmentEligibilityReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ResolveCustomers returns several customers at once.
func (s *CustomerQueryServer) ResolveCustomers(ctx context.Context, req *customersv1.ResolveCustomersRequest) (*customersv1.ResolveCustomersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "resolve customers", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().ProtoReflect()
	tenantId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("tenant_id"))).String()
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	suggestPriceQuotesRequest := &pricingv1.SuggestPriceQuotesRequest{
		Context: req.GetContext(),
		Filter: &pricingv1.SuggestPriceQuotesRequest_Filter{
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      tenantId,
			Labels:       baggage,
		},
	}
	suggestPriceQuotesReply, err := s.meteredPriceQuoteQuery.SuggestPriceQuotes(ctx, suggestPriceQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteQueryService/SuggestPriceQuotes")
	}
	return &customersv1.ResolveCustomersResponse{
		Result: &commonv1.OperationResult{
			Details: suggestPriceQuotesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ExportCustomers streams every matching customer.
func (s *CustomerQueryServer) ExportCustomers(req *customersv1.ExportCustomersRequest, stream grpc.ServerStreamingServer[customersv1.ExportCustomersResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export customers", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.ExportCustomersRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	encoded2, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded2 commonv1.RequestContext
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return err
	}
	lintProductVariantsRequest := &catalogv1.LintProductVariantsRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Locator: &catalogv1.ProductVariant_Address{Address: &commonv1.PostalAddress{Region: (&decoded).GetRegion()}},
			Owner: &identityv1.UserRef{
				Email:  (&decoded2).GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	}
	lintProductVariantsReply, err := grpcx.Call(ctx, s.productVariantValidationClient.LintProductVariants, lintProductVariantsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/LintProductVariants")
	}
	featureFlagValidation, err := registry.Lookup[featureflagsv1.FeatureFlagValidationServiceClient](s.clients, "acme.featureflags.v1.FeatureFlagValidationService")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	lintFeatureFlagsRequest := &featureflagsv1.LintFeatureFlagsRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{
				Etag:      lintProductVariantsReply.GetProductVariant().GetAudit().GetEtag(),
				UpdatedAt: lintProductVariantsReply.GetProductVariant().GetAudit().GetUpdatedAt(),
			},
			Owner:       lintProductVariantsReply.GetProductVariant().GetOwner(),
			Description: lintProductVariantsReply.GetProductVariant().GetComponents()["primary"].GetDescription(),
		},
	}
	lintFeatureFlagsReply, err := featureFlagValidation.LintFeatureFlags(ctx, lintFeatureFlagsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/LintFeatureFlags")
	}
	explainCustomerSegmentDecisionReply, err := s.customerSegmentValidationClient.ExplainCustomerSegmentDecision(ctx, &customersv1.ExplainCustomerSegmentDecisionRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Route{Route: &customersv1.CustomerSegmentRouteAction{
				Target: lintFeatureFlagsReply.GetFeatureFlag().GetRoute().GetTarget(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintProductVariantsReply.GetProductVariant().GetAudit().GetCreatedBy(),
				UpdatedBy: lintProductVariantsReply.GetProductVariant().GetAudit().GetUpdatedBy(),
			},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ExplainCustomerSegmentDecision")
	}
	if err := stream.Send(&customersv1.ExportCustomersResponse{
		Result: &commonv1.OperationResult{
			Message: lintProductVariantsReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: lintProductVariantsReply.GetProductVariant().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      lintFeatureFlagsReply.GetFeatureFlag().GetAudit().GetEtag(),
				UpdatedBy: explainCustomerSegmentDecisionReply.GetCustomerSegment().GetAudit().GetUpdatedBy(),
				CreatedAt: explainCustomerSegmentDecisionReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
			},
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeCustomers summarizes a customer.
func (s *CustomerQueryServer) SummarizeCustomers(ctx context.Context, req *customersv1.SummarizeCustomersRequest) (*customersv1.SummarizeCustomersResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer", req.GetCustomerId(), "summarize_customers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize customers", "tenant", req.GetContext().GetTenantId())
	estimateHouseholdRequest := &customersv1.EstimateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
				}},
			}},
		},
	}
	estimateHouseholdReply := new(customersv1.EstimateHouseholdResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.HouseholdCalculationService_EstimateHousehold_FullMethodName, estimateHouseholdRequest, estimateHouseholdReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/EstimateHousehold")
	}
	compareCustomerSegmentOptionsRequest := &customersv1.CompareCustomerSegmentOptionsRequest{
		Context: req.GetContext(),
		Details: &customersv1.CompareCustomerSegmentOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: estimateHouseholdReply.GetHousehold().GetLocation().GetCountryCode(),
			},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				DisplayName: estimateHouseholdReply.GetHousehold().GetManager().GetDisplayName(),
			},
		},
	}
	compareCustomerSegmentOptionsReply, err := s.customerSegmentCalculationLazy().CompareCustomerSegmentOptions(ctx, compareCustomerSegmentOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/CompareCustomerSegmentOptions")
	}
	previewCustomerSegmentRequest := &customersv1.PreviewCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{
				Reason: compareCustomerSegmentOptionsReply.GetCustomerSegment().GetBlock().GetReason(),
			}},
		},
	}
	var previewCustomerSegmentReply *customersv1.PreviewCustomerSegmentResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.CustomerSegmentValidationService/PreviewCustomerSegment", func(ctx context.Context) error {
		answer, err := s.customerSegmentValidationClient.PreviewCustomerSegment(ctx, previewCustomerSegmentRequest)
		previewCustomerSegmentReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/PreviewCustomerSegment")
	}
	recalculateRewardsRequest := &loyaltyv1.RecalculateRewardsRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.RecalculateRewardsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: estimateHouseholdReply.GetHousehold().GetLocation().GetLocality(),
				Region:   estimateHouseholdReply.GetHousehold().GetLocation().GetRegion(),
			},
		},
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				Email: compareCustomerSegmentOptionsReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: compareCustomerSegmentOptionsReply.GetCustomerSegment().GetAudit().GetCreatedBy(),
			},
		},
	}
	recalculateRewardsReply, err := s.meteredRewardCalculation.RecalculateRewards(ctx, recalculateRewardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/RecalculateRewards")
	}
	_ = previewCustomerSegmentReply
	return &customersv1.SummarizeCustomersResponse{
		Result: &commonv1.OperationResult{
			Status:        estimateHouseholdReply.GetResult().GetStatus(),
			Message:       estimateHouseholdReply.GetResult().GetMessage(),
			ReferenceIds:  compareCustomerSegmentOptionsReply.GetResult().GetReferenceIds(),
			CorrelationId: recalculateRewardsReply.GetResult().GetCorrelationId(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestCustomers lists customers a page at a time.
func (s *CustomerQueryServer) SuggestCustomers(ctx context.Context, req *customersv1.SuggestCustomersRequest) (*customersv1.SuggestCustomersResponse, error) {
	s.log.InfoContext(ctx, "suggest customers", "tenant", req.GetContext().GetTenantId())
	return &customersv1.SuggestCustomersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// PeekCustomer returns a customer.
func (s *CustomerQueryServer) PeekCustomer(ctx context.Context, req *customersv1.PeekCustomerRequest) (*customersv1.PeekCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer", req.GetCustomerId(), "peek_customer"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek customer", "tenant", req.GetContext().GetTenantId())
	return &customersv1.PeekCustomerResponse{
		Customer: &customersv1.Customer{CustomerId: req.GetCustomerId()},
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// customerSegmentCalculationLazy dials acme.customers.v1.CustomerSegmentCalculationService the first time a handler
// needs it.
func (s *CustomerQueryServer) customerSegmentCalculationLazy() customersv1.CustomerSegmentCalculationServiceClient {
	s.customerSegmentCalculationOnce.Do(func() {
		s.customerSegmentCalculationCached = customersv1.NewCustomerSegmentCalculationServiceClient(s.conn)
	})
	return s.customerSegmentCalculationCached
}
