package loyalty

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// PointsLedgerQueryServer implements `acme.loyalty.v1.PointsLedgerQueryService`.
// Answers read-only questions about points ledgers, for screens and reports.
type PointsLedgerQueryServer struct {
	loyaltyv1.UnimplementedPointsLedgerQueryServiceServer
	store                         *Store
	log                           *slog.Logger
	sellerListingQueryClient      sellersv1.SellerListingQueryServiceClient
	synonymValidationClient       searchv1.SynonymValidationServiceClient
	formQueryClient               formsv1.FormQueryServiceClient
	pointsLedgerCalculationPort   pointsLedgerCalculationPort
	synonymNotificationClient     searchv1.SynonymNotificationServiceClient
	pointsLedgerCalculationClient loyaltyv1.PointsLedgerCalculationServiceClient
	accessPolicyClient            authorizationv1.AccessPolicyServiceClient
	conn                          *grpc.ClientConn
	getPointsLedgerRatesFn        func(context.Context, *loyaltyv1.GetPointsLedgerRatesRequest, ...grpc.CallOption) (*loyaltyv1.GetPointsLedgerRatesResponse, error)
}

// NewPointsLedgerQueryServer wires a PointsLedgerQueryServer.
func NewPointsLedgerQueryServer(deps platform.Deps) *PointsLedgerQueryServer {
	pointsLedgerCalculation := loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn)
	return &PointsLedgerQueryServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.loyalty.v1.PointsLedgerQueryService"),
		sellerListingQueryClient:      sellersv1.NewSellerListingQueryServiceClient(deps.Conn),
		synonymValidationClient:       searchv1.NewSynonymValidationServiceClient(deps.Conn),
		formQueryClient:               formsv1.NewFormQueryServiceClient(deps.Conn),
		pointsLedgerCalculationPort:   newPointsLedgerCalculationPort(deps),
		synonymNotificationClient:     searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		pointsLedgerCalculationClient: loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		accessPolicyClient:            authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		conn:                          deps.Conn,
		getPointsLedgerRatesFn:        pointsLedgerCalculation.GetPointsLedgerRates,
	}
}

// LookupPointsLedger returns a points ledger.
func (s *PointsLedgerQueryServer) LookupPointsLedger(ctx context.Context, req *loyaltyv1.LookupPointsLedgerRequest) (*loyaltyv1.LookupPointsLedgerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "points_ledger", req.GetPointsLedgerId(), "lookup_points_ledger"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup points ledger", "tenant", req.GetContext().GetTenantId())
	exportSellerListingsStream, err := s.sellerListingQueryClient.ExportSellerListings(ctx, &sellersv1.ExportSellerListingsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ExportSellerListingsRequest_Filter{
			OwnerId:      req.GetContext().GetActor().GetUserId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/ExportSellerListings")
	}
	for {
		received, err := exportSellerListingsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/ExportSellerListings")
		}
		_ = received
	}
	return &loyaltyv1.LookupPointsLedgerResponse{
		PointsLedger: &loyaltyv1.PointsLedger{PointsLedgerId: req.GetPointsLedgerId()},
		Result:       &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowsePointsLedgers lists points ledgers a page at a time.
func (s *PointsLedgerQueryServer) BrowsePointsLedgers(ctx context.Context, req *loyaltyv1.BrowsePointsLedgersRequest) (*loyaltyv1.BrowsePointsLedgersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse points ledgers", "tenant", req.GetContext().GetTenantId())
	previewSynonymRequest := &searchv1.PreviewSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &searchv1.PreviewSynonymRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	send := s.previewSynonymSender(previewSynonymRequest)
	previewSynonymReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/PreviewSynonym")
	}
	ctx = tenancy.With(ctx, "owner_id", req.GetFilter().GetOwnerId())
	suggestFormsRequest := &formsv1.SuggestFormsRequest{
		Context: req.GetContext(),
		Filter: &formsv1.SuggestFormsRequest_Filter{
			Labels:       req.GetFilter().GetLabels(),
			OwnerId:      tenancy.Value(ctx, "owner_id"),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
	}
	suggestFormsReply, err := formsv1.FormQueryServiceClient.SuggestForms(s.formQueryClient, ctx, suggestFormsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forms.v1.FormQueryService/SuggestForms")
	}
	return &loyaltyv1.BrowsePointsLedgersResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  previewSynonymReply.GetResult().GetReferenceIds(),
			CorrelationId: suggestFormsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// FindPointsLedgers lists points ledgers a page at a time.
func (s *PointsLedgerQueryServer) FindPointsLedgers(ctx context.Context, req *loyaltyv1.FindPointsLedgersRequest) (*loyaltyv1.FindPointsLedgersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "find points ledgers", "tenant", req.GetContext().GetTenantId())
	explainPointsLedgerCalculationRequest := &loyaltyv1.ExplainPointsLedgerCalculationRequest{
		Context:      req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{Labels: req.GetFilter().GetLabels()},
	}
	explainPointsLedgerCalculationReply, err := s.pointsLedgerCalculationPort.ExplainPointsLedgerCalculation(ctx, explainPointsLedgerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/ExplainPointsLedgerCalculation")
	}
	return &loyaltyv1.FindPointsLedgersResponse{
		PointsLedgers: []*loyaltyv1.PointsLedger{&loyaltyv1.PointsLedger{
			Holder: &loyaltyv1.RewardRef{
				RewardId: explainPointsLedgerCalculationReply.GetPointsLedger().GetHolder().GetRewardId(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountPointsLedgers counts points ledgers by status.
func (s *PointsLedgerQueryServer) CountPointsLedgers(ctx context.Context, req *loyaltyv1.CountPointsLedgersRequest) (*loyaltyv1.CountPointsLedgersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountPointsLedgers", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count points ledgers", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendSynonymTestNoticeReply, err := s.synonymNotificationClient.SendSynonymTestNotice(ctx, &searchv1.SendSynonymTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSynonymTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: req.GetFilter().GetLabels(),
			Payload:   packed,
		},
		Annotations: req.GetContext().GetBaggage(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/SendSynonymTestNotice")
	}
	return &loyaltyv1.CountPointsLedgersResponse{
		Result: &commonv1.OperationResult{
			Status:  sendSynonymTestNoticeReply.GetOutcome().GetStatus(),
			Details: sendSynonymTestNoticeReply.GetResult().GetDetails(),
		},
	}, nil
}

// ExportPointsLedgers streams every matching points ledger.
func (s *PointsLedgerQueryServer) ExportPointsLedgers(req *loyaltyv1.ExportPointsLedgersRequest, stream grpc.ServerStreamingServer[loyaltyv1.ExportPointsLedgersResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "export points ledgers", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded loyaltyv1.ExportPointsLedgersRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	calculatePointsLedgerRequest := &loyaltyv1.CalculatePointsLedgerRequest{}
	calculatePointsLedgerRequest.Context = req.GetContext()
	calculatePointsLedgerRequest.PointsLedger = &loyaltyv1.PointsLedger{
		Labels:  req.GetFilter().GetLabels(),
		Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
	}
	calculatePointsLedgerRequest.Details = &loyaltyv1.CalculatePointsLedgerRequest_Details{
		Destination: &commonv1.PostalAddress{Region: (&decoded).GetRegion()},
	}
	calculatePointsLedgerReply, err := s.pointsLedgerCalculationClient.CalculatePointsLedger(ctx, calculatePointsLedgerRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/CalculatePointsLedger")
	}
	if err := stream.Send(&loyaltyv1.ExportPointsLedgersResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			CurrencyCode: calculatePointsLedgerReply.GetBreakdown().GetCurrencyCode(),
			Labels:       req.GetFilter().GetLabels(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizePointsLedgers summarizes a points ledger.
func (s *PointsLedgerQueryServer) SummarizePointsLedgers(ctx context.Context, req *loyaltyv1.SummarizePointsLedgersRequest) (*loyaltyv1.SummarizePointsLedgersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "points_ledger", req.GetPointsLedgerId(), "summarize_points_ledgers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize points ledgers", "tenant", req.GetContext().GetTenantId())
	batchGetAccessPoliciesRequest := &authorizationv1.BatchGetAccessPoliciesRequest{Context: req.GetContext()}
	batchGetAccessPoliciesReply, err := authorizationv1.AccessPolicyServiceClient.BatchGetAccessPolicies(s.accessPolicyClient, ctx, batchGetAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/BatchGetAccessPolicies")
	}
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	summarizeSellerListingsRequest := &sellersv1.SummarizeSellerListingsRequest{
		Context: req.GetContext(),
		Details: &sellersv1.SummarizeSellerListingsRequest_Details{
			Filters: filters,
			Format:  req.GetDetails().GetFormat(),
			Period:  req.GetDetails().GetPeriod(),
			Cron:    req.GetDetails().GetCron(),
		},
	}
	summarizeSellerListingsReply := new(sellersv1.SummarizeSellerListingsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", sellersSellerListingQueryServiceName, "SummarizeSellerListings"), summarizeSellerListingsRequest, summarizeSellerListingsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/SummarizeSellerListings")
	}
	return &loyaltyv1.SummarizePointsLedgersResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetAccessPoliciesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ReportId: summarizeSellerListingsReply.GetReportId(),
		Period:   req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestPointsLedgers lists points ledgers a page at a time.
func (s *PointsLedgerQueryServer) SuggestPointsLedgers(ctx context.Context, req *loyaltyv1.SuggestPointsLedgersRequest) (*loyaltyv1.SuggestPointsLedgersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest points ledgers", "tenant", req.GetContext().GetTenantId())
	return &loyaltyv1.SuggestPointsLedgersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// PeekPointsLedger returns a points ledger.
func (s *PointsLedgerQueryServer) PeekPointsLedger(ctx context.Context, req *loyaltyv1.PeekPointsLedgerRequest) (*loyaltyv1.PeekPointsLedgerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "points_ledger", req.GetPointsLedgerId(), "peek_points_ledger"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek points ledger", "tenant", req.GetContext().GetTenantId())
	getPointsLedgerRatesRequest := &loyaltyv1.GetPointsLedgerRatesRequest{
		Context:        req.GetContext(),
		PointsLedgerId: req.GetPointsLedgerId(),
	}
	getPointsLedgerRatesReply, err := s.getPointsLedgerRatesFn(ctx, getPointsLedgerRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/GetPointsLedgerRates")
	}
	return &loyaltyv1.PeekPointsLedgerResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: getPointsLedgerRatesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			Balance:        getPointsLedgerRatesReply.GetPointsLedger().GetBalance(),
			PointsLedgerId: req.GetPointsLedgerId(),
		},
	}, nil
}

// previewSynonymSender returns a function that sends one request to
// acme.search.v1.SynonymValidationService/PreviewSynonym.
func (s *PointsLedgerQueryServer) previewSynonymSender(request *searchv1.PreviewSynonymRequest) func(context.Context) (*searchv1.PreviewSynonymResponse, error) {
	return func(ctx context.Context) (*searchv1.PreviewSynonymResponse, error) {
		return s.synonymValidationClient.PreviewSynonym(ctx, request)
	}
}
