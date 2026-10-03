package shipping

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// CarrierRateReportingServer implements `acme.shipping.v1.CarrierRateReportingService`.
// Builds and schedules reports about carrier rates.
type CarrierRateReportingServer struct {
	shippingv1.UnimplementedCarrierRateReportingServiceServer
	store                             *Store
	log                               *slog.Logger
	searchQueryNotificationClient     searchv1.SearchQueryNotificationServiceClient
	conn                              *grpc.ClientConn
	householdQueryClient              customersv1.HouseholdQueryServiceClient
	resumeMerchandisingRuleWorkflowFn func(context.Context, *merchandisingv1.ResumeMerchandisingRuleWorkflowRequest, ...grpc.CallOption) (*merchandisingv1.ResumeMerchandisingRuleWorkflowResponse, error)
	returnLabelWorkflowClient         returnsv1.ReturnLabelWorkflowServiceClient
	trackingEventCalculationClient    shippingv1.TrackingEventCalculationServiceClient
	householdCalculationClient        customersv1.HouseholdCalculationServiceClient
	carrierRateValidationClient       shippingv1.CarrierRateValidationServiceClient
	featureFlagValidationPort         featureFlagValidationPort
}

// NewCarrierRateReportingServer wires a CarrierRateReportingServer.
func NewCarrierRateReportingServer(deps platform.Deps) CarrierRateReportingServer {
	merchandisingRuleWorkflow := merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn)
	return CarrierRateReportingServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.shipping.v1.CarrierRateReportingService"),
		searchQueryNotificationClient:     searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		conn:                              deps.Conn,
		householdQueryClient:              customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		resumeMerchandisingRuleWorkflowFn: merchandisingRuleWorkflow.ResumeMerchandisingRuleWorkflow,
		returnLabelWorkflowClient:         returnsv1.NewReturnLabelWorkflowServiceClient(deps.Conn),
		trackingEventCalculationClient:    shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn),
		householdCalculationClient:        customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		carrierRateValidationClient:       shippingv1.NewCarrierRateValidationServiceClient(deps.Conn),
		featureFlagValidationPort:         newFeatureFlagValidationPort(deps),
	}
}

// GenerateCarrierRateReport generates a carrier rate.
func (s CarrierRateReportingServer) GenerateCarrierRateReport(ctx context.Context, req *shippingv1.GenerateCarrierRateReportRequest) (*shippingv1.GenerateCarrierRateReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "generate_carrier_rate_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate carrier rate report", "tenant", req.GetContext().GetTenantId())
	subscribeSearchQueryUpdatesRequest := buildSubscribeSearchQueryUpdatesRequestForGenerateCarrierRateReport(req)
	subscribeSearchQueryUpdatesReply, err := s.searchQueryNotificationClient.SubscribeSearchQueryUpdates(ctx, subscribeSearchQueryUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/SubscribeSearchQueryUpdates")
	}
	lintParcelsRequest := &shippingv1.LintParcelsRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				DisplayName: subscribeSearchQueryUpdatesReply.GetSearchQuery().GetSubject().GetDisplayName(),
				UserId:      req.GetContext().GetActor().GetUserId(),
				Email:       req.GetContext().GetActor().GetEmail(),
			},
			Code: subscribeSearchQueryUpdatesReply.GetSearchQuery().GetSubject().GetCode(),
		},
	}
	lintParcelsReply := new(shippingv1.LintParcelsResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.ParcelValidationService_LintParcels_FullMethodName, lintParcelsRequest, lintParcelsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/LintParcels")
	}
	summarizeHouseholdsRequest := &customersv1.SummarizeHouseholdsRequest{
		Context: req.GetContext(),
		Details: &customersv1.SummarizeHouseholdsRequest_Details{Period: req.GetDetails().GetPeriod()},
	}
	type summarizeHouseholdsResult struct {
		reply *customersv1.SummarizeHouseholdsResponse
		err   error
	}
	summarizeHouseholdsResults := make(chan summarizeHouseholdsResult, 1)
	go func() {
		reply, err := s.householdQueryClient.SummarizeHouseholds(ctx, summarizeHouseholdsRequest)
		summarizeHouseholdsResults <- summarizeHouseholdsResult{reply, err}
	}()
	var summarizeHouseholdsReply *customersv1.SummarizeHouseholdsResponse
	select {
	case result := <-summarizeHouseholdsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.HouseholdQueryService/SummarizeHouseholds")
		}
		summarizeHouseholdsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifySearchQueryWatchersRequest := &searchv1.NotifySearchQueryWatchersRequest{
		Context: req.GetContext(),
		Details: &searchv1.NotifySearchQueryWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: lintParcelsReply.GetParcel().GetOwner().GetEmail()},
			Payload:   packed,
		},
	}
	notifySearchQueryWatchersReply, err := searchv1.SearchQueryNotificationServiceClient.NotifySearchQueryWatchers(s.searchQueryNotificationClient, ctx, notifySearchQueryWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/NotifySearchQueryWatchers")
	}
	_ = notifySearchQueryWatchersReply
	return &shippingv1.GenerateCarrierRateReportResponse{
		Result: &commonv1.OperationResult{
			Status:       subscribeSearchQueryUpdatesReply.GetResult().GetStatus(),
			Message:      subscribeSearchQueryUpdatesReply.GetOutcome().GetMessage(),
			ReferenceIds: lintParcelsReply.GetResult().GetReferenceIds(),
		},
		Summary: summarizeHouseholdsReply.GetSummary(),
		Period:  req.GetDetails().GetPeriod(),
	}, nil
}

// FetchCarrierRateReport returns a carrier rate.
func (s CarrierRateReportingServer) FetchCarrierRateReport(ctx context.Context, req *shippingv1.FetchCarrierRateReportRequest) (*shippingv1.FetchCarrierRateReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "fetch_carrier_rate_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch carrier rate report", "tenant", req.GetContext().GetTenantId())
	resumeMerchandisingRuleWorkflowRequest := &merchandisingv1.ResumeMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		Details: &merchandisingv1.ResumeMerchandisingRuleWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	}
	resumeMerchandisingRuleWorkflowReply, err := s.resumeMerchandisingRuleWorkflowFn(ctx, resumeMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/ResumeMerchandisingRuleWorkflow")
	}
	return &shippingv1.FetchCarrierRateReportResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: resumeMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetAudit().GetUpdatedBy(),
			},
			CarrierRateId: req.GetCarrierRateId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamCarrierRateMetrics streams the changes of carrier rates as they happen.
func (s CarrierRateReportingServer) StreamCarrierRateMetrics(req *shippingv1.StreamCarrierRateMetricsRequest, stream grpc.ServerStreamingServer[shippingv1.StreamCarrierRateMetricsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream carrier rate metrics", "tenant", req.GetContext().GetTenantId())
	pauseReturnLabelWorkflowRequest := &returnsv1.PauseReturnLabelWorkflowRequest{}
	pauseReturnLabelWorkflowRequest.Context = req.GetContext()
	pauseReturnLabelWorkflowRequest.Details = &returnsv1.PauseReturnLabelWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{PreferredChannel: req.GetSubscriber().GetPreferredChannel()},
	}
	pauseReturnLabelWorkflowRequest.ReturnLabel = &returnsv1.ReturnLabel{
		BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
	}
	pauseReturnLabelWorkflowReply, err := s.returnLabelWorkflowClient.PauseReturnLabelWorkflow(ctx, pauseReturnLabelWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.returns.v1.ReturnLabelWorkflowService/PauseReturnLabelWorkflow")
	}
	if err := stream.Send(&shippingv1.StreamCarrierRateMetricsResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: pauseReturnLabelWorkflowReply.GetReturnLabel().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// ListCarrierRateReports lists carrier rates a page at a time.
func (s CarrierRateReportingServer) ListCarrierRateReports(ctx context.Context, req *shippingv1.ListCarrierRateReportsRequest) (*shippingv1.ListCarrierRateReportsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list carrier rate reports", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareTrackingEventOptionsRequest := newCompareTrackingEventOptionsRequest(withCompareTrackingEventOptionsContext(req.GetContext()), withCompareTrackingEventOptionsDetails(&shippingv1.CompareTrackingEventOptionsRequest_Details{
		Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
	}), withCompareTrackingEventOptionsTrackingEvent(&shippingv1.TrackingEvent{
		Measurements: []*shippingv1.TrackingEventMeasurement{&shippingv1.TrackingEventMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
		Detail:       packed,
	}))
	compareTrackingEventOptionsReply, err := s.trackingEventCalculationClient.CompareTrackingEventOptions(ctx, compareTrackingEventOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/CompareTrackingEventOptions")
	}
	estimateHouseholdReply, err := s.householdCalculationClient.EstimateHousehold(ctx, &customersv1.EstimateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						DisplayName: compareTrackingEventOptionsReply.GetTrackingEvent().GetSubject().GetDisplayName(),
					},
					Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/EstimateHousehold")
	}
	return &shippingv1.ListCarrierRateReportsResponse{
		Result: &commonv1.OperationResult{
			Details: compareTrackingEventOptionsReply.GetResult().GetDetails(),
			Status:  compareTrackingEventOptionsReply.GetResult().GetStatus(),
		},
		CarrierRates: []*shippingv1.CarrierRate{&shippingv1.CarrierRate{Name: estimateHouseholdReply.GetHousehold().GetName()}},
	}, nil
}

// ShareCarrierRateReport shares a carrier rate.
func (s CarrierRateReportingServer) ShareCarrierRateReport(ctx context.Context, req *shippingv1.ShareCarrierRateReportRequest) (*shippingv1.ShareCarrierRateReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ShareCarrierRateReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "share_carrier_rate_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share carrier rate report", "tenant", req.GetContext().GetTenantId())
	var envelope shippingv1.ShipmentRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	previewCarrierRateRequest := &shippingv1.PreviewCarrierRateRequest{}
	previewCarrierRateRequest.Context = req.GetContext()
	previewCarrierRateRequest.Details = &shippingv1.PreviewCarrierRateRequest_Details{
		Contact: &commonv1.ContactInfo{
			Phone: req.GetDetails().GetRecipient().GetPhone(),
			Email: req.GetDetails().GetRecipient().GetEmail(),
		},
	}
	previewCarrierRateRequest.CarrierRateId = envelope.GetDisplayName()
	previewCarrierRateReply, err := s.carrierRateValidationClient.PreviewCarrierRate(ctx, previewCarrierRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateValidationService/PreviewCarrierRate")
	}
	var envelope2 shippingv1.ShipmentRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkFeatureFlagEligibilityRequest := &featureflagsv1.CheckFeatureFlagEligibilityRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Owner: &identityv1.UserRef{
				UserId: previewCarrierRateReply.GetCarrierRate().GetOwner().GetUserId(),
			},
		},
		FeatureFlagId: envelope2.GetDisplayName(),
	}
	checkFeatureFlagEligibilityReply, err := s.featureFlagValidationPort.CheckFeatureFlagEligibility(ctx, checkFeatureFlagEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/CheckFeatureFlagEligibility")
	}
	resolveHouseholdsRequest := buildResolveHouseholdsRequestForShareCarrierRateReport(req)
	resolveHouseholdsReply, err := s.householdQueryClient.ResolveHouseholds(ctx, resolveHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ResolveHouseholds")
	}
	return &shippingv1.ShareCarrierRateReportResponse{
		Result: &commonv1.OperationResult{
			Status:  previewCarrierRateReply.GetOutcome().GetStatus(),
			Message: checkFeatureFlagEligibilityReply.GetOutcome().GetMessage(),
			Details: resolveHouseholdsReply.GetResult().GetDetails(),
		},
		CarrierRate: &shippingv1.CarrierRate{
			Owner:         previewCarrierRateReply.GetCarrierRate().GetOwner(),
			CarrierRateId: req.GetCarrierRateId(),
		},
	}, nil
}

// CountCarrierRateTotals counts carrier rates by status.
func (s CarrierRateReportingServer) CountCarrierRateTotals(ctx context.Context, req *shippingv1.CountCarrierRateTotalsRequest) (*shippingv1.CountCarrierRateTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountCarrierRateTotals", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count carrier rate totals", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.CountCarrierRateTotalsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DeleteCarrierRateReport deletes a carrier rate.
func (s CarrierRateReportingServer) DeleteCarrierRateReport(ctx context.Context, req *shippingv1.DeleteCarrierRateReportRequest) (*shippingv1.DeleteCarrierRateReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "delete_carrier_rate_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete carrier rate report", "tenant", req.GetContext().GetTenantId())
	checkParcelEligibilityRequest := &shippingv1.CheckParcelEligibilityRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{Etag: req.GetEtag()},
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &shippingv1.CheckParcelEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	checkParcelEligibilityReply, err := checkParcelEligibilityCall(ctx, checkParcelEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/CheckParcelEligibility")
	}
	simulateParcelRequest := &shippingv1.SimulateParcelRequest{
		Context: req.GetContext(),
		Details: &shippingv1.SimulateParcelRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: checkParcelEligibilityReply.GetParcel().GetAddress().GetRecipient(),
			},
		},
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				Etag: checkParcelEligibilityReply.GetParcel().GetAudit().GetEtag(),
			},
			Components: map[string]*shippingv1.ParcelComponent{"primary": &shippingv1.ParcelComponent{
				Description: checkParcelEligibilityReply.GetParcel().GetComponents()["primary"].GetDescription(),
			}},
			Dimensions: &shippingv1.ParcelDimensions{
				Width: checkParcelEligibilityReply.GetParcel().GetDimensions().GetWidth(),
			},
		},
	}
	simulateParcelReply, err := simulateParcelCall(ctx, simulateParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/SimulateParcel")
	}
	return &shippingv1.DeleteCarrierRateReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: checkParcelEligibilityReply.GetResult().GetCorrelationId(),
			Status:        simulateParcelReply.GetResult().GetStatus(),
		},
	}, nil
}
