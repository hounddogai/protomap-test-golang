package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	assetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/assets/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	driversv1 "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	fuelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fuel/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PreferenceLifecycleServer implements `acme.customers.v1.PreferenceLifecycleService`.
// Moves a preference between the states of its lifecycle.
type PreferenceLifecycleServer struct {
	customersv1.UnimplementedPreferenceLifecycleServiceServer
	store                             *Store
	log                               *slog.Logger
	fuelCardClient                    fuelv1.FuelCardServiceClient
	sessionQueryClient                identityv1.SessionQueryServiceClient
	preferenceReportingClient         customersv1.PreferenceReportingServiceClient
	conn                              *grpc.ClientConn
	venueClient                       venuesv1.VenueServiceClient
	usageRecordApprovalClient         subscriptionsv1.UsageRecordApprovalServiceClient
	usageRecordApprovalClientFallback subscriptionsv1.UsageRecordApprovalServiceClient
	venueQueryClient                  venuesv1.VenueQueryServiceClient
	orderWorkflowClient               ordersv1.OrderWorkflowServiceClient
	sellerListingWorkflowClient       sellersv1.SellerListingWorkflowServiceClient
	clients                           *registry.Registry
	householdCalculationClient        customersv1.HouseholdCalculationServiceClient
	findSessionsFn                    func(context.Context, *identityv1.FindSessionsRequest, ...grpc.CallOption) (*identityv1.FindSessionsResponse, error)
}

// NewPreferenceLifecycleServer wires a PreferenceLifecycleServer.
func NewPreferenceLifecycleServer(deps platform.Deps) *PreferenceLifecycleServer {
	sessionQuery := identityv1.NewSessionQueryServiceClient(deps.Conn)
	return &PreferenceLifecycleServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.customers.v1.PreferenceLifecycleService"),
		fuelCardClient:                    fuelv1.NewFuelCardServiceClient(deps.Conn),
		sessionQueryClient:                identityv1.NewSessionQueryServiceClient(deps.Conn),
		preferenceReportingClient:         customersv1.NewPreferenceReportingServiceClient(deps.Conn),
		conn:                              deps.Conn,
		venueClient:                       venuesv1.NewVenueServiceClient(deps.Conn),
		usageRecordApprovalClient:         subscriptionsv1.NewUsageRecordApprovalServiceClient(deps.Conn),
		usageRecordApprovalClientFallback: subscriptionsv1.NewUsageRecordApprovalServiceClient(deps.Conn),
		venueQueryClient:                  venuesv1.NewVenueQueryServiceClient(deps.Conn),
		orderWorkflowClient:               ordersv1.NewOrderWorkflowServiceClient(deps.Conn),
		sellerListingWorkflowClient:       sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn),
		clients:                           deps.Clients,
		householdCalculationClient:        customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		findSessionsFn:                    sessionQuery.FindSessions,
	}
}

// ActivatePreference activates a preference.
func (s *PreferenceLifecycleServer) ActivatePreference(ctx context.Context, req *customersv1.ActivatePreferenceRequest) (*customersv1.ActivatePreferenceResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "activate_preference"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate preference", "tenant", req.GetContext().GetTenantId())
	cloneFuelCardReply, err := s.fuelCardClient.CloneFuelCard(ctx, &fuelv1.CloneFuelCardRequest{
		Context: req.GetContext(),
		FuelCard: &fuelv1.FuelCard{
			Holder: &driversv1.DriverRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardService/CloneFuelCard")
	}
	exportSessionsStream, err := s.sessionQueryClient.ExportSessions(ctx, &identityv1.ExportSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ExportSessionsRequest_Filter{
			Labels:       cloneFuelCardReply.GetFuelCard().GetLabels(),
			CreatedAfter: cloneFuelCardReply.GetFuelCard().GetOpenedAt(),
			OwnerId:      cloneFuelCardReply.GetFuelCard().GetHolder().GetDriverId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
	}
	for {
		received, err := exportSessionsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
		}
		_ = received
	}
	var holder struct{ Phone string }
	if record := req.GetDetails().GetNotifyContact(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Phone"); err != nil {
			s.log.WarnContext(ctx, "copy phone", "error", err)
		}
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sharePreferenceReportRequest := &customersv1.SharePreferenceReportRequest{
		Context: req.GetContext(),
		Details: &customersv1.SharePreferenceReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            cloneFuelCardReply.GetFuelCard().GetHolder().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Phone:            holder.Phone,
				Name:             cloneFuelCardReply.GetFuelCard().GetContact().GetName(),
			},
			Payload: packed,
		},
	}
	sharePreferenceReportReply, err := grpcx.Call(ctx, s.preferenceReportingClient.SharePreferenceReport, sharePreferenceReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceReportingService/SharePreferenceReport")
	}
	return &customersv1.ActivatePreferenceResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: cloneFuelCardReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: cloneFuelCardReply.GetFuelCard().GetHolder().GetDisplayName(),
			},
			Name:         sharePreferenceReportReply.GetPreference().GetName(),
			Effective:    sharePreferenceReportReply.GetPreference().GetEffective(),
			PreferenceId: req.GetPreferenceId(),
		},
		Outcome: &customersv1.ActivatePreferenceResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendPreference suspends a preference.
func (s *PreferenceLifecycleServer) SuspendPreference(ctx context.Context, req *customersv1.SuspendPreferenceRequest) (*customersv1.SuspendPreferenceResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "suspend_preference"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend preference", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveConsentRecordConflictRequest := &customersv1.ResolveConsentRecordConflictRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details:     &customersv1.ResolveConsentRecordConflictRequest_Details{Payload: snapshot},
	}
	resolveConsentRecordConflictReply := new(customersv1.ResolveConsentRecordConflictResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.ConsentRecordSyncService_ResolveConsentRecordConflict_FullMethodName, resolveConsentRecordConflictRequest, resolveConsentRecordConflictReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/ResolveConsentRecordConflict")
	}
	lintApiCredentialsRequest := &identityv1.LintApiCredentialsRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNotifyContact().GetEmail(),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
			},
			Holder: &identityv1.SessionRef{
				DisplayName: resolveConsentRecordConflictReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
		},
		Details: &identityv1.LintApiCredentialsRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	lintApiCredentialsReply, err := lintApiCredentialsCall(ctx, lintApiCredentialsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/LintApiCredentials")
	}
	return &customersv1.SuspendPreferenceResponse{
		Result: &commonv1.OperationResult{
			Details: resolveConsentRecordConflictReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email: lintApiCredentialsReply.GetApiCredential().GetContact().GetEmail(),
			},
			PreferenceId: req.GetPreferenceId(),
		},
		Outcome: &customersv1.SuspendPreferenceResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstatePreference reinstates a preference.
func (s *PreferenceLifecycleServer) ReinstatePreference(ctx context.Context, req *customersv1.ReinstatePreferenceRequest) (*customersv1.ReinstatePreferenceResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "reinstate_preference"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate preference", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotifyContact().GetPhone())
	createFuelCardReply, err := s.fuelCardClient.CreateFuelCard(ctx, &fuelv1.CreateFuelCardRequest{
		Context: req.GetContext(),
		FuelCard: &fuelv1.FuelCard{
			Holder:  &driversv1.DriverRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			Contact: &commonv1.ContactInfo{Phone: tenancy.Value(ctx, "phone")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardService/CreateFuelCard")
	}
	source := req.GetDetails().GetNewOwner().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	searchVenuesRequest := &venuesv1.SearchVenuesRequest{
		Context: req.GetContext(),
		Filter: &venuesv1.SearchVenuesRequest_Filter{
			Labels:       createFuelCardReply.GetFuelCard().GetLabels(),
			CreatedAfter: createFuelCardReply.GetFuelCard().GetAudit().GetCreatedAt(),
			OwnerId:      userId,
		},
	}
	searchVenuesReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*venuesv1.SearchVenuesResponse, error) {
		return s.venueClient.SearchVenues(ctx, searchVenuesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/SearchVenues")
	}
	return &customersv1.ReinstatePreferenceResponse{
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: createFuelCardReply.GetFuelCard().GetHolder().GetDisplayName(),
			},
			PreferenceId: req.GetPreferenceId(),
		},
		Outcome: &customersv1.ReinstatePreferenceResponse_Outcome{
			Message:     searchVenuesReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: searchVenuesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// TerminatePreference terminates a preference.
func (s *PreferenceLifecycleServer) TerminatePreference(ctx context.Context, req *customersv1.TerminatePreferenceRequest) (*customersv1.TerminatePreferenceResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "terminate_preference"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate preference", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *subscriptionsv1.RejectUsageRecordRequest, ...grpc.CallOption) (*subscriptionsv1.RejectUsageRecordResponse, error){
		"primary":  s.usageRecordApprovalClient.RejectUsageRecord,
		"fallback": s.usageRecordApprovalClientFallback.RejectUsageRecord,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	rejectUsageRecordRequest := &subscriptionsv1.RejectUsageRecordRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.RejectUsageRecordRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
			Detail:  packed,
		},
	}
	rejectUsageRecordReply, err := route(ctx, rejectUsageRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordApprovalService/RejectUsageRecord")
	}
	findVenuesRequest := &venuesv1.FindVenuesRequest{
		Context: req.GetContext(),
		Filter: &venuesv1.FindVenuesRequest_Filter{
			CreatedAfter: rejectUsageRecordReply.GetOutcome().GetEffectiveAt(),
			OwnerId:      rejectUsageRecordReply.GetUsageRecord().GetSubject().GetCustomerId(),
			Labels:       rejectUsageRecordReply.GetResult().GetDetails(),
		},
	}
	findVenuesReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*venuesv1.FindVenuesResponse, error) {
		return s.venueQueryClient.FindVenues(ctx, findVenuesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/FindVenues")
	}
	return &customersv1.TerminatePreferenceResponse{
		Result: &commonv1.OperationResult{
			Status:       rejectUsageRecordReply.GetResult().GetStatus(),
			ReferenceIds: rejectUsageRecordReply.GetResult().GetReferenceIds(),
		},
		Outcome: &customersv1.TerminatePreferenceResponse_Outcome{
			Message:     findVenuesReply.GetResult().GetMessage(),
			Status:      findVenuesReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Preference: &customersv1.Preference{PreferenceId: req.GetPreferenceId()},
	}, nil
}

// RenewPreference renews a preference.
func (s *PreferenceLifecycleServer) RenewPreference(ctx context.Context, req *customersv1.RenewPreferenceRequest) (*customersv1.RenewPreferenceResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "renew_preference"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew preference", "tenant", req.GetContext().GetTenantId())
	advanceOrderWorkflowRequest := buildAdvanceOrderWorkflowRequestForRenewPreference(req)
	advanceOrderWorkflowReply, err := s.orderWorkflowClient.AdvanceOrderWorkflow(ctx, advanceOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderWorkflowService/AdvanceOrderWorkflow")
	}
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotifyContact().GetPhone())
	retrySellerListingWorkflowRequest := &sellersv1.RetrySellerListingWorkflowRequest{
		Context: req.GetContext(),
		Details: &sellersv1.RetrySellerListingWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: tenancy.Value(ctx, "phone")},
		},
	}
	retrySellerListingWorkflowReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*sellersv1.RetrySellerListingWorkflowResponse, error) {
		return s.sellerListingWorkflowClient.RetrySellerListingWorkflow(ctx, retrySellerListingWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/RetrySellerListingWorkflow")
	}
	return &customersv1.RenewPreferenceResponse{
		Outcome: &customersv1.RenewPreferenceResponse_Outcome{
			Status:      advanceOrderWorkflowReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message: retrySellerListingWorkflowReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Preference: &customersv1.Preference{PreferenceId: req.GetPreferenceId()},
	}, nil
}

// ExpirePreference expires a preference.
func (s *PreferenceLifecycleServer) ExpirePreference(ctx context.Context, req *customersv1.ExpirePreferenceRequest) (*customersv1.ExpirePreferenceResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "expire_preference"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire preference", "tenant", req.GetContext().GetTenantId())
	fixedAssetCalculation, err := registry.Lookup[assetsv1.FixedAssetCalculationServiceClient](s.clients, "acme.assets.v1.FixedAssetCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	quoteFixedAssetRequest := &assetsv1.QuoteFixedAssetRequest{
		Context: req.GetContext(),
		FixedAsset: &assetsv1.FixedAsset{
			Owner: &identityv1.UserRef{
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	quoteFixedAssetReply, err := fixedAssetCalculation.QuoteFixedAsset(ctx, quoteFixedAssetRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.assets.v1.FixedAssetCalculationService/QuoteFixedAsset")
	}
	compareHouseholdOptionsRequest := buildCompareHouseholdOptionsRequestForExpirePreference(req, quoteFixedAssetReply)
	compareHouseholdOptionsReply, err := s.householdCalculationClient.CompareHouseholdOptions(ctx, compareHouseholdOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions")
	}
	return &customersv1.ExpirePreferenceResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: quoteFixedAssetReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Preference: &customersv1.Preference{
			Name:         compareHouseholdOptionsReply.GetHousehold().GetName(),
			PreferenceId: req.GetPreferenceId(),
		},
		Outcome: &customersv1.ExpirePreferenceResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetPreferenceLifecycle returns a preference.
func (s *PreferenceLifecycleServer) GetPreferenceLifecycle(ctx context.Context, req *customersv1.GetPreferenceLifecycleRequest) (*customersv1.GetPreferenceLifecycleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "get_preference_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get preference lifecycle", "tenant", req.GetContext().GetTenantId())
	return &customersv1.GetPreferenceLifecycleResponse{
		Preference: &customersv1.Preference{PreferenceId: req.GetPreferenceId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SchedulePreferenceExpiry schedules a preference.
func (s *PreferenceLifecycleServer) SchedulePreferenceExpiry(ctx context.Context, req *customersv1.SchedulePreferenceExpiryRequest) (*customersv1.SchedulePreferenceExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SchedulePreferenceExpiry", "error", err)
		return nil, err
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "schedule preference expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	findSessionsRequest := &identityv1.FindSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.FindSessionsRequest_Filter{
			CreatedAfter: req.GetPreference().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetPreference().GetPreferenceId(),
			Labels:       req.GetAnnotations(),
		},
	}
	findSessionsReply, err := s.findSessionsFn(ctx, findSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/FindSessions")
	}
	batchGetFuelCardsRequest := &fuelv1.BatchGetFuelCardsRequest{Context: req.GetContext()}
	batchGetFuelCardsReply := new(fuelv1.BatchGetFuelCardsResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.fuel.v1.FuelCardService/BatchGetFuelCards", batchGetFuelCardsRequest, batchGetFuelCardsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardService/BatchGetFuelCards")
	}
	recalculateCustomerSegmentsRequest := &customersv1.RecalculateCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email:       req.GetPreference().GetOwner().GetEmail(),
				DisplayName: req.GetPreference().GetOwner().GetDisplayName(),
			},
			Action: &customersv1.CustomerSegment_Route{Route: &customersv1.CustomerSegmentRouteAction{Target: req.GetPreference().GetRoute().GetTarget()}},
			Audit:  &commonv1.AuditInfo{UpdatedBy: req.GetPreference().GetAudit().GetUpdatedBy()},
		},
	}
	recalculateCustomerSegmentsReply := new(customersv1.RecalculateCustomerSegmentsResponse)
	if err := s.conn.Invoke(ctx, customersCustomerSegmentCalculationServiceRecalculateCustomerSegmentsMethod, recalculateCustomerSegmentsRequest, recalculateCustomerSegmentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments")
	}
	return &customersv1.SchedulePreferenceExpiryResponse{
		Result: &commonv1.OperationResult{
			Message:       findSessionsReply.GetResult().GetMessage(),
			CorrelationId: findSessionsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  batchGetFuelCardsReply.GetResult().GetReferenceIds(),
			Status:        recalculateCustomerSegmentsReply.GetResult().GetStatus(),
		},
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email: recalculateCustomerSegmentsReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
			PreferenceId: req.GetPreferenceId(),
			Status:       req.GetPreference().GetStatus(),
		},
	}, nil
}
