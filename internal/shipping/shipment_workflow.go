package shipping

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	assetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/assets/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// ShipmentWorkflowServer implements `acme.shipping.v1.ShipmentWorkflowService`.
// Runs the long-lived workflow that takes a shipment from start to finish.
type ShipmentWorkflowServer struct {
	shippingv1.UnimplementedShipmentWorkflowServiceServer
	store                          *Store
	log                            *slog.Logger
	conn                           *grpc.ClientConn
	customerQueryClient            customersv1.CustomerQueryServiceClient
	trackingEventLifecycleClient   shippingv1.TrackingEventLifecycleServiceClient
	trackingEventReportingClient   shippingv1.TrackingEventReportingServiceClient
	searchIndexQueryClient         searchv1.SearchIndexQueryServiceClient
	renewalScheduleApprovalClient  subscriptionsv1.RenewalScheduleApprovalServiceClient
	meteredLedgerAccount           meteredLedgerAccount
	clients                        *registry.Registry
	reviewVoteApprovalPort         reviewVoteApprovalPort
	manifestReportingPort          manifestReportingPort
	householdQueryClient           customersv1.HouseholdQueryServiceClient
	parcelApprovalClient           shippingv1.ParcelApprovalServiceClient
	subscriptionPlanApprovalClient subscriptionsv1.SubscriptionPlanApprovalServiceClient
	shipmentApprovalClient         shippingv1.ShipmentApprovalServiceClient
	checkAccessPolicyEligibilityFn func(context.Context, *authorizationv1.CheckAccessPolicyEligibilityRequest, ...grpc.CallOption) (*authorizationv1.CheckAccessPolicyEligibilityResponse, error)
}

// NewShipmentWorkflowServer wires a ShipmentWorkflowServer.
func NewShipmentWorkflowServer(deps platform.Deps) *ShipmentWorkflowServer {
	accessPolicyValidation := authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn)
	return &ShipmentWorkflowServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.shipping.v1.ShipmentWorkflowService"),
		conn:                           deps.Conn,
		customerQueryClient:            customersv1.NewCustomerQueryServiceClient(deps.Conn),
		trackingEventLifecycleClient:   shippingv1.NewTrackingEventLifecycleServiceClient(deps.Conn),
		trackingEventReportingClient:   shippingv1.NewTrackingEventReportingServiceClient(deps.Conn),
		searchIndexQueryClient:         searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
		renewalScheduleApprovalClient:  subscriptionsv1.NewRenewalScheduleApprovalServiceClient(deps.Conn),
		meteredLedgerAccount:           meteredLedgerAccount{LedgerAccountServiceClient: ledgerv1.NewLedgerAccountServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                        deps.Clients,
		reviewVoteApprovalPort:         newReviewVoteApprovalPort(deps),
		manifestReportingPort:          newManifestReportingPort(deps),
		householdQueryClient:           customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		parcelApprovalClient:           shippingv1.NewParcelApprovalServiceClient(deps.Conn),
		subscriptionPlanApprovalClient: subscriptionsv1.NewSubscriptionPlanApprovalServiceClient(deps.Conn),
		shipmentApprovalClient:         shippingv1.NewShipmentApprovalServiceClient(deps.Conn),
		checkAccessPolicyEligibilityFn: accessPolicyValidation.CheckAccessPolicyEligibility,
	}
}

// StartShipmentWorkflow starts a shipment.
func (s *ShipmentWorkflowServer) StartShipmentWorkflow(ctx context.Context, req *shippingv1.StartShipmentWorkflowRequest) (*shippingv1.StartShipmentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "start shipment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	var holder struct{ UpdatedBy string }
	if record := req.GetShipment().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "UpdatedBy"); err != nil {
			s.log.WarnContext(ctx, "copy updated_by", "error", err)
		}
	}
	submitSubscriptionPlanRequest := &subscriptionsv1.SubmitSubscriptionPlanRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: holder.UpdatedBy,
				UpdatedAt: req.GetShipment().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetShipment().GetAudit().GetCreatedBy(),
			},
		},
		Details: &subscriptionsv1.SubmitSubscriptionPlanRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
	submitSubscriptionPlanReply := new(subscriptionsv1.SubmitSubscriptionPlanResponse)
	if err := grpcx.Invoke(ctx, s.conn, subscriptionsv1.SubscriptionPlanApprovalService_SubmitSubscriptionPlan_FullMethodName, submitSubscriptionPlanRequest, submitSubscriptionPlanReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanApprovalService/SubmitSubscriptionPlan")
	}
	jobs := make(chan *shippingv1.ShipmentLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.customerQueryClient.SummarizeCustomers(workCtx, &customersv1.SummarizeCustomersRequest{
					Context: req.GetContext(),
					Reason:  submitSubscriptionPlanReply.GetSubscriptionPlan().GetBlock().GetReason(),
					Details: &customersv1.SummarizeCustomersRequest_Details{Filters: line.GetAttributes()},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetShipment().GetLines() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SummarizeCustomers")
	}
	return &shippingv1.StartShipmentWorkflowResponse{
		Outcome: &shippingv1.StartShipmentWorkflowResponse_Outcome{
			Message: submitSubscriptionPlanReply.GetOutcome().GetMessage(),
			Status:  submitSubscriptionPlanReply.GetOutcome().GetStatus(),
		},
		Shipment: &shippingv1.Shipment{
			ShipmentId: req.GetShipmentId(),
			DueAt:      req.GetShipment().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvanceShipmentWorkflow advances a shipment.
func (s *ShipmentWorkflowServer) AdvanceShipmentWorkflow(ctx context.Context, req *shippingv1.AdvanceShipmentWorkflowRequest) (*shippingv1.AdvanceShipmentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "advance shipment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*shippingv1.ScheduleTrackingEventExpiryResponse, len(req.GetShipment().GetLines()))
	for i, line := range req.GetShipment().GetLines() {
		group.Go(func() error {
			reply, err := s.trackingEventLifecycleClient.ScheduleTrackingEventExpiry(groupCtx, &shippingv1.ScheduleTrackingEventExpiryRequest{
				Context:       req.GetContext(),
				TrackingEvent: &shippingv1.TrackingEvent{Attributes: line.GetAttributes()},
				Details: &shippingv1.ScheduleTrackingEventExpiryRequest_Details{
					Notify: &commonv1.ContactInfo{
						PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
						Name:             req.GetDetails().GetNotify().GetName(),
					},
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventLifecycleService/ScheduleTrackingEventExpiry")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	shareTrackingEventReportRequest := &shippingv1.ShareTrackingEventReportRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ShareTrackingEventReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetNotify().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Name:             req.GetDetails().GetNotify().GetName(),
			},
		},
	}
	shareTrackingEventReportReply, err := grpcx.Call(ctx, s.trackingEventReportingClient.ShareTrackingEventReport, shareTrackingEventReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventReportingService/ShareTrackingEventReport")
	}
	return &shippingv1.AdvanceShipmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: shareTrackingEventReportReply.GetResult().GetStatus(),
		},
		Outcome: &shippingv1.AdvanceShipmentWorkflowResponse_Outcome{
			Message: shareTrackingEventReportReply.GetOutcome().GetMessage(),
		},
		Shipment: &shippingv1.Shipment{
			ShipmentId:   req.GetShipmentId(),
			Counterparty: req.GetShipment().GetCounterparty(),
		},
	}, nil
}

// CancelShipmentWorkflow cancels a shipment.
func (s *ShipmentWorkflowServer) CancelShipmentWorkflow(ctx context.Context, req *shippingv1.CancelShipmentWorkflowRequest) (*shippingv1.CancelShipmentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "cancel shipment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	var holder struct{ ParcelId string }
	if record := req.GetShipment().GetCounterparty(); record != nil {
		if err := mapper.CopyFields(&holder, record, "ParcelId"); err != nil {
			s.log.WarnContext(ctx, "copy parcel_id", "error", err)
		}
	}
	exportSearchIndexesStream, err := s.searchIndexQueryClient.ExportSearchIndexes(ctx, &searchv1.ExportSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ExportSearchIndexesRequest_Filter{
			Region:       req.GetShipment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetShipment().GetAudit().GetUpdatedAt(),
			OwnerId:      holder.ParcelId,
			Labels:       req.GetDetails().GetParameters(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/ExportSearchIndexes")
	}
	for {
		received, err := exportSearchIndexesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/ExportSearchIndexes")
		}
		_ = received
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotify().GetEmail())
	detailsCache := cache.New[string, *shippingv1.CancelShipmentWorkflowRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	delegateRenewalScheduleApprovalRequest := &subscriptionsv1.DelegateRenewalScheduleApprovalRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email:            tenancy.Value(ctx, "email"),
					PreferredChannel: cachedDetails.GetNotify().GetPreferredChannel(),
				},
			}},
		},
	}
	delegateRenewalScheduleApprovalReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*subscriptionsv1.DelegateRenewalScheduleApprovalResponse, error) {
		return s.renewalScheduleApprovalClient.DelegateRenewalScheduleApproval(ctx, delegateRenewalScheduleApprovalRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleApprovalService/DelegateRenewalScheduleApproval")
	}
	submitRenewalScheduleRequest := buildSubmitRenewalScheduleRequestForCancelShipmentWorkflow(req, delegateRenewalScheduleApprovalReply)
	submitRenewalScheduleReply, err := s.renewalScheduleApprovalClient.SubmitRenewalSchedule(ctx, submitRenewalScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleApprovalService/SubmitRenewalSchedule")
	}
	shipmentCache := cache.New[string, *shippingv1.Shipment](1)
	shipmentCache.Put(req.GetContext().GetTraceId(), req.GetShipment())
	cachedShipment, _ := shipmentCache.Get(req.GetContext().GetTraceId())
	cloned := proto.Clone(req.GetShipment()).(*shippingv1.Shipment)
	listLedgerAccountsRequest := &ledgerv1.ListLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.ListLedgerAccountsRequest_Filter{
			Region:       cachedShipment.GetBillingAddress().GetRegion(),
			CreatedAfter: cloned.GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetShipment().GetCounterparty().GetParcelId(),
		},
	}
	listLedgerAccountsReply, err := s.meteredLedgerAccount.ListLedgerAccounts(ctx, listLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/ListLedgerAccounts")
	}
	return &shippingv1.CancelShipmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:        delegateRenewalScheduleApprovalReply.GetResult().GetStatus(),
			CorrelationId: submitRenewalScheduleReply.GetResult().GetCorrelationId(),
			ReferenceIds:  listLedgerAccountsReply.GetResult().GetReferenceIds(),
		},
		Outcome: &shippingv1.CancelShipmentWorkflowResponse_Outcome{
			NextSteps: delegateRenewalScheduleApprovalReply.GetOutcome().GetNextSteps(),
			Message:   listLedgerAccountsReply.GetResult().GetMessage(),
		},
		Shipment: &shippingv1.Shipment{
			ShipmentId:     req.GetShipmentId(),
			BillingAddress: req.GetShipment().GetBillingAddress(),
		},
	}, nil
}

// RetryShipmentWorkflow retrys a shipment.
func (s *ShipmentWorkflowServer) RetryShipmentWorkflow(ctx context.Context, req *shippingv1.RetryShipmentWorkflowRequest) (*shippingv1.RetryShipmentWorkflowResponse, error) {
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "retry shipment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	preferenceCalculation, ok := s.clients.Get("acme.customers.v1.PreferenceCalculationService").(customersv1.PreferenceCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.PreferenceCalculationService")
	}
	ctx = tenancy.With(ctx, "updated_by", req.GetShipment().GetAudit().GetUpdatedBy())
	simulatePreferenceRequest := &customersv1.SimulatePreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.SimulatePreferenceRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient:   req.GetShipment().GetBillingAddress().GetRecipient(),
				Lines:       req.GetShipment().GetBillingAddress().GetLines(),
				CountryCode: req.GetShipment().GetBillingAddress().GetCountryCode(),
			},
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{UpdatedBy: tenancy.Value(ctx, "updated_by")},
		},
	}
	simulatePreferenceReply, err := preferenceCalculation.SimulatePreference(ctx, simulatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/SimulatePreference")
	}
	detailsCache := cache.New[string, *shippingv1.RetryShipmentWorkflowRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.RetryShipmentWorkflowRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetShipment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareSynonymExportRequest := &searchv1.ShareSynonymExportRequest{
		Context: req.GetContext(),
		Details: &searchv1.ShareSynonymExportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            simulatePreferenceReply.GetPreference().GetOwner().GetEmail(),
				Phone:            req.GetDetails().GetNotify().GetPhone(),
				PreferredChannel: cachedDetails.GetNotify().GetPreferredChannel(),
				Name:             (&decoded).GetNotify().GetName(),
			},
			Payload: packed,
		},
	}
	shareSynonymExportReply := new(searchv1.ShareSynonymExportResponse)
	if err := s.conn.Invoke(ctx, searchSynonymExportServiceShareSynonymExportMethod, shareSynonymExportRequest, shareSynonymExportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ShareSynonymExport")
	}
	return &shippingv1.RetryShipmentWorkflowResponse{
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{
				Etag: simulatePreferenceReply.GetPreference().GetAudit().GetEtag(),
			},
			ShipmentId: req.GetShipmentId(),
			Status:     req.GetShipment().GetStatus(),
		},
		Outcome: &shippingv1.RetryShipmentWorkflowResponse_Outcome{Status: shareSynonymExportReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseShipmentWorkflow pauses a shipment.
func (s *ShipmentWorkflowServer) PauseShipmentWorkflow(ctx context.Context, req *shippingv1.PauseShipmentWorkflowRequest) (*shippingv1.PauseShipmentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseShipmentWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "pause shipment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "parcel_id", req.GetShipment().GetCounterparty().GetParcelId())
	browseFixedAssetsRequest := &assetsv1.BrowseFixedAssetsRequest{
		Context: req.GetContext(),
		Filter: &assetsv1.BrowseFixedAssetsRequest_Filter{
			Region:       req.GetShipment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetShipment().GetIssuedAt(),
			OwnerId:      tenancy.Value(ctx, "parcel_id"),
			Labels:       req.GetDetails().GetParameters(),
		},
	}
	browseFixedAssetsReply, err := browseFixedAssetsCall(ctx, browseFixedAssetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.assets.v1.FixedAssetQueryService/BrowseFixedAssets")
	}
	return &shippingv1.PauseShipmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: browseFixedAssetsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Shipment: &shippingv1.Shipment{
			ShipmentId:   req.GetShipmentId(),
			Counterparty: req.GetShipment().GetCounterparty(),
		},
	}, nil
}

// ResumeShipmentWorkflow resumes a shipment.
func (s *ShipmentWorkflowServer) ResumeShipmentWorkflow(ctx context.Context, req *shippingv1.ResumeShipmentWorkflowRequest) (*shippingv1.ResumeShipmentWorkflowResponse, error) {
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "resume shipment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetShipment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	commentReviewVoteApprovalRequest := &reviewsv1.CommentReviewVoteApprovalRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.CommentReviewVoteApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetShipment().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	commentReviewVoteApprovalReply, err := s.reviewVoteApprovalPort.CommentReviewVoteApproval(ctx, commentReviewVoteApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/CommentReviewVoteApproval")
	}
	source := req.GetShipment().GetCounterparty().ProtoReflect()
	parcelId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("parcel_id"))).String()
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetNotify().GetPreferredChannel())
	terminateParcelRequest := &shippingv1.TerminateParcelRequest{
		Context:  req.GetContext(),
		ParcelId: parcelId,
		Details: &shippingv1.TerminateParcelRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
				Name:             req.GetDetails().GetNotify().GetName(),
			},
			NewOwner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
	terminateParcelReply := new(shippingv1.TerminateParcelResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", shippingParcelLifecycleServiceName, "TerminateParcel"), terminateParcelRequest, terminateParcelReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelLifecycleService/TerminateParcel")
	}
	packed2, err := anypb.New(req.GetShipment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareManifestReportRequest := &shippingv1.ShareManifestReportRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ShareManifestReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetNotify().GetName(),
				Email: terminateParcelReply.GetParcel().GetOwner().GetEmail(),
			},
			Payload: packed2,
		},
	}
	shareManifestReportReply, err := s.manifestReportingPort.ShareManifestReport(ctx, shareManifestReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestReportingService/ShareManifestReport")
	}
	parameters := make(map[string]string, len(req.GetDetails().GetParameters()))
	for key, value := range req.GetDetails().GetParameters() {
		parameters[key] = value
	}
	suggestHouseholdsRequest := &customersv1.SuggestHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SuggestHouseholdsRequest_Filter{
			Region:       terminateParcelReply.GetParcel().GetAddress().GetRegion(),
			CreatedAfter: terminateParcelReply.GetParcel().GetAudit().GetCreatedAt(),
			Labels:       parameters,
			OwnerId:      shareManifestReportReply.GetManifest().GetCounterparty().GetCustomerId(),
		},
	}
	type suggestHouseholdsResult struct {
		reply *customersv1.SuggestHouseholdsResponse
		err   error
	}
	suggestHouseholdsResults := make(chan suggestHouseholdsResult, 1)
	go func() {
		reply, err := s.householdQueryClient.SuggestHouseholds(ctx, suggestHouseholdsRequest)
		suggestHouseholdsResults <- suggestHouseholdsResult{reply, err}
	}()
	var suggestHouseholdsReply *customersv1.SuggestHouseholdsResponse
	select {
	case result := <-suggestHouseholdsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.HouseholdQueryService/SuggestHouseholds")
		}
		suggestHouseholdsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &shippingv1.ResumeShipmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:        commentReviewVoteApprovalReply.GetOutcome().GetStatus(),
			CorrelationId: suggestHouseholdsReply.GetResult().GetCorrelationId(),
		},
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: terminateParcelReply.GetParcel().GetAddress().GetRecipient(),
				Locality:  shareManifestReportReply.GetManifest().GetBillingAddress().GetLocality(),
			},
			ShipmentId: req.GetShipmentId(),
			Number:     req.GetShipment().GetNumber(),
		},
	}, nil
}

// InspectShipmentWorkflow returns a shipment.
func (s *ShipmentWorkflowServer) InspectShipmentWorkflow(ctx context.Context, req *shippingv1.InspectShipmentWorkflowRequest) (*shippingv1.InspectShipmentWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipment", req.GetShipmentId(), "inspect_shipment_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect shipment workflow", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.InspectShipmentWorkflowResponse{
		Shipment: &shippingv1.Shipment{ShipmentId: req.GetShipmentId()},
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListShipmentWorkflows lists shipments a page at a time.
func (s *ShipmentWorkflowServer) ListShipmentWorkflows(ctx context.Context, req *shippingv1.ListShipmentWorkflowsRequest) (*shippingv1.ListShipmentWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list shipment workflows", "tenant", req.GetContext().GetTenantId())
	submitParcelRequest := newSubmitParcelRequest(withSubmitParcelContext(req.GetContext()), withSubmitParcelParcel(&shippingv1.Parcel{
		Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
		Audit:   &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
	}), withSubmitParcelDetails(&shippingv1.SubmitParcelRequest_Details{
		Approver: &identityv1.UserRef{
			UserId: req.GetContext().GetActor().GetUserId(),
			Email:  req.GetContext().GetActor().GetEmail(),
		},
	}))
	submitParcelReply, err := s.parcelApprovalClient.SubmitParcel(ctx, submitParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelApprovalService/SubmitParcel")
	}
	approveSubscriptionPlanRequest := &subscriptionsv1.ApproveSubscriptionPlanRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.ApproveSubscriptionPlanRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  submitParcelReply.GetParcel().GetOwner().GetEmail(),
				UserId: submitParcelReply.GetParcel().GetOwner().GetUserId(),
			},
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				CreatedBy: submitParcelReply.GetParcel().GetAudit().GetCreatedBy(),
				CreatedAt: submitParcelReply.GetParcel().GetAudit().GetCreatedAt(),
			},
		},
	}
	approveSubscriptionPlanReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*subscriptionsv1.ApproveSubscriptionPlanResponse, error) {
		return s.subscriptionPlanApprovalClient.ApproveSubscriptionPlan(ctx, approveSubscriptionPlanRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanApprovalService/ApproveSubscriptionPlan")
	}
	return &shippingv1.ListShipmentWorkflowsResponse{
		Shipments: []*shippingv1.Shipment{&shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				Locality: submitParcelReply.GetParcel().GetAddress().GetLocality(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: approveSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetEtag(),
			},
		}},
		Result: &commonv1.OperationResult{
			Status: approveSubscriptionPlanReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// SignalShipmentWorkflow signals a shipment.
func (s *ShipmentWorkflowServer) SignalShipmentWorkflow(ctx context.Context, req *shippingv1.SignalShipmentWorkflowRequest) (*shippingv1.SignalShipmentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "signal shipment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	recallShipmentRequest := buildRecallShipmentRequestForSignalShipmentWorkflow(req)
	recallShipmentReply, err := s.shipmentApprovalClient.RecallShipment(ctx, recallShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentApprovalService/RecallShipment")
	}
	checkAccessPolicyEligibilityRequest := &authorizationv1.CheckAccessPolicyEligibilityRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Audit: &commonv1.AuditInfo{
				Etag:      recallShipmentReply.GetShipment().GetAudit().GetEtag(),
				UpdatedAt: req.GetShipment().GetAudit().GetUpdatedAt(),
				CreatedAt: req.GetShipment().GetAudit().GetCreatedAt(),
			},
		},
		Details: &authorizationv1.CheckAccessPolicyEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	checkAccessPolicyEligibilityReply, err := s.checkAccessPolicyEligibilityFn(ctx, checkAccessPolicyEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/CheckAccessPolicyEligibility")
	}
	return &shippingv1.SignalShipmentWorkflowResponse{
		Shipment: &shippingv1.Shipment{
			Notes: recallShipmentReply.GetShipment().GetNotes(),
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkAccessPolicyEligibilityReply.GetAccessPolicy().GetAudit().GetCreatedBy(),
			},
			ShipmentId: req.GetShipmentId(),
			DueAt:      req.GetShipment().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
