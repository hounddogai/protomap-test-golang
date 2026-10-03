package shipping

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	boxofficev1 "github.com/hounddogai/protomap-test-golang/gen/acme/boxoffice/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	prescriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/prescriptions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TrackingEventLifecycleServer implements `acme.shipping.v1.TrackingEventLifecycleService`.
// Moves a tracking event between the states of its lifecycle.
type TrackingEventLifecycleServer struct {
	shippingv1.UnimplementedTrackingEventLifecycleServiceServer
	store                              *Store
	log                                *slog.Logger
	userValidationClient               identityv1.UserValidationServiceClient
	conn                               *grpc.ClientConn
	subscriptionPlanApprovalClient     subscriptionsv1.SubscriptionPlanApprovalServiceClient
	ledgerAccountQueryClient           ledgerv1.LedgerAccountQueryServiceClient
	trackingEventValidationClient      shippingv1.TrackingEventValidationServiceClient
	sellerPayoutScheduleApprovalClient sellersv1.SellerPayoutScheduleApprovalServiceClient
	customerClient                     customersv1.CustomerServiceClient
	consentRecordCalculationClient     customersv1.ConsentRecordCalculationServiceClient
	bus                                *eventbus.Bus
	synonymHistoryClient               searchv1.SynonymHistoryServiceClient
	carrierRateValidationClient        shippingv1.CarrierRateValidationServiceClient
	giftCardApprovalClient             giftcardsv1.GiftCardApprovalServiceClient
	shipmentClient                     shippingv1.ShipmentServiceClient
	trackingEventReportingClient       shippingv1.TrackingEventReportingServiceClient
	prescriptionQueryClient            prescriptionsv1.PrescriptionQueryServiceClient
}

// NewTrackingEventLifecycleServer wires a TrackingEventLifecycleServer.
func NewTrackingEventLifecycleServer(deps platform.Deps) TrackingEventLifecycleServer {
	return TrackingEventLifecycleServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.shipping.v1.TrackingEventLifecycleService"),
		userValidationClient:               identityv1.NewUserValidationServiceClient(deps.Conn),
		conn:                               deps.Conn,
		subscriptionPlanApprovalClient:     subscriptionsv1.NewSubscriptionPlanApprovalServiceClient(deps.Conn),
		ledgerAccountQueryClient:           ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		trackingEventValidationClient:      shippingv1.NewTrackingEventValidationServiceClient(deps.Conn),
		sellerPayoutScheduleApprovalClient: sellersv1.NewSellerPayoutScheduleApprovalServiceClient(deps.Conn),
		customerClient:                     customersv1.NewCustomerServiceClient(deps.Conn),
		consentRecordCalculationClient:     customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		bus:                                deps.Bus,
		synonymHistoryClient:               searchv1.NewSynonymHistoryServiceClient(deps.Conn),
		carrierRateValidationClient:        shippingv1.NewCarrierRateValidationServiceClient(deps.Conn),
		giftCardApprovalClient:             giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		shipmentClient:                     shippingv1.NewShipmentServiceClient(deps.Conn),
		trackingEventReportingClient:       shippingv1.NewTrackingEventReportingServiceClient(deps.Conn),
		prescriptionQueryClient:            prescriptionsv1.NewPrescriptionQueryServiceClient(deps.Conn),
	}
}

// ActivateTrackingEvent activates a tracking event.
func (s TrackingEventLifecycleServer) ActivateTrackingEvent(ctx context.Context, req *shippingv1.ActivateTrackingEventRequest) (*shippingv1.ActivateTrackingEventResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "activate_tracking_event"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate tracking event", "tenant", req.GetContext().GetTenantId())
	checkUserEligibilityRequest := &identityv1.CheckUserEligibilityRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
		},
	}
	checkUserEligibilityReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*identityv1.CheckUserEligibilityResponse, error) {
		return s.userValidationClient.CheckUserEligibility(ctx, checkUserEligibilityRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/CheckUserEligibility")
	}
	batchGetCustomersRequest := &customersv1.BatchGetCustomersRequest{Context: req.GetContext()}
	batchGetCustomersReply := new(customersv1.BatchGetCustomersResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersCustomerServiceName, "BatchGetCustomers"), batchGetCustomersRequest, batchGetCustomersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchGetCustomers")
	}
	cloned := proto.Clone(req.GetDetails()).(*shippingv1.ActivateTrackingEventRequest_Details)
	advanceCollectionWorkflowRequest := &merchandisingv1.AdvanceCollectionWorkflowRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Location: &commonv1.PostalAddress{
						Recipient: checkUserEligibilityReply.GetUser().GetPrimaryAddress().GetRecipient(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedAt: checkUserEligibilityReply.GetUser().GetAudit().GetCreatedAt(),
					},
					Manager: &identityv1.UserRef{
						UserId: checkUserEligibilityReply.GetUser().GetRelationshipManager().GetUserId(),
						Email:  cloned.GetNewOwner().GetEmail(),
					},
				}},
			}},
		},
	}
	advanceCollectionWorkflowReply := new(merchandisingv1.AdvanceCollectionWorkflowResponse)
	if err := s.conn.Invoke(ctx, merchandisingCollectionWorkflowServiceAdvanceCollectionWorkflowMethod, advanceCollectionWorkflowRequest, advanceCollectionWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/AdvanceCollectionWorkflow")
	}
	return &shippingv1.ActivateTrackingEventResponse{
		TrackingEvent: &shippingv1.TrackingEvent{
			CorrelationId:   checkUserEligibilityReply.GetResult().GetCorrelationId(),
			TrackingEventId: req.GetTrackingEventId(),
		},
		Result: &commonv1.OperationResult{Status: batchGetCustomersReply.GetResult().GetStatus()},
		Outcome: &shippingv1.ActivateTrackingEventResponse_Outcome{
			Status:      advanceCollectionWorkflowReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// SuspendTrackingEvent suspends a tracking event.
func (s TrackingEventLifecycleServer) SuspendTrackingEvent(ctx context.Context, req *shippingv1.SuspendTrackingEventRequest) (*shippingv1.SuspendTrackingEventResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "suspend_tracking_event"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend tracking event", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotifyContact().GetEmail())
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetNewOwner().GetUserId())
	recallSubscriptionPlanRequest := &subscriptionsv1.RecallSubscriptionPlanRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				Email:       tenancy.Value(ctx, "email"),
				UserId:      tenancy.Value(ctx, "user_id"),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	recallSubscriptionPlanReply, err := grpcx.Call(ctx, s.subscriptionPlanApprovalClient.RecallSubscriptionPlan, recallSubscriptionPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanApprovalService/RecallSubscriptionPlan")
	}
	resolveLedgerAccountsReply, err := s.ledgerAccountQueryClient.ResolveLedgerAccounts(ctx, &ledgerv1.ResolveLedgerAccountsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/ResolveLedgerAccounts")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetNewOwner().GetDisplayName())
	explainTrackingEventDecisionReply, err := s.trackingEventValidationClient.ExplainTrackingEventDecision(ctx, &shippingv1.ExplainTrackingEventDecisionRequest{
		Context: req.GetContext(),
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
		Details: &shippingv1.ExplainTrackingEventDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventValidationService/ExplainTrackingEventDecision")
	}
	approveSellerPayoutScheduleRequest := buildApproveSellerPayoutScheduleRequestForSuspendTrackingEvent(req, explainTrackingEventDecisionReply)
	approveSellerPayoutScheduleReply, err := s.sellerPayoutScheduleApprovalClient.ApproveSellerPayoutSchedule(ctx, approveSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleApprovalService/ApproveSellerPayoutSchedule")
	}
	return &shippingv1.SuspendTrackingEventResponse{
		Outcome: &shippingv1.SuspendTrackingEventResponse_Outcome{
			NextSteps:   recallSubscriptionPlanReply.GetOutcome().GetNextSteps(),
			Status:      approveSellerPayoutScheduleReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message: resolveLedgerAccountsReply.GetResult().GetMessage(),
			Status:  resolveLedgerAccountsReply.GetResult().GetStatus(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			Kind: explainTrackingEventDecisionReply.GetTrackingEvent().GetKind(),
			Subject: &shippingv1.ShipmentRef{
				DisplayName: approveSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
			},
			TrackingEventId: req.GetTrackingEventId(),
		},
	}, nil
}

// ReinstateTrackingEvent reinstates a tracking event.
func (s TrackingEventLifecycleServer) ReinstateTrackingEvent(ctx context.Context, req *shippingv1.ReinstateTrackingEventRequest) (*shippingv1.ReinstateTrackingEventResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "reinstate_tracking_event"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate tracking event", "tenant", req.GetContext().GetTenantId())
	cloneCustomerRequest := &customersv1.CloneCustomerRequest{}
	cloneCustomerRequest.Context = req.GetContext()
	cloneCustomerRequest.Customer = &customersv1.Customer{
		RelationshipManager: &identityv1.UserRef{
			Email:  req.GetDetails().GetNotifyContact().GetEmail(),
			UserId: req.GetDetails().GetNewOwner().GetUserId(),
		},
		Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
	}
	cloneCustomerReply, err := s.customerClient.CloneCustomer(ctx, cloneCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CloneCustomer")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	searchBoxOfficeSalesRequest := &boxofficev1.SearchBoxOfficeSalesRequest{
		Context: req.GetContext(),
		Filter: &boxofficev1.SearchBoxOfficeSalesRequest_Filter{
			Region:       cloneCustomerReply.GetCustomer().GetPrimaryAddress().GetRegion(),
			CreatedAfter: cloneCustomerReply.GetCustomer().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			Labels:       annotations,
		},
	}
	searchBoxOfficeSalesReply, err := searchBoxOfficeSalesCall(ctx, searchBoxOfficeSalesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.boxoffice.v1.BoxOfficeSaleService/SearchBoxOfficeSales")
	}
	quoteConsentRecordRequest := newQuoteConsentRecordRequest(withQuoteConsentRecordContext(req.GetContext()), withQuoteConsentRecordDetails(&customersv1.QuoteConsentRecordRequest_Details{
		Destination: &commonv1.PostalAddress{
			PostalCode: cloneCustomerReply.GetCustomer().GetPrimaryAddress().GetPostalCode(),
			Region:     cloneCustomerReply.GetCustomer().GetPrimaryAddress().GetRegion(),
		},
	}), withQuoteConsentRecordConsentRecord(&customersv1.ConsentRecord{
		Subject: &customersv1.HouseholdRef{
			DisplayName: cloneCustomerReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
		},
	}))
	quoteConsentRecordReply, err := s.consentRecordCalculationClient.QuoteConsentRecord(ctx, quoteConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord")
	}
	return &shippingv1.ReinstateTrackingEventResponse{
		TrackingEvent: &shippingv1.TrackingEvent{
			CorrelationId:   cloneCustomerReply.GetResult().GetCorrelationId(),
			Kind:            quoteConsentRecordReply.GetConsentRecord().GetKind(),
			TrackingEventId: req.GetTrackingEventId(),
		},
		Result: &commonv1.OperationResult{
			Details:      searchBoxOfficeSalesReply.GetResult().GetDetails(),
			ReferenceIds: quoteConsentRecordReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &shippingv1.ReinstateTrackingEventResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// TerminateTrackingEvent terminates a tracking event.
func (s TrackingEventLifecycleServer) TerminateTrackingEvent(ctx context.Context, req *shippingv1.TerminateTrackingEventRequest) (*shippingv1.TerminateTrackingEventResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "terminate_tracking_event"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate tracking event", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "shipping.terminate_tracking_event.retry_collection_workflow",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"collection.children.children.manager.display_name": req.GetDetails().GetNewOwner().GetDisplayName(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	cloned := proto.Clone(req.GetDetails()).(*shippingv1.TerminateTrackingEventRequest_Details)
	validatePreferenceRequest := &customersv1.ValidatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{DisplayName: cloned.GetNewOwner().GetDisplayName()},
		},
	}
	validatePreferenceReply, err := grpcx.NewClient(s.conn, customersv1.NewPreferenceValidationServiceClient).ValidatePreference(ctx, validatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ValidatePreference")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateSynonymVersionRequest := newAnnotateSynonymVersionRequest(withAnnotateSynonymVersionContext(req.GetContext()), withAnnotateSynonymVersionSynonym(&searchv1.Synonym{
		Action: &searchv1.Synonym_Route{Route: &searchv1.SynonymRouteAction{
			Weight: validatePreferenceReply.GetPreference().GetRoute().GetWeight(),
		}},
	}), withAnnotateSynonymVersionDetails(&searchv1.AnnotateSynonymVersionRequest_Details{Snapshot: snapshot}))
	annotateSynonymVersionReply, err := s.synonymHistoryClient.AnnotateSynonymVersion(ctx, annotateSynonymVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/AnnotateSynonymVersion")
	}
	return &shippingv1.TerminateTrackingEventResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: validatePreferenceReply.GetResult().GetCorrelationId(),
			Status:        annotateSynonymVersionReply.GetResult().GetStatus(),
		},
		Outcome: &shippingv1.TerminateTrackingEventResponse_Outcome{
			Message:     validatePreferenceReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			CorrelationId:   annotateSynonymVersionReply.GetResult().GetCorrelationId(),
			TrackingEventId: req.GetTrackingEventId(),
		},
	}, nil
}

// RenewTrackingEvent renews a tracking event.
func (s TrackingEventLifecycleServer) RenewTrackingEvent(ctx context.Context, req *shippingv1.RenewTrackingEventRequest) (*shippingv1.RenewTrackingEventResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewTrackingEvent", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "renew_tracking_event"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew tracking event", "tenant", req.GetContext().GetTenantId())
	var holder struct{ DisplayName string }
	if record := req.GetDetails().GetNewOwner(); record != nil {
		if err := mapper.CopyFields(&holder, record, "DisplayName"); err != nil {
			s.log.WarnContext(ctx, "copy display_name", "error", err)
		}
	}
	scoreCarrierRateRequest := &shippingv1.ScoreCarrierRateRequest{
		Context: req.GetContext(),
		CarrierRate: &shippingv1.CarrierRate{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
				DisplayName: holder.DisplayName,
			},
			Action: &shippingv1.CarrierRate_Block{Block: &shippingv1.CarrierRateBlockAction{Reason: req.GetReason()}},
		},
	}
	type scoreCarrierRateResult struct {
		reply *shippingv1.ScoreCarrierRateResponse
		err   error
	}
	scoreCarrierRateResults := make(chan scoreCarrierRateResult, 1)
	go func() {
		reply, err := s.carrierRateValidationClient.ScoreCarrierRate(ctx, scoreCarrierRateRequest)
		scoreCarrierRateResults <- scoreCarrierRateResult{reply, err}
	}()
	var scoreCarrierRateReply *shippingv1.ScoreCarrierRateResponse
	select {
	case result := <-scoreCarrierRateResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.shipping.v1.CarrierRateValidationService/ScoreCarrierRate")
		}
		scoreCarrierRateReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &shippingv1.RenewTrackingEventResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scoreCarrierRateReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		TrackingEvent: &shippingv1.TrackingEvent{TrackingEventId: req.GetTrackingEventId()},
		Outcome:       &shippingv1.RenewTrackingEventResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ScheduleTrackingEventExpiry schedules a tracking event.
func (s TrackingEventLifecycleServer) ScheduleTrackingEventExpiry(ctx context.Context, req *shippingv1.ScheduleTrackingEventExpiryRequest) (*shippingv1.ScheduleTrackingEventExpiryResponse, error) {
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "schedule tracking event expiry", "tenant", req.GetContext().GetTenantId())
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	rejectGiftCardRequest := &giftcardsv1.RejectGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
		GiftCardId: envelope.GetDisplayName(),
	}
	type rejectGiftCardResult struct {
		reply *giftcardsv1.RejectGiftCardResponse
		err   error
	}
	rejectGiftCardResults := make(chan rejectGiftCardResult, 1)
	go func() {
		reply, err := s.giftCardApprovalClient.RejectGiftCard(ctx, rejectGiftCardRequest)
		rejectGiftCardResults <- rejectGiftCardResult{reply, err}
	}()
	var rejectGiftCardReply *giftcardsv1.RejectGiftCardResponse
	select {
	case result := <-rejectGiftCardResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.giftcards.v1.GiftCardApprovalService/RejectGiftCard")
		}
		rejectGiftCardReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	jobs := make(chan *shippingv1.TrackingEventMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.shipmentClient.SearchShipments(workCtx, &shippingv1.SearchShipmentsRequest{
					Context: req.GetContext(),
					Filter: &shippingv1.SearchShipmentsRequest_Filter{
						Labels:       rejectGiftCardReply.GetGiftCard().GetLabels(),
						CreatedAfter: measurement.GetObservedAt(),
						OwnerId:      req.GetTrackingEvent().GetTrackingEventId(),
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetTrackingEvent().GetMeasurements() {
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
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentService/SearchShipments")
	}
	var envelope2 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	countTrackingEventTotalsRequest := &shippingv1.CountTrackingEventTotalsRequest{}
	countTrackingEventTotalsRequest.Context = req.GetContext()
	countTrackingEventTotalsRequest.Filter = &shippingv1.CountTrackingEventTotalsRequest_Filter{Labels: rejectGiftCardReply.GetGiftCard().GetLabels()}
	countTrackingEventTotalsRequest.Page = &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()}
	countTrackingEventTotalsReply, err := s.trackingEventReportingClient.CountTrackingEventTotals(ctx, countTrackingEventTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventReportingService/CountTrackingEventTotals")
	}
	resolvePrescriptionsRequest := &prescriptionsv1.ResolvePrescriptionsRequest{Context: req.GetContext()}
	resolvePrescriptionsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*prescriptionsv1.ResolvePrescriptionsResponse, error) {
		return s.prescriptionQueryClient.ResolvePrescriptions(ctx, resolvePrescriptionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.prescriptions.v1.PrescriptionQueryService/ResolvePrescriptions")
	}
	_ = resolvePrescriptionsReply
	return &shippingv1.ScheduleTrackingEventExpiryResponse{
		Outcome: &shippingv1.ScheduleTrackingEventExpiryResponse_Outcome{Status: rejectGiftCardReply.GetOutcome().GetStatus()},
		Result: &commonv1.OperationResult{
			ReferenceIds: countTrackingEventTotalsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			TrackingEventId: req.GetTrackingEventId(),
			OccurredAt:      req.GetTrackingEvent().GetOccurredAt(),
		},
	}, nil
}
