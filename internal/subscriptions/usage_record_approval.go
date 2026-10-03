package subscriptions

import (
	"context"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	messagingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/messaging/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// UsageRecordApprovalServer implements `acme.subscriptions.v1.UsageRecordApprovalService`.
// Routes usage records through approval, with escalation and delegation.
type UsageRecordApprovalServer struct {
	subscriptionsv1.UnimplementedUsageRecordApprovalServiceServer
	store                              *Store
	log                                *slog.Logger
	auditEventValidationClient         auditv1.AuditEventValidationServiceClient
	auditEventValidationClientFallback auditv1.AuditEventValidationServiceClient
	clients                            *registry.Registry
	subscriptionClient                 subscriptionsv1.SubscriptionServiceClient
	conversationCalculationClient      messagingv1.ConversationCalculationServiceClient
	consentRecordCalculationClient     customersv1.ConsentRecordCalculationServiceClient
	sellerScorecardApprovalClient      sellersv1.SellerScorecardApprovalServiceClient
	giftCardApprovalClient             giftcardsv1.GiftCardApprovalServiceClient
	householdCalculationClient         customersv1.HouseholdCalculationServiceClient
	customerSegmentCalculationClient   customersv1.CustomerSegmentCalculationServiceClient
	synonymExportClient                searchv1.SynonymExportServiceClient
}

// NewUsageRecordApprovalServer wires a UsageRecordApprovalServer.
func NewUsageRecordApprovalServer(deps platform.Deps) *UsageRecordApprovalServer {
	return &UsageRecordApprovalServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.subscriptions.v1.UsageRecordApprovalService"),
		auditEventValidationClient:         auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		auditEventValidationClientFallback: auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		clients:                            deps.Clients,
		subscriptionClient:                 subscriptionsv1.NewSubscriptionServiceClient(deps.Conn),
		conversationCalculationClient:      messagingv1.NewConversationCalculationServiceClient(deps.Conn),
		consentRecordCalculationClient:     customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		sellerScorecardApprovalClient:      sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		giftCardApprovalClient:             giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		householdCalculationClient:         customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		customerSegmentCalculationClient:   customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		synonymExportClient:                searchv1.NewSynonymExportServiceClient(deps.Conn),
	}
}

// SubmitUsageRecord submits a usage record.
func (s *UsageRecordApprovalServer) SubmitUsageRecord(ctx context.Context, req *subscriptionsv1.SubmitUsageRecordRequest) (*subscriptionsv1.SubmitUsageRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitUsageRecord", "error", err)
		return nil, err
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "submit usage record", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *auditv1.ScoreAuditEventRequest, ...grpc.CallOption) (*auditv1.ScoreAuditEventResponse, error){
		"primary":  s.auditEventValidationClient.ScoreAuditEvent,
		"fallback": s.auditEventValidationClientFallback.ScoreAuditEvent,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	scoreAuditEventRequest := &auditv1.ScoreAuditEventRequest{
		Context: req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{
			Subject: &auditv1.AuditTrailRef{
				DisplayName: req.GetUsageRecord().GetSubject().GetDisplayName(),
			},
		},
	}
	scoreAuditEventReply, err := route(ctx, scoreAuditEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/ScoreAuditEvent")
	}
	return &subscriptionsv1.SubmitUsageRecordResponse{
		UsageRecord: &subscriptionsv1.UsageRecord{
			Source:        scoreAuditEventReply.GetAuditEvent().GetSource(),
			UsageRecordId: req.GetUsageRecordId(),
			Subject:       req.GetUsageRecord().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: scoreAuditEventReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ApproveUsageRecord approves a usage record.
func (s *UsageRecordApprovalServer) ApproveUsageRecord(ctx context.Context, req *subscriptionsv1.ApproveUsageRecordRequest) (*subscriptionsv1.ApproveUsageRecordResponse, error) {
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "approve usage record", "tenant", req.GetContext().GetTenantId())
	renewalScheduleApproval, ok := s.clients.Get("acme.subscriptions.v1.RenewalScheduleApprovalService").(subscriptionsv1.RenewalScheduleApprovalServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.subscriptions.v1.RenewalScheduleApprovalService")
	}
	var holder struct{ DisplayName string }
	if record := req.GetUsageRecord().GetSubject(); record != nil {
		if err := mapper.CopyFields(&holder, record, "DisplayName"); err != nil {
			s.log.WarnContext(ctx, "copy display_name", "error", err)
		}
	}
	detailsCache := cache.New[string, *subscriptionsv1.ApproveUsageRecordRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	var envelope subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	delegateRenewalScheduleApprovalRequest := &subscriptionsv1.DelegateRenewalScheduleApprovalRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.DelegateRenewalScheduleApprovalRequest_Details{
			Approver: &identityv1.UserRef{DisplayName: holder.DisplayName},
		},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Organizer: &identityv1.UserRef{Email: cachedDetails.GetApprover().GetEmail()},
		},
		RenewalScheduleId: envelope.GetDisplayName(),
	}
	delegateRenewalScheduleApprovalReply, err := renewalScheduleApproval.DelegateRenewalScheduleApproval(ctx, delegateRenewalScheduleApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleApprovalService/DelegateRenewalScheduleApproval")
	}
	return &subscriptionsv1.ApproveUsageRecordResponse{
		Result: &commonv1.OperationResult{
			Details: delegateRenewalScheduleApprovalReply.GetResult().GetDetails(),
			Message: delegateRenewalScheduleApprovalReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		UsageRecord: &subscriptionsv1.UsageRecord{UsageRecordId: req.GetUsageRecordId()},
		Outcome:     &subscriptionsv1.ApproveUsageRecordResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RejectUsageRecord rejects a usage record.
func (s *UsageRecordApprovalServer) RejectUsageRecord(ctx context.Context, req *subscriptionsv1.RejectUsageRecordRequest) (*subscriptionsv1.RejectUsageRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectUsageRecord", "error", err)
		return nil, err
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "reject usage record", "tenant", req.GetContext().GetTenantId())
	jobs := make(chan *subscriptionsv1.UsageRecordMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.subscriptionClient.CreateSubscription(workCtx, &subscriptionsv1.CreateSubscriptionRequest{
					Context: req.GetContext(),
					Subscription: &subscriptionsv1.Subscription{
						Holder: &customersv1.CustomerRef{
							DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
						},
						Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
						Audit:   &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetUsageRecord().GetMeasurements() {
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
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionService/CreateSubscription")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := usageRecordApprovalServerQuoteConversationSteps[mode]
	quoteConversationRequest := &messagingv1.QuoteConversationRequest{Context: req.GetContext()}
	quoteConversationReply, err := run(s, ctx, quoteConversationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.messaging.v1.ConversationCalculationService/QuoteConversation")
	}
	jobs2 := make(chan *subscriptionsv1.UsageRecordMeasurement)
	failures2 := make(chan error, 4)
	workCtx2, stopWork2 := context.WithCancel(ctx)
	var wg2 sync.WaitGroup
	for range 4 {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for measurement2 := range jobs2 {
				if _, err := s.consentRecordCalculationClient.EstimateConsentRecord(workCtx2, &customersv1.EstimateConsentRecordRequest{
					Context: req.GetContext(),
					ConsentRecord: &customersv1.ConsentRecord{
						Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{
							Metric: measurement2.GetMetric(),
							Value:  measurement2.GetValue(),
							Unit:   measurement2.GetUnit(),
						}},
					},
				}); err != nil {
					failures2 <- err
					stopWork2()
					return
				}
			}
		}()
	}
	for _, queued2 := range req.GetUsageRecord().GetMeasurements() {
		if workCtx2.Err() != nil {
			break
		}
		select {
		case jobs2 <- queued2:
		case <-workCtx2.Done():
		}
	}
	close(jobs2)
	wg2.Wait()
	stopWork2()
	close(failures2)
	if err := <-failures2; err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/EstimateConsentRecord")
	}
	return &subscriptionsv1.RejectUsageRecordResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: quoteConversationReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		UsageRecord: &subscriptionsv1.UsageRecord{UsageRecordId: req.GetUsageRecordId()},
		Outcome:     &subscriptionsv1.RejectUsageRecordResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ListPendingUsageRecords lists usage records a page at a time.
func (s *UsageRecordApprovalServer) ListPendingUsageRecords(ctx context.Context, req *subscriptionsv1.ListPendingUsageRecordsRequest) (*subscriptionsv1.ListPendingUsageRecordsResponse, error) {
	s.log.InfoContext(ctx, "list pending usage records", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	commentSellerScorecardApprovalRequest := &sellersv1.CommentSellerScorecardApprovalRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Audit:          &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &sellersv1.CommentSellerScorecardApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: (&decoded).GetActor().GetEmail()},
		},
	}
	type commentSellerScorecardApprovalResult struct {
		reply *sellersv1.CommentSellerScorecardApprovalResponse
		err   error
	}
	commentSellerScorecardApprovalResults := make(chan commentSellerScorecardApprovalResult, 1)
	go func() {
		reply, err := s.sellerScorecardApprovalClient.CommentSellerScorecardApproval(ctx, commentSellerScorecardApprovalRequest)
		commentSellerScorecardApprovalResults <- commentSellerScorecardApprovalResult{reply, err}
	}()
	var commentSellerScorecardApprovalReply *sellersv1.CommentSellerScorecardApprovalResponse
	select {
	case result := <-commentSellerScorecardApprovalResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerScorecardApprovalService/CommentSellerScorecardApproval")
		}
		commentSellerScorecardApprovalReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &subscriptionsv1.ListPendingUsageRecordsResponse{
		UsageRecords: []*subscriptionsv1.UsageRecord{&subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{
				CustomerId: commentSellerScorecardApprovalReply.GetSellerScorecard().GetCounterparty().GetCustomerId(),
			},
		}},
		Result: &commonv1.OperationResult{
			Status: commentSellerScorecardApprovalReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// DelegateUsageRecordApproval delegates a usage record.
func (s *UsageRecordApprovalServer) DelegateUsageRecordApproval(ctx context.Context, req *subscriptionsv1.DelegateUsageRecordApprovalRequest) (*subscriptionsv1.DelegateUsageRecordApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "delegate usage record approval", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*giftcardsv1.ApproveGiftCardResponse, len(req.GetUsageRecord().GetMeasurements()))
	for i, measurement := range req.GetUsageRecord().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.giftCardApprovalClient.ApproveGiftCard(groupCtx, &giftcardsv1.ApproveGiftCardRequest{
				Context: req.GetContext(),
				GiftCard: &giftcardsv1.GiftCard{
					Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
					Holder: &giftcardsv1.GiftCardTransactionRef{
						DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
					},
				},
				Details: &giftcardsv1.ApproveGiftCardRequest_Details{EffectiveAt: measurement.GetObservedAt()},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/ApproveGiftCard")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &subscriptionsv1.DelegateUsageRecordApprovalResponse{
		UsageRecord: &subscriptionsv1.UsageRecord{
			UsageRecordId: req.GetUsageRecordId(),
			Kind:          req.GetUsageRecord().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallUsageRecord recalls a usage record.
func (s *UsageRecordApprovalServer) RecallUsageRecord(ctx context.Context, req *subscriptionsv1.RecallUsageRecordRequest) (*subscriptionsv1.RecallUsageRecordResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "recall usage record", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetApprover().GetDisplayName())
	encoded, err := protojson.Marshal(req.GetUsageRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded subscriptionsv1.UsageRecord
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cloned := proto.Clone(req.GetDetails()).(*subscriptionsv1.RecallUsageRecordRequest_Details)
	var envelope subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	commentSellerScorecardApprovalRequest := &sellersv1.CommentSellerScorecardApprovalRequest{
		Context: req.GetContext(),
		Details: &sellersv1.CommentSellerScorecardApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: tenancy.Value(ctx, "display_name"),
				Email:       (&decoded).GetSubject().GetEmail(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
			},
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			Counterparty: &customersv1.CustomerRef{Email: cloned.GetApprover().GetEmail()},
		},
		SellerScorecardId: envelope.GetDisplayName(),
	}
	commentSellerScorecardApprovalReply, err := commentSellerScorecardApprovalCall(ctx, commentSellerScorecardApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/CommentSellerScorecardApproval")
	}
	explainHouseholdCalculationRequest := &customersv1.ExplainHouseholdCalculationRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						Locality: commentSellerScorecardApprovalReply.GetSellerScorecard().GetBillingAddress().GetLocality(),
					},
					Audit: &commonv1.AuditInfo{
						Etag: commentSellerScorecardApprovalReply.GetSellerScorecard().GetAudit().GetEtag(),
					},
					Manager: &identityv1.UserRef{
						Email:       commentSellerScorecardApprovalReply.GetSellerScorecard().GetCounterparty().GetEmail(),
						DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
					},
				}},
			}},
		},
	}
	explainHouseholdCalculationReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.ExplainHouseholdCalculationResponse, error) {
		return s.householdCalculationClient.ExplainHouseholdCalculation(ctx, explainHouseholdCalculationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/ExplainHouseholdCalculation")
	}
	var envelope2 subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	estimateCustomerSegmentReply, err := s.customerSegmentCalculationClient.EstimateCustomerSegment(ctx, &customersv1.EstimateCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: commentSellerScorecardApprovalReply.GetSellerScorecard().GetAudit().GetUpdatedAt(),
				CreatedAt: explainHouseholdCalculationReply.GetHousehold().GetAudit().GetCreatedAt(),
			},
		},
		Details: &customersv1.EstimateCustomerSegmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: commentSellerScorecardApprovalReply.GetSellerScorecard().GetBillingAddress().GetPostalCode(),
			},
		},
		CustomerSegmentId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/EstimateCustomerSegment")
	}
	return &subscriptionsv1.RecallUsageRecordResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: commentSellerScorecardApprovalReply.GetResult().GetReferenceIds(),
			Message:      estimateCustomerSegmentReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Outcome: &subscriptionsv1.RecallUsageRecordResponse_Outcome{
			Status:      commentSellerScorecardApprovalReply.GetOutcome().GetStatus(),
			ReferenceId: explainHouseholdCalculationReply.GetOutcome().GetReferenceId(),
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			UsageRecordId: req.GetUsageRecordId(),
			Source:        req.GetUsageRecord().GetSource(),
		},
	}, nil
}

// GetUsageRecordApprovalStatus returns a usage record.
func (s *UsageRecordApprovalServer) GetUsageRecordApprovalStatus(ctx context.Context, req *subscriptionsv1.GetUsageRecordApprovalStatusRequest) (*subscriptionsv1.GetUsageRecordApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetUsageRecordApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "usage_record", req.GetUsageRecordId(), "get_usage_record_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get usage record approval status", "tenant", req.GetContext().GetTenantId())
	listSynonymExportsRequest := buildListSynonymExportsRequestForGetUsageRecordApprovalStatus(req)
	listSynonymExportsReply, err := s.synonymExportClient.ListSynonymExports(ctx, listSynonymExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ListSynonymExports")
	}
	return &subscriptionsv1.GetUsageRecordApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Message: listSynonymExportsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		UsageRecord: &subscriptionsv1.UsageRecord{UsageRecordId: req.GetUsageRecordId()},
	}, nil
}

// CommentUsageRecordApproval comments a usage record.
func (s *UsageRecordApprovalServer) CommentUsageRecordApproval(ctx context.Context, req *subscriptionsv1.CommentUsageRecordApprovalRequest) (*subscriptionsv1.CommentUsageRecordApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "comment usage record approval", "tenant", req.GetContext().GetTenantId())
	var envelope subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recalculateConsentRecordsReply, err := s.consentRecordCalculationClient.RecalculateConsentRecords(ctx, &customersv1.RecalculateConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Kind: req.GetUsageRecord().GetKind(),
		},
		ConsentRecordId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords")
	}
	return &subscriptionsv1.CommentUsageRecordApprovalResponse{
		UsageRecord: &subscriptionsv1.UsageRecord{
			Kind:          recalculateConsentRecordsReply.GetConsentRecord().GetKind(),
			UsageRecordId: req.GetUsageRecordId(),
			Attributes:    req.GetUsageRecord().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// usageRecordApprovalServerQuoteConversationSteps are the ways to run
// acme.messaging.v1.ConversationCalculationService/QuoteConversation, by name.
var usageRecordApprovalServerQuoteConversationSteps = map[string]func(*UsageRecordApprovalServer, context.Context, *messagingv1.QuoteConversationRequest) (*messagingv1.QuoteConversationResponse, error){
	"default": (*UsageRecordApprovalServer).quoteConversationStep,
	"audited": (*UsageRecordApprovalServer).quoteConversationAuditedStep,
}

func (s *UsageRecordApprovalServer) quoteConversationStep(ctx context.Context, request *messagingv1.QuoteConversationRequest) (*messagingv1.QuoteConversationResponse, error) {
	return s.conversationCalculationClient.QuoteConversation(ctx, request)
}

func (s *UsageRecordApprovalServer) quoteConversationAuditedStep(ctx context.Context, request *messagingv1.QuoteConversationRequest) (*messagingv1.QuoteConversationResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.messaging.v1.ConversationCalculationService/QuoteConversation")
	return s.conversationCalculationClient.QuoteConversation(ctx, request)
}
