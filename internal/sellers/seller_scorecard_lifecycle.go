package sellers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerScorecardLifecycleServer implements `acme.sellers.v1.SellerScorecardLifecycleService`.
// Moves a seller scorecard between the states of its lifecycle.
type SellerScorecardLifecycleServer struct {
	sellersv1.UnimplementedSellerScorecardLifecycleServiceServer
	store                             *Store
	log                               *slog.Logger
	preferenceValidationClient        customersv1.PreferenceValidationServiceClient
	searchQueryNotificationClient     searchv1.SearchQueryNotificationServiceClient
	userHistoryClient                 identityv1.UserHistoryServiceClient
	giftCardWorkflowClient            giftcardsv1.GiftCardWorkflowServiceClient
	softwareLicenseClient             licensingv1.SoftwareLicenseServiceClient
	metricSeriesQueryClient           metricsv1.MetricSeriesQueryServiceClient
	giftCardTransactionWorkflowClient giftcardsv1.GiftCardTransactionWorkflowServiceClient
	sellerScorecardReportingClient    sellersv1.SellerScorecardReportingServiceClient
	consentRecordValidationClient     customersv1.ConsentRecordValidationServiceClient
	conn                              *grpc.ClientConn
	searchIndexHistoryOnce            sync.Once
	searchIndexHistoryCached          searchv1.SearchIndexHistoryServiceClient
	customerClient                    customersv1.CustomerServiceClient
	userValidationClient              identityv1.UserValidationServiceClient
}

// NewSellerScorecardLifecycleServer wires a SellerScorecardLifecycleServer.
func NewSellerScorecardLifecycleServer(deps platform.Deps) *SellerScorecardLifecycleServer {
	return &SellerScorecardLifecycleServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.sellers.v1.SellerScorecardLifecycleService"),
		preferenceValidationClient:        customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		searchQueryNotificationClient:     searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		userHistoryClient:                 identityv1.NewUserHistoryServiceClient(deps.Conn),
		giftCardWorkflowClient:            giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
		softwareLicenseClient:             licensingv1.NewSoftwareLicenseServiceClient(deps.Conn),
		metricSeriesQueryClient:           metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		giftCardTransactionWorkflowClient: giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn),
		sellerScorecardReportingClient:    sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn),
		consentRecordValidationClient:     customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		conn:                              deps.Conn,
		customerClient:                    customersv1.NewCustomerServiceClient(deps.Conn),
		userValidationClient:              identityv1.NewUserValidationServiceClient(deps.Conn),
	}
}

// ActivateSellerScorecard activates a seller scorecard.
func (s *SellerScorecardLifecycleServer) ActivateSellerScorecard(ctx context.Context, req *sellersv1.ActivateSellerScorecardRequest) (*sellersv1.ActivateSellerScorecardResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "activate_seller_scorecard"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate seller scorecard", "tenant", req.GetContext().GetTenantId())
	previewPreferenceRequest := buildPreviewPreferenceRequestForActivateSellerScorecard(req)
	previewPreferenceReply, err := s.preferenceValidationClient.PreviewPreference(ctx, previewPreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	streamSearchQueryNoticesStream, err := s.searchQueryNotificationClient.StreamSearchQueryNotices(ctx, &searchv1.StreamSearchQueryNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotifyContact().GetPhone()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/StreamSearchQueryNotices")
	}
	for {
		received, err := streamSearchQueryNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/StreamSearchQueryNotices")
		}
		_ = received
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateUserVersionRequest := &identityv1.AnnotateUserVersionRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: previewPreferenceReply.GetPreference().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &identityv1.AnnotateUserVersionRequest_Details{Snapshot: snapshot},
	}
	annotateUserVersionReply, err := grpcx.Call(ctx, s.userHistoryClient.AnnotateUserVersion, annotateUserVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/AnnotateUserVersion")
	}
	advanceGiftCardWorkflowReply, err := s.giftCardWorkflowClient.AdvanceGiftCardWorkflow(ctx, &giftcardsv1.AdvanceGiftCardWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.AdvanceGiftCardWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/AdvanceGiftCardWorkflow")
	}
	return &sellersv1.ActivateSellerScorecardResponse{
		Outcome: &sellersv1.ActivateSellerScorecardResponse_Outcome{
			Message:     previewPreferenceReply.GetOutcome().GetMessage(),
			EffectiveAt: previewPreferenceReply.GetOutcome().GetEffectiveAt(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			Audit: &commonv1.AuditInfo{
				CreatedAt: annotateUserVersionReply.GetUser().GetAudit().GetCreatedAt(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
			Counterparty:      &customersv1.CustomerRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: advanceGiftCardWorkflowReply.GetResult().GetReferenceIds(),
			Message:      advanceGiftCardWorkflowReply.GetResult().GetMessage(),
			Status:       "ok",
		},
	}, nil
}

// SuspendSellerScorecard suspends a seller scorecard.
func (s *SellerScorecardLifecycleServer) SuspendSellerScorecard(ctx context.Context, req *sellersv1.SuspendSellerScorecardRequest) (*sellersv1.SuspendSellerScorecardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendSellerScorecard", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "suspend_seller_scorecard"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend seller scorecard", "tenant", req.GetContext().GetTenantId())
	createSoftwareLicenseRequest := newCreateSoftwareLicenseRequest(withCreateSoftwareLicenseContext(req.GetContext()), withCreateSoftwareLicenseSoftwareLicense(&licensingv1.SoftwareLicense{
		Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
	}))
	createSoftwareLicenseReply, err := s.softwareLicenseClient.CreateSoftwareLicense(ctx, createSoftwareLicenseRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.licensing.v1.SoftwareLicenseService/CreateSoftwareLicense")
	}
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetNewOwner().GetUserId())
	browseMetricSeriesesRequest := newBrowseMetricSeriesesRequest(withBrowseMetricSeriesesContext(req.GetContext()), withBrowseMetricSeriesesFilter(&metricsv1.BrowseMetricSeriesesRequest_Filter{
		Labels:       createSoftwareLicenseReply.GetSoftwareLicense().GetLabels(),
		CreatedAfter: req.GetDetails().GetEffectiveAt(),
		OwnerId:      tenancy.Value(ctx, "user_id"),
	}))
	browseMetricSeriesesReply, err := s.metricSeriesQueryClient.BrowseMetricSerieses(ctx, browseMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses")
	}
	return &sellersv1.SuspendSellerScorecardResponse{
		Result: &commonv1.OperationResult{
			Message: createSoftwareLicenseReply.GetResult().GetMessage(),
			Details: browseMetricSeriesesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &sellersv1.SuspendSellerScorecardResponse_Outcome{
			Status:      createSoftwareLicenseReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{SellerScorecardId: req.GetSellerScorecardId()},
	}, nil
}

// ReinstateSellerScorecard reinstates a seller scorecard.
func (s *SellerScorecardLifecycleServer) ReinstateSellerScorecard(ctx context.Context, req *sellersv1.ReinstateSellerScorecardRequest) (*sellersv1.ReinstateSellerScorecardResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "reinstate_seller_scorecard"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate seller scorecard", "tenant", req.GetContext().GetTenantId())
	resolveSoftwareLicensesRequest := &licensingv1.ResolveSoftwareLicensesRequest{Context: req.GetContext()}
	resolveSoftwareLicensesReply, err := resolveSoftwareLicensesCall(ctx, resolveSoftwareLicensesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.licensing.v1.SoftwareLicenseQueryService/ResolveSoftwareLicenses")
	}
	source := req.GetDetails().GetNewOwner().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	resumeGiftCardTransactionWorkflowReply, err := s.giftCardTransactionWorkflowClient.ResumeGiftCardTransactionWorkflow(ctx, &giftcardsv1.ResumeGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ResumeGiftCardTransactionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Email:            email,
				Name:             req.GetDetails().GetNotifyContact().GetName(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			CorrelationId: resolveSoftwareLicensesReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/ResumeGiftCardTransactionWorkflow")
	}
	return &sellersv1.ReinstateSellerScorecardResponse{
		Outcome: &sellersv1.ReinstateSellerScorecardResponse_Outcome{
			Status:      resolveSoftwareLicensesReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
				Attributes: resumeGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetAttributes(),
			}},
			SellerScorecardId: req.GetSellerScorecardId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TerminateSellerScorecard terminates a seller scorecard.
func (s *SellerScorecardLifecycleServer) TerminateSellerScorecard(ctx context.Context, req *sellersv1.TerminateSellerScorecardRequest) (*sellersv1.TerminateSellerScorecardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "terminate_seller_scorecard"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate seller scorecard", "tenant", req.GetContext().GetTenantId())
	scheduleSellerScorecardReportRequest := &sellersv1.ScheduleSellerScorecardReportRequest{}
	scheduleSellerScorecardReportRequest.Context = req.GetContext()
	scheduleSellerScorecardReportRequest.SellerScorecardId = req.GetSellerScorecardId()
	scheduleSellerScorecardReportRequest.Reason = req.GetReason()
	scheduleSellerScorecardReportReply, err := s.sellerScorecardReportingClient.ScheduleSellerScorecardReport(ctx, scheduleSellerScorecardReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/ScheduleSellerScorecardReport")
	}
	return &sellersv1.TerminateSellerScorecardResponse{
		Outcome: &sellersv1.TerminateSellerScorecardResponse_Outcome{
			Message:     scheduleSellerScorecardReportReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{SellerScorecardId: req.GetSellerScorecardId()},
		Result:          &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RenewSellerScorecard renews a seller scorecard.
func (s *SellerScorecardLifecycleServer) RenewSellerScorecard(ctx context.Context, req *sellersv1.RenewSellerScorecardRequest) (*sellersv1.RenewSellerScorecardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewSellerScorecard", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "renew_seller_scorecard"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew seller scorecard", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *sellersv1.RenewSellerScorecardRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateConsentRecordReply, err := s.consentRecordValidationClient.ValidateConsentRecord(ctx, &customersv1.ValidateConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.ValidateConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNotifyContact().GetEmail(),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
				Phone: cachedDetails.GetNotifyContact().GetPhone(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Detail: packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ValidateConsentRecord")
	}
	return &sellersv1.RenewSellerScorecardResponse{
		Result: &commonv1.OperationResult{
			Status:        validateConsentRecordReply.GetResult().GetStatus(),
			CorrelationId: validateConsentRecordReply.GetConsentRecord().GetCorrelationId(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{SellerScorecardId: req.GetSellerScorecardId()},
		Outcome:         &sellersv1.RenewSellerScorecardResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ExpireSellerScorecard expires a seller scorecard.
func (s *SellerScorecardLifecycleServer) ExpireSellerScorecard(ctx context.Context, req *sellersv1.ExpireSellerScorecardRequest) (*sellersv1.ExpireSellerScorecardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "expire_seller_scorecard"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire seller scorecard", "tenant", req.GetContext().GetTenantId())
	explainCustomerSegmentDecisionRequest := &customersv1.ExplainCustomerSegmentDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainCustomerSegmentDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotifyContact().GetPhone(),
				Email: req.GetDetails().GetNewOwner().GetEmail(),
			},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	explainCustomerSegmentDecisionReply, err := explainCustomerSegmentDecisionCall(ctx, explainCustomerSegmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ExplainCustomerSegmentDecision")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	compareSearchIndexVersionsRequest := &searchv1.CompareSearchIndexVersionsRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Owner: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
		},
		Details: &searchv1.CompareSearchIndexVersionsRequest_Details{Snapshot: snapshot},
	}
	compareSearchIndexVersionsReply, err := s.searchIndexHistoryLazy().CompareSearchIndexVersions(ctx, compareSearchIndexVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/CompareSearchIndexVersions")
	}
	return &sellersv1.ExpireSellerScorecardResponse{
		SellerScorecard: &sellersv1.SellerScorecard{
			Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
				Tax: &sellersv1.SellerScorecardLineTax{
					Amount: explainCustomerSegmentDecisionReply.GetCustomerSegment().GetDiscount().GetAmount(),
				},
			}},
			SellerScorecardId: req.GetSellerScorecardId(),
		},
		Result: &commonv1.OperationResult{
			Message: explainCustomerSegmentDecisionReply.GetResult().GetMessage(),
			Status:  compareSearchIndexVersionsReply.GetResult().GetStatus(),
		},
		Outcome: &sellersv1.ExpireSellerScorecardResponse_Outcome{
			Message:     compareSearchIndexVersionsReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// GetSellerScorecardLifecycle returns a seller scorecard.
func (s *SellerScorecardLifecycleServer) GetSellerScorecardLifecycle(ctx context.Context, req *sellersv1.GetSellerScorecardLifecycleRequest) (*sellersv1.GetSellerScorecardLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSellerScorecardLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "get_seller_scorecard_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller scorecard lifecycle", "tenant", req.GetContext().GetTenantId())
	summarizeCustomersRequest := &customersv1.SummarizeCustomersRequest{
		Context:     req.GetContext(),
		Annotations: req.GetContext().GetBaggage(),
	}
	summarizeCustomersReply, err := summarizeCustomersCall(ctx, summarizeCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SummarizeCustomers")
	}
	return &sellersv1.GetSellerScorecardLifecycleResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeCustomersReply.GetResult().GetCorrelationId(),
			Status:        summarizeCustomersReply.GetResult().GetStatus(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{SellerScorecardId: req.GetSellerScorecardId()},
	}, nil
}

// ScheduleSellerScorecardExpiry schedules a seller scorecard.
func (s *SellerScorecardLifecycleServer) ScheduleSellerScorecardExpiry(ctx context.Context, req *sellersv1.ScheduleSellerScorecardExpiryRequest) (*sellersv1.ScheduleSellerScorecardExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleSellerScorecardExpiry", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "schedule seller scorecard expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetSellerScorecard())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	signalGiftCardTransactionWorkflowRequest := newSignalGiftCardTransactionWorkflowRequest(withSignalGiftCardTransactionWorkflowContext(req.GetContext()), withSignalGiftCardTransactionWorkflowDetails(&giftcardsv1.SignalGiftCardTransactionWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{
			Email: req.GetSellerScorecard().GetCounterparty().GetEmail(),
		},
	}), withSignalGiftCardTransactionWorkflowGiftCardTransaction(&giftcardsv1.GiftCardTransaction{Detail: packed}))
	signalGiftCardTransactionWorkflowReply, err := s.giftCardTransactionWorkflowClient.SignalGiftCardTransactionWorkflow(ctx, signalGiftCardTransactionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/SignalGiftCardTransactionWorkflow")
	}
	cloned := proto.Clone(req.GetSellerScorecard()).(*sellersv1.SellerScorecard)
	batchCreateCustomersStream, err := s.customerClient.BatchCreateCustomers(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchCreateCustomers")
	}
	if err := batchCreateCustomersStream.Send(&customersv1.BatchCreateCustomersRequest{
		Context: req.GetContext(),
		Chunk: &customersv1.BatchCreateCustomersRequest_Header{Header: &customersv1.BatchCreateCustomersRequest_StreamHeader{
			SubmittedBy: &commonv1.ContactInfo{Email: cloned.GetCounterparty().GetEmail()},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchCreateCustomers")
	}
	if _, err := batchCreateCustomersStream.CloseAndRecv(); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchCreateCustomers")
	}
	checkUserEligibilityRequest := &identityv1.CheckUserEligibilityRequest{}
	checkUserEligibilityRequest.Context = req.GetContext()
	checkUserEligibilityRequest.User = &identityv1.User{
		Audit: &commonv1.AuditInfo{Etag: req.GetSellerScorecard().GetAudit().GetEtag()},
		RelationshipManager: &identityv1.UserRef{
			DisplayName: signalGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
		},
		Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
	}
	checkUserEligibilityReply, err := s.userValidationClient.CheckUserEligibility(ctx, checkUserEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/CheckUserEligibility")
	}
	return &sellersv1.ScheduleSellerScorecardExpiryResponse{
		SellerScorecard: &sellersv1.SellerScorecard{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: signalGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Locality: checkUserEligibilityReply.GetUser().GetPrimaryAddress().GetLocality(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
			Metadata:          req.GetSellerScorecard().GetMetadata(),
		},
		Outcome: &sellersv1.ScheduleSellerScorecardExpiryResponse_Outcome{Status: checkUserEligibilityReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// searchIndexHistoryLazy dials acme.search.v1.SearchIndexHistoryService the first time a handler needs it.
func (s *SellerScorecardLifecycleServer) searchIndexHistoryLazy() searchv1.SearchIndexHistoryServiceClient {
	s.searchIndexHistoryOnce.Do(func() { s.searchIndexHistoryCached = searchv1.NewSearchIndexHistoryServiceClient(s.conn) })
	return s.searchIndexHistoryCached
}
