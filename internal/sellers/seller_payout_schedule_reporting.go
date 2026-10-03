package sellers

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featurestorev1 "github.com/hounddogai/protomap-test-golang/gen/acme/featurestore/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerPayoutScheduleReportingServer implements `acme.sellers.v1.SellerPayoutScheduleReportingService`.
// Builds and schedules reports about seller payout schedules.
type SellerPayoutScheduleReportingServer struct {
	sellersv1.UnimplementedSellerPayoutScheduleReportingServiceServer
	store                                 *Store
	log                                   *slog.Logger
	sellerScorecardSyncClient             sellersv1.SellerScorecardSyncServiceClient
	roleHistoryClient                     identityv1.RoleHistoryServiceClient
	conn                                  *grpc.ClientConn
	featureViewQueryClient                featurestorev1.FeatureViewQueryServiceClient
	giftCardTransactionWorkflowOnce       sync.Once
	giftCardTransactionWorkflowCached     giftcardsv1.GiftCardTransactionWorkflowServiceClient
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	sellerListingReportingClient          sellersv1.SellerListingReportingServiceClient
	meteredSoftwareLicense                meteredSoftwareLicense
	sellerLifecycleClient                 sellersv1.SellerLifecycleServiceClient
	giftCardTransactionApprovalClient     giftcardsv1.GiftCardTransactionApprovalServiceClient
	sellerCalculationClient               sellersv1.SellerCalculationServiceClient
	consentRecordValidationClient         customersv1.ConsentRecordValidationServiceClient
	householdValidationClient             customersv1.HouseholdValidationServiceClient
}

// NewSellerPayoutScheduleReportingServer wires a SellerPayoutScheduleReportingServer.
func NewSellerPayoutScheduleReportingServer(deps platform.Deps) *SellerPayoutScheduleReportingServer {
	return &SellerPayoutScheduleReportingServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.sellers.v1.SellerPayoutScheduleReportingService"),
		sellerScorecardSyncClient:             sellersv1.NewSellerScorecardSyncServiceClient(deps.Conn),
		roleHistoryClient:                     identityv1.NewRoleHistoryServiceClient(deps.Conn),
		conn:                                  deps.Conn,
		featureViewQueryClient:                featurestorev1.NewFeatureViewQueryServiceClient(deps.Conn),
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		sellerListingReportingClient:          sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		meteredSoftwareLicense:                meteredSoftwareLicense{SoftwareLicenseServiceClient: licensingv1.NewSoftwareLicenseServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerLifecycleClient:                 sellersv1.NewSellerLifecycleServiceClient(deps.Conn),
		giftCardTransactionApprovalClient:     giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn),
		sellerCalculationClient:               sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		consentRecordValidationClient:         customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		householdValidationClient:             customersv1.NewHouseholdValidationServiceClient(deps.Conn),
	}
}

// GenerateSellerPayoutScheduleReport generates a seller payout schedule.
func (s *SellerPayoutScheduleReportingServer) GenerateSellerPayoutScheduleReport(ctx context.Context, req *sellersv1.GenerateSellerPayoutScheduleReportRequest) (*sellersv1.GenerateSellerPayoutScheduleReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GenerateSellerPayoutScheduleReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "generate_seller_payout_schedule_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate seller payout schedule report", "tenant", req.GetContext().GetTenantId())
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	generateSellerListingReportRequest := &sellersv1.GenerateSellerListingReportRequest{
		Context: req.GetContext(),
		Details: &sellersv1.GenerateSellerListingReportRequest_Details{
			Period:          req.GetDetails().GetPeriod(),
			Filters:         filters,
			RecipientEmails: req.GetDetails().GetRecipientEmails(),
		},
	}
	generateSellerListingReportReply, err := generateSellerListingReportCall(ctx, generateSellerListingReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/GenerateSellerListingReport")
	}
	return &sellersv1.GenerateSellerPayoutScheduleReportResponse{
		Period: generateSellerListingReportReply.GetPeriod(),
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// FetchSellerPayoutScheduleReport returns a seller payout schedule.
func (s *SellerPayoutScheduleReportingServer) FetchSellerPayoutScheduleReport(ctx context.Context, req *sellersv1.FetchSellerPayoutScheduleReportRequest) (*sellersv1.FetchSellerPayoutScheduleReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "fetch_seller_payout_schedule_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch seller payout schedule report", "tenant", req.GetContext().GetTenantId())
	listSellerScorecardConflictsRequest := &sellersv1.ListSellerScorecardConflictsRequest{
		Context: req.GetContext(),
		Filter:  &sellersv1.ListSellerScorecardConflictsRequest_Filter{CreatedAfter: req.GetContext().GetDeadline()},
	}
	type listSellerScorecardConflictsResult struct {
		reply *sellersv1.ListSellerScorecardConflictsResponse
		err   error
	}
	listSellerScorecardConflictsResults := make(chan listSellerScorecardConflictsResult, 1)
	go func() {
		reply, err := s.sellerScorecardSyncClient.ListSellerScorecardConflicts(ctx, listSellerScorecardConflictsRequest)
		listSellerScorecardConflictsResults <- listSellerScorecardConflictsResult{reply, err}
	}()
	var listSellerScorecardConflictsReply *sellersv1.ListSellerScorecardConflictsResponse
	select {
	case result := <-listSellerScorecardConflictsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerScorecardSyncService/ListSellerScorecardConflicts")
		}
		listSellerScorecardConflictsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	annotateRoleVersionRequest := buildAnnotateRoleVersionRequestForFetchSellerPayoutScheduleReport(req)
	annotateRoleVersionReply, err := s.roleHistoryClient.AnnotateRoleVersion(ctx, annotateRoleVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/AnnotateRoleVersion")
	}
	return &sellersv1.FetchSellerPayoutScheduleReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listSellerScorecardConflictsReply.GetResult().GetReferenceIds(),
			Details:      listSellerScorecardConflictsReply.GetResult().GetDetails(),
			Status:       annotateRoleVersionReply.GetOutcome().GetStatus(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{SellerPayoutScheduleId: req.GetSellerPayoutScheduleId()},
	}, nil
}

// ScheduleSellerPayoutScheduleReport schedules a seller payout schedule.
func (s *SellerPayoutScheduleReportingServer) ScheduleSellerPayoutScheduleReport(ctx context.Context, req *sellersv1.ScheduleSellerPayoutScheduleReportRequest) (*sellersv1.ScheduleSellerPayoutScheduleReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleSellerPayoutScheduleReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "schedule_seller_payout_schedule_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule seller payout schedule report", "tenant", req.GetContext().GetTenantId())
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	countFeatureViewsRequest := &featurestorev1.CountFeatureViewsRequest{
		Context: req.GetContext(),
		Filter: &featurestorev1.CountFeatureViewsRequest_Filter{
			Labels:       filters,
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      req.GetSellerPayoutScheduleId(),
		},
	}
	countFeatureViewsReply, err := grpcx.NewClient(s.conn, featurestorev1.NewFeatureViewQueryServiceClient).CountFeatureViews(ctx, countFeatureViewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featurestore.v1.FeatureViewQueryService/CountFeatureViews")
	}
	browseFeatureViewsRequest := &featurestorev1.BrowseFeatureViewsRequest{
		Context: req.GetContext(),
		Filter: &featurestorev1.BrowseFeatureViewsRequest_Filter{
			OwnerId: countFeatureViewsReply.GetResult().GetCorrelationId(),
		},
	}
	browseFeatureViewsReply, err := featurestorev1.FeatureViewQueryServiceClient.BrowseFeatureViews(s.featureViewQueryClient, ctx, browseFeatureViewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featurestore.v1.FeatureViewQueryService/BrowseFeatureViews")
	}
	return &sellersv1.ScheduleSellerPayoutScheduleReportResponse{
		Result: &commonv1.OperationResult{
			Status:        countFeatureViewsReply.GetResult().GetStatus(),
			ReferenceIds:  countFeatureViewsReply.GetResult().GetReferenceIds(),
			CorrelationId: browseFeatureViewsReply.GetResult().GetCorrelationId(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamSellerPayoutScheduleMetrics streams the changes of seller payout schedules as they happen.
func (s *SellerPayoutScheduleReportingServer) StreamSellerPayoutScheduleMetrics(req *sellersv1.StreamSellerPayoutScheduleMetricsRequest, stream grpc.ServerStreamingServer[sellersv1.StreamSellerPayoutScheduleMetricsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream seller payout schedule metrics", "tenant", req.GetContext().GetTenantId())
	source := req.GetSubscriber().ProtoReflect()
	phone := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("phone"))).String()
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	advanceGiftCardTransactionWorkflowRequest := &giftcardsv1.AdvanceGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.AdvanceGiftCardTransactionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: req.GetSubscriber().GetEmail(),
				Phone: phone,
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	advanceGiftCardTransactionWorkflowReply, err := s.giftCardTransactionWorkflowLazy().AdvanceGiftCardTransactionWorkflow(ctx, advanceGiftCardTransactionWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/AdvanceGiftCardTransactionWorkflow")
	}
	var holder struct{ Region string }
	if record := req.GetFilter(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	quoteSellerPayoutScheduleRequest := &sellersv1.QuoteSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email:            req.GetSubscriber().GetEmail(),
					Phone:            req.GetSubscriber().GetPhone(),
					PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
				},
			}},
		},
		Details: &sellersv1.QuoteSellerPayoutScheduleRequest_Details{Destination: &commonv1.PostalAddress{Region: holder.Region}},
	}
	quoteSellerPayoutScheduleReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*sellersv1.QuoteSellerPayoutScheduleResponse, error) {
		return s.sellerPayoutScheduleCalculationClient.QuoteSellerPayoutSchedule(ctx, quoteSellerPayoutScheduleRequest)
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/QuoteSellerPayoutSchedule")
	}
	source2 := req.GetFilter().ProtoReflect()
	region := source2.Get(source2.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	listSellerListingReportsReply, err := s.sellerListingReportingClient.ListSellerListingReports(ctx, &sellersv1.ListSellerListingReportsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListSellerListingReportsRequest_Filter{
			Region:  region,
			Labels:  req.GetFilter().GetLabels(),
			Query:   req.GetFilter().GetQuery(),
			OwnerId: req.GetFilter().GetOwnerId(),
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/ListSellerListingReports")
	}
	createSoftwareLicenseRequest := &licensingv1.CreateSoftwareLicenseRequest{
		Context: req.GetContext(),
		SoftwareLicense: &licensingv1.SoftwareLicense{
			Contact: &commonv1.ContactInfo{Email: req.GetSubscriber().GetEmail()},
			Audit: &commonv1.AuditInfo{
				Etag:      quoteSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetEtag(),
				UpdatedAt: quoteSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetUpdatedAt(),
			},
		},
	}
	createSoftwareLicenseReply, err := s.meteredSoftwareLicense.CreateSoftwareLicense(ctx, createSoftwareLicenseRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.licensing.v1.SoftwareLicenseService/CreateSoftwareLicense")
	}
	if err := stream.Send(&sellersv1.StreamSellerPayoutScheduleMetricsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: advanceGiftCardTransactionWorkflowReply.GetResult().GetReferenceIds(),
			Message:      listSellerListingReportsReply.GetResult().GetMessage(),
			Status:       listSellerListingReportsReply.GetResult().GetStatus(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Title:    quoteSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetTitle(),
			Location: quoteSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetLocation(),
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Name: createSoftwareLicenseReply.GetSoftwareLicense().GetContact().GetName(),
				},
			}},
			Organizer: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// ShareSellerPayoutScheduleReport shares a seller payout schedule.
func (s *SellerPayoutScheduleReportingServer) ShareSellerPayoutScheduleReport(ctx context.Context, req *sellersv1.ShareSellerPayoutScheduleReportRequest) (*sellersv1.ShareSellerPayoutScheduleReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "share_seller_payout_schedule_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share seller payout schedule report", "tenant", req.GetContext().GetTenantId())
	var envelope sellersv1.SellerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scheduleSellerExpiryRequest := &sellersv1.ScheduleSellerExpiryRequest{
		Context: req.GetContext(),
		Details: &sellersv1.ScheduleSellerExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
		},
		SellerId: envelope.GetDisplayName(),
	}
	var scheduleSellerExpiryReply *sellersv1.ScheduleSellerExpiryResponse
	if err := pipeline.Run(ctx, "acme.sellers.v1.SellerLifecycleService/ScheduleSellerExpiry", func(ctx context.Context) error {
		answer, err := s.sellerLifecycleClient.ScheduleSellerExpiry(ctx, scheduleSellerExpiryRequest)
		scheduleSellerExpiryReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerLifecycleService/ScheduleSellerExpiry")
	}
	return &sellersv1.ShareSellerPayoutScheduleReportResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Name: scheduleSellerExpiryReply.GetSeller().GetName()},
			}},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Organizer:              &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: scheduleSellerExpiryReply.GetResult().GetStatus()},
	}, nil
}

// CountSellerPayoutScheduleTotals counts seller payout schedules by status.
func (s *SellerPayoutScheduleReportingServer) CountSellerPayoutScheduleTotals(ctx context.Context, req *sellersv1.CountSellerPayoutScheduleTotalsRequest) (*sellersv1.CountSellerPayoutScheduleTotalsResponse, error) {
	s.log.InfoContext(ctx, "count seller payout schedule totals", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := sellerPayoutScheduleReportingServerDelegateGiftCardTransactionApprovalSteps[mode]
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	delegateGiftCardTransactionApprovalRequest := &giftcardsv1.DelegateGiftCardTransactionApprovalRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.DelegateGiftCardTransactionApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  tenancy.Value(ctx, "email"),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Measurements:  []*giftcardsv1.GiftCardTransactionMeasurement{&giftcardsv1.GiftCardTransactionMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			CorrelationId: req.GetFilter().GetOwnerId(),
		},
	}
	delegateGiftCardTransactionApprovalReply, err := run(s, ctx, delegateGiftCardTransactionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/DelegateGiftCardTransactionApproval")
	}
	quoteSellerRequest := &sellersv1.QuoteSellerRequest{}
	quoteSellerRequest.Context = req.GetContext()
	quoteSellerRequest.Seller = &sellersv1.Seller{
		IdentityDocument: &sellersv1.Seller_Passport{Passport: &sellersv1.SellerPassport{
			Number: delegateGiftCardTransactionApprovalReply.GetGiftCardTransaction().GetSubject().GetNumber(),
		}},
	}
	quoteSellerReply, err := s.sellerCalculationClient.QuoteSeller(ctx, quoteSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/QuoteSeller")
	}
	return &sellersv1.CountSellerPayoutScheduleTotalsResponse{
		Result: &commonv1.OperationResult{
			Status:        delegateGiftCardTransactionApprovalReply.GetOutcome().GetStatus(),
			Details:       delegateGiftCardTransactionApprovalReply.GetResult().GetDetails(),
			CorrelationId: quoteSellerReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// DeleteSellerPayoutScheduleReport deletes a seller payout schedule.
func (s *SellerPayoutScheduleReportingServer) DeleteSellerPayoutScheduleReport(ctx context.Context, req *sellersv1.DeleteSellerPayoutScheduleReportRequest) (*sellersv1.DeleteSellerPayoutScheduleReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DeleteSellerPayoutScheduleReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "delete_seller_payout_schedule_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete seller payout schedule report", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainConsentRecordDecisionRequest := &customersv1.ExplainConsentRecordDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainConsentRecordDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
	}
	type explainConsentRecordDecisionResult struct {
		reply *customersv1.ExplainConsentRecordDecisionResponse
		err   error
	}
	explainConsentRecordDecisionResults := make(chan explainConsentRecordDecisionResult, 1)
	go func() {
		reply, err := s.consentRecordValidationClient.ExplainConsentRecordDecision(ctx, explainConsentRecordDecisionRequest)
		explainConsentRecordDecisionResults <- explainConsentRecordDecisionResult{reply, err}
	}()
	var explainConsentRecordDecisionReply *customersv1.ExplainConsentRecordDecisionResponse
	select {
	case result := <-explainConsentRecordDecisionResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
		}
		explainConsentRecordDecisionReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	validateHouseholdRequest := buildValidateHouseholdRequestForDeleteSellerPayoutScheduleReport(req, explainConsentRecordDecisionReply)
	validateHouseholdReply, err := s.householdValidationClient.ValidateHousehold(ctx, validateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ValidateHousehold")
	}
	_ = validateHouseholdReply
	return &sellersv1.DeleteSellerPayoutScheduleReportResponse{
		Result: &commonv1.OperationResult{
			Status: explainConsentRecordDecisionReply.GetResult().GetStatus(),
		},
	}, nil
}

// giftCardTransactionWorkflowLazy dials acme.giftcards.v1.GiftCardTransactionWorkflowService the first time a handler
// needs it.
func (s *SellerPayoutScheduleReportingServer) giftCardTransactionWorkflowLazy() giftcardsv1.GiftCardTransactionWorkflowServiceClient {
	s.giftCardTransactionWorkflowOnce.Do(func() {
		s.giftCardTransactionWorkflowCached = giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(s.conn)
	})
	return s.giftCardTransactionWorkflowCached
}

// sellerPayoutScheduleReportingServerDelegateGiftCardTransactionApprovalSteps are the ways to run
// acme.giftcards.v1.GiftCardTransactionApprovalService/DelegateGiftCardTransactionApproval, by name.
var sellerPayoutScheduleReportingServerDelegateGiftCardTransactionApprovalSteps = map[string]func(*SellerPayoutScheduleReportingServer, context.Context, *giftcardsv1.DelegateGiftCardTransactionApprovalRequest) (*giftcardsv1.DelegateGiftCardTransactionApprovalResponse, error){
	"default": (*SellerPayoutScheduleReportingServer).delegateGiftCardTransactionApprovalStep,
	"audited": (*SellerPayoutScheduleReportingServer).delegateGiftCardTransactionApprovalAuditedStep,
}

func (s *SellerPayoutScheduleReportingServer) delegateGiftCardTransactionApprovalStep(ctx context.Context, request *giftcardsv1.DelegateGiftCardTransactionApprovalRequest) (*giftcardsv1.DelegateGiftCardTransactionApprovalResponse, error) {
	return s.giftCardTransactionApprovalClient.DelegateGiftCardTransactionApproval(ctx, request)
}

func (s *SellerPayoutScheduleReportingServer) delegateGiftCardTransactionApprovalAuditedStep(ctx context.Context, request *giftcardsv1.DelegateGiftCardTransactionApprovalRequest) (*giftcardsv1.DelegateGiftCardTransactionApprovalResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.giftcards.v1.GiftCardTransactionApprovalService/DelegateGiftCardTransactionApproval")
	return s.giftCardTransactionApprovalClient.DelegateGiftCardTransactionApproval(ctx, request)
}
