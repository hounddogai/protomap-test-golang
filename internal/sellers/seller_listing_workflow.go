package sellers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SellerListingWorkflowServer implements `acme.sellers.v1.SellerListingWorkflowService`.
// Runs the long-lived workflow that takes a seller listing from start to finish.
type SellerListingWorkflowServer struct {
	sellersv1.UnimplementedSellerListingWorkflowServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	sellerValidationClient          sellersv1.SellerValidationServiceClient
	sellerScorecardValidationClient sellersv1.SellerScorecardValidationServiceClient
	giftCardApprovalClient          giftcardsv1.GiftCardApprovalServiceClient
	translationKeyQueryClient       localizationv1.TranslationKeyQueryServiceClient
	sellerListingReportingClient    sellersv1.SellerListingReportingServiceClient
	sellerApprovalClient            sellersv1.SellerApprovalServiceClient
	userQueryClient                 identityv1.UserQueryServiceClient
	meteredSynonymSync              meteredSynonymSync
	sellerScorecardReportingClient  sellersv1.SellerScorecardReportingServiceClient
	roleHistoryPort                 roleHistoryPort
	giftCardBatchApprovalClient     giftcardsv1.GiftCardBatchApprovalServiceClient
}

// NewSellerListingWorkflowServer wires a SellerListingWorkflowServer.
func NewSellerListingWorkflowServer(deps platform.Deps) *SellerListingWorkflowServer {
	return &SellerListingWorkflowServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.sellers.v1.SellerListingWorkflowService"),
		conn:                            deps.Conn,
		sellerValidationClient:          sellersv1.NewSellerValidationServiceClient(deps.Conn),
		sellerScorecardValidationClient: sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		giftCardApprovalClient:          giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		translationKeyQueryClient:       localizationv1.NewTranslationKeyQueryServiceClient(deps.Conn),
		sellerListingReportingClient:    sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		sellerApprovalClient:            sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		userQueryClient:                 identityv1.NewUserQueryServiceClient(deps.Conn),
		meteredSynonymSync:              meteredSynonymSync{SynonymSyncServiceClient: searchv1.NewSynonymSyncServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerScorecardReportingClient:  sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn),
		roleHistoryPort:                 newRoleHistoryPort(deps),
		giftCardBatchApprovalClient:     giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
	}
}

// StartSellerListingWorkflow starts a seller listing.
func (s *SellerListingWorkflowServer) StartSellerListingWorkflow(ctx context.Context, req *sellersv1.StartSellerListingWorkflowRequest) (*sellersv1.StartSellerListingWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "start seller listing workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	countSellerScorecardTotalsRequest := &sellersv1.CountSellerScorecardTotalsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.CountSellerScorecardTotalsRequest_Filter{
			Region:       req.GetSellerListing().GetAddress().GetRegion(),
			CreatedAfter: req.GetSellerListing().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetSellerListing().GetOwner().GetUserId(),
		},
	}
	countSellerScorecardTotalsReply := new(sellersv1.CountSellerScorecardTotalsResponse)
	if err := grpcx.Invoke(ctx, s.conn, sellersv1.SellerScorecardReportingService_CountSellerScorecardTotals_FullMethodName, countSellerScorecardTotalsRequest, countSellerScorecardTotalsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/CountSellerScorecardTotals")
	}
	var region string
	switch choice := req.GetSellerListing().GetLocator().(type) {
	case *sellersv1.SellerListing_Address:
		region = choice.Address.GetRegion()
	case *sellersv1.SellerListing_FacilityCode:
		region = choice.FacilityCode
	}
	checkSellerEligibilityRequest := newCheckSellerEligibilityRequest(withCheckSellerEligibilityContext(req.GetContext()), withCheckSellerEligibilitySeller(&sellersv1.Seller{
		PrimaryAddress: &commonv1.PostalAddress{
			Region:    region,
			Recipient: req.GetSellerListing().GetAddress().GetRecipient(),
		},
		Contact: &commonv1.ContactInfo{
			Name:  req.GetDetails().GetNotify().GetName(),
			Email: req.GetSellerListing().GetOwner().GetEmail(),
		},
	}))
	checkSellerEligibilityReply, err := s.sellerValidationClient.CheckSellerEligibility(ctx, checkSellerEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/CheckSellerEligibility")
	}
	return &sellersv1.StartSellerListingWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  countSellerScorecardTotalsReply.GetResult().GetReferenceIds(),
			Message:       countSellerScorecardTotalsReply.GetResult().GetMessage(),
			CorrelationId: checkSellerEligibilityReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				UserId: checkSellerEligibilityReply.GetSeller().GetRelationshipManager().GetUserId(),
			},
			SellerListingId: req.GetSellerListingId(),
			Status:          req.GetSellerListing().GetStatus(),
		},
	}, nil
}

// AdvanceSellerListingWorkflow advances a seller listing.
func (s *SellerListingWorkflowServer) AdvanceSellerListingWorkflow(ctx context.Context, req *sellersv1.AdvanceSellerListingWorkflowRequest) (*sellersv1.AdvanceSellerListingWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvanceSellerListingWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "advance seller listing workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	source := req.GetSellerListing().GetAudit().ProtoReflect()
	etag := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("etag"))).String()
	explainSellerScorecardDecisionRequest := &sellersv1.ExplainSellerScorecardDecisionRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Audit: &commonv1.AuditInfo{Etag: etag},
			Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
				Description: req.GetSellerListing().GetComponents()["primary"].GetDescription(),
			}},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetSellerListing().GetAddress().GetRegion()},
		},
	}
	explainSellerScorecardDecisionReply, err := grpcx.Call(ctx, s.sellerScorecardValidationClient.ExplainSellerScorecardDecision, explainSellerScorecardDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/ExplainSellerScorecardDecision")
	}
	rejectGiftCardRequest := &giftcardsv1.RejectGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				CreatedAt: explainSellerScorecardDecisionReply.GetSellerScorecard().GetAudit().GetCreatedAt(),
			},
		},
	}
	rejectGiftCardReply, err := giftcardsv1.GiftCardApprovalServiceClient.RejectGiftCard(s.giftCardApprovalClient, ctx, rejectGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/RejectGiftCard")
	}
	return &sellersv1.AdvanceSellerListingWorkflowResponse{
		Outcome: &sellersv1.AdvanceSellerListingWorkflowResponse_Outcome{
			Message: explainSellerScorecardDecisionReply.GetOutcome().GetMessage(),
			Status:  rejectGiftCardReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: rejectGiftCardReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Status:          req.GetSellerListing().GetStatus(),
		},
	}, nil
}

// CancelSellerListingWorkflow cancels a seller listing.
func (s *SellerListingWorkflowServer) CancelSellerListingWorkflow(ctx context.Context, req *sellersv1.CancelSellerListingWorkflowRequest) (*sellersv1.CancelSellerListingWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CancelSellerListingWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "cancel seller listing workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	var region string
	switch choice := req.GetSellerListing().GetLocator().(type) {
	case *sellersv1.SellerListing_Address:
		region = choice.Address.GetRegion()
	case *sellersv1.SellerListing_FacilityCode:
		region = choice.FacilityCode
	}
	browseNotificationsRequest := &notificationsv1.BrowseNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.BrowseNotificationsRequest_Filter{
			Region:       region,
			CreatedAfter: req.GetSellerListing().GetAudit().GetUpdatedAt(),
		},
	}
	browseNotificationsReply, err := grpcx.NewClient(s.conn, notificationsv1.NewNotificationQueryServiceClient).BrowseNotifications(ctx, browseNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/BrowseNotifications")
	}
	exportTranslationKeysStream, err := s.translationKeyQueryClient.ExportTranslationKeys(ctx, &localizationv1.ExportTranslationKeysRequest{
		Context: req.GetContext(),
		Filter:  &localizationv1.ExportTranslationKeysRequest_Filter{Region: req.GetSellerListing().GetAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.localization.v1.TranslationKeyQueryService/ExportTranslationKeys")
	}
	for {
		received, err := exportTranslationKeysStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.localization.v1.TranslationKeyQueryService/ExportTranslationKeys")
		}
		_ = received
	}
	return &sellersv1.CancelSellerListingWorkflowResponse{
		Result: &commonv1.OperationResult{Status: browseNotificationsReply.GetResult().GetStatus()},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Owner:           req.GetSellerListing().GetOwner(),
		},
	}, nil
}

// RetrySellerListingWorkflow retrys a seller listing.
func (s *SellerListingWorkflowServer) RetrySellerListingWorkflow(ctx context.Context, req *sellersv1.RetrySellerListingWorkflowRequest) (*sellersv1.RetrySellerListingWorkflowResponse, error) {
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "retry seller listing workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	fetchSellerListingReportRequest := buildFetchSellerListingReportRequestForRetrySellerListingWorkflow(req)
	fetchSellerListingReportReply, err := s.sellerListingReportingClient.FetchSellerListingReport(ctx, fetchSellerListingReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/FetchSellerListingReport")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := sellerListingWorkflowServerApproveSellerSteps[mode]
	approveSellerRequest := &sellersv1.ApproveSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				Etag: fetchSellerListingReportReply.GetSellerListing().GetAudit().GetEtag(),
			},
			RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
	approveSellerReply, err := run(s, ctx, approveSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/ApproveSeller")
	}
	return &sellersv1.RetrySellerListingWorkflowResponse{
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: fetchSellerListingReportReply.GetSellerListing().GetAudit().GetUpdatedBy(),
			},
			Status: fetchSellerListingReportReply.GetSellerListing().GetStatus(),
			Owner: &identityv1.UserRef{
				UserId: approveSellerReply.GetSeller().GetRelationshipManager().GetUserId(),
			},
			SellerListingId: req.GetSellerListingId(),
			Name:            req.GetSellerListing().GetName(),
		},
		Outcome: &sellersv1.RetrySellerListingWorkflowResponse_Outcome{Status: approveSellerReply.GetOutcome().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseSellerListingWorkflow pauses a seller listing.
func (s *SellerListingWorkflowServer) PauseSellerListingWorkflow(ctx context.Context, req *sellersv1.PauseSellerListingWorkflowRequest) (*sellersv1.PauseSellerListingWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseSellerListingWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "pause seller listing workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	step := &sellerListingWorkflowServerPauseSellerListingWorkflowPeekUserStep{request: &identityv1.PeekUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetSellerListing().GetOwner().GetUserId(),
	}}
	if err := s.runPauseSellerListingWorkflowPeekUser(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/PeekUser")
	}
	peekUserReply := step.reply
	syncSynonymsRequest := &searchv1.SyncSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetSellerListing().GetAudit().GetCreatedBy(),
				UpdatedAt: req.GetSellerListing().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				UserId: peekUserReply.GetUser().GetRelationshipManager().GetUserId(),
			},
		},
	}
	syncSynonymsReply, err := s.meteredSynonymSync.SyncSynonyms(ctx, syncSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/SyncSynonyms")
	}
	step2 := &sellerListingWorkflowServerPauseSellerListingWorkflowListSellerScorecardReportsStep{request: &sellersv1.ListSellerScorecardReportsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListSellerScorecardReportsRequest_Filter{
			Region:       req.GetSellerListing().GetAddress().GetRegion(),
			CreatedAfter: peekUserReply.GetUser().GetAudit().GetCreatedAt(),
			Labels:       req.GetSellerListing().GetSpecs(),
			OwnerId:      syncSynonymsReply.GetOutcome().GetReferenceId(),
		},
	}}
	if err := s.runPauseSellerListingWorkflowListSellerScorecardReports(ctx, step2); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/ListSellerScorecardReports")
	}
	listSellerScorecardReportsReply := step2.reply
	return &sellersv1.PauseSellerListingWorkflowResponse{
		Outcome: &sellersv1.PauseSellerListingWorkflowResponse_Outcome{Message: peekUserReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			CorrelationId: syncSynonymsReply.GetResult().GetCorrelationId(),
			Status:        listSellerScorecardReportsReply.GetResult().GetStatus(),
			Message:       listSellerScorecardReportsReply.GetResult().GetMessage(),
		},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Status:          req.GetSellerListing().GetStatus(),
		},
	}, nil
}

// ResumeSellerListingWorkflow resumes a seller listing.
func (s *SellerListingWorkflowServer) ResumeSellerListingWorkflow(ctx context.Context, req *sellersv1.ResumeSellerListingWorkflowRequest) (*sellersv1.ResumeSellerListingWorkflowResponse, error) {
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "resume seller listing workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	pullSynonymsRequest := &searchv1.PullSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetSellerListing().GetAudit().GetCreatedAt()},
		},
	}
	pullSynonymsReply, err := pullSynonymsCall(ctx, pullSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PullSynonyms")
	}
	return &sellersv1.ResumeSellerListingWorkflowResponse{
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: pullSynonymsReply.GetSynonym().GetAudit().GetUpdatedBy(),
			},
			SellerListingId: req.GetSellerListingId(),
			Code:            req.GetSellerListing().GetCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectSellerListingWorkflow returns a seller listing.
func (s *SellerListingWorkflowServer) InspectSellerListingWorkflow(ctx context.Context, req *sellersv1.InspectSellerListingWorkflowRequest) (*sellersv1.InspectSellerListingWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "inspect_seller_listing_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect seller listing workflow", "tenant", req.GetContext().GetTenantId())
	annotateRoleVersionRequest := &identityv1.AnnotateRoleVersionRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
	}
	annotateRoleVersionReply, err := s.roleHistoryPort.AnnotateRoleVersion(ctx, annotateRoleVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/AnnotateRoleVersion")
	}
	recalculateCustomerSegmentsRequest := &customersv1.RecalculateCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				UserId: annotateRoleVersionReply.GetRole().GetOwner().GetUserId(),
			},
		},
	}
	recalculateCustomerSegmentsReply, err := recalculateCustomerSegmentsCall(ctx, recalculateCustomerSegmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments")
	}
	return &sellersv1.InspectSellerListingWorkflowResponse{
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				UserId:      annotateRoleVersionReply.GetRole().GetOwner().GetUserId(),
				DisplayName: recalculateCustomerSegmentsReply.GetCustomerSegment().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: recalculateCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetCreatedBy(),
			},
			SellerListingId: req.GetSellerListingId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: annotateRoleVersionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ListSellerListingWorkflows lists seller listings a page at a time.
func (s *SellerListingWorkflowServer) ListSellerListingWorkflows(ctx context.Context, req *sellersv1.ListSellerListingWorkflowsRequest) (*sellersv1.ListSellerListingWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list seller listing workflows", "tenant", req.GetContext().GetTenantId())
	listPendingGiftCardBatchesRequest := &giftcardsv1.ListPendingGiftCardBatchesRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
	}
	listPendingGiftCardBatchesReply, err := grpcx.Call(ctx, s.giftCardBatchApprovalClient.ListPendingGiftCardBatches, listPendingGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/ListPendingGiftCardBatches")
	}
	return &sellersv1.ListSellerListingWorkflowsResponse{
		PageInfo: &commonv1.PageInfo{
			NextPageToken: listPendingGiftCardBatchesReply.GetPageInfo().GetNextPageToken(),
		},
		Result: &commonv1.OperationResult{
			Status: listPendingGiftCardBatchesReply.GetResult().GetStatus(),
		},
	}, nil
}

// sellerListingWorkflowServerApproveSellerSteps are the ways to run
// acme.sellers.v1.SellerApprovalService/ApproveSeller, by name.
var sellerListingWorkflowServerApproveSellerSteps = map[string]func(*SellerListingWorkflowServer, context.Context, *sellersv1.ApproveSellerRequest) (*sellersv1.ApproveSellerResponse, error){
	"default": (*SellerListingWorkflowServer).approveSellerStep,
	"audited": (*SellerListingWorkflowServer).approveSellerAuditedStep,
}

func (s *SellerListingWorkflowServer) approveSellerStep(ctx context.Context, request *sellersv1.ApproveSellerRequest) (*sellersv1.ApproveSellerResponse, error) {
	return s.sellerApprovalClient.ApproveSeller(ctx, request)
}

func (s *SellerListingWorkflowServer) approveSellerAuditedStep(ctx context.Context, request *sellersv1.ApproveSellerRequest) (*sellersv1.ApproveSellerResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.sellers.v1.SellerApprovalService/ApproveSeller")
	return s.sellerApprovalClient.ApproveSeller(ctx, request)
}

// sellerListingWorkflowServerPauseSellerListingWorkflowPeekUserStep carries the request of one step of
// PauseSellerListingWorkflow and, once it ran, its reply.
type sellerListingWorkflowServerPauseSellerListingWorkflowPeekUserStep struct {
	request *identityv1.PeekUserRequest
	reply   *identityv1.PeekUserResponse
}

func (s *SellerListingWorkflowServer) runPauseSellerListingWorkflowPeekUser(ctx context.Context, step *sellerListingWorkflowServerPauseSellerListingWorkflowPeekUserStep) error {
	reply, err := s.userQueryClient.PeekUser(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// sellerListingWorkflowServerPauseSellerListingWorkflowListSellerScorecardReportsStep carries the request of one step
// of PauseSellerListingWorkflow and, once it ran, its reply.
type sellerListingWorkflowServerPauseSellerListingWorkflowListSellerScorecardReportsStep struct {
	request *sellersv1.ListSellerScorecardReportsRequest
	reply   *sellersv1.ListSellerScorecardReportsResponse
}

func (s *SellerListingWorkflowServer) runPauseSellerListingWorkflowListSellerScorecardReports(ctx context.Context, step *sellerListingWorkflowServerPauseSellerListingWorkflowListSellerScorecardReportsStep) error {
	reply, err := s.sellerScorecardReportingClient.ListSellerScorecardReports(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
