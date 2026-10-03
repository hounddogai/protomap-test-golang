package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	performersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/performers/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	surveysv1 "github.com/hounddogai/protomap-test-golang/gen/acme/surveys/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReturnLabelApprovalServer implements `acme.returns.v1.ReturnLabelApprovalService`.
// Routes return labels through approval, with escalation and delegation.
type ReturnLabelApprovalServer struct {
	returnsv1.UnimplementedReturnLabelApprovalServiceServer
	store                          *Store
	log                            *slog.Logger
	preferenceCalculationClient    customersv1.PreferenceCalculationServiceClient
	performerQueryClient           performersv1.PerformerQueryServiceClient
	returnRequestClient            returnsv1.ReturnRequestServiceClient
	surveyQueryClient              surveysv1.SurveyQueryServiceClient
	sessionQueryClient             identityv1.SessionQueryServiceClient
	conn                           *grpc.ClientConn
	inspectionLifecycleOnce        sync.Once
	inspectionLifecycleCached      returnsv1.InspectionLifecycleServiceClient
	returnLabelQueryClient         returnsv1.ReturnLabelQueryServiceClient
	roleHistoryClient              identityv1.RoleHistoryServiceClient
	roleHistoryClientFallback      identityv1.RoleHistoryServiceClient
	returnRequestApprovalClient    returnsv1.ReturnRequestApprovalServiceClient
	tenantExportClient             identityv1.TenantExportServiceClient
	sessionHistoryClient           identityv1.SessionHistoryServiceClient
	sessionHistoryClientFallback   identityv1.SessionHistoryServiceClient
	sellerScorecardLifecycleClient sellersv1.SellerScorecardLifecycleServiceClient
	clients                        *registry.Registry
	restockDecisionLifecycleClient returnsv1.RestockDecisionLifecycleServiceClient
	returnLabelCalculationClient   returnsv1.ReturnLabelCalculationServiceClient
	sessionQueryOnce               sync.Once
	sessionQueryCached             identityv1.SessionQueryServiceClient
}

// NewReturnLabelApprovalServer wires a ReturnLabelApprovalServer.
func NewReturnLabelApprovalServer(deps platform.Deps) *ReturnLabelApprovalServer {
	return &ReturnLabelApprovalServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.returns.v1.ReturnLabelApprovalService"),
		preferenceCalculationClient:    customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		performerQueryClient:           performersv1.NewPerformerQueryServiceClient(deps.Conn),
		returnRequestClient:            returnsv1.NewReturnRequestServiceClient(deps.Conn),
		surveyQueryClient:              surveysv1.NewSurveyQueryServiceClient(deps.Conn),
		sessionQueryClient:             identityv1.NewSessionQueryServiceClient(deps.Conn),
		conn:                           deps.Conn,
		returnLabelQueryClient:         returnsv1.NewReturnLabelQueryServiceClient(deps.Conn),
		roleHistoryClient:              identityv1.NewRoleHistoryServiceClient(deps.Conn),
		roleHistoryClientFallback:      identityv1.NewRoleHistoryServiceClient(deps.Conn),
		returnRequestApprovalClient:    returnsv1.NewReturnRequestApprovalServiceClient(deps.Conn),
		tenantExportClient:             identityv1.NewTenantExportServiceClient(deps.Conn),
		sessionHistoryClient:           identityv1.NewSessionHistoryServiceClient(deps.Conn),
		sessionHistoryClientFallback:   identityv1.NewSessionHistoryServiceClient(deps.Conn),
		sellerScorecardLifecycleClient: sellersv1.NewSellerScorecardLifecycleServiceClient(deps.Conn),
		clients:                        deps.Clients,
		restockDecisionLifecycleClient: returnsv1.NewRestockDecisionLifecycleServiceClient(deps.Conn),
		returnLabelCalculationClient:   returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn),
	}
}

// SubmitReturnLabel submits a return label.
func (s *ReturnLabelApprovalServer) SubmitReturnLabel(ctx context.Context, req *returnsv1.SubmitReturnLabelRequest) (*returnsv1.SubmitReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "submit return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetReturnLabel()).(*returnsv1.ReturnLabel)
	quotePreferenceRequest := &customersv1.QuotePreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.QuotePreferenceRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetReturnLabel().GetBillingAddress().GetPostalCode(),
				Recipient:  req.GetReturnLabel().GetBillingAddress().GetRecipient(),
				Region:     cloned.GetBillingAddress().GetRegion(),
			},
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetReturnLabel().GetAudit().GetUpdatedBy()},
		},
	}
	quotePreferenceReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.QuotePreferenceResponse, error) {
		return s.preferenceCalculationClient.QuotePreference(ctx, quotePreferenceRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/QuotePreference")
	}
	ctx = tenancy.With(ctx, "region", req.GetReturnLabel().GetBillingAddress().GetRegion())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	browsePerformersRequest := &performersv1.BrowsePerformersRequest{}
	browsePerformersRequest.Context = req.GetContext()
	browsePerformersRequest.Filter = &performersv1.BrowsePerformersRequest_Filter{
		Region:       tenancy.Value(ctx, "region"),
		CreatedAfter: req.GetReturnLabel().GetAudit().GetUpdatedAt(),
		OwnerId:      req.GetReturnLabel().GetCounterparty().GetInspectionId(),
		Labels:       annotations,
	}
	browsePerformersReply, err := s.performerQueryClient.BrowsePerformers(ctx, browsePerformersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.performers.v1.PerformerQueryService/BrowsePerformers")
	}
	return &returnsv1.SubmitReturnLabelResponse{
		Outcome: &returnsv1.SubmitReturnLabelResponse_Outcome{
			Status:  quotePreferenceReply.GetResult().GetStatus(),
			Message: browsePerformersReply.GetResult().GetMessage(),
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{
				DisplayName: quotePreferenceReply.GetPreference().GetOwner().GetDisplayName(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
			Number:        req.GetReturnLabel().GetNumber(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveReturnLabel approves a return label.
func (s *ReturnLabelApprovalServer) ApproveReturnLabel(ctx context.Context, req *returnsv1.ApproveReturnLabelRequest) (*returnsv1.ApproveReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveReturnLabel", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "approve return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetReturnLabel()).(*returnsv1.ReturnLabel)
	countSellerScorecardTotalsRequest := &sellersv1.CountSellerScorecardTotalsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.CountSellerScorecardTotalsRequest_Filter{
			Region:       req.GetReturnLabel().GetBillingAddress().GetRegion(),
			CreatedAfter: cloned.GetDueAt(),
		},
	}
	countSellerScorecardTotalsReply, err := countSellerScorecardTotalsCall(ctx, countSellerScorecardTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/CountSellerScorecardTotals")
	}
	updateReturnRequestReply, err := s.returnRequestClient.UpdateReturnRequest(ctx, &returnsv1.UpdateReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetReturnLabel().GetAudit().GetCreatedBy()},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetReturnLabel().GetBillingAddress().GetCountryCode(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestService/UpdateReturnRequest")
	}
	return &returnsv1.ApproveReturnLabelResponse{
		Result: &commonv1.OperationResult{
			Status: countSellerScorecardTotalsReply.GetResult().GetStatus(),
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			DueAt:         updateReturnRequestReply.GetReturnRequest().GetDueAt(),
			Number:        updateReturnRequestReply.GetReturnRequest().GetNumber(),
			ReturnLabelId: req.GetReturnLabelId(),
		},
		Outcome: &returnsv1.ApproveReturnLabelResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RejectReturnLabel rejects a return label.
func (s *ReturnLabelApprovalServer) RejectReturnLabel(ctx context.Context, req *returnsv1.RejectReturnLabelRequest) (*returnsv1.RejectReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "reject return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetReturnLabel()).(*returnsv1.ReturnLabel)
	countSurveysRequest := &surveysv1.CountSurveysRequest{
		Context: req.GetContext(),
		Filter:  &surveysv1.CountSurveysRequest_Filter{Region: cloned.GetBillingAddress().GetRegion()},
	}
	countSurveysReply, err := surveysv1.SurveyQueryServiceClient.CountSurveys(s.surveyQueryClient, ctx, countSurveysRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.surveys.v1.SurveyQueryService/CountSurveys")
	}
	source := req.GetReturnLabel().GetBillingAddress().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	suggestSessionsReply, err := s.sessionQueryClient.SuggestSessions(ctx, &identityv1.SuggestSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.SuggestSessionsRequest_Filter{
			Region:       region,
			CreatedAfter: req.GetReturnLabel().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/SuggestSessions")
	}
	_ = suggestSessionsReply
	return &returnsv1.RejectReturnLabelResponse{
		Result: &commonv1.OperationResult{
			Details: countSurveysReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			ReturnLabelId: req.GetReturnLabelId(),
			Notes:         req.GetReturnLabel().GetNotes(),
		},
	}, nil
}

// EscalateReturnLabel escalates a return label.
func (s *ReturnLabelApprovalServer) EscalateReturnLabel(ctx context.Context, req *returnsv1.EscalateReturnLabelRequest) (*returnsv1.EscalateReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "escalate return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	scheduleInspectionExpiryRequest := &returnsv1.ScheduleInspectionExpiryRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Origin: &returnsv1.Inspection_Customer{Customer: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			}},
		},
	}
	scheduleInspectionExpiryReply, err := s.inspectionLifecycleLazy().ScheduleInspectionExpiry(ctx, scheduleInspectionExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionLifecycleService/ScheduleInspectionExpiry")
	}
	findReturnLabelsRequest := &returnsv1.FindReturnLabelsRequest{
		Context: req.GetContext(),
		Filter: &returnsv1.FindReturnLabelsRequest_Filter{
			Region:       req.GetReturnLabel().GetBillingAddress().GetRegion(),
			CreatedAfter: scheduleInspectionExpiryReply.GetInspection().GetSla().GetDueAt(),
			OwnerId:      scheduleInspectionExpiryReply.GetInspection().GetCustomer().GetCustomerId(),
			Labels:       scheduleInspectionExpiryReply.GetInspection().GetCustomFields(),
		},
	}
	send := s.findReturnLabelsSender(findReturnLabelsRequest)
	findReturnLabelsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelQueryService/FindReturnLabels")
	}
	return &returnsv1.EscalateReturnLabelResponse{
		Result: &commonv1.OperationResult{
			Status:        scheduleInspectionExpiryReply.GetResult().GetStatus(),
			CorrelationId: findReturnLabelsReply.GetResult().GetCorrelationId(),
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{
				InspectionId: scheduleInspectionExpiryReply.GetInspection().GetInspectionId(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
			Status:        req.GetReturnLabel().GetStatus(),
		},
	}, nil
}

// ListPendingReturnLabels lists return labels a page at a time.
func (s *ReturnLabelApprovalServer) ListPendingReturnLabels(ctx context.Context, req *returnsv1.ListPendingReturnLabelsRequest) (*returnsv1.ListPendingReturnLabelsResponse, error) {
	s.log.InfoContext(ctx, "list pending return labels", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *identityv1.AnnotateRoleVersionRequest, ...grpc.CallOption) (*identityv1.AnnotateRoleVersionResponse, error){
		"primary":  s.roleHistoryClient.AnnotateRoleVersion,
		"fallback": s.roleHistoryClientFallback.AnnotateRoleVersion,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	annotateRoleVersionRequest := &identityv1.AnnotateRoleVersionRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	annotateRoleVersionReply, err := route(ctx, annotateRoleVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/AnnotateRoleVersion")
	}
	return &returnsv1.ListPendingReturnLabelsResponse{
		ReturnLabels: []*returnsv1.ReturnLabel{&returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: annotateRoleVersionReply.GetRole().GetAudit().GetUpdatedAt(),
				CreatedBy: annotateRoleVersionReply.GetRole().GetAudit().GetCreatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateReturnLabelApproval delegates a return label.
func (s *ReturnLabelApprovalServer) DelegateReturnLabelApproval(ctx context.Context, req *returnsv1.DelegateReturnLabelApprovalRequest) (*returnsv1.DelegateReturnLabelApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DelegateReturnLabelApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "delegate return label approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	escalateReturnRequestRequest := &returnsv1.EscalateReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetReturnLabel().GetBillingAddress().GetPostalCode(),
				Recipient:  req.GetReturnLabel().GetBillingAddress().GetRecipient(),
			},
			Settlement: &returnsv1.ReturnRequest_Bank{Bank: &returnsv1.ReturnRequestBankSettlement{Bic: req.GetReturnLabel().GetBank().GetBic()}},
		},
		Details: &returnsv1.EscalateReturnRequestRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
	var escalateReturnRequestReply *returnsv1.EscalateReturnRequestResponse
	if err := pipeline.Run(ctx, "acme.returns.v1.ReturnRequestApprovalService/EscalateReturnRequest", func(ctx context.Context) error {
		answer, err := s.returnRequestApprovalClient.EscalateReturnRequest(ctx, escalateReturnRequestRequest)
		escalateReturnRequestReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestApprovalService/EscalateReturnRequest")
	}
	packed, err := anypb.New(req.GetReturnLabel())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	advanceReviewVoteWorkflowRequest := &reviewsv1.AdvanceReviewVoteWorkflowRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: escalateReturnRequestReply.GetReturnRequest().GetCounterparty().GetDisplayName(),
			},
			CorrelationId: escalateReturnRequestReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
		Details: &reviewsv1.AdvanceReviewVoteWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: escalateReturnRequestReply.GetReturnRequest().GetCounterparty().GetEmail(),
			},
		},
	}
	advanceReviewVoteWorkflowReply := new(reviewsv1.AdvanceReviewVoteWorkflowResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.reviews.v1.ReviewVoteWorkflowService/AdvanceReviewVoteWorkflow", advanceReviewVoteWorkflowRequest, advanceReviewVoteWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteWorkflowService/AdvanceReviewVoteWorkflow")
	}
	return &returnsv1.DelegateReturnLabelApprovalResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{
				DisplayName: escalateReturnRequestReply.GetReturnRequest().GetCounterparty().GetDisplayName(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
			DueAt:         req.GetReturnLabel().GetDueAt(),
		},
		Result: &commonv1.OperationResult{
			Status: advanceReviewVoteWorkflowReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// RecallReturnLabel recalls a return label.
func (s *ReturnLabelApprovalServer) RecallReturnLabel(ctx context.Context, req *returnsv1.RecallReturnLabelRequest) (*returnsv1.RecallReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallReturnLabel", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "recall return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	streamTenantExportStream, err := s.tenantExportClient.StreamTenantExport(ctx, &identityv1.StreamTenantExportRequest{
		Context: req.GetContext(),
		Filter: &identityv1.StreamTenantExportRequest_Filter{
			Region:       req.GetReturnLabel().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetReturnLabel().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
	}
	for {
		received, err := streamTenantExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
		}
		_ = received
	}
	routes := map[string]func(context.Context, *identityv1.AnnotateSessionVersionRequest, ...grpc.CallOption) (*identityv1.AnnotateSessionVersionResponse, error){
		"primary":  s.sessionHistoryClient.AnnotateSessionVersion,
		"fallback": s.sessionHistoryClientFallback.AnnotateSessionVersion,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloned := proto.Clone(req.GetReturnLabel()).(*returnsv1.ReturnLabel)
	cloned2 := proto.Clone(req.GetReturnLabel()).(*returnsv1.ReturnLabel)
	snapshot, err := protojson.Marshal(req.GetReturnLabel())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateSessionVersionRequest := &identityv1.AnnotateSessionVersionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Audit: &commonv1.AuditInfo{
				CreatedAt: cloned.GetAudit().GetCreatedAt(),
				UpdatedAt: cloned2.GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		Details: &identityv1.AnnotateSessionVersionRequest_Details{Snapshot: snapshot},
	}
	annotateSessionVersionReply, err := route(ctx, annotateSessionVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/AnnotateSessionVersion")
	}
	scheduleSellerScorecardExpiryRequest := &sellersv1.ScheduleSellerScorecardExpiryRequest{}
	scheduleSellerScorecardExpiryRequest.Context = req.GetContext()
	scheduleSellerScorecardExpiryRequest.SellerScorecard = &sellersv1.SellerScorecard{
		Audit: &commonv1.AuditInfo{
			UpdatedAt: annotateSessionVersionReply.GetSession().GetAudit().GetUpdatedAt(),
			UpdatedBy: annotateSessionVersionReply.GetSession().GetAudit().GetUpdatedBy(),
			Etag:      req.GetReturnLabel().GetAudit().GetEtag(),
		},
		Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetApprover().GetEmail()},
	}
	scheduleSellerScorecardExpiryReply, err := s.sellerScorecardLifecycleClient.ScheduleSellerScorecardExpiry(ctx, scheduleSellerScorecardExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardLifecycleService/ScheduleSellerScorecardExpiry")
	}
	inspectionValidation, ok := s.clients.Get("acme.returns.v1.InspectionValidationService").(returnsv1.InspectionValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.returns.v1.InspectionValidationService")
	}
	previewInspectionRequest := &returnsv1.PreviewInspectionRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Comments: []*returnsv1.InspectionComment{&returnsv1.InspectionComment{
				CreatedAt: scheduleSellerScorecardExpiryReply.GetSellerScorecard().GetAudit().GetCreatedAt(),
			}},
			Requester: &commonv1.ContactInfo{
				Name: annotateSessionVersionReply.GetSession().GetRequester().GetName(),
			},
			Origin: &returnsv1.Inspection_Customer{Customer: &customersv1.CustomerRef{
				CustomerId: scheduleSellerScorecardExpiryReply.GetSellerScorecard().GetCounterparty().GetCustomerId(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag: scheduleSellerScorecardExpiryReply.GetSellerScorecard().GetAudit().GetEtag(),
			},
		},
	}
	previewInspectionReply, err := inspectionValidation.PreviewInspection(ctx, previewInspectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionValidationService/PreviewInspection")
	}
	return &returnsv1.RecallReturnLabelResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{
				DisplayName: annotateSessionVersionReply.GetSession().GetCustomer().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: scheduleSellerScorecardExpiryReply.GetSellerScorecard().GetBillingAddress().GetPostalCode(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
			Status:        req.GetReturnLabel().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Details: scheduleSellerScorecardExpiryReply.GetResult().GetDetails(),
			Message: previewInspectionReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetReturnLabelApprovalStatus returns a return label.
func (s *ReturnLabelApprovalServer) GetReturnLabelApprovalStatus(ctx context.Context, req *returnsv1.GetReturnLabelApprovalStatusRequest) (*returnsv1.GetReturnLabelApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetReturnLabelApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "get_return_label_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get return label approval status", "tenant", req.GetContext().GetTenantId())
	recallReturnRequestRequest := &returnsv1.RecallReturnRequestRequest{}
	recallReturnRequestRequest.Context = req.GetContext()
	recallReturnRequestRequest.Details = &returnsv1.RecallReturnRequestRequest_Details{
		Approver: &identityv1.UserRef{
			Email:  req.GetContext().GetActor().GetEmail(),
			UserId: req.GetContext().GetActor().GetUserId(),
		},
	}
	recallReturnRequestRequest.ReturnRequest = &returnsv1.ReturnRequest{
		DueAt: req.GetContext().GetDeadline(),
		Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{Attributes: req.GetContext().GetBaggage()}},
	}
	recallReturnRequestReply, err := s.returnRequestApprovalClient.RecallReturnRequest(ctx, recallReturnRequestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestApprovalService/RecallReturnRequest")
	}
	return &returnsv1.GetReturnLabelApprovalStatusResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: recallReturnRequestReply.GetReturnRequest().GetBillingAddress().GetPostalCode(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentReturnLabelApproval comments a return label.
func (s *ReturnLabelApprovalServer) CommentReturnLabelApproval(ctx context.Context, req *returnsv1.CommentReturnLabelApprovalRequest) (*returnsv1.CommentReturnLabelApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentReturnLabelApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "comment return label approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	scheduleRestockDecisionExpiryReply, err := s.restockDecisionLifecycleClient.ScheduleRestockDecisionExpiry(ctx, &returnsv1.ScheduleRestockDecisionExpiryRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetReturnLabel().GetAudit().GetUpdatedBy()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionLifecycleService/ScheduleRestockDecisionExpiry")
	}
	for _, line := range req.GetReturnLabel().GetLines() {
		if _, err := s.returnLabelCalculationClient.QuoteReturnLabel(ctx, &returnsv1.QuoteReturnLabelRequest{
			Context: req.GetContext(),
			ReturnLabel: &returnsv1.ReturnLabel{
				Lines: []*returnsv1.ReturnLabelLine{&returnsv1.ReturnLabelLine{
					Tax: &returnsv1.ReturnLabelLineTax{
						Jurisdiction: line.GetTax().GetJurisdiction(),
						Amount:       line.GetTax().GetAmount(),
					},
					Amount: line.GetAmount(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/QuoteReturnLabel")
		}
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	summarizeSessionsRequest := &identityv1.SummarizeSessionsRequest{
		Context: req.GetContext(),
		Reason:  tenancy.Value(ctx, "reason"),
	}
	summarizeSessionsReply, err := s.sessionQueryLazy().SummarizeSessions(ctx, summarizeSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/SummarizeSessions")
	}
	return &returnsv1.CommentReturnLabelApprovalResponse{
		Outcome: &returnsv1.CommentReturnLabelApprovalResponse_Outcome{
			ReferenceId: scheduleRestockDecisionExpiryReply.GetOutcome().GetReferenceId(),
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scheduleRestockDecisionExpiryReply.GetRestockDecision().GetAudit().GetUpdatedBy(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
			DueAt:         req.GetReturnLabel().GetDueAt(),
		},
		Result: &commonv1.OperationResult{
			Message: summarizeSessionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// inspectionLifecycleLazy dials acme.returns.v1.InspectionLifecycleService the first time a handler needs it.
func (s *ReturnLabelApprovalServer) inspectionLifecycleLazy() returnsv1.InspectionLifecycleServiceClient {
	s.inspectionLifecycleOnce.Do(func() { s.inspectionLifecycleCached = returnsv1.NewInspectionLifecycleServiceClient(s.conn) })
	return s.inspectionLifecycleCached
}

// findReturnLabelsSender returns a function that sends one request to
// acme.returns.v1.ReturnLabelQueryService/FindReturnLabels.
func (s *ReturnLabelApprovalServer) findReturnLabelsSender(request *returnsv1.FindReturnLabelsRequest) func(context.Context) (*returnsv1.FindReturnLabelsResponse, error) {
	return func(ctx context.Context) (*returnsv1.FindReturnLabelsResponse, error) {
		return s.returnLabelQueryClient.FindReturnLabels(ctx, request)
	}
}

// sessionQueryLazy dials acme.identity.v1.SessionQueryService the first time a handler needs it.
func (s *ReturnLabelApprovalServer) sessionQueryLazy() identityv1.SessionQueryServiceClient {
	s.sessionQueryOnce.Do(func() { s.sessionQueryCached = identityv1.NewSessionQueryServiceClient(s.conn) })
	return s.sessionQueryCached
}
