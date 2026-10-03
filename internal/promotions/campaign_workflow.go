package promotions

import (
	"context"
	"fmt"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// CampaignWorkflowServer implements `acme.promotions.v1.CampaignWorkflowService`.
// Runs the long-lived workflow that takes a campaign from start to finish.
type CampaignWorkflowServer struct {
	promotionsv1.UnsafeCampaignWorkflowServiceServer
	store                                 *Store
	log                                   *slog.Logger
	returnLabelWorkflowClient             returnsv1.ReturnLabelWorkflowServiceClient
	meteredReturnLabelWorkflow            meteredReturnLabelWorkflow
	fraudSignalWorkflowClient             fraudv1.FraudSignalWorkflowServiceClient
	campaignReportingClient               promotionsv1.CampaignReportingServiceClient
	campaignApprovalClient                promotionsv1.CampaignApprovalServiceClient
	campaignSyncClient                    promotionsv1.CampaignSyncServiceClient
	clients                               *registry.Registry
	redemptionImportClient                promotionsv1.RedemptionImportServiceClient
	fraudSignalWorkflowPort               fraudSignalWorkflowPort
	userHistoryClient                     identityv1.UserHistoryServiceClient
	apiCredentialValidationClient         identityv1.ApiCredentialValidationServiceClient
	apiCredentialValidationClientFallback identityv1.ApiCredentialValidationServiceClient
	notificationQueryClient               notificationsv1.NotificationQueryServiceClient
	inspectionApprovalClient              returnsv1.InspectionApprovalServiceClient
	conn                                  *grpc.ClientConn
}

// NewCampaignWorkflowServer wires a CampaignWorkflowServer.
func NewCampaignWorkflowServer(deps platform.Deps) *CampaignWorkflowServer {
	return &CampaignWorkflowServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.promotions.v1.CampaignWorkflowService"),
		returnLabelWorkflowClient:             returnsv1.NewReturnLabelWorkflowServiceClient(deps.Conn),
		meteredReturnLabelWorkflow:            meteredReturnLabelWorkflow{ReturnLabelWorkflowServiceClient: returnsv1.NewReturnLabelWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		fraudSignalWorkflowClient:             fraudv1.NewFraudSignalWorkflowServiceClient(deps.Conn),
		campaignReportingClient:               promotionsv1.NewCampaignReportingServiceClient(deps.Conn),
		campaignApprovalClient:                promotionsv1.NewCampaignApprovalServiceClient(deps.Conn),
		campaignSyncClient:                    promotionsv1.NewCampaignSyncServiceClient(deps.Conn),
		clients:                               deps.Clients,
		redemptionImportClient:                promotionsv1.NewRedemptionImportServiceClient(deps.Conn),
		fraudSignalWorkflowPort:               newFraudSignalWorkflowPort(deps),
		userHistoryClient:                     identityv1.NewUserHistoryServiceClient(deps.Conn),
		apiCredentialValidationClient:         identityv1.NewApiCredentialValidationServiceClient(deps.Conn),
		apiCredentialValidationClientFallback: identityv1.NewApiCredentialValidationServiceClient(deps.Conn),
		notificationQueryClient:               notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
		inspectionApprovalClient:              returnsv1.NewInspectionApprovalServiceClient(deps.Conn),
		conn:                                  deps.Conn,
	}
}

// StartCampaignWorkflow starts a campaign.
func (s *CampaignWorkflowServer) StartCampaignWorkflow(ctx context.Context, req *promotionsv1.StartCampaignWorkflowRequest) (*promotionsv1.StartCampaignWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "start campaign workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*returnsv1.ListReturnLabelWorkflowsResponse, len(req.GetCampaign().GetParticipants()))
	for i, participant := range req.GetCampaign().GetParticipants() {
		group.Go(func() error {
			reply, err := s.returnLabelWorkflowClient.ListReturnLabelWorkflows(groupCtx, &returnsv1.ListReturnLabelWorkflowsRequest{
				Context: req.GetContext(),
				Filter: &returnsv1.ListReturnLabelWorkflowsRequest_Filter{
					Region:       req.GetCampaign().GetLocation().GetRegion(),
					OwnerId:      participant.GetParticipantId(),
					CreatedAfter: req.GetCampaign().GetAudit().GetUpdatedAt(),
					Labels:       req.GetDetails().GetParameters(),
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
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelWorkflowService/ListReturnLabelWorkflows")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	source := req.GetCampaign().GetLocation().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	startReturnLabelWorkflowRequest := &returnsv1.StartReturnLabelWorkflowRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				Lines:  req.GetCampaign().GetLocation().GetLines(),
				Region: region,
			},
			Counterparty: &returnsv1.InspectionRef{
				DisplayName: req.GetCampaign().GetOrganizer().GetDisplayName(),
			},
		},
		Details: &returnsv1.StartReturnLabelWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
	}
	startReturnLabelWorkflowReply, err := s.meteredReturnLabelWorkflow.StartReturnLabelWorkflow(ctx, startReturnLabelWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelWorkflowService/StartReturnLabelWorkflow")
	}
	return &promotionsv1.StartCampaignWorkflowResponse{
		Campaign: &promotionsv1.Campaign{
			Audit: &commonv1.AuditInfo{
				CreatedBy: startReturnLabelWorkflowReply.GetReturnLabel().GetAudit().GetCreatedBy(),
				UpdatedAt: startReturnLabelWorkflowReply.GetReturnLabel().GetAudit().GetUpdatedAt(),
			},
			CampaignId: req.GetCampaignId(),
			Metadata:   req.GetCampaign().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvanceCampaignWorkflow advances a campaign.
func (s *CampaignWorkflowServer) AdvanceCampaignWorkflow(ctx context.Context, req *promotionsv1.AdvanceCampaignWorkflowRequest) (*promotionsv1.AdvanceCampaignWorkflowResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "advance campaign workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*fraudv1.PauseFraudSignalWorkflowResponse, len(req.GetCampaign().GetParticipants()))
	for i, participant := range req.GetCampaign().GetParticipants() {
		group.Go(func() error {
			reply, err := s.fraudSignalWorkflowClient.PauseFraudSignalWorkflow(groupCtx, &fraudv1.PauseFraudSignalWorkflowRequest{
				Context: req.GetContext(),
				Details: &fraudv1.PauseFraudSignalWorkflowRequest_Details{
					Notify: &commonv1.ContactInfo{Name: participant.GetContact().GetName()},
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
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalWorkflowService/PauseFraudSignalWorkflow")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	scheduleCampaignReportRequest := &promotionsv1.ScheduleCampaignReportRequest{
		Context:    req.GetContext(),
		CampaignId: req.GetCampaign().GetCampaignId(),
		Reason:     req.GetReason(),
	}
	scheduleCampaignReportReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*promotionsv1.ScheduleCampaignReportResponse, error) {
		return s.campaignReportingClient.ScheduleCampaignReport(ctx, scheduleCampaignReportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignReportingService/ScheduleCampaignReport")
	}
	return &promotionsv1.AdvanceCampaignWorkflowResponse{
		Outcome: &promotionsv1.AdvanceCampaignWorkflowResponse_Outcome{
			Message: scheduleCampaignReportReply.GetResult().GetMessage(),
		},
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Metadata:   req.GetCampaign().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelCampaignWorkflow cancels a campaign.
func (s *CampaignWorkflowServer) CancelCampaignWorkflow(ctx context.Context, req *promotionsv1.CancelCampaignWorkflowRequest) (*promotionsv1.CancelCampaignWorkflowResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "cancel campaign workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	escalateCampaignRequest := &promotionsv1.EscalateCampaignRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
			}},
		},
	}
	type escalateCampaignResult struct {
		reply *promotionsv1.EscalateCampaignResponse
		err   error
	}
	escalateCampaignResults := make(chan escalateCampaignResult, 1)
	go func() {
		reply, err := s.campaignApprovalClient.EscalateCampaign(ctx, escalateCampaignRequest)
		escalateCampaignResults <- escalateCampaignResult{reply, err}
	}()
	var escalateCampaignReply *promotionsv1.EscalateCampaignResponse
	select {
	case result := <-escalateCampaignResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.promotions.v1.CampaignApprovalService/EscalateCampaign")
		}
		escalateCampaignReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	pullCampaignsRequest := buildPullCampaignsRequestForCancelCampaignWorkflow(req, escalateCampaignReply)
	pullCampaignsReply, err := s.campaignSyncClient.PullCampaigns(ctx, pullCampaignsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/PullCampaigns")
	}
	redemptionImport, err := registry.Lookup[promotionsv1.RedemptionImportServiceClient](s.clients, "acme.promotions.v1.RedemptionImportService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	packed, err := anypb.New(req.GetCampaign())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateRedemptionImportRequest := &promotionsv1.ValidateRedemptionImportRequest{
		Context: req.GetContext(),
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: pullCampaignsReply.GetCampaign().GetOrganizer().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &promotionsv1.ValidateRedemptionImportRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            escalateCampaignReply.GetCampaign().GetOrganizer().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	validateRedemptionImportReply, err := redemptionImport.ValidateRedemptionImport(ctx, validateRedemptionImportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionImportService/ValidateRedemptionImport")
	}
	return &promotionsv1.CancelCampaignWorkflowResponse{
		Campaign: &promotionsv1.Campaign{
			Location: &commonv1.PostalAddress{
				Recipient: escalateCampaignReply.GetCampaign().GetLocation().GetRecipient(),
			},
			Window: pullCampaignsReply.GetCampaign().GetWindow(),
			Organizer: &identityv1.UserRef{
				DisplayName: validateRedemptionImportReply.GetRedemption().GetSubject().GetDisplayName(),
			},
			CampaignId: req.GetCampaignId(),
			Title:      req.GetCampaign().GetTitle(),
		},
		Outcome: &promotionsv1.CancelCampaignWorkflowResponse_Outcome{NextSteps: pullCampaignsReply.GetOutcome().GetNextSteps()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryCampaignWorkflow retrys a campaign.
func (s *CampaignWorkflowServer) RetryCampaignWorkflow(ctx context.Context, req *promotionsv1.RetryCampaignWorkflowRequest) (*promotionsv1.RetryCampaignWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "retry campaign workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*promotionsv1.MapRedemptionImportColumnsResponse, len(req.GetCampaign().GetParticipants()))
	for i, participant := range req.GetCampaign().GetParticipants() {
		group.Go(func() error {
			reply, err := s.redemptionImportClient.MapRedemptionImportColumns(groupCtx, &promotionsv1.MapRedemptionImportColumnsRequest{
				Context: req.GetContext(),
				Details: &promotionsv1.MapRedemptionImportColumnsRequest_Details{
					Contact: &commonv1.ContactInfo{
						Name:  participant.GetContact().GetName(),
						Phone: participant.GetContact().GetPhone(),
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
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionImportService/MapRedemptionImportColumns")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	resumeFraudSignalWorkflowRequest := &fraudv1.ResumeFraudSignalWorkflowRequest{
		Context: req.GetContext(),
		Details: &fraudv1.ResumeFraudSignalWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetNotify().GetName(),
				Email: req.GetDetails().GetNotify().GetEmail(),
			},
			Parameters: req.GetDetails().GetParameters(),
		},
	}
	resumeFraudSignalWorkflowReply, err := s.fraudSignalWorkflowPort.ResumeFraudSignalWorkflow(ctx, resumeFraudSignalWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalWorkflowService/ResumeFraudSignalWorkflow")
	}
	pauseReturnLabelWorkflowRequest := &returnsv1.PauseReturnLabelWorkflowRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				Region:      req.GetCampaign().GetLocation().GetRegion(),
				CountryCode: req.GetCampaign().GetLocation().GetCountryCode(),
			},
		},
		Details: &returnsv1.PauseReturnLabelWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotify().GetPhone(),
				Name:  req.GetDetails().GetNotify().GetName(),
			},
		},
	}
	pauseReturnLabelWorkflowReply, err := pauseReturnLabelWorkflowCall(ctx, pauseReturnLabelWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelWorkflowService/PauseReturnLabelWorkflow")
	}
	return &promotionsv1.RetryCampaignWorkflowResponse{
		Outcome: &promotionsv1.RetryCampaignWorkflowResponse_Outcome{
			Status:      resumeFraudSignalWorkflowReply.GetOutcome().GetStatus(),
			ReferenceId: pauseReturnLabelWorkflowReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			Message: resumeFraudSignalWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Campaign: &promotionsv1.Campaign{
			Audit: &commonv1.AuditInfo{
				CreatedAt: pauseReturnLabelWorkflowReply.GetReturnLabel().GetAudit().GetCreatedAt(),
			},
			CampaignId: req.GetCampaignId(),
			Title:      req.GetCampaign().GetTitle(),
		},
	}, nil
}

// PauseCampaignWorkflow pauses a campaign.
func (s *CampaignWorkflowServer) PauseCampaignWorkflow(ctx context.Context, req *promotionsv1.PauseCampaignWorkflowRequest) (*promotionsv1.PauseCampaignWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseCampaignWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "pause campaign workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetCampaign())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertUserReply, err := s.userHistoryClient.RevertUser(ctx, &identityv1.RevertUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient: req.GetCampaign().GetLocation().GetRecipient(),
				Region:    req.GetCampaign().GetLocation().GetRegion(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetCampaign().GetAudit().GetCreatedAt()},
		},
		Details: &identityv1.RevertUserRequest_Details{Snapshot: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RevertUser")
	}
	snapshot2, err := protojson.Marshal(req.GetCampaign())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushCampaignRequest := &promotionsv1.PushCampaignRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{
					Email:            req.GetDetails().GetNotify().GetEmail(),
					PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				},
			}},
			Recurrence: &promotionsv1.Campaign_Weekly{Weekly: &promotionsv1.CampaignWeeklyRule{Interval: req.GetCampaign().GetDaily().GetInterval()}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: revertUserReply.GetUser().GetAudit().GetCreatedBy(),
			},
		},
		Details: &promotionsv1.PushCampaignRequest_Details{Payload: snapshot2},
	}
	pushCampaignReply, err := grpcx.Call(ctx, s.campaignSyncClient.PushCampaign, pushCampaignRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/PushCampaign")
	}
	return &promotionsv1.PauseCampaignWorkflowResponse{
		Campaign: &promotionsv1.Campaign{
			Audit: &commonv1.AuditInfo{
				CreatedBy: revertUserReply.GetUser().GetAudit().GetCreatedBy(),
			},
			CampaignId: pushCampaignReply.GetCampaign().GetCampaignId(),
			Title:      req.GetCampaign().GetTitle(),
			Organizer:  req.GetCampaign().GetOrganizer(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeCampaignWorkflow resumes a campaign.
func (s *CampaignWorkflowServer) ResumeCampaignWorkflow(ctx context.Context, req *promotionsv1.ResumeCampaignWorkflowRequest) (*promotionsv1.ResumeCampaignWorkflowResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "resume campaign workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *identityv1.ValidateApiCredentialRequest, ...grpc.CallOption) (*identityv1.ValidateApiCredentialResponse, error){
		"primary":  s.apiCredentialValidationClient.ValidateApiCredential,
		"fallback": s.apiCredentialValidationClientFallback.ValidateApiCredential,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	ctx = tenancy.With(ctx, "updated_by", req.GetCampaign().GetAudit().GetUpdatedBy())
	ctx = tenancy.With(ctx, "etag", req.GetCampaign().GetAudit().GetEtag())
	validateApiCredentialRequest := &identityv1.ValidateApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: tenancy.Value(ctx, "updated_by"),
				Etag:      tenancy.Value(ctx, "etag"),
			},
		},
		Details: &identityv1.ValidateApiCredentialRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
	}
	validateApiCredentialReply, err := route(ctx, validateApiCredentialRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/ValidateApiCredential")
	}
	browseNotificationsRequest := newBrowseNotificationsRequest(withBrowseNotificationsContext(req.GetContext()), withBrowseNotificationsFilter(&notificationsv1.BrowseNotificationsRequest_Filter{
		Labels:       validateApiCredentialReply.GetApiCredential().GetLabels(),
		Region:       req.GetCampaign().GetLocation().GetRegion(),
		CreatedAfter: validateApiCredentialReply.GetApiCredential().GetAudit().GetUpdatedAt(),
	}))
	browseNotificationsReply, err := s.notificationQueryClient.BrowseNotifications(ctx, browseNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/BrowseNotifications")
	}
	return &promotionsv1.ResumeCampaignWorkflowResponse{
		Outcome: &promotionsv1.ResumeCampaignWorkflowResponse_Outcome{
			Message: validateApiCredentialReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: browseNotificationsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Status:     req.GetCampaign().GetStatus(),
		},
	}, nil
}

// InspectCampaignWorkflow returns a campaign.
func (s *CampaignWorkflowServer) InspectCampaignWorkflow(ctx context.Context, req *promotionsv1.InspectCampaignWorkflowRequest) (*promotionsv1.InspectCampaignWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "inspect_campaign_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect campaign workflow", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	submitInspectionRequest := &returnsv1.SubmitInspectionRequest{
		Context: req.GetContext(),
		Details: &returnsv1.SubmitInspectionRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: tenancy.Value(ctx, "user_id"),
			},
		},
	}
	var submitInspectionReply *returnsv1.SubmitInspectionResponse
	if err := pipeline.Run(ctx, "acme.returns.v1.InspectionApprovalService/SubmitInspection", func(ctx context.Context) error {
		answer, err := s.inspectionApprovalClient.SubmitInspection(ctx, submitInspectionRequest)
		submitInspectionReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionApprovalService/SubmitInspection")
	}
	return &promotionsv1.InspectCampaignWorkflowResponse{
		Campaign: &promotionsv1.Campaign{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: submitInspectionReply.GetInspection().GetAudit().GetUpdatedBy(),
			},
			CampaignId: req.GetCampaignId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListCampaignWorkflows lists campaigns a page at a time.
func (s *CampaignWorkflowServer) ListCampaignWorkflows(ctx context.Context, req *promotionsv1.ListCampaignWorkflowsRequest) (*promotionsv1.ListCampaignWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list campaign workflows", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	signalFraudSignalWorkflowRequest := &fraudv1.SignalFraudSignalWorkflowRequest{
		Context: req.GetContext(),
		Details: &fraudv1.SignalFraudSignalWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		FraudSignal: &fraudv1.FraudSignal{Detail: packed},
	}
	signalFraudSignalWorkflowReply := new(fraudv1.SignalFraudSignalWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", fraudFraudSignalWorkflowServiceName, "SignalFraudSignalWorkflow"), signalFraudSignalWorkflowRequest, signalFraudSignalWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalWorkflowService/SignalFraudSignalWorkflow")
	}
	return &promotionsv1.ListCampaignWorkflowsResponse{
		Result: &commonv1.OperationResult{
			Details: signalFraudSignalWorkflowReply.GetResult().GetDetails(),
			Message: signalFraudSignalWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// SignalCampaignWorkflow signals a campaign.
func (s *CampaignWorkflowServer) SignalCampaignWorkflow(ctx context.Context, req *promotionsv1.SignalCampaignWorkflowRequest) (*promotionsv1.SignalCampaignWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "signal campaign workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	for _, participant := range req.GetCampaign().GetParticipants() {
		if _, err := s.campaignApprovalClient.ApproveCampaign(ctx, &promotionsv1.ApproveCampaignRequest{
			Context: req.GetContext(),
			Campaign: &promotionsv1.Campaign{
				Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
					Contact: &commonv1.ContactInfo{
						PreferredChannel: participant.GetContact().GetPreferredChannel(),
					},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignApprovalService/ApproveCampaign")
		}
	}
	return &promotionsv1.SignalCampaignWorkflowResponse{
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Status:     req.GetCampaign().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
