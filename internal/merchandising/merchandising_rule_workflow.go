package merchandising

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	accountingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/accounting/v1"
	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	secretsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/secrets/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// MerchandisingRuleWorkflowServer implements `acme.merchandising.v1.MerchandisingRuleWorkflowService`.
// Runs the long-lived workflow that takes a merchandising rule from start to finish.
type MerchandisingRuleWorkflowServer struct {
	merchandisingv1.UnimplementedMerchandisingRuleWorkflowServiceServer
	store                               *Store
	log                                 *slog.Logger
	merchandisingRuleApprovalClient     merchandisingv1.MerchandisingRuleApprovalServiceClient
	sellerPayoutScheduleReportingClient sellersv1.SellerPayoutScheduleReportingServiceClient
	accessPolicyClient                  authorizationv1.AccessPolicyServiceClient
	collectionApprovalClient            merchandisingv1.CollectionApprovalServiceClient
	conn                                *grpc.ClientConn
	collectionWorkflowClient            merchandisingv1.CollectionWorkflowServiceClient
	bus                                 *eventbus.Bus
	clients                             *registry.Registry
	synonymNotificationClient           searchv1.SynonymNotificationServiceClient
	chartOfAccountsClient               accountingv1.ChartOfAccountsServiceClient
	placementWorkflowClient             merchandisingv1.PlacementWorkflowServiceClient
	auditEventClient                    auditv1.AuditEventServiceClient
	secretClient                        secretsv1.SecretServiceClient
	collectionClient                    merchandisingv1.CollectionServiceClient
	placementValidationClient           merchandisingv1.PlacementValidationServiceClient
	sellerListingWorkflowClient         sellersv1.SellerListingWorkflowServiceClient
}

// NewMerchandisingRuleWorkflowServer wires a MerchandisingRuleWorkflowServer.
func NewMerchandisingRuleWorkflowServer(deps platform.Deps) *MerchandisingRuleWorkflowServer {
	return &MerchandisingRuleWorkflowServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.merchandising.v1.MerchandisingRuleWorkflowService"),
		merchandisingRuleApprovalClient:     merchandisingv1.NewMerchandisingRuleApprovalServiceClient(deps.Conn),
		sellerPayoutScheduleReportingClient: sellersv1.NewSellerPayoutScheduleReportingServiceClient(deps.Conn),
		accessPolicyClient:                  authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		collectionApprovalClient:            merchandisingv1.NewCollectionApprovalServiceClient(deps.Conn),
		conn:                                deps.Conn,
		collectionWorkflowClient:            merchandisingv1.NewCollectionWorkflowServiceClient(deps.Conn),
		bus:                                 deps.Bus,
		clients:                             deps.Clients,
		synonymNotificationClient:           searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		chartOfAccountsClient:               accountingv1.NewChartOfAccountsServiceClient(deps.Conn),
		placementWorkflowClient:             merchandisingv1.NewPlacementWorkflowServiceClient(deps.Conn),
		auditEventClient:                    auditv1.NewAuditEventServiceClient(deps.Conn),
		secretClient:                        secretsv1.NewSecretServiceClient(deps.Conn),
		collectionClient:                    merchandisingv1.NewCollectionServiceClient(deps.Conn),
		placementValidationClient:           merchandisingv1.NewPlacementValidationServiceClient(deps.Conn),
		sellerListingWorkflowClient:         sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn),
	}
}

// StartMerchandisingRuleWorkflow starts a merchandising rule.
func (s *MerchandisingRuleWorkflowServer) StartMerchandisingRuleWorkflow(ctx context.Context, req *merchandisingv1.StartMerchandisingRuleWorkflowRequest) (*merchandisingv1.StartMerchandisingRuleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StartMerchandisingRuleWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "start merchandising rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	jobs := make(chan *merchandisingv1.MerchandisingRuleCondition)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for condition := range jobs {
				if _, err := s.merchandisingRuleApprovalClient.EscalateMerchandisingRule(workCtx, &merchandisingv1.EscalateMerchandisingRuleRequest{
					Context: req.GetContext(),
					MerchandisingRule: &merchandisingv1.MerchandisingRule{
						Conditions: []*merchandisingv1.MerchandisingRuleCondition{&merchandisingv1.MerchandisingRuleCondition{
							Negate:    condition.GetNegate(),
							Attribute: condition.GetAttribute(),
						}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetMerchandisingRule().GetConditions() {
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
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleApprovalService/EscalateMerchandisingRule")
	}
	packed, err := anypb.New(req.GetMerchandisingRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareSellerPayoutScheduleReportReply, err := s.sellerPayoutScheduleReportingClient.ShareSellerPayoutScheduleReport(ctx, &sellersv1.ShareSellerPayoutScheduleReportRequest{
		Context: req.GetContext(),
		Details: &sellersv1.ShareSellerPayoutScheduleReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
			Payload:   packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/ShareSellerPayoutScheduleReport")
	}
	return &merchandisingv1.StartMerchandisingRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: shareSellerPayoutScheduleReportReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				CreatedAt: shareSellerPayoutScheduleReportReply.GetSellerPayoutSchedule().GetAudit().GetCreatedAt(),
			},
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Effective:           req.GetMerchandisingRule().GetEffective(),
		},
	}, nil
}

// AdvanceMerchandisingRuleWorkflow advances a merchandising rule.
func (s *MerchandisingRuleWorkflowServer) AdvanceMerchandisingRuleWorkflow(ctx context.Context, req *merchandisingv1.AdvanceMerchandisingRuleWorkflowRequest) (*merchandisingv1.AdvanceMerchandisingRuleWorkflowResponse, error) {
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "advance merchandising rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	var reason string
	switch choice := req.GetMerchandisingRule().GetAction().(type) {
	case *merchandisingv1.MerchandisingRule_Route:
		reason = choice.Route.GetTarget()
	case *merchandisingv1.MerchandisingRule_Block:
		reason = choice.Block.GetReason()
	}
	updateAccessPolicyRequest := newUpdateAccessPolicyRequest(withUpdateAccessPolicyContext(req.GetContext()), withUpdateAccessPolicyAccessPolicy(&authorizationv1.AccessPolicy{
		Action: &authorizationv1.AccessPolicy_Block{Block: &authorizationv1.AccessPolicyBlockAction{Reason: reason}},
	}))
	updateAccessPolicyReply, err := s.accessPolicyClient.UpdateAccessPolicy(ctx, updateAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/UpdateAccessPolicy")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := merchandisingRuleWorkflowServerApproveCollectionSteps[mode]
	approveCollectionRequest := &merchandisingv1.ApproveCollectionRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Manager: &identityv1.UserRef{
						DisplayName: req.GetMerchandisingRule().GetOwner().GetDisplayName(),
					},
				}},
			}},
		},
	}
	approveCollectionReply, err := run(s, ctx, approveCollectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionApprovalService/ApproveCollection")
	}
	startPlacementWorkflowRequest := &merchandisingv1.StartPlacementWorkflowRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					Email: updateAccessPolicyReply.GetAccessPolicy().GetOwner().GetEmail(),
				},
			}},
			Location: &commonv1.PostalAddress{
				Region: approveCollectionReply.GetCollection().GetLocation().GetRegion(),
			},
		},
	}
	startPlacementWorkflowReply := new(merchandisingv1.StartPlacementWorkflowResponse)
	if err := s.conn.Invoke(ctx, merchandisingPlacementWorkflowServiceStartPlacementWorkflowMethod, startPlacementWorkflowRequest, startPlacementWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/StartPlacementWorkflow")
	}
	return &merchandisingv1.AdvanceMerchandisingRuleWorkflowResponse{
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Description: updateAccessPolicyReply.GetAccessPolicy().GetDescription(),
			Audit: &commonv1.AuditInfo{
				UpdatedBy: startPlacementWorkflowReply.GetPlacement().GetAudit().GetUpdatedBy(),
				CreatedAt: startPlacementWorkflowReply.GetPlacement().GetAudit().GetCreatedAt(),
			},
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Owner:               req.GetMerchandisingRule().GetOwner(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: approveCollectionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// CancelMerchandisingRuleWorkflow cancels a merchandising rule.
func (s *MerchandisingRuleWorkflowServer) CancelMerchandisingRuleWorkflow(ctx context.Context, req *merchandisingv1.CancelMerchandisingRuleWorkflowRequest) (*merchandisingv1.CancelMerchandisingRuleWorkflowResponse, error) {
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "cancel merchandising rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	listCollectionWorkflowsRequest := &merchandisingv1.ListCollectionWorkflowsRequest{}
	listCollectionWorkflowsRequest.Context = req.GetContext()
	listCollectionWorkflowsRequest.Filter = &merchandisingv1.ListCollectionWorkflowsRequest_Filter{
		CreatedAfter: req.GetMerchandisingRule().GetAudit().GetCreatedAt(),
	}
	listCollectionWorkflowsReply, err := s.collectionWorkflowClient.ListCollectionWorkflows(ctx, listCollectionWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/ListCollectionWorkflows")
	}
	signalSellerWorkflowRequest := &sellersv1.SignalSellerWorkflowRequest{
		Context: req.GetContext(),
		Details: &sellersv1.SignalSellerWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{Email: req.GetMerchandisingRule().GetOwner().GetEmail()},
			Contact:             &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetMerchandisingRule().GetAudit().GetCreatedBy(),
			},
		},
	}
	signalSellerWorkflowReply := new(sellersv1.SignalSellerWorkflowResponse)
	if err := grpcx.Invoke(ctx, s.conn, sellersv1.SellerWorkflowService_SignalSellerWorkflow_FullMethodName, signalSellerWorkflowRequest, signalSellerWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerWorkflowService/SignalSellerWorkflow")
	}
	previewPlacementRequest := &merchandisingv1.PreviewPlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					Email:            req.GetMerchandisingRule().GetOwner().GetEmail(),
					PreferredChannel: signalSellerWorkflowReply.GetSeller().GetContact().GetPreferredChannel(),
				},
			}},
		},
	}
	previewPlacementReply := new(merchandisingv1.PreviewPlacementResponse)
	if err := grpcx.Invoke(ctx, s.conn, merchandisingv1.PlacementValidationService_PreviewPlacement_FullMethodName, previewPlacementRequest, previewPlacementReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/PreviewPlacement")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "merchandising.cancel_merchandising_rule_workflow.clone_collection",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"collection.children.children.location.locality":    signalSellerWorkflowReply.GetSeller().GetPrimaryAddress().GetLocality(),
			"collection.children.children.location.postal_code": signalSellerWorkflowReply.GetSeller().GetPrimaryAddress().GetPostalCode(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &merchandisingv1.CancelMerchandisingRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listCollectionWorkflowsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  previewPlacementReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Outcome: &merchandisingv1.CancelMerchandisingRuleWorkflowResponse_Outcome{
			ReferenceId: signalSellerWorkflowReply.GetOutcome().GetReferenceId(),
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				Etag: signalSellerWorkflowReply.GetSeller().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				Email: previewPlacementReply.GetPlacement().GetOrganizer().GetEmail(),
			},
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Priority:            req.GetMerchandisingRule().GetPriority(),
		},
	}, nil
}

// RetryMerchandisingRuleWorkflow retrys a merchandising rule.
func (s *MerchandisingRuleWorkflowServer) RetryMerchandisingRuleWorkflow(ctx context.Context, req *merchandisingv1.RetryMerchandisingRuleWorkflowRequest) (*merchandisingv1.RetryMerchandisingRuleWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "retry merchandising rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	sessionQuery, ok := s.clients.Get("acme.identity.v1.SessionQueryService").(identityv1.SessionQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.identity.v1.SessionQueryService")
	}
	parameters := make(map[string]string, len(req.GetMerchandisingRule().GetParameters()))
	for key, value := range req.GetMerchandisingRule().GetParameters() {
		parameters[key] = value
	}
	suggestSessionsRequest := &identityv1.SuggestSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.SuggestSessionsRequest_Filter{
			CreatedAfter: req.GetMerchandisingRule().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetMerchandisingRule().GetOwner().GetUserId(),
			Labels:       parameters,
		},
	}
	suggestSessionsReply, err := sessionQuery.SuggestSessions(ctx, suggestSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/SuggestSessions")
	}
	streamSynonymNoticesStream, err := s.synonymNotificationClient.StreamSynonymNotices(ctx, &searchv1.StreamSynonymNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone: req.GetDetails().GetNotify().GetPhone(),
			Email: req.GetMerchandisingRule().GetOwner().GetEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/StreamSynonymNotices")
	}
	for {
		received, err := streamSynonymNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/StreamSynonymNotices")
		}
		_ = received
	}
	createChartOfAccountsRequest := &accountingv1.CreateChartOfAccountsRequest{
		Context: req.GetContext(),
		ChartOfAccounts: &accountingv1.ChartOfAccounts{
			Children: []*accountingv1.ChartOfAccounts{&accountingv1.ChartOfAccounts{
				Children: []*accountingv1.ChartOfAccounts{&accountingv1.ChartOfAccounts{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: req.GetMerchandisingRule().GetAudit().GetUpdatedBy(),
						UpdatedAt: req.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
					},
				}},
			}},
		},
	}
	createChartOfAccountsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*accountingv1.CreateChartOfAccountsResponse, error) {
		return s.chartOfAccountsClient.CreateChartOfAccounts(ctx, createChartOfAccountsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.accounting.v1.ChartOfAccountsService/CreateChartOfAccounts")
	}
	return &merchandisingv1.RetryMerchandisingRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  suggestSessionsReply.GetResult().GetReferenceIds(),
			CorrelationId: suggestSessionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				UserId: createChartOfAccountsReply.GetChartOfAccounts().GetManager().GetUserId(),
			},
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Parameters:          req.GetMerchandisingRule().GetParameters(),
		},
	}, nil
}

// PauseMerchandisingRuleWorkflow pauses a merchandising rule.
func (s *MerchandisingRuleWorkflowServer) PauseMerchandisingRuleWorkflow(ctx context.Context, req *merchandisingv1.PauseMerchandisingRuleWorkflowRequest) (*merchandisingv1.PauseMerchandisingRuleWorkflowResponse, error) {
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "pause merchandising rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	resumePlacementWorkflowRequest := &merchandisingv1.ResumePlacementWorkflowRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetMerchandisingRule().GetOwner().GetEmail()},
			}},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetMerchandisingRule().GetAudit().GetEtag(),
				CreatedAt: req.GetMerchandisingRule().GetAudit().GetCreatedAt(),
			},
		},
		Details: &merchandisingv1.ResumePlacementWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
	resumePlacementWorkflowReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*merchandisingv1.ResumePlacementWorkflowResponse, error) {
		return s.placementWorkflowClient.ResumePlacementWorkflow(ctx, resumePlacementWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/ResumePlacementWorkflow")
	}
	merchandisingRuleCache := cache.New[string, *merchandisingv1.MerchandisingRule](1)
	merchandisingRuleCache.Put(req.GetContext().GetTraceId(), req.GetMerchandisingRule())
	cachedMerchandisingRule, _ := merchandisingRuleCache.Get(req.GetContext().GetTraceId())
	watchAuditEventsStream, err := s.auditEventClient.WatchAuditEvents(ctx, &auditv1.WatchAuditEventsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: cachedMerchandisingRule.GetOwner().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/WatchAuditEvents")
	}
	for {
		received, err := watchAuditEventsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/WatchAuditEvents")
		}
		_ = received
	}
	return &merchandisingv1.PauseMerchandisingRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: resumePlacementWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &merchandisingv1.PauseMerchandisingRuleWorkflowResponse_Outcome{
			EffectiveAt: resumePlacementWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Version:             req.GetMerchandisingRule().GetVersion(),
		},
	}, nil
}

// ResumeMerchandisingRuleWorkflow resumes a merchandising rule.
func (s *MerchandisingRuleWorkflowServer) ResumeMerchandisingRuleWorkflow(ctx context.Context, req *merchandisingv1.ResumeMerchandisingRuleWorkflowRequest) (*merchandisingv1.ResumeMerchandisingRuleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "resume merchandising rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetMerchandisingRule()).(*merchandisingv1.MerchandisingRule)
	listSecretsReply, err := s.secretClient.ListSecrets(ctx, &secretsv1.ListSecretsRequest{
		Context: req.GetContext(),
		Filter: &secretsv1.ListSecretsRequest_Filter{
			CreatedAfter: cloned.GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetMerchandisingRule().GetOwner().GetUserId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.secrets.v1.SecretService/ListSecrets")
	}
	return &merchandisingv1.ResumeMerchandisingRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listSecretsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Name:                req.GetMerchandisingRule().GetName(),
		},
	}, nil
}

// InspectMerchandisingRuleWorkflow returns a merchandising rule.
func (s *MerchandisingRuleWorkflowServer) InspectMerchandisingRuleWorkflow(ctx context.Context, req *merchandisingv1.InspectMerchandisingRuleWorkflowRequest) (*merchandisingv1.InspectMerchandisingRuleWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "merchandising_rule", req.GetMerchandisingRuleId(), "inspect_merchandising_rule_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect merchandising rule workflow", "tenant", req.GetContext().GetTenantId())
	return &merchandisingv1.InspectMerchandisingRuleWorkflowResponse{
		MerchandisingRule: &merchandisingv1.MerchandisingRule{MerchandisingRuleId: req.GetMerchandisingRuleId()},
		Result:            &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListMerchandisingRuleWorkflows lists merchandising rules a page at a time.
func (s *MerchandisingRuleWorkflowServer) ListMerchandisingRuleWorkflows(ctx context.Context, req *merchandisingv1.ListMerchandisingRuleWorkflowsRequest) (*merchandisingv1.ListMerchandisingRuleWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list merchandising rule workflows", "tenant", req.GetContext().GetTenantId())
	recallGiftCardRequest := &giftcardsv1.RecallGiftCardRequest{
		Context:  req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{Labels: req.GetFilter().GetLabels()},
	}
	recallGiftCardReply, err := recallGiftCardCall(ctx, recallGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/RecallGiftCard")
	}
	batchGetCollectionsRequest := &merchandisingv1.BatchGetCollectionsRequest{}
	batchGetCollectionsRequest.Context = req.GetContext()
	batchGetCollectionsReply, err := s.collectionClient.BatchGetCollections(ctx, batchGetCollectionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionService/BatchGetCollections")
	}
	return &merchandisingv1.ListMerchandisingRuleWorkflowsResponse{
		MerchandisingRules: []*merchandisingv1.MerchandisingRule{&merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recallGiftCardReply.GetGiftCard().GetAudit().GetUpdatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: batchGetCollectionsReply.GetResult().GetStatus()},
	}, nil
}

// SignalMerchandisingRuleWorkflow signals a merchandising rule.
func (s *MerchandisingRuleWorkflowServer) SignalMerchandisingRuleWorkflow(ctx context.Context, req *merchandisingv1.SignalMerchandisingRuleWorkflowRequest) (*merchandisingv1.SignalMerchandisingRuleWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "signal merchandising rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	lintPlacementsRequest := &merchandisingv1.LintPlacementsRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
			}},
		},
	}
	lintPlacementsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*merchandisingv1.LintPlacementsResponse, error) {
		return s.placementValidationClient.LintPlacements(ctx, lintPlacementsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/LintPlacements")
	}
	pauseSellerListingWorkflowRequest := &sellersv1.PauseSellerListingWorkflowRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{Etag: req.GetMerchandisingRule().GetAudit().GetEtag()},
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				Lines: lintPlacementsReply.GetPlacement().GetLocation().GetLines(),
			}},
			Owner: &identityv1.UserRef{
				DisplayName: lintPlacementsReply.GetPlacement().GetOrganizer().GetDisplayName(),
			},
		},
		Details: &sellersv1.PauseSellerListingWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: lintPlacementsReply.GetPlacement().GetOrganizer().GetEmail(),
			},
		},
	}
	pauseSellerListingWorkflowReply, err := grpcx.Call(ctx, s.sellerListingWorkflowClient.PauseSellerListingWorkflow, pauseSellerListingWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/PauseSellerListingWorkflow")
	}
	return &merchandisingv1.SignalMerchandisingRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: lintPlacementsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &merchandisingv1.SignalMerchandisingRuleWorkflowResponse_Outcome{
			Message: pauseSellerListingWorkflowReply.GetResult().GetMessage(),
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: pauseSellerListingWorkflowReply.GetSellerListing().GetAudit().GetUpdatedBy(),
			},
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Effective:           req.GetMerchandisingRule().GetEffective(),
		},
	}, nil
}

// merchandisingRuleWorkflowServerApproveCollectionSteps are the ways to run
// acme.merchandising.v1.CollectionApprovalService/ApproveCollection, by name.
var merchandisingRuleWorkflowServerApproveCollectionSteps = map[string]func(*MerchandisingRuleWorkflowServer, context.Context, *merchandisingv1.ApproveCollectionRequest) (*merchandisingv1.ApproveCollectionResponse, error){
	"default": (*MerchandisingRuleWorkflowServer).approveCollectionStep,
	"audited": (*MerchandisingRuleWorkflowServer).approveCollectionAuditedStep,
}

func (s *MerchandisingRuleWorkflowServer) approveCollectionStep(ctx context.Context, request *merchandisingv1.ApproveCollectionRequest) (*merchandisingv1.ApproveCollectionResponse, error) {
	return s.collectionApprovalClient.ApproveCollection(ctx, request)
}

func (s *MerchandisingRuleWorkflowServer) approveCollectionAuditedStep(ctx context.Context, request *merchandisingv1.ApproveCollectionRequest) (*merchandisingv1.ApproveCollectionResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.merchandising.v1.CollectionApprovalService/ApproveCollection")
	return s.collectionApprovalClient.ApproveCollection(ctx, request)
}
