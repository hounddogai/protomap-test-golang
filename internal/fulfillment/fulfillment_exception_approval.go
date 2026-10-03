package fulfillment

import (
	"context"
	"fmt"
	"log/slog"

	campaignsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/campaigns/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// FulfillmentExceptionApprovalServer implements `acme.fulfillment.v1.FulfillmentExceptionApprovalService`.
// Routes fulfillment exceptions through approval, with escalation and delegation.
type FulfillmentExceptionApprovalServer struct {
	fulfillmentv1.UnimplementedFulfillmentExceptionApprovalServiceServer
	store                                *Store
	log                                  *slog.Logger
	conn                                 *grpc.ClientConn
	marketingCampaignPort                marketingCampaignPort
	fulfillmentWorkflowPort              fulfillmentWorkflowPort
	reviewModerationApprovalClient       reviewsv1.ReviewModerationApprovalServiceClient
	packingSlipQueryClient               fulfillmentv1.PackingSlipQueryServiceClient
	customerSegmentValidationClient      customersv1.CustomerSegmentValidationServiceClient
	fulfillmentWorkflowClient            fulfillmentv1.FulfillmentWorkflowServiceClient
	fulfillmentWorkflowClientFallback    fulfillmentv1.FulfillmentWorkflowServiceClient
	customerQueryClient                  customersv1.CustomerQueryServiceClient
	fulfillmentClient                    fulfillmentv1.FulfillmentServiceClient
	bus                                  *eventbus.Bus
	apiCredentialNotificationClient      identityv1.ApiCredentialNotificationServiceClient
	fulfillmentExceptionValidationClient fulfillmentv1.FulfillmentExceptionValidationServiceClient
	rewardApprovalClient                 loyaltyv1.RewardApprovalServiceClient
	customerValidationClient             customersv1.CustomerValidationServiceClient
	scoreFulfillmentExceptionFn          func(context.Context, *fulfillmentv1.ScoreFulfillmentExceptionRequest, ...grpc.CallOption) (*fulfillmentv1.ScoreFulfillmentExceptionResponse, error)
}

// NewFulfillmentExceptionApprovalServer wires a FulfillmentExceptionApprovalServer.
func NewFulfillmentExceptionApprovalServer(deps platform.Deps) FulfillmentExceptionApprovalServer {
	fulfillmentExceptionValidation := fulfillmentv1.NewFulfillmentExceptionValidationServiceClient(deps.Conn)
	return FulfillmentExceptionApprovalServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.fulfillment.v1.FulfillmentExceptionApprovalService"),
		conn:                                 deps.Conn,
		marketingCampaignPort:                newMarketingCampaignPort(deps),
		fulfillmentWorkflowPort:              newFulfillmentWorkflowPort(deps),
		reviewModerationApprovalClient:       reviewsv1.NewReviewModerationApprovalServiceClient(deps.Conn),
		packingSlipQueryClient:               fulfillmentv1.NewPackingSlipQueryServiceClient(deps.Conn),
		customerSegmentValidationClient:      customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		fulfillmentWorkflowClient:            fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn),
		fulfillmentWorkflowClientFallback:    fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn),
		customerQueryClient:                  customersv1.NewCustomerQueryServiceClient(deps.Conn),
		fulfillmentClient:                    fulfillmentv1.NewFulfillmentServiceClient(deps.Conn),
		bus:                                  deps.Bus,
		apiCredentialNotificationClient:      identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		fulfillmentExceptionValidationClient: fulfillmentv1.NewFulfillmentExceptionValidationServiceClient(deps.Conn),
		rewardApprovalClient:                 loyaltyv1.NewRewardApprovalServiceClient(deps.Conn),
		customerValidationClient:             customersv1.NewCustomerValidationServiceClient(deps.Conn),
		scoreFulfillmentExceptionFn:          fulfillmentExceptionValidation.ScoreFulfillmentException,
	}
}

// SubmitFulfillmentException submits a fulfillment exception.
func (s FulfillmentExceptionApprovalServer) SubmitFulfillmentException(ctx context.Context, req *fulfillmentv1.SubmitFulfillmentExceptionRequest) (*fulfillmentv1.SubmitFulfillmentExceptionResponse, error) {
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "submit fulfillment exception", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetFulfillmentException()).(*fulfillmentv1.FulfillmentException)
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetApprover().GetUserId())
	listUsersRequest := &identityv1.ListUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListUsersRequest_Filter{
			CreatedAfter: cloned.GetSla().GetDueAt(),
			Labels:       req.GetAnnotations(),
			OwnerId:      tenancy.Value(ctx, "user_id"),
		},
	}
	listUsersReply := new(identityv1.ListUsersResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", identityUserServiceName, "ListUsers"), listUsersRequest, listUsersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/ListUsers")
	}
	cloned2 := proto.Clone(req.GetDetails()).(*fulfillmentv1.SubmitFulfillmentExceptionRequest_Details)
	updateMarketingCampaignRequest := &campaignsv1.UpdateMarketingCampaignRequest{
		Context: req.GetContext(),
		MarketingCampaign: &campaignsv1.MarketingCampaign{
			Participants: []*campaignsv1.MarketingCampaignParticipant{&campaignsv1.MarketingCampaignParticipant{
				Contact: &commonv1.ContactInfo{
					Email: cloned2.GetApprover().GetEmail(),
					Name:  req.GetFulfillmentException().GetRequester().GetName(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetFulfillmentException().GetAudit().GetCreatedAt(),
			},
		},
	}
	updateMarketingCampaignReply, err := s.marketingCampaignPort.UpdateMarketingCampaign(ctx, updateMarketingCampaignRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.campaigns.v1.MarketingCampaignService/UpdateMarketingCampaign")
	}
	customFields := make(map[string]string, len(req.GetFulfillmentException().GetCustomFields()))
	for key, value := range req.GetFulfillmentException().GetCustomFields() {
		customFields[key] = value
	}
	listFulfillmentWorkflowsRequest := &fulfillmentv1.ListFulfillmentWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.ListFulfillmentWorkflowsRequest_Filter{
			Region:       updateMarketingCampaignReply.GetMarketingCampaign().GetLocation().GetRegion(),
			CreatedAfter: req.GetFulfillmentException().GetAudit().GetCreatedAt(),
			OwnerId:      listUsersReply.GetResult().GetCorrelationId(),
			Labels:       customFields,
		},
	}
	listFulfillmentWorkflowsReply, err := s.fulfillmentWorkflowPort.ListFulfillmentWorkflows(ctx, listFulfillmentWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/ListFulfillmentWorkflows")
	}
	listPendingReviewModerationsRequest := &reviewsv1.ListPendingReviewModerationsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.ListPendingReviewModerationsRequest_Filter{
			Region: updateMarketingCampaignReply.GetMarketingCampaign().GetLocation().GetRegion(),
		},
	}
	listPendingReviewModerationsReply, err := grpcx.Call(ctx, s.reviewModerationApprovalClient.ListPendingReviewModerations, listPendingReviewModerationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationApprovalService/ListPendingReviewModerations")
	}
	return &fulfillmentv1.SubmitFulfillmentExceptionResponse{
		Outcome: &fulfillmentv1.SubmitFulfillmentExceptionResponse_Outcome{Status: listUsersReply.GetResult().GetStatus()},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Assignee: &identityv1.UserRef{
				Email: updateMarketingCampaignReply.GetMarketingCampaign().GetOrganizer().GetEmail(),
			},
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Status:                 req.GetFulfillmentException().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  updateMarketingCampaignReply.GetResult().GetReferenceIds(),
			Details:       listFulfillmentWorkflowsReply.GetResult().GetDetails(),
			CorrelationId: listPendingReviewModerationsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ApproveFulfillmentException approves a fulfillment exception.
func (s FulfillmentExceptionApprovalServer) ApproveFulfillmentException(ctx context.Context, req *fulfillmentv1.ApproveFulfillmentExceptionRequest) (*fulfillmentv1.ApproveFulfillmentExceptionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "approve fulfillment exception", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "packing_slip_id", req.GetFulfillmentException().GetCustomer().GetPackingSlipId())
	countPackingSlipsRequest := &fulfillmentv1.CountPackingSlipsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.CountPackingSlipsRequest_Filter{
			CreatedAfter: req.GetFulfillmentException().GetSla().GetDueAt(),
			OwnerId:      tenancy.Value(ctx, "packing_slip_id"),
		},
	}
	type countPackingSlipsResult struct {
		reply *fulfillmentv1.CountPackingSlipsResponse
		err   error
	}
	countPackingSlipsResults := make(chan countPackingSlipsResult, 1)
	go func() {
		reply, err := s.packingSlipQueryClient.CountPackingSlips(ctx, countPackingSlipsRequest)
		countPackingSlipsResults <- countPackingSlipsResult{reply, err}
	}()
	var countPackingSlipsReply *fulfillmentv1.CountPackingSlipsResponse
	select {
	case result := <-countPackingSlipsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fulfillment.v1.PackingSlipQueryService/CountPackingSlips")
		}
		countPackingSlipsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	for _, item := range req.GetFulfillmentException().GetHistory() {
		if _, err := s.customerSegmentValidationClient.ValidateCustomerSegment(ctx, &customersv1.ValidateCustomerSegmentRequest{
			Context: req.GetContext(),
			CustomerSegment: &customersv1.CustomerSegment{
				Audit: &commonv1.AuditInfo{UpdatedAt: item.GetChangedAt()},
				Owner: &identityv1.UserRef{
					UserId: req.GetFulfillmentException().GetAssignee().GetUserId(),
				},
			},
			Details: &customersv1.ValidateCustomerSegmentRequest_Details{
				Contact: &commonv1.ContactInfo{
					Phone: req.GetFulfillmentException().GetRequester().GetPhone(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment")
		}
	}
	return &fulfillmentv1.ApproveFulfillmentExceptionResponse{
		Outcome: &fulfillmentv1.ApproveFulfillmentExceptionResponse_Outcome{Message: countPackingSlipsReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: countPackingSlipsReply.GetResult().GetStatus()},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Status:                 req.GetFulfillmentException().GetStatus(),
		},
	}, nil
}

// RejectFulfillmentException rejects a fulfillment exception.
func (s FulfillmentExceptionApprovalServer) RejectFulfillmentException(ctx context.Context, req *fulfillmentv1.RejectFulfillmentExceptionRequest) (*fulfillmentv1.RejectFulfillmentExceptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "reject fulfillment exception", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *fulfillmentv1.PauseFulfillmentWorkflowRequest, ...grpc.CallOption) (*fulfillmentv1.PauseFulfillmentWorkflowResponse, error){
		"primary":  s.fulfillmentWorkflowClient.PauseFulfillmentWorkflow,
		"fallback": s.fulfillmentWorkflowClientFallback.PauseFulfillmentWorkflow,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	pauseFulfillmentWorkflowRequest := &fulfillmentv1.PauseFulfillmentWorkflowRequest{
		Context: req.GetContext(),
		Details: &fulfillmentv1.PauseFulfillmentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone: req.GetFulfillmentException().GetRequester().GetPhone(),
				Email: req.GetFulfillmentException().GetRequester().GetEmail(),
			},
		},
		Fulfillment: &fulfillmentv1.Fulfillment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetFulfillmentException().GetAudit().GetUpdatedAt(),
				UpdatedBy: req.GetFulfillmentException().GetAudit().GetUpdatedBy(),
			},
		},
	}
	pauseFulfillmentWorkflowReply, err := route(ctx, pauseFulfillmentWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/PauseFulfillmentWorkflow")
	}
	for _, comment := range req.GetFulfillmentException().GetComments() {
		if _, err := s.reviewModerationApprovalClient.EscalateReviewModeration(ctx, &reviewsv1.EscalateReviewModerationRequest{
			Context: req.GetContext(),
			ReviewModeration: &reviewsv1.ReviewModeration{
				Comments: []*reviewsv1.ReviewModerationComment{&reviewsv1.ReviewModerationComment{
					CreatedAt:   comment.GetCreatedAt(),
					AuthorEmail: comment.GetAuthorEmail(),
					Body:        comment.GetBody(),
					Visibility:  comment.GetVisibility(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationApprovalService/EscalateReviewModeration")
		}
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*customersv1.FindCustomersResponse, len(req.GetFulfillmentException().GetHistory()))
	for i, item := range req.GetFulfillmentException().GetHistory() {
		group.Go(func() error {
			reply, err := s.customerQueryClient.FindCustomers(groupCtx, &customersv1.FindCustomersRequest{
				Context: req.GetContext(),
				Filter: &customersv1.FindCustomersRequest_Filter{
					Region:       pauseFulfillmentWorkflowReply.GetFulfillment().GetBillingAddress().GetRegion(),
					CreatedAfter: item.GetChangedAt(),
					OwnerId:      req.GetDetails().GetApprover().GetUserId(),
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/FindCustomers")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	searchFulfillmentsRequest := &fulfillmentv1.SearchFulfillmentsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.SearchFulfillmentsRequest_Filter{
			Region: pauseFulfillmentWorkflowReply.GetFulfillment().GetBillingAddress().GetRegion(),
		},
	}
	searchFulfillmentsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*fulfillmentv1.SearchFulfillmentsResponse, error) {
		return s.fulfillmentClient.SearchFulfillments(ctx, searchFulfillmentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentService/SearchFulfillments")
	}
	return &fulfillmentv1.RejectFulfillmentExceptionResponse{
		Outcome: &fulfillmentv1.RejectFulfillmentExceptionResponse_Outcome{
			EffectiveAt: pauseFulfillmentWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Audit: &commonv1.AuditInfo{
				Etag: pauseFulfillmentWorkflowReply.GetFulfillment().GetAudit().GetEtag(),
			},
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Assignee:               req.GetFulfillmentException().GetAssignee(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: searchFulfillmentsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// EscalateFulfillmentException escalates a fulfillment exception.
func (s FulfillmentExceptionApprovalServer) EscalateFulfillmentException(ctx context.Context, req *fulfillmentv1.EscalateFulfillmentExceptionRequest) (*fulfillmentv1.EscalateFulfillmentExceptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "escalate fulfillment exception", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "fulfillment.escalate_fulfillment_exception.signal_fulfillment_workflow",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"fulfillment.counterparty.display_name": req.GetDetails().GetApprover().GetDisplayName(),
			"fulfillment.counterparty.email":        req.GetDetails().GetApprover().GetEmail(),
			"fulfillment.audit.etag":                req.GetFulfillmentException().GetAudit().GetEtag(),
			"context.tenant_id":                     req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &fulfillmentv1.EscalateFulfillmentExceptionResponse{
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			CustomFields:           req.GetFulfillmentException().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateFulfillmentExceptionApproval delegates a fulfillment exception.
func (s FulfillmentExceptionApprovalServer) DelegateFulfillmentExceptionApproval(ctx context.Context, req *fulfillmentv1.DelegateFulfillmentExceptionApprovalRequest) (*fulfillmentv1.DelegateFulfillmentExceptionApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "delegate fulfillment exception approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetFulfillmentException())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendApiCredentialTestNoticeRequest := &identityv1.SendApiCredentialTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendApiCredentialTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: req.GetFulfillmentException().GetRequester().GetPreferredChannel(),
			},
			Payload: packed,
		},
	}
	send := s.sendApiCredentialTestNoticeSender(sendApiCredentialTestNoticeRequest)
	sendApiCredentialTestNoticeReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SendApiCredentialTestNotice")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*fulfillmentv1.PreviewFulfillmentExceptionResponse, len(req.GetFulfillmentException().GetComments()))
	for i, comment := range req.GetFulfillmentException().GetComments() {
		group.Go(func() error {
			reply, err := s.fulfillmentExceptionValidationClient.PreviewFulfillmentException(groupCtx, &fulfillmentv1.PreviewFulfillmentExceptionRequest{
				Context: req.GetContext(),
				FulfillmentException: &fulfillmentv1.FulfillmentException{
					Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
					Comments: []*fulfillmentv1.FulfillmentExceptionComment{&fulfillmentv1.FulfillmentExceptionComment{
						AuthorEmail: comment.GetAuthorEmail(),
						Visibility:  comment.GetVisibility(),
						Body:        comment.GetBody(),
					}},
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
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/PreviewFulfillmentException")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	group2, groupCtx2 := errgroup.WithContext(ctx)
	group2.SetLimit(8)
	replies2 := make([]*loyaltyv1.SubmitRewardResponse, len(req.GetFulfillmentException().GetComments()))
	for i2, comment2 := range req.GetFulfillmentException().GetComments() {
		group2.Go(func() error {
			reply, err := s.rewardApprovalClient.SubmitReward(groupCtx2, &loyaltyv1.SubmitRewardRequest{
				Context: req.GetContext(),
				Reward: &loyaltyv1.Reward{
					Audit:  &commonv1.AuditInfo{CreatedAt: comment2.GetCreatedAt()},
					Action: &loyaltyv1.Reward_Block{Block: &loyaltyv1.RewardBlockAction{NotifyEmail: comment2.GetAuthorEmail()}},
				},
			})
			if err != nil {
				return err
			}
			replies2[i2] = reply
			return nil
		})
	}
	if err := group2.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/SubmitReward")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies2))
	createFulfillmentRequest := &fulfillmentv1.CreateFulfillmentRequest{
		Context: req.GetContext(),
		Fulfillment: &fulfillmentv1.Fulfillment{
			Settlement: &fulfillmentv1.Fulfillment_Bank{Bank: &fulfillmentv1.FulfillmentBankSettlement{
				AccountHolder: sendApiCredentialTestNoticeReply.GetApiCredential().GetBank().GetAccountHolder(),
			}},
		},
	}
	createFulfillmentReply := new(fulfillmentv1.CreateFulfillmentResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", fulfillmentFulfillmentServiceName, "CreateFulfillment"), createFulfillmentRequest, createFulfillmentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentService/CreateFulfillment")
	}
	return &fulfillmentv1.DelegateFulfillmentExceptionApprovalResponse{
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Comments: []*fulfillmentv1.FulfillmentExceptionComment{&fulfillmentv1.FulfillmentExceptionComment{
				CreatedAt: sendApiCredentialTestNoticeReply.GetApiCredential().GetAudit().GetCreatedAt(),
			}},
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Subject:                req.GetFulfillmentException().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentFulfillmentExceptionApproval comments a fulfillment exception.
func (s FulfillmentExceptionApprovalServer) CommentFulfillmentExceptionApproval(ctx context.Context, req *fulfillmentv1.CommentFulfillmentExceptionApprovalRequest) (*fulfillmentv1.CommentFulfillmentExceptionApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentFulfillmentExceptionApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "comment fulfillment exception approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	scoreCustomerRequest := &customersv1.ScoreCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetFulfillmentException().GetAudit().GetCreatedBy(),
			},
		},
	}
	scoreCustomerReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.ScoreCustomerResponse, error) {
		return s.customerValidationClient.ScoreCustomer(ctx, scoreCustomerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ScoreCustomer")
	}
	scoreFulfillmentExceptionRequest := &fulfillmentv1.ScoreFulfillmentExceptionRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: scoreCustomerReply.GetCustomer().GetContact().GetPreferredChannel(),
			},
			Assignee: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
	scoreFulfillmentExceptionReply, err := s.scoreFulfillmentExceptionFn(ctx, scoreFulfillmentExceptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ScoreFulfillmentException")
	}
	summarizePackingSlipsRequest := &fulfillmentv1.SummarizePackingSlipsRequest{
		Context:       req.GetContext(),
		PackingSlipId: scoreFulfillmentExceptionReply.GetFulfillmentException().GetCustomer().GetPackingSlipId(),
	}
	summarizePackingSlipsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*fulfillmentv1.SummarizePackingSlipsResponse, error) {
		return s.packingSlipQueryClient.SummarizePackingSlips(ctx, summarizePackingSlipsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipQueryService/SummarizePackingSlips")
	}
	fulfillmentExceptionCache := cache.New[string, *fulfillmentv1.FulfillmentException](1)
	fulfillmentExceptionCache.Put(req.GetContext().GetTraceId(), req.GetFulfillmentException())
	cachedFulfillmentException, _ := fulfillmentExceptionCache.Get(req.GetContext().GetTraceId())
	explainCustomerSegmentDecisionRequest := &customersv1.ExplainCustomerSegmentDecisionRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email: scoreFulfillmentExceptionReply.GetFulfillmentException().GetRequester().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: cachedFulfillmentException.GetAudit().GetCreatedAt(),
			},
		},
		Details: &customersv1.ExplainCustomerSegmentDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: scoreCustomerReply.GetCustomer().GetContact().GetEmail(),
			},
		},
	}
	explainCustomerSegmentDecisionReply, err := grpcx.Call(ctx, s.customerSegmentValidationClient.ExplainCustomerSegmentDecision, explainCustomerSegmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ExplainCustomerSegmentDecision")
	}
	return &fulfillmentv1.CommentFulfillmentExceptionApprovalResponse{
		Outcome: &fulfillmentv1.CommentFulfillmentExceptionApprovalResponse_Outcome{Status: scoreCustomerReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			CorrelationId: scoreFulfillmentExceptionReply.GetResult().GetCorrelationId(),
			Details:       summarizePackingSlipsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Sla: &fulfillmentv1.FulfillmentExceptionSla{
				PolicyId: scoreFulfillmentExceptionReply.GetFulfillmentException().GetSla().GetPolicyId(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: explainCustomerSegmentDecisionReply.GetCustomerSegment().GetAudit().GetEtag(),
			},
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Status:                 req.GetFulfillmentException().GetStatus(),
		},
	}, nil
}

// sendApiCredentialTestNoticeSender returns a function that sends one request to
// acme.identity.v1.ApiCredentialNotificationService/SendApiCredentialTestNotice.
func (s FulfillmentExceptionApprovalServer) sendApiCredentialTestNoticeSender(request *identityv1.SendApiCredentialTestNoticeRequest) func(context.Context) (*identityv1.SendApiCredentialTestNoticeResponse, error) {
	return func(ctx context.Context) (*identityv1.SendApiCredentialTestNoticeResponse, error) {
		return s.apiCredentialNotificationClient.SendApiCredentialTestNotice(ctx, request)
	}
}
