package partnerapi

import (
	"context"
	"time"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	edgegatewayv1 "github.com/hounddogai/protomap-test-golang/gen/acme/edgegateway/v1"
	fanportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fanportal/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredAccessPolicy records how long each call to acme.authorization.v1.AccessPolicyService takes.
type meteredAccessPolicy struct {
	authorizationv1.AccessPolicyServiceClient
	recorder *metrics.Recorder
}

func (c meteredAccessPolicy) CreateAccessPolicy(ctx context.Context, request *authorizationv1.CreateAccessPolicyRequest, options ...grpc.CallOption) (*authorizationv1.CreateAccessPolicyResponse, error) {
	defer c.recorder.Observe("acme.authorization.v1.AccessPolicyService/CreateAccessPolicy", time.Now())
	return c.AccessPolicyServiceClient.CreateAccessPolicy(ctx, request, options...)
}

// meteredFanSessionQuery records how long each call to acme.fanportal.v1.FanSessionQueryService takes.
type meteredFanSessionQuery struct {
	fanportalv1.FanSessionQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredFanSessionQuery) CountFanSessions(ctx context.Context, request *fanportalv1.CountFanSessionsRequest, options ...grpc.CallOption) (*fanportalv1.CountFanSessionsResponse, error) {
	defer c.recorder.Observe("acme.fanportal.v1.FanSessionQueryService/CountFanSessions", time.Now())
	return c.FanSessionQueryServiceClient.CountFanSessions(ctx, request, options...)
}

// meteredPartnerIntegrationReporting records how long each call to
// acme.partnerapi.v1.PartnerIntegrationReportingService takes.
type meteredPartnerIntegrationReporting struct {
	partnerapiv1.PartnerIntegrationReportingServiceClient
	recorder *metrics.Recorder
}

func (c meteredPartnerIntegrationReporting) FetchPartnerIntegrationReport(ctx context.Context, request *partnerapiv1.FetchPartnerIntegrationReportRequest, options ...grpc.CallOption) (*partnerapiv1.FetchPartnerIntegrationReportResponse, error) {
	defer c.recorder.Observe("acme.partnerapi.v1.PartnerIntegrationReportingService/FetchPartnerIntegrationReport", time.Now())
	return c.PartnerIntegrationReportingServiceClient.FetchPartnerIntegrationReport(ctx, request, options...)
}

func (c meteredPartnerIntegrationReporting) SharePartnerIntegrationReport(ctx context.Context, request *partnerapiv1.SharePartnerIntegrationReportRequest, options ...grpc.CallOption) (*partnerapiv1.SharePartnerIntegrationReportResponse, error) {
	defer c.recorder.Observe("acme.partnerapi.v1.PartnerIntegrationReportingService/SharePartnerIntegrationReport", time.Now())
	return c.PartnerIntegrationReportingServiceClient.SharePartnerIntegrationReport(ctx, request, options...)
}

// meteredPartnerIntegrationWorkflow records how long each call to acme.partnerapi.v1.PartnerIntegrationWorkflowService
// takes.
type meteredPartnerIntegrationWorkflow struct {
	partnerapiv1.PartnerIntegrationWorkflowServiceClient
	recorder *metrics.Recorder
}

func (c meteredPartnerIntegrationWorkflow) CancelPartnerIntegrationWorkflow(ctx context.Context, request *partnerapiv1.CancelPartnerIntegrationWorkflowRequest, options ...grpc.CallOption) (*partnerapiv1.CancelPartnerIntegrationWorkflowResponse, error) {
	defer c.recorder.Observe("acme.partnerapi.v1.PartnerIntegrationWorkflowService/CancelPartnerIntegrationWorkflow", time.Now())
	return c.PartnerIntegrationWorkflowServiceClient.CancelPartnerIntegrationWorkflow(ctx, request, options...)
}

func (c meteredPartnerIntegrationWorkflow) PausePartnerIntegrationWorkflow(ctx context.Context, request *partnerapiv1.PausePartnerIntegrationWorkflowRequest, options ...grpc.CallOption) (*partnerapiv1.PausePartnerIntegrationWorkflowResponse, error) {
	defer c.recorder.Observe("acme.partnerapi.v1.PartnerIntegrationWorkflowService/PausePartnerIntegrationWorkflow", time.Now())
	return c.PartnerIntegrationWorkflowServiceClient.PausePartnerIntegrationWorkflow(ctx, request, options...)
}

// meteredPartnerOrderApproval records how long each call to acme.partnerapi.v1.PartnerOrderApprovalService takes.
type meteredPartnerOrderApproval struct {
	partnerapiv1.PartnerOrderApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredPartnerOrderApproval) DelegatePartnerOrderApproval(ctx context.Context, request *partnerapiv1.DelegatePartnerOrderApprovalRequest, options ...grpc.CallOption) (*partnerapiv1.DelegatePartnerOrderApprovalResponse, error) {
	defer c.recorder.Observe("acme.partnerapi.v1.PartnerOrderApprovalService/DelegatePartnerOrderApproval", time.Now())
	return c.PartnerOrderApprovalServiceClient.DelegatePartnerOrderApproval(ctx, request, options...)
}

// meteredPartnerOrderReporting records how long each call to acme.partnerapi.v1.PartnerOrderReportingService takes.
type meteredPartnerOrderReporting struct {
	partnerapiv1.PartnerOrderReportingServiceClient
	recorder *metrics.Recorder
}

func (c meteredPartnerOrderReporting) SchedulePartnerOrderReport(ctx context.Context, request *partnerapiv1.SchedulePartnerOrderReportRequest, options ...grpc.CallOption) (*partnerapiv1.SchedulePartnerOrderReportResponse, error) {
	defer c.recorder.Observe("acme.partnerapi.v1.PartnerOrderReportingService/SchedulePartnerOrderReport", time.Now())
	return c.PartnerOrderReportingServiceClient.SchedulePartnerOrderReport(ctx, request, options...)
}

// meteredPartnerOrderWorkflow records how long each call to acme.partnerapi.v1.PartnerOrderWorkflowService takes.
type meteredPartnerOrderWorkflow struct {
	partnerapiv1.PartnerOrderWorkflowServiceClient
	recorder *metrics.Recorder
}

func (c meteredPartnerOrderWorkflow) CancelPartnerOrderWorkflow(ctx context.Context, request *partnerapiv1.CancelPartnerOrderWorkflowRequest, options ...grpc.CallOption) (*partnerapiv1.CancelPartnerOrderWorkflowResponse, error) {
	defer c.recorder.Observe("acme.partnerapi.v1.PartnerOrderWorkflowService/CancelPartnerOrderWorkflow", time.Now())
	return c.PartnerOrderWorkflowServiceClient.CancelPartnerOrderWorkflow(ctx, request, options...)
}

// meteredPartnerWebhookReporting records how long each call to acme.partnerapi.v1.PartnerWebhookReportingService takes.
type meteredPartnerWebhookReporting struct {
	partnerapiv1.PartnerWebhookReportingServiceClient
	recorder *metrics.Recorder
}

func (c meteredPartnerWebhookReporting) SharePartnerWebhookReport(ctx context.Context, request *partnerapiv1.SharePartnerWebhookReportRequest, options ...grpc.CallOption) (*partnerapiv1.SharePartnerWebhookReportResponse, error) {
	defer c.recorder.Observe("acme.partnerapi.v1.PartnerWebhookReportingService/SharePartnerWebhookReport", time.Now())
	return c.PartnerWebhookReportingServiceClient.SharePartnerWebhookReport(ctx, request, options...)
}

// meteredPartnerWebhookValidation records how long each call to acme.partnerapi.v1.PartnerWebhookValidationService
// takes.
type meteredPartnerWebhookValidation struct {
	partnerapiv1.PartnerWebhookValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPartnerWebhookValidation) ExplainPartnerWebhookDecision(ctx context.Context, request *partnerapiv1.ExplainPartnerWebhookDecisionRequest, options ...grpc.CallOption) (*partnerapiv1.ExplainPartnerWebhookDecisionResponse, error) {
	defer c.recorder.Observe("acme.partnerapi.v1.PartnerWebhookValidationService/ExplainPartnerWebhookDecision", time.Now())
	return c.PartnerWebhookValidationServiceClient.ExplainPartnerWebhookDecision(ctx, request, options...)
}

func (c meteredPartnerWebhookValidation) GetPartnerWebhookRules(ctx context.Context, request *partnerapiv1.GetPartnerWebhookRulesRequest, options ...grpc.CallOption) (*partnerapiv1.GetPartnerWebhookRulesResponse, error) {
	defer c.recorder.Observe("acme.partnerapi.v1.PartnerWebhookValidationService/GetPartnerWebhookRules", time.Now())
	return c.PartnerWebhookValidationServiceClient.GetPartnerWebhookRules(ctx, request, options...)
}

// meteredReviewVoteApproval records how long each call to acme.reviews.v1.ReviewVoteApprovalService takes.
type meteredReviewVoteApproval struct {
	reviewsv1.ReviewVoteApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredReviewVoteApproval) DelegateReviewVoteApproval(ctx context.Context, request *reviewsv1.DelegateReviewVoteApprovalRequest, options ...grpc.CallOption) (*reviewsv1.DelegateReviewVoteApprovalResponse, error) {
	defer c.recorder.Observe("acme.reviews.v1.ReviewVoteApprovalService/DelegateReviewVoteApproval", time.Now())
	return c.ReviewVoteApprovalServiceClient.DelegateReviewVoteApproval(ctx, request, options...)
}

// meteredRouteBinding records how long each call to acme.edgegateway.v1.RouteBindingService takes.
type meteredRouteBinding struct {
	edgegatewayv1.RouteBindingServiceClient
	recorder *metrics.Recorder
}

// meteredSellerApproval records how long each call to acme.sellers.v1.SellerApprovalService takes.
type meteredSellerApproval struct {
	sellersv1.SellerApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredSellerApproval) CommentSellerApproval(ctx context.Context, request *sellersv1.CommentSellerApprovalRequest, options ...grpc.CallOption) (*sellersv1.CommentSellerApprovalResponse, error) {
	defer c.recorder.Observe("acme.sellers.v1.SellerApprovalService/CommentSellerApproval", time.Now())
	return c.SellerApprovalServiceClient.CommentSellerApproval(ctx, request, options...)
}

// meteredSellerLifecycle records how long each call to acme.sellers.v1.SellerLifecycleService takes.
type meteredSellerLifecycle struct {
	sellersv1.SellerLifecycleServiceClient
	recorder *metrics.Recorder
}

func (c meteredSellerLifecycle) ExpireSeller(ctx context.Context, request *sellersv1.ExpireSellerRequest, options ...grpc.CallOption) (*sellersv1.ExpireSellerResponse, error) {
	defer c.recorder.Observe("acme.sellers.v1.SellerLifecycleService/ExpireSeller", time.Now())
	return c.SellerLifecycleServiceClient.ExpireSeller(ctx, request, options...)
}

// meteredSellerPayoutScheduleWorkflow records how long each call to acme.sellers.v1.SellerPayoutScheduleWorkflowService
// takes.
type meteredSellerPayoutScheduleWorkflow struct {
	sellersv1.SellerPayoutScheduleWorkflowServiceClient
	recorder *metrics.Recorder
}

// meteredSellerScorecardReporting records how long each call to acme.sellers.v1.SellerScorecardReportingService takes.
type meteredSellerScorecardReporting struct {
	sellersv1.SellerScorecardReportingServiceClient
	recorder *metrics.Recorder
}

func (c meteredSellerScorecardReporting) GenerateSellerScorecardReport(ctx context.Context, request *sellersv1.GenerateSellerScorecardReportRequest, options ...grpc.CallOption) (*sellersv1.GenerateSellerScorecardReportResponse, error) {
	defer c.recorder.Observe("acme.sellers.v1.SellerScorecardReportingService/GenerateSellerScorecardReport", time.Now())
	return c.SellerScorecardReportingServiceClient.GenerateSellerScorecardReport(ctx, request, options...)
}

// meteredSessionHistory records how long each call to acme.identity.v1.SessionHistoryService takes.
type meteredSessionHistory struct {
	identityv1.SessionHistoryServiceClient
	recorder *metrics.Recorder
}

func (c meteredSessionHistory) CompareSessionVersions(ctx context.Context, request *identityv1.CompareSessionVersionsRequest, options ...grpc.CallOption) (*identityv1.CompareSessionVersionsResponse, error) {
	defer c.recorder.Observe("acme.identity.v1.SessionHistoryService/CompareSessionVersions", time.Now())
	return c.SessionHistoryServiceClient.CompareSessionVersions(ctx, request, options...)
}

// meteredUser records how long each call to acme.identity.v1.UserService takes.
type meteredUser struct {
	identityv1.UserServiceClient
	recorder *metrics.Recorder
}
