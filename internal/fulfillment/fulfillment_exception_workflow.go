package fulfillment

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// FulfillmentExceptionWorkflowServer implements `acme.fulfillment.v1.FulfillmentExceptionWorkflowService`.
// Runs the long-lived workflow that takes a fulfillment exception from start to finish.
type FulfillmentExceptionWorkflowServer struct {
	fulfillmentv1.UnimplementedFulfillmentExceptionWorkflowServiceServer
	store                              *Store
	log                                *slog.Logger
	fulfillmentQueryClient             fulfillmentv1.FulfillmentQueryServiceClient
	fulfillmentQueryClientFallback     fulfillmentv1.FulfillmentQueryServiceClient
	householdCalculationClient         customersv1.HouseholdCalculationServiceClient
	fulfillmentWorkflowClient          fulfillmentv1.FulfillmentWorkflowServiceClient
	featureFlagValidationClient        featureflagsv1.FeatureFlagValidationServiceClient
	fulfillmentExceptionApprovalClient fulfillmentv1.FulfillmentExceptionApprovalServiceClient
	carrierRateApprovalClient          shippingv1.CarrierRateApprovalServiceClient
	subscriptionApprovalClient         subscriptionsv1.SubscriptionApprovalServiceClient
	advanceLoyaltyMemberWorkflowFn     func(context.Context, *loyaltyv1.AdvanceLoyaltyMemberWorkflowRequest, ...grpc.CallOption) (*loyaltyv1.AdvanceLoyaltyMemberWorkflowResponse, error)
	conn                               *grpc.ClientConn
	preferenceCalculationClient        customersv1.PreferenceCalculationServiceClient
	customerSegmentValidationClient    customersv1.CustomerSegmentValidationServiceClient
}

// NewFulfillmentExceptionWorkflowServer wires a FulfillmentExceptionWorkflowServer.
func NewFulfillmentExceptionWorkflowServer(deps platform.Deps) *FulfillmentExceptionWorkflowServer {
	loyaltyMemberWorkflow := loyaltyv1.NewLoyaltyMemberWorkflowServiceClient(deps.Conn)
	return &FulfillmentExceptionWorkflowServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.fulfillment.v1.FulfillmentExceptionWorkflowService"),
		fulfillmentQueryClient:             fulfillmentv1.NewFulfillmentQueryServiceClient(deps.Conn),
		fulfillmentQueryClientFallback:     fulfillmentv1.NewFulfillmentQueryServiceClient(deps.Conn),
		householdCalculationClient:         customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		fulfillmentWorkflowClient:          fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn),
		featureFlagValidationClient:        featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		fulfillmentExceptionApprovalClient: fulfillmentv1.NewFulfillmentExceptionApprovalServiceClient(deps.Conn),
		carrierRateApprovalClient:          shippingv1.NewCarrierRateApprovalServiceClient(deps.Conn),
		subscriptionApprovalClient:         subscriptionsv1.NewSubscriptionApprovalServiceClient(deps.Conn),
		advanceLoyaltyMemberWorkflowFn:     loyaltyMemberWorkflow.AdvanceLoyaltyMemberWorkflow,
		conn:                               deps.Conn,
		preferenceCalculationClient:        customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		customerSegmentValidationClient:    customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
	}
}

// StartFulfillmentExceptionWorkflow starts a fulfillment exception.
func (s *FulfillmentExceptionWorkflowServer) StartFulfillmentExceptionWorkflow(ctx context.Context, req *fulfillmentv1.StartFulfillmentExceptionWorkflowRequest) (*fulfillmentv1.StartFulfillmentExceptionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "start fulfillment exception workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *fulfillmentv1.CountFulfillmentsRequest, ...grpc.CallOption) (*fulfillmentv1.CountFulfillmentsResponse, error){
		"primary":  s.fulfillmentQueryClient.CountFulfillments,
		"fallback": s.fulfillmentQueryClientFallback.CountFulfillments,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	source := req.GetFulfillmentException().ProtoReflect()
	fulfillmentExceptionId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("fulfillment_exception_id"))).String()
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	countFulfillmentsRequest := &fulfillmentv1.CountFulfillmentsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.CountFulfillmentsRequest_Filter{
			CreatedAfter: req.GetFulfillmentException().GetAudit().GetUpdatedAt(),
			OwnerId:      fulfillmentExceptionId,
			Labels:       annotations,
		},
	}
	countFulfillmentsReply, err := route(ctx, countFulfillmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/CountFulfillments")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*customersv1.ExplainHouseholdCalculationResponse, len(req.GetFulfillmentException().GetHistory()))
	for i, item := range req.GetFulfillmentException().GetHistory() {
		group.Go(func() error {
			reply, err := s.householdCalculationClient.ExplainHouseholdCalculation(groupCtx, &customersv1.ExplainHouseholdCalculationRequest{
				Context: req.GetContext(),
				Household: &customersv1.Household{
					Children: []*customersv1.Household{&customersv1.Household{
						Children: []*customersv1.Household{&customersv1.Household{
							Audit: &commonv1.AuditInfo{
								CreatedAt: item.GetChangedAt(),
								UpdatedAt: req.GetFulfillmentException().GetAudit().GetUpdatedAt(),
							},
							Manager: &identityv1.UserRef{
								Email: req.GetFulfillmentException().GetAssignee().GetEmail(),
							},
						}},
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/ExplainHouseholdCalculation")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &fulfillmentv1.StartFulfillmentExceptionWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: countFulfillmentsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &fulfillmentv1.StartFulfillmentExceptionWorkflowResponse_Outcome{Message: countFulfillmentsReply.GetResult().GetMessage()},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Priority:               req.GetFulfillmentException().GetPriority(),
		},
	}, nil
}

// AdvanceFulfillmentExceptionWorkflow advances a fulfillment exception.
func (s *FulfillmentExceptionWorkflowServer) AdvanceFulfillmentExceptionWorkflow(ctx context.Context, req *fulfillmentv1.AdvanceFulfillmentExceptionWorkflowRequest) (*fulfillmentv1.AdvanceFulfillmentExceptionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "advance fulfillment exception workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	pauseFulfillmentWorkflowReply, err := s.fulfillmentWorkflowClient.PauseFulfillmentWorkflow(ctx, &fulfillmentv1.PauseFulfillmentWorkflowRequest{
		Context: req.GetContext(),
		Details: &fulfillmentv1.PauseFulfillmentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
		Fulfillment: &fulfillmentv1.Fulfillment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetFulfillmentException().GetAudit().GetUpdatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/PauseFulfillmentWorkflow")
	}
	return &fulfillmentv1.AdvanceFulfillmentExceptionWorkflowResponse{
		Outcome: &fulfillmentv1.AdvanceFulfillmentExceptionWorkflowResponse_Outcome{
			EffectiveAt: pauseFulfillmentWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Assignee:               req.GetFulfillmentException().GetAssignee(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelFulfillmentExceptionWorkflow cancels a fulfillment exception.
func (s *FulfillmentExceptionWorkflowServer) CancelFulfillmentExceptionWorkflow(ctx context.Context, req *fulfillmentv1.CancelFulfillmentExceptionWorkflowRequest) (*fulfillmentv1.CancelFulfillmentExceptionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "cancel fulfillment exception workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeFulfillmentsRequest := &fulfillmentv1.SummarizeFulfillmentsRequest{}
	summarizeFulfillmentsRequest.Context = req.GetContext()
	summarizeFulfillmentsRequest.Annotations = annotations
	summarizeFulfillmentsRequest.Reason = req.GetReason()
	summarizeFulfillmentsReply, err := s.fulfillmentQueryClient.SummarizeFulfillments(ctx, summarizeFulfillmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/SummarizeFulfillments")
	}
	var holder struct{ Phone string }
	if record := req.GetDetails().GetNotify(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Phone"); err != nil {
			s.log.WarnContext(ctx, "copy phone", "error", err)
		}
	}
	previewFeatureFlagRequest := &featureflagsv1.PreviewFeatureFlagRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetFulfillmentException().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetFulfillmentException().GetAssignee().GetDisplayName(),
			},
		},
		Details: &featureflagsv1.PreviewFeatureFlagRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Phone:            holder.Phone,
			},
		},
	}
	send := s.previewFeatureFlagSender(previewFeatureFlagRequest)
	previewFeatureFlagReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/PreviewFeatureFlag")
	}
	browseFulfillmentsRequest := buildBrowseFulfillmentsRequestForCancelFulfillmentExceptionWorkflow(req, previewFeatureFlagReply)
	browseFulfillmentsReply, err := s.fulfillmentQueryClient.BrowseFulfillments(ctx, browseFulfillmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/BrowseFulfillments")
	}
	return &fulfillmentv1.CancelFulfillmentExceptionWorkflowResponse{
		Outcome: &fulfillmentv1.CancelFulfillmentExceptionWorkflowResponse_Outcome{
			Status:  summarizeFulfillmentsReply.GetResult().GetStatus(),
			Message: browseFulfillmentsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message: summarizeFulfillmentsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Assignee: &identityv1.UserRef{
				UserId: previewFeatureFlagReply.GetFeatureFlag().GetOwner().GetUserId(),
			},
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			CustomFields:           req.GetFulfillmentException().GetCustomFields(),
		},
	}, nil
}

// RetryFulfillmentExceptionWorkflow retrys a fulfillment exception.
func (s *FulfillmentExceptionWorkflowServer) RetryFulfillmentExceptionWorkflow(ctx context.Context, req *fulfillmentv1.RetryFulfillmentExceptionWorkflowRequest) (*fulfillmentv1.RetryFulfillmentExceptionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "retry fulfillment exception workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	for item := range iterx.Each(req.GetFulfillmentException().GetHistory(), func(candidate *fulfillmentv1.FulfillmentExceptionStatusChange) bool { return candidate != nil }) {
		if _, err := s.fulfillmentExceptionApprovalClient.RejectFulfillmentException(ctx, &fulfillmentv1.RejectFulfillmentExceptionRequest{
			Context: req.GetContext(),
			FulfillmentException: &fulfillmentv1.FulfillmentException{
				History: []*fulfillmentv1.FulfillmentExceptionStatusChange{&fulfillmentv1.FulfillmentExceptionStatusChange{
					FromStatus: item.GetFromStatus(),
					ChangedAt:  item.GetChangedAt(),
					ChangedBy:  item.GetChangedBy(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionApprovalService/RejectFulfillmentException")
		}
	}
	delegateCarrierRateApprovalReply, err := s.carrierRateApprovalClient.DelegateCarrierRateApproval(ctx, &shippingv1.DelegateCarrierRateApprovalRequest{
		Context: req.GetContext(),
		Details: &shippingv1.DelegateCarrierRateApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email: req.GetFulfillmentException().GetRequester().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateApprovalService/DelegateCarrierRateApproval")
	}
	commentSubscriptionApprovalRequest := buildCommentSubscriptionApprovalRequestForRetryFulfillmentExceptionWorkflow(req, delegateCarrierRateApprovalReply)
	commentSubscriptionApprovalReply, err := s.subscriptionApprovalClient.CommentSubscriptionApproval(ctx, commentSubscriptionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionApprovalService/CommentSubscriptionApproval")
	}
	advanceLoyaltyMemberWorkflowRequest := &loyaltyv1.AdvanceLoyaltyMemberWorkflowRequest{Context: req.GetContext()}
	advanceLoyaltyMemberWorkflowReply, err := s.advanceLoyaltyMemberWorkflowFn(ctx, advanceLoyaltyMemberWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberWorkflowService/AdvanceLoyaltyMemberWorkflow")
	}
	_ = commentSubscriptionApprovalReply
	return &fulfillmentv1.RetryFulfillmentExceptionWorkflowResponse{
		Outcome: &fulfillmentv1.RetryFulfillmentExceptionWorkflowResponse_Outcome{
			EffectiveAt: delegateCarrierRateApprovalReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Status: advanceLoyaltyMemberWorkflowReply.GetOutcome().GetStatus(),
		},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			CustomFields:           req.GetFulfillmentException().GetCustomFields(),
		},
	}, nil
}

// PauseFulfillmentExceptionWorkflow pauses a fulfillment exception.
func (s *FulfillmentExceptionWorkflowServer) PauseFulfillmentExceptionWorkflow(ctx context.Context, req *fulfillmentv1.PauseFulfillmentExceptionWorkflowRequest) (*fulfillmentv1.PauseFulfillmentExceptionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "pause fulfillment exception workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	advanceFulfillmentWorkflowRequest := &fulfillmentv1.AdvanceFulfillmentWorkflowRequest{
		Context: req.GetContext(),
		Fulfillment: &fulfillmentv1.Fulfillment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetFulfillmentException().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &fulfillmentv1.AdvanceFulfillmentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name: req.GetFulfillmentException().GetRequester().GetName(),
			},
		},
	}
	advanceFulfillmentWorkflowReply := new(fulfillmentv1.AdvanceFulfillmentWorkflowResponse)
	if err := grpcx.Invoke(ctx, s.conn, fulfillmentv1.FulfillmentWorkflowService_AdvanceFulfillmentWorkflow_FullMethodName, advanceFulfillmentWorkflowRequest, advanceFulfillmentWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/AdvanceFulfillmentWorkflow")
	}
	comparePreferenceOptionsRequest := &customersv1.ComparePreferenceOptionsRequest{}
	comparePreferenceOptionsRequest.Context = req.GetContext()
	comparePreferenceOptionsRequest.Details = &customersv1.ComparePreferenceOptionsRequest_Details{
		Destination: &commonv1.PostalAddress{
			Locality: advanceFulfillmentWorkflowReply.GetFulfillment().GetBillingAddress().GetLocality(),
		},
	}
	comparePreferenceOptionsRequest.Preference = &customersv1.Preference{
		Audit: &commonv1.AuditInfo{
			CreatedBy: req.GetFulfillmentException().GetAudit().GetCreatedBy(),
		},
	}
	comparePreferenceOptionsReply, err := s.preferenceCalculationClient.ComparePreferenceOptions(ctx, comparePreferenceOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions")
	}
	return &fulfillmentv1.PauseFulfillmentExceptionWorkflowResponse{
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: advanceFulfillmentWorkflowReply.GetFulfillment().GetAudit().GetUpdatedAt(),
				CreatedBy: comparePreferenceOptionsReply.GetPreference().GetAudit().GetCreatedBy(),
			},
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			CustomFields:           req.GetFulfillmentException().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeFulfillmentExceptionWorkflow resumes a fulfillment exception.
func (s *FulfillmentExceptionWorkflowServer) ResumeFulfillmentExceptionWorkflow(ctx context.Context, req *fulfillmentv1.ResumeFulfillmentExceptionWorkflowRequest) (*fulfillmentv1.ResumeFulfillmentExceptionWorkflowResponse, error) {
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "resume fulfillment exception workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	checkCustomerSegmentEligibilityReply, err := s.customerSegmentValidationClient.CheckCustomerSegmentEligibility(ctx, &customersv1.CheckCustomerSegmentEligibilityRequest{
		Context: req.GetContext(),
		Details: &customersv1.CheckCustomerSegmentEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetFulfillmentException().GetRequester().GetPreferredChannel(),
			},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetFulfillmentException().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/CheckCustomerSegmentEligibility")
	}
	return &fulfillmentv1.ResumeFulfillmentExceptionWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: checkCustomerSegmentEligibilityReply.GetResult().GetReferenceIds(),
			Status:       checkCustomerSegmentEligibilityReply.GetOutcome().GetStatus(),
		},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			CustomFields:           req.GetFulfillmentException().GetCustomFields(),
		},
	}, nil
}

// SignalFulfillmentExceptionWorkflow signals a fulfillment exception.
func (s *FulfillmentExceptionWorkflowServer) SignalFulfillmentExceptionWorkflow(ctx context.Context, req *fulfillmentv1.SignalFulfillmentExceptionWorkflowRequest) (*fulfillmentv1.SignalFulfillmentExceptionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "signal fulfillment exception workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*customersv1.QuotePreferenceResponse, len(req.GetFulfillmentException().GetHistory()))
	for i, item := range req.GetFulfillmentException().GetHistory() {
		group.Go(func() error {
			reply, err := s.preferenceCalculationClient.QuotePreference(groupCtx, &customersv1.QuotePreferenceRequest{
				Context: req.GetContext(),
				Preference: &customersv1.Preference{
					Audit: &commonv1.AuditInfo{
						CreatedBy: req.GetFulfillmentException().GetAudit().GetCreatedBy(),
						UpdatedAt: item.GetChangedAt(),
					},
					Owner: &identityv1.UserRef{
						Email:       req.GetDetails().GetNotify().GetEmail(),
						DisplayName: req.GetFulfillmentException().GetCustomer().GetDisplayName(),
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/QuotePreference")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &fulfillmentv1.SignalFulfillmentExceptionWorkflowResponse{
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Status:                 req.GetFulfillmentException().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// previewFeatureFlagSender returns a function that sends one request to
// acme.featureflags.v1.FeatureFlagValidationService/PreviewFeatureFlag.
func (s *FulfillmentExceptionWorkflowServer) previewFeatureFlagSender(request *featureflagsv1.PreviewFeatureFlagRequest) func(context.Context) (*featureflagsv1.PreviewFeatureFlagResponse, error) {
	return func(ctx context.Context) (*featureflagsv1.PreviewFeatureFlagResponse, error) {
		return s.featureFlagValidationClient.PreviewFeatureFlag(ctx, request)
	}
}
