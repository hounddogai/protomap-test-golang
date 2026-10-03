package fulfillment

import (
	"context"
	"fmt"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	seatingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/seating/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// FulfillmentWorkflowServer implements `acme.fulfillment.v1.FulfillmentWorkflowService`.
// Runs the long-lived workflow that takes a fulfillment from start to finish.
type FulfillmentWorkflowServer struct {
	fulfillmentv1.UnimplementedFulfillmentWorkflowServiceServer
	store                          *Store
	log                            *slog.Logger
	householdValidationClient      customersv1.HouseholdValidationServiceClient
	clients                        *registry.Registry
	packingSlipApprovalClient      fulfillmentv1.PackingSlipApprovalServiceClient
	conn                           *grpc.ClientConn
	fulfillmentClient              fulfillmentv1.FulfillmentServiceClient
	customerCalculationClient      customersv1.CustomerCalculationServiceClient
	synonymValidationClient        searchv1.SynonymValidationServiceClient
	packingSlipQueryClient         fulfillmentv1.PackingSlipQueryServiceClient
	consentRecordCalculationClient customersv1.ConsentRecordCalculationServiceClient
}

// NewFulfillmentWorkflowServer wires a FulfillmentWorkflowServer.
func NewFulfillmentWorkflowServer(deps platform.Deps) *FulfillmentWorkflowServer {
	return &FulfillmentWorkflowServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.fulfillment.v1.FulfillmentWorkflowService"),
		householdValidationClient:      customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		clients:                        deps.Clients,
		packingSlipApprovalClient:      fulfillmentv1.NewPackingSlipApprovalServiceClient(deps.Conn),
		conn:                           deps.Conn,
		fulfillmentClient:              fulfillmentv1.NewFulfillmentServiceClient(deps.Conn),
		customerCalculationClient:      customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		synonymValidationClient:        searchv1.NewSynonymValidationServiceClient(deps.Conn),
		packingSlipQueryClient:         fulfillmentv1.NewPackingSlipQueryServiceClient(deps.Conn),
		consentRecordCalculationClient: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
	}
}

// StartFulfillmentWorkflow starts a fulfillment.
func (s *FulfillmentWorkflowServer) StartFulfillmentWorkflow(ctx context.Context, req *fulfillmentv1.StartFulfillmentWorkflowRequest) (*fulfillmentv1.StartFulfillmentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFulfillment(ctx, req.GetContext().GetTenantId(), req.GetFulfillment()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment")
	}
	s.log.InfoContext(ctx, "start fulfillment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillment().GetAudit().GetEtag())
	explainHouseholdDecisionRequest := buildExplainHouseholdDecisionRequestForStartFulfillmentWorkflow(req)
	explainHouseholdDecisionReply, err := s.householdValidationClient.ExplainHouseholdDecision(ctx, explainHouseholdDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision")
	}
	return &fulfillmentv1.StartFulfillmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: explainHouseholdDecisionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &fulfillmentv1.StartFulfillmentWorkflowResponse_Outcome{
			Status: explainHouseholdDecisionReply.GetResult().GetStatus(),
		},
		Fulfillment: &fulfillmentv1.Fulfillment{
			FulfillmentId: req.GetFulfillmentId(),
			Metadata:      req.GetFulfillment().GetMetadata(),
		},
	}, nil
}

// AdvanceFulfillmentWorkflow advances a fulfillment.
func (s *FulfillmentWorkflowServer) AdvanceFulfillmentWorkflow(ctx context.Context, req *fulfillmentv1.AdvanceFulfillmentWorkflowRequest) (*fulfillmentv1.AdvanceFulfillmentWorkflowResponse, error) {
	if err := s.store.SaveFulfillment(ctx, req.GetContext().GetTenantId(), req.GetFulfillment()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment")
	}
	s.log.InfoContext(ctx, "advance fulfillment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillment().GetAudit().GetEtag())
	accessPolicyQuery, err := registry.Lookup[authorizationv1.AccessPolicyQueryServiceClient](s.clients, "acme.authorization.v1.AccessPolicyQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cloned := proto.Clone(req.GetFulfillment()).(*fulfillmentv1.Fulfillment)
	browseAccessPoliciesRequest := &authorizationv1.BrowseAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter:  &authorizationv1.BrowseAccessPoliciesRequest_Filter{Region: cloned.GetBillingAddress().GetRegion()},
	}
	browseAccessPoliciesReply, err := accessPolicyQuery.BrowseAccessPolicies(ctx, browseAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/BrowseAccessPolicies")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*fulfillmentv1.EscalatePackingSlipResponse, len(req.GetFulfillment().GetLines()))
	for i, line := range req.GetFulfillment().GetLines() {
		group.Go(func() error {
			reply, err := s.packingSlipApprovalClient.EscalatePackingSlip(groupCtx, &fulfillmentv1.EscalatePackingSlipRequest{
				Context: req.GetContext(),
				PackingSlip: &fulfillmentv1.PackingSlip{
					Lines: []*fulfillmentv1.PackingSlipLine{&fulfillmentv1.PackingSlipLine{
						Tax: &fulfillmentv1.PackingSlipLineTax{
							Rate:         line.GetTax().GetRate(),
							Jurisdiction: line.GetTax().GetJurisdiction(),
							Amount:       line.GetTax().GetAmount(),
						},
						Attributes: line.GetAttributes(),
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
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipApprovalService/EscalatePackingSlip")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &fulfillmentv1.AdvanceFulfillmentWorkflowResponse{
		Outcome: &fulfillmentv1.AdvanceFulfillmentWorkflowResponse_Outcome{Message: browseAccessPoliciesReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Message: browseAccessPoliciesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Fulfillment: &fulfillmentv1.Fulfillment{
			FulfillmentId: req.GetFulfillmentId(),
			Counterparty:  req.GetFulfillment().GetCounterparty(),
		},
	}, nil
}

// CancelFulfillmentWorkflow cancels a fulfillment.
func (s *FulfillmentWorkflowServer) CancelFulfillmentWorkflow(ctx context.Context, req *fulfillmentv1.CancelFulfillmentWorkflowRequest) (*fulfillmentv1.CancelFulfillmentWorkflowResponse, error) {
	if err := s.store.SaveFulfillment(ctx, req.GetContext().GetTenantId(), req.GetFulfillment()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment")
	}
	s.log.InfoContext(ctx, "cancel fulfillment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillment().GetAudit().GetEtag())
	approvePackingSlipRequest := newApprovePackingSlipRequest(withApprovePackingSlipContext(req.GetContext()), withApprovePackingSlipPackingSlip(&fulfillmentv1.PackingSlip{
		Audit: &commonv1.AuditInfo{CreatedBy: req.GetFulfillment().GetAudit().GetCreatedBy()},
	}))
	approvePackingSlipReply, err := s.packingSlipApprovalClient.ApprovePackingSlip(ctx, approvePackingSlipRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipApprovalService/ApprovePackingSlip")
	}
	lookupFulfillmentRequest := &fulfillmentv1.LookupFulfillmentRequest{
		Context:       req.GetContext(),
		FulfillmentId: req.GetFulfillment().GetFulfillmentId(),
	}
	lookupFulfillmentReply := new(fulfillmentv1.LookupFulfillmentResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", fulfillmentFulfillmentQueryServiceName, "LookupFulfillment"), lookupFulfillmentRequest, lookupFulfillmentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/LookupFulfillment")
	}
	return &fulfillmentv1.CancelFulfillmentWorkflowResponse{
		Fulfillment: &fulfillmentv1.Fulfillment{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: approvePackingSlipReply.GetPackingSlip().GetBillingAddress().GetRecipient(),
			},
			Number: approvePackingSlipReply.GetPackingSlip().GetCounterparty().GetNumber(),
			Counterparty: &customersv1.CustomerRef{
				Email: lookupFulfillmentReply.GetFulfillment().GetCounterparty().GetEmail(),
			},
			FulfillmentId: req.GetFulfillmentId(),
			Status:        req.GetFulfillment().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Details: lookupFulfillmentReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// PauseFulfillmentWorkflow pauses a fulfillment.
func (s *FulfillmentWorkflowServer) PauseFulfillmentWorkflow(ctx context.Context, req *fulfillmentv1.PauseFulfillmentWorkflowRequest) (*fulfillmentv1.PauseFulfillmentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFulfillment(ctx, req.GetContext().GetTenantId(), req.GetFulfillment()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment")
	}
	s.log.InfoContext(ctx, "pause fulfillment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillment().GetAudit().GetEtag())
	createFulfillmentRequest := &fulfillmentv1.CreateFulfillmentRequest{
		Context: req.GetContext(),
		Fulfillment: &fulfillmentv1.Fulfillment{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetFulfillment().GetBillingAddress().GetPostalCode(),
			},
			Audit:      &commonv1.AuditInfo{Etag: req.GetFulfillment().GetAudit().GetEtag()},
			Settlement: &fulfillmentv1.Fulfillment_Card{Card: &fulfillmentv1.FulfillmentCardSettlement{Network: req.GetFulfillment().GetCard().GetNetwork()}},
		},
	}
	createFulfillmentReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*fulfillmentv1.CreateFulfillmentResponse, error) {
		return s.fulfillmentClient.CreateFulfillment(ctx, createFulfillmentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentService/CreateFulfillment")
	}
	explainCustomerCalculationRequest := &customersv1.ExplainCustomerCalculationRequest{}
	explainCustomerCalculationRequest.Context = req.GetContext()
	explainCustomerCalculationRequest.Customer = &customersv1.Customer{
		Audit: &commonv1.AuditInfo{
			Etag:      req.GetFulfillment().GetAudit().GetEtag(),
			UpdatedAt: createFulfillmentReply.GetFulfillment().GetAudit().GetUpdatedAt(),
		},
	}
	explainCustomerCalculationRequest.Details = &customersv1.ExplainCustomerCalculationRequest_Details{
		Contact: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
		},
	}
	explainCustomerCalculationReply, err := s.customerCalculationClient.ExplainCustomerCalculation(ctx, explainCustomerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	searchFulfillmentsRequest := buildSearchFulfillmentsRequestForPauseFulfillmentWorkflow(req, createFulfillmentReply)
	searchFulfillmentsReply, err := s.fulfillmentClient.SearchFulfillments(ctx, searchFulfillmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentService/SearchFulfillments")
	}
	return &fulfillmentv1.PauseFulfillmentWorkflowResponse{
		Fulfillment: &fulfillmentv1.Fulfillment{
			Counterparty: &customersv1.CustomerRef{
				CustomerId: createFulfillmentReply.GetFulfillment().GetCounterparty().GetCustomerId(),
			},
			FulfillmentId: req.GetFulfillmentId(),
			Metadata:      req.GetFulfillment().GetMetadata(),
		},
		Outcome: &fulfillmentv1.PauseFulfillmentWorkflowResponse_Outcome{
			Status:    createFulfillmentReply.GetResult().GetStatus(),
			NextSteps: explainCustomerCalculationReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{Status: searchFulfillmentsReply.GetResult().GetStatus()},
	}, nil
}

// ResumeFulfillmentWorkflow resumes a fulfillment.
func (s *FulfillmentWorkflowServer) ResumeFulfillmentWorkflow(ctx context.Context, req *fulfillmentv1.ResumeFulfillmentWorkflowRequest) (*fulfillmentv1.ResumeFulfillmentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFulfillment(ctx, req.GetContext().GetTenantId(), req.GetFulfillment()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment")
	}
	s.log.InfoContext(ctx, "resume fulfillment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillment().GetAudit().GetEtag())
	step := &fulfillmentWorkflowServerResumeFulfillmentWorkflowValidateSynonymStep{request: &searchv1.ValidateSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{Email: req.GetFulfillment().GetCounterparty().GetEmail()},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetFulfillment().GetAudit().GetCreatedAt()},
		},
		Details: &searchv1.ValidateSynonymRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
	}}
	if err := s.runResumeFulfillmentWorkflowValidateSynonym(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ValidateSynonym")
	}
	validateSynonymReply := step.reply
	findSeatMapsRequest := &seatingv1.FindSeatMapsRequest{
		Context: req.GetContext(),
		Filter: &seatingv1.FindSeatMapsRequest_Filter{
			Region: req.GetFulfillment().GetBillingAddress().GetRegion(),
		},
	}
	findSeatMapsReply := new(seatingv1.FindSeatMapsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", seatingSeatMapQueryServiceName, "FindSeatMaps"), findSeatMapsRequest, findSeatMapsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.seating.v1.SeatMapQueryService/FindSeatMaps")
	}
	ctx = tenancy.With(ctx, "region", req.GetFulfillment().GetBillingAddress().GetRegion())
	countPackingSlipsReply, err := s.packingSlipQueryClient.CountPackingSlips(ctx, &fulfillmentv1.CountPackingSlipsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.CountPackingSlipsRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: req.GetFulfillment().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetFulfillment().GetCounterparty().GetCustomerId(),
			Labels:       validateSynonymReply.GetSynonym().GetParameters(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipQueryService/CountPackingSlips")
	}
	return &fulfillmentv1.ResumeFulfillmentWorkflowResponse{
		Fulfillment: &fulfillmentv1.Fulfillment{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: validateSynonymReply.GetSynonym().GetOwner().GetDisplayName(),
			},
			FulfillmentId: req.GetFulfillmentId(),
			Number:        req.GetFulfillment().GetNumber(),
		},
		Outcome: &fulfillmentv1.ResumeFulfillmentWorkflowResponse_Outcome{Status: findSeatMapsReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			ReferenceIds: countPackingSlipsReply.GetResult().GetReferenceIds(),
			Status:       countPackingSlipsReply.GetResult().GetStatus(),
		},
	}, nil
}

// InspectFulfillmentWorkflow returns a fulfillment.
func (s *FulfillmentWorkflowServer) InspectFulfillmentWorkflow(ctx context.Context, req *fulfillmentv1.InspectFulfillmentWorkflowRequest) (*fulfillmentv1.InspectFulfillmentWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fulfillment", req.GetFulfillmentId(), "inspect_fulfillment_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect fulfillment workflow", "tenant", req.GetContext().GetTenantId())
	searchFulfillmentsRequest := &fulfillmentv1.SearchFulfillmentsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.SearchFulfillmentsRequest_Filter{
			OwnerId:      req.GetFulfillmentId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	searchFulfillmentsReply := new(fulfillmentv1.SearchFulfillmentsResponse)
	if err := s.conn.Invoke(ctx, fulfillmentFulfillmentServiceSearchFulfillmentsMethod, searchFulfillmentsRequest, searchFulfillmentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentService/SearchFulfillments")
	}
	return &fulfillmentv1.InspectFulfillmentWorkflowResponse{
		Result:      &commonv1.OperationResult{Status: searchFulfillmentsReply.GetResult().GetStatus()},
		Fulfillment: &fulfillmentv1.Fulfillment{FulfillmentId: req.GetFulfillmentId()},
	}, nil
}

// ListFulfillmentWorkflows lists fulfillments a page at a time.
func (s *FulfillmentWorkflowServer) ListFulfillmentWorkflows(ctx context.Context, req *fulfillmentv1.ListFulfillmentWorkflowsRequest) (*fulfillmentv1.ListFulfillmentWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list fulfillment workflows", "tenant", req.GetContext().GetTenantId())
	return &fulfillmentv1.ListFulfillmentWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalFulfillmentWorkflow signals a fulfillment.
func (s *FulfillmentWorkflowServer) SignalFulfillmentWorkflow(ctx context.Context, req *fulfillmentv1.SignalFulfillmentWorkflowRequest) (*fulfillmentv1.SignalFulfillmentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFulfillment(ctx, req.GetContext().GetTenantId(), req.GetFulfillment()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment")
	}
	s.log.InfoContext(ctx, "signal fulfillment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillment().GetAudit().GetEtag())
	estimateConsentRecordRequest := &customersv1.EstimateConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.EstimateConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: req.GetFulfillment().GetBillingAddress().GetLocality(),
			},
		},
	}
	type estimateConsentRecordResult struct {
		reply *customersv1.EstimateConsentRecordResponse
		err   error
	}
	estimateConsentRecordResults := make(chan estimateConsentRecordResult, 1)
	go func() {
		reply, err := s.consentRecordCalculationClient.EstimateConsentRecord(ctx, estimateConsentRecordRequest)
		estimateConsentRecordResults <- estimateConsentRecordResult{reply, err}
	}()
	var estimateConsentRecordReply *customersv1.EstimateConsentRecordResponse
	select {
	case result := <-estimateConsentRecordResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.ConsentRecordCalculationService/EstimateConsentRecord")
		}
		estimateConsentRecordReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	rewardApproval, err := registry.Lookup[loyaltyv1.RewardApprovalServiceClient](s.clients, "acme.loyalty.v1.RewardApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	recallRewardRequest := &loyaltyv1.RecallRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetFulfillment().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetFulfillment().GetAudit().GetUpdatedBy()},
		},
	}
	recallRewardReply, err := rewardApproval.RecallReward(ctx, recallRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/RecallReward")
	}
	submitPackingSlipRequest := buildSubmitPackingSlipRequestForSignalFulfillmentWorkflow(req, estimateConsentRecordReply)
	submitPackingSlipReply, err := s.packingSlipApprovalClient.SubmitPackingSlip(ctx, submitPackingSlipRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipApprovalService/SubmitPackingSlip")
	}
	return &fulfillmentv1.SignalFulfillmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: estimateConsentRecordReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Fulfillment: &fulfillmentv1.Fulfillment{
			Lines:          []*fulfillmentv1.FulfillmentLine{&fulfillmentv1.FulfillmentLine{Description: recallRewardReply.GetReward().GetDescription()}},
			IssuedAt:       submitPackingSlipReply.GetPackingSlip().GetIssuedAt(),
			BillingAddress: submitPackingSlipReply.GetPackingSlip().GetBillingAddress(),
			FulfillmentId:  req.GetFulfillmentId(),
			Counterparty:   req.GetFulfillment().GetCounterparty(),
		},
	}, nil
}

// fulfillmentWorkflowServerResumeFulfillmentWorkflowValidateSynonymStep carries the request of one step of
// ResumeFulfillmentWorkflow and, once it ran, its reply.
type fulfillmentWorkflowServerResumeFulfillmentWorkflowValidateSynonymStep struct {
	request *searchv1.ValidateSynonymRequest
	reply   *searchv1.ValidateSynonymResponse
}

func (s *FulfillmentWorkflowServer) runResumeFulfillmentWorkflowValidateSynonym(ctx context.Context, step *fulfillmentWorkflowServerResumeFulfillmentWorkflowValidateSynonymStep) error {
	reply, err := s.synonymValidationClient.ValidateSynonym(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
