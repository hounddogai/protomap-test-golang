package checkout

import (
	"context"
	"log/slog"

	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// CheckoutSessionWorkflowServer implements `acme.checkout.v1.CheckoutSessionWorkflowService`.
// Runs the long-lived workflow that takes a checkout session from start to finish.
type CheckoutSessionWorkflowServer struct {
	checkoutv1.UnimplementedCheckoutSessionWorkflowServiceServer
	store                         *Store
	log                           *slog.Logger
	shippingQuoteQueryClient      checkoutv1.ShippingQuoteQueryServiceClient
	checkoutSessionQueryClient    checkoutv1.CheckoutSessionQueryServiceClient
	checkoutSessionApprovalClient checkoutv1.CheckoutSessionApprovalServiceClient
	shipmentApprovalClient        shippingv1.ShipmentApprovalServiceClient
	checkoutSessionClient         checkoutv1.CheckoutSessionServiceClient
	sessionValidationClient       identityv1.SessionValidationServiceClient
	shipmentWorkflowClient        shippingv1.ShipmentWorkflowServiceClient
	householdValidationPort       householdValidationPort
	restockDecisionApprovalClient returnsv1.RestockDecisionApprovalServiceClient
	carePlanCalculationPort       carePlanCalculationPort
	customerCalculationClient     customersv1.CustomerCalculationServiceClient
}

// NewCheckoutSessionWorkflowServer wires a CheckoutSessionWorkflowServer.
func NewCheckoutSessionWorkflowServer(deps platform.Deps) *CheckoutSessionWorkflowServer {
	return &CheckoutSessionWorkflowServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.checkout.v1.CheckoutSessionWorkflowService"),
		shippingQuoteQueryClient:      checkoutv1.NewShippingQuoteQueryServiceClient(deps.Conn),
		checkoutSessionQueryClient:    checkoutv1.NewCheckoutSessionQueryServiceClient(deps.Conn),
		checkoutSessionApprovalClient: checkoutv1.NewCheckoutSessionApprovalServiceClient(deps.Conn),
		shipmentApprovalClient:        shippingv1.NewShipmentApprovalServiceClient(deps.Conn),
		checkoutSessionClient:         checkoutv1.NewCheckoutSessionServiceClient(deps.Conn),
		sessionValidationClient:       identityv1.NewSessionValidationServiceClient(deps.Conn),
		shipmentWorkflowClient:        shippingv1.NewShipmentWorkflowServiceClient(deps.Conn),
		householdValidationPort:       newHouseholdValidationPort(deps),
		restockDecisionApprovalClient: returnsv1.NewRestockDecisionApprovalServiceClient(deps.Conn),
		carePlanCalculationPort:       newCarePlanCalculationPort(deps),
		customerCalculationClient:     customersv1.NewCustomerCalculationServiceClient(deps.Conn),
	}
}

// StartCheckoutSessionWorkflow starts a checkout session.
func (s *CheckoutSessionWorkflowServer) StartCheckoutSessionWorkflow(ctx context.Context, req *checkoutv1.StartCheckoutSessionWorkflowRequest) (*checkoutv1.StartCheckoutSessionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StartCheckoutSessionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "start checkout session workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	summarizeShippingQuotesRequest := buildSummarizeShippingQuotesRequestForStartCheckoutSessionWorkflow(req)
	summarizeShippingQuotesReply, err := s.shippingQuoteQueryClient.SummarizeShippingQuotes(ctx, summarizeShippingQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/SummarizeShippingQuotes")
	}
	return &checkoutv1.StartCheckoutSessionWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details:      summarizeShippingQuotesReply.GetResult().GetDetails(),
			ReferenceIds: summarizeShippingQuotesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			CheckoutSessionId: req.GetCheckoutSessionId(),
			Notes:             req.GetCheckoutSession().GetNotes(),
		},
	}, nil
}

// AdvanceCheckoutSessionWorkflow advances a checkout session.
func (s *CheckoutSessionWorkflowServer) AdvanceCheckoutSessionWorkflow(ctx context.Context, req *checkoutv1.AdvanceCheckoutSessionWorkflowRequest) (*checkoutv1.AdvanceCheckoutSessionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "advance checkout session workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	countCheckoutSessionsRequest := &checkoutv1.CountCheckoutSessionsRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.CountCheckoutSessionsRequest_Filter{
			Region:       req.GetCheckoutSession().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetCheckoutSession().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetCheckoutSession().GetCounterparty().GetShippingQuoteId(),
			Labels:       req.GetDetails().GetParameters(),
		},
	}
	countCheckoutSessionsReply, err := grpcx.Call(ctx, s.checkoutSessionQueryClient.CountCheckoutSessions, countCheckoutSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionQueryService/CountCheckoutSessions")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*checkoutv1.RejectCheckoutSessionResponse, len(req.GetCheckoutSession().GetLines()))
	for i, line := range req.GetCheckoutSession().GetLines() {
		group.Go(func() error {
			reply, err := s.checkoutSessionApprovalClient.RejectCheckoutSession(groupCtx, &checkoutv1.RejectCheckoutSessionRequest{
				Context: req.GetContext(),
				CheckoutSession: &checkoutv1.CheckoutSession{
					Lines: []*checkoutv1.CheckoutSessionLine{&checkoutv1.CheckoutSessionLine{
						Tax: &checkoutv1.CheckoutSessionLineTax{
							Rate:         line.GetTax().GetRate(),
							Amount:       line.GetTax().GetAmount(),
							Jurisdiction: line.GetTax().GetJurisdiction(),
						},
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
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionApprovalService/RejectCheckoutSession")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &checkoutv1.AdvanceCheckoutSessionWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:  countCheckoutSessionsReply.GetResult().GetStatus(),
			Details: countCheckoutSessionsReply.GetResult().GetDetails(),
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			CheckoutSessionId: req.GetCheckoutSessionId(),
			IssuedAt:          req.GetCheckoutSession().GetIssuedAt(),
		},
	}, nil
}

// CancelCheckoutSessionWorkflow cancels a checkout session.
func (s *CheckoutSessionWorkflowServer) CancelCheckoutSessionWorkflow(ctx context.Context, req *checkoutv1.CancelCheckoutSessionWorkflowRequest) (*checkoutv1.CancelCheckoutSessionWorkflowResponse, error) {
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "cancel checkout session workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*shippingv1.RecallShipmentResponse, len(req.GetCheckoutSession().GetAttachments()))
	for i, attachment := range req.GetCheckoutSession().GetAttachments() {
		group.Go(func() error {
			reply, err := s.shipmentApprovalClient.RecallShipment(groupCtx, &shippingv1.RecallShipmentRequest{
				Context: req.GetContext(),
				Shipment: &shippingv1.Shipment{
					Attachments: []*shippingv1.ShipmentAttachment{&shippingv1.ShipmentAttachment{
						ContentType: attachment.GetContentType(),
						Checksum:    attachment.GetChecksum(),
						FileName:    attachment.GetFileName(),
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
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentApprovalService/RecallShipment")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	searchCheckoutSessionsRequest := &checkoutv1.SearchCheckoutSessionsRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.SearchCheckoutSessionsRequest_Filter{
			Region: req.GetCheckoutSession().GetBillingAddress().GetRegion(),
		},
	}
	searchCheckoutSessionsReply, err := checkoutv1.CheckoutSessionServiceClient.SearchCheckoutSessions(s.checkoutSessionClient, ctx, searchCheckoutSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionService/SearchCheckoutSessions")
	}
	validateSessionRequest := buildValidateSessionRequestForCancelCheckoutSessionWorkflow(req)
	validateSessionReply, err := s.sessionValidationClient.ValidateSession(ctx, validateSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/ValidateSession")
	}
	return &checkoutv1.CancelCheckoutSessionWorkflowResponse{
		Outcome: &checkoutv1.CancelCheckoutSessionWorkflowResponse_Outcome{
			Message: searchCheckoutSessionsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message: searchCheckoutSessionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			Counterparty: &checkoutv1.ShippingQuoteRef{
				DisplayName: validateSessionReply.GetSession().GetCustomer().GetDisplayName(),
			},
			CheckoutSessionId: req.GetCheckoutSessionId(),
			DueAt:             req.GetCheckoutSession().GetDueAt(),
		},
	}, nil
}

// ResumeCheckoutSessionWorkflow resumes a checkout session.
func (s *CheckoutSessionWorkflowServer) ResumeCheckoutSessionWorkflow(ctx context.Context, req *checkoutv1.ResumeCheckoutSessionWorkflowRequest) (*checkoutv1.ResumeCheckoutSessionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeCheckoutSessionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "resume checkout session workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	for line := range iterx.Each(req.GetCheckoutSession().GetLines(), func(candidate *checkoutv1.CheckoutSessionLine) bool { return candidate != nil }) {
		if _, err := s.shipmentWorkflowClient.AdvanceShipmentWorkflow(ctx, &shippingv1.AdvanceShipmentWorkflowRequest{
			Context: req.GetContext(),
			Shipment: &shippingv1.Shipment{
				Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{
					Tax: &shippingv1.ShipmentLineTax{Amount: line.GetTax().GetAmount()},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentWorkflowService/AdvanceShipmentWorkflow")
		}
	}
	return &checkoutv1.ResumeCheckoutSessionWorkflowResponse{
		CheckoutSession: &checkoutv1.CheckoutSession{
			CheckoutSessionId: req.GetCheckoutSessionId(),
			Total:             req.GetCheckoutSession().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectCheckoutSessionWorkflow returns a checkout session.
func (s *CheckoutSessionWorkflowServer) InspectCheckoutSessionWorkflow(ctx context.Context, req *checkoutv1.InspectCheckoutSessionWorkflowRequest) (*checkoutv1.InspectCheckoutSessionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectCheckoutSessionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "checkout_session", req.GetCheckoutSessionId(), "inspect_checkout_session_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect checkout session workflow", "tenant", req.GetContext().GetTenantId())
	normalizeHouseholdRequest := &customersv1.NormalizeHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						UserId: req.GetContext().GetActor().GetUserId(),
						Email:  req.GetContext().GetActor().GetEmail(),
					},
					Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
				}},
			}},
		},
		Details: &customersv1.NormalizeHouseholdRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	normalizeHouseholdReply, err := s.householdValidationPort.NormalizeHousehold(ctx, normalizeHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/NormalizeHousehold")
	}
	return &checkoutv1.InspectCheckoutSessionWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: normalizeHouseholdReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		CheckoutSession: &checkoutv1.CheckoutSession{CheckoutSessionId: req.GetCheckoutSessionId()},
	}, nil
}

// ListCheckoutSessionWorkflows lists checkout sessions a page at a time.
func (s *CheckoutSessionWorkflowServer) ListCheckoutSessionWorkflows(ctx context.Context, req *checkoutv1.ListCheckoutSessionWorkflowsRequest) (*checkoutv1.ListCheckoutSessionWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListCheckoutSessionWorkflows", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list checkout session workflows", "tenant", req.GetContext().GetTenantId())
	return &checkoutv1.ListCheckoutSessionWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalCheckoutSessionWorkflow signals a checkout session.
func (s *CheckoutSessionWorkflowServer) SignalCheckoutSessionWorkflow(ctx context.Context, req *checkoutv1.SignalCheckoutSessionWorkflowRequest) (*checkoutv1.SignalCheckoutSessionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SignalCheckoutSessionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "signal checkout session workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := checkoutSessionWorkflowServerPeekShippingQuoteSteps[mode]
	cloned := proto.Clone(req.GetCheckoutSession()).(*checkoutv1.CheckoutSession)
	peekShippingQuoteRequest := &checkoutv1.PeekShippingQuoteRequest{
		Context:         req.GetContext(),
		ShippingQuoteId: cloned.GetCounterparty().GetShippingQuoteId(),
	}
	peekShippingQuoteReply, err := run(s, ctx, peekShippingQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/PeekShippingQuote")
	}
	submitRestockDecisionRequest := buildSubmitRestockDecisionRequestForSignalCheckoutSessionWorkflow(req, peekShippingQuoteReply)
	submitRestockDecisionReply, err := s.restockDecisionApprovalClient.SubmitRestockDecision(ctx, submitRestockDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionApprovalService/SubmitRestockDecision")
	}
	explainCarePlanCalculationRequest := &careplansv1.ExplainCarePlanCalculationRequest{
		Context: req.GetContext(),
		CarePlan: &careplansv1.CarePlan{
			Lines: []*careplansv1.CarePlanLine{&careplansv1.CarePlanLine{
				Tax: &careplansv1.CarePlanLineTax{
					Amount: submitRestockDecisionReply.GetRestockDecision().GetDiscount().GetAmount(),
				},
			}},
		},
	}
	explainCarePlanCalculationReply, err := s.carePlanCalculationPort.ExplainCarePlanCalculation(ctx, explainCarePlanCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.careplans.v1.CarePlanCalculationService/ExplainCarePlanCalculation")
	}
	recalculateCustomersRequest := &customersv1.RecalculateCustomersRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{
				Email: peekShippingQuoteReply.GetShippingQuote().GetCounterparty().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: submitRestockDecisionReply.GetRestockDecision().GetAudit().GetUpdatedAt(),
				UpdatedBy: explainCarePlanCalculationReply.GetCarePlan().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &customersv1.RecalculateCustomersRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: explainCarePlanCalculationReply.GetCarePlan().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	send := s.recalculateCustomersSender(recalculateCustomersRequest)
	recalculateCustomersReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	return &checkoutv1.SignalCheckoutSessionWorkflowResponse{
		CheckoutSession: &checkoutv1.CheckoutSession{
			Number: peekShippingQuoteReply.GetShippingQuote().GetNumber(),
			BillingAddress: &commonv1.PostalAddress{
				Locality: peekShippingQuoteReply.GetShippingQuote().GetBillingAddress().GetLocality(),
			},
			Metadata: explainCarePlanCalculationReply.GetCarePlan().GetMetadata(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recalculateCustomersReply.GetCustomer().GetAudit().GetUpdatedAt(),
			},
			CheckoutSessionId: req.GetCheckoutSessionId(),
			IssuedAt:          req.GetCheckoutSession().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{
			Message: submitRestockDecisionReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Outcome: &checkoutv1.SignalCheckoutSessionWorkflowResponse_Outcome{
			Status: explainCarePlanCalculationReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// checkoutSessionWorkflowServerPeekShippingQuoteSteps are the ways to run
// acme.checkout.v1.ShippingQuoteQueryService/PeekShippingQuote, by name.
var checkoutSessionWorkflowServerPeekShippingQuoteSteps = map[string]func(*CheckoutSessionWorkflowServer, context.Context, *checkoutv1.PeekShippingQuoteRequest) (*checkoutv1.PeekShippingQuoteResponse, error){
	"default": (*CheckoutSessionWorkflowServer).peekShippingQuoteStep,
	"audited": (*CheckoutSessionWorkflowServer).peekShippingQuoteAuditedStep,
}

func (s *CheckoutSessionWorkflowServer) peekShippingQuoteStep(ctx context.Context, request *checkoutv1.PeekShippingQuoteRequest) (*checkoutv1.PeekShippingQuoteResponse, error) {
	return s.shippingQuoteQueryClient.PeekShippingQuote(ctx, request)
}

func (s *CheckoutSessionWorkflowServer) peekShippingQuoteAuditedStep(ctx context.Context, request *checkoutv1.PeekShippingQuoteRequest) (*checkoutv1.PeekShippingQuoteResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.checkout.v1.ShippingQuoteQueryService/PeekShippingQuote")
	return s.shippingQuoteQueryClient.PeekShippingQuote(ctx, request)
}

// recalculateCustomersSender returns a function that sends one request to
// acme.customers.v1.CustomerCalculationService/RecalculateCustomers.
func (s *CheckoutSessionWorkflowServer) recalculateCustomersSender(request *customersv1.RecalculateCustomersRequest) func(context.Context) (*customersv1.RecalculateCustomersResponse, error) {
	return func(ctx context.Context) (*customersv1.RecalculateCustomersResponse, error) {
		return s.customerCalculationClient.RecalculateCustomers(ctx, request)
	}
}
