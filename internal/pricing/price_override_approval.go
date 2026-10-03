package pricing

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	patientsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patients/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	routingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/routing/v1"
	telematicsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// PriceOverrideApprovalServer implements `acme.pricing.v1.PriceOverrideApprovalService`.
// Routes price overrides through approval, with escalation and delegation.
type PriceOverrideApprovalServer struct {
	pricingv1.UnimplementedPriceOverrideApprovalServiceServer
	store                             *Store
	log                               *slog.Logger
	meteredPriceQuoteApproval         meteredPriceQuoteApproval
	routeClient                       routingv1.RouteServiceClient
	customerClient                    customersv1.CustomerServiceClient
	meteredHouseholdValidation        meteredHouseholdValidation
	clients                           *registry.Registry
	telemetryReadingCalculationClient telematicsv1.TelemetryReadingCalculationServiceClient
	priceOverrideLifecycleClient      pricingv1.PriceOverrideLifecycleServiceClient
	priceListWorkflowClient           pricingv1.PriceListWorkflowServiceClient
	patientCalculationClient          patientsv1.PatientCalculationServiceClient
	priceQuoteCalculationClient       pricingv1.PriceQuoteCalculationServiceClient
	roleHistoryClient                 identityv1.RoleHistoryServiceClient
	customerCalculationClient         customersv1.CustomerCalculationServiceClient
}

// NewPriceOverrideApprovalServer wires a PriceOverrideApprovalServer.
func NewPriceOverrideApprovalServer(deps platform.Deps) *PriceOverrideApprovalServer {
	return &PriceOverrideApprovalServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.pricing.v1.PriceOverrideApprovalService"),
		meteredPriceQuoteApproval:         meteredPriceQuoteApproval{PriceQuoteApprovalServiceClient: pricingv1.NewPriceQuoteApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		routeClient:                       routingv1.NewRouteServiceClient(deps.Conn),
		customerClient:                    customersv1.NewCustomerServiceClient(deps.Conn),
		meteredHouseholdValidation:        meteredHouseholdValidation{HouseholdValidationServiceClient: customersv1.NewHouseholdValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                           deps.Clients,
		telemetryReadingCalculationClient: telematicsv1.NewTelemetryReadingCalculationServiceClient(deps.Conn),
		priceOverrideLifecycleClient:      pricingv1.NewPriceOverrideLifecycleServiceClient(deps.Conn),
		priceListWorkflowClient:           pricingv1.NewPriceListWorkflowServiceClient(deps.Conn),
		patientCalculationClient:          patientsv1.NewPatientCalculationServiceClient(deps.Conn),
		priceQuoteCalculationClient:       pricingv1.NewPriceQuoteCalculationServiceClient(deps.Conn),
		roleHistoryClient:                 identityv1.NewRoleHistoryServiceClient(deps.Conn),
		customerCalculationClient:         customersv1.NewCustomerCalculationServiceClient(deps.Conn),
	}
}

// SubmitPriceOverride submits a price override.
func (s *PriceOverrideApprovalServer) SubmitPriceOverride(ctx context.Context, req *pricingv1.SubmitPriceOverrideRequest) (*pricingv1.SubmitPriceOverrideResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "submit price override", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	delegatePriceQuoteApprovalRequest := &pricingv1.DelegatePriceQuoteApprovalRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{
				Tax: &pricingv1.PriceQuoteLineTax{Amount: req.GetPriceOverride().GetDiscount().GetAmount()},
			}},
		},
		Details: &pricingv1.DelegatePriceQuoteApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetPriceOverride().GetOwner().GetEmail()},
		},
	}
	delegatePriceQuoteApprovalReply, err := s.meteredPriceQuoteApproval.DelegatePriceQuoteApproval(ctx, delegatePriceQuoteApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteApprovalService/DelegatePriceQuoteApproval")
	}
	searchRoutesRequest := buildSearchRoutesRequestForSubmitPriceOverride(req, delegatePriceQuoteApprovalReply)
	searchRoutesReply, err := s.routeClient.SearchRoutes(ctx, searchRoutesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.routing.v1.RouteService/SearchRoutes")
	}
	return &pricingv1.SubmitPriceOverrideResponse{
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				CreatedAt: delegatePriceQuoteApprovalReply.GetPriceQuote().GetAudit().GetCreatedAt(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
			Effective:       req.GetPriceOverride().GetEffective(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: delegatePriceQuoteApprovalReply.GetResult().GetCorrelationId(),
			Status:        searchRoutesReply.GetResult().GetStatus(),
			ReferenceIds:  searchRoutesReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// ApprovePriceOverride approves a price override.
func (s *PriceOverrideApprovalServer) ApprovePriceOverride(ctx context.Context, req *pricingv1.ApprovePriceOverrideRequest) (*pricingv1.ApprovePriceOverrideResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApprovePriceOverride", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "approve price override", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*pricingv1.ApprovePriceOverrideRequest_Details)
	cloned2 := proto.Clone(req.GetPriceOverride()).(*pricingv1.PriceOverride)
	cloneCustomerRequest := &customersv1.CloneCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				Email:       cloned.GetApprover().GetEmail(),
				UserId:      req.GetPriceOverride().GetOwner().GetUserId(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{Etag: cloned2.GetAudit().GetEtag()},
		},
	}
	send := s.cloneCustomerSender(cloneCustomerRequest)
	cloneCustomerReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CloneCustomer")
	}
	explainHouseholdDecisionRequest := &customersv1.ExplainHouseholdDecisionRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						CreatedBy: cloneCustomerReply.GetCustomer().GetAudit().GetCreatedBy(),
					},
				}},
			}},
		},
	}
	explainHouseholdDecisionReply, err := s.meteredHouseholdValidation.ExplainHouseholdDecision(ctx, explainHouseholdDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision")
	}
	priceQuoteApproval, err := registry.Lookup[pricingv1.PriceQuoteApprovalServiceClient](s.clients, "acme.pricing.v1.PriceQuoteApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	commentPriceQuoteApprovalRequest := &pricingv1.CommentPriceQuoteApprovalRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				Etag:      explainHouseholdDecisionReply.GetHousehold().GetAudit().GetEtag(),
				UpdatedAt: cloneCustomerReply.GetCustomer().GetAudit().GetUpdatedAt(),
			},
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		Details: &pricingv1.CommentPriceQuoteApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: explainHouseholdDecisionReply.GetHousehold().GetManager().GetUserId(),
			},
		},
	}
	commentPriceQuoteApprovalReply, err := priceQuoteApproval.CommentPriceQuoteApproval(ctx, commentPriceQuoteApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteApprovalService/CommentPriceQuoteApproval")
	}
	return &pricingv1.ApprovePriceOverrideResponse{
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				CreatedAt: cloneCustomerReply.GetCustomer().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: commentPriceQuoteApprovalReply.GetPriceQuote().GetCounterparty().GetDisplayName(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
			Effective:       req.GetPriceOverride().GetEffective(),
		},
		Result: &commonv1.OperationResult{
			Details: cloneCustomerReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &pricingv1.ApprovePriceOverrideResponse_Outcome{
			NextSteps: explainHouseholdDecisionReply.GetOutcome().GetNextSteps(),
		},
	}, nil
}

// RejectPriceOverride rejects a price override.
func (s *PriceOverrideApprovalServer) RejectPriceOverride(ctx context.Context, req *pricingv1.RejectPriceOverrideRequest) (*pricingv1.RejectPriceOverrideResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectPriceOverride", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "reject price override", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	recalculateTelemetryReadingsRequest := buildRecalculateTelemetryReadingsRequestForRejectPriceOverride(req)
	recalculateTelemetryReadingsReply, err := s.telemetryReadingCalculationClient.RecalculateTelemetryReadings(ctx, recalculateTelemetryReadingsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telematics.v1.TelemetryReadingCalculationService/RecalculateTelemetryReadings")
	}
	return &pricingv1.RejectPriceOverrideResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recalculateTelemetryReadingsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &pricingv1.RejectPriceOverrideResponse_Outcome{
			Message: recalculateTelemetryReadingsReply.GetResult().GetMessage(),
		},
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Status:          req.GetPriceOverride().GetStatus(),
		},
	}, nil
}

// EscalatePriceOverride escalates a price override.
func (s *PriceOverrideApprovalServer) EscalatePriceOverride(ctx context.Context, req *pricingv1.EscalatePriceOverrideRequest) (*pricingv1.EscalatePriceOverrideResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "escalate price override", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	suspendPriceOverrideRequest := &pricingv1.SuspendPriceOverrideRequest{
		Context:         req.GetContext(),
		PriceOverrideId: req.GetPriceOverrideId(),
		Details: &pricingv1.SuspendPriceOverrideRequest_Details{
			NotifyContact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				UserId:      req.GetPriceOverride().GetOwner().GetUserId(),
			},
		},
	}
	var suspendPriceOverrideReply *pricingv1.SuspendPriceOverrideResponse
	if err := pipeline.Run(ctx, "acme.pricing.v1.PriceOverrideLifecycleService/SuspendPriceOverride", func(ctx context.Context) error {
		answer, err := s.priceOverrideLifecycleClient.SuspendPriceOverride(ctx, suspendPriceOverrideRequest)
		suspendPriceOverrideReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideLifecycleService/SuspendPriceOverride")
	}
	priceQuoteQuery, err := registry.Lookup[pricingv1.PriceQuoteQueryServiceClient](s.clients, "acme.pricing.v1.PriceQuoteQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	browsePriceQuotesRequest := &pricingv1.BrowsePriceQuotesRequest{
		Context: req.GetContext(),
		Filter: &pricingv1.BrowsePriceQuotesRequest_Filter{
			CreatedAfter: req.GetPriceOverride().GetAudit().GetUpdatedAt(),
		},
	}
	browsePriceQuotesReply, err := priceQuoteQuery.BrowsePriceQuotes(ctx, browsePriceQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteQueryService/BrowsePriceQuotes")
	}
	customerCalculation, err := registry.Lookup[customersv1.CustomerCalculationServiceClient](s.clients, "acme.customers.v1.CustomerCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	quoteCustomerRequest := &customersv1.QuoteCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetPriceOverride().GetOwner().GetUserId()},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetPriceOverride().GetAudit().GetCreatedBy(),
				UpdatedAt: req.GetPriceOverride().GetAudit().GetUpdatedAt(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	quoteCustomerReply, err := customerCalculation.QuoteCustomer(ctx, quoteCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/QuoteCustomer")
	}
	currencyRateReporting, err := registry.Lookup[pricingv1.CurrencyRateReportingServiceClient](s.clients, "acme.pricing.v1.CurrencyRateReportingService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	scheduleCurrencyRateReportRequest := &pricingv1.ScheduleCurrencyRateReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetPriceOverride().GetBlock().GetReason(),
		Annotations: annotations,
	}
	scheduleCurrencyRateReportReply, err := currencyRateReporting.ScheduleCurrencyRateReport(ctx, scheduleCurrencyRateReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateReportingService/ScheduleCurrencyRateReport")
	}
	return &pricingv1.EscalatePriceOverrideResponse{
		Outcome: &pricingv1.EscalatePriceOverrideResponse_Outcome{
			Status:  suspendPriceOverrideReply.GetResult().GetStatus(),
			Message: browsePriceQuotesReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: browsePriceQuotesReply.GetResult().GetCorrelationId(),
			Message:       scheduleCurrencyRateReportReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		PriceOverride: &pricingv1.PriceOverride{
			Owner: &identityv1.UserRef{
				Email: quoteCustomerReply.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: quoteCustomerReply.GetCustomer().GetAudit().GetCreatedBy(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
			Priority:        req.GetPriceOverride().GetPriority(),
		},
	}, nil
}

// DelegatePriceOverrideApproval delegates a price override.
func (s *PriceOverrideApprovalServer) DelegatePriceOverrideApproval(ctx context.Context, req *pricingv1.DelegatePriceOverrideApprovalRequest) (*pricingv1.DelegatePriceOverrideApprovalResponse, error) {
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "delegate price override approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPriceOverride()).(*pricingv1.PriceOverride)
	advancePriceListWorkflowRequest := &pricingv1.AdvancePriceListWorkflowRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Audit:  &commonv1.AuditInfo{UpdatedAt: req.GetPriceOverride().GetAudit().GetUpdatedAt()},
			Owner:  &identityv1.UserRef{DisplayName: cloned.GetOwner().GetDisplayName()},
			Action: &pricingv1.PriceList_Route{Route: &pricingv1.PriceListRouteAction{Weight: req.GetPriceOverride().GetRoute().GetWeight()}},
		},
	}
	advancePriceListWorkflowReply, err := grpcx.Call(ctx, s.priceListWorkflowClient.AdvancePriceListWorkflow, advancePriceListWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListWorkflowService/AdvancePriceListWorkflow")
	}
	estimatePatientReply, err := s.patientCalculationClient.EstimatePatient(ctx, &patientsv1.EstimatePatientRequest{
		Context: req.GetContext(),
		Patient: &patientsv1.Patient{
			Audit: &commonv1.AuditInfo{
				CreatedBy: advancePriceListWorkflowReply.GetPriceList().GetAudit().GetCreatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.patients.v1.PatientCalculationService/EstimatePatient")
	}
	simulatePriceQuoteRequest := &pricingv1.SimulatePriceQuoteRequest{}
	simulatePriceQuoteRequest.Context = req.GetContext()
	simulatePriceQuoteRequest.PriceQuote = &pricingv1.PriceQuote{
		BillingAddress: &commonv1.PostalAddress{
			Recipient:   estimatePatientReply.GetPatient().GetPrimaryAddress().GetRecipient(),
			Locality:    estimatePatientReply.GetPatient().GetPrimaryAddress().GetLocality(),
			CountryCode: estimatePatientReply.GetPatient().GetPrimaryAddress().GetCountryCode(),
		},
		Number: estimatePatientReply.GetPatient().GetNationalId().GetNumber(),
	}
	simulatePriceQuoteReply, err := s.priceQuoteCalculationClient.SimulatePriceQuote(ctx, simulatePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/SimulatePriceQuote")
	}
	return &pricingv1.DelegatePriceOverrideApprovalResponse{
		Outcome: &pricingv1.DelegatePriceOverrideApprovalResponse_Outcome{
			NextSteps: advancePriceListWorkflowReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			Message: estimatePatientReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				Etag:      estimatePatientReply.GetPatient().GetAudit().GetEtag(),
				UpdatedAt: simulatePriceQuoteReply.GetPriceQuote().GetAudit().GetUpdatedAt(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
			Version:         req.GetPriceOverride().GetVersion(),
		},
	}, nil
}

// GetPriceOverrideApprovalStatus returns a price override.
func (s *PriceOverrideApprovalServer) GetPriceOverrideApprovalStatus(ctx context.Context, req *pricingv1.GetPriceOverrideApprovalStatusRequest) (*pricingv1.GetPriceOverrideApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetPriceOverrideApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_override", req.GetPriceOverrideId(), "get_price_override_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get price override approval status", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	step := &priceOverrideApprovalServerGetPriceOverrideApprovalStatusAnnotateRoleVersionStep{request: &identityv1.AnnotateRoleVersionRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				Email:  cloned.GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
		Annotations: baggage,
	}}
	if err := s.runGetPriceOverrideApprovalStatusAnnotateRoleVersion(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/AnnotateRoleVersion")
	}
	annotateRoleVersionReply := step.reply
	return &pricingv1.GetPriceOverrideApprovalStatusResponse{
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: annotateRoleVersionReply.GetRole().GetAudit().GetUpdatedAt(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentPriceOverrideApproval comments a price override.
func (s *PriceOverrideApprovalServer) CommentPriceOverrideApproval(ctx context.Context, req *pricingv1.CommentPriceOverrideApprovalRequest) (*pricingv1.CommentPriceOverrideApprovalResponse, error) {
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "comment price override approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetPriceOverride().GetOwner().GetDisplayName())
	simulateCustomerRequest := &customersv1.SimulateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact:             &commonv1.ContactInfo{Email: req.GetPriceOverride().GetOwner().GetEmail()},
			RelationshipManager: &identityv1.UserRef{DisplayName: tenancy.Value(ctx, "display_name")},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetPriceOverride().GetAudit().GetUpdatedBy(),
				CreatedBy: req.GetPriceOverride().GetAudit().GetCreatedBy(),
			},
		},
	}
	type simulateCustomerResult struct {
		reply *customersv1.SimulateCustomerResponse
		err   error
	}
	simulateCustomerResults := make(chan simulateCustomerResult, 1)
	go func() {
		reply, err := s.customerCalculationClient.SimulateCustomer(ctx, simulateCustomerRequest)
		simulateCustomerResults <- simulateCustomerResult{reply, err}
	}()
	var simulateCustomerReply *customersv1.SimulateCustomerResponse
	select {
	case result := <-simulateCustomerResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.CustomerCalculationService/SimulateCustomer")
		}
		simulateCustomerReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &pricingv1.CommentPriceOverrideApprovalResponse{
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulateCustomerReply.GetCustomer().GetAudit().GetUpdatedAt(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
			Version:         req.GetPriceOverride().GetVersion(),
		},
		Outcome: &pricingv1.CommentPriceOverrideApprovalResponse_Outcome{Message: simulateCustomerReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// cloneCustomerSender returns a function that sends one request to acme.customers.v1.CustomerService/CloneCustomer.
func (s *PriceOverrideApprovalServer) cloneCustomerSender(request *customersv1.CloneCustomerRequest) func(context.Context) (*customersv1.CloneCustomerResponse, error) {
	return func(ctx context.Context) (*customersv1.CloneCustomerResponse, error) {
		return s.customerClient.CloneCustomer(ctx, request)
	}
}

// priceOverrideApprovalServerGetPriceOverrideApprovalStatusAnnotateRoleVersionStep carries the request of one step of
// GetPriceOverrideApprovalStatus and, once it ran, its reply.
type priceOverrideApprovalServerGetPriceOverrideApprovalStatusAnnotateRoleVersionStep struct {
	request *identityv1.AnnotateRoleVersionRequest
	reply   *identityv1.AnnotateRoleVersionResponse
}

func (s *PriceOverrideApprovalServer) runGetPriceOverrideApprovalStatusAnnotateRoleVersion(ctx context.Context, step *priceOverrideApprovalServerGetPriceOverrideApprovalStatusAnnotateRoleVersionStep) error {
	reply, err := s.roleHistoryClient.AnnotateRoleVersion(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
