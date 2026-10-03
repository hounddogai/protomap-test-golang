package fraud

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	ticketingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ticketing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// RiskAssessmentValidationServer implements `acme.fraud.v1.RiskAssessmentValidationService`.
// Checks risk assessments against the rules before anything acts on them.
type RiskAssessmentValidationServer struct {
	fraudv1.UnimplementedRiskAssessmentValidationServiceServer
	store                                   *Store
	log                                     *slog.Logger
	clients                                 *registry.Registry
	conn                                    *grpc.ClientConn
	riskAssessmentCalculationClient         fraudv1.RiskAssessmentCalculationServiceClient
	fraudSignalCalculationClient            fraudv1.FraudSignalCalculationServiceClient
	quoteReturnRequestFn                    func(context.Context, *returnsv1.QuoteReturnRequestRequest, ...grpc.CallOption) (*returnsv1.QuoteReturnRequestResponse, error)
	inspectionCalculationClient             returnsv1.InspectionCalculationServiceClient
	synonymValidationClient                 searchv1.SynonymValidationServiceClient
	synonymSyncClient                       searchv1.SynonymSyncServiceClient
	riskAssessmentCalculationClientFallback fraudv1.RiskAssessmentCalculationServiceClient
	meteredRiskAssessmentCalculation        meteredRiskAssessmentCalculation
	ticketQueryClient                       ticketingv1.TicketQueryServiceClient
	manifestCalculationClient               shippingv1.ManifestCalculationServiceClient
}

// NewRiskAssessmentValidationServer wires a RiskAssessmentValidationServer.
func NewRiskAssessmentValidationServer(deps platform.Deps) *RiskAssessmentValidationServer {
	returnRequestCalculation := returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn)
	return &RiskAssessmentValidationServer{
		store:                                   NewStore(deps.DB),
		log:                                     deps.Logger.With("service", "acme.fraud.v1.RiskAssessmentValidationService"),
		clients:                                 deps.Clients,
		conn:                                    deps.Conn,
		riskAssessmentCalculationClient:         fraudv1.NewRiskAssessmentCalculationServiceClient(deps.Conn),
		fraudSignalCalculationClient:            fraudv1.NewFraudSignalCalculationServiceClient(deps.Conn),
		quoteReturnRequestFn:                    returnRequestCalculation.QuoteReturnRequest,
		inspectionCalculationClient:             returnsv1.NewInspectionCalculationServiceClient(deps.Conn),
		synonymValidationClient:                 searchv1.NewSynonymValidationServiceClient(deps.Conn),
		synonymSyncClient:                       searchv1.NewSynonymSyncServiceClient(deps.Conn),
		riskAssessmentCalculationClientFallback: fraudv1.NewRiskAssessmentCalculationServiceClient(deps.Conn),
		meteredRiskAssessmentCalculation:        meteredRiskAssessmentCalculation{RiskAssessmentCalculationServiceClient: fraudv1.NewRiskAssessmentCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		ticketQueryClient:                       ticketingv1.NewTicketQueryServiceClient(deps.Conn),
		manifestCalculationClient:               shippingv1.NewManifestCalculationServiceClient(deps.Conn),
	}
}

// ValidateRiskAssessment validates a risk assessment.
func (s *RiskAssessmentValidationServer) ValidateRiskAssessment(ctx context.Context, req *fraudv1.ValidateRiskAssessmentRequest) (*fraudv1.ValidateRiskAssessmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "validate risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	returnRequestValidation, err := registry.Lookup[returnsv1.ReturnRequestValidationServiceClient](s.clients, "acme.returns.v1.ReturnRequestValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var holder struct{ Etag string }
	if record := req.GetRiskAssessment().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Etag"); err != nil {
			s.log.WarnContext(ctx, "copy etag", "error", err)
		}
	}
	explainReturnRequestDecisionRequest := &returnsv1.ExplainReturnRequestDecisionRequest{
		Context: req.GetContext(),
		Details: &returnsv1.ExplainReturnRequestDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetRiskAssessment().GetAssignee().GetDisplayName(),
				Email:       req.GetRiskAssessment().GetCustomer().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{Etag: holder.Etag},
		},
	}
	explainReturnRequestDecisionReply, err := returnRequestValidation.ExplainReturnRequestDecision(ctx, explainReturnRequestDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestValidationService/ExplainReturnRequestDecision")
	}
	return &fraudv1.ValidateRiskAssessmentResponse{
		Result: &commonv1.OperationResult{
			Message: explainReturnRequestDecisionReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			RiskAssessmentId: req.GetRiskAssessmentId(),
			CustomFields:     req.GetRiskAssessment().GetCustomFields(),
		},
	}, nil
}

// PreviewRiskAssessment previews a risk assessment.
func (s *RiskAssessmentValidationServer) PreviewRiskAssessment(ctx context.Context, req *fraudv1.PreviewRiskAssessmentRequest) (*fraudv1.PreviewRiskAssessmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewRiskAssessment", "error", err)
		return nil, err
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "preview risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	getRiskAssessmentRatesRequest := &fraudv1.GetRiskAssessmentRatesRequest{
		Context:          req.GetContext(),
		RiskAssessmentId: req.GetRiskAssessmentId(),
	}
	getRiskAssessmentRatesReply := new(fraudv1.GetRiskAssessmentRatesResponse)
	if err := grpcx.Invoke(ctx, s.conn, fraudv1.RiskAssessmentCalculationService_GetRiskAssessmentRates_FullMethodName, getRiskAssessmentRatesRequest, getRiskAssessmentRatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/GetRiskAssessmentRates")
	}
	compareRiskAssessmentOptionsRequest := buildCompareRiskAssessmentOptionsRequestForPreviewRiskAssessment(req, getRiskAssessmentRatesReply)
	compareRiskAssessmentOptionsReply, err := s.riskAssessmentCalculationClient.CompareRiskAssessmentOptions(ctx, compareRiskAssessmentOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/CompareRiskAssessmentOptions")
	}
	return &fraudv1.PreviewRiskAssessmentResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: getRiskAssessmentRatesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: getRiskAssessmentRatesReply.GetRiskAssessment().GetAudit().GetCreatedAt(),
				CreatedBy: compareRiskAssessmentOptionsReply.GetRiskAssessment().GetAudit().GetCreatedBy(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Priority:         req.GetRiskAssessment().GetPriority(),
		},
	}, nil
}

// CheckRiskAssessmentEligibility checks a risk assessment.
func (s *RiskAssessmentValidationServer) CheckRiskAssessmentEligibility(ctx context.Context, req *fraudv1.CheckRiskAssessmentEligibilityRequest) (*fraudv1.CheckRiskAssessmentEligibilityResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "check risk assessment eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetRiskAssessment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainFraudSignalCalculationReply, err := s.fraudSignalCalculationClient.ExplainFraudSignalCalculation(ctx, &fraudv1.ExplainFraudSignalCalculationRequest{
		Context: req.GetContext(),
		Details: &fraudv1.ExplainFraudSignalCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetRiskAssessment().GetCustomer().GetEmail(),
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
		FraudSignal: &fraudv1.FraudSignal{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalCalculationService/ExplainFraudSignalCalculation")
	}
	riskAssessmentCalculation, ok := s.clients.Get("acme.fraud.v1.RiskAssessmentCalculationService").(fraudv1.RiskAssessmentCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.fraud.v1.RiskAssessmentCalculationService")
	}
	cloned := proto.Clone(req.GetRiskAssessment()).(*fraudv1.RiskAssessment)
	detailsCache := cache.New[string, *fraudv1.CheckRiskAssessmentEligibilityRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	quoteRiskAssessmentRequest := &fraudv1.QuoteRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin: &fraudv1.RiskAssessment_Channel{Channel: &fraudv1.RiskAssessmentChannelOrigin{Channel: req.GetRiskAssessment().GetChannel().GetChannel()}},
			Requester: &commonv1.ContactInfo{
				Email:            cloned.GetAssignee().GetEmail(),
				PreferredChannel: cachedDetails.GetContact().GetPreferredChannel(),
			},
		},
	}
	quoteRiskAssessmentReply, err := riskAssessmentCalculation.QuoteRiskAssessment(ctx, quoteRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/QuoteRiskAssessment")
	}
	return &fraudv1.CheckRiskAssessmentEligibilityResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				DisplayName: explainFraudSignalCalculationReply.GetFraudSignal().GetSubject().GetDisplayName(),
			},
			Requester: &commonv1.ContactInfo{
				Name: quoteRiskAssessmentReply.GetRiskAssessment().GetRequester().GetName(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			CustomFields:     req.GetRiskAssessment().GetCustomFields(),
		},
		Outcome: &fraudv1.CheckRiskAssessmentEligibilityResponse_Outcome{
			NextSteps: explainFraudSignalCalculationReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: quoteRiskAssessmentReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// NormalizeRiskAssessment normalizes a risk assessment.
func (s *RiskAssessmentValidationServer) NormalizeRiskAssessment(ctx context.Context, req *fraudv1.NormalizeRiskAssessmentRequest) (*fraudv1.NormalizeRiskAssessmentResponse, error) {
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "normalize risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	quoteReturnRequestRequest := &returnsv1.QuoteReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				Email:       req.GetRiskAssessment().GetRequester().GetEmail(),
				DisplayName: req.GetRiskAssessment().GetAssignee().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetRiskAssessment().GetAudit().GetUpdatedAt(),
				UpdatedBy: req.GetRiskAssessment().GetAudit().GetUpdatedBy(),
			},
		},
	}
	quoteReturnRequestReply, err := s.quoteReturnRequestFn(ctx, quoteReturnRequestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/QuoteReturnRequest")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := riskAssessmentValidationServerSimulateInspectionSteps[mode]
	ctx = tenancy.With(ctx, "customer_id", req.GetRiskAssessment().GetCustomer().GetCustomerId())
	simulateInspectionRequest := &returnsv1.SimulateInspectionRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Origin: &returnsv1.Inspection_Customer{Customer: &customersv1.CustomerRef{CustomerId: tenancy.Value(ctx, "customer_id")}},
			Comments: []*returnsv1.InspectionComment{&returnsv1.InspectionComment{
				CreatedAt: quoteReturnRequestReply.GetReturnRequest().GetAudit().GetCreatedAt(),
			}},
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetRiskAssessment().GetAssignee().GetDisplayName(),
			},
			Sla: &returnsv1.InspectionSla{PolicyId: req.GetRiskAssessment().GetSla().GetPolicyId()},
		},
	}
	simulateInspectionReply, err := run(s, ctx, simulateInspectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/SimulateInspection")
	}
	return &fraudv1.NormalizeRiskAssessmentResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: quoteReturnRequestReply.GetReturnRequest().GetAudit().GetUpdatedBy(),
				Etag:      quoteReturnRequestReply.GetReturnRequest().GetAudit().GetEtag(),
				UpdatedAt: simulateInspectionReply.GetInspection().GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{
				Email: simulateInspectionReply.GetInspection().GetRequester().GetEmail(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Subject:          req.GetRiskAssessment().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainRiskAssessmentDecision explains a risk assessment.
func (s *RiskAssessmentValidationServer) ExplainRiskAssessmentDecision(ctx context.Context, req *fraudv1.ExplainRiskAssessmentDecisionRequest) (*fraudv1.ExplainRiskAssessmentDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "explain risk assessment decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	var address string
	switch choice := req.GetRiskAssessment().GetOrigin().(type) {
	case *fraudv1.RiskAssessment_Customer:
		address = choice.Customer.GetCustomerId()
	case *fraudv1.RiskAssessment_ExternalReference:
		address = choice.ExternalReference
	case *fraudv1.RiskAssessment_Channel:
		address = choice.Channel.GetAddress()
	}
	source := req.GetRiskAssessment().GetRequester().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	calculateRiskAssessmentRequest := &fraudv1.CalculateRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin:   &fraudv1.RiskAssessment_Channel{Channel: &fraudv1.RiskAssessmentChannelOrigin{Address: address}},
			Sla:      &fraudv1.RiskAssessmentSla{DueAt: req.GetRiskAssessment().GetSla().GetDueAt()},
			Assignee: &identityv1.UserRef{Email: email},
		},
	}
	calculateRiskAssessmentReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*fraudv1.CalculateRiskAssessmentResponse, error) {
		return s.riskAssessmentCalculationClient.CalculateRiskAssessment(ctx, calculateRiskAssessmentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/CalculateRiskAssessment")
	}
	lintSynonymsRequest := &searchv1.LintSynonymsRequest{
		Context: req.GetContext(),
		Details: &searchv1.LintSynonymsRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
	}
	lintSynonymsReply, err := searchv1.SynonymValidationServiceClient.LintSynonyms(s.synonymValidationClient, ctx, lintSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/LintSynonyms")
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetContact().GetPreferredChannel())
	compareRiskAssessmentOptionsRequest := &fraudv1.CompareRiskAssessmentOptionsRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				DisplayName: calculateRiskAssessmentReply.GetRiskAssessment().GetCustomer().GetDisplayName(),
				Email:       req.GetDetails().GetContact().GetEmail(),
			},
			Requester: &commonv1.ContactInfo{
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
				Email:            calculateRiskAssessmentReply.GetRiskAssessment().GetCustomer().GetEmail(),
			},
		},
	}
	compareRiskAssessmentOptionsReply := new(fraudv1.CompareRiskAssessmentOptionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, fraudv1.RiskAssessmentCalculationService_CompareRiskAssessmentOptions_FullMethodName, compareRiskAssessmentOptionsRequest, compareRiskAssessmentOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/CompareRiskAssessmentOptions")
	}
	for comment := range iterx.Each(req.GetRiskAssessment().GetComments(), func(candidate *fraudv1.RiskAssessmentComment) bool { return candidate != nil }) {
		if _, err := s.synonymSyncClient.SyncSynonyms(ctx, &searchv1.SyncSynonymsRequest{
			Context: req.GetContext(),
			Synonym: &searchv1.Synonym{
				Audit: &commonv1.AuditInfo{
					CreatedAt: comment.GetCreatedAt(),
					Etag:      req.GetRiskAssessment().GetAudit().GetEtag(),
				},
				Owner: &identityv1.UserRef{Email: comment.GetAuthorEmail()},
				Action: &searchv1.Synonym_Route{Route: &searchv1.SynonymRouteAction{
					Target: lintSynonymsReply.GetSynonym().GetRoute().GetTarget(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/SyncSynonyms")
		}
	}
	return &fraudv1.ExplainRiskAssessmentDecisionResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Requester:        calculateRiskAssessmentReply.GetRiskAssessment().GetRequester(),
			Assignee:         compareRiskAssessmentOptionsReply.GetRiskAssessment().GetAssignee(),
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Priority:         req.GetRiskAssessment().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			Details: lintSynonymsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &fraudv1.ExplainRiskAssessmentDecisionResponse_Outcome{
			Message: compareRiskAssessmentOptionsReply.GetResult().GetMessage(),
		},
	}, nil
}

// ScoreRiskAssessment scores a risk assessment.
func (s *RiskAssessmentValidationServer) ScoreRiskAssessment(ctx context.Context, req *fraudv1.ScoreRiskAssessmentRequest) (*fraudv1.ScoreRiskAssessmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "score risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *fraudv1.QuoteRiskAssessmentRequest, ...grpc.CallOption) (*fraudv1.QuoteRiskAssessmentResponse, error){
		"primary":  s.riskAssessmentCalculationClient.QuoteRiskAssessment,
		"fallback": s.riskAssessmentCalculationClientFallback.QuoteRiskAssessment,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	var email string
	switch choice := req.GetRiskAssessment().GetOrigin().(type) {
	case *fraudv1.RiskAssessment_Customer:
		email = choice.Customer.GetEmail()
	case *fraudv1.RiskAssessment_ExternalReference:
		email = choice.ExternalReference
	case *fraudv1.RiskAssessment_Channel:
		email = choice.Channel.GetChannel()
	}
	quoteRiskAssessmentRequest := &fraudv1.QuoteRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{
				DisplayName: req.GetRiskAssessment().GetCustomer().GetDisplayName(),
			}},
			Requester: &commonv1.ContactInfo{Email: email},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetRiskAssessment().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &fraudv1.QuoteRiskAssessmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
	}
	quoteRiskAssessmentReply, err := route(ctx, quoteRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/QuoteRiskAssessment")
	}
	explainRiskAssessmentCalculationRequest := &fraudv1.ExplainRiskAssessmentCalculationRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetRiskAssessment().GetAudit().GetUpdatedAt(),
			},
		},
	}
	explainRiskAssessmentCalculationReply, err := s.meteredRiskAssessmentCalculation.ExplainRiskAssessmentCalculation(ctx, explainRiskAssessmentCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/ExplainRiskAssessmentCalculation")
	}
	estimateRiskAssessmentReply, err := s.riskAssessmentCalculationClient.EstimateRiskAssessment(ctx, &fraudv1.EstimateRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Sla: &fraudv1.RiskAssessmentSla{
				PausedFor: quoteRiskAssessmentReply.GetRiskAssessment().GetSla().GetPausedFor(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetRiskAssessment().GetAssignee().GetDisplayName(),
			},
			Requester: &commonv1.ContactInfo{
				Email: quoteRiskAssessmentReply.GetRiskAssessment().GetRequester().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/EstimateRiskAssessment")
	}
	source := req.GetDetails().GetDestination().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	exportTicketsStream, err := s.ticketQueryClient.ExportTickets(ctx, &ticketingv1.ExportTicketsRequest{
		Context: req.GetContext(),
		Filter: &ticketingv1.ExportTicketsRequest_Filter{
			Region:       region,
			CreatedAfter: req.GetRiskAssessment().GetSla().GetDueAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ticketing.v1.TicketQueryService/ExportTickets")
	}
	for {
		received, err := exportTicketsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.ticketing.v1.TicketQueryService/ExportTickets")
		}
		_ = received
	}
	return &fraudv1.ScoreRiskAssessmentResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Sla: &fraudv1.RiskAssessmentSla{
				PolicyId: quoteRiskAssessmentReply.GetRiskAssessment().GetSla().GetPolicyId(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: quoteRiskAssessmentReply.GetRiskAssessment().GetAudit().GetUpdatedBy(),
				CreatedAt: estimateRiskAssessmentReply.GetRiskAssessment().GetAudit().GetCreatedAt(),
			},
			Requester: &commonv1.ContactInfo{
				Phone: explainRiskAssessmentCalculationReply.GetRiskAssessment().GetRequester().GetPhone(),
			},
			Assignee:         estimateRiskAssessmentReply.GetRiskAssessment().GetAssignee(),
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Subject:          req.GetRiskAssessment().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintRiskAssessments lints a risk assessment.
func (s *RiskAssessmentValidationServer) LintRiskAssessments(ctx context.Context, req *fraudv1.LintRiskAssessmentsRequest) (*fraudv1.LintRiskAssessmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintRiskAssessments", "error", err)
		return nil, err
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "lint risk assessments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	recalculateRiskAssessmentsRequest := buildRecalculateRiskAssessmentsRequestForLintRiskAssessments(req)
	recalculateRiskAssessmentsReply, err := s.riskAssessmentCalculationClient.RecalculateRiskAssessments(ctx, recalculateRiskAssessmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/RecalculateRiskAssessments")
	}
	for comment := range iterx.Each(req.GetRiskAssessment().GetComments(), func(candidate *fraudv1.RiskAssessmentComment) bool { return candidate != nil }) {
		if _, err := s.manifestCalculationClient.EstimateManifest(ctx, &shippingv1.EstimateManifestRequest{
			Context: req.GetContext(),
			Manifest: &shippingv1.Manifest{
				Audit: &commonv1.AuditInfo{
					CreatedAt: comment.GetCreatedAt(),
					UpdatedBy: recalculateRiskAssessmentsReply.GetRiskAssessment().GetAudit().GetUpdatedBy(),
					Etag:      req.GetRiskAssessment().GetAudit().GetEtag(),
				},
				Counterparty: &customersv1.CustomerRef{Email: comment.GetAuthorEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/EstimateManifest")
		}
	}
	for comment2 := range iterx.Each(req.GetRiskAssessment().GetComments(), func(candidate2 *fraudv1.RiskAssessmentComment) bool { return candidate2 != nil }) {
		if _, err := s.riskAssessmentCalculationClient.QuoteRiskAssessment(ctx, &fraudv1.QuoteRiskAssessmentRequest{
			Context: req.GetContext(),
			RiskAssessment: &fraudv1.RiskAssessment{
				Comments: []*fraudv1.RiskAssessmentComment{&fraudv1.RiskAssessmentComment{
					CommentId:   comment2.GetCommentId(),
					AuthorEmail: comment2.GetAuthorEmail(),
				}},
				Audit: &commonv1.AuditInfo{CreatedAt: comment2.GetCreatedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/QuoteRiskAssessment")
		}
	}
	jobs := make(chan *fraudv1.RiskAssessmentStatusChange)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				if _, err := s.riskAssessmentCalculationClient.SimulateRiskAssessment(workCtx, &fraudv1.SimulateRiskAssessmentRequest{
					Context: req.GetContext(),
					RiskAssessment: &fraudv1.RiskAssessment{
						History: []*fraudv1.RiskAssessmentStatusChange{&fraudv1.RiskAssessmentStatusChange{ToStatus: item.GetToStatus()}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetRiskAssessment().GetHistory() {
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
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/SimulateRiskAssessment")
	}
	return &fraudv1.LintRiskAssessmentsResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				Etag: recalculateRiskAssessmentsReply.GetRiskAssessment().GetAudit().GetEtag(),
			},
			RiskAssessmentId: recalculateRiskAssessmentsReply.GetRiskAssessment().GetRiskAssessmentId(),
			Requester:        req.GetRiskAssessment().GetRequester(),
			Priority:         req.GetRiskAssessment().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// riskAssessmentValidationServerSimulateInspectionSteps are the ways to run
// acme.returns.v1.InspectionCalculationService/SimulateInspection, by name.
var riskAssessmentValidationServerSimulateInspectionSteps = map[string]func(*RiskAssessmentValidationServer, context.Context, *returnsv1.SimulateInspectionRequest) (*returnsv1.SimulateInspectionResponse, error){
	"default": (*RiskAssessmentValidationServer).simulateInspectionStep,
	"audited": (*RiskAssessmentValidationServer).simulateInspectionAuditedStep,
}

func (s *RiskAssessmentValidationServer) simulateInspectionStep(ctx context.Context, request *returnsv1.SimulateInspectionRequest) (*returnsv1.SimulateInspectionResponse, error) {
	return s.inspectionCalculationClient.SimulateInspection(ctx, request)
}

func (s *RiskAssessmentValidationServer) simulateInspectionAuditedStep(ctx context.Context, request *returnsv1.SimulateInspectionRequest) (*returnsv1.SimulateInspectionResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.returns.v1.InspectionCalculationService/SimulateInspection")
	return s.inspectionCalculationClient.SimulateInspection(ctx, request)
}
