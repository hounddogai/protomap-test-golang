package fraud

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// FraudSignalValidationServer implements `acme.fraud.v1.FraudSignalValidationService`.
// Checks fraud signals against the rules before anything acts on them.
type FraudSignalValidationServer struct {
	fraudv1.UnsafeFraudSignalValidationServiceServer
	store                               *Store
	log                                 *slog.Logger
	returnRequestCalculationClient      returnsv1.ReturnRequestCalculationServiceClient
	riskAssessmentValidationClient      fraudv1.RiskAssessmentValidationServiceClient
	clients                             *registry.Registry
	explainRiskAssessmentDecisionFn     func(context.Context, *fraudv1.ExplainRiskAssessmentDecisionRequest, ...grpc.CallOption) (*fraudv1.ExplainRiskAssessmentDecisionResponse, error)
	searchIndexExportClient             searchv1.SearchIndexExportServiceClient
	giftCardTransactionValidationClient giftcardsv1.GiftCardTransactionValidationServiceClient
	conn                                *grpc.ClientConn
}

// NewFraudSignalValidationServer wires a FraudSignalValidationServer.
func NewFraudSignalValidationServer(deps platform.Deps) *FraudSignalValidationServer {
	riskAssessmentValidation := fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn)
	return &FraudSignalValidationServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.fraud.v1.FraudSignalValidationService"),
		returnRequestCalculationClient:      returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn),
		riskAssessmentValidationClient:      fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn),
		clients:                             deps.Clients,
		explainRiskAssessmentDecisionFn:     riskAssessmentValidation.ExplainRiskAssessmentDecision,
		searchIndexExportClient:             searchv1.NewSearchIndexExportServiceClient(deps.Conn),
		giftCardTransactionValidationClient: giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn),
		conn:                                deps.Conn,
	}
}

// ValidateFraudSignal validates a fraud signal.
func (s *FraudSignalValidationServer) ValidateFraudSignal(ctx context.Context, req *fraudv1.ValidateFraudSignalRequest) (*fraudv1.ValidateFraudSignalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "validate fraud signal", "tenant", req.GetContext().GetTenantId())
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	compareReturnRequestOptionsRequest := newCompareReturnRequestOptionsRequest(withCompareReturnRequestOptionsContext(req.GetContext()), withCompareReturnRequestOptionsReturnRequest(&returnsv1.ReturnRequest{
		Counterparty: &customersv1.CustomerRef{
			DisplayName: req.GetFraudSignal().GetSubject().GetDisplayName(),
		},
	}), withCompareReturnRequestOptionsReturnRequestId(envelope.GetDisplayName()))
	compareReturnRequestOptionsReply, err := s.returnRequestCalculationClient.CompareReturnRequestOptions(ctx, compareReturnRequestOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/CompareReturnRequestOptions")
	}
	var envelope2 fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintRiskAssessmentsReply, err := s.riskAssessmentValidationClient.LintRiskAssessments(ctx, &fraudv1.LintRiskAssessmentsRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: compareReturnRequestOptionsReply.GetReturnRequest().GetAudit().GetCreatedAt(),
			},
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
		RiskAssessmentId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/LintRiskAssessments")
	}
	var envelope3 fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &fraudSignalValidationServerValidateFraudSignalNormalizeRiskAssessmentStep{request: &fraudv1.NormalizeRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin: &fraudv1.RiskAssessment_Channel{Channel: &fraudv1.RiskAssessmentChannelOrigin{
				Channel: lintRiskAssessmentsReply.GetRiskAssessment().GetChannel().GetChannel(),
			}},
			Sla: &fraudv1.RiskAssessmentSla{
				DueAt: lintRiskAssessmentsReply.GetRiskAssessment().GetSla().GetDueAt(),
			},
		},
		RiskAssessmentId: envelope3.GetDisplayName(),
	}}
	if err := s.runValidateFraudSignalNormalizeRiskAssessment(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/NormalizeRiskAssessment")
	}
	normalizeRiskAssessmentReply := step.reply
	return &fraudv1.ValidateFraudSignalResponse{
		Outcome: &fraudv1.ValidateFraudSignalResponse_Outcome{
			Status: compareReturnRequestOptionsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: compareReturnRequestOptionsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  lintRiskAssessmentsReply.GetResult().GetReferenceIds(),
			Status:        normalizeRiskAssessmentReply.GetOutcome().GetStatus(),
			Message:       normalizeRiskAssessmentReply.GetResult().GetMessage(),
		},
		FraudSignal: &fraudv1.FraudSignal{
			FraudSignalId: req.GetFraudSignalId(),
			Source:        req.GetFraudSignal().GetSource(),
		},
	}, nil
}

// PreviewFraudSignal previews a fraud signal.
func (s *FraudSignalValidationServer) PreviewFraudSignal(ctx context.Context, req *fraudv1.PreviewFraudSignalRequest) (*fraudv1.PreviewFraudSignalResponse, error) {
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "preview fraud signal", "tenant", req.GetContext().GetTenantId())
	fraudRuleCalculation, ok := s.clients.Get("acme.fraud.v1.FraudRuleCalculationService").(fraudv1.FraudRuleCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.fraud.v1.FraudRuleCalculationService")
	}
	cloned := proto.Clone(req.GetFraudSignal()).(*fraudv1.FraudSignal)
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recalculateFraudRulesRequest := &fraudv1.RecalculateFraudRulesRequest{
		Context: req.GetContext(),
		FraudRule: &fraudv1.FraudRule{
			Owner:   &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
			Version: cloned.GetSubject().GetVersion(),
		},
		FraudRuleId: envelope.GetDisplayName(),
	}
	recalculateFraudRulesReply, err := fraudRuleCalculation.RecalculateFraudRules(ctx, recalculateFraudRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleCalculationService/RecalculateFraudRules")
	}
	var envelope2 fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainRiskAssessmentDecisionRequest := &fraudv1.ExplainRiskAssessmentDecisionRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recalculateFraudRulesReply.GetFraudRule().GetAudit().GetUpdatedAt(),
				CreatedAt: recalculateFraudRulesReply.GetFraudRule().GetAudit().GetCreatedAt(),
			},
			Assignee: &identityv1.UserRef{
				Email:       req.GetDetails().GetContact().GetEmail(),
				DisplayName: recalculateFraudRulesReply.GetFraudRule().GetOwner().GetDisplayName(),
			},
		},
		RiskAssessmentId: envelope2.GetDisplayName(),
	}
	explainRiskAssessmentDecisionReply, err := s.explainRiskAssessmentDecisionFn(ctx, explainRiskAssessmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/ExplainRiskAssessmentDecision")
	}
	return &fraudv1.PreviewFraudSignalResponse{
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				FraudRuleId: recalculateFraudRulesReply.GetFraudRule().GetFraudRuleId(),
				DisplayName: explainRiskAssessmentDecisionReply.GetRiskAssessment().GetAssignee().GetDisplayName(),
			},
			FraudSignalId: req.GetFraudSignalId(),
			Kind:          req.GetFraudSignal().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: recalculateFraudRulesReply.GetResult().GetStatus()},
	}, nil
}

// CheckFraudSignalEligibility checks a fraud signal.
func (s *FraudSignalValidationServer) CheckFraudSignalEligibility(ctx context.Context, req *fraudv1.CheckFraudSignalEligibilityRequest) (*fraudv1.CheckFraudSignalEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckFraudSignalEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "check fraud signal eligibility", "tenant", req.GetContext().GetTenantId())
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintRiskAssessmentsRequest := &fraudv1.LintRiskAssessmentsRequest{
		Context: req.GetContext(),
		Details: &fraudv1.LintRiskAssessmentsRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetContact().GetName(),
				Phone: req.GetDetails().GetContact().GetPhone(),
			},
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{
				DisplayName: req.GetFraudSignal().GetSubject().GetDisplayName(),
			}},
			Assignee: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
		},
		RiskAssessmentId: envelope.GetDisplayName(),
	}
	lintRiskAssessmentsReply, err := lintRiskAssessmentsCall(ctx, lintRiskAssessmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/LintRiskAssessments")
	}
	subscriptionPlanQuery, err := registry.Lookup[subscriptionsv1.SubscriptionPlanQueryServiceClient](s.clients, "acme.subscriptions.v1.SubscriptionPlanQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var envelope2 fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	summarizeSubscriptionPlansRequest := &subscriptionsv1.SummarizeSubscriptionPlansRequest{
		Context:            req.GetContext(),
		Reason:             req.GetReason(),
		SubscriptionPlanId: envelope2.GetDisplayName(),
	}
	summarizeSubscriptionPlansReply, err := subscriptionPlanQuery.SummarizeSubscriptionPlans(ctx, summarizeSubscriptionPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/SummarizeSubscriptionPlans")
	}
	streamSearchIndexExportStream, err := s.searchIndexExportClient.StreamSearchIndexExport(ctx, &searchv1.StreamSearchIndexExportRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/StreamSearchIndexExport")
	}
	for {
		received, err := streamSearchIndexExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/StreamSearchIndexExport")
		}
		_ = received
	}
	riskAssessmentValidation, ok := s.clients.Get("acme.fraud.v1.RiskAssessmentValidationService").(fraudv1.RiskAssessmentValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.fraud.v1.RiskAssessmentValidationService")
	}
	var envelope3 fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	previewRiskAssessmentRequest := &fraudv1.PreviewRiskAssessmentRequest{
		Context: req.GetContext(),
		Details: &fraudv1.PreviewRiskAssessmentRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name: lintRiskAssessmentsReply.GetRiskAssessment().GetRequester().GetName(),
			},
		},
		RiskAssessmentId: envelope3.GetDisplayName(),
	}
	previewRiskAssessmentReply, err := riskAssessmentValidation.PreviewRiskAssessment(ctx, previewRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/PreviewRiskAssessment")
	}
	return &fraudv1.CheckFraudSignalEligibilityResponse{
		Result: &commonv1.OperationResult{
			Details: lintRiskAssessmentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &fraudv1.CheckFraudSignalEligibilityResponse_Outcome{
			ReferenceId: lintRiskAssessmentsReply.GetOutcome().GetReferenceId(),
			Message:     summarizeSubscriptionPlansReply.GetResult().GetMessage(),
			Status:      previewRiskAssessmentReply.GetOutcome().GetStatus(),
		},
		FraudSignal: &fraudv1.FraudSignal{
			FraudSignalId: req.GetFraudSignalId(),
			Attributes:    req.GetFraudSignal().GetAttributes(),
		},
	}, nil
}

// NormalizeFraudSignal normalizes a fraud signal.
func (s *FraudSignalValidationServer) NormalizeFraudSignal(ctx context.Context, req *fraudv1.NormalizeFraudSignalRequest) (*fraudv1.NormalizeFraudSignalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "normalize fraud signal", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFraudSignal()).(*fraudv1.FraudSignal)
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listSearchIndexExportsRequest := &searchv1.ListSearchIndexExportsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexExportsRequest_Filter{
			CreatedAfter: req.GetFraudSignal().GetOccurredAt(),
			OwnerId:      cloned.GetSubject().GetFraudRuleId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	listSearchIndexExportsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*searchv1.ListSearchIndexExportsResponse, error) {
		return s.searchIndexExportClient.ListSearchIndexExports(ctx, listSearchIndexExportsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ListSearchIndexExports")
	}
	previewGiftCardTransactionRequest := buildPreviewGiftCardTransactionRequestForNormalizeFraudSignal(req)
	previewGiftCardTransactionReply, err := s.giftCardTransactionValidationClient.PreviewGiftCardTransaction(ctx, previewGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/PreviewGiftCardTransaction")
	}
	attributes := make(map[string]string, len(req.GetFraudSignal().GetAttributes()))
	for key, value := range req.GetFraudSignal().GetAttributes() {
		attributes[key] = value
	}
	var envelope2 fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	quoteFraudSignalRequest := &fraudv1.QuoteFraudSignalRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: req.GetFraudSignal().GetSubject().GetDisplayName(),
				Version:     req.GetFraudSignal().GetSubject().GetVersion(),
				FraudRuleId: req.GetFraudSignal().GetSubject().GetFraudRuleId(),
			},
			Attributes: attributes,
		},
		FraudSignalId: envelope2.GetDisplayName(),
	}
	quoteFraudSignalReply := new(fraudv1.QuoteFraudSignalResponse)
	if err := s.conn.Invoke(ctx, fraudFraudSignalCalculationServiceQuoteFraudSignalMethod, quoteFraudSignalRequest, quoteFraudSignalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalCalculationService/QuoteFraudSignal")
	}
	return &fraudv1.NormalizeFraudSignalResponse{
		FraudSignal: &fraudv1.FraudSignal{
			CorrelationId: listSearchIndexExportsReply.GetResult().GetCorrelationId(),
			OccurredAt:    previewGiftCardTransactionReply.GetGiftCardTransaction().GetOccurredAt(),
			FraudSignalId: req.GetFraudSignalId(),
		},
		Result: &commonv1.OperationResult{
			Status:        quoteFraudSignalReply.GetResult().GetStatus(),
			ReferenceIds:  quoteFraudSignalReply.GetResult().GetReferenceIds(),
			CorrelationId: req.GetFraudSignal().GetCorrelationId(),
		},
	}, nil
}

// LintFraudSignals lints a fraud signal.
func (s *FraudSignalValidationServer) LintFraudSignals(ctx context.Context, req *fraudv1.LintFraudSignalsRequest) (*fraudv1.LintFraudSignalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintFraudSignals", "error", err)
		return nil, err
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "lint fraud signals", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetDetails().GetContact(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	explainRiskAssessmentDecisionRequest := &fraudv1.ExplainRiskAssessmentDecisionRequest{
		Context:        req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{Assignee: &identityv1.UserRef{Email: holder.Email}},
		Details: &fraudv1.ExplainRiskAssessmentDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
	}
	explainRiskAssessmentDecisionReply, err := grpcx.Call(ctx, s.riskAssessmentValidationClient.ExplainRiskAssessmentDecision, explainRiskAssessmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/ExplainRiskAssessmentDecision")
	}
	return &fraudv1.LintFraudSignalsResponse{
		Result: &commonv1.OperationResult{
			Status: explainRiskAssessmentDecisionReply.GetResult().GetStatus(),
		},
		Outcome: &fraudv1.LintFraudSignalsResponse_Outcome{
			Status: explainRiskAssessmentDecisionReply.GetResult().GetStatus(),
		},
		FraudSignal: &fraudv1.FraudSignal{
			FraudSignalId: req.GetFraudSignalId(),
			Sequence:      req.GetFraudSignal().GetSequence(),
		},
	}, nil
}

// GetFraudSignalRules returns a fraud signal.
func (s *FraudSignalValidationServer) GetFraudSignalRules(ctx context.Context, req *fraudv1.GetFraudSignalRulesRequest) (*fraudv1.GetFraudSignalRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_signal", req.GetFraudSignalId(), "get_fraud_signal_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get fraud signal rules", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	validateLoyaltyMemberRequest := &loyaltyv1.ValidateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Email: email},
			Funding: &loyaltyv1.LoyaltyMember_ExternalAccountId{ExternalAccountId: req.GetFraudSignalId()},
		},
		Details: &loyaltyv1.ValidateLoyaltyMemberRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	validateLoyaltyMemberReply, err := grpcx.NewClient(s.conn, loyaltyv1.NewLoyaltyMemberValidationServiceClient).ValidateLoyaltyMember(ctx, validateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberValidationService/ValidateLoyaltyMember")
	}
	return &fraudv1.GetFraudSignalRulesResponse{
		Result: &commonv1.OperationResult{
			Message: validateLoyaltyMemberReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		FraudSignal: &fraudv1.FraudSignal{FraudSignalId: req.GetFraudSignalId()},
	}, nil
}

// fraudSignalValidationServerValidateFraudSignalNormalizeRiskAssessmentStep carries the request of one step of
// ValidateFraudSignal and, once it ran, its reply.
type fraudSignalValidationServerValidateFraudSignalNormalizeRiskAssessmentStep struct {
	request *fraudv1.NormalizeRiskAssessmentRequest
	reply   *fraudv1.NormalizeRiskAssessmentResponse
}

func (s *FraudSignalValidationServer) runValidateFraudSignalNormalizeRiskAssessment(ctx context.Context, step *fraudSignalValidationServerValidateFraudSignalNormalizeRiskAssessmentStep) error {
	reply, err := s.riskAssessmentValidationClient.NormalizeRiskAssessment(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
