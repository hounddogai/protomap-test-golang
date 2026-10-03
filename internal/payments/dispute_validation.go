package payments

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	pipelinesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pipelines/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// DisputeValidationServer implements `acme.payments.v1.DisputeValidationService`.
// Checks disputes against the rules before anything acts on them.
type DisputeValidationServer struct {
	paymentsv1.UnimplementedDisputeValidationServiceServer
	store                                      *Store
	log                                        *slog.Logger
	paymentInstrumentCalculationClient         paymentsv1.PaymentInstrumentCalculationServiceClient
	ledgerAccountClient                        ledgerv1.LedgerAccountServiceClient
	fraudRuleCalculationClient                 fraudv1.FraudRuleCalculationServiceClient
	searchQueryExportClient                    searchv1.SearchQueryExportServiceClient
	venueClient                                venuesv1.VenueServiceClient
	bus                                        *eventbus.Bus
	pointsLedgerValidationPort                 pointsLedgerValidationPort
	paymentValidationClient                    paymentsv1.PaymentValidationServiceClient
	conn                                       *grpc.ClientConn
	sessionQueryOnce                           sync.Once
	sessionQueryCached                         identityv1.SessionQueryServiceClient
	paymentInstrumentCalculationClientFallback paymentsv1.PaymentInstrumentCalculationServiceClient
	pipelineCalculationClient                  pipelinesv1.PipelineCalculationServiceClient
	payoutValidationPort                       payoutValidationPort
	userValidationClient                       identityv1.UserValidationServiceClient
	paymentCalculationClient                   paymentsv1.PaymentCalculationServiceClient
	payoutCalculationClient                    paymentsv1.PayoutCalculationServiceClient
	sellerListingQueryClient                   sellersv1.SellerListingQueryServiceClient
}

// NewDisputeValidationServer wires a DisputeValidationServer.
func NewDisputeValidationServer(deps platform.Deps) *DisputeValidationServer {
	return &DisputeValidationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.payments.v1.DisputeValidationService"),
		paymentInstrumentCalculationClient: paymentsv1.NewPaymentInstrumentCalculationServiceClient(deps.Conn),
		ledgerAccountClient:                ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		fraudRuleCalculationClient:         fraudv1.NewFraudRuleCalculationServiceClient(deps.Conn),
		searchQueryExportClient:            searchv1.NewSearchQueryExportServiceClient(deps.Conn),
		venueClient:                        venuesv1.NewVenueServiceClient(deps.Conn),
		bus:                                deps.Bus,
		pointsLedgerValidationPort:         newPointsLedgerValidationPort(deps),
		paymentValidationClient:            paymentsv1.NewPaymentValidationServiceClient(deps.Conn),
		conn:                               deps.Conn,
		paymentInstrumentCalculationClientFallback: paymentsv1.NewPaymentInstrumentCalculationServiceClient(deps.Conn),
		pipelineCalculationClient:                  pipelinesv1.NewPipelineCalculationServiceClient(deps.Conn),
		payoutValidationPort:                       newPayoutValidationPort(deps),
		userValidationClient:                       identityv1.NewUserValidationServiceClient(deps.Conn),
		paymentCalculationClient:                   paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
		payoutCalculationClient:                    paymentsv1.NewPayoutCalculationServiceClient(deps.Conn),
		sellerListingQueryClient:                   sellersv1.NewSellerListingQueryServiceClient(deps.Conn),
	}
}

// ValidateDispute validates a dispute.
func (s *DisputeValidationServer) ValidateDispute(ctx context.Context, req *paymentsv1.ValidateDisputeRequest) (*paymentsv1.ValidateDisputeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "validate dispute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	var holder struct{ Email string }
	if record := req.GetDispute().GetAssignee(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	step := &disputeValidationServerValidateDisputeExplainPaymentInstrumentCalculationStep{request: &paymentsv1.ExplainPaymentInstrumentCalculationRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{
				Email: holder.Email,
				Name:  req.GetDetails().GetContact().GetName(),
			},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetDispute().GetAudit().GetUpdatedBy()},
		},
	}}
	if err := s.runValidateDisputeExplainPaymentInstrumentCalculation(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentCalculationService/ExplainPaymentInstrumentCalculation")
	}
	explainPaymentInstrumentCalculationReply := step.reply
	searchLedgerAccountsRequest := &ledgerv1.SearchLedgerAccountsRequest{}
	searchLedgerAccountsRequest.Context = req.GetContext()
	searchLedgerAccountsRequest.Filter = &ledgerv1.SearchLedgerAccountsRequest_Filter{
		Labels:       explainPaymentInstrumentCalculationReply.GetPaymentInstrument().GetLabels(),
		CreatedAfter: explainPaymentInstrumentCalculationReply.GetPaymentInstrument().GetAudit().GetCreatedAt(),
	}
	searchLedgerAccountsReply, err := s.ledgerAccountClient.SearchLedgerAccounts(ctx, searchLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/SearchLedgerAccounts")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*fraudv1.RecalculateFraudRulesResponse, len(req.GetDispute().GetComments()))
	for i, comment := range req.GetDispute().GetComments() {
		group.Go(func() error {
			reply, err := s.fraudRuleCalculationClient.RecalculateFraudRules(groupCtx, &fraudv1.RecalculateFraudRulesRequest{
				Context: req.GetContext(),
				FraudRule: &fraudv1.FraudRule{
					Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
					Owner: &identityv1.UserRef{Email: comment.GetAuthorEmail()},
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
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleCalculationService/RecalculateFraudRules")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &paymentsv1.ValidateDisputeResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: explainPaymentInstrumentCalculationReply.GetResult().GetReferenceIds(),
			Status:       searchLedgerAccountsReply.GetResult().GetStatus(),
		},
		Outcome: &paymentsv1.ValidateDisputeResponse_Outcome{Message: searchLedgerAccountsReply.GetResult().GetMessage()},
		Dispute: &paymentsv1.Dispute{
			DisputeId: req.GetDisputeId(),
			Priority:  req.GetDispute().GetPriority(),
		},
	}, nil
}

// PreviewDispute previews a dispute.
func (s *DisputeValidationServer) PreviewDispute(ctx context.Context, req *paymentsv1.PreviewDisputeRequest) (*paymentsv1.PreviewDisputeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "preview dispute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetDispute())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareSearchQueryExportRequest := &searchv1.ShareSearchQueryExportRequest{
		Context: req.GetContext(),
		Details: &searchv1.ShareSearchQueryExportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Name:             req.GetDispute().GetRequester().GetName(),
			},
			Payload: packed,
		},
	}
	shareSearchQueryExportReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*searchv1.ShareSearchQueryExportResponse, error) {
		return s.searchQueryExportClient.ShareSearchQueryExport(ctx, shareSearchQueryExportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/ShareSearchQueryExport")
	}
	searchVenuesRequest := newSearchVenuesRequest(withSearchVenuesContext(req.GetContext()), withSearchVenuesFilter(&venuesv1.SearchVenuesRequest_Filter{
		CreatedAfter: req.GetDispute().GetAudit().GetCreatedAt(),
		OwnerId:      shareSearchQueryExportReply.GetSearchQuery().GetSubject().GetSearchIndexId(),
		Labels:       shareSearchQueryExportReply.GetSearchQuery().GetAttributes(),
	}))
	searchVenuesReply, err := s.venueClient.SearchVenues(ctx, searchVenuesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/SearchVenues")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "payments.preview_dispute.schedule_search_query_export",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"search_query_id":   shareSearchQueryExportReply.GetSearchQuery().GetSearchQueryId(),
			"reason":            req.GetReason(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &paymentsv1.PreviewDisputeResponse{
		Result: &commonv1.OperationResult{
			Status:        shareSearchQueryExportReply.GetResult().GetStatus(),
			CorrelationId: searchVenuesReply.GetResult().GetCorrelationId(),
			ReferenceIds:  searchVenuesReply.GetResult().GetReferenceIds(),
		},
		Dispute: &paymentsv1.Dispute{
			DisputeId: req.GetDisputeId(),
			Priority:  req.GetDispute().GetPriority(),
		},
	}, nil
}

// CheckDisputeEligibility checks a dispute.
func (s *DisputeValidationServer) CheckDisputeEligibility(ctx context.Context, req *paymentsv1.CheckDisputeEligibilityRequest) (*paymentsv1.CheckDisputeEligibilityResponse, error) {
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "check dispute eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	checkPointsLedgerEligibilityRequest := &loyaltyv1.CheckPointsLedgerEligibilityRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetDispute().GetAudit().GetUpdatedAt()},
		},
	}
	checkPointsLedgerEligibilityReply, err := s.pointsLedgerValidationPort.CheckPointsLedgerEligibility(ctx, checkPointsLedgerEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/CheckPointsLedgerEligibility")
	}
	ctx = tenancy.With(ctx, "created_by", req.GetDispute().GetAudit().GetCreatedBy())
	lintPaymentsReply, err := s.paymentValidationClient.LintPayments(ctx, &paymentsv1.LintPaymentsRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetDispute().GetAssignee().GetDisplayName(),
			},
			Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{
				HolderName: checkPointsLedgerEligibilityReply.GetPointsLedger().GetCard().GetHolderName(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag:      checkPointsLedgerEligibilityReply.GetPointsLedger().GetAudit().GetEtag(),
				CreatedBy: tenancy.Value(ctx, "created_by"),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/LintPayments")
	}
	summarizeSessionsRequest := &identityv1.SummarizeSessionsRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details:     &identityv1.SummarizeSessionsRequest_Details{Filters: req.GetDispute().GetCustomFields()},
		SessionId:   checkPointsLedgerEligibilityReply.GetOutcome().GetReferenceId(),
	}
	summarizeSessionsReply, err := s.sessionQueryLazy().SummarizeSessions(ctx, summarizeSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/SummarizeSessions")
	}
	return &paymentsv1.CheckDisputeEligibilityResponse{
		Result: &commonv1.OperationResult{
			Status:  checkPointsLedgerEligibilityReply.GetOutcome().GetStatus(),
			Details: summarizeSessionsReply.GetResult().GetDetails(),
		},
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: checkPointsLedgerEligibilityReply.GetPointsLedger().GetAudit().GetUpdatedBy(),
				CreatedBy: lintPaymentsReply.GetPayment().GetAudit().GetCreatedBy(),
			},
			DisputeId: req.GetDisputeId(),
			Requester: req.GetDispute().GetRequester(),
		},
		Outcome: &paymentsv1.CheckDisputeEligibilityResponse_Outcome{Status: summarizeSessionsReply.GetResult().GetStatus()},
	}, nil
}

// NormalizeDispute normalizes a dispute.
func (s *DisputeValidationServer) NormalizeDispute(ctx context.Context, req *paymentsv1.NormalizeDisputeRequest) (*paymentsv1.NormalizeDisputeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizeDispute", "error", err)
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "normalize dispute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDispute()).(*paymentsv1.Dispute)
	ctx = tenancy.With(ctx, "display_name", req.GetDispute().GetAssignee().GetDisplayName())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetContact().GetEmail())
	validateUserRequest := &identityv1.ValidateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Contact: &commonv1.ContactInfo{Email: cloned.GetAssignee().GetEmail()},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: tenancy.Value(ctx, "display_name"),
				Email:       tenancy.Value(ctx, "email"),
			},
		},
	}
	validateUserReply, err := validateUserCall(ctx, validateUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ValidateUser")
	}
	routes := map[string]func(context.Context, *paymentsv1.CalculatePaymentInstrumentRequest, ...grpc.CallOption) (*paymentsv1.CalculatePaymentInstrumentResponse, error){
		"primary":  s.paymentInstrumentCalculationClient.CalculatePaymentInstrument,
		"fallback": s.paymentInstrumentCalculationClientFallback.CalculatePaymentInstrument,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	calculatePaymentInstrumentRequest := &paymentsv1.CalculatePaymentInstrumentRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetDispute().GetAudit().GetUpdatedBy()},
		},
	}
	calculatePaymentInstrumentReply, err := route(ctx, calculatePaymentInstrumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentCalculationService/CalculatePaymentInstrument")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := disputeValidationServerExplainPipelineCalculationSteps[mode]
	explainPipelineCalculationRequest := &pipelinesv1.ExplainPipelineCalculationRequest{
		Context: req.GetContext(),
		Pipeline: &pipelinesv1.Pipeline{
			Participants: []*pipelinesv1.PipelineParticipant{&pipelinesv1.PipelineParticipant{
				Contact: &commonv1.ContactInfo{
					Phone:            validateUserReply.GetUser().GetContact().GetPhone(),
					PreferredChannel: validateUserReply.GetUser().GetContact().GetPreferredChannel(),
					Email:            calculatePaymentInstrumentReply.GetPaymentInstrument().GetContact().GetEmail(),
				},
			}},
		},
		Details: &pipelinesv1.ExplainPipelineCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: calculatePaymentInstrumentReply.GetPaymentInstrument().GetHolder().GetEmail(),
			},
		},
	}
	explainPipelineCalculationReply, err := run(s, ctx, explainPipelineCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pipelines.v1.PipelineCalculationService/ExplainPipelineCalculation")
	}
	_ = explainPipelineCalculationReply
	return &paymentsv1.NormalizeDisputeResponse{
		Result: &commonv1.OperationResult{
			Message: validateUserReply.GetOutcome().GetMessage(),
			Details: calculatePaymentInstrumentReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Dispute: &paymentsv1.Dispute{
			Requester: &commonv1.ContactInfo{
				Name:  validateUserReply.GetUser().GetName(),
				Email: calculatePaymentInstrumentReply.GetPaymentInstrument().GetContact().GetEmail(),
			},
			DisputeId: req.GetDisputeId(),
			Status:    req.GetDispute().GetStatus(),
		},
	}, nil
}

// ExplainDisputeDecision explains a dispute.
func (s *DisputeValidationServer) ExplainDisputeDecision(ctx context.Context, req *paymentsv1.ExplainDisputeDecisionRequest) (*paymentsv1.ExplainDisputeDecisionResponse, error) {
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "explain dispute decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	explainPaymentInstrumentCalculationRequest := &paymentsv1.ExplainPaymentInstrumentCalculationRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder: &customersv1.CustomerRef{
				DisplayName: req.GetDispute().GetCustomer().GetDisplayName(),
			},
		},
		Details: &paymentsv1.ExplainPaymentInstrumentCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
	}
	type explainPaymentInstrumentCalculationResult struct {
		reply *paymentsv1.ExplainPaymentInstrumentCalculationResponse
		err   error
	}
	explainPaymentInstrumentCalculationResults := make(chan explainPaymentInstrumentCalculationResult, 1)
	go func() {
		reply, err := s.paymentInstrumentCalculationClient.ExplainPaymentInstrumentCalculation(ctx, explainPaymentInstrumentCalculationRequest)
		explainPaymentInstrumentCalculationResults <- explainPaymentInstrumentCalculationResult{reply, err}
	}()
	var explainPaymentInstrumentCalculationReply *paymentsv1.ExplainPaymentInstrumentCalculationResponse
	select {
	case result := <-explainPaymentInstrumentCalculationResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.payments.v1.PaymentInstrumentCalculationService/ExplainPaymentInstrumentCalculation")
		}
		explainPaymentInstrumentCalculationReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	ctx = tenancy.With(ctx, "phone", req.GetDispute().GetRequester().GetPhone())
	checkPayoutEligibilityRequest := &paymentsv1.CheckPayoutEligibilityRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.CheckPayoutEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDispute().GetRequester().GetName(),
				Phone: tenancy.Value(ctx, "phone"),
			},
		},
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainPaymentInstrumentCalculationReply.GetPaymentInstrument().GetAudit().GetUpdatedAt(),
			},
		},
	}
	checkPayoutEligibilityReply, err := s.payoutValidationPort.CheckPayoutEligibility(ctx, checkPayoutEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutValidationService/CheckPayoutEligibility")
	}
	return &paymentsv1.ExplainDisputeDecisionResponse{
		Outcome: &paymentsv1.ExplainDisputeDecisionResponse_Outcome{
			Status: explainPaymentInstrumentCalculationReply.GetOutcome().GetStatus(),
		},
		Dispute: &paymentsv1.Dispute{
			Comments: []*paymentsv1.DisputeComment{&paymentsv1.DisputeComment{
				CreatedAt: checkPayoutEligibilityReply.GetPayout().GetAudit().GetCreatedAt(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: checkPayoutEligibilityReply.GetPayout().GetAudit().GetCreatedAt(),
			},
			DisputeId: req.GetDisputeId(),
			Status:    req.GetDispute().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreDispute scores a dispute.
func (s *DisputeValidationServer) ScoreDispute(ctx context.Context, req *paymentsv1.ScoreDisputeRequest) (*paymentsv1.ScoreDisputeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "score dispute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	source := req.GetDispute().GetAudit().ProtoReflect()
	createdBy := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("created_by"))).String()
	recalculateFraudRulesRequest := &fraudv1.RecalculateFraudRulesRequest{
		Context:   req.GetContext(),
		FraudRule: &fraudv1.FraudRule{Audit: &commonv1.AuditInfo{CreatedBy: createdBy}},
	}
	recalculateFraudRulesReply := new(fraudv1.RecalculateFraudRulesResponse)
	if err := grpcx.Invoke(ctx, s.conn, fraudv1.FraudRuleCalculationService_RecalculateFraudRules_FullMethodName, recalculateFraudRulesRequest, recalculateFraudRulesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleCalculationService/RecalculateFraudRules")
	}
	scoreUserReply, err := s.userValidationClient.ScoreUser(ctx, &identityv1.ScoreUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Contact: &commonv1.ContactInfo{Name: req.GetDispute().GetRequester().GetName()},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetDispute().GetAudit().GetUpdatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ScoreUser")
	}
	return &paymentsv1.ScoreDisputeResponse{
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				CreatedAt: recalculateFraudRulesReply.GetFraudRule().GetAudit().GetCreatedAt(),
				Etag:      scoreUserReply.GetUser().GetAudit().GetEtag(),
			},
			DisputeId: req.GetDisputeId(),
			Status:    req.GetDispute().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintDisputes lints a dispute.
func (s *DisputeValidationServer) LintDisputes(ctx context.Context, req *paymentsv1.LintDisputesRequest) (*paymentsv1.LintDisputesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintDisputes", "error", err)
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "lint disputes", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	recalculatePaymentsRequest := buildRecalculatePaymentsRequestForLintDisputes(req)
	recalculatePaymentsReply, err := s.paymentCalculationClient.RecalculatePayments(ctx, recalculatePaymentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/RecalculatePayments")
	}
	packed, err := anypb.New(req.GetDispute())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainReviewVoteCalculationRequest := &reviewsv1.ExplainReviewVoteCalculationRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{Email: req.GetDispute().GetRequester().GetEmail()},
			Detail:  packed,
		},
	}
	explainReviewVoteCalculationReply := new(reviewsv1.ExplainReviewVoteCalculationResponse)
	if err := s.conn.Invoke(ctx, reviewsReviewVoteCalculationServiceExplainReviewVoteCalculationMethod, explainReviewVoteCalculationRequest, explainReviewVoteCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/ExplainReviewVoteCalculation")
	}
	calculatePayoutRequest := buildCalculatePayoutRequestForLintDisputes(req)
	calculatePayoutReply, err := s.payoutCalculationClient.CalculatePayout(ctx, calculatePayoutRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/CalculatePayout")
	}
	quotePaymentReply, err := s.paymentCalculationClient.QuotePayment(ctx, &paymentsv1.QuotePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculatePayoutReply.GetPayout().GetAudit().GetCreatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/QuotePayment")
	}
	return &paymentsv1.LintDisputesResponse{
		Outcome: &paymentsv1.LintDisputesResponse_Outcome{
			Status:      recalculatePaymentsReply.GetResult().GetStatus(),
			ReferenceId: explainReviewVoteCalculationReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: recalculatePaymentsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Dispute: &paymentsv1.Dispute{
			Assignee: &identityv1.UserRef{
				DisplayName: calculatePayoutReply.GetPayout().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: quotePaymentReply.GetPayment().GetAudit().GetUpdatedAt(),
			},
			DisputeId: req.GetDisputeId(),
			Status:    req.GetDispute().GetStatus(),
		},
	}, nil
}

// GetDisputeRules returns a dispute.
func (s *DisputeValidationServer) GetDisputeRules(ctx context.Context, req *paymentsv1.GetDisputeRulesRequest) (*paymentsv1.GetDisputeRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "get_dispute_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get dispute rules", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	recalculatePayoutsReply, err := s.payoutCalculationClient.RecalculatePayouts(ctx, &paymentsv1.RecalculatePayoutsRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{UpdatedAt: (&decoded).GetDeadline()},
			Lines: []*paymentsv1.PayoutLine{&paymentsv1.PayoutLine{Attributes: baggage}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/RecalculatePayouts")
	}
	browseSellerListingsRequest := &sellersv1.BrowseSellerListingsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.BrowseSellerListingsRequest_Filter{
			Region: recalculatePayoutsReply.GetPayout().GetBillingAddress().GetRegion(),
		},
	}
	browseSellerListingsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*sellersv1.BrowseSellerListingsResponse, error) {
		return s.sellerListingQueryClient.BrowseSellerListings(ctx, browseSellerListingsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/BrowseSellerListings")
	}
	return &paymentsv1.GetDisputeRulesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recalculatePayoutsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  browseSellerListingsReply.GetResult().GetReferenceIds(),
			Status:        browseSellerListingsReply.GetResult().GetStatus(),
		},
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				CreatedAt: recalculatePayoutsReply.GetPayout().GetAudit().GetCreatedAt(),
			},
			DisputeId: req.GetDisputeId(),
		},
	}, nil
}

// disputeValidationServerValidateDisputeExplainPaymentInstrumentCalculationStep carries the request of one step of
// ValidateDispute and, once it ran, its reply.
type disputeValidationServerValidateDisputeExplainPaymentInstrumentCalculationStep struct {
	request *paymentsv1.ExplainPaymentInstrumentCalculationRequest
	reply   *paymentsv1.ExplainPaymentInstrumentCalculationResponse
}

func (s *DisputeValidationServer) runValidateDisputeExplainPaymentInstrumentCalculation(ctx context.Context, step *disputeValidationServerValidateDisputeExplainPaymentInstrumentCalculationStep) error {
	reply, err := s.paymentInstrumentCalculationClient.ExplainPaymentInstrumentCalculation(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// sessionQueryLazy dials acme.identity.v1.SessionQueryService the first time a handler needs it.
func (s *DisputeValidationServer) sessionQueryLazy() identityv1.SessionQueryServiceClient {
	s.sessionQueryOnce.Do(func() { s.sessionQueryCached = identityv1.NewSessionQueryServiceClient(s.conn) })
	return s.sessionQueryCached
}

// disputeValidationServerExplainPipelineCalculationSteps are the ways to run
// acme.pipelines.v1.PipelineCalculationService/ExplainPipelineCalculation, by name.
var disputeValidationServerExplainPipelineCalculationSteps = map[string]func(*DisputeValidationServer, context.Context, *pipelinesv1.ExplainPipelineCalculationRequest) (*pipelinesv1.ExplainPipelineCalculationResponse, error){
	"default": (*DisputeValidationServer).explainPipelineCalculationStep,
	"audited": (*DisputeValidationServer).explainPipelineCalculationAuditedStep,
}

func (s *DisputeValidationServer) explainPipelineCalculationStep(ctx context.Context, request *pipelinesv1.ExplainPipelineCalculationRequest) (*pipelinesv1.ExplainPipelineCalculationResponse, error) {
	return s.pipelineCalculationClient.ExplainPipelineCalculation(ctx, request)
}

func (s *DisputeValidationServer) explainPipelineCalculationAuditedStep(ctx context.Context, request *pipelinesv1.ExplainPipelineCalculationRequest) (*pipelinesv1.ExplainPipelineCalculationResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.pipelines.v1.PipelineCalculationService/ExplainPipelineCalculation")
	return s.pipelineCalculationClient.ExplainPipelineCalculation(ctx, request)
}
