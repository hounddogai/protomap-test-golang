package giftcards

import (
	"context"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	compliancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/compliance/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	timekeepingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/timekeeping/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// GiftCardValidationServer implements `acme.giftcards.v1.GiftCardValidationService`.
// Checks gift cards against the rules before anything acts on them.
type GiftCardValidationServer struct {
	giftcardsv1.UnimplementedGiftCardValidationServiceServer
	store                          *Store
	log                            *slog.Logger
	conn                           *grpc.ClientConn
	timesheetClient                timekeepingv1.TimesheetServiceClient
	giftCardBatchCalculationClient giftcardsv1.GiftCardBatchCalculationServiceClient
	estimateGiftCardFn             func(context.Context, *giftcardsv1.EstimateGiftCardRequest, ...grpc.CallOption) (*giftcardsv1.EstimateGiftCardResponse, error)
	giftCardCalculationClient      giftcardsv1.GiftCardCalculationServiceClient
	giftCardCalculationOnce        sync.Once
	giftCardCalculationCached      giftcardsv1.GiftCardCalculationServiceClient
	documentValidationClient       documentsv1.DocumentValidationServiceClient
	venueClient                    venuesv1.VenueServiceClient
	accessPolicyValidationClient   authorizationv1.AccessPolicyValidationServiceClient
	complianceCaseQueryClient      compliancev1.ComplianceCaseQueryServiceClient
}

// NewGiftCardValidationServer wires a GiftCardValidationServer.
func NewGiftCardValidationServer(deps platform.Deps) *GiftCardValidationServer {
	giftCardCalculation := giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn)
	return &GiftCardValidationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.giftcards.v1.GiftCardValidationService"),
		conn:                           deps.Conn,
		timesheetClient:                timekeepingv1.NewTimesheetServiceClient(deps.Conn),
		giftCardBatchCalculationClient: giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
		estimateGiftCardFn:             giftCardCalculation.EstimateGiftCard,
		giftCardCalculationClient:      giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
		documentValidationClient:       documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		venueClient:                    venuesv1.NewVenueServiceClient(deps.Conn),
		accessPolicyValidationClient:   authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		complianceCaseQueryClient:      compliancev1.NewComplianceCaseQueryServiceClient(deps.Conn),
	}
}

// ValidateGiftCard validates a gift card.
func (s *GiftCardValidationServer) ValidateGiftCard(ctx context.Context, req *giftcardsv1.ValidateGiftCardRequest) (*giftcardsv1.ValidateGiftCardResponse, error) {
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "validate gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	simulateGiftCardRequest := &giftcardsv1.SimulateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{
				GiftCardTransactionId: req.GetGiftCard().GetHolder().GetGiftCardTransactionId(),
			},
		},
	}
	simulateGiftCardReply := new(giftcardsv1.SimulateGiftCardResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardCalculationService_SimulateGiftCard_FullMethodName, simulateGiftCardRequest, simulateGiftCardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/SimulateGiftCard")
	}
	ctx = tenancy.With(ctx, "gift_card_id", req.GetGiftCard().GetGiftCardId())
	searchTimesheetsReply, err := s.timesheetClient.SearchTimesheets(ctx, &timekeepingv1.SearchTimesheetsRequest{
		Context: req.GetContext(),
		Filter: &timekeepingv1.SearchTimesheetsRequest_Filter{
			Labels:       simulateGiftCardReply.GetGiftCard().GetLabels(),
			CreatedAfter: simulateGiftCardReply.GetGiftCard().GetOpenedAt(),
			OwnerId:      tenancy.Value(ctx, "gift_card_id"),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.timekeeping.v1.TimesheetService/SearchTimesheets")
	}
	return &giftcardsv1.ValidateGiftCardResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: simulateGiftCardReply.GetResult().GetReferenceIds(),
			Details:      searchTimesheetsReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			Available:  simulateGiftCardReply.GetGiftCard().GetAvailable(),
			GiftCardId: req.GetGiftCardId(),
			Balance:    req.GetGiftCard().GetBalance(),
		},
	}, nil
}

// PreviewGiftCard previews a gift card.
func (s *GiftCardValidationServer) PreviewGiftCard(ctx context.Context, req *giftcardsv1.PreviewGiftCardRequest) (*giftcardsv1.PreviewGiftCardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "preview gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	recalculateGiftCardBatchesRequest := &giftcardsv1.RecalculateGiftCardBatchesRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Settlement: &giftcardsv1.GiftCardBatch_Bank{Bank: &giftcardsv1.GiftCardBatchBankSettlement{Bic: req.GetGiftCard().GetBank().GetBic()}},
		},
	}
	send := s.recalculateGiftCardBatchesSender(recalculateGiftCardBatchesRequest)
	recalculateGiftCardBatchesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/RecalculateGiftCardBatches")
	}
	encoded, err := protojson.Marshal(req.GetGiftCard())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded giftcardsv1.GiftCard
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	estimateGiftCardRequest := &giftcardsv1.EstimateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
			Audit:   &commonv1.AuditInfo{Etag: (&decoded).GetAudit().GetEtag()},
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{
				Token: recalculateGiftCardBatchesReply.GetGiftCardBatch().GetCard().GetToken(),
			}},
		},
	}
	estimateGiftCardReply, err := s.estimateGiftCardFn(ctx, estimateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/EstimateGiftCard")
	}
	return &giftcardsv1.PreviewGiftCardResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recalculateGiftCardBatchesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{
				GiftCardTransactionId: estimateGiftCardReply.GetGiftCard().GetHolder().GetGiftCardTransactionId(),
			},
			GiftCardId: req.GetGiftCardId(),
			Available:  req.GetGiftCard().GetAvailable(),
		},
	}, nil
}

// CheckGiftCardEligibility checks a gift card.
func (s *GiftCardValidationServer) CheckGiftCardEligibility(ctx context.Context, req *giftcardsv1.CheckGiftCardEligibilityRequest) (*giftcardsv1.CheckGiftCardEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckGiftCardEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "check gift card eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	explainGiftCardCalculationRequest := &giftcardsv1.ExplainGiftCardCalculationRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ExplainGiftCardCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
		GiftCard: &giftcardsv1.GiftCard{
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetGiftCard().GetAudit().GetCreatedAt()},
			Funding: &giftcardsv1.GiftCard_Bank{Bank: &giftcardsv1.GiftCardBankFunding{Bic: req.GetGiftCard().GetBank().GetBic()}},
		},
	}
	explainGiftCardCalculationReply, err := grpcx.NewClient(s.conn, giftcardsv1.NewGiftCardCalculationServiceClient).ExplainGiftCardCalculation(ctx, explainGiftCardCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/ExplainGiftCardCalculation")
	}
	calculateGiftCardRequest := buildCalculateGiftCardRequestForCheckGiftCardEligibility(req)
	calculateGiftCardReply, err := s.giftCardCalculationClient.CalculateGiftCard(ctx, calculateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/CalculateGiftCard")
	}
	return &giftcardsv1.CheckGiftCardEligibilityResponse{
		Outcome: &giftcardsv1.CheckGiftCardEligibilityResponse_Outcome{
			EffectiveAt: explainGiftCardCalculationReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: calculateGiftCardReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: req.GetGiftCardId(),
			Labels:     req.GetGiftCard().GetLabels(),
		},
	}, nil
}

// NormalizeGiftCard normalizes a gift card.
func (s *GiftCardValidationServer) NormalizeGiftCard(ctx context.Context, req *giftcardsv1.NormalizeGiftCardRequest) (*giftcardsv1.NormalizeGiftCardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "normalize gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	estimateGiftCardRequest := &giftcardsv1.EstimateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{
				GiftCardTransactionId: req.GetGiftCard().GetHolder().GetGiftCardTransactionId(),
			},
		},
	}
	estimateGiftCardReply, err := s.giftCardCalculationLazy().EstimateGiftCard(ctx, estimateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/EstimateGiftCard")
	}
	recalculateGiftCardsReply, err := s.giftCardCalculationClient.RecalculateGiftCards(ctx, &giftcardsv1.RecalculateGiftCardsRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{
				HolderName: estimateGiftCardReply.GetGiftCard().GetCard().GetHolderName(),
			}},
			Contact: &commonv1.ContactInfo{
				Email: estimateGiftCardReply.GetGiftCard().GetContact().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/RecalculateGiftCards")
	}
	lintDocumentsRequest := &documentsv1.LintDocumentsRequest{}
	lintDocumentsRequest.Context = req.GetContext()
	lintDocumentsRequest.Document = &documentsv1.Document{
		Settlement: &documentsv1.Document_Card{Card: &documentsv1.DocumentCardSettlement{
			Token: recalculateGiftCardsReply.GetGiftCard().GetCard().GetToken(),
			Last4: recalculateGiftCardsReply.GetGiftCard().GetCard().GetLast4(),
		}},
		Audit: &commonv1.AuditInfo{
			Etag: recalculateGiftCardsReply.GetGiftCard().GetAudit().GetEtag(),
		},
	}
	lintDocumentsRequest.Details = &documentsv1.LintDocumentsRequest_Details{
		Contact: &commonv1.ContactInfo{
			Phone: recalculateGiftCardsReply.GetGiftCard().GetContact().GetPhone(),
		},
	}
	lintDocumentsReply, err := s.documentValidationClient.LintDocuments(ctx, lintDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/LintDocuments")
	}
	updateTimesheetRequest := buildUpdateTimesheetRequestForNormalizeGiftCard(req, estimateGiftCardReply)
	updateTimesheetReply, err := s.timesheetClient.UpdateTimesheet(ctx, updateTimesheetRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.timekeeping.v1.TimesheetService/UpdateTimesheet")
	}
	_ = lintDocumentsReply
	return &giftcardsv1.NormalizeGiftCardResponse{
		Result: &commonv1.OperationResult{
			Status:       estimateGiftCardReply.GetResult().GetStatus(),
			Message:      recalculateGiftCardsReply.GetResult().GetMessage(),
			ReferenceIds: updateTimesheetReply.GetResult().GetReferenceIds(),
		},
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{
				Email: recalculateGiftCardsReply.GetGiftCard().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: updateTimesheetReply.GetTimesheet().GetAudit().GetUpdatedAt(),
			},
			GiftCardId: req.GetGiftCardId(),
			Holder:     req.GetGiftCard().GetHolder(),
		},
	}, nil
}

// ExplainGiftCardDecision explains a gift card.
func (s *GiftCardValidationServer) ExplainGiftCardDecision(ctx context.Context, req *giftcardsv1.ExplainGiftCardDecisionRequest) (*giftcardsv1.ExplainGiftCardDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainGiftCardDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "explain gift card decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	cloneVenueRequest := &venuesv1.CloneVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetGiftCard().GetAudit().GetCreatedAt()},
		},
	}
	cloneVenueReply, err := grpcx.Call(ctx, s.venueClient.CloneVenue, cloneVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/CloneVenue")
	}
	for limit := range iterx.Each(req.GetGiftCard().GetLimits(), func(candidate *giftcardsv1.GiftCardLimit) bool { return candidate != nil }) {
		if _, err := s.giftCardCalculationClient.ExplainGiftCardCalculation(ctx, &giftcardsv1.ExplainGiftCardCalculationRequest{
			Context: req.GetContext(),
			GiftCard: &giftcardsv1.GiftCard{
				Limits: []*giftcardsv1.GiftCardLimit{&giftcardsv1.GiftCardLimit{Name: limit.GetName()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/ExplainGiftCardCalculation")
		}
	}
	return &giftcardsv1.ExplainGiftCardDecisionResponse{
		Result: &commonv1.OperationResult{
			Message: cloneVenueReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &giftcardsv1.ExplainGiftCardDecisionResponse_Outcome{Message: cloneVenueReply.GetResult().GetMessage()},
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: req.GetGiftCardId(),
			Available:  req.GetGiftCard().GetAvailable(),
		},
	}, nil
}

// ScoreGiftCard scores a gift card.
func (s *GiftCardValidationServer) ScoreGiftCard(ctx context.Context, req *giftcardsv1.ScoreGiftCardRequest) (*giftcardsv1.ScoreGiftCardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "score gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	normalizeAccessPolicyRequest := &authorizationv1.NormalizeAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetGiftCard().GetAudit().GetEtag(),
				CreatedAt: req.GetGiftCard().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{DisplayName: req.GetGiftCard().GetHolder().GetDisplayName()},
		},
	}
	normalizeAccessPolicyReply, err := authorizationv1.AccessPolicyValidationServiceClient.NormalizeAccessPolicy(s.accessPolicyValidationClient, ctx, normalizeAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/NormalizeAccessPolicy")
	}
	ctx = tenancy.With(ctx, "bic", req.GetGiftCard().GetBank().GetBic())
	simulateGiftCardRequest := &giftcardsv1.SimulateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Limits: []*giftcardsv1.GiftCardLimit{&giftcardsv1.GiftCardLimit{
				Amount: normalizeAccessPolicyReply.GetAccessPolicy().GetDiscount().GetAmount(),
			}},
			Funding: &giftcardsv1.GiftCard_Bank{Bank: &giftcardsv1.GiftCardBankFunding{Bic: tenancy.Value(ctx, "bic")}},
		},
	}
	simulateGiftCardReply, err := s.giftCardCalculationLazy().SimulateGiftCard(ctx, simulateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/SimulateGiftCard")
	}
	return &giftcardsv1.ScoreGiftCardResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: normalizeAccessPolicyReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			CurrencyCode: simulateGiftCardReply.GetBreakdown().GetCurrencyCode(),
			GiftCardId:   simulateGiftCardReply.GetGiftCard().GetGiftCardId(),
			Labels:       req.GetGiftCard().GetLabels(),
			Available:    req.GetGiftCard().GetAvailable(),
		},
	}, nil
}

// GetGiftCardRules returns a gift card.
func (s *GiftCardValidationServer) GetGiftCardRules(ctx context.Context, req *giftcardsv1.GetGiftCardRulesRequest) (*giftcardsv1.GetGiftCardRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetGiftCardRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card", req.GetGiftCardId(), "get_gift_card_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get gift card rules", "tenant", req.GetContext().GetTenantId())
	countComplianceCasesRequest := &compliancev1.CountComplianceCasesRequest{
		Context: req.GetContext(),
		Filter: &compliancev1.CountComplianceCasesRequest_Filter{
			OwnerId:      req.GetGiftCardId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	}
	send := s.countComplianceCasesSender(countComplianceCasesRequest)
	countComplianceCasesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.compliance.v1.ComplianceCaseQueryService/CountComplianceCases")
	}
	return &giftcardsv1.GetGiftCardRulesResponse{
		Result: &commonv1.OperationResult{
			Details: countComplianceCasesReply.GetResult().GetDetails(),
			Message: countComplianceCasesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{GiftCardId: req.GetGiftCardId()},
	}, nil
}

// recalculateGiftCardBatchesSender returns a function that sends one request to
// acme.giftcards.v1.GiftCardBatchCalculationService/RecalculateGiftCardBatches.
func (s *GiftCardValidationServer) recalculateGiftCardBatchesSender(request *giftcardsv1.RecalculateGiftCardBatchesRequest) func(context.Context) (*giftcardsv1.RecalculateGiftCardBatchesResponse, error) {
	return func(ctx context.Context) (*giftcardsv1.RecalculateGiftCardBatchesResponse, error) {
		return s.giftCardBatchCalculationClient.RecalculateGiftCardBatches(ctx, request)
	}
}

// giftCardCalculationLazy dials acme.giftcards.v1.GiftCardCalculationService the first time a handler needs it.
func (s *GiftCardValidationServer) giftCardCalculationLazy() giftcardsv1.GiftCardCalculationServiceClient {
	s.giftCardCalculationOnce.Do(func() { s.giftCardCalculationCached = giftcardsv1.NewGiftCardCalculationServiceClient(s.conn) })
	return s.giftCardCalculationCached
}

// countComplianceCasesSender returns a function that sends one request to
// acme.compliance.v1.ComplianceCaseQueryService/CountComplianceCases.
func (s *GiftCardValidationServer) countComplianceCasesSender(request *compliancev1.CountComplianceCasesRequest) func(context.Context) (*compliancev1.CountComplianceCasesResponse, error) {
	return func(ctx context.Context) (*compliancev1.CountComplianceCasesResponse, error) {
		return s.complianceCaseQueryClient.CountComplianceCases(ctx, request)
	}
}
