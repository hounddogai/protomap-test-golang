package giftcards

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	mediav1 "github.com/hounddogai/protomap-test-golang/gen/acme/media/v1"
	schedulingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/scheduling/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// GiftCardTransactionValidationServer implements `acme.giftcards.v1.GiftCardTransactionValidationService`.
// Checks gift card transactions against the rules before anything acts on them.
type GiftCardTransactionValidationServer struct {
	giftcardsv1.UnimplementedGiftCardTransactionValidationServiceServer
	store                                *Store
	log                                  *slog.Logger
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
	shiftClient                          schedulingv1.ShiftServiceClient
	bus                                  *eventbus.Bus
	giftCardBatchValidationClient        giftcardsv1.GiftCardBatchValidationServiceClient
	giftCardTransactionCalculationPort   giftCardTransactionCalculationPort
	conn                                 *grpc.ClientConn
	mediaAssetCalculationClient          mediav1.MediaAssetCalculationServiceClient
	giftCardCalculationClient            giftcardsv1.GiftCardCalculationServiceClient
	formCalculationClient                formsv1.FormCalculationServiceClient
	giftCardValidationOnce               sync.Once
	giftCardValidationCached             giftcardsv1.GiftCardValidationServiceClient
	synonymValidationPort                synonymValidationPort
}

// NewGiftCardTransactionValidationServer wires a GiftCardTransactionValidationServer.
func NewGiftCardTransactionValidationServer(deps platform.Deps) *GiftCardTransactionValidationServer {
	return &GiftCardTransactionValidationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.giftcards.v1.GiftCardTransactionValidationService"),
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		shiftClient:                          schedulingv1.NewShiftServiceClient(deps.Conn),
		bus:                                  deps.Bus,
		giftCardBatchValidationClient:        giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn),
		giftCardTransactionCalculationPort:   newGiftCardTransactionCalculationPort(deps),
		conn:                                 deps.Conn,
		mediaAssetCalculationClient:          mediav1.NewMediaAssetCalculationServiceClient(deps.Conn),
		giftCardCalculationClient:            giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
		formCalculationClient:                formsv1.NewFormCalculationServiceClient(deps.Conn),
		synonymValidationPort:                newSynonymValidationPort(deps),
	}
}

// ValidateGiftCardTransaction validates a gift card transaction.
func (s *GiftCardTransactionValidationServer) ValidateGiftCardTransaction(ctx context.Context, req *giftcardsv1.ValidateGiftCardTransactionRequest) (*giftcardsv1.ValidateGiftCardTransactionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "validate gift card transaction", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetGiftCardTransaction()).(*giftcardsv1.GiftCardTransaction)
	packed, err := anypb.New(req.GetGiftCardTransaction())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &giftCardTransactionValidationServerValidateGiftCardTransactionSimulateGiftCardTransactionStep{request: &giftcardsv1.SimulateGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject:               &giftcardsv1.GiftCardBatchRef{DisplayName: cloned.GetSubject().GetDisplayName()},
			GiftCardTransactionId: req.GetGiftCardTransaction().GetGiftCardTransactionId(),
			Detail:                packed,
		},
		GiftCardTransactionId: envelope.GetDisplayName(),
	}}
	if err := s.runValidateGiftCardTransactionSimulateGiftCardTransaction(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/SimulateGiftCardTransaction")
	}
	simulateGiftCardTransactionReply := step.reply
	jobs := make(chan *giftcardsv1.GiftCardTransactionMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.shiftClient.CreateShift(workCtx, &schedulingv1.CreateShiftRequest{
					Context: req.GetContext(),
					Shift: &schedulingv1.Shift{
						Participants: []*schedulingv1.ShiftParticipant{&schedulingv1.ShiftParticipant{
							Contact: &commonv1.ContactInfo{
								PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
								Phone:            req.GetDetails().GetContact().GetPhone(),
								Name:             req.GetDetails().GetContact().GetName(),
							},
						}},
						Audit: &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetGiftCardTransaction().GetMeasurements() {
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
		return nil, grpcx.Wrap(err, "acme.scheduling.v1.ShiftService/CreateShift")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "giftcards.validate_gift_card_transaction.explain_metric_series_decision",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.contact.email": req.GetDetails().GetContact().GetEmail(),
			"context.tenant_id":     req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	validateGiftCardBatchRequest := &giftcardsv1.ValidateGiftCardBatchRequest{Context: req.GetContext()}
	type validateGiftCardBatchResult struct {
		reply *giftcardsv1.ValidateGiftCardBatchResponse
		err   error
	}
	validateGiftCardBatchResults := make(chan validateGiftCardBatchResult, 1)
	go func() {
		reply, err := s.giftCardBatchValidationClient.ValidateGiftCardBatch(ctx, validateGiftCardBatchRequest)
		validateGiftCardBatchResults <- validateGiftCardBatchResult{reply, err}
	}()
	var validateGiftCardBatchReply *giftcardsv1.ValidateGiftCardBatchResponse
	select {
	case result := <-validateGiftCardBatchResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.giftcards.v1.GiftCardBatchValidationService/ValidateGiftCardBatch")
		}
		validateGiftCardBatchReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &giftcardsv1.ValidateGiftCardTransactionResponse{
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				GiftCardBatchId: simulateGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetGiftCardBatchId(),
			},
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Kind:                  req.GetGiftCardTransaction().GetKind(),
		},
		Result: &commonv1.OperationResult{
			Message: simulateGiftCardTransactionReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &giftcardsv1.ValidateGiftCardTransactionResponse_Outcome{
			NextSteps: validateGiftCardBatchReply.GetOutcome().GetNextSteps(),
		},
	}, nil
}

// PreviewGiftCardTransaction previews a gift card transaction.
func (s *GiftCardTransactionValidationServer) PreviewGiftCardTransaction(ctx context.Context, req *giftcardsv1.PreviewGiftCardTransactionRequest) (*giftcardsv1.PreviewGiftCardTransactionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "preview gift card transaction", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetGiftCardTransaction())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateGiftCardTransactionsRequest := &giftcardsv1.RecalculateGiftCardTransactionsRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				GiftCardBatchId: req.GetGiftCardTransaction().GetSubject().GetGiftCardBatchId(),
			},
			Attributes: req.GetGiftCardTransaction().GetAttributes(),
			Kind:       req.GetGiftCardTransaction().GetKind(),
			Detail:     packed,
		},
	}
	recalculateGiftCardTransactionsReply, err := s.giftCardTransactionCalculationPort.RecalculateGiftCardTransactions(ctx, recalculateGiftCardTransactionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/RecalculateGiftCardTransactions")
	}
	return &giftcardsv1.PreviewGiftCardTransactionResponse{
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			GiftCardTransactionId: recalculateGiftCardTransactionsReply.GetGiftCardTransaction().GetGiftCardTransactionId(),
			CorrelationId:         req.GetGiftCardTransaction().GetCorrelationId(),
			Sequence:              req.GetGiftCardTransaction().GetSequence(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckGiftCardTransactionEligibility checks a gift card transaction.
func (s *GiftCardTransactionValidationServer) CheckGiftCardTransactionEligibility(ctx context.Context, req *giftcardsv1.CheckGiftCardTransactionEligibilityRequest) (*giftcardsv1.CheckGiftCardTransactionEligibilityResponse, error) {
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "check gift card transaction eligibility", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetGiftCardTransaction())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded giftcardsv1.GiftCardTransaction
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cloned := proto.Clone(req.GetDetails()).(*giftcardsv1.CheckGiftCardTransactionEligibilityRequest_Details)
	cloned2 := proto.Clone(req.GetDetails()).(*giftcardsv1.CheckGiftCardTransactionEligibilityRequest_Details)
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateSynonymRequest := &searchv1.ValidateSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				DisplayName: (&decoded).GetSubject().GetDisplayName(),
				Email:       cloned.GetContact().GetEmail(),
			},
		},
		Details: &searchv1.ValidateSynonymRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
			Strict:  cloned2.GetStrict(),
		},
		SynonymId: envelope.GetDisplayName(),
	}
	validateSynonymReply, err := validateSynonymCall(ctx, validateSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ValidateSynonym")
	}
	var envelope2 giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	previewGiftCardBatchRequest := &giftcardsv1.PreviewGiftCardBatchRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.PreviewGiftCardBatchRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
		GiftCardBatchId: envelope2.GetDisplayName(),
	}
	previewGiftCardBatchReply, err := grpcx.NewClient(s.conn, giftcardsv1.NewGiftCardBatchValidationServiceClient).PreviewGiftCardBatch(ctx, previewGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/PreviewGiftCardBatch")
	}
	return &giftcardsv1.CheckGiftCardTransactionEligibilityResponse{
		Result:  &commonv1.OperationResult{Status: validateSynonymReply.GetOutcome().GetStatus()},
		Outcome: &giftcardsv1.CheckGiftCardTransactionEligibilityResponse_Outcome{Status: previewGiftCardBatchReply.GetOutcome().GetStatus()},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: previewGiftCardBatchReply.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			OccurredAt:            req.GetGiftCardTransaction().GetOccurredAt(),
		},
	}, nil
}

// NormalizeGiftCardTransaction normalizes a gift card transaction.
func (s *GiftCardTransactionValidationServer) NormalizeGiftCardTransaction(ctx context.Context, req *giftcardsv1.NormalizeGiftCardTransactionRequest) (*giftcardsv1.NormalizeGiftCardTransactionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "normalize gift card transaction", "tenant", req.GetContext().GetTenantId())
	compareMediaAssetOptionsReply, err := s.mediaAssetCalculationClient.CompareMediaAssetOptions(ctx, &mediav1.CompareMediaAssetOptionsRequest{
		Context: req.GetContext(),
		MediaAsset: &mediav1.MediaAsset{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetContact().GetEmail(),
				DisplayName: req.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetCalculationService/CompareMediaAssetOptions")
	}
	cloned := proto.Clone(req.GetDetails()).(*giftcardsv1.NormalizeGiftCardTransactionRequest_Details)
	recalculateGiftCardsRequest := &giftcardsv1.RecalculateGiftCardsRequest{}
	recalculateGiftCardsRequest.Context = req.GetContext()
	recalculateGiftCardsRequest.Details = &giftcardsv1.RecalculateGiftCardsRequest_Details{
		Destination: &commonv1.PostalAddress{
			CountryCode: compareMediaAssetOptionsReply.GetMediaAsset().GetAddress().GetCountryCode(),
			Lines:       compareMediaAssetOptionsReply.GetMediaAsset().GetAddress().GetLines(),
		},
	}
	recalculateGiftCardsRequest.GiftCard = &giftcardsv1.GiftCard{
		Contact: &commonv1.ContactInfo{Phone: cloned.GetContact().GetPhone()},
	}
	recalculateGiftCardsReply, err := s.giftCardCalculationClient.RecalculateGiftCards(ctx, recalculateGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/RecalculateGiftCards")
	}
	_ = recalculateGiftCardsReply
	return &giftcardsv1.NormalizeGiftCardTransactionResponse{
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			CorrelationId:         compareMediaAssetOptionsReply.GetResult().GetCorrelationId(),
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Source:                req.GetGiftCardTransaction().GetSource(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreGiftCardTransaction scores a gift card transaction.
func (s *GiftCardTransactionValidationServer) ScoreGiftCardTransaction(ctx context.Context, req *giftcardsv1.ScoreGiftCardTransactionRequest) (*giftcardsv1.ScoreGiftCardTransactionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "score gift card transaction", "tenant", req.GetContext().GetTenantId())
	giftCardTransactionCache := cache.New[string, *giftcardsv1.GiftCardTransaction](1)
	giftCardTransactionCache.Put(req.GetContext().GetTraceId(), req.GetGiftCardTransaction())
	cachedGiftCardTransaction, _ := giftCardTransactionCache.Get(req.GetContext().GetTraceId())
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	compareGiftCardTransactionOptionsRequest := &giftcardsv1.CompareGiftCardTransactionOptionsRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{Number: cachedGiftCardTransaction.GetSubject().GetNumber()},
		},
		Details: &giftcardsv1.CompareGiftCardTransactionOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
		GiftCardTransactionId: envelope.GetDisplayName(),
	}
	compareGiftCardTransactionOptionsReply := new(giftcardsv1.CompareGiftCardTransactionOptionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardTransactionCalculationService_CompareGiftCardTransactionOptions_FullMethodName, compareGiftCardTransactionOptionsRequest, compareGiftCardTransactionOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/CompareGiftCardTransactionOptions")
	}
	for measurement := range iterx.Each(req.GetGiftCardTransaction().GetMeasurements(), func(candidate *giftcardsv1.GiftCardTransactionMeasurement) bool { return candidate != nil }) {
		if _, err := s.giftCardCalculationClient.SimulateGiftCard(ctx, &giftcardsv1.SimulateGiftCardRequest{
			Context: req.GetContext(),
			GiftCard: &giftcardsv1.GiftCard{
				Audit: &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
				Holder: &giftcardsv1.GiftCardTransactionRef{
					GiftCardTransactionId: req.GetGiftCardTransaction().GetGiftCardTransactionId(),
				},
			},
			Details: &giftcardsv1.SimulateGiftCardRequest_Details{Budget: req.GetDetails().GetBudget()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/SimulateGiftCard")
		}
	}
	explainFormCalculationRequest := &formsv1.ExplainFormCalculationRequest{
		Context: req.GetContext(),
		Form: &formsv1.Form{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
	}
	explainFormCalculationReply, err := grpcx.Call(ctx, s.formCalculationClient.ExplainFormCalculation, explainFormCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forms.v1.FormCalculationService/ExplainFormCalculation")
	}
	getGiftCardTransactionRatesRequest := &giftcardsv1.GetGiftCardTransactionRatesRequest{
		Context:               req.GetContext(),
		GiftCardTransactionId: req.GetGiftCardTransaction().GetGiftCardTransactionId(),
	}
	getGiftCardTransactionRatesReply, err := s.giftCardTransactionCalculationPort.GetGiftCardTransactionRates(ctx, getGiftCardTransactionRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/GetGiftCardTransactionRates")
	}
	return &giftcardsv1.ScoreGiftCardTransactionResponse{
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Kind:                  compareGiftCardTransactionOptionsReply.GetGiftCardTransaction().GetKind(),
			Sequence:              compareGiftCardTransactionOptionsReply.GetGiftCardTransaction().GetSequence(),
			GiftCardTransactionId: getGiftCardTransactionRatesReply.GetGiftCardTransaction().GetGiftCardTransactionId(),
			Source:                req.GetGiftCardTransaction().GetSource(),
			Subject:               req.GetGiftCardTransaction().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			Message: explainFormCalculationReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// LintGiftCardTransactions lints a gift card transaction.
func (s *GiftCardTransactionValidationServer) LintGiftCardTransactions(ctx context.Context, req *giftcardsv1.LintGiftCardTransactionsRequest) (*giftcardsv1.LintGiftCardTransactionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintGiftCardTransactions", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "lint gift card transactions", "tenant", req.GetContext().GetTenantId())
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainGiftCardCalculationRequest := newExplainGiftCardCalculationRequest(withExplainGiftCardCalculationContext(req.GetContext()), withExplainGiftCardCalculationGiftCard(&giftcardsv1.GiftCard{
		Contact: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			Name:             req.GetDetails().GetContact().GetName(),
		},
	}), withExplainGiftCardCalculationDetails(&giftcardsv1.ExplainGiftCardCalculationRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
	}), withExplainGiftCardCalculationGiftCardId(envelope.GetDisplayName()))
	explainGiftCardCalculationReply, err := s.giftCardCalculationClient.ExplainGiftCardCalculation(ctx, explainGiftCardCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/ExplainGiftCardCalculation")
	}
	var envelope2 giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scoreGiftCardRequest := &giftcardsv1.ScoreGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetContact().GetEmail(),
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: req.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
		},
		GiftCardId: envelope2.GetDisplayName(),
	}
	scoreGiftCardReply, err := s.giftCardValidationLazy().ScoreGiftCard(ctx, scoreGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ScoreGiftCard")
	}
	var envelope3 giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	quoteGiftCardBatchRequest := &giftcardsv1.QuoteGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Settlement: &giftcardsv1.GiftCardBatch_Bank{Bank: &giftcardsv1.GiftCardBatchBankSettlement{Bic: scoreGiftCardReply.GetGiftCard().GetBank().GetBic()}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scoreGiftCardReply.GetGiftCard().GetAudit().GetUpdatedBy(),
			},
		},
		GiftCardBatchId: envelope3.GetDisplayName(),
	}
	quoteGiftCardBatchReply, err := quoteGiftCardBatchCall(ctx, quoteGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/QuoteGiftCardBatch")
	}
	var envelope4 giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope4); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	normalizeSynonymRequest := &searchv1.NormalizeSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainGiftCardCalculationReply.GetGiftCard().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &searchv1.NormalizeSynonymRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            scoreGiftCardReply.GetGiftCard().GetContact().GetEmail(),
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
		SynonymId: envelope4.GetDisplayName(),
	}
	normalizeSynonymReply, err := s.synonymValidationPort.NormalizeSynonym(ctx, normalizeSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/NormalizeSynonym")
	}
	return &giftcardsv1.LintGiftCardTransactionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  explainGiftCardCalculationReply.GetResult().GetReferenceIds(),
			Message:       scoreGiftCardReply.GetResult().GetMessage(),
			CorrelationId: quoteGiftCardBatchReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &giftcardsv1.LintGiftCardTransactionsResponse_Outcome{
			Message:     scoreGiftCardReply.GetResult().GetMessage(),
			ReferenceId: normalizeSynonymReply.GetOutcome().GetReferenceId(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: normalizeSynonymReply.GetSynonym().GetOwner().GetDisplayName(),
			},
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Attributes:            req.GetGiftCardTransaction().GetAttributes(),
		},
	}, nil
}

// giftCardTransactionValidationServerValidateGiftCardTransactionSimulateGiftCardTransactionStep carries the request of
// one step of ValidateGiftCardTransaction and, once it ran, its reply.
type giftCardTransactionValidationServerValidateGiftCardTransactionSimulateGiftCardTransactionStep struct {
	request *giftcardsv1.SimulateGiftCardTransactionRequest
	reply   *giftcardsv1.SimulateGiftCardTransactionResponse
}

func (s *GiftCardTransactionValidationServer) runValidateGiftCardTransactionSimulateGiftCardTransaction(ctx context.Context, step *giftCardTransactionValidationServerValidateGiftCardTransactionSimulateGiftCardTransactionStep) error {
	reply, err := s.giftCardTransactionCalculationClient.SimulateGiftCardTransaction(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// giftCardValidationLazy dials acme.giftcards.v1.GiftCardValidationService the first time a handler needs it.
func (s *GiftCardTransactionValidationServer) giftCardValidationLazy() giftcardsv1.GiftCardValidationServiceClient {
	s.giftCardValidationOnce.Do(func() { s.giftCardValidationCached = giftcardsv1.NewGiftCardValidationServiceClient(s.conn) })
	return s.giftCardValidationCached
}
