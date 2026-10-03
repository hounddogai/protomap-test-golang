package giftcards

import (
	"context"
	"errors"
	"io"
	"log/slog"

	audiencesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audiences/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	dispatchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/dispatch/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	onboardingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	suppliersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/suppliers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// GiftCardQueryServer implements `acme.giftcards.v1.GiftCardQueryService`.
// Answers read-only questions about gift cards, for screens and reports.
type GiftCardQueryServer struct {
	giftcardsv1.UnimplementedGiftCardQueryServiceServer
	store                                *Store
	log                                  *slog.Logger
	clients                              *registry.Registry
	conn                                 *grpc.ClientConn
	giftCardValidationClient             giftcardsv1.GiftCardValidationServiceClient
	synonymHistoryPort                   synonymHistoryPort
	dispatchOrderQueryClient             dispatchv1.DispatchOrderQueryServiceClient
	giftCardTransactionValidationPort    giftCardTransactionValidationPort
	findSuppliersFn                      func(context.Context, *suppliersv1.FindSuppliersRequest, ...grpc.CallOption) (*suppliersv1.FindSuppliersResponse, error)
	giftCardCalculationClient            giftcardsv1.GiftCardCalculationServiceClient
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
	onboardingPlanQueryClient            onboardingv1.OnboardingPlanQueryServiceClient
	audienceQueryClient                  audiencesv1.AudienceQueryServiceClient
	giftCardTransactionValidationClient  giftcardsv1.GiftCardTransactionValidationServiceClient
}

// NewGiftCardQueryServer wires a GiftCardQueryServer.
func NewGiftCardQueryServer(deps platform.Deps) GiftCardQueryServer {
	supplierQuery := suppliersv1.NewSupplierQueryServiceClient(deps.Conn)
	return GiftCardQueryServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.giftcards.v1.GiftCardQueryService"),
		clients:                              deps.Clients,
		conn:                                 deps.Conn,
		giftCardValidationClient:             giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		synonymHistoryPort:                   newSynonymHistoryPort(deps),
		dispatchOrderQueryClient:             dispatchv1.NewDispatchOrderQueryServiceClient(deps.Conn),
		giftCardTransactionValidationPort:    newGiftCardTransactionValidationPort(deps),
		findSuppliersFn:                      supplierQuery.FindSuppliers,
		giftCardCalculationClient:            giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		onboardingPlanQueryClient:            onboardingv1.NewOnboardingPlanQueryServiceClient(deps.Conn),
		audienceQueryClient:                  audiencesv1.NewAudienceQueryServiceClient(deps.Conn),
		giftCardTransactionValidationClient:  giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn),
	}
}

// LookupGiftCard returns a gift card.
func (s GiftCardQueryServer) LookupGiftCard(ctx context.Context, req *giftcardsv1.LookupGiftCardRequest) (*giftcardsv1.LookupGiftCardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card", req.GetGiftCardId(), "lookup_gift_card"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup gift card", "tenant", req.GetContext().GetTenantId())
	giftCardTransactionValidation, ok := s.clients.Get("acme.giftcards.v1.GiftCardTransactionValidationService").(giftcardsv1.GiftCardTransactionValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardTransactionValidationService")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreGiftCardTransactionRequest := &giftcardsv1.ScoreGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			CorrelationId: req.GetGiftCardId(),
			Detail:        packed,
		},
	}
	scoreGiftCardTransactionReply, err := giftCardTransactionValidation.ScoreGiftCardTransaction(ctx, scoreGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/ScoreGiftCardTransaction")
	}
	return &giftcardsv1.LookupGiftCardResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scoreGiftCardTransactionReply.GetResult().GetReferenceIds(),
			Message:      scoreGiftCardTransactionReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{GiftCardId: req.GetGiftCardId()},
	}, nil
}

// BrowseGiftCards lists gift cards a page at a time.
func (s GiftCardQueryServer) BrowseGiftCards(ctx context.Context, req *giftcardsv1.BrowseGiftCardsRequest) (*giftcardsv1.BrowseGiftCardsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "browse gift cards", "tenant", req.GetContext().GetTenantId())
	previewGiftCardTransactionRequest := &giftcardsv1.PreviewGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.PreviewGiftCardTransactionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			CorrelationId: req.GetFilter().GetOwnerId(),
			Measurements:  []*giftcardsv1.GiftCardTransactionMeasurement{&giftcardsv1.GiftCardTransactionMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
		},
	}
	previewGiftCardTransactionReply, err := grpcx.NewClient(s.conn, giftcardsv1.NewGiftCardTransactionValidationServiceClient).PreviewGiftCardTransaction(ctx, previewGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/PreviewGiftCardTransaction")
	}
	explainGiftCardDecisionRequest := &giftcardsv1.ExplainGiftCardDecisionRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{
				GiftCardTransactionId: previewGiftCardTransactionReply.GetGiftCardTransaction().GetGiftCardTransactionId(),
			},
		},
	}
	explainGiftCardDecisionReply := new(giftcardsv1.ExplainGiftCardDecisionResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardValidationService_ExplainGiftCardDecision_FullMethodName, explainGiftCardDecisionRequest, explainGiftCardDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ExplainGiftCardDecision")
	}
	return &giftcardsv1.BrowseGiftCardsResponse{
		Result: &commonv1.OperationResult{
			Message:      previewGiftCardTransactionReply.GetResult().GetMessage(),
			ReferenceIds: previewGiftCardTransactionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		GiftCards: []*giftcardsv1.GiftCard{&giftcardsv1.GiftCard{
			Labels: explainGiftCardDecisionReply.GetGiftCard().GetLabels(),
		}},
	}, nil
}

// FindGiftCards lists gift cards a page at a time.
func (s GiftCardQueryServer) FindGiftCards(ctx context.Context, req *giftcardsv1.FindGiftCardsRequest) (*giftcardsv1.FindGiftCardsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "find gift cards", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	normalizeGiftCardReply, err := s.giftCardValidationClient.NormalizeGiftCard(ctx, &giftcardsv1.NormalizeGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Labels:  labels,
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/NormalizeGiftCard")
	}
	annotateSynonymVersionRequest := &searchv1.AnnotateSynonymVersionRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				DisplayName: normalizeGiftCardReply.GetGiftCard().GetHolder().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizeGiftCardReply.GetGiftCard().GetAudit().GetUpdatedAt(),
				Etag:      normalizeGiftCardReply.GetGiftCard().GetAudit().GetEtag(),
			},
		},
	}
	annotateSynonymVersionReply, err := s.synonymHistoryPort.AnnotateSynonymVersion(ctx, annotateSynonymVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/AnnotateSynonymVersion")
	}
	return &giftcardsv1.FindGiftCardsResponse{
		GiftCards: []*giftcardsv1.GiftCard{&giftcardsv1.GiftCard{
			GiftCardId: normalizeGiftCardReply.GetGiftCard().GetGiftCardId(),
		}},
		Result: &commonv1.OperationResult{
			Message:      normalizeGiftCardReply.GetOutcome().GetMessage(),
			Details:      annotateSynonymVersionReply.GetResult().GetDetails(),
			ReferenceIds: annotateSynonymVersionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// CountGiftCards counts gift cards by status.
func (s GiftCardQueryServer) CountGiftCards(ctx context.Context, req *giftcardsv1.CountGiftCardsRequest) (*giftcardsv1.CountGiftCardsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count gift cards", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	scoreGiftCardRequest := &giftcardsv1.ScoreGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Labels: labels,
			Audit:  &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &giftcardsv1.ScoreGiftCardRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	scoreGiftCardReply, err := grpcx.Call(ctx, s.giftCardValidationClient.ScoreGiftCard, scoreGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ScoreGiftCard")
	}
	resolveDispatchOrdersRequest := &dispatchv1.ResolveDispatchOrdersRequest{}
	resolveDispatchOrdersRequest.Context = req.GetContext()
	resolveDispatchOrdersReply, err := s.dispatchOrderQueryClient.ResolveDispatchOrders(ctx, resolveDispatchOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.dispatch.v1.DispatchOrderQueryService/ResolveDispatchOrders")
	}
	_ = resolveDispatchOrdersReply
	return &giftcardsv1.CountGiftCardsResponse{
		Result: &commonv1.OperationResult{
			Message: scoreGiftCardReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ResolveGiftCards returns several gift cards at once.
func (s GiftCardQueryServer) ResolveGiftCards(ctx context.Context, req *giftcardsv1.ResolveGiftCardsRequest) (*giftcardsv1.ResolveGiftCardsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve gift cards", "tenant", req.GetContext().GetTenantId())
	previewGiftCardRequest := &giftcardsv1.PreviewGiftCardRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.PreviewGiftCardRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
	}
	previewGiftCardReply, err := grpcx.Call(ctx, s.giftCardValidationClient.PreviewGiftCard, previewGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/PreviewGiftCard")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateGiftCardTransactionRequest := &giftcardsv1.ValidateGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ValidateGiftCardTransactionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  previewGiftCardReply.GetGiftCard().GetContact().GetName(),
				Phone: previewGiftCardReply.GetGiftCard().GetContact().GetPhone(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	validateGiftCardTransactionReply, err := s.giftCardTransactionValidationPort.ValidateGiftCardTransaction(ctx, validateGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/ValidateGiftCardTransaction")
	}
	return &giftcardsv1.ResolveGiftCardsResponse{
		GiftCards: []*giftcardsv1.GiftCard{&giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{
				Email: previewGiftCardReply.GetGiftCard().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewGiftCardReply.GetGiftCard().GetAudit().GetCreatedBy(),
			},
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: validateGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExportGiftCards streams every matching gift card.
func (s GiftCardQueryServer) ExportGiftCards(req *giftcardsv1.ExportGiftCardsRequest, stream grpc.ServerStreamingServer[giftcardsv1.ExportGiftCardsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export gift cards", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	previewGiftCardRequest := &giftcardsv1.PreviewGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Labels:  labels,
			Contact: &commonv1.ContactInfo{Email: (&decoded).GetActor().GetEmail()},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &giftcardsv1.PreviewGiftCardRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	previewGiftCardReply, err := previewGiftCardCall(ctx, previewGiftCardRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/PreviewGiftCard")
	}
	findSuppliersRequest := &suppliersv1.FindSuppliersRequest{
		Context: req.GetContext(),
		Filter: &suppliersv1.FindSuppliersRequest_Filter{
			OwnerId: req.GetFilter().GetOwnerId(),
			Query:   req.GetFilter().GetQuery(),
		},
	}
	findSuppliersReply, err := s.findSuppliersFn(ctx, findSuppliersRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.suppliers.v1.SupplierQueryService/FindSuppliers")
	}
	if err := stream.Send(&giftcardsv1.ExportGiftCardsResponse{
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{
				GiftCardTransactionId: previewGiftCardReply.GetGiftCard().GetHolder().GetGiftCardTransactionId(),
			},
			CurrencyCode: previewGiftCardReply.GetGiftCard().GetCurrencyCode(),
			Labels:       req.GetFilter().GetLabels(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: findSuppliersReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeGiftCards summarizes a gift card.
func (s GiftCardQueryServer) SummarizeGiftCards(ctx context.Context, req *giftcardsv1.SummarizeGiftCardsRequest) (*giftcardsv1.SummarizeGiftCardsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SummarizeGiftCards", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card", req.GetGiftCardId(), "summarize_gift_cards"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize gift cards", "tenant", req.GetContext().GetTenantId())
	calculateGiftCardReply, err := s.giftCardCalculationClient.CalculateGiftCard(ctx, &giftcardsv1.CalculateGiftCardRequest{
		Context:     req.GetContext(),
		GiftCard:    &giftcardsv1.GiftCard{GiftCardId: req.GetGiftCardId()},
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/CalculateGiftCard")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainGiftCardTransactionCalculationRequest := &giftcardsv1.ExplainGiftCardTransactionCalculationRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ExplainGiftCardTransactionCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            calculateGiftCardReply.GetGiftCard().GetContact().GetEmail(),
				PreferredChannel: calculateGiftCardReply.GetGiftCard().GetContact().GetPreferredChannel(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: calculateGiftCardReply.GetGiftCard().GetHolder().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	explainGiftCardTransactionCalculationReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*giftcardsv1.ExplainGiftCardTransactionCalculationResponse, error) {
		return s.giftCardTransactionCalculationClient.ExplainGiftCardTransactionCalculation(ctx, explainGiftCardTransactionCalculationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/ExplainGiftCardTransactionCalculation")
	}
	suggestOnboardingPlansRequest := &onboardingv1.SuggestOnboardingPlansRequest{
		Context: req.GetContext(),
		Filter: &onboardingv1.SuggestOnboardingPlansRequest_Filter{
			Labels:       calculateGiftCardReply.GetGiftCard().GetLabels(),
			CreatedAfter: calculateGiftCardReply.GetGiftCard().GetOpenedAt(),
		},
	}
	type suggestOnboardingPlansResult struct {
		reply *onboardingv1.SuggestOnboardingPlansResponse
		err   error
	}
	suggestOnboardingPlansResults := make(chan suggestOnboardingPlansResult, 1)
	go func() {
		reply, err := s.onboardingPlanQueryClient.SuggestOnboardingPlans(ctx, suggestOnboardingPlansRequest)
		suggestOnboardingPlansResults <- suggestOnboardingPlansResult{reply, err}
	}()
	var suggestOnboardingPlansReply *onboardingv1.SuggestOnboardingPlansResponse
	select {
	case result := <-suggestOnboardingPlansResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.onboarding.v1.OnboardingPlanQueryService/SuggestOnboardingPlans")
		}
		suggestOnboardingPlansReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	exportAudiencesStream, err := s.audienceQueryClient.ExportAudiences(ctx, &audiencesv1.ExportAudiencesRequest{
		Context: req.GetContext(),
		Filter:  &audiencesv1.ExportAudiencesRequest_Filter{Labels: calculateGiftCardReply.GetGiftCard().GetLabels()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audiences.v1.AudienceQueryService/ExportAudiences")
	}
	for {
		received, err := exportAudiencesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.audiences.v1.AudienceQueryService/ExportAudiences")
		}
		_ = received
	}
	_ = suggestOnboardingPlansReply
	return &giftcardsv1.SummarizeGiftCardsResponse{
		Result: &commonv1.OperationResult{
			Message: calculateGiftCardReply.GetResult().GetMessage(),
			Status:  explainGiftCardTransactionCalculationReply.GetOutcome().GetStatus(),
			Details: explainGiftCardTransactionCalculationReply.GetResult().GetDetails(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestGiftCards lists gift cards a page at a time.
func (s GiftCardQueryServer) SuggestGiftCards(ctx context.Context, req *giftcardsv1.SuggestGiftCardsRequest) (*giftcardsv1.SuggestGiftCardsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "suggest gift cards", "tenant", req.GetContext().GetTenantId())
	checkGiftCardTransactionEligibilityRequest := buildCheckGiftCardTransactionEligibilityRequestForSuggestGiftCards(req)
	checkGiftCardTransactionEligibilityReply, err := s.giftCardTransactionValidationClient.CheckGiftCardTransactionEligibility(ctx, checkGiftCardTransactionEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/CheckGiftCardTransactionEligibility")
	}
	return &giftcardsv1.SuggestGiftCardsResponse{
		Result: &commonv1.OperationResult{
			Status: checkGiftCardTransactionEligibilityReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// PeekGiftCard returns a gift card.
func (s GiftCardQueryServer) PeekGiftCard(ctx context.Context, req *giftcardsv1.PeekGiftCardRequest) (*giftcardsv1.PeekGiftCardResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card", req.GetGiftCardId(), "peek_gift_card"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek gift card", "tenant", req.GetContext().GetTenantId())
	scoreGiftCardRequest := &giftcardsv1.ScoreGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: req.GetGiftCardId(),
			Contact:    &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			OpenedAt:   req.GetContext().GetDeadline(),
			Funding:    &giftcardsv1.GiftCard_ExternalAccountId{ExternalAccountId: req.GetContext().GetActor().GetUserId()},
		},
	}
	scoreGiftCardReply, err := grpcx.Call(ctx, s.giftCardValidationClient.ScoreGiftCard, scoreGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ScoreGiftCard")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewGiftCardTransactionRequest := &giftcardsv1.PreviewGiftCardTransactionRequest{}
	previewGiftCardTransactionRequest.Context = req.GetContext()
	previewGiftCardTransactionRequest.Details = &giftcardsv1.PreviewGiftCardTransactionRequest_Details{
		Contact: &commonv1.ContactInfo{
			Email: scoreGiftCardReply.GetGiftCard().GetContact().GetEmail(),
		},
	}
	previewGiftCardTransactionRequest.GiftCardTransaction = &giftcardsv1.GiftCardTransaction{Detail: packed}
	previewGiftCardTransactionReply, err := s.giftCardTransactionValidationClient.PreviewGiftCardTransaction(ctx, previewGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/PreviewGiftCardTransaction")
	}
	return &giftcardsv1.PeekGiftCardResponse{
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: scoreGiftCardReply.GetGiftCard().GetContact().GetPreferredChannel(),
			},
			GiftCardId: req.GetGiftCardId(),
		},
		Result: &commonv1.OperationResult{
			Message: previewGiftCardTransactionReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}
