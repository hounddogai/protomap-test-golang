package shipping

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ShipmentValidationServer implements `acme.shipping.v1.ShipmentValidationService`.
// Checks shipments against the rules before anything acts on them.
type ShipmentValidationServer struct {
	shippingv1.UnsafeShipmentValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	conn                             *grpc.ClientConn
	shipmentCalculationClient        shippingv1.ShipmentCalculationServiceClient
	searchQueryValidationOnce        sync.Once
	searchQueryValidationCached      searchv1.SearchQueryValidationServiceClient
	ledgerAccountQueryClient         ledgerv1.LedgerAccountQueryServiceClient
	scoreRenewalScheduleFn           func(context.Context, *subscriptionsv1.ScoreRenewalScheduleRequest, ...grpc.CallOption) (*subscriptionsv1.ScoreRenewalScheduleResponse, error)
	restockDecisionCalculationClient returnsv1.RestockDecisionCalculationServiceClient
	sellerScorecardValidationClient  sellersv1.SellerScorecardValidationServiceClient
	searchQueryNotificationClient    searchv1.SearchQueryNotificationServiceClient
	bus                              *eventbus.Bus
}

// NewShipmentValidationServer wires a ShipmentValidationServer.
func NewShipmentValidationServer(deps platform.Deps) *ShipmentValidationServer {
	renewalScheduleValidation := subscriptionsv1.NewRenewalScheduleValidationServiceClient(deps.Conn)
	return &ShipmentValidationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.shipping.v1.ShipmentValidationService"),
		conn:                             deps.Conn,
		shipmentCalculationClient:        shippingv1.NewShipmentCalculationServiceClient(deps.Conn),
		ledgerAccountQueryClient:         ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		scoreRenewalScheduleFn:           renewalScheduleValidation.ScoreRenewalSchedule,
		restockDecisionCalculationClient: returnsv1.NewRestockDecisionCalculationServiceClient(deps.Conn),
		sellerScorecardValidationClient:  sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		searchQueryNotificationClient:    searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		bus:                              deps.Bus,
	}
}

// ValidateShipment validates a shipment.
func (s *ShipmentValidationServer) ValidateShipment(ctx context.Context, req *shippingv1.ValidateShipmentRequest) (*shippingv1.ValidateShipmentResponse, error) {
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "validate shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetShipment()).(*shippingv1.Shipment)
	simulateShipmentRequest := &shippingv1.SimulateShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Audit:      &commonv1.AuditInfo{UpdatedBy: req.GetShipment().GetAudit().GetUpdatedBy()},
			Settlement: &shippingv1.Shipment_Bank{Bank: &shippingv1.ShipmentBankSettlement{Bic: req.GetShipment().GetBank().GetBic()}},
			BillingAddress: &commonv1.PostalAddress{
				Locality: req.GetShipment().GetBillingAddress().GetLocality(),
			},
		},
		Details: &shippingv1.SimulateShipmentRequest_Details{
			Destination: &commonv1.PostalAddress{PostalCode: cloned.GetBillingAddress().GetPostalCode()},
		},
	}
	simulateShipmentReply := new(shippingv1.SimulateShipmentResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.ShipmentCalculationService_SimulateShipment_FullMethodName, simulateShipmentRequest, simulateShipmentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/SimulateShipment")
	}
	for _, attachment := range req.GetShipment().GetAttachments() {
		if _, err := s.shipmentCalculationClient.CalculateShipment(ctx, &shippingv1.CalculateShipmentRequest{
			Context: req.GetContext(),
			Shipment: &shippingv1.Shipment{
				Attachments: []*shippingv1.ShipmentAttachment{&shippingv1.ShipmentAttachment{ContentType: attachment.GetContentType()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/CalculateShipment")
		}
	}
	quoteShipmentRequest := &shippingv1.QuoteShipmentRequest{Context: req.GetContext()}
	quoteShipmentReply, err := grpcx.NewClient(s.conn, shippingv1.NewShipmentCalculationServiceClient).QuoteShipment(ctx, quoteShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/QuoteShipment")
	}
	explainShipmentCalculationRequest := &shippingv1.ExplainShipmentCalculationRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ExplainShipmentCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	explainShipmentCalculationReply := new(shippingv1.ExplainShipmentCalculationResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", shippingShipmentCalculationServiceName, "ExplainShipmentCalculation"), explainShipmentCalculationRequest, explainShipmentCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/ExplainShipmentCalculation")
	}
	return &shippingv1.ValidateShipmentResponse{
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{
				Etag: simulateShipmentReply.GetShipment().GetAudit().GetEtag(),
			},
			Counterparty: &shippingv1.ParcelRef{
				ParcelId: simulateShipmentReply.GetShipment().GetCounterparty().GetParcelId(),
			},
			DueAt:      quoteShipmentReply.GetShipment().GetDueAt(),
			ShipmentId: req.GetShipmentId(),
			Status:     req.GetShipment().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: quoteShipmentReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// PreviewShipment previews a shipment.
func (s *ShipmentValidationServer) PreviewShipment(ctx context.Context, req *shippingv1.PreviewShipmentRequest) (*shippingv1.PreviewShipmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewShipment", "error", err)
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "preview shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	shipmentCache := cache.New[string, *shippingv1.Shipment](1)
	shipmentCache.Put(req.GetContext().GetTraceId(), req.GetShipment())
	cachedShipment, _ := shipmentCache.Get(req.GetContext().GetTraceId())
	explainShipmentCalculationRequest := &shippingv1.ExplainShipmentCalculationRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetShipment().GetBillingAddress().GetCountryCode(),
			},
			Counterparty: &shippingv1.ParcelRef{Code: cachedShipment.GetCounterparty().GetCode()},
		},
	}
	type explainShipmentCalculationResult struct {
		reply *shippingv1.ExplainShipmentCalculationResponse
		err   error
	}
	explainShipmentCalculationResults := make(chan explainShipmentCalculationResult, 1)
	go func() {
		reply, err := s.shipmentCalculationClient.ExplainShipmentCalculation(ctx, explainShipmentCalculationRequest)
		explainShipmentCalculationResults <- explainShipmentCalculationResult{reply, err}
	}()
	var explainShipmentCalculationReply *shippingv1.ExplainShipmentCalculationResponse
	select {
	case result := <-explainShipmentCalculationResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.shipping.v1.ShipmentCalculationService/ExplainShipmentCalculation")
		}
		explainShipmentCalculationReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	normalizeSearchQueryRequest := &searchv1.NormalizeSearchQueryRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				Code:        req.GetShipment().GetCounterparty().GetCode(),
				DisplayName: explainShipmentCalculationReply.GetShipment().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &searchv1.NormalizeSearchQueryRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}
	normalizeSearchQueryReply, err := s.searchQueryValidationLazy().NormalizeSearchQuery(ctx, normalizeSearchQueryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/NormalizeSearchQuery")
	}
	compareShipmentOptionsReply, err := s.shipmentCalculationClient.CompareShipmentOptions(ctx, &shippingv1.CompareShipmentOptionsRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Settlement: &shippingv1.Shipment_Card{Card: &shippingv1.ShipmentCardSettlement{
				HolderName: explainShipmentCalculationReply.GetShipment().GetCard().GetHolderName(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: explainShipmentCalculationReply.GetShipment().GetBillingAddress().GetCountryCode(),
				PostalCode:  explainShipmentCalculationReply.GetShipment().GetBillingAddress().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetShipment().GetAudit().GetCreatedBy()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/CompareShipmentOptions")
	}
	return &shippingv1.PreviewShipmentResponse{
		Shipment: &shippingv1.Shipment{
			Total: explainShipmentCalculationReply.GetShipment().GetTotal(),
			BillingAddress: &commonv1.PostalAddress{
				Region: explainShipmentCalculationReply.GetShipment().GetBillingAddress().GetRegion(),
			},
			ShipmentId:   req.GetShipmentId(),
			Counterparty: req.GetShipment().GetCounterparty(),
		},
		Outcome: &shippingv1.PreviewShipmentResponse_Outcome{
			NextSteps: normalizeSearchQueryReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: compareShipmentOptionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// CheckShipmentEligibility checks a shipment.
func (s *ShipmentValidationServer) CheckShipmentEligibility(ctx context.Context, req *shippingv1.CheckShipmentEligibilityRequest) (*shippingv1.CheckShipmentEligibilityResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "check shipment eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	jobs := make(chan *shippingv1.ShipmentLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.shipmentCalculationClient.RecalculateShipments(workCtx, &shippingv1.RecalculateShipmentsRequest{
					Context: req.GetContext(),
					Shipment: &shippingv1.Shipment{
						Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{Tax: &shippingv1.ShipmentLineTax{Amount: line.GetAmount()}}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetShipment().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/RecalculateShipments")
	}
	for _, line2 := range req.GetShipment().GetLines() {
		if _, err := s.shipmentCalculationClient.CompareShipmentOptions(ctx, &shippingv1.CompareShipmentOptionsRequest{
			Context: req.GetContext(),
			Shipment: &shippingv1.Shipment{
				Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{
					Tax:      &shippingv1.ShipmentLineTax{Amount: line2.GetTax().GetAmount()},
					Quantity: line2.GetQuantity(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/CompareShipmentOptions")
		}
	}
	return &shippingv1.CheckShipmentEligibilityResponse{
		Shipment: &shippingv1.Shipment{
			ShipmentId: req.GetShipmentId(),
			IssuedAt:   req.GetShipment().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeShipment normalizes a shipment.
func (s *ShipmentValidationServer) NormalizeShipment(ctx context.Context, req *shippingv1.NormalizeShipmentRequest) (*shippingv1.NormalizeShipmentResponse, error) {
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "normalize shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	exportLedgerAccountsStream, err := s.ledgerAccountQueryClient.ExportLedgerAccounts(ctx, &ledgerv1.ExportLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.ExportLedgerAccountsRequest_Filter{
			Region:       req.GetShipment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetShipment().GetAudit().GetCreatedAt(),
			Labels:       req.GetShipment().GetMetadata(),
			OwnerId:      req.GetShipmentId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/ExportLedgerAccounts")
	}
	for {
		received, err := exportLedgerAccountsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/ExportLedgerAccounts")
		}
		_ = received
	}
	var token string
	switch choice := req.GetShipment().GetSettlement().(type) {
	case *shippingv1.Shipment_Card:
		token = choice.Card.GetToken()
	case *shippingv1.Shipment_Bank:
		token = choice.Bank.GetIban()
	case *shippingv1.Shipment_VoucherCode:
		token = choice.VoucherCode
	}
	var holder struct{ DisplayName string }
	if record := req.GetShipment().GetCounterparty(); record != nil {
		if err := mapper.CopyFields(&holder, record, "DisplayName"); err != nil {
			s.log.WarnContext(ctx, "copy display_name", "error", err)
		}
	}
	estimateShipmentRequest := &shippingv1.EstimateShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Audit:        &commonv1.AuditInfo{Etag: req.GetShipment().GetAudit().GetEtag()},
			Settlement:   &shippingv1.Shipment_Card{Card: &shippingv1.ShipmentCardSettlement{Token: token}},
			Counterparty: &shippingv1.ParcelRef{DisplayName: holder.DisplayName},
		},
	}
	estimateShipmentReply, err := grpcx.NewClient(s.conn, shippingv1.NewShipmentCalculationServiceClient).EstimateShipment(ctx, estimateShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/EstimateShipment")
	}
	return &shippingv1.NormalizeShipmentResponse{
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				Region: estimateShipmentReply.GetShipment().GetBillingAddress().GetRegion(),
			},
			Counterparty: estimateShipmentReply.GetShipment().GetCounterparty(),
			ShipmentId:   req.GetShipmentId(),
			DueAt:        req.GetShipment().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainShipmentDecision explains a shipment.
func (s *ShipmentValidationServer) ExplainShipmentDecision(ctx context.Context, req *shippingv1.ExplainShipmentDecisionRequest) (*shippingv1.ExplainShipmentDecisionResponse, error) {
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "explain shipment decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "etag", req.GetShipment().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetShipment()).(*shippingv1.Shipment)
	scoreRenewalScheduleRequest := &subscriptionsv1.ScoreRenewalScheduleRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
			}},
			Audit: &commonv1.AuditInfo{Etag: tenancy.Value(ctx, "etag")},
		},
		Details: &subscriptionsv1.ScoreRenewalScheduleRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:      req.GetShipment().GetBillingAddress().GetRegion(),
				CountryCode: cloned.GetBillingAddress().GetCountryCode(),
			},
		},
	}
	scoreRenewalScheduleReply, err := s.scoreRenewalScheduleFn(ctx, scoreRenewalScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/ScoreRenewalSchedule")
	}
	estimateRestockDecisionRequest := &returnsv1.EstimateRestockDecisionRequest{}
	estimateRestockDecisionRequest.Context = req.GetContext()
	estimateRestockDecisionRequest.Details = &returnsv1.EstimateRestockDecisionRequest_Details{
		Destination: &commonv1.PostalAddress{
			CountryCode: req.GetShipment().GetBillingAddress().GetCountryCode(),
		},
	}
	estimateRestockDecisionReply, err := s.restockDecisionCalculationClient.EstimateRestockDecision(ctx, estimateRestockDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/EstimateRestockDecision")
	}
	return &shippingv1.ExplainShipmentDecisionResponse{
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: scoreRenewalScheduleReply.GetRenewalSchedule().GetLocation().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: estimateRestockDecisionReply.GetRestockDecision().GetAudit().GetUpdatedBy(),
			},
			ShipmentId: req.GetShipmentId(),
			Metadata:   req.GetShipment().GetMetadata(),
		},
		Outcome: &shippingv1.ExplainShipmentDecisionResponse_Outcome{
			Message: estimateRestockDecisionReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreShipment scores a shipment.
func (s *ShipmentValidationServer) ScoreShipment(ctx context.Context, req *shippingv1.ScoreShipmentRequest) (*shippingv1.ScoreShipmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreShipment", "error", err)
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "score shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	jobs := make(chan *shippingv1.ShipmentLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.sellerScorecardValidationClient.NormalizeSellerScorecard(workCtx, &sellersv1.NormalizeSellerScorecardRequest{
					Context: req.GetContext(),
					SellerScorecard: &sellersv1.SellerScorecard{
						Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
							Tax: &sellersv1.SellerScorecardLineTax{
								Rate:       line.GetTax().GetRate(),
								Exemptions: line.GetTax().GetExemptions(),
								Amount:     line.GetAmount(),
							},
						}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetShipment().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/NormalizeSellerScorecard")
	}
	for _, line2 := range req.GetShipment().GetLines() {
		if _, err := s.searchQueryNotificationClient.ListSearchQuerySubscriptions(ctx, &searchv1.ListSearchQuerySubscriptionsRequest{
			Context: req.GetContext(),
			Filter: &searchv1.ListSearchQuerySubscriptionsRequest_Filter{
				Region:  req.GetDetails().GetDestination().GetRegion(),
				OwnerId: line2.GetLineId(),
				Labels:  line2.GetAttributes(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/ListSearchQuerySubscriptions")
		}
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "shipping.score_shipment.recalculate_seller_payout_schedules",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"seller_payout_schedule.audit.created_by": req.GetShipment().GetAudit().GetCreatedBy(),
			"details.destination.recipient":           req.GetShipment().GetBillingAddress().GetRecipient(),
			"seller_payout_schedule.audit.updated_by": req.GetShipment().GetAudit().GetUpdatedBy(),
			"details.destination.region":              req.GetShipment().GetBillingAddress().GetRegion(),
			"context.tenant_id":                       req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	shipmentCache := cache.New[string, *shippingv1.Shipment](1)
	shipmentCache.Put(req.GetContext().GetTraceId(), req.GetShipment())
	cachedShipment, _ := shipmentCache.Get(req.GetContext().GetTraceId())
	calculateShipmentRequest := newCalculateShipmentRequest(withCalculateShipmentContext(req.GetContext()), withCalculateShipmentDetails(&shippingv1.CalculateShipmentRequest_Details{
		Destination: &commonv1.PostalAddress{Locality: cachedShipment.GetBillingAddress().GetLocality()},
	}))
	calculateShipmentReply, err := s.shipmentCalculationClient.CalculateShipment(ctx, calculateShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/CalculateShipment")
	}
	return &shippingv1.ScoreShipmentResponse{
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: calculateShipmentReply.GetShipment().GetBillingAddress().GetPostalCode(),
			},
			Total:      calculateShipmentReply.GetShipment().GetTotal(),
			ShipmentId: req.GetShipmentId(),
			Metadata:   req.GetShipment().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintShipments lints a shipment.
func (s *ShipmentValidationServer) LintShipments(ctx context.Context, req *shippingv1.LintShipmentsRequest) (*shippingv1.LintShipmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "lint shipments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*shippingv1.QuoteShipmentResponse, len(req.GetShipment().GetLines()))
	for i, line := range req.GetShipment().GetLines() {
		group.Go(func() error {
			reply, err := s.shipmentCalculationClient.QuoteShipment(groupCtx, &shippingv1.QuoteShipmentRequest{
				Context: req.GetContext(),
				Shipment: &shippingv1.Shipment{
					Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{
						Tax: &shippingv1.ShipmentLineTax{
							Exemptions:   line.GetTax().GetExemptions(),
							Jurisdiction: line.GetTax().GetJurisdiction(),
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
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/QuoteShipment")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &shippingv1.LintShipmentsResponse{
		Shipment: &shippingv1.Shipment{
			ShipmentId:     req.GetShipmentId(),
			BillingAddress: req.GetShipment().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetShipmentRules returns a shipment.
func (s *ShipmentValidationServer) GetShipmentRules(ctx context.Context, req *shippingv1.GetShipmentRulesRequest) (*shippingv1.GetShipmentRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipment", req.GetShipmentId(), "get_shipment_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get shipment rules", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.GetShipmentRulesResponse{
		Shipment: &shippingv1.Shipment{ShipmentId: req.GetShipmentId()},
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// searchQueryValidationLazy dials acme.search.v1.SearchQueryValidationService the first time a handler needs it.
func (s *ShipmentValidationServer) searchQueryValidationLazy() searchv1.SearchQueryValidationServiceClient {
	s.searchQueryValidationOnce.Do(func() { s.searchQueryValidationCached = searchv1.NewSearchQueryValidationServiceClient(s.conn) })
	return s.searchQueryValidationCached
}
