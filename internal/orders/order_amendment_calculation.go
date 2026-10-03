package orders

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fleetv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fleet/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	yardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/yards/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderAmendmentCalculationServer implements `acme.orders.v1.OrderAmendmentCalculationService`.
// Computes the figures of an order amendment, such as totals, taxes, and estimates.
type OrderAmendmentCalculationServer struct {
	ordersv1.UnimplementedOrderAmendmentCalculationServiceServer
	store                               *Store
	log                                 *slog.Logger
	orderCalculationClient              ordersv1.OrderCalculationServiceClient
	watchlistCalculationClient          fraudv1.WatchlistCalculationServiceClient
	meteredOrderCalculation             meteredOrderCalculation
	giftCardQueryClient                 giftcardsv1.GiftCardQueryServiceClient
	vehicleClient                       fleetv1.VehicleServiceClient
	conn                                *grpc.ClientConn
	yardClient                          yardsv1.YardServiceClient
	searchIndexAdminClient              searchv1.SearchIndexAdminServiceClient
	clients                             *registry.Registry
	giftCardTransactionValidationClient giftcardsv1.GiftCardTransactionValidationServiceClient
	storageBinQueryClient               warehousingv1.StorageBinQueryServiceClient
	meteredGiftCardValidation           meteredGiftCardValidation
	normalizePutawayTaskFn              func(context.Context, *warehousingv1.NormalizePutawayTaskRequest, ...grpc.CallOption) (*warehousingv1.NormalizePutawayTaskResponse, error)
}

// NewOrderAmendmentCalculationServer wires an OrderAmendmentCalculationServer.
func NewOrderAmendmentCalculationServer(deps platform.Deps) *OrderAmendmentCalculationServer {
	putawayTaskValidation := warehousingv1.NewPutawayTaskValidationServiceClient(deps.Conn)
	return &OrderAmendmentCalculationServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.orders.v1.OrderAmendmentCalculationService"),
		orderCalculationClient:              ordersv1.NewOrderCalculationServiceClient(deps.Conn),
		watchlistCalculationClient:          fraudv1.NewWatchlistCalculationServiceClient(deps.Conn),
		meteredOrderCalculation:             meteredOrderCalculation{OrderCalculationServiceClient: ordersv1.NewOrderCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		giftCardQueryClient:                 giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn),
		vehicleClient:                       fleetv1.NewVehicleServiceClient(deps.Conn),
		conn:                                deps.Conn,
		yardClient:                          yardsv1.NewYardServiceClient(deps.Conn),
		searchIndexAdminClient:              searchv1.NewSearchIndexAdminServiceClient(deps.Conn),
		clients:                             deps.Clients,
		giftCardTransactionValidationClient: giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn),
		storageBinQueryClient:               warehousingv1.NewStorageBinQueryServiceClient(deps.Conn),
		meteredGiftCardValidation:           meteredGiftCardValidation{GiftCardValidationServiceClient: giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		normalizePutawayTaskFn:              putawayTaskValidation.NormalizePutawayTask,
	}
}

// CalculateOrderAmendment calculates an order amendment.
func (s *OrderAmendmentCalculationServer) CalculateOrderAmendment(ctx context.Context, req *ordersv1.CalculateOrderAmendmentRequest) (*ordersv1.CalculateOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateOrderAmendment", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "calculate order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	var last4 string
	switch choice := req.GetOrderAmendment().GetSettlement().(type) {
	case *ordersv1.OrderAmendment_Card:
		last4 = choice.Card.GetLast4()
	case *ordersv1.OrderAmendment_Bank:
		last4 = choice.Bank.GetIban()
	case *ordersv1.OrderAmendment_VoucherCode:
		last4 = choice.VoucherCode
	}
	calculateOrderRequest := newCalculateOrderRequest(withCalculateOrderContext(req.GetContext()), withCalculateOrderOrder(&ordersv1.Order{
		Settlement: &ordersv1.Order_Card{Card: &ordersv1.OrderCardSettlement{Last4: last4}},
	}))
	calculateOrderReply, err := s.orderCalculationClient.CalculateOrder(ctx, calculateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/CalculateOrder")
	}
	for line := range iterx.Each(req.GetOrderAmendment().GetLines(), func(candidate *ordersv1.OrderAmendmentLine) bool { return candidate != nil }) {
		if _, err := s.watchlistCalculationClient.SimulateWatchlist(ctx, &fraudv1.SimulateWatchlistRequest{
			Context: req.GetContext(),
			Watchlist: &fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
						Attributes: line.GetAttributes(),
						Audit:      &commonv1.AuditInfo{Etag: req.GetOrderAmendment().GetAudit().GetEtag()},
						Manager: &identityv1.UserRef{
							Email: calculateOrderReply.GetOrder().GetCounterparty().GetEmail(),
						},
					}},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/SimulateWatchlist")
		}
	}
	explainOrderCalculationRequest := &ordersv1.ExplainOrderCalculationRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: req.GetOrderAmendment().GetBillingAddress().GetRecipient(),
				Locality:  calculateOrderReply.GetOrder().GetBillingAddress().GetLocality(),
			},
			Counterparty: &customersv1.CustomerRef{
				CustomerId: calculateOrderReply.GetOrder().GetCounterparty().GetCustomerId(),
			},
			Settlement: &ordersv1.Order_Card{Card: &ordersv1.OrderCardSettlement{Token: req.GetOrderAmendment().GetCard().GetToken()}},
		},
	}
	explainOrderCalculationReply, err := s.meteredOrderCalculation.ExplainOrderCalculation(ctx, explainOrderCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/ExplainOrderCalculation")
	}
	return &ordersv1.CalculateOrderAmendmentResponse{
		Breakdown: &ordersv1.CalculateOrderAmendmentResponse_Breakdown{Total: calculateOrderReply.GetBreakdown().GetTotal()},
		OrderAmendment: &ordersv1.OrderAmendment{
			Metadata:         explainOrderCalculationReply.GetOrder().GetMetadata(),
			OrderAmendmentId: req.GetOrderAmendmentId(),
			DueAt:            req.GetOrderAmendment().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateOrderAmendment estimates an order amendment.
func (s *OrderAmendmentCalculationServer) EstimateOrderAmendment(ctx context.Context, req *ordersv1.EstimateOrderAmendmentRequest) (*ordersv1.EstimateOrderAmendmentResponse, error) {
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "estimate order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	simulateOrderRequest := newSimulateOrderRequest(withSimulateOrderContext(req.GetContext()), withSimulateOrderOrder(&ordersv1.Order{
		Settlement: &ordersv1.Order_Bank{Bank: &ordersv1.OrderBankSettlement{
			AccountHolder: req.GetOrderAmendment().GetBank().GetAccountHolder(),
		}},
	}))
	simulateOrderReply, err := s.orderCalculationClient.SimulateOrder(ctx, simulateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/SimulateOrder")
	}
	return &ordersv1.EstimateOrderAmendmentResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit:            &commonv1.AuditInfo{Etag: simulateOrderReply.GetOrder().GetAudit().GetEtag()},
			IssuedAt:         simulateOrderReply.GetOrder().GetIssuedAt(),
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Number:           req.GetOrderAmendment().GetNumber(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteOrderAmendment quotes an order amendment.
func (s *OrderAmendmentCalculationServer) QuoteOrderAmendment(ctx context.Context, req *ordersv1.QuoteOrderAmendmentRequest) (*ordersv1.QuoteOrderAmendmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "quote order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	summarizeGiftCardsRequest := &giftcardsv1.SummarizeGiftCardsRequest{}
	summarizeGiftCardsRequest.Context = req.GetContext()
	summarizeGiftCardsRequest.Reason = tenancy.Value(ctx, "reason")
	summarizeGiftCardsRequest.Annotations = req.GetAnnotations()
	summarizeGiftCardsRequest.Details = &giftcardsv1.SummarizeGiftCardsRequest_Details{Filters: req.GetOrderAmendment().GetMetadata()}
	summarizeGiftCardsReply, err := s.giftCardQueryClient.SummarizeGiftCards(ctx, summarizeGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/SummarizeGiftCards")
	}
	return &ordersv1.QuoteOrderAmendmentResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeGiftCardsReply.GetResult().GetReferenceIds(),
			Status:       summarizeGiftCardsReply.GetResult().GetStatus(),
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Notes:            req.GetOrderAmendment().GetNotes(),
		},
	}, nil
}

// CompareOrderAmendmentOptions compares an order amendment.
func (s *OrderAmendmentCalculationServer) CompareOrderAmendmentOptions(ctx context.Context, req *ordersv1.CompareOrderAmendmentOptionsRequest) (*ordersv1.CompareOrderAmendmentOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "compare order amendment options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "postal_code", req.GetOrderAmendment().GetBillingAddress().GetPostalCode())
	calculateOrderRequest := &ordersv1.CalculateOrderRequest{}
	calculateOrderRequest.Context = req.GetContext()
	calculateOrderRequest.Details = &ordersv1.CalculateOrderRequest_Details{
		Destination: &commonv1.PostalAddress{PostalCode: tenancy.Value(ctx, "postal_code")},
	}
	calculateOrderReply, err := s.orderCalculationClient.CalculateOrder(ctx, calculateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/CalculateOrder")
	}
	source := req.GetOrderAmendment().GetCounterparty().ProtoReflect()
	backorderId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("backorder_id"))).String()
	listVehiclesRequest := &fleetv1.ListVehiclesRequest{
		Context: req.GetContext(),
		Filter: &fleetv1.ListVehiclesRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: calculateOrderReply.GetOrder().GetAudit().GetUpdatedAt(),
			OwnerId:      backorderId,
		},
	}
	listVehiclesReply, err := fleetv1.VehicleServiceClient.ListVehicles(s.vehicleClient, ctx, listVehiclesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fleet.v1.VehicleService/ListVehicles")
	}
	packed, err := anypb.New(req.GetOrderAmendment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizeGiftCardTransactionRequest := &giftcardsv1.NormalizeGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: calculateOrderReply.GetOrder().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	normalizeGiftCardTransactionReply, err := grpcx.NewClient(s.conn, giftcardsv1.NewGiftCardTransactionValidationServiceClient).NormalizeGiftCardTransaction(ctx, normalizeGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/NormalizeGiftCardTransaction")
	}
	searchYardsRequest := newSearchYardsRequest(withSearchYardsContext(req.GetContext()), withSearchYardsFilter(&yardsv1.SearchYardsRequest_Filter{
		Region: calculateOrderReply.GetOrder().GetBillingAddress().GetRegion(),
	}))
	searchYardsReply, err := s.yardClient.SearchYards(ctx, searchYardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.yards.v1.YardService/SearchYards")
	}
	return &ordersv1.CompareOrderAmendmentOptionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  calculateOrderReply.GetResult().GetReferenceIds(),
			CorrelationId: listVehiclesReply.GetResult().GetCorrelationId(),
			Status:        searchYardsReply.GetResult().GetStatus(),
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			Lines: []*ordersv1.OrderAmendmentLine{&ordersv1.OrderAmendmentLine{
				Attributes: normalizeGiftCardTransactionReply.GetGiftCardTransaction().GetAttributes(),
			}},
			Counterparty: &ordersv1.BackorderRef{
				DisplayName: normalizeGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			OrderAmendmentId: req.GetOrderAmendmentId(),
			BillingAddress:   req.GetOrderAmendment().GetBillingAddress(),
		},
	}, nil
}

// RecalculateOrderAmendments recalculates an order amendment.
func (s *OrderAmendmentCalculationServer) RecalculateOrderAmendments(ctx context.Context, req *ordersv1.RecalculateOrderAmendmentsRequest) (*ordersv1.RecalculateOrderAmendmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "recalculate order amendments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*searchv1.RepairSearchIndexResponse, len(req.GetOrderAmendment().GetLines()))
	for i, line := range req.GetOrderAmendment().GetLines() {
		group.Go(func() error {
			reply, err := s.searchIndexAdminClient.RepairSearchIndex(groupCtx, &searchv1.RepairSearchIndexRequest{
				Context: req.GetContext(),
				SearchIndex: &searchv1.SearchIndex{
					Components: map[string]*searchv1.SearchIndexComponent{"primary": &searchv1.SearchIndexComponent{Description: line.GetDescription()}},
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
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexAdminService/RepairSearchIndex")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	orderCalculation, err := registry.Lookup[ordersv1.OrderCalculationServiceClient](s.clients, "acme.orders.v1.OrderCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	simulateOrderRequest := &ordersv1.SimulateOrderRequest{
		Context: req.GetContext(),
		Details: &ordersv1.SimulateOrderRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
	}
	simulateOrderReply, err := orderCalculation.SimulateOrder(ctx, simulateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/SimulateOrder")
	}
	return &ordersv1.RecalculateOrderAmendmentsResponse{
		Result: &commonv1.OperationResult{
			Details: simulateOrderReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		OrderAmendment: &ordersv1.OrderAmendment{OrderAmendmentId: req.GetOrderAmendmentId()},
		Breakdown:      &ordersv1.RecalculateOrderAmendmentsResponse_Breakdown{Total: req.GetOrderAmendment().GetTotal()},
	}, nil
}

// SimulateOrderAmendment simulates an order amendment.
func (s *OrderAmendmentCalculationServer) SimulateOrderAmendment(ctx context.Context, req *ordersv1.SimulateOrderAmendmentRequest) (*ordersv1.SimulateOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SimulateOrderAmendment", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "simulate order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetOrderAmendment()).(*ordersv1.OrderAmendment)
	simulateOrderRequest := &ordersv1.SimulateOrderRequest{
		Context: req.GetContext(),
		Details: &ordersv1.SimulateOrderRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{UpdatedAt: cloned.GetAudit().GetUpdatedAt()},
		},
	}
	simulateOrderReply, err := s.meteredOrderCalculation.SimulateOrder(ctx, simulateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/SimulateOrder")
	}
	packed, err := anypb.New(req.GetOrderAmendment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreGiftCardTransactionRequest := newScoreGiftCardTransactionRequest(withScoreGiftCardTransactionContext(req.GetContext()), withScoreGiftCardTransactionDetails(&giftcardsv1.ScoreGiftCardTransactionRequest_Details{
		Destination: &commonv1.PostalAddress{
			PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			Locality:   req.GetDetails().GetDestination().GetLocality(),
		},
	}), withScoreGiftCardTransactionGiftCardTransaction(&giftcardsv1.GiftCardTransaction{Detail: packed}))
	scoreGiftCardTransactionReply, err := s.giftCardTransactionValidationClient.ScoreGiftCardTransaction(ctx, scoreGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/ScoreGiftCardTransaction")
	}
	return &ordersv1.SimulateOrderAmendmentResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			Metadata: simulateOrderReply.GetOrder().GetMetadata(),
			Lines: []*ordersv1.OrderAmendmentLine{&ordersv1.OrderAmendmentLine{
				Attributes: scoreGiftCardTransactionReply.GetGiftCardTransaction().GetAttributes(),
			}},
			OrderAmendmentId: req.GetOrderAmendmentId(),
		},
		Result: &commonv1.OperationResult{
			Details: scoreGiftCardTransactionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Breakdown: &ordersv1.SimulateOrderAmendmentResponse_Breakdown{Total: req.GetOrderAmendment().GetTotal()},
	}, nil
}

// ExplainOrderAmendmentCalculation explains an order amendment.
func (s *OrderAmendmentCalculationServer) ExplainOrderAmendmentCalculation(ctx context.Context, req *ordersv1.ExplainOrderAmendmentCalculationRequest) (*ordersv1.ExplainOrderAmendmentCalculationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "explain order amendment calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	browseStorageBinsReply, err := s.storageBinQueryClient.BrowseStorageBins(ctx, &warehousingv1.BrowseStorageBinsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.BrowseStorageBinsRequest_Filter{
			Region:       req.GetOrderAmendment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetOrderAmendment().GetDueAt(),
			OwnerId:      req.GetOrderAmendment().GetCounterparty().GetBackorderId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/BrowseStorageBins")
	}
	var last4 string
	switch choice := req.GetOrderAmendment().GetSettlement().(type) {
	case *ordersv1.OrderAmendment_Card:
		last4 = choice.Card.GetLast4()
	case *ordersv1.OrderAmendment_Bank:
		last4 = choice.Bank.GetIban()
	case *ordersv1.OrderAmendment_VoucherCode:
		last4 = choice.VoucherCode
	}
	checkGiftCardEligibilityRequest := &giftcardsv1.CheckGiftCardEligibilityRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetOrderAmendment().GetAudit().GetCreatedAt(),
			},
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{Last4: last4}},
		},
	}
	checkGiftCardEligibilityReply, err := s.meteredGiftCardValidation.CheckGiftCardEligibility(ctx, checkGiftCardEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/CheckGiftCardEligibility")
	}
	return &ordersv1.ExplainOrderAmendmentCalculationResponse{
		Result: &commonv1.OperationResult{
			Details: browseStorageBinsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &ordersv1.ExplainOrderAmendmentCalculationResponse_Outcome{
			Status: checkGiftCardEligibilityReply.GetOutcome().GetStatus(),
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Metadata:         req.GetOrderAmendment().GetMetadata(),
		},
	}, nil
}

// GetOrderAmendmentRates returns an order amendment.
func (s *OrderAmendmentCalculationServer) GetOrderAmendmentRates(ctx context.Context, req *ordersv1.GetOrderAmendmentRatesRequest) (*ordersv1.GetOrderAmendmentRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetOrderAmendmentRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "get_order_amendment_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order amendment rates", "tenant", req.GetContext().GetTenantId())
	normalizePutawayTaskRequest := &warehousingv1.NormalizePutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Assignee:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Subject:   req.GetOrderAmendmentId(),
		},
		Details: &warehousingv1.NormalizePutawayTaskRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	normalizePutawayTaskReply, err := s.normalizePutawayTaskFn(ctx, normalizePutawayTaskRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/NormalizePutawayTask")
	}
	return &ordersv1.GetOrderAmendmentRatesResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			DueAt:            normalizePutawayTaskReply.GetPutawayTask().GetSla().GetDueAt(),
			OrderAmendmentId: req.GetOrderAmendmentId(),
		},
		Result: &commonv1.OperationResult{
			Details: normalizePutawayTaskReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}
