package payments

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	personalizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/personalization/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PaymentInstrumentSyncServer implements `acme.payments.v1.PaymentInstrumentSyncService`.
// Keeps payment instruments in step with the systems of record in other regions.
type PaymentInstrumentSyncServer struct {
	paymentsv1.UnsafePaymentInstrumentSyncServiceServer
	store                           *Store
	log                             *slog.Logger
	refundValidationClient          paymentsv1.RefundValidationServiceClient
	paymentClient                   paymentsv1.PaymentServiceClient
	paymentClientFallback           paymentsv1.PaymentServiceClient
	bus                             *eventbus.Bus
	conn                            *grpc.ClientConn
	shipmentWorkflowClient          shippingv1.ShipmentWorkflowServiceClient
	sponsorQueryPort                sponsorQueryPort
	stockItemApprovalClient         inventoryv1.StockItemApprovalServiceClient
	paymentCalculationClient        paymentsv1.PaymentCalculationServiceClient
	disputeCalculationClient        paymentsv1.DisputeCalculationServiceClient
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	householdValidationClient       customersv1.HouseholdValidationServiceClient
	searchQueryHistoryClient        searchv1.SearchQueryHistoryServiceClient
}

// NewPaymentInstrumentSyncServer wires a PaymentInstrumentSyncServer.
func NewPaymentInstrumentSyncServer(deps platform.Deps) *PaymentInstrumentSyncServer {
	return &PaymentInstrumentSyncServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.payments.v1.PaymentInstrumentSyncService"),
		refundValidationClient:          paymentsv1.NewRefundValidationServiceClient(deps.Conn),
		paymentClient:                   paymentsv1.NewPaymentServiceClient(deps.Conn),
		paymentClientFallback:           paymentsv1.NewPaymentServiceClient(deps.Conn),
		bus:                             deps.Bus,
		conn:                            deps.Conn,
		shipmentWorkflowClient:          shippingv1.NewShipmentWorkflowServiceClient(deps.Conn),
		sponsorQueryPort:                newSponsorQueryPort(deps),
		stockItemApprovalClient:         inventoryv1.NewStockItemApprovalServiceClient(deps.Conn),
		paymentCalculationClient:        paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
		disputeCalculationClient:        paymentsv1.NewDisputeCalculationServiceClient(deps.Conn),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		householdValidationClient:       customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		searchQueryHistoryClient:        searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
	}
}

// SyncPaymentInstruments syncs a payment instrument.
func (s *PaymentInstrumentSyncServer) SyncPaymentInstruments(ctx context.Context, req *paymentsv1.SyncPaymentInstrumentsRequest) (*paymentsv1.SyncPaymentInstrumentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "sync payment instruments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	scoreRefundRequest := &paymentsv1.ScoreRefundRequest{}
	scoreRefundRequest.Context = req.GetContext()
	scoreRefundRequest.Refund = &paymentsv1.Refund{
		Counterparty: &customersv1.CustomerRef{
			Email:       req.GetPaymentInstrument().GetContact().GetEmail(),
			DisplayName: req.GetPaymentInstrument().GetHolder().GetDisplayName(),
		},
		Settlement: &paymentsv1.Refund_Bank{Bank: &paymentsv1.RefundBankSettlement{
			AccountHolder: req.GetPaymentInstrument().GetBank().GetAccountHolder(),
		}},
		Audit: &commonv1.AuditInfo{
			CreatedAt: req.GetPaymentInstrument().GetAudit().GetCreatedAt(),
		},
	}
	scoreRefundReply, err := s.refundValidationClient.ScoreRefund(ctx, scoreRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/ScoreRefund")
	}
	routes := map[string]func(context.Context, *paymentsv1.UpdatePaymentRequest, ...grpc.CallOption) (*paymentsv1.UpdatePaymentResponse, error){
		"primary":  s.paymentClient.UpdatePayment,
		"fallback": s.paymentClientFallback.UpdatePayment,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	var token string
	switch choice := req.GetPaymentInstrument().GetFunding().(type) {
	case *paymentsv1.PaymentInstrument_Card:
		token = choice.Card.GetToken()
	case *paymentsv1.PaymentInstrument_Bank:
		token = choice.Bank.GetIban()
	case *paymentsv1.PaymentInstrument_ExternalAccountId:
		token = choice.ExternalAccountId
	}
	updatePaymentRequest := &paymentsv1.UpdatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{Token: token}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetPaymentInstrument().GetAudit().GetUpdatedAt(),
			},
		},
	}
	updatePaymentReply, err := route(ctx, updatePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentService/UpdatePayment")
	}
	return &paymentsv1.SyncPaymentInstrumentsResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder: &customersv1.CustomerRef{
				CustomerId:  scoreRefundReply.GetRefund().GetCounterparty().GetCustomerId(),
				DisplayName: updatePaymentReply.GetPayment().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: updatePaymentReply.GetPayment().GetAudit().GetCreatedBy(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Available:           req.GetPaymentInstrument().GetAvailable(),
		},
		Outcome: &paymentsv1.SyncPaymentInstrumentsResponse_Outcome{Status: scoreRefundReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PushPaymentInstrument pushs a payment instrument.
func (s *PaymentInstrumentSyncServer) PushPaymentInstrument(ctx context.Context, req *paymentsv1.PushPaymentInstrumentRequest) (*paymentsv1.PushPaymentInstrumentResponse, error) {
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "push payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "payments.push_payment_instrument.approve_stock_movement",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"stock_movement.subject.customer_id": req.GetPaymentInstrument().GetHolder().GetCustomerId(),
			"details.approver.display_name":      req.GetPaymentInstrument().GetHolder().GetDisplayName(),
			"context.tenant_id":                  req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetPaymentInstrument().GetContact().GetPreferredChannel())
	packed, err := anypb.New(req.GetPaymentInstrument())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	unsubscribeSearchIndexUpdatesRequest := &searchv1.UnsubscribeSearchIndexUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSearchIndexUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            req.GetPaymentInstrument().GetContact().GetPhone(),
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
			},
			Payload: packed,
		},
	}
	unsubscribeSearchIndexUpdatesReply := new(searchv1.UnsubscribeSearchIndexUpdatesResponse)
	if err := grpcx.Invoke(ctx, s.conn, searchv1.SearchIndexNotificationService_UnsubscribeSearchIndexUpdates_FullMethodName, unsubscribeSearchIndexUpdatesRequest, unsubscribeSearchIndexUpdatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/UnsubscribeSearchIndexUpdates")
	}
	jobs := make(chan *paymentsv1.PaymentInstrumentLimit)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for limit := range jobs {
				if _, err := s.shipmentWorkflowClient.AdvanceShipmentWorkflow(workCtx, &shippingv1.AdvanceShipmentWorkflowRequest{
					Context: req.GetContext(),
					Shipment: &shippingv1.Shipment{
						Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{Amount: limit.GetAmount()}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPaymentInstrument().GetLimits() {
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
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentWorkflowService/AdvanceShipmentWorkflow")
	}
	return &paymentsv1.PushPaymentInstrumentResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: unsubscribeSearchIndexUpdatesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Contact:             req.GetPaymentInstrument().GetContact(),
		},
	}, nil
}

// PullPaymentInstruments pulls a payment instrument.
func (s *PaymentInstrumentSyncServer) PullPaymentInstruments(ctx context.Context, req *paymentsv1.PullPaymentInstrumentsRequest) (*paymentsv1.PullPaymentInstrumentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "pull payment instruments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeSponsorsRequest := &sponsorshipsv1.SummarizeSponsorsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
	}
	summarizeSponsorsReply, err := s.sponsorQueryPort.SummarizeSponsors(ctx, summarizeSponsorsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sponsorships.v1.SponsorQueryService/SummarizeSponsors")
	}
	return &paymentsv1.PullPaymentInstrumentsResponse{
		Result: &commonv1.OperationResult{
			Status:        summarizeSponsorsReply.GetResult().GetStatus(),
			CorrelationId: summarizeSponsorsReply.GetResult().GetCorrelationId(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			OpenedAt:            req.GetPaymentInstrument().GetOpenedAt(),
		},
	}, nil
}

// ReconcilePaymentInstruments reconciles a payment instrument.
func (s *PaymentInstrumentSyncServer) ReconcilePaymentInstruments(ctx context.Context, req *paymentsv1.ReconcilePaymentInstrumentsRequest) (*paymentsv1.ReconcilePaymentInstrumentsResponse, error) {
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "reconcile payment instruments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	commentStockItemApprovalReply, err := s.stockItemApprovalClient.CommentStockItemApproval(ctx, &inventoryv1.CommentStockItemApprovalRequest{
		Context: req.GetContext(),
		StockItem: &inventoryv1.StockItem{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetPaymentInstrument().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{Email: req.GetPaymentInstrument().GetContact().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemApprovalService/CommentStockItemApproval")
	}
	for _, limit := range req.GetPaymentInstrument().GetLimits() {
		if _, err := s.paymentCalculationClient.RecalculatePayments(ctx, &paymentsv1.RecalculatePaymentsRequest{
			Context: req.GetContext(),
			Payment: &paymentsv1.Payment{
				Lines: []*paymentsv1.PaymentLine{&paymentsv1.PaymentLine{Tax: &paymentsv1.PaymentLineTax{Amount: limit.GetAmount()}}},
				BillingAddress: &commonv1.PostalAddress{
					CountryCode: commentStockItemApprovalReply.GetStockItem().GetAddress().GetCountryCode(),
					Region:      commentStockItemApprovalReply.GetStockItem().GetAddress().GetRegion(),
				},
			},
			Details: &paymentsv1.RecalculatePaymentsRequest_Details{
				Destination: &commonv1.PostalAddress{
					PostalCode: commentStockItemApprovalReply.GetStockItem().GetAddress().GetPostalCode(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/RecalculatePayments")
		}
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*shippingv1.PauseShipmentWorkflowResponse, len(req.GetPaymentInstrument().GetLimits()))
	for i, limit2 := range req.GetPaymentInstrument().GetLimits() {
		group.Go(func() error {
			reply, err := s.shipmentWorkflowClient.PauseShipmentWorkflow(groupCtx, &shippingv1.PauseShipmentWorkflowRequest{
				Context: req.GetContext(),
				Shipment: &shippingv1.Shipment{
					Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{Amount: limit2.GetAmount()}},
					Audit: &commonv1.AuditInfo{
						CreatedBy: commentStockItemApprovalReply.GetStockItem().GetAudit().GetCreatedBy(),
					},
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
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentWorkflowService/PauseShipmentWorkflow")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &paymentsv1.ReconcilePaymentInstrumentsResponse{
		Result: &commonv1.OperationResult{
			Status: commentStockItemApprovalReply.GetOutcome().GetStatus(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Holder:              req.GetPaymentInstrument().GetHolder(),
		},
	}, nil
}

// StreamPaymentInstrumentChanges exchanges changes of payment instruments with a peer.
func (s *PaymentInstrumentSyncServer) StreamPaymentInstrumentChanges(stream grpc.BidiStreamingServer[paymentsv1.StreamPaymentInstrumentChangesRequest, paymentsv1.StreamPaymentInstrumentChangesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *paymentsv1.StreamPaymentInstrumentChangesRequest)
	failed := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			received, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					failed <- err
				}
				return
			}
			select {
			case incoming <- received:
			case <-ctx.Done():
				return
			}
		}
	}()
	for msg := range incoming {
		s.log.InfoContext(ctx, "stream payment instrument changes", "tenant", msg.GetContext().GetTenantId())
		if _, err := s.disputeCalculationClient.ExplainDisputeCalculation(ctx, &paymentsv1.ExplainDisputeCalculationRequest{
			Context: msg.GetContext(),
			Dispute: &paymentsv1.Dispute{
				Requester: &commonv1.ContactInfo{
					Phone: msg.GetChange().GetContact().GetPhone(),
					Name:  msg.GetChange().GetContact().GetName(),
				},
				Audit: &commonv1.AuditInfo{UpdatedAt: msg.GetChange().GetAudit().GetUpdatedAt()},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.payments.v1.DisputeCalculationService/ExplainDisputeCalculation")
		}
		if _, err := s.customerSegmentValidationClient.ExplainCustomerSegmentDecision(ctx, &customersv1.ExplainCustomerSegmentDecisionRequest{
			Context: msg.GetContext(),
			Details: &customersv1.ExplainCustomerSegmentDecisionRequest_Details{
				Contact: &commonv1.ContactInfo{Email: msg.GetChange().GetContact().GetEmail()},
			},
			CustomerSegment: &customersv1.CustomerSegment{
				Audit: &commonv1.AuditInfo{UpdatedBy: msg.GetChange().GetAudit().GetUpdatedBy()},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ExplainCustomerSegmentDecision")
		}
		if err := stream.Send(&paymentsv1.StreamPaymentInstrumentChangesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
			return err
		}
	}
	select {
	case err := <-failed:
		return err
	default:
		return nil
	}
}

// ResolvePaymentInstrumentConflict resolves a payment instrument.
func (s *PaymentInstrumentSyncServer) ResolvePaymentInstrumentConflict(ctx context.Context, req *paymentsv1.ResolvePaymentInstrumentConflictRequest) (*paymentsv1.ResolvePaymentInstrumentConflictResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "resolve payment instrument conflict", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	jobs := make(chan *paymentsv1.PaymentInstrumentLimit)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for limit := range jobs {
				if _, err := s.householdValidationClient.PreviewHousehold(workCtx, &customersv1.PreviewHouseholdRequest{
					Context: req.GetContext(),
					Household: &customersv1.Household{
						Children: []*customersv1.Household{&customersv1.Household{
							Children: []*customersv1.Household{&customersv1.Household{
								Children: []*customersv1.Household{&customersv1.Household{Name: limit.GetName()}},
								Audit: &commonv1.AuditInfo{
									UpdatedBy: req.GetPaymentInstrument().GetAudit().GetUpdatedBy(),
									CreatedBy: req.GetPaymentInstrument().GetAudit().GetCreatedBy(),
								},
								Manager: &identityv1.UserRef{
									DisplayName: req.GetPaymentInstrument().GetHolder().GetDisplayName(),
								},
							}},
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
	for _, queued := range req.GetPaymentInstrument().GetLimits() {
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/PreviewHousehold")
	}
	return &paymentsv1.ResolvePaymentInstrumentConflictResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Balance:             req.GetPaymentInstrument().GetBalance(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPaymentInstrumentSyncState returns a payment instrument.
func (s *PaymentInstrumentSyncServer) GetPaymentInstrumentSyncState(ctx context.Context, req *paymentsv1.GetPaymentInstrumentSyncStateRequest) (*paymentsv1.GetPaymentInstrumentSyncStateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "get_payment_instrument_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payment instrument sync state", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	explainProfileCalculationRequest := &personalizationv1.ExplainProfileCalculationRequest{
		Context: req.GetContext(),
		Profile: &personalizationv1.Profile{
			RelationshipManager: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: userId,
			},
		},
		Details: &personalizationv1.ExplainProfileCalculationRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	explainProfileCalculationReply := new(personalizationv1.ExplainProfileCalculationResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", personalizationProfileCalculationServiceName, "ExplainProfileCalculation"), explainProfileCalculationRequest, explainProfileCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.personalization.v1.ProfileCalculationService/ExplainProfileCalculation")
	}
	return &paymentsv1.GetPaymentInstrumentSyncStateResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainProfileCalculationReply.GetProfile().GetAudit().GetCreatedBy(),
			},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: explainProfileCalculationReply.GetProfile().GetContact().GetPreferredChannel(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPaymentInstrumentConflicts lists payment instruments a page at a time.
func (s *PaymentInstrumentSyncServer) ListPaymentInstrumentConflicts(ctx context.Context, req *paymentsv1.ListPaymentInstrumentConflictsRequest) (*paymentsv1.ListPaymentInstrumentConflictsResponse, error) {
	s.log.InfoContext(ctx, "list payment instrument conflicts", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	recordSearchQueryVersionRequest := &searchv1.RecordSearchQueryVersionRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Measurements: []*searchv1.SearchQueryMeasurement{&searchv1.SearchQueryMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Detail:       packed,
		},
		Details: &searchv1.RecordSearchQueryVersionRequest_Details{
			AuthorEmail: req.GetContext().GetActor().GetEmail(),
			Snapshot:    snapshot,
		},
	}
	recordSearchQueryVersionReply, err := grpcx.Call(ctx, s.searchQueryHistoryClient.RecordSearchQueryVersion, recordSearchQueryVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/RecordSearchQueryVersion")
	}
	listLedgerAccountsRequest := &ledgerv1.ListLedgerAccountsRequest{
		Context: req.GetContext(),
		Page: &commonv1.PageRequest{
			PageSize:  req.GetPage().GetPageSize(),
			PageToken: req.GetPage().GetPageToken(),
		},
		Filter: &ledgerv1.ListLedgerAccountsRequest_Filter{OwnerId: req.GetFilter().GetOwnerId()},
	}
	listLedgerAccountsReply, err := grpcx.NewClient(s.conn, ledgerv1.NewLedgerAccountServiceClient).ListLedgerAccounts(ctx, listLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/ListLedgerAccounts")
	}
	return &paymentsv1.ListPaymentInstrumentConflictsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recordSearchQueryVersionReply.GetSearchQuery().GetCorrelationId(),
			Details:       recordSearchQueryVersionReply.GetResult().GetDetails(),
			Message:       listLedgerAccountsReply.GetResult().GetMessage(),
			ReferenceIds:  listLedgerAccountsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
	}, nil
}
