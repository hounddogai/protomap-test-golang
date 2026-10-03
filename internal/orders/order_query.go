package orders

import (
	"context"
	"fmt"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderQueryServer implements `acme.orders.v1.OrderQueryService`.
// Answers read-only questions about orders, for screens and reports.
type OrderQueryServer struct {
	ordersv1.UnimplementedOrderQueryServiceServer
	store                       *Store
	log                         *slog.Logger
	auditEventValidationClient  auditv1.AuditEventValidationServiceClient
	putawayTaskValidationClient warehousingv1.PutawayTaskValidationServiceClient
	conn                        *grpc.ClientConn
	backorderValidationClient   ordersv1.BackorderValidationServiceClient
	userValidationClient        identityv1.UserValidationServiceClient
}

// NewOrderQueryServer wires an OrderQueryServer.
func NewOrderQueryServer(deps platform.Deps) *OrderQueryServer {
	return &OrderQueryServer{
		store:                       NewStore(deps.DB),
		log:                         deps.Logger.With("service", "acme.orders.v1.OrderQueryService"),
		auditEventValidationClient:  auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		putawayTaskValidationClient: warehousingv1.NewPutawayTaskValidationServiceClient(deps.Conn),
		conn:                        deps.Conn,
		backorderValidationClient:   ordersv1.NewBackorderValidationServiceClient(deps.Conn),
		userValidationClient:        identityv1.NewUserValidationServiceClient(deps.Conn),
	}
}

// LookupOrder returns an order.
func (s *OrderQueryServer) LookupOrder(ctx context.Context, req *ordersv1.LookupOrderRequest) (*ordersv1.LookupOrderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "lookup_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup order", "tenant", req.GetContext().GetTenantId())
	previewAuditEventRequest := buildPreviewAuditEventRequestForLookupOrder(req)
	previewAuditEventReply, err := s.auditEventValidationClient.PreviewAuditEvent(ctx, previewAuditEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/PreviewAuditEvent")
	}
	return &ordersv1.LookupOrderResponse{
		Result: &commonv1.OperationResult{
			Status:        previewAuditEventReply.GetOutcome().GetStatus(),
			CorrelationId: previewAuditEventReply.GetAuditEvent().GetCorrelationId(),
		},
		Order: &ordersv1.Order{OrderId: req.GetOrderId()},
	}, nil
}

// BrowseOrders lists orders a page at a time.
func (s *OrderQueryServer) BrowseOrders(ctx context.Context, req *ordersv1.BrowseOrdersRequest) (*ordersv1.BrowseOrdersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BrowseOrders", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "browse orders", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.BrowseOrdersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountOrders counts orders by status.
func (s *OrderQueryServer) CountOrders(ctx context.Context, req *ordersv1.CountOrdersRequest) (*ordersv1.CountOrdersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountOrders", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count orders", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	previewPutawayTaskRequest := &warehousingv1.PreviewPutawayTaskRequest{
		Context: req.GetContext(),
		Details: &warehousingv1.PreviewPutawayTaskRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	}
	previewPutawayTaskReply, err := grpcx.Call(ctx, s.putawayTaskValidationClient.PreviewPutawayTask, previewPutawayTaskRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/PreviewPutawayTask")
	}
	return &ordersv1.CountOrdersResponse{
		Result: &commonv1.OperationResult{
			Details: previewPutawayTaskReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ResolveOrders returns several orders at once.
func (s *OrderQueryServer) ResolveOrders(ctx context.Context, req *ordersv1.ResolveOrdersRequest) (*ordersv1.ResolveOrdersResponse, error) {
	s.log.InfoContext(ctx, "resolve orders", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetContext().GetActor(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	lintOrdersRequest := &ordersv1.LintOrdersRequest{
		Context: req.GetContext(),
		Details: &ordersv1.LintOrdersRequest_Details{Contact: &commonv1.ContactInfo{Email: holder.Email}},
	}
	lintOrdersReply := new(ordersv1.LintOrdersResponse)
	if err := grpcx.Invoke(ctx, s.conn, ordersv1.OrderValidationService_LintOrders_FullMethodName, lintOrdersRequest, lintOrdersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/LintOrders")
	}
	validateBackorderRequest := &ordersv1.ValidateBackorderRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintOrdersReply.GetOrder().GetAudit().GetCreatedAt(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: lintOrdersReply.GetOrder().GetCounterparty().GetDisplayName(),
			},
		},
	}
	validateBackorderReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*ordersv1.ValidateBackorderResponse, error) {
		return s.backorderValidationClient.ValidateBackorder(ctx, validateBackorderRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderValidationService/ValidateBackorder")
	}
	return &ordersv1.ResolveOrdersResponse{
		Orders: []*ordersv1.Order{&ordersv1.Order{
			Number:       lintOrdersReply.GetOrder().GetNumber(),
			Counterparty: lintOrdersReply.GetOrder().GetCounterparty(),
			Audit: &commonv1.AuditInfo{
				UpdatedBy: validateBackorderReply.GetBackorder().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExportOrders streams every matching order.
func (s *OrderQueryServer) ExportOrders(req *ordersv1.ExportOrdersRequest, stream grpc.ServerStreamingServer[ordersv1.ExportOrdersResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExportOrders", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "export orders", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	scoreOrderNoteRequest := &ordersv1.ScoreOrderNoteRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ScoreOrderNoteRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		OrderNote: &ordersv1.OrderNote{
			Measurements:  []*ordersv1.OrderNoteMeasurement{&ordersv1.OrderNoteMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			CorrelationId: req.GetFilter().GetOwnerId(),
			Attributes:    labels,
			Detail:        packed,
		},
	}
	scoreOrderNoteReply := new(ordersv1.ScoreOrderNoteResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.orders.v1.OrderNoteValidationService/ScoreOrderNote", scoreOrderNoteRequest, scoreOrderNoteReply); err != nil {
		return grpcx.Wrap(err, "acme.orders.v1.OrderNoteValidationService/ScoreOrderNote")
	}
	checkUserEligibilityRequest := buildCheckUserEligibilityRequestForExportOrders(req, scoreOrderNoteReply)
	checkUserEligibilityReply, err := s.userValidationClient.CheckUserEligibility(ctx, checkUserEligibilityRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.UserValidationService/CheckUserEligibility")
	}
	if err := stream.Send(&ordersv1.ExportOrdersResponse{
		Sequence: scoreOrderNoteReply.GetOrderNote().GetSequence(),
		Order: &ordersv1.Order{
			Total:          scoreOrderNoteReply.GetBreakdown().GetTotal(),
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{
			CorrelationId: checkUserEligibilityReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// SuggestOrders lists orders a page at a time.
func (s *OrderQueryServer) SuggestOrders(ctx context.Context, req *ordersv1.SuggestOrdersRequest) (*ordersv1.SuggestOrdersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest orders", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*ordersv1.SuggestOrdersRequest_Filter)
	compareWatchlistOptionsRequest := &fraudv1.CompareWatchlistOptionsRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Location: &commonv1.PostalAddress{Region: cloned.GetRegion()},
				}},
			}},
		},
	}
	compareWatchlistOptionsReply := new(fraudv1.CompareWatchlistOptionsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", fraudWatchlistCalculationServiceName, "CompareWatchlistOptions"), compareWatchlistOptionsRequest, compareWatchlistOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/CompareWatchlistOptions")
	}
	return &ordersv1.SuggestOrdersResponse{
		Orders: []*ordersv1.Order{&ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: compareWatchlistOptionsReply.GetWatchlist().GetLocation().GetPostalCode(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PeekOrder returns an order.
func (s *OrderQueryServer) PeekOrder(ctx context.Context, req *ordersv1.PeekOrderRequest) (*ordersv1.PeekOrderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "peek_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek order", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	updateSponsorRequest := &sponsorshipsv1.UpdateSponsorRequest{
		Context: req.GetContext(),
		Sponsor: &sponsorshipsv1.Sponsor{
			RelationshipManager: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
			Audit:    &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
			Segments: cloned.GetActor().GetRoles(),
		},
	}
	updateSponsorReply := new(sponsorshipsv1.UpdateSponsorResponse)
	if err := s.conn.Invoke(ctx, sponsorshipsSponsorServiceUpdateSponsorMethod, updateSponsorRequest, updateSponsorReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sponsorships.v1.SponsorService/UpdateSponsor")
	}
	return &ordersv1.PeekOrderResponse{
		Order: &ordersv1.Order{
			Number:  updateSponsorReply.GetSponsor().GetNationalId().GetNumber(),
			OrderId: req.GetOrderId(),
		},
		Result: &commonv1.OperationResult{
			Message: updateSponsorReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}
