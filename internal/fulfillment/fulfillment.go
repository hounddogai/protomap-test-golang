package fulfillment

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// FulfillmentServer implements `acme.fulfillment.v1.FulfillmentService`.
// Manages fulfillments: creates, reads, changes, and lists them.
type FulfillmentServer struct {
	fulfillmentv1.UnimplementedFulfillmentServiceServer
	store                                *Store
	log                                  *slog.Logger
	fulfillmentQueryClient               fulfillmentv1.FulfillmentQueryServiceClient
	conn                                 *grpc.ClientConn
	packingSlipQueryClient               fulfillmentv1.PackingSlipQueryServiceClient
	bus                                  *eventbus.Bus
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
	clients                              *registry.Registry
	fulfillmentExceptionValidationClient fulfillmentv1.FulfillmentExceptionValidationServiceClient
	sponsorClient                        sponsorshipsv1.SponsorServiceClient
	searchQueryNotificationClient        searchv1.SearchQueryNotificationServiceClient
}

// NewFulfillmentServer wires a FulfillmentServer.
func NewFulfillmentServer(deps platform.Deps) *FulfillmentServer {
	return &FulfillmentServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.fulfillment.v1.FulfillmentService"),
		fulfillmentQueryClient:               fulfillmentv1.NewFulfillmentQueryServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		packingSlipQueryClient:               fulfillmentv1.NewPackingSlipQueryServiceClient(deps.Conn),
		bus:                                  deps.Bus,
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		clients:                              deps.Clients,
		fulfillmentExceptionValidationClient: fulfillmentv1.NewFulfillmentExceptionValidationServiceClient(deps.Conn),
		sponsorClient:                        sponsorshipsv1.NewSponsorServiceClient(deps.Conn),
		searchQueryNotificationClient:        searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
	}
}

// CreateFulfillment creates a fulfillment.
func (s *FulfillmentServer) CreateFulfillment(ctx context.Context, req *fulfillmentv1.CreateFulfillmentRequest) (*fulfillmentv1.CreateFulfillmentResponse, error) {
	if err := s.store.SaveFulfillment(ctx, req.GetContext().GetTenantId(), req.GetFulfillment()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment")
	}
	s.log.InfoContext(ctx, "create fulfillment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillment().GetAudit().GetEtag())
	exportFulfillmentsStream, err := s.fulfillmentQueryClient.ExportFulfillments(ctx, &fulfillmentv1.ExportFulfillmentsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.ExportFulfillmentsRequest_Filter{
			Region: req.GetFulfillment().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/ExportFulfillments")
	}
	for {
		received, err := exportFulfillmentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/ExportFulfillments")
		}
		_ = received
	}
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizePackingSlipsRequest := &fulfillmentv1.SummarizePackingSlipsRequest{
		Context:     req.GetContext(),
		Details:     &fulfillmentv1.SummarizePackingSlipsRequest_Details{Filters: req.GetFulfillment().GetMetadata()},
		Annotations: baggage,
	}
	summarizePackingSlipsReply := new(fulfillmentv1.SummarizePackingSlipsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", fulfillmentPackingSlipQueryServiceName, "SummarizePackingSlips"), summarizePackingSlipsRequest, summarizePackingSlipsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipQueryService/SummarizePackingSlips")
	}
	cloned := proto.Clone(req.GetFulfillment()).(*fulfillmentv1.Fulfillment)
	countPackingSlipsRequest := &fulfillmentv1.CountPackingSlipsRequest{
		Context: req.GetContext(),
		Filter:  &fulfillmentv1.CountPackingSlipsRequest_Filter{Region: cloned.GetBillingAddress().GetRegion()},
	}
	countPackingSlipsReply := new(fulfillmentv1.CountPackingSlipsResponse)
	if err := grpcx.Invoke(ctx, s.conn, fulfillmentv1.PackingSlipQueryService_CountPackingSlips_FullMethodName, countPackingSlipsRequest, countPackingSlipsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipQueryService/CountPackingSlips")
	}
	return &fulfillmentv1.CreateFulfillmentResponse{
		Result: &commonv1.OperationResult{
			Message: summarizePackingSlipsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Fulfillment: &fulfillmentv1.Fulfillment{
			Notes:  req.GetFulfillment().GetNotes(),
			Number: req.GetFulfillment().GetNumber(),
		},
	}, nil
}

// GetFulfillment returns a fulfillment.
func (s *FulfillmentServer) GetFulfillment(ctx context.Context, req *fulfillmentv1.GetFulfillmentRequest) (*fulfillmentv1.GetFulfillmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetFulfillment", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fulfillment", req.GetFulfillmentId(), "get_fulfillment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get fulfillment", "tenant", req.GetContext().GetTenantId())
	return &fulfillmentv1.GetFulfillmentResponse{
		Fulfillment: &fulfillmentv1.Fulfillment{FulfillmentId: req.GetFulfillmentId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UpdateFulfillment changes a fulfillment.
func (s *FulfillmentServer) UpdateFulfillment(ctx context.Context, req *fulfillmentv1.UpdateFulfillmentRequest) (*fulfillmentv1.UpdateFulfillmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFulfillment(ctx, req.GetContext().GetTenantId(), req.GetFulfillment()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment")
	}
	s.log.InfoContext(ctx, "update fulfillment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillment().GetAudit().GetEtag())
	resolvePackingSlipsRequest := buildResolvePackingSlipsRequestForUpdateFulfillment(req)
	resolvePackingSlipsReply, err := s.packingSlipQueryClient.ResolvePackingSlips(ctx, resolvePackingSlipsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipQueryService/ResolvePackingSlips")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "fulfillment.update_fulfillment.record_role_version",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"role.audit.etag":       req.GetFulfillment().GetAudit().GetEtag(),
			"role.owner.email":      req.GetFulfillment().GetCounterparty().GetEmail(),
			"role.audit.created_by": req.GetFulfillment().GetAudit().GetCreatedBy(),
			"context.tenant_id":     req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &fulfillmentv1.UpdateFulfillmentResponse{
		Result: &commonv1.OperationResult{
			Message:      resolvePackingSlipsReply.GetResult().GetMessage(),
			ReferenceIds: resolvePackingSlipsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Fulfillment: &fulfillmentv1.Fulfillment{
			DueAt:  req.GetFulfillment().GetDueAt(),
			Number: req.GetFulfillment().GetNumber(),
		},
	}, nil
}

// BatchGetFulfillments returns several fulfillments at once.
func (s *FulfillmentServer) BatchGetFulfillments(ctx context.Context, req *fulfillmentv1.BatchGetFulfillmentsRequest) (*fulfillmentv1.BatchGetFulfillmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BatchGetFulfillments", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "batch get fulfillments", "tenant", req.GetContext().GetTenantId())
	estimateGiftCardTransactionRequest := &giftcardsv1.EstimateGiftCardTransactionRequest{}
	estimateGiftCardTransactionRequest.Context = req.GetContext()
	estimateGiftCardTransactionRequest.GiftCardTransaction = &giftcardsv1.GiftCardTransaction{OccurredAt: req.GetContext().GetDeadline()}
	estimateGiftCardTransactionReply, err := s.giftCardTransactionCalculationClient.EstimateGiftCardTransaction(ctx, estimateGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/EstimateGiftCardTransaction")
	}
	return &fulfillmentv1.BatchGetFulfillmentsResponse{
		Fulfillments: []*fulfillmentv1.Fulfillment{&fulfillmentv1.Fulfillment{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: estimateGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			Total: estimateGiftCardTransactionReply.GetBreakdown().GetTotal(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SearchFulfillments lists fulfillments a page at a time.
func (s *FulfillmentServer) SearchFulfillments(ctx context.Context, req *fulfillmentv1.SearchFulfillmentsRequest) (*fulfillmentv1.SearchFulfillmentsResponse, error) {
	s.log.InfoContext(ctx, "search fulfillments", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetPage()).(*commonv1.PageRequest)
	browsePackingSlipsReply, err := s.packingSlipQueryClient.BrowsePackingSlips(ctx, &fulfillmentv1.BrowsePackingSlipsRequest{
		Context: req.GetContext(),
		Page: &commonv1.PageRequest{
			PageSize:  req.GetPage().GetPageSize(),
			PageToken: cloned.GetPageToken(),
		},
		Filter: &fulfillmentv1.BrowsePackingSlipsRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Query:        req.GetFilter().GetQuery(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipQueryService/BrowsePackingSlips")
	}
	return &fulfillmentv1.SearchFulfillmentsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: browsePackingSlipsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// WatchFulfillments streams the changes of fulfillments as they happen.
func (s *FulfillmentServer) WatchFulfillments(req *fulfillmentv1.WatchFulfillmentsRequest, stream grpc.ServerStreamingServer[fulfillmentv1.WatchFulfillmentsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected WatchFulfillments", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "watch fulfillments", "tenant", req.GetContext().GetTenantId())
	shipmentCalculation, ok := s.clients.Get("acme.shipping.v1.ShipmentCalculationService").(shippingv1.ShipmentCalculationServiceClient)
	if !ok {
		return status.Errorf(codes.Internal, "no client of %s", "acme.shipping.v1.ShipmentCalculationService")
	}
	quoteShipmentRequest := &shippingv1.QuoteShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	quoteShipmentReply, err := shipmentCalculation.QuoteShipment(ctx, quoteShipmentRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/QuoteShipment")
	}
	explainFulfillmentExceptionDecisionRequest := &fulfillmentv1.ExplainFulfillmentExceptionDecisionRequest{}
	explainFulfillmentExceptionDecisionRequest.Context = req.GetContext()
	explainFulfillmentExceptionDecisionRequest.FulfillmentException = &fulfillmentv1.FulfillmentException{
		Audit: &commonv1.AuditInfo{
			UpdatedAt: quoteShipmentReply.GetShipment().GetAudit().GetUpdatedAt(),
			Etag:      quoteShipmentReply.GetShipment().GetAudit().GetEtag(),
		},
	}
	explainFulfillmentExceptionDecisionReply, err := s.fulfillmentExceptionValidationClient.ExplainFulfillmentExceptionDecision(ctx, explainFulfillmentExceptionDecisionRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ExplainFulfillmentExceptionDecision")
	}
	browsePackingSlipsRequest := &fulfillmentv1.BrowsePackingSlipsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.BrowsePackingSlipsRequest_Filter{
			Region: quoteShipmentReply.GetShipment().GetBillingAddress().GetRegion(),
		},
	}
	browsePackingSlipsReply := new(fulfillmentv1.BrowsePackingSlipsResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.fulfillment.v1.PackingSlipQueryService/BrowsePackingSlips", browsePackingSlipsRequest, browsePackingSlipsReply); err != nil {
		return grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipQueryService/BrowsePackingSlips")
	}
	if err := stream.Send(&fulfillmentv1.WatchFulfillmentsResponse{
		Fulfillment: &fulfillmentv1.Fulfillment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: quoteShipmentReply.GetShipment().GetAudit().GetUpdatedBy(),
			},
			DueAt:          explainFulfillmentExceptionDecisionReply.GetFulfillmentException().GetSla().GetDueAt(),
			Counterparty:   &customersv1.CustomerRef{Email: req.GetSubscriber().GetEmail()},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{
			Status:       quoteShipmentReply.GetResult().GetStatus(),
			ReferenceIds: browsePackingSlipsReply.GetResult().GetReferenceIds(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreateFulfillments takes a stream of fulfillments and answers once at the end.
func (s *FulfillmentServer) BatchCreateFulfillments(stream grpc.ClientStreamingServer[fulfillmentv1.BatchCreateFulfillmentsRequest, fulfillmentv1.BatchCreateFulfillmentsResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "batch create fulfillments", "tenant", msg.GetContext().GetTenantId())
		if _, err := s.sponsorClient.ListSponsors(ctx, &sponsorshipsv1.ListSponsorsRequest{
			Context: msg.GetContext(),
			Filter: &sponsorshipsv1.ListSponsorsRequest_Filter{
				Region:       msg.GetRecord().GetBillingAddress().GetRegion(),
				CreatedAfter: msg.GetRecord().GetAudit().GetUpdatedAt(),
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.sponsorships.v1.SponsorService/ListSponsors")
		}
		notifySearchQueryWatchersRequest := &searchv1.NotifySearchQueryWatchersRequest{
			Context: msg.GetContext(),
			Details: &searchv1.NotifySearchQueryWatchersRequest_Details{
				Recipient: &commonv1.ContactInfo{
					Name:             msg.GetHeader().GetSubmittedBy().GetName(),
					Email:            msg.GetHeader().GetSubmittedBy().GetEmail(),
					PreferredChannel: msg.GetHeader().GetSubmittedBy().GetPreferredChannel(),
					Phone:            msg.GetHeader().GetSubmittedBy().GetPhone(),
				},
			},
		}
		if _, err := grpcx.Call(ctx, s.searchQueryNotificationClient.NotifySearchQueryWatchers, notifySearchQueryWatchersRequest); err != nil {
			return grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/NotifySearchQueryWatchers")
		}
		previewFulfillmentExceptionRequest := &fulfillmentv1.PreviewFulfillmentExceptionRequest{
			Context: msg.GetContext(),
			FulfillmentException: &fulfillmentv1.FulfillmentException{
				Assignee: &identityv1.UserRef{Email: msg.GetHeader().GetSubmittedBy().GetEmail()},
				Audit:    &commonv1.AuditInfo{Etag: msg.GetRecord().GetAudit().GetEtag()},
			},
		}
		if _, err := fulfillmentv1.FulfillmentExceptionValidationServiceClient.PreviewFulfillmentException(s.fulfillmentExceptionValidationClient, ctx, previewFulfillmentExceptionRequest); err != nil {
			return grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/PreviewFulfillmentException")
		}
		accepted++
	}
	return stream.SendAndClose(&fulfillmentv1.BatchCreateFulfillmentsResponse{Accepted: accepted})
}

// CloneFulfillment creates a fulfillment.
func (s *FulfillmentServer) CloneFulfillment(ctx context.Context, req *fulfillmentv1.CloneFulfillmentRequest) (*fulfillmentv1.CloneFulfillmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFulfillment(ctx, req.GetContext().GetTenantId(), req.GetFulfillment()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment")
	}
	s.log.InfoContext(ctx, "clone fulfillment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillment().GetAudit().GetEtag())
	packingSlipQuery, err := registry.Lookup[fulfillmentv1.PackingSlipQueryServiceClient](s.clients, "acme.fulfillment.v1.PackingSlipQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	browsePackingSlipsRequest := &fulfillmentv1.BrowsePackingSlipsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.BrowsePackingSlipsRequest_Filter{
			Region:       req.GetFulfillment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetFulfillment().GetIssuedAt(),
		},
	}
	browsePackingSlipsReply, err := packingSlipQuery.BrowsePackingSlips(ctx, browsePackingSlipsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipQueryService/BrowsePackingSlips")
	}
	peekFulfillmentRequest := &fulfillmentv1.PeekFulfillmentRequest{
		Context:       req.GetContext(),
		FulfillmentId: req.GetFulfillment().GetFulfillmentId(),
	}
	peekFulfillmentReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*fulfillmentv1.PeekFulfillmentResponse, error) {
		return s.fulfillmentQueryClient.PeekFulfillment(ctx, peekFulfillmentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/PeekFulfillment")
	}
	explainFulfillmentExceptionDecisionReply, err := s.fulfillmentExceptionValidationClient.ExplainFulfillmentExceptionDecision(ctx, &fulfillmentv1.ExplainFulfillmentExceptionDecisionRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Requester: &commonv1.ContactInfo{Email: req.GetFulfillment().GetCounterparty().GetEmail()},
			Assignee: &identityv1.UserRef{
				DisplayName: peekFulfillmentReply.GetFulfillment().GetCounterparty().GetDisplayName(),
			},
			Origin: &fulfillmentv1.FulfillmentException_Customer{Customer: &fulfillmentv1.PackingSlipRef{
				DisplayName: req.GetFulfillment().GetCounterparty().GetDisplayName(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ExplainFulfillmentExceptionDecision")
	}
	return &fulfillmentv1.CloneFulfillmentResponse{
		Result: &commonv1.OperationResult{
			Message:       browsePackingSlipsReply.GetResult().GetMessage(),
			CorrelationId: browsePackingSlipsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  peekFulfillmentReply.GetResult().GetReferenceIds(),
			Status:        explainFulfillmentExceptionDecisionReply.GetResult().GetStatus(),
		},
		Fulfillment: &fulfillmentv1.Fulfillment{
			Notes:        peekFulfillmentReply.GetFulfillment().GetNotes(),
			Total:        req.GetFulfillment().GetTotal(),
			Counterparty: req.GetFulfillment().GetCounterparty(),
		},
	}, nil
}
