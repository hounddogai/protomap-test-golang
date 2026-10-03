package shipping

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// ShipmentQueryServer implements `acme.shipping.v1.ShipmentQueryService`.
// Answers read-only questions about shipments, for screens and reports.
type ShipmentQueryServer struct {
	*shipmentQueryHandlers
}

// shipmentQueryHandlers holds the handlers of ShipmentQueryServer, with what they use.
type shipmentQueryHandlers struct {
	shippingv1.UnimplementedShipmentQueryServiceServer
	store                          *Store
	log                            *slog.Logger
	parcelValidationClient         shippingv1.ParcelValidationServiceClient
	synonymNotificationClient      searchv1.SynonymNotificationServiceClient
	trackingEventCalculationClient shippingv1.TrackingEventCalculationServiceClient
	clients                        *registry.Registry
	conn                           *grpc.ClientConn
	carePlanQueryClient            careplansv1.CarePlanQueryServiceClient
	reviewModerationQueryClient    reviewsv1.ReviewModerationQueryServiceClient
	loyaltyMemberCalculationClient loyaltyv1.LoyaltyMemberCalculationServiceClient
	shipmentValidationClient       shippingv1.ShipmentValidationServiceClient
	ledgerAccountOnce              sync.Once
	ledgerAccountCached            ledgerv1.LedgerAccountServiceClient
	userValidationClient           identityv1.UserValidationServiceClient
}

// NewShipmentQueryServer wires a ShipmentQueryServer.
func NewShipmentQueryServer(deps platform.Deps) *ShipmentQueryServer {
	return &ShipmentQueryServer{shipmentQueryHandlers: &shipmentQueryHandlers{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.shipping.v1.ShipmentQueryService"),
		parcelValidationClient:         shippingv1.NewParcelValidationServiceClient(deps.Conn),
		synonymNotificationClient:      searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		trackingEventCalculationClient: shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn),
		clients:                        deps.Clients,
		conn:                           deps.Conn,
		carePlanQueryClient:            careplansv1.NewCarePlanQueryServiceClient(deps.Conn),
		reviewModerationQueryClient:    reviewsv1.NewReviewModerationQueryServiceClient(deps.Conn),
		loyaltyMemberCalculationClient: loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		shipmentValidationClient:       shippingv1.NewShipmentValidationServiceClient(deps.Conn),
		userValidationClient:           identityv1.NewUserValidationServiceClient(deps.Conn),
	}}
}

// LookupShipment returns a shipment.
func (s *shipmentQueryHandlers) LookupShipment(ctx context.Context, req *shippingv1.LookupShipmentRequest) (*shippingv1.LookupShipmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipment", req.GetShipmentId(), "lookup_shipment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup shipment", "tenant", req.GetContext().GetTenantId())
	previewParcelRequest := newPreviewParcelRequest(withPreviewParcelContext(req.GetContext()), withPreviewParcelParcel(&shippingv1.Parcel{
		Owner: &identityv1.UserRef{
			Email:  req.GetContext().GetActor().GetEmail(),
			UserId: req.GetContext().GetActor().GetUserId(),
		},
	}))
	previewParcelReply, err := s.parcelValidationClient.PreviewParcel(ctx, previewParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/PreviewParcel")
	}
	step := &shipmentQueryHandlersLookupShipmentListSynonymSubscriptionsStep{request: &searchv1.ListSynonymSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymSubscriptionsRequest_Filter{
			Region:       previewParcelReply.GetParcel().GetAddress().GetRegion(),
			CreatedAfter: previewParcelReply.GetParcel().GetAudit().GetUpdatedAt(),
			Labels:       previewParcelReply.GetResult().GetDetails(),
			OwnerId:      previewParcelReply.GetParcel().GetComponents()["primary"].GetComponentId(),
		},
	}}
	if err := s.runLookupShipmentListSynonymSubscriptions(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/ListSynonymSubscriptions")
	}
	listSynonymSubscriptionsReply := step.reply
	return &shippingv1.LookupShipmentResponse{
		Result: &commonv1.OperationResult{
			Status:  previewParcelReply.GetResult().GetStatus(),
			Message: listSynonymSubscriptionsReply.GetResult().GetMessage(),
		},
		Shipment: &shippingv1.Shipment{ShipmentId: req.GetShipmentId()},
	}, nil
}

// BrowseShipments lists shipments a page at a time.
func (s *shipmentQueryHandlers) BrowseShipments(ctx context.Context, req *shippingv1.BrowseShipmentsRequest) (*shippingv1.BrowseShipmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse shipments", "tenant", req.GetContext().GetTenantId())
	estimateTrackingEventRequest := buildEstimateTrackingEventRequestForBrowseShipments(req)
	estimateTrackingEventReply, err := s.trackingEventCalculationClient.EstimateTrackingEvent(ctx, estimateTrackingEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/EstimateTrackingEvent")
	}
	shipmentValidation, ok := s.clients.Get("acme.shipping.v1.ShipmentValidationService").(shippingv1.ShipmentValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.shipping.v1.ShipmentValidationService")
	}
	scoreShipmentRequest := &shippingv1.ScoreShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{
				DisplayName: estimateTrackingEventReply.GetTrackingEvent().GetSubject().GetDisplayName(),
			},
			ShipmentId: estimateTrackingEventReply.GetTrackingEvent().GetSubject().GetShipmentId(),
		},
		Details: &shippingv1.ScoreShipmentRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	scoreShipmentReply, err := shipmentValidation.ScoreShipment(ctx, scoreShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/ScoreShipment")
	}
	return &shippingv1.BrowseShipmentsResponse{
		Shipments: []*shippingv1.Shipment{&shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{
				DisplayName: estimateTrackingEventReply.GetTrackingEvent().GetSubject().GetDisplayName(),
			},
			Total: scoreShipmentReply.GetBreakdown().GetTotal(),
			Notes: scoreShipmentReply.GetShipment().GetNotes(),
		}},
		Result: &commonv1.OperationResult{Status: estimateTrackingEventReply.GetResult().GetStatus()},
	}, nil
}

// FindShipments lists shipments a page at a time.
func (s *shipmentQueryHandlers) FindShipments(ctx context.Context, req *shippingv1.FindShipmentsRequest) (*shippingv1.FindShipmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FindShipments", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "find shipments", "tenant", req.GetContext().GetTenantId())
	resolveDocumentsRequest := &documentsv1.ResolveDocumentsRequest{Context: req.GetContext()}
	resolveDocumentsReply, err := grpcx.NewClient(s.conn, documentsv1.NewDocumentQueryServiceClient).ResolveDocuments(ctx, resolveDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/ResolveDocuments")
	}
	exportCarePlansStream, err := s.carePlanQueryClient.ExportCarePlans(ctx, &careplansv1.ExportCarePlansRequest{
		Context: req.GetContext(),
		Filter: &careplansv1.ExportCarePlansRequest_Filter{
			Region:  req.GetFilter().GetRegion(),
			Query:   req.GetFilter().GetQuery(),
			OwnerId: req.GetFilter().GetOwnerId(),
			Labels:  req.GetFilter().GetLabels(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.careplans.v1.CarePlanQueryService/ExportCarePlans")
	}
	for {
		received, err := exportCarePlansStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.careplans.v1.CarePlanQueryService/ExportCarePlans")
		}
		_ = received
	}
	return &shippingv1.FindShipmentsResponse{
		PageInfo: &commonv1.PageInfo{
			NextPageToken: resolveDocumentsReply.GetPageInfo().GetNextPageToken(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountShipments counts shipments by status.
func (s *shipmentQueryHandlers) CountShipments(ctx context.Context, req *shippingv1.CountShipmentsRequest) (*shippingv1.CountShipmentsResponse, error) {
	s.log.InfoContext(ctx, "count shipments", "tenant", req.GetContext().GetTenantId())
	resolveReviewModerationsRequest := &reviewsv1.ResolveReviewModerationsRequest{}
	resolveReviewModerationsRequest.Context = req.GetContext()
	resolveReviewModerationsReply, err := s.reviewModerationQueryClient.ResolveReviewModerations(ctx, resolveReviewModerationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/ResolveReviewModerations")
	}
	return &shippingv1.CountShipmentsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolveReviewModerationsReply.GetResult().GetCorrelationId(),
			Status:        resolveReviewModerationsReply.GetResult().GetStatus(),
		},
	}, nil
}

// ResolveShipments returns several shipments at once.
func (s *shipmentQueryHandlers) ResolveShipments(ctx context.Context, req *shippingv1.ResolveShipmentsRequest) (*shippingv1.ResolveShipmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResolveShipments", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve shipments", "tenant", req.GetContext().GetTenantId())
	calculateLoyaltyMemberReply, err := s.loyaltyMemberCalculationClient.CalculateLoyaltyMember(ctx, &loyaltyv1.CalculateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CalculateLoyaltyMember")
	}
	previewShipmentReply, err := s.shipmentValidationClient.PreviewShipment(ctx, &shippingv1.PreviewShipmentRequest{
		Context: req.GetContext(),
		Details: &shippingv1.PreviewShipmentRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name: calculateLoyaltyMemberReply.GetLoyaltyMember().GetContact().GetName(),
			},
		},
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculateLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetCreatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/PreviewShipment")
	}
	return &shippingv1.ResolveShipmentsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  calculateLoyaltyMemberReply.GetResult().GetReferenceIds(),
			CorrelationId: calculateLoyaltyMemberReply.GetResult().GetCorrelationId(),
			Details:       previewShipmentReply.GetResult().GetDetails(),
			Status:        "ok",
		},
	}, nil
}

// ExportShipments streams every matching shipment.
func (s *shipmentQueryHandlers) ExportShipments(req *shippingv1.ExportShipmentsRequest, stream grpc.ServerStreamingServer[shippingv1.ExportShipmentsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "export shipments", "tenant", req.GetContext().GetTenantId())
	searchLedgerAccountsRequest := &ledgerv1.SearchLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter:  &ledgerv1.SearchLedgerAccountsRequest_Filter{CreatedAfter: req.GetFilter().GetCreatedAfter()},
	}
	searchLedgerAccountsReply, err := s.ledgerAccountLazy().SearchLedgerAccounts(ctx, searchLedgerAccountsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/SearchLedgerAccounts")
	}
	normalizeParcelRequest := &shippingv1.NormalizeParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
			Owner:   &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &shippingv1.NormalizeParcelRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	normalizeParcelReply := new(shippingv1.NormalizeParcelResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.ParcelValidationService_NormalizeParcel_FullMethodName, normalizeParcelRequest, normalizeParcelReply); err != nil {
		return grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/NormalizeParcel")
	}
	if err := stream.Send(&shippingv1.ExportShipmentsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: searchLedgerAccountsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Shipment: &shippingv1.Shipment{
			Counterparty:   &shippingv1.ParcelRef{Code: normalizeParcelReply.GetParcel().GetCode()},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeShipments summarizes a shipment.
func (s *shipmentQueryHandlers) SummarizeShipments(ctx context.Context, req *shippingv1.SummarizeShipmentsRequest) (*shippingv1.SummarizeShipmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipment", req.GetShipmentId(), "summarize_shipments"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize shipments", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key2, value2 := range req.GetDetails().GetFilters() {
		filters[key2] = value2
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareTrackingEventOptionsRequest := &shippingv1.CompareTrackingEventOptionsRequest{}
	compareTrackingEventOptionsRequest.Context = req.GetContext()
	compareTrackingEventOptionsRequest.TrackingEvent = &shippingv1.TrackingEvent{
		Subject:    &shippingv1.ShipmentRef{ShipmentId: req.GetShipmentId()},
		Attributes: filters,
		Detail:     packed,
	}
	compareTrackingEventOptionsRequest.Reason = req.GetReason()
	compareTrackingEventOptionsRequest.Annotations = annotations
	compareTrackingEventOptionsReply, err := s.trackingEventCalculationClient.CompareTrackingEventOptions(ctx, compareTrackingEventOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/CompareTrackingEventOptions")
	}
	getUserRulesRequest := &identityv1.GetUserRulesRequest{
		Context: req.GetContext(),
		UserId:  req.GetContext().GetActor().GetUserId(),
	}
	type getUserRulesResult struct {
		reply *identityv1.GetUserRulesResponse
		err   error
	}
	getUserRulesResults := make(chan getUserRulesResult, 1)
	go func() {
		reply, err := s.userValidationClient.GetUserRules(ctx, getUserRulesRequest)
		getUserRulesResults <- getUserRulesResult{reply, err}
	}()
	var getUserRulesReply *identityv1.GetUserRulesResponse
	select {
	case result := <-getUserRulesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.UserValidationService/GetUserRules")
		}
		getUserRulesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &shippingv1.SummarizeShipmentsResponse{
		Result: &commonv1.OperationResult{
			Status:  compareTrackingEventOptionsReply.GetResult().GetStatus(),
			Details: getUserRulesReply.GetResult().GetDetails(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestShipments lists shipments a page at a time.
func (s *shipmentQueryHandlers) SuggestShipments(ctx context.Context, req *shippingv1.SuggestShipmentsRequest) (*shippingv1.SuggestShipmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "suggest shipments", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.SuggestShipmentsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// PeekShipment returns a shipment.
func (s *shipmentQueryHandlers) PeekShipment(ctx context.Context, req *shippingv1.PeekShipmentRequest) (*shippingv1.PeekShipmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipment", req.GetShipmentId(), "peek_shipment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek shipment", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.PeekShipmentResponse{
		Shipment: &shippingv1.Shipment{ShipmentId: req.GetShipmentId()},
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// shipmentQueryHandlersLookupShipmentListSynonymSubscriptionsStep carries the request of one step of LookupShipment
// and, once it ran, its reply.
type shipmentQueryHandlersLookupShipmentListSynonymSubscriptionsStep struct {
	request *searchv1.ListSynonymSubscriptionsRequest
	reply   *searchv1.ListSynonymSubscriptionsResponse
}

func (s *shipmentQueryHandlers) runLookupShipmentListSynonymSubscriptions(ctx context.Context, step *shipmentQueryHandlersLookupShipmentListSynonymSubscriptionsStep) error {
	reply, err := s.synonymNotificationClient.ListSynonymSubscriptions(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// ledgerAccountLazy dials acme.ledger.v1.LedgerAccountService the first time a handler needs it.
func (s *shipmentQueryHandlers) ledgerAccountLazy() ledgerv1.LedgerAccountServiceClient {
	s.ledgerAccountOnce.Do(func() { s.ledgerAccountCached = ledgerv1.NewLedgerAccountServiceClient(s.conn) })
	return s.ledgerAccountCached
}
