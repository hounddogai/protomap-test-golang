package shipping

import (
	"context"
	"fmt"
	"log/slog"

	collectionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/collections/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// ParcelQueryServer implements `acme.shipping.v1.ParcelQueryService`.
// Answers read-only questions about parcels, for screens and reports.
type ParcelQueryServer struct {
	shippingv1.UnimplementedParcelQueryServiceServer
	store                             *Store
	log                               *slog.Logger
	collectionCaseQueryClient         collectionsv1.CollectionCaseQueryServiceClient
	manifestCalculationClient         shippingv1.ManifestCalculationServiceClient
	manifestCalculationClientFallback shippingv1.ManifestCalculationServiceClient
	meteredSearchQueryValidation      meteredSearchQueryValidation
	shipmentCalculationClient         shippingv1.ShipmentCalculationServiceClient
	sessionNotificationPort           sessionNotificationPort
	sellerScorecardCalculationClient  sellersv1.SellerScorecardCalculationServiceClient
	conn                              *grpc.ClientConn
}

// NewParcelQueryServer wires a ParcelQueryServer.
func NewParcelQueryServer(deps platform.Deps) *ParcelQueryServer {
	return &ParcelQueryServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.shipping.v1.ParcelQueryService"),
		collectionCaseQueryClient:         collectionsv1.NewCollectionCaseQueryServiceClient(deps.Conn),
		manifestCalculationClient:         shippingv1.NewManifestCalculationServiceClient(deps.Conn),
		manifestCalculationClientFallback: shippingv1.NewManifestCalculationServiceClient(deps.Conn),
		meteredSearchQueryValidation:      meteredSearchQueryValidation{SearchQueryValidationServiceClient: searchv1.NewSearchQueryValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		shipmentCalculationClient:         shippingv1.NewShipmentCalculationServiceClient(deps.Conn),
		sessionNotificationPort:           newSessionNotificationPort(deps),
		sellerScorecardCalculationClient:  sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn),
		conn:                              deps.Conn,
	}
}

// LookupParcel returns a parcel.
func (s *ParcelQueryServer) LookupParcel(ctx context.Context, req *shippingv1.LookupParcelRequest) (*shippingv1.LookupParcelResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "lookup_parcel"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup parcel", "tenant", req.GetContext().GetTenantId())
	resolveCollectionCasesReply, err := s.collectionCaseQueryClient.ResolveCollectionCases(ctx, &collectionsv1.ResolveCollectionCasesRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.collections.v1.CollectionCaseQueryService/ResolveCollectionCases")
	}
	return &shippingv1.LookupParcelResponse{
		Result: &commonv1.OperationResult{
			Message: resolveCollectionCasesReply.GetResult().GetMessage(),
			Details: resolveCollectionCasesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Parcel: &shippingv1.Parcel{ParcelId: req.GetParcelId()},
	}, nil
}

// BrowseParcels lists parcels a page at a time.
func (s *ParcelQueryServer) BrowseParcels(ctx context.Context, req *shippingv1.BrowseParcelsRequest) (*shippingv1.BrowseParcelsResponse, error) {
	s.log.InfoContext(ctx, "browse parcels", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.BrowseParcelsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountParcels counts parcels by status.
func (s *ParcelQueryServer) CountParcels(ctx context.Context, req *shippingv1.CountParcelsRequest) (*shippingv1.CountParcelsResponse, error) {
	s.log.InfoContext(ctx, "count parcels", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *shippingv1.RecalculateManifestsRequest, ...grpc.CallOption) (*shippingv1.RecalculateManifestsResponse, error){
		"primary":  s.manifestCalculationClient.RecalculateManifests,
		"fallback": s.manifestCalculationClientFallback.RecalculateManifests,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	recalculateManifestsRequest := &shippingv1.RecalculateManifestsRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			BillingAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		},
	}
	recalculateManifestsReply, err := route(ctx, recalculateManifestsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/RecalculateManifests")
	}
	explainSearchQueryDecisionRequest := &searchv1.ExplainSearchQueryDecisionRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: recalculateManifestsReply.GetManifest().GetCounterparty().GetDisplayName(),
			},
		},
	}
	explainSearchQueryDecisionReply, err := s.meteredSearchQueryValidation.ExplainSearchQueryDecision(ctx, explainSearchQueryDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/ExplainSearchQueryDecision")
	}
	return &shippingv1.CountParcelsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  recalculateManifestsReply.GetResult().GetReferenceIds(),
			CorrelationId: recalculateManifestsReply.GetResult().GetCorrelationId(),
			Status:        explainSearchQueryDecisionReply.GetOutcome().GetStatus(),
			Details:       explainSearchQueryDecisionReply.GetResult().GetDetails(),
		},
	}, nil
}

// ResolveParcels returns several parcels at once.
func (s *ParcelQueryServer) ResolveParcels(ctx context.Context, req *shippingv1.ResolveParcelsRequest) (*shippingv1.ResolveParcelsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "resolve parcels", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	compareShipmentOptionsRequest := &shippingv1.CompareShipmentOptionsRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Audit:    &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Metadata: baggage,
		},
	}
	type compareShipmentOptionsResult struct {
		reply *shippingv1.CompareShipmentOptionsResponse
		err   error
	}
	compareShipmentOptionsResults := make(chan compareShipmentOptionsResult, 1)
	go func() {
		reply, err := s.shipmentCalculationClient.CompareShipmentOptions(ctx, compareShipmentOptionsRequest)
		compareShipmentOptionsResults <- compareShipmentOptionsResult{reply, err}
	}()
	var compareShipmentOptionsReply *shippingv1.CompareShipmentOptionsResponse
	select {
	case result := <-compareShipmentOptionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.shipping.v1.ShipmentCalculationService/CompareShipmentOptions")
		}
		compareShipmentOptionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	unsubscribeSessionUpdatesRequest := &identityv1.UnsubscribeSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: compareShipmentOptionsReply.GetResult().GetDetails(),
		},
		SessionId: compareShipmentOptionsReply.GetResult().GetCorrelationId(),
	}
	unsubscribeSessionUpdatesReply, err := s.sessionNotificationPort.UnsubscribeSessionUpdates(ctx, unsubscribeSessionUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/UnsubscribeSessionUpdates")
	}
	return &shippingv1.ResolveParcelsResponse{
		Parcels: []*shippingv1.Parcel{&shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				CreatedBy: compareShipmentOptionsReply.GetShipment().GetAudit().GetCreatedBy(),
				UpdatedAt: unsubscribeSessionUpdatesReply.GetSession().GetAudit().GetUpdatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SummarizeParcels summarizes a parcel.
func (s *ParcelQueryServer) SummarizeParcels(ctx context.Context, req *shippingv1.SummarizeParcelsRequest) (*shippingv1.SummarizeParcelsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "summarize_parcels"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize parcels", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	explainSellerScorecardCalculationRequest := &sellersv1.ExplainSellerScorecardCalculationRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		SellerScorecard: &sellersv1.SellerScorecard{
			Counterparty: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		Details: &sellersv1.ExplainSellerScorecardCalculationRequest_Details{Locale: (&decoded).GetLocale()},
	}
	type explainSellerScorecardCalculationResult struct {
		reply *sellersv1.ExplainSellerScorecardCalculationResponse
		err   error
	}
	explainSellerScorecardCalculationResults := make(chan explainSellerScorecardCalculationResult, 1)
	go func() {
		reply, err := s.sellerScorecardCalculationClient.ExplainSellerScorecardCalculation(ctx, explainSellerScorecardCalculationRequest)
		explainSellerScorecardCalculationResults <- explainSellerScorecardCalculationResult{reply, err}
	}()
	var explainSellerScorecardCalculationReply *sellersv1.ExplainSellerScorecardCalculationResponse
	select {
	case result := <-explainSellerScorecardCalculationResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerScorecardCalculationService/ExplainSellerScorecardCalculation")
		}
		explainSellerScorecardCalculationReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	revertUserRequest := &identityv1.RevertUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainSellerScorecardCalculationReply.GetSellerScorecard().GetAudit().GetUpdatedBy(),
			},
			RelationshipManager: &identityv1.UserRef{
				Email:       explainSellerScorecardCalculationReply.GetSellerScorecard().GetCounterparty().GetEmail(),
				DisplayName: explainSellerScorecardCalculationReply.GetSellerScorecard().GetCounterparty().GetDisplayName(),
			},
		},
	}
	revertUserReply, err := grpcx.NewClient(s.conn, identityv1.NewUserHistoryServiceClient).RevertUser(ctx, revertUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RevertUser")
	}
	return &shippingv1.SummarizeParcelsResponse{
		Result: &commonv1.OperationResult{
			Status:        explainSellerScorecardCalculationReply.GetResult().GetStatus(),
			CorrelationId: explainSellerScorecardCalculationReply.GetResult().GetCorrelationId(),
			ReferenceIds:  revertUserReply.GetResult().GetReferenceIds(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// PeekParcel returns a parcel.
func (s *ParcelQueryServer) PeekParcel(ctx context.Context, req *shippingv1.PeekParcelRequest) (*shippingv1.PeekParcelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PeekParcel", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "peek_parcel"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek parcel", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	compareManifestOptionsRequest := &shippingv1.CompareManifestOptionsRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Counterparty: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			Audit:        &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
			Lines:        []*shippingv1.ManifestLine{&shippingv1.ManifestLine{Attributes: baggage}},
		},
	}
	compareManifestOptionsReply := new(shippingv1.CompareManifestOptionsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", shippingManifestCalculationServiceName, "CompareManifestOptions"), compareManifestOptionsRequest, compareManifestOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/CompareManifestOptions")
	}
	explainRewardDecisionRequest := &loyaltyv1.ExplainRewardDecisionRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: compareManifestOptionsReply.GetManifest().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: compareManifestOptionsReply.GetManifest().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &loyaltyv1.ExplainRewardDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: compareManifestOptionsReply.GetManifest().GetCounterparty().GetEmail(),
			},
		},
	}
	explainRewardDecisionReply, err := explainRewardDecisionCall(ctx, explainRewardDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardValidationService/ExplainRewardDecision")
	}
	return &shippingv1.PeekParcelResponse{
		Result: &commonv1.OperationResult{
			Details:      compareManifestOptionsReply.GetResult().GetDetails(),
			ReferenceIds: compareManifestOptionsReply.GetResult().GetReferenceIds(),
			Message:      explainRewardDecisionReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Parcel: &shippingv1.Parcel{ParcelId: req.GetParcelId()},
	}, nil
}
