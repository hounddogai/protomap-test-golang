package inventory

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// StockReservationQueryServer implements `acme.inventory.v1.StockReservationQueryService`.
// Answers read-only questions about stock reservations, for screens and reports.
type StockReservationQueryServer struct {
	inventoryv1.UnimplementedStockReservationQueryServiceServer
	store                              *Store
	log                                *slog.Logger
	replenishmentPlanValidationClient  inventoryv1.ReplenishmentPlanValidationServiceClient
	parcelCalculationClient            shippingv1.ParcelCalculationServiceClient
	clients                            *registry.Registry
	replenishmentPlanCalculationClient inventoryv1.ReplenishmentPlanCalculationServiceClient
	restockDecisionValidationClient    returnsv1.RestockDecisionValidationServiceClient
	roleNotificationClient             identityv1.RoleNotificationServiceClient
	apiCredentialHistoryClient         identityv1.ApiCredentialHistoryServiceClient
	stockItemQueryPort                 stockItemQueryPort
	conn                               *grpc.ClientConn
}

// NewStockReservationQueryServer wires a StockReservationQueryServer.
func NewStockReservationQueryServer(deps platform.Deps) *StockReservationQueryServer {
	return &StockReservationQueryServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.inventory.v1.StockReservationQueryService"),
		replenishmentPlanValidationClient:  inventoryv1.NewReplenishmentPlanValidationServiceClient(deps.Conn),
		parcelCalculationClient:            shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		clients:                            deps.Clients,
		replenishmentPlanCalculationClient: inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn),
		restockDecisionValidationClient:    returnsv1.NewRestockDecisionValidationServiceClient(deps.Conn),
		roleNotificationClient:             identityv1.NewRoleNotificationServiceClient(deps.Conn),
		apiCredentialHistoryClient:         identityv1.NewApiCredentialHistoryServiceClient(deps.Conn),
		stockItemQueryPort:                 newStockItemQueryPort(deps),
		conn:                               deps.Conn,
	}
}

// LookupStockReservation returns a stock reservation.
func (s *StockReservationQueryServer) LookupStockReservation(ctx context.Context, req *inventoryv1.LookupStockReservationRequest) (*inventoryv1.LookupStockReservationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupStockReservation", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_reservation", req.GetStockReservationId(), "lookup_stock_reservation"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup stock reservation", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "stock_reservation_id", req.GetStockReservationId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	step := &stockReservationQueryServerLookupStockReservationScoreReplenishmentPlanStep{request: &inventoryv1.ScoreReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Counterparty: &inventoryv1.StockReservationRef{
				StockReservationId: tenancy.Value(ctx, "stock_reservation_id"),
			},
			IssuedAt: req.GetContext().GetDeadline(),
		},
		Annotations: baggage,
	}}
	if err := s.runLookupStockReservationScoreReplenishmentPlan(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/ScoreReplenishmentPlan")
	}
	scoreReplenishmentPlanReply := step.reply
	return &inventoryv1.LookupStockReservationResponse{
		Result: &commonv1.OperationResult{
			Message: scoreReplenishmentPlanReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		StockReservation: &inventoryv1.StockReservation{
			StockReservationId: scoreReplenishmentPlanReply.GetReplenishmentPlan().GetCounterparty().GetStockReservationId(),
		},
	}, nil
}

// BrowseStockReservations lists stock reservations a page at a time.
func (s *StockReservationQueryServer) BrowseStockReservations(ctx context.Context, req *inventoryv1.BrowseStockReservationsRequest) (*inventoryv1.BrowseStockReservationsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse stock reservations", "tenant", req.GetContext().GetTenantId())
	return &inventoryv1.BrowseStockReservationsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindStockReservations lists stock reservations a page at a time.
func (s *StockReservationQueryServer) FindStockReservations(ctx context.Context, req *inventoryv1.FindStockReservationsRequest) (*inventoryv1.FindStockReservationsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FindStockReservations", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "find stock reservations", "tenant", req.GetContext().GetTenantId())
	return &inventoryv1.FindStockReservationsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountStockReservations counts stock reservations by status.
func (s *StockReservationQueryServer) CountStockReservations(ctx context.Context, req *inventoryv1.CountStockReservationsRequest) (*inventoryv1.CountStockReservationsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountStockReservations", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count stock reservations", "tenant", req.GetContext().GetTenantId())
	explainReplenishmentPlanDecisionRequest := &inventoryv1.ExplainReplenishmentPlanDecisionRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.ExplainReplenishmentPlanDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Audit:          &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
	}
	explainReplenishmentPlanDecisionReply, err := explainReplenishmentPlanDecisionCall(ctx, explainReplenishmentPlanDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/ExplainReplenishmentPlanDecision")
	}
	return &inventoryv1.CountStockReservationsResponse{
		Result: &commonv1.OperationResult{
			Status:  explainReplenishmentPlanDecisionReply.GetOutcome().GetStatus(),
			Details: explainReplenishmentPlanDecisionReply.GetResult().GetDetails(),
		},
	}, nil
}

// ExportStockReservations streams every matching stock reservation.
func (s *StockReservationQueryServer) ExportStockReservations(req *inventoryv1.ExportStockReservationsRequest, stream grpc.ServerStreamingServer[inventoryv1.ExportStockReservationsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "export stock reservations", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := stockReservationQueryServerEstimateParcelSteps[mode]
	estimateParcelRequest := &shippingv1.EstimateParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
			Owner:   &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	estimateParcelReply, err := run(s, ctx, estimateParcelRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/EstimateParcel")
	}
	putawayTaskValidation, ok := s.clients.Get("acme.warehousing.v1.PutawayTaskValidationService").(warehousingv1.PutawayTaskValidationServiceClient)
	if !ok {
		return status.Errorf(codes.Internal, "no client of %s", "acme.warehousing.v1.PutawayTaskValidationService")
	}
	validatePutawayTaskRequest := &warehousingv1.ValidatePutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Origin: &warehousingv1.PutawayTask_Customer{Customer: &warehousingv1.StorageBinRef{
				DisplayName: estimateParcelReply.GetParcel().GetOwner().GetDisplayName(),
			}},
			Comments: []*warehousingv1.PutawayTaskComment{&warehousingv1.PutawayTaskComment{
				CreatedAt: estimateParcelReply.GetParcel().GetAudit().GetCreatedAt(),
			}},
		},
	}
	validatePutawayTaskReply, err := putawayTaskValidation.ValidatePutawayTask(ctx, validatePutawayTaskRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/ValidatePutawayTask")
	}
	previewReplenishmentPlanRequest := &inventoryv1.PreviewReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Audit: &commonv1.AuditInfo{
				CreatedAt: validatePutawayTaskReply.GetPutawayTask().GetAudit().GetCreatedAt(),
				Etag:      estimateParcelReply.GetParcel().GetAudit().GetEtag(),
			},
		},
		Details: &inventoryv1.PreviewReplenishmentPlanRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: validatePutawayTaskReply.GetPutawayTask().GetRequester().GetPhone(),
				Email: validatePutawayTaskReply.GetPutawayTask().GetRequester().GetEmail(),
			},
		},
	}
	type previewReplenishmentPlanResult struct {
		reply *inventoryv1.PreviewReplenishmentPlanResponse
		err   error
	}
	previewReplenishmentPlanResults := make(chan previewReplenishmentPlanResult, 1)
	go func() {
		reply, err := s.replenishmentPlanValidationClient.PreviewReplenishmentPlan(ctx, previewReplenishmentPlanRequest)
		previewReplenishmentPlanResults <- previewReplenishmentPlanResult{reply, err}
	}()
	var previewReplenishmentPlanReply *inventoryv1.PreviewReplenishmentPlanResponse
	select {
	case result := <-previewReplenishmentPlanResults:
		if result.err != nil {
			return grpcx.Wrap(result.err, "acme.inventory.v1.ReplenishmentPlanValidationService/PreviewReplenishmentPlan")
		}
		previewReplenishmentPlanReply = result.reply
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	quoteReplenishmentPlanRequest := buildQuoteReplenishmentPlanRequestForExportStockReservations(req, previewReplenishmentPlanReply)
	quoteReplenishmentPlanReply, err := s.replenishmentPlanCalculationClient.QuoteReplenishmentPlan(ctx, quoteReplenishmentPlanRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/QuoteReplenishmentPlan")
	}
	if err := stream.Send(&inventoryv1.ExportStockReservationsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  estimateParcelReply.GetResult().GetReferenceIds(),
			CorrelationId: estimateParcelReply.GetResult().GetCorrelationId(),
			Message:       previewReplenishmentPlanReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		StockReservation: &inventoryv1.StockReservation{
			Participants: []*inventoryv1.StockReservationParticipant{&inventoryv1.StockReservationParticipant{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: validatePutawayTaskReply.GetPutawayTask().GetRequester().GetPreferredChannel(),
				},
			}},
			Metadata: quoteReplenishmentPlanReply.GetReplenishmentPlan().GetMetadata(),
			Location: &commonv1.PostalAddress{
				Region: quoteReplenishmentPlanReply.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
			},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeStockReservations summarizes a stock reservation.
func (s *StockReservationQueryServer) SummarizeStockReservations(ctx context.Context, req *inventoryv1.SummarizeStockReservationsRequest) (*inventoryv1.SummarizeStockReservationsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_reservation", req.GetStockReservationId(), "summarize_stock_reservations"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize stock reservations", "tenant", req.GetContext().GetTenantId())
	step := &stockReservationQueryServerSummarizeStockReservationsLintRestockDecisionsStep{request: &returnsv1.LintRestockDecisionsRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Owner:  &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
			Action: &returnsv1.RestockDecision_Block{Block: &returnsv1.RestockDecisionBlockAction{Reason: req.GetReason()}},
		},
	}}
	if err := s.runSummarizeStockReservationsLintRestockDecisions(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionValidationService/LintRestockDecisions")
	}
	lintRestockDecisionsReply := step.reply
	replenishmentPlanValidation, err := registry.Lookup[inventoryv1.ReplenishmentPlanValidationServiceClient](s.clients, "acme.inventory.v1.ReplenishmentPlanValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	normalizeReplenishmentPlanRequest := &inventoryv1.NormalizeReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: lintRestockDecisionsReply.GetRestockDecision().GetAudit().GetUpdatedBy(),
			},
		},
	}
	normalizeReplenishmentPlanReply, err := replenishmentPlanValidation.NormalizeReplenishmentPlan(ctx, normalizeReplenishmentPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/NormalizeReplenishmentPlan")
	}
	unsubscribeRoleUpdatesRequest := &identityv1.UnsubscribeRoleUpdatesRequest{
		Context: req.GetContext(),
		Reason:  lintRestockDecisionsReply.GetRestockDecision().GetBlock().GetReason(),
		Details: &identityv1.UnsubscribeRoleUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: lintRestockDecisionsReply.GetRestockDecision().GetBlock().GetNotifyEmail(),
			},
		},
	}
	unsubscribeRoleUpdatesReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*identityv1.UnsubscribeRoleUpdatesResponse, error) {
		return s.roleNotificationClient.UnsubscribeRoleUpdates(ctx, unsubscribeRoleUpdatesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/UnsubscribeRoleUpdates")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	recordApiCredentialVersionReply, err := s.apiCredentialHistoryClient.RecordApiCredentialVersion(ctx, &identityv1.RecordApiCredentialVersionRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: unsubscribeRoleUpdatesReply.GetRole().GetAudit().GetUpdatedBy(),
				UpdatedAt: unsubscribeRoleUpdatesReply.GetRole().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &identityv1.RecordApiCredentialVersionRequest_Details{Snapshot: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/RecordApiCredentialVersion")
	}
	_ = recordApiCredentialVersionReply
	return &inventoryv1.SummarizeStockReservationsResponse{
		Result: &commonv1.OperationResult{
			Status:        lintRestockDecisionsReply.GetOutcome().GetStatus(),
			Message:       lintRestockDecisionsReply.GetOutcome().GetMessage(),
			CorrelationId: normalizeReplenishmentPlanReply.GetResult().GetCorrelationId(),
			Details:       unsubscribeRoleUpdatesReply.GetResult().GetDetails(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestStockReservations lists stock reservations a page at a time.
func (s *StockReservationQueryServer) SuggestStockReservations(ctx context.Context, req *inventoryv1.SuggestStockReservationsRequest) (*inventoryv1.SuggestStockReservationsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "suggest stock reservations", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded inventoryv1.SuggestStockReservationsRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	browseStockItemsRequest := &inventoryv1.BrowseStockItemsRequest{
		Context: req.GetContext(),
		Filter: &inventoryv1.BrowseStockItemsRequest_Filter{
			OwnerId:      (&decoded).GetOwnerId(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Query:        req.GetFilter().GetQuery(),
		},
		Page: &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
	}
	browseStockItemsReply, err := s.stockItemQueryPort.BrowseStockItems(ctx, browseStockItemsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemQueryService/BrowseStockItems")
	}
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	scoreReplenishmentPlanRequest := &inventoryv1.ScoreReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
			Audit:          &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
			Lines:          []*inventoryv1.ReplenishmentPlanLine{&inventoryv1.ReplenishmentPlanLine{Attributes: browseStockItemsReply.GetResult().GetDetails()}},
			IssuedAt:       req.GetContext().GetDeadline(),
		},
	}
	scoreReplenishmentPlanReply, err := grpcx.NewClient(s.conn, inventoryv1.NewReplenishmentPlanValidationServiceClient).ScoreReplenishmentPlan(ctx, scoreReplenishmentPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/ScoreReplenishmentPlan")
	}
	return &inventoryv1.SuggestStockReservationsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: browseStockItemsReply.GetResult().GetReferenceIds(),
			Status:       browseStockItemsReply.GetResult().GetStatus(),
		},
		StockReservations: []*inventoryv1.StockReservation{&inventoryv1.StockReservation{
			Location: &commonv1.PostalAddress{
				Lines: scoreReplenishmentPlanReply.GetReplenishmentPlan().GetBillingAddress().GetLines(),
			},
		}},
	}, nil
}

// PeekStockReservation returns a stock reservation.
func (s *StockReservationQueryServer) PeekStockReservation(ctx context.Context, req *inventoryv1.PeekStockReservationRequest) (*inventoryv1.PeekStockReservationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PeekStockReservation", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_reservation", req.GetStockReservationId(), "peek_stock_reservation"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek stock reservation", "tenant", req.GetContext().GetTenantId())
	return &inventoryv1.PeekStockReservationResponse{
		StockReservation: &inventoryv1.StockReservation{StockReservationId: req.GetStockReservationId()},
		Result:           &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// stockReservationQueryServerLookupStockReservationScoreReplenishmentPlanStep carries the request of one step of
// LookupStockReservation and, once it ran, its reply.
type stockReservationQueryServerLookupStockReservationScoreReplenishmentPlanStep struct {
	request *inventoryv1.ScoreReplenishmentPlanRequest
	reply   *inventoryv1.ScoreReplenishmentPlanResponse
}

func (s *StockReservationQueryServer) runLookupStockReservationScoreReplenishmentPlan(ctx context.Context, step *stockReservationQueryServerLookupStockReservationScoreReplenishmentPlanStep) error {
	reply, err := s.replenishmentPlanValidationClient.ScoreReplenishmentPlan(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// stockReservationQueryServerEstimateParcelSteps are the ways to run
// acme.shipping.v1.ParcelCalculationService/EstimateParcel, by name.
var stockReservationQueryServerEstimateParcelSteps = map[string]func(*StockReservationQueryServer, context.Context, *shippingv1.EstimateParcelRequest) (*shippingv1.EstimateParcelResponse, error){
	"default": (*StockReservationQueryServer).estimateParcelStep,
	"audited": (*StockReservationQueryServer).estimateParcelAuditedStep,
}

func (s *StockReservationQueryServer) estimateParcelStep(ctx context.Context, request *shippingv1.EstimateParcelRequest) (*shippingv1.EstimateParcelResponse, error) {
	return s.parcelCalculationClient.EstimateParcel(ctx, request)
}

func (s *StockReservationQueryServer) estimateParcelAuditedStep(ctx context.Context, request *shippingv1.EstimateParcelRequest) (*shippingv1.EstimateParcelResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.shipping.v1.ParcelCalculationService/EstimateParcel")
	return s.parcelCalculationClient.EstimateParcel(ctx, request)
}

// stockReservationQueryServerSummarizeStockReservationsLintRestockDecisionsStep carries the request of one step of
// SummarizeStockReservations and, once it ran, its reply.
type stockReservationQueryServerSummarizeStockReservationsLintRestockDecisionsStep struct {
	request *returnsv1.LintRestockDecisionsRequest
	reply   *returnsv1.LintRestockDecisionsResponse
}

func (s *StockReservationQueryServer) runSummarizeStockReservationsLintRestockDecisions(ctx context.Context, step *stockReservationQueryServerSummarizeStockReservationsLintRestockDecisionsStep) error {
	reply, err := s.restockDecisionValidationClient.LintRestockDecisions(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
