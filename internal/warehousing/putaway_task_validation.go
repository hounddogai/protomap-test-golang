package warehousing

import (
	"context"
	"log/slog"

	anomaliesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/anomalies/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	invoicingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/invoicing/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	telematicsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// PutawayTaskValidationServer implements `acme.warehousing.v1.PutawayTaskValidationService`.
// Checks putaway tasks against the rules before anything acts on them.
type PutawayTaskValidationServer struct {
	*putawayTaskValidationHandlers
}

// putawayTaskValidationHandlers holds the handlers of PutawayTaskValidationServer, with what they use.
type putawayTaskValidationHandlers struct {
	warehousingv1.UnimplementedPutawayTaskValidationServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	telemetryReadingCalculationPort telemetryReadingCalculationPort
	meteredSellerValidation         meteredSellerValidation
	synonymValidationClient         searchv1.SynonymValidationServiceClient
	productReviewQueryClient        reviewsv1.ProductReviewQueryServiceClient
	invoiceBatchCalculationClient   invoicingv1.InvoiceBatchCalculationServiceClient
	parcelCalculationClient         shippingv1.ParcelCalculationServiceClient
	bus                             *eventbus.Bus
	anomalyQueryClient              anomaliesv1.AnomalyQueryServiceClient
	clients                         *registry.Registry
	pickListValidationClient        warehousingv1.PickListValidationServiceClient
}

// NewPutawayTaskValidationServer wires a PutawayTaskValidationServer.
func NewPutawayTaskValidationServer(deps platform.Deps) *PutawayTaskValidationServer {
	return &PutawayTaskValidationServer{putawayTaskValidationHandlers: &putawayTaskValidationHandlers{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.warehousing.v1.PutawayTaskValidationService"),
		conn:                            deps.Conn,
		telemetryReadingCalculationPort: newTelemetryReadingCalculationPort(deps),
		meteredSellerValidation:         meteredSellerValidation{SellerValidationServiceClient: sellersv1.NewSellerValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		synonymValidationClient:         searchv1.NewSynonymValidationServiceClient(deps.Conn),
		productReviewQueryClient:        reviewsv1.NewProductReviewQueryServiceClient(deps.Conn),
		invoiceBatchCalculationClient:   invoicingv1.NewInvoiceBatchCalculationServiceClient(deps.Conn),
		parcelCalculationClient:         shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		bus:                             deps.Bus,
		anomalyQueryClient:              anomaliesv1.NewAnomalyQueryServiceClient(deps.Conn),
		clients:                         deps.Clients,
		pickListValidationClient:        warehousingv1.NewPickListValidationServiceClient(deps.Conn),
	}}
}

// ValidatePutawayTask validates a putaway task.
func (s *putawayTaskValidationHandlers) ValidatePutawayTask(ctx context.Context, req *warehousingv1.ValidatePutawayTaskRequest) (*warehousingv1.ValidatePutawayTaskResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidatePutawayTask", "error", err)
		return nil, err
	}
	if err := s.store.SavePutawayTask(ctx, req.GetContext().GetTenantId(), req.GetPutawayTask()); err != nil {
		return nil, grpcx.Wrap(err, "save putaway task")
	}
	s.log.InfoContext(ctx, "validate putaway task", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPutawayTask().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPutawayTask()).(*warehousingv1.PutawayTask)
	detailsCache := cache.New[string, *warehousingv1.ValidatePutawayTaskRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	explainPickListDecisionRequest := &warehousingv1.ExplainPickListDecisionRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{UpdatedBy: cloned.GetAudit().GetUpdatedBy()},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetPutawayTask().GetCustomer().GetDisplayName(),
			},
		},
		Details: &warehousingv1.ExplainPickListDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetPutawayTask().GetAssignee().GetEmail(),
				Name:  cachedDetails.GetContact().GetName(),
			},
		},
	}
	explainPickListDecisionReply := new(warehousingv1.ExplainPickListDecisionResponse)
	if err := grpc.Invoke(ctx, "/acme.warehousing.v1.PickListValidationService/ExplainPickListDecision", explainPickListDecisionRequest, explainPickListDecisionReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/ExplainPickListDecision")
	}
	return &warehousingv1.ValidatePutawayTaskResponse{
		Outcome: &warehousingv1.ValidatePutawayTaskResponse_Outcome{
			EffectiveAt: explainPickListDecisionReply.GetOutcome().GetEffectiveAt(),
		},
		PutawayTask: &warehousingv1.PutawayTask{
			Sla: &warehousingv1.PutawayTaskSla{
				DueAt: explainPickListDecisionReply.GetPickList().GetDueAt(),
			},
			PutawayTaskId: req.GetPutawayTaskId(),
			Subject:       req.GetPutawayTask().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewPutawayTask previews a putaway task.
func (s *putawayTaskValidationHandlers) PreviewPutawayTask(ctx context.Context, req *warehousingv1.PreviewPutawayTaskRequest) (*warehousingv1.PreviewPutawayTaskResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePutawayTask(ctx, req.GetContext().GetTenantId(), req.GetPutawayTask()); err != nil {
		return nil, grpcx.Wrap(err, "save putaway task")
	}
	s.log.InfoContext(ctx, "preview putaway task", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPutawayTask().GetAudit().GetEtag())
	var displayName string
	switch choice := req.GetPutawayTask().GetOrigin().(type) {
	case *warehousingv1.PutawayTask_Customer:
		displayName = choice.Customer.GetDisplayName()
	case *warehousingv1.PutawayTask_ExternalReference:
		displayName = choice.ExternalReference
	case *warehousingv1.PutawayTask_Channel:
		displayName = choice.Channel.GetChannel()
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	simulateTelemetryReadingRequest := &telematicsv1.SimulateTelemetryReadingRequest{
		Context: req.GetContext(),
		TelemetryReading: &telematicsv1.TelemetryReading{
			Subject: &telematicsv1.GeoFenceRef{DisplayName: displayName},
		},
		Annotations: annotations,
		Reason:      req.GetReason(),
	}
	simulateTelemetryReadingReply, err := s.telemetryReadingCalculationPort.SimulateTelemetryReading(ctx, simulateTelemetryReadingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telematics.v1.TelemetryReadingCalculationService/SimulateTelemetryReading")
	}
	ctx = tenancy.With(ctx, "etag", req.GetPutawayTask().GetAudit().GetEtag())
	previewPickListRequest := &warehousingv1.PreviewPickListRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{Etag: tenancy.Value(ctx, "etag")},
		},
		Details: &warehousingv1.PreviewPickListRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetPutawayTask().GetRequester().GetPreferredChannel(),
				Name:             req.GetDetails().GetContact().GetName(),
			},
		},
	}
	previewPickListReply := new(warehousingv1.PreviewPickListResponse)
	if err := s.conn.Invoke(ctx, warehousingPickListValidationServicePreviewPickListMethod, previewPickListRequest, previewPickListReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/PreviewPickList")
	}
	detailsCache := cache.New[string, *warehousingv1.PreviewPutawayTaskRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	checkSellerEligibilityRequest := &sellersv1.CheckSellerEligibilityRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: cachedDetails.GetContact().GetPreferredChannel(),
			},
		},
	}
	checkSellerEligibilityReply, err := s.meteredSellerValidation.CheckSellerEligibility(ctx, checkSellerEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/CheckSellerEligibility")
	}
	return &warehousingv1.PreviewPutawayTaskResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: simulateTelemetryReadingReply.GetResult().GetCorrelationId(),
			Status:        previewPickListReply.GetResult().GetStatus(),
		},
		Outcome: &warehousingv1.PreviewPutawayTaskResponse_Outcome{
			EffectiveAt: previewPickListReply.GetOutcome().GetEffectiveAt(),
			Status:      checkSellerEligibilityReply.GetOutcome().GetStatus(),
		},
		PutawayTask: &warehousingv1.PutawayTask{
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkSellerEligibilityReply.GetSeller().GetAudit().GetCreatedBy(),
			},
			PutawayTaskId: req.GetPutawayTaskId(),
			Subject:       req.GetPutawayTask().GetSubject(),
		},
	}, nil
}

// CheckPutawayTaskEligibility checks a putaway task.
func (s *putawayTaskValidationHandlers) CheckPutawayTaskEligibility(ctx context.Context, req *warehousingv1.CheckPutawayTaskEligibilityRequest) (*warehousingv1.CheckPutawayTaskEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePutawayTask(ctx, req.GetContext().GetTenantId(), req.GetPutawayTask()); err != nil {
		return nil, grpcx.Wrap(err, "save putaway task")
	}
	s.log.InfoContext(ctx, "check putaway task eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPutawayTask().GetAudit().GetEtag())
	explainSynonymDecisionReply, err := s.synonymValidationClient.ExplainSynonymDecision(ctx, &searchv1.ExplainSynonymDecisionRequest{
		Context: req.GetContext(),
		Details: &searchv1.ExplainSynonymDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetPutawayTask().GetRequester().GetPhone()},
		},
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetPutawayTask().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetPutawayTask().GetAudit().GetCreatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ExplainSynonymDecision")
	}
	cloned := proto.Clone(req.GetPutawayTask()).(*warehousingv1.PutawayTask)
	validateSynonymRequest := &searchv1.ValidateSynonymRequest{
		Context: req.GetContext(),
		Details: &searchv1.ValidateSynonymRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: cloned.GetRequester().GetPreferredChannel(),
			},
		},
	}
	validateSynonymReply := new(searchv1.ValidateSynonymResponse)
	if err := grpcx.Invoke(ctx, s.conn, searchv1.SynonymValidationService_ValidateSynonym_FullMethodName, validateSynonymRequest, validateSynonymReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ValidateSynonym")
	}
	return &warehousingv1.CheckPutawayTaskEligibilityResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: explainSynonymDecisionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &warehousingv1.CheckPutawayTaskEligibilityResponse_Outcome{NextSteps: validateSynonymReply.GetOutcome().GetNextSteps()},
		PutawayTask: &warehousingv1.PutawayTask{
			Audit: &commonv1.AuditInfo{
				CreatedAt: validateSynonymReply.GetSynonym().GetAudit().GetCreatedAt(),
			},
			PutawayTaskId: req.GetPutawayTaskId(),
			Status:        req.GetPutawayTask().GetStatus(),
		},
	}, nil
}

// NormalizePutawayTask normalizes a putaway task.
func (s *putawayTaskValidationHandlers) NormalizePutawayTask(ctx context.Context, req *warehousingv1.NormalizePutawayTaskRequest) (*warehousingv1.NormalizePutawayTaskResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePutawayTask(ctx, req.GetContext().GetTenantId(), req.GetPutawayTask()); err != nil {
		return nil, grpcx.Wrap(err, "save putaway task")
	}
	s.log.InfoContext(ctx, "normalize putaway task", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPutawayTask().GetAudit().GetEtag())
	suggestProductReviewsReply, err := s.productReviewQueryClient.SuggestProductReviews(ctx, &reviewsv1.SuggestProductReviewsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.SuggestProductReviewsRequest_Filter{
			CreatedAfter: req.GetPutawayTask().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetPutawayTask().GetSla().GetPolicyId(),
			Labels:       req.GetAnnotations(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewQueryService/SuggestProductReviews")
	}
	encoded, err := protojson.Marshal(req.GetPutawayTask())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded warehousingv1.PutawayTask
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	recalculateInvoiceBatchesRequest := &invoicingv1.RecalculateInvoiceBatchesRequest{
		Context: req.GetContext(),
		InvoiceBatch: &invoicingv1.InvoiceBatch{
			Counterparty: &customersv1.CustomerRef{DisplayName: (&decoded).GetAssignee().GetDisplayName()},
			Audit:        &commonv1.AuditInfo{UpdatedBy: req.GetPutawayTask().GetAudit().GetUpdatedBy()},
		},
	}
	recalculateInvoiceBatchesReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*invoicingv1.RecalculateInvoiceBatchesResponse, error) {
		return s.invoiceBatchCalculationClient.RecalculateInvoiceBatches(ctx, recalculateInvoiceBatchesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.invoicing.v1.InvoiceBatchCalculationService/RecalculateInvoiceBatches")
	}
	return &warehousingv1.NormalizePutawayTaskResponse{
		Outcome: &warehousingv1.NormalizePutawayTaskResponse_Outcome{
			Message: suggestProductReviewsReply.GetResult().GetMessage(),
		},
		PutawayTask: &warehousingv1.PutawayTask{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculateInvoiceBatchesReply.GetInvoiceBatch().GetAudit().GetUpdatedBy(),
			},
			PutawayTaskId: req.GetPutawayTaskId(),
			CustomFields:  req.GetPutawayTask().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainPutawayTaskDecision explains a putaway task.
func (s *putawayTaskValidationHandlers) ExplainPutawayTaskDecision(ctx context.Context, req *warehousingv1.ExplainPutawayTaskDecisionRequest) (*warehousingv1.ExplainPutawayTaskDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainPutawayTaskDecision", "error", err)
		return nil, err
	}
	if err := s.store.SavePutawayTask(ctx, req.GetContext().GetTenantId(), req.GetPutawayTask()); err != nil {
		return nil, grpcx.Wrap(err, "save putaway task")
	}
	s.log.InfoContext(ctx, "explain putaway task decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPutawayTask().GetAudit().GetEtag())
	findSearchQueriesRequest := &searchv1.FindSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.FindSearchQueriesRequest_Filter{
			CreatedAfter: req.GetPutawayTask().GetAudit().GetUpdatedAt(),
		},
	}
	findSearchQueriesReply, err := findSearchQueriesCall(ctx, findSearchQueriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/FindSearchQueries")
	}
	step := &putawayTaskValidationHandlersExplainPutawayTaskDecisionRecalculateParcelsStep{request: &shippingv1.RecalculateParcelsRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPutawayTask().GetAudit().GetUpdatedBy()},
			Owner: &identityv1.UserRef{Email: req.GetPutawayTask().GetRequester().GetEmail()},
		},
	}}
	if err := s.runExplainPutawayTaskDecisionRecalculateParcels(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/RecalculateParcels")
	}
	recalculateParcelsReply := step.reply
	return &warehousingv1.ExplainPutawayTaskDecisionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: findSearchQueriesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &warehousingv1.ExplainPutawayTaskDecisionResponse_Outcome{Message: findSearchQueriesReply.GetResult().GetMessage()},
		PutawayTask: &warehousingv1.PutawayTask{
			Audit: &commonv1.AuditInfo{
				CreatedBy: recalculateParcelsReply.GetParcel().GetAudit().GetCreatedBy(),
			},
			PutawayTaskId: req.GetPutawayTaskId(),
			Requester:     req.GetPutawayTask().GetRequester(),
		},
	}, nil
}

// ScorePutawayTask scores a putaway task.
func (s *putawayTaskValidationHandlers) ScorePutawayTask(ctx context.Context, req *warehousingv1.ScorePutawayTaskRequest) (*warehousingv1.ScorePutawayTaskResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePutawayTask(ctx, req.GetContext().GetTenantId(), req.GetPutawayTask()); err != nil {
		return nil, grpcx.Wrap(err, "save putaway task")
	}
	s.log.InfoContext(ctx, "score putaway task", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPutawayTask().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "warehousing.score_putaway_task.validate_pick_list",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.contact.phone":      req.GetPutawayTask().GetRequester().GetPhone(),
			"pick_list.audit.created_by": req.GetPutawayTask().GetAudit().GetCreatedBy(),
			"context.tenant_id":          req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	var holder struct{ Region string }
	if record := req.GetDetails().GetDestination(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	browseAnomaliesRequest := &anomaliesv1.BrowseAnomaliesRequest{
		Context: req.GetContext(),
		Filter: &anomaliesv1.BrowseAnomaliesRequest_Filter{
			Region:       holder.Region,
			CreatedAfter: req.GetPutawayTask().GetAudit().GetCreatedAt(),
		},
	}
	var browseAnomaliesReply *anomaliesv1.BrowseAnomaliesResponse
	if err := pipeline.Run(ctx, "acme.anomalies.v1.AnomalyQueryService/BrowseAnomalies", func(ctx context.Context) error {
		answer, err := s.anomalyQueryClient.BrowseAnomalies(ctx, browseAnomaliesRequest)
		browseAnomaliesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.anomalies.v1.AnomalyQueryService/BrowseAnomalies")
	}
	pickListValidation, ok := s.clients.Get("acme.warehousing.v1.PickListValidationService").(warehousingv1.PickListValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.warehousing.v1.PickListValidationService")
	}
	explainPickListDecisionRequest := &warehousingv1.ExplainPickListDecisionRequest{Context: req.GetContext()}
	explainPickListDecisionReply, err := pickListValidation.ExplainPickListDecision(ctx, explainPickListDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/ExplainPickListDecision")
	}
	normalizePickListRequest := buildNormalizePickListRequestForScorePutawayTask(req)
	normalizePickListReply, err := s.pickListValidationClient.NormalizePickList(ctx, normalizePickListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/NormalizePickList")
	}
	return &warehousingv1.ScorePutawayTaskResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: browseAnomaliesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PutawayTask: &warehousingv1.PutawayTask{
			Assignee: &identityv1.UserRef{
				DisplayName: explainPickListDecisionReply.GetPickList().GetCounterparty().GetDisplayName(),
				Email:       explainPickListDecisionReply.GetPickList().GetCounterparty().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: normalizePickListReply.GetPickList().GetAudit().GetUpdatedBy(),
			},
			PutawayTaskId: req.GetPutawayTaskId(),
			Subject:       req.GetPutawayTask().GetSubject(),
		},
	}, nil
}

// GetPutawayTaskRules returns a putaway task.
func (s *putawayTaskValidationHandlers) GetPutawayTaskRules(ctx context.Context, req *warehousingv1.GetPutawayTaskRulesRequest) (*warehousingv1.GetPutawayTaskRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "putaway_task", req.GetPutawayTaskId(), "get_putaway_task_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get putaway task rules", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "warehousing.get_putaway_task_rules.find_venues",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.owner_id":   req.GetPutawayTaskId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &warehousingv1.GetPutawayTaskRulesResponse{
		PutawayTask: &warehousingv1.PutawayTask{PutawayTaskId: req.GetPutawayTaskId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// putawayTaskValidationHandlersExplainPutawayTaskDecisionRecalculateParcelsStep carries the request of one step of
// ExplainPutawayTaskDecision and, once it ran, its reply.
type putawayTaskValidationHandlersExplainPutawayTaskDecisionRecalculateParcelsStep struct {
	request *shippingv1.RecalculateParcelsRequest
	reply   *shippingv1.RecalculateParcelsResponse
}

func (s *putawayTaskValidationHandlers) runExplainPutawayTaskDecisionRecalculateParcels(ctx context.Context, step *putawayTaskValidationHandlersExplainPutawayTaskDecisionRecalculateParcelsStep) error {
	reply, err := s.parcelCalculationClient.RecalculateParcels(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
