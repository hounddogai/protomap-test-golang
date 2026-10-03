package partnerapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	datahubv1 "github.com/hounddogai/protomap-test-golang/gen/acme/datahub/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fanportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fanportal/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	telehealthv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telehealth/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// PartnerWebhookValidationServer implements `acme.partnerapi.v1.PartnerWebhookValidationService`.
// Checks partner webhooks against the rules before anything acts on them.
type PartnerWebhookValidationServer struct {
	partnerapiv1.UnimplementedPartnerWebhookValidationServiceServer
	store                                 *Store
	log                                   *slog.Logger
	watchlistApprovalClient               fraudv1.WatchlistApprovalServiceClient
	fanSessionPort                        fanSessionPort
	documentQueryClient                   documentsv1.DocumentQueryServiceClient
	submitSellerListingFn                 func(context.Context, *sellersv1.SubmitSellerListingRequest, ...grpc.CallOption) (*sellersv1.SubmitSellerListingResponse, error)
	searchIndexQueryClient                searchv1.SearchIndexQueryServiceClient
	clients                               *registry.Registry
	usageRecordApprovalClient             subscriptionsv1.UsageRecordApprovalServiceClient
	workspaceQueryClient                  datahubv1.WorkspaceQueryServiceClient
	parcelWorkflowClient                  shippingv1.ParcelWorkflowServiceClient
	ledgerAccountQueryClient              ledgerv1.LedgerAccountQueryServiceClient
	disputeReportingClient                paymentsv1.DisputeReportingServiceClient
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	sellerApprovalClient                  sellersv1.SellerApprovalServiceClient
	accessPolicyQueryClient               authorizationv1.AccessPolicyQueryServiceClient
	productVariantWorkflowClient          catalogv1.ProductVariantWorkflowServiceClient
	workspaceClient                       datahubv1.WorkspaceServiceClient
	meteredFanSessionQuery                meteredFanSessionQuery
	sellerListingApprovalClient           sellersv1.SellerListingApprovalServiceClient
}

// NewPartnerWebhookValidationServer wires a PartnerWebhookValidationServer.
func NewPartnerWebhookValidationServer(deps platform.Deps) *PartnerWebhookValidationServer {
	sellerListingApproval := sellersv1.NewSellerListingApprovalServiceClient(deps.Conn)
	return &PartnerWebhookValidationServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.partnerapi.v1.PartnerWebhookValidationService"),
		watchlistApprovalClient:               fraudv1.NewWatchlistApprovalServiceClient(deps.Conn),
		fanSessionPort:                        newFanSessionPort(deps),
		documentQueryClient:                   documentsv1.NewDocumentQueryServiceClient(deps.Conn),
		submitSellerListingFn:                 sellerListingApproval.SubmitSellerListing,
		searchIndexQueryClient:                searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
		clients:                               deps.Clients,
		usageRecordApprovalClient:             subscriptionsv1.NewUsageRecordApprovalServiceClient(deps.Conn),
		workspaceQueryClient:                  datahubv1.NewWorkspaceQueryServiceClient(deps.Conn),
		parcelWorkflowClient:                  shippingv1.NewParcelWorkflowServiceClient(deps.Conn),
		ledgerAccountQueryClient:              ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		disputeReportingClient:                paymentsv1.NewDisputeReportingServiceClient(deps.Conn),
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		sellerApprovalClient:                  sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		accessPolicyQueryClient:               authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn),
		productVariantWorkflowClient:          catalogv1.NewProductVariantWorkflowServiceClient(deps.Conn),
		workspaceClient:                       datahubv1.NewWorkspaceServiceClient(deps.Conn),
		meteredFanSessionQuery:                meteredFanSessionQuery{FanSessionQueryServiceClient: fanportalv1.NewFanSessionQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerListingApprovalClient:           sellersv1.NewSellerListingApprovalServiceClient(deps.Conn),
	}
}

// ValidatePartnerWebhook validates a partner webhook.
func (s *PartnerWebhookValidationServer) ValidatePartnerWebhook(ctx context.Context, req *partnerapiv1.ValidatePartnerWebhookRequest) (*partnerapiv1.ValidatePartnerWebhookResponse, error) {
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "validate partner webhook", "tenant", req.GetContext().GetTenantId())
	jobs := make(chan *partnerapiv1.PartnerWebhookMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.watchlistApprovalClient.ListPendingWatchlists(workCtx, &fraudv1.ListPendingWatchlistsRequest{
					Context: req.GetContext(),
					Filter:  &fraudv1.ListPendingWatchlistsRequest_Filter{CreatedAfter: measurement.GetObservedAt()},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPartnerWebhook().GetMeasurements() {
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
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistApprovalService/ListPendingWatchlists")
	}
	source := req.GetDetails().GetContact().ProtoReflect()
	phone := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("phone"))).String()
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	createFanSessionRequest := &fanportalv1.CreateFanSessionRequest{
		Context: req.GetContext(),
		FanSession: &fanportalv1.FanSession{
			Assignee:     &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
			Requester:    &commonv1.ContactInfo{Phone: phone},
			FanSessionId: envelope.GetDisplayName(),
		},
	}
	createFanSessionReply, err := s.fanSessionPort.CreateFanSession(ctx, createFanSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fanportal.v1.FanSessionService/CreateFanSession")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	browseDocumentsRequest := &documentsv1.BrowseDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.BrowseDocumentsRequest_Filter{
			CreatedAfter: createFanSessionReply.GetFanSession().GetAudit().GetCreatedAt(),
			OwnerId:      createFanSessionReply.GetFanSession().GetSla().GetPolicyId(),
			Labels:       createFanSessionReply.GetFanSession().GetCustomFields(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	}
	browseDocumentsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*documentsv1.BrowseDocumentsResponse, error) {
		return s.documentQueryClient.BrowseDocuments(ctx, browseDocumentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/BrowseDocuments")
	}
	_ = browseDocumentsReply
	return &partnerapiv1.ValidatePartnerWebhookResponse{
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId:    createFanSessionReply.GetResult().GetCorrelationId(),
			PartnerWebhookId: req.GetPartnerWebhookId(),
			Sequence:         req.GetPartnerWebhook().GetSequence(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: createFanSessionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// PreviewPartnerWebhook previews a partner webhook.
func (s *PartnerWebhookValidationServer) PreviewPartnerWebhook(ctx context.Context, req *partnerapiv1.PreviewPartnerWebhookRequest) (*partnerapiv1.PreviewPartnerWebhookResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "preview partner webhook", "tenant", req.GetContext().GetTenantId())
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	submitSellerListingRequest := &sellersv1.SubmitSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetContact().GetEmail(),
				DisplayName: req.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
		},
		SellerListingId: envelope.GetDisplayName(),
	}
	submitSellerListingReply, err := s.submitSellerListingFn(ctx, submitSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/SubmitSellerListing")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	browseSearchIndexesRequest := &searchv1.BrowseSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.BrowseSearchIndexesRequest_Filter{
			Region:       submitSellerListingReply.GetSellerListing().GetAddress().GetRegion(),
			CreatedAfter: submitSellerListingReply.GetSellerListing().GetAudit().GetCreatedAt(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	}
	type browseSearchIndexesResult struct {
		reply *searchv1.BrowseSearchIndexesResponse
		err   error
	}
	browseSearchIndexesResults := make(chan browseSearchIndexesResult, 1)
	go func() {
		reply, err := s.searchIndexQueryClient.BrowseSearchIndexes(ctx, browseSearchIndexesRequest)
		browseSearchIndexesResults <- browseSearchIndexesResult{reply, err}
	}()
	var browseSearchIndexesReply *searchv1.BrowseSearchIndexesResponse
	select {
	case result := <-browseSearchIndexesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.search.v1.SearchIndexQueryService/BrowseSearchIndexes")
		}
		browseSearchIndexesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	videoVisitQuery, err := registry.Lookup[telehealthv1.VideoVisitQueryServiceClient](s.clients, "acme.telehealth.v1.VideoVisitQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resolveVideoVisitsRequest := &telehealthv1.ResolveVideoVisitsRequest{Context: req.GetContext()}
	resolveVideoVisitsReply, err := videoVisitQuery.ResolveVideoVisits(ctx, resolveVideoVisitsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telehealth.v1.VideoVisitQueryService/ResolveVideoVisits")
	}
	_ = browseSearchIndexesReply
	_ = resolveVideoVisitsReply
	return &partnerapiv1.PreviewPartnerWebhookResponse{
		Result: &commonv1.OperationResult{
			Details: submitSellerListingReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &partnerapiv1.PreviewPartnerWebhookResponse_Outcome{Status: submitSellerListingReply.GetResult().GetStatus()},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			PartnerWebhookId: req.GetPartnerWebhookId(),
			Sequence:         req.GetPartnerWebhook().GetSequence(),
		},
	}, nil
}

// NormalizePartnerWebhook normalizes a partner webhook.
func (s *PartnerWebhookValidationServer) NormalizePartnerWebhook(ctx context.Context, req *partnerapiv1.NormalizePartnerWebhookRequest) (*partnerapiv1.NormalizePartnerWebhookResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "normalize partner webhook", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetPartnerWebhook())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.PartnerWebhook
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listPendingUsageRecordsRequest := &subscriptionsv1.ListPendingUsageRecordsRequest{
		Context: req.GetContext(),
		Filter:  &subscriptionsv1.ListPendingUsageRecordsRequest_Filter{OwnerId: (&decoded).GetSubject().GetPartnerIntegrationId()},
		Page:    &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	send := s.listPendingUsageRecordsSender(listPendingUsageRecordsRequest)
	listPendingUsageRecordsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordApprovalService/ListPendingUsageRecords")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	countWorkspacesReply, err := s.workspaceQueryClient.CountWorkspaces(ctx, &datahubv1.CountWorkspacesRequest{
		Context: req.GetContext(),
		Filter:  &datahubv1.CountWorkspacesRequest_Filter{CreatedAfter: req.GetPartnerWebhook().GetOccurredAt()},
		Page:    &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.datahub.v1.WorkspaceQueryService/CountWorkspaces")
	}
	var envelope3 partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	retryParcelWorkflowRequest := &shippingv1.RetryParcelWorkflowRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
		},
		ParcelId: envelope3.GetDisplayName(),
	}
	send2 := s.retryParcelWorkflowSender(retryParcelWorkflowRequest)
	retryParcelWorkflowReply, err := send2(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelWorkflowService/RetryParcelWorkflow")
	}
	exportLedgerAccountsStream, err := s.ledgerAccountQueryClient.ExportLedgerAccounts(ctx, &ledgerv1.ExportLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.ExportLedgerAccountsRequest_Filter{
			Region:       retryParcelWorkflowReply.GetParcel().GetAddress().GetRegion(),
			CreatedAfter: retryParcelWorkflowReply.GetParcel().GetAudit().GetCreatedAt(),
			OwnerId:      retryParcelWorkflowReply.GetParcel().GetOwner().GetUserId(),
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
	return &partnerapiv1.NormalizePartnerWebhookResponse{
		Outcome: &partnerapiv1.NormalizePartnerWebhookResponse_Outcome{
			Status:      listPendingUsageRecordsReply.GetResult().GetStatus(),
			EffectiveAt: retryParcelWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Status:  listPendingUsageRecordsReply.GetResult().GetStatus(),
			Details: countWorkspacesReply.GetResult().GetDetails(),
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId:    countWorkspacesReply.GetResult().GetCorrelationId(),
			PartnerWebhookId: req.GetPartnerWebhookId(),
			OccurredAt:       req.GetPartnerWebhook().GetOccurredAt(),
		},
	}, nil
}

// ExplainPartnerWebhookDecision explains a partner webhook.
func (s *PartnerWebhookValidationServer) ExplainPartnerWebhookDecision(ctx context.Context, req *partnerapiv1.ExplainPartnerWebhookDecisionRequest) (*partnerapiv1.ExplainPartnerWebhookDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainPartnerWebhookDecision", "error", err)
		return nil, err
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "explain partner webhook decision", "tenant", req.GetContext().GetTenantId())
	accessPolicyQuery, err := registry.Lookup[authorizationv1.AccessPolicyQueryServiceClient](s.clients, "acme.authorization.v1.AccessPolicyQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resolveAccessPoliciesRequest := &authorizationv1.ResolveAccessPoliciesRequest{Context: req.GetContext()}
	resolveAccessPoliciesReply, err := accessPolicyQuery.ResolveAccessPolicies(ctx, resolveAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/ResolveAccessPolicies")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	attributes := make(map[string]string, len(req.GetPartnerWebhook().GetAttributes()))
	for key2, value2 := range req.GetPartnerWebhook().GetAttributes() {
		attributes[key2] = value2
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scheduleDisputeReportRequest := &paymentsv1.ScheduleDisputeReportRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &paymentsv1.ScheduleDisputeReportRequest_Details{Filters: attributes},
		DisputeId:   envelope.GetDisplayName(),
	}
	scheduleDisputeReportReply, err := grpcx.Call(ctx, s.disputeReportingClient.ScheduleDisputeReport, scheduleDisputeReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeReportingService/ScheduleDisputeReport")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainSellerPayoutScheduleCalculationRequest := &sellersv1.ExplainSellerPayoutScheduleCalculationRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
			}},
		},
		SellerPayoutScheduleId: envelope2.GetDisplayName(),
	}
	explainSellerPayoutScheduleCalculationReply, err := sellersv1.SellerPayoutScheduleCalculationServiceClient.ExplainSellerPayoutScheduleCalculation(s.sellerPayoutScheduleCalculationClient, ctx, explainSellerPayoutScheduleCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/ExplainSellerPayoutScheduleCalculation")
	}
	return &partnerapiv1.ExplainPartnerWebhookDecisionResponse{
		Result: &commonv1.OperationResult{
			Message: resolveAccessPoliciesReply.GetResult().GetMessage(),
			Status:  explainSellerPayoutScheduleCalculationReply.GetResult().GetStatus(),
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId:    resolveAccessPoliciesReply.GetResult().GetCorrelationId(),
			PartnerWebhookId: req.GetPartnerWebhookId(),
			Subject:          req.GetPartnerWebhook().GetSubject(),
		},
		Outcome: &partnerapiv1.ExplainPartnerWebhookDecisionResponse_Outcome{
			Message: scheduleDisputeReportReply.GetResult().GetMessage(),
		},
	}, nil
}

// ScorePartnerWebhook scores a partner webhook.
func (s *PartnerWebhookValidationServer) ScorePartnerWebhook(ctx context.Context, req *partnerapiv1.ScorePartnerWebhookRequest) (*partnerapiv1.ScorePartnerWebhookResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "score partner webhook", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetPartnerWebhook()).(*partnerapiv1.PartnerWebhook)
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	delegateSellerApprovalRequest := &sellersv1.DelegateSellerApprovalRequest{
		Context: req.GetContext(),
		Details: &sellersv1.DelegateSellerApprovalRequest_Details{
			Approver: &identityv1.UserRef{DisplayName: cloned.GetSubject().GetDisplayName()},
		},
		SellerId: envelope.GetDisplayName(),
	}
	send := s.delegateSellerApprovalSender(delegateSellerApprovalRequest)
	delegateSellerApprovalReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/DelegateSellerApproval")
	}
	for measurement := range iterx.Each(req.GetPartnerWebhook().GetMeasurements(), func(candidate *partnerapiv1.PartnerWebhookMeasurement) bool { return candidate != nil }) {
		if _, err := s.accessPolicyQueryClient.BrowseAccessPolicies(ctx, &authorizationv1.BrowseAccessPoliciesRequest{
			Context: req.GetContext(),
			Filter: &authorizationv1.BrowseAccessPoliciesRequest_Filter{
				Region:       delegateSellerApprovalReply.GetSeller().GetPrimaryAddress().GetRegion(),
				CreatedAfter: measurement.GetObservedAt(),
				OwnerId:      delegateSellerApprovalReply.GetSeller().GetRelationshipManager().GetUserId(),
				Labels:       req.GetPartnerWebhook().GetAttributes(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/BrowseAccessPolicies")
		}
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := partnerWebhookValidationServerAdvanceProductVariantWorkflowSteps[mode]
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	advanceProductVariantWorkflowRequest := &catalogv1.AdvanceProductVariantWorkflowRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Locator: &catalogv1.ProductVariant_Address{Address: &commonv1.PostalAddress{
				Recipient: delegateSellerApprovalReply.GetSeller().GetPrimaryAddress().GetRecipient(),
			}},
		},
		ProductVariantId: envelope2.GetDisplayName(),
	}
	advanceProductVariantWorkflowReply, err := run(s, ctx, advanceProductVariantWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantWorkflowService/AdvanceProductVariantWorkflow")
	}
	return &partnerapiv1.ScorePartnerWebhookResponse{
		Result: &commonv1.OperationResult{
			Status:  delegateSellerApprovalReply.GetResult().GetStatus(),
			Message: advanceProductVariantWorkflowReply.GetOutcome().GetMessage(),
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			PartnerWebhookId: req.GetPartnerWebhookId(),
			CorrelationId:    req.GetPartnerWebhook().GetCorrelationId(),
		},
	}, nil
}

// LintPartnerWebhooks lints a partner webhook.
func (s *PartnerWebhookValidationServer) LintPartnerWebhooks(ctx context.Context, req *partnerapiv1.LintPartnerWebhooksRequest) (*partnerapiv1.LintPartnerWebhooksResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintPartnerWebhooks", "error", err)
		return nil, err
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "lint partner webhooks", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *partnerapiv1.LintPartnerWebhooksRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	encoded, err := protojson.Marshal(req.GetPartnerWebhook())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.PartnerWebhook
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	cloneWorkspaceRequest := &datahubv1.CloneWorkspaceRequest{}
	cloneWorkspaceRequest.Context = req.GetContext()
	cloneWorkspaceRequest.Workspace = &datahubv1.Workspace{
		Assignee: &identityv1.UserRef{
			Email:       cachedDetails.GetContact().GetEmail(),
			DisplayName: (&decoded).GetSubject().GetDisplayName(),
		},
		Requester: &commonv1.ContactInfo{
			Name:             req.GetDetails().GetContact().GetName(),
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
		},
		WorkspaceId: envelope.GetDisplayName(),
	}
	cloneWorkspaceReply, err := s.workspaceClient.CloneWorkspace(ctx, cloneWorkspaceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.datahub.v1.WorkspaceService/CloneWorkspace")
	}
	countFanSessionsRequest := &fanportalv1.CountFanSessionsRequest{
		Context: req.GetContext(),
		Filter: &fanportalv1.CountFanSessionsRequest_Filter{
			CreatedAfter: cloneWorkspaceReply.GetWorkspace().GetAudit().GetCreatedAt(),
		},
	}
	countFanSessionsReply, err := s.meteredFanSessionQuery.CountFanSessions(ctx, countFanSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fanportal.v1.FanSessionQueryService/CountFanSessions")
	}
	return &partnerapiv1.LintPartnerWebhooksResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: cloneWorkspaceReply.GetResult().GetReferenceIds(),
			Status:       countFanSessionsReply.GetResult().GetStatus(),
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			PartnerWebhookId: req.GetPartnerWebhookId(),
			CorrelationId:    req.GetPartnerWebhook().GetCorrelationId(),
		},
	}, nil
}

// GetPartnerWebhookRules returns a partner webhook.
func (s *PartnerWebhookValidationServer) GetPartnerWebhookRules(ctx context.Context, req *partnerapiv1.GetPartnerWebhookRulesRequest) (*partnerapiv1.GetPartnerWebhookRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetPartnerWebhookRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "get_partner_webhook_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get partner webhook rules", "tenant", req.GetContext().GetTenantId())
	resolveLedgerAccountsRequest := &ledgerv1.ResolveLedgerAccountsRequest{}
	resolveLedgerAccountsRequest.Context = req.GetContext()
	resolveLedgerAccountsReply, err := s.ledgerAccountQueryClient.ResolveLedgerAccounts(ctx, resolveLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/ResolveLedgerAccounts")
	}
	recallSellerListingReply, err := s.sellerListingApprovalClient.RecallSellerListing(ctx, &sellersv1.RecallSellerListingRequest{
		Context: req.GetContext(),
		Details: &sellersv1.RecallSellerListingRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/RecallSellerListing")
	}
	return &partnerapiv1.GetPartnerWebhookRulesResponse{
		Result: &commonv1.OperationResult{
			Message:       resolveLedgerAccountsReply.GetResult().GetMessage(),
			CorrelationId: recallSellerListingReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{PartnerWebhookId: req.GetPartnerWebhookId()},
	}, nil
}

// listPendingUsageRecordsSender returns a function that sends one request to
// acme.subscriptions.v1.UsageRecordApprovalService/ListPendingUsageRecords.
func (s *PartnerWebhookValidationServer) listPendingUsageRecordsSender(request *subscriptionsv1.ListPendingUsageRecordsRequest) func(context.Context) (*subscriptionsv1.ListPendingUsageRecordsResponse, error) {
	return func(ctx context.Context) (*subscriptionsv1.ListPendingUsageRecordsResponse, error) {
		return s.usageRecordApprovalClient.ListPendingUsageRecords(ctx, request)
	}
}

// retryParcelWorkflowSender returns a function that sends one request to
// acme.shipping.v1.ParcelWorkflowService/RetryParcelWorkflow.
func (s *PartnerWebhookValidationServer) retryParcelWorkflowSender(request *shippingv1.RetryParcelWorkflowRequest) func(context.Context) (*shippingv1.RetryParcelWorkflowResponse, error) {
	return func(ctx context.Context) (*shippingv1.RetryParcelWorkflowResponse, error) {
		return s.parcelWorkflowClient.RetryParcelWorkflow(ctx, request)
	}
}

// delegateSellerApprovalSender returns a function that sends one request to
// acme.sellers.v1.SellerApprovalService/DelegateSellerApproval.
func (s *PartnerWebhookValidationServer) delegateSellerApprovalSender(request *sellersv1.DelegateSellerApprovalRequest) func(context.Context) (*sellersv1.DelegateSellerApprovalResponse, error) {
	return func(ctx context.Context) (*sellersv1.DelegateSellerApprovalResponse, error) {
		return s.sellerApprovalClient.DelegateSellerApproval(ctx, request)
	}
}

// partnerWebhookValidationServerAdvanceProductVariantWorkflowSteps are the ways to run
// acme.catalog.v1.ProductVariantWorkflowService/AdvanceProductVariantWorkflow, by name.
var partnerWebhookValidationServerAdvanceProductVariantWorkflowSteps = map[string]func(*PartnerWebhookValidationServer, context.Context, *catalogv1.AdvanceProductVariantWorkflowRequest) (*catalogv1.AdvanceProductVariantWorkflowResponse, error){
	"default": (*PartnerWebhookValidationServer).advanceProductVariantWorkflowStep,
	"audited": (*PartnerWebhookValidationServer).advanceProductVariantWorkflowAuditedStep,
}

func (s *PartnerWebhookValidationServer) advanceProductVariantWorkflowStep(ctx context.Context, request *catalogv1.AdvanceProductVariantWorkflowRequest) (*catalogv1.AdvanceProductVariantWorkflowResponse, error) {
	return s.productVariantWorkflowClient.AdvanceProductVariantWorkflow(ctx, request)
}

func (s *PartnerWebhookValidationServer) advanceProductVariantWorkflowAuditedStep(ctx context.Context, request *catalogv1.AdvanceProductVariantWorkflowRequest) (*catalogv1.AdvanceProductVariantWorkflowResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.catalog.v1.ProductVariantWorkflowService/AdvanceProductVariantWorkflow")
	return s.productVariantWorkflowClient.AdvanceProductVariantWorkflow(ctx, request)
}
