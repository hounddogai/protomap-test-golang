package cart

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// SavedListApprovalServer implements `acme.cart.v1.SavedListApprovalService`.
// Routes saved lists through approval, with escalation and delegation.
type SavedListApprovalServer struct {
	cartv1.UnimplementedSavedListApprovalServiceServer
	store                         *Store
	log                           *slog.Logger
	cartQueryClient               cartv1.CartQueryServiceClient
	savedListQueryClient          cartv1.SavedListQueryServiceClient
	searchIndexValidationClient   searchv1.SearchIndexValidationServiceClient
	storageBinApprovalClient      warehousingv1.StorageBinApprovalServiceClient
	cartRuleCalculationClient     cartv1.CartRuleCalculationServiceClient
	searchQueryNotificationClient searchv1.SearchQueryNotificationServiceClient
	conn                          *grpc.ClientConn
	savedListCalculationClient    cartv1.SavedListCalculationServiceClient
	cartRuleValidationOnce        sync.Once
	cartRuleValidationCached      cartv1.CartRuleValidationServiceClient
	clients                       *registry.Registry
	cartApprovalClient            cartv1.CartApprovalServiceClient
	cartRuleLifecycleClient       cartv1.CartRuleLifecycleServiceClient
	savedListValidationClient     cartv1.SavedListValidationServiceClient
	cartValidationClient          cartv1.CartValidationServiceClient
}

// NewSavedListApprovalServer wires a SavedListApprovalServer.
func NewSavedListApprovalServer(deps platform.Deps) *SavedListApprovalServer {
	return &SavedListApprovalServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.cart.v1.SavedListApprovalService"),
		cartQueryClient:               cartv1.NewCartQueryServiceClient(deps.Conn),
		savedListQueryClient:          cartv1.NewSavedListQueryServiceClient(deps.Conn),
		searchIndexValidationClient:   searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		storageBinApprovalClient:      warehousingv1.NewStorageBinApprovalServiceClient(deps.Conn),
		cartRuleCalculationClient:     cartv1.NewCartRuleCalculationServiceClient(deps.Conn),
		searchQueryNotificationClient: searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		conn:                          deps.Conn,
		savedListCalculationClient:    cartv1.NewSavedListCalculationServiceClient(deps.Conn),
		clients:                       deps.Clients,
		cartApprovalClient:            cartv1.NewCartApprovalServiceClient(deps.Conn),
		cartRuleLifecycleClient:       cartv1.NewCartRuleLifecycleServiceClient(deps.Conn),
		savedListValidationClient:     cartv1.NewSavedListValidationServiceClient(deps.Conn),
		cartValidationClient:          cartv1.NewCartValidationServiceClient(deps.Conn),
	}
}

// SubmitSavedList submits a saved list.
func (s *SavedListApprovalServer) SubmitSavedList(ctx context.Context, req *cartv1.SubmitSavedListRequest) (*cartv1.SubmitSavedListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitSavedList", "error", err)
		return nil, err
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "submit saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	metadata2 := make(map[string]string, len(req.GetSavedList().GetMetadata()))
	for key, value := range req.GetSavedList().GetMetadata() {
		metadata2[key] = value
	}
	summarizeCartsRequest := newSummarizeCartsRequest(withSummarizeCartsContext(req.GetContext()), withSummarizeCartsReason(req.GetReason()), withSummarizeCartsAnnotations(req.GetAnnotations()), withSummarizeCartsDetails(&cartv1.SummarizeCartsRequest_Details{Filters: metadata2}))
	summarizeCartsReply, err := s.cartQueryClient.SummarizeCarts(ctx, summarizeCartsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartQueryService/SummarizeCarts")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key2, value2 := range req.GetAnnotations() {
		annotations[key2] = value2
	}
	summarizeSavedListsRequest := &cartv1.SummarizeSavedListsRequest{}
	summarizeSavedListsRequest.Context = req.GetContext()
	summarizeSavedListsRequest.SavedListId = req.GetSavedList().GetSavedListId()
	summarizeSavedListsRequest.Annotations = annotations
	summarizeSavedListsRequest.Reason = req.GetReason()
	summarizeSavedListsRequest.Details = &cartv1.SummarizeSavedListsRequest_Details{Period: summarizeCartsReply.GetPeriod()}
	summarizeSavedListsReply, err := s.savedListQueryClient.SummarizeSavedLists(ctx, summarizeSavedListsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListQueryService/SummarizeSavedLists")
	}
	previewSearchIndexRequest := &searchv1.PreviewSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{
				Locality: req.GetSavedList().GetBillingAddress().GetLocality(),
			}},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetSavedList().GetCounterparty().GetDisplayName(),
			},
		},
	}
	type previewSearchIndexResult struct {
		reply *searchv1.PreviewSearchIndexResponse
		err   error
	}
	previewSearchIndexResults := make(chan previewSearchIndexResult, 1)
	go func() {
		reply, err := s.searchIndexValidationClient.PreviewSearchIndex(ctx, previewSearchIndexRequest)
		previewSearchIndexResults <- previewSearchIndexResult{reply, err}
	}()
	var previewSearchIndexReply *searchv1.PreviewSearchIndexResponse
	select {
	case result := <-previewSearchIndexResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.search.v1.SearchIndexValidationService/PreviewSearchIndex")
		}
		previewSearchIndexReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	_ = summarizeSavedListsReply
	return &cartv1.SubmitSavedListResponse{
		Outcome: &cartv1.SubmitSavedListResponse_Outcome{Status: summarizeCartsReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Status:       previewSearchIndexReply.GetResult().GetStatus(),
			ReferenceIds: previewSearchIndexReply.GetResult().GetReferenceIds(),
		},
		SavedList: &cartv1.SavedList{
			SavedListId: req.GetSavedListId(),
			IssuedAt:    req.GetSavedList().GetIssuedAt(),
		},
	}, nil
}

// ApproveSavedList approves a saved list.
func (s *SavedListApprovalServer) ApproveSavedList(ctx context.Context, req *cartv1.ApproveSavedListRequest) (*cartv1.ApproveSavedListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "approve saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetSavedList()).(*cartv1.SavedList)
	rejectStorageBinRequest := newRejectStorageBinRequest(withRejectStorageBinContext(req.GetContext()), withRejectStorageBinStorageBin(&warehousingv1.StorageBin{
		Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Audit: &commonv1.AuditInfo{CreatedBy: cloned.GetAudit().GetCreatedBy()},
				Location: &commonv1.PostalAddress{
					PostalCode: req.GetSavedList().GetBillingAddress().GetPostalCode(),
				},
			}},
			Manager: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		}},
	}))
	rejectStorageBinReply, err := s.storageBinApprovalClient.RejectStorageBin(ctx, rejectStorageBinRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinApprovalService/RejectStorageBin")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*cartv1.CalculateCartRuleResponse, len(req.GetSavedList().GetLines()))
	for i, line := range req.GetSavedList().GetLines() {
		group.Go(func() error {
			reply, err := s.cartRuleCalculationClient.CalculateCartRule(groupCtx, &cartv1.CalculateCartRuleRequest{
				Context: req.GetContext(),
				CartRule: &cartv1.CartRule{
					Action: &cartv1.CartRule_Discount{Discount: &cartv1.CartRuleDiscountAction{Amount: line.GetAmount()}},
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
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/CalculateCartRule")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &cartv1.ApproveSavedListResponse{
		SavedList: &cartv1.SavedList{
			Audit: &commonv1.AuditInfo{
				CreatedBy: rejectStorageBinReply.GetStorageBin().GetAudit().GetCreatedBy(),
			},
			SavedListId: req.GetSavedListId(),
			Status:      req.GetSavedList().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectSavedList rejects a saved list.
func (s *SavedListApprovalServer) RejectSavedList(ctx context.Context, req *cartv1.RejectSavedListRequest) (*cartv1.RejectSavedListResponse, error) {
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "reject saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	subscribeSearchQueryUpdatesRequest := buildSubscribeSearchQueryUpdatesRequestForRejectSavedList(req)
	subscribeSearchQueryUpdatesReply, err := s.searchQueryNotificationClient.SubscribeSearchQueryUpdates(ctx, subscribeSearchQueryUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/SubscribeSearchQueryUpdates")
	}
	cloned := proto.Clone(req.GetSavedList()).(*cartv1.SavedList)
	countSavedListsRequest := &cartv1.CountSavedListsRequest{
		Context: req.GetContext(),
		Filter: &cartv1.CountSavedListsRequest_Filter{
			Region:       cloned.GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetSavedList().GetIssuedAt(),
			OwnerId:      subscribeSearchQueryUpdatesReply.GetResult().GetCorrelationId(),
		},
	}
	countSavedListsReply := new(cartv1.CountSavedListsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", cartSavedListQueryServiceName, "CountSavedLists"), countSavedListsRequest, countSavedListsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListQueryService/CountSavedLists")
	}
	searchMetricSeriesesRequest := &metricsv1.SearchMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.SearchMetricSeriesesRequest_Filter{
			Region:       req.GetSavedList().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetSavedList().GetIssuedAt(),
			OwnerId:      req.GetSavedList().GetCounterparty().GetCartRuleId(),
		},
	}
	searchMetricSeriesesReply := new(metricsv1.SearchMetricSeriesesResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", metricsMetricSeriesServiceName, "SearchMetricSerieses"), searchMetricSeriesesRequest, searchMetricSeriesesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/SearchMetricSerieses")
	}
	return &cartv1.RejectSavedListResponse{
		Outcome: &cartv1.RejectSavedListResponse_Outcome{
			NextSteps: subscribeSearchQueryUpdatesReply.GetOutcome().GetNextSteps(),
			Message:   searchMetricSeriesesReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: countSavedListsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SavedList: &cartv1.SavedList{
			SavedListId: req.GetSavedListId(),
			Metadata:    req.GetSavedList().GetMetadata(),
		},
	}, nil
}

// EscalateSavedList escalates a saved list.
func (s *SavedListApprovalServer) EscalateSavedList(ctx context.Context, req *cartv1.EscalateSavedListRequest) (*cartv1.EscalateSavedListResponse, error) {
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "escalate saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	recalculateSavedListsReply, err := s.savedListCalculationClient.RecalculateSavedLists(ctx, &cartv1.RecalculateSavedListsRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Settlement: &cartv1.SavedList_Card{Card: &cartv1.SavedListCardSettlement{HolderName: req.GetSavedList().GetCard().GetHolderName()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListCalculationService/RecalculateSavedLists")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeTranslationKeysRequest := &localizationv1.SummarizeTranslationKeysRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details: &localizationv1.SummarizeTranslationKeysRequest_Details{
			Filters: recalculateSavedListsReply.GetSavedList().GetMetadata(),
		},
	}
	summarizeTranslationKeysReply, err := summarizeTranslationKeysCall(ctx, summarizeTranslationKeysRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.localization.v1.TranslationKeyQueryService/SummarizeTranslationKeys")
	}
	return &cartv1.EscalateSavedListResponse{
		SavedList: &cartv1.SavedList{
			Counterparty: &cartv1.CartRuleRef{
				CartRuleId: recalculateSavedListsReply.GetSavedList().GetCounterparty().GetCartRuleId(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Region: recalculateSavedListsReply.GetSavedList().GetBillingAddress().GetRegion(),
			},
			SavedListId: req.GetSavedListId(),
			IssuedAt:    req.GetSavedList().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeTranslationKeysReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ListPendingSavedLists lists saved lists a page at a time.
func (s *SavedListApprovalServer) ListPendingSavedLists(ctx context.Context, req *cartv1.ListPendingSavedListsRequest) (*cartv1.ListPendingSavedListsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending saved lists", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	explainCartRuleDecisionRequest := &cartv1.ExplainCartRuleDecisionRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
	}
	explainCartRuleDecisionReply, err := s.cartRuleValidationLazy().ExplainCartRuleDecision(ctx, explainCartRuleDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleValidationService/ExplainCartRuleDecision")
	}
	auditEventValidation, ok := s.clients.Get("acme.audit.v1.AuditEventValidationService").(auditv1.AuditEventValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.audit.v1.AuditEventValidationService")
	}
	normalizeAuditEventRequest := &auditv1.NormalizeAuditEventRequest{
		Context: req.GetContext(),
		Details: &auditv1.NormalizeAuditEventRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: explainCartRuleDecisionReply.GetCartRule().GetOwner().GetEmail(),
			},
		},
	}
	normalizeAuditEventReply, err := auditEventValidation.NormalizeAuditEvent(ctx, normalizeAuditEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/NormalizeAuditEvent")
	}
	return &cartv1.ListPendingSavedListsResponse{
		Result: &commonv1.OperationResult{
			Message:       explainCartRuleDecisionReply.GetResult().GetMessage(),
			CorrelationId: normalizeAuditEventReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// DelegateSavedListApproval delegates a saved list.
func (s *SavedListApprovalServer) DelegateSavedListApproval(ctx context.Context, req *cartv1.DelegateSavedListApprovalRequest) (*cartv1.DelegateSavedListApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DelegateSavedListApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "delegate saved list approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	for attachment := range iterx.Each(req.GetSavedList().GetAttachments(), func(candidate *cartv1.SavedListAttachment) bool { return candidate != nil }) {
		if _, err := s.cartApprovalClient.DelegateCartApproval(ctx, &cartv1.DelegateCartApprovalRequest{
			Context: req.GetContext(),
			Cart: &cartv1.Cart{
				Attachments: []*cartv1.CartAttachment{&cartv1.CartAttachment{
					Checksum:    attachment.GetChecksum(),
					SizeBytes:   attachment.GetSizeBytes(),
					Uri:         attachment.GetUri(),
					ContentType: attachment.GetContentType(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.CartApprovalService/DelegateCartApproval")
		}
	}
	terminateCartRuleReply, err := s.cartRuleLifecycleClient.TerminateCartRule(ctx, &cartv1.TerminateCartRuleRequest{
		Context:    req.GetContext(),
		CartRuleId: req.GetSavedList().GetCounterparty().GetCartRuleId(),
		Details: &cartv1.TerminateCartRuleRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				Email:       req.GetDetails().GetApprover().GetEmail(),
			},
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleLifecycleService/TerminateCartRule")
	}
	return &cartv1.DelegateSavedListApprovalResponse{
		Outcome: &cartv1.DelegateSavedListApprovalResponse_Outcome{
			Status:      terminateCartRuleReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: terminateCartRuleReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SavedList: &cartv1.SavedList{SavedListId: req.GetSavedListId()},
	}, nil
}

// RecallSavedList recalls a saved list.
func (s *SavedListApprovalServer) RecallSavedList(ctx context.Context, req *cartv1.RecallSavedListRequest) (*cartv1.RecallSavedListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "recall saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	savedListValidation, ok := s.clients.Get("acme.cart.v1.SavedListValidationService").(cartv1.SavedListValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.cart.v1.SavedListValidationService")
	}
	cloned := proto.Clone(req.GetSavedList()).(*cartv1.SavedList)
	scoreSavedListRequest := &cartv1.ScoreSavedListRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Audit:      &commonv1.AuditInfo{Etag: cloned.GetAudit().GetEtag()},
			Settlement: &cartv1.SavedList_Card{Card: &cartv1.SavedListCardSettlement{Token: req.GetSavedList().GetCard().GetToken()}},
		},
	}
	scoreSavedListReply, err := savedListValidation.ScoreSavedList(ctx, scoreSavedListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListValidationService/ScoreSavedList")
	}
	summarizeSavedListsRequest := buildSummarizeSavedListsRequestForRecallSavedList(req, scoreSavedListReply)
	summarizeSavedListsReply, err := s.savedListQueryClient.SummarizeSavedLists(ctx, summarizeSavedListsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListQueryService/SummarizeSavedLists")
	}
	return &cartv1.RecallSavedListResponse{
		SavedList: &cartv1.SavedList{
			Counterparty: &cartv1.CartRuleRef{
				Version: scoreSavedListReply.GetSavedList().GetCounterparty().GetVersion(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: scoreSavedListReply.GetSavedList().GetAudit().GetCreatedBy(),
			},
			SavedListId: req.GetSavedListId(),
			IssuedAt:    req.GetSavedList().GetIssuedAt(),
		},
		Outcome: &cartv1.RecallSavedListResponse_Outcome{
			Status:  summarizeSavedListsReply.GetResult().GetStatus(),
			Message: summarizeSavedListsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSavedListApprovalStatus returns a saved list.
func (s *SavedListApprovalServer) GetSavedListApprovalStatus(ctx context.Context, req *cartv1.GetSavedListApprovalStatusRequest) (*cartv1.GetSavedListApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "saved_list", req.GetSavedListId(), "get_saved_list_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get saved list approval status", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	lintSavedListsRequest := &cartv1.LintSavedListsRequest{
		Context:   req.GetContext(),
		SavedList: &cartv1.SavedList{SavedListId: req.GetSavedListId()},
		Details: &cartv1.LintSavedListsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cachedContext.GetActor().GetEmail()},
			Locale:  (&decoded).GetLocale(),
		},
	}
	lintSavedListsReply, err := cartv1.SavedListValidationServiceClient.LintSavedLists(s.savedListValidationClient, ctx, lintSavedListsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListValidationService/LintSavedLists")
	}
	return &cartv1.GetSavedListApprovalStatusResponse{
		SavedList: &cartv1.SavedList{
			Status: lintSavedListsReply.GetSavedList().GetStatus(),
			BillingAddress: &commonv1.PostalAddress{
				Region: lintSavedListsReply.GetSavedList().GetBillingAddress().GetRegion(),
			},
			SavedListId: req.GetSavedListId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentSavedListApproval comments a saved list.
func (s *SavedListApprovalServer) CommentSavedListApproval(ctx context.Context, req *cartv1.CommentSavedListApprovalRequest) (*cartv1.CommentSavedListApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentSavedListApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "comment saved list approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	findSavedListsRequest := &cartv1.FindSavedListsRequest{
		Context: req.GetContext(),
		Filter:  &cartv1.FindSavedListsRequest_Filter{Region: req.GetSavedList().GetBillingAddress().GetRegion()},
	}
	findSavedListsReply, err := findSavedListsCall(ctx, findSavedListsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListQueryService/FindSavedLists")
	}
	for attachment := range iterx.Each(req.GetSavedList().GetAttachments(), func(candidate *cartv1.SavedListAttachment) bool { return candidate != nil }) {
		if _, err := s.cartValidationClient.LintCarts(ctx, &cartv1.LintCartsRequest{
			Context: req.GetContext(),
			Cart: &cartv1.Cart{
				Attachments: []*cartv1.CartAttachment{&cartv1.CartAttachment{ContentType: attachment.GetContentType()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/LintCarts")
		}
	}
	return &cartv1.CommentSavedListApprovalResponse{
		Result: &commonv1.OperationResult{Status: findSavedListsReply.GetResult().GetStatus()},
		SavedList: &cartv1.SavedList{
			SavedListId:    req.GetSavedListId(),
			BillingAddress: req.GetSavedList().GetBillingAddress(),
		},
	}, nil
}

// cartRuleValidationLazy dials acme.cart.v1.CartRuleValidationService the first time a handler needs it.
func (s *SavedListApprovalServer) cartRuleValidationLazy() cartv1.CartRuleValidationServiceClient {
	s.cartRuleValidationOnce.Do(func() { s.cartRuleValidationCached = cartv1.NewCartRuleValidationServiceClient(s.conn) })
	return s.cartRuleValidationCached
}
