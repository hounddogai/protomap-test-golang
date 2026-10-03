package sellers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerScorecardApprovalServer implements `acme.sellers.v1.SellerScorecardApprovalService`.
// Routes seller scorecards through approval, with escalation and delegation.
type SellerScorecardApprovalServer struct {
	sellersv1.UnimplementedSellerScorecardApprovalServiceServer
	store                                 *Store
	log                                   *slog.Logger
	conn                                  *grpc.ClientConn
	clients                               *registry.Registry
	preferenceCalculationClient           customersv1.PreferenceCalculationServiceClient
	sellerScorecardSyncClient             sellersv1.SellerScorecardSyncServiceClient
	preferenceValidationClient            customersv1.PreferenceValidationServiceClient
	sellerQueryClient                     sellersv1.SellerQueryServiceClient
	searchQueryExportClient               searchv1.SearchQueryExportServiceClient
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	giftCardWorkflowClient                giftcardsv1.GiftCardWorkflowServiceClient
	apiCredentialNotificationClient       identityv1.ApiCredentialNotificationServiceClient
	giftCardTransactionWorkflowClient     giftcardsv1.GiftCardTransactionWorkflowServiceClient
}

// NewSellerScorecardApprovalServer wires a SellerScorecardApprovalServer.
func NewSellerScorecardApprovalServer(deps platform.Deps) *SellerScorecardApprovalServer {
	return &SellerScorecardApprovalServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.sellers.v1.SellerScorecardApprovalService"),
		conn:                                  deps.Conn,
		clients:                               deps.Clients,
		preferenceCalculationClient:           customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		sellerScorecardSyncClient:             sellersv1.NewSellerScorecardSyncServiceClient(deps.Conn),
		preferenceValidationClient:            customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		sellerQueryClient:                     sellersv1.NewSellerQueryServiceClient(deps.Conn),
		searchQueryExportClient:               searchv1.NewSearchQueryExportServiceClient(deps.Conn),
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		giftCardWorkflowClient:                giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
		apiCredentialNotificationClient:       identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		giftCardTransactionWorkflowClient:     giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn),
	}
}

// SubmitSellerScorecard submits a seller scorecard.
func (s *SellerScorecardApprovalServer) SubmitSellerScorecard(ctx context.Context, req *sellersv1.SubmitSellerScorecardRequest) (*sellersv1.SubmitSellerScorecardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitSellerScorecard", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "submit seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	var holder struct{ DisplayName string }
	if record := req.GetSellerScorecard().GetCounterparty(); record != nil {
		if err := mapper.CopyFields(&holder, record, "DisplayName"); err != nil {
			s.log.WarnContext(ctx, "copy display_name", "error", err)
		}
	}
	cloned := proto.Clone(req.GetSellerScorecard()).(*sellersv1.SellerScorecard)
	packed, err := anypb.New(req.GetSellerScorecard())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	signalGiftCardTransactionWorkflowRequest := &giftcardsv1.SignalGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: holder.DisplayName,
				Number:      cloned.GetNumber(),
			},
			Detail: packed,
		},
		Details: &giftcardsv1.SignalGiftCardTransactionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: req.GetSellerScorecard().GetCounterparty().GetEmail(),
			},
		},
		Reason: req.GetReason(),
	}
	signalGiftCardTransactionWorkflowReply := new(giftcardsv1.SignalGiftCardTransactionWorkflowResponse)
	if err := s.conn.Invoke(ctx, giftcardsGiftCardTransactionWorkflowServiceSignalGiftCardTransactionWorkflowMethod, signalGiftCardTransactionWorkflowRequest, signalGiftCardTransactionWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/SignalGiftCardTransactionWorkflow")
	}
	preferenceCalculation, ok := s.clients.Get("acme.customers.v1.PreferenceCalculationService").(customersv1.PreferenceCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.PreferenceCalculationService")
	}
	cloned2 := proto.Clone(req.GetSellerScorecard()).(*sellersv1.SellerScorecard)
	estimatePreferenceRequest := &customersv1.EstimatePreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.EstimatePreferenceRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: cloned2.GetBillingAddress().GetRecipient()},
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetSellerScorecard().GetAudit().GetCreatedAt(),
			},
		},
	}
	estimatePreferenceReply, err := preferenceCalculation.EstimatePreference(ctx, estimatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/EstimatePreference")
	}
	return &sellersv1.SubmitSellerScorecardResponse{
		Result: &commonv1.OperationResult{
			Status:        signalGiftCardTransactionWorkflowReply.GetResult().GetStatus(),
			CorrelationId: signalGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetCorrelationId(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
				Tax: &sellersv1.SellerScorecardLineTax{
					Amount: estimatePreferenceReply.GetPreference().GetDiscount().GetAmount(),
				},
			}},
			SellerScorecardId: req.GetSellerScorecardId(),
			Metadata:          req.GetSellerScorecard().GetMetadata(),
		},
		Outcome: &sellersv1.SubmitSellerScorecardResponse_Outcome{Status: estimatePreferenceReply.GetResult().GetStatus()},
	}, nil
}

// ApproveSellerScorecard approves a seller scorecard.
func (s *SellerScorecardApprovalServer) ApproveSellerScorecard(ctx context.Context, req *sellersv1.ApproveSellerScorecardRequest) (*sellersv1.ApproveSellerScorecardResponse, error) {
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "approve seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	quotePreferenceRequest := &customersv1.QuotePreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.QuotePreferenceRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: req.GetSellerScorecard().GetBillingAddress().GetLocality(),
			},
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetSellerScorecard().GetAudit().GetCreatedAt(),
			},
		},
	}
	quotePreferenceReply, err := customersv1.PreferenceCalculationServiceClient.QuotePreference(s.preferenceCalculationClient, ctx, quotePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/QuotePreference")
	}
	return &sellersv1.ApproveSellerScorecardResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: quotePreferenceReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			SellerScorecardId: req.GetSellerScorecardId(),
			Counterparty:      req.GetSellerScorecard().GetCounterparty(),
		},
	}, nil
}

// RejectSellerScorecard rejects a seller scorecard.
func (s *SellerScorecardApprovalServer) RejectSellerScorecard(ctx context.Context, req *sellersv1.RejectSellerScorecardRequest) (*sellersv1.RejectSellerScorecardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectSellerScorecard", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "reject seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	resolveSellerScorecardConflictRequest := &sellersv1.ResolveSellerScorecardConflictRequest{}
	resolveSellerScorecardConflictRequest.Context = req.GetContext()
	resolveSellerScorecardConflictRequest.SellerScorecard = &sellersv1.SellerScorecard{
		Audit: &commonv1.AuditInfo{
			CreatedBy: req.GetSellerScorecard().GetAudit().GetCreatedBy(),
		},
		Settlement: &sellersv1.SellerScorecard_Card{Card: &sellersv1.SellerScorecardCardSettlement{Token: req.GetSellerScorecard().GetCard().GetToken()}},
	}
	resolveSellerScorecardConflictReply, err := s.sellerScorecardSyncClient.ResolveSellerScorecardConflict(ctx, resolveSellerScorecardConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardSyncService/ResolveSellerScorecardConflict")
	}
	jobs := make(chan *sellersv1.SellerScorecardLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.preferenceValidationClient.PreviewPreference(workCtx, &customersv1.PreviewPreferenceRequest{
					Context: req.GetContext(),
					Preference: &customersv1.Preference{
						Action: &customersv1.Preference_Discount{Discount: &customersv1.PreferenceDiscountAction{Amount: line.GetTax().GetAmount()}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetSellerScorecard().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	return &sellersv1.RejectSellerScorecardResponse{
		Outcome: &sellersv1.RejectSellerScorecardResponse_Outcome{
			Status: resolveSellerScorecardConflictReply.GetResult().GetStatus(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			SellerScorecardId: req.GetSellerScorecardId(),
			Total:             req.GetSellerScorecard().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalateSellerScorecard escalates a seller scorecard.
func (s *SellerScorecardApprovalServer) EscalateSellerScorecard(ctx context.Context, req *sellersv1.EscalateSellerScorecardRequest) (*sellersv1.EscalateSellerScorecardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "escalate seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	jobs := make(chan *sellersv1.SellerScorecardLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.sellerQueryClient.SuggestSellers(workCtx, &sellersv1.SuggestSellersRequest{
					Context: req.GetContext(),
					Filter: &sellersv1.SuggestSellersRequest_Filter{
						Region:  req.GetSellerScorecard().GetBillingAddress().GetRegion(),
						Labels:  line.GetAttributes(),
						OwnerId: line.GetLineId(),
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetSellerScorecard().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/SuggestSellers")
	}
	getSellerScorecardLifecycleRequest := &sellersv1.GetSellerScorecardLifecycleRequest{
		Context:           req.GetContext(),
		SellerScorecardId: req.GetSellerScorecardId(),
	}
	getSellerScorecardLifecycleReply := new(sellersv1.GetSellerScorecardLifecycleResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", sellersSellerScorecardLifecycleServiceName, "GetSellerScorecardLifecycle"), getSellerScorecardLifecycleRequest, getSellerScorecardLifecycleReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardLifecycleService/GetSellerScorecardLifecycle")
	}
	return &sellersv1.EscalateSellerScorecardResponse{
		SellerScorecard: &sellersv1.SellerScorecard{
			Number:            getSellerScorecardLifecycleReply.GetSellerScorecard().GetNumber(),
			DueAt:             getSellerScorecardLifecycleReply.GetSellerScorecard().GetDueAt(),
			SellerScorecardId: req.GetSellerScorecardId(),
			Counterparty:      req.GetSellerScorecard().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingSellerScorecards lists seller scorecards a page at a time.
func (s *SellerScorecardApprovalServer) ListPendingSellerScorecards(ctx context.Context, req *sellersv1.ListPendingSellerScorecardsRequest) (*sellersv1.ListPendingSellerScorecardsResponse, error) {
	s.log.InfoContext(ctx, "list pending seller scorecards", "tenant", req.GetContext().GetTenantId())
	listSearchQueryExportsRequest := &searchv1.ListSearchQueryExportsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchQueryExportsRequest_Filter{
			Query:  req.GetFilter().GetQuery(),
			Labels: req.GetFilter().GetLabels(),
		},
	}
	listSearchQueryExportsReply, err := grpcx.Call(ctx, s.searchQueryExportClient.ListSearchQueryExports, listSearchQueryExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/ListSearchQueryExports")
	}
	return &sellersv1.ListPendingSellerScorecardsResponse{
		Result: &commonv1.OperationResult{
			Details: listSearchQueryExportsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PageInfo: &commonv1.PageInfo{
			TotalSize: listSearchQueryExportsReply.GetPageInfo().GetTotalSize(),
		},
	}, nil
}

// RecallSellerScorecard recalls a seller scorecard.
func (s *SellerScorecardApprovalServer) RecallSellerScorecard(ctx context.Context, req *sellersv1.RecallSellerScorecardRequest) (*sellersv1.RecallSellerScorecardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "recall seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	calculateSellerPayoutScheduleRequest := newCalculateSellerPayoutScheduleRequest(withCalculateSellerPayoutScheduleContext(req.GetContext()), withCalculateSellerPayoutScheduleSellerPayoutSchedule(&sellersv1.SellerPayoutSchedule{
		Organizer: &identityv1.UserRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			Email:       req.GetSellerScorecard().GetCounterparty().GetEmail(),
		},
		Location: &commonv1.PostalAddress{
			Recipient:  req.GetSellerScorecard().GetBillingAddress().GetRecipient(),
			PostalCode: req.GetSellerScorecard().GetBillingAddress().GetPostalCode(),
		},
	}))
	calculateSellerPayoutScheduleReply, err := s.sellerPayoutScheduleCalculationClient.CalculateSellerPayoutSchedule(ctx, calculateSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/CalculateSellerPayoutSchedule")
	}
	pauseGiftCardWorkflowReply, err := s.giftCardWorkflowClient.PauseGiftCardWorkflow(ctx, &giftcardsv1.PauseGiftCardWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.PauseGiftCardWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: req.GetSellerScorecard().GetCounterparty().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/PauseGiftCardWorkflow")
	}
	streamApiCredentialNoticesStream, err := s.apiCredentialNotificationClient.StreamApiCredentialNotices(ctx, &identityv1.StreamApiCredentialNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone: pauseGiftCardWorkflowReply.GetGiftCard().GetContact().GetPhone(),
		},
		Filter: &identityv1.StreamApiCredentialNoticesRequest_Filter{
			Region: req.GetSellerScorecard().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/StreamApiCredentialNotices")
	}
	for {
		received, err := streamApiCredentialNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/StreamApiCredentialNotices")
		}
		_ = received
	}
	return &sellersv1.RecallSellerScorecardResponse{
		Outcome: &sellersv1.RecallSellerScorecardResponse_Outcome{
			Message:     calculateSellerPayoutScheduleReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: calculateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetLocation().GetRecipient(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
		},
		Result: &commonv1.OperationResult{
			Status:  pauseGiftCardWorkflowReply.GetResult().GetStatus(),
			Message: pauseGiftCardWorkflowReply.GetResult().GetMessage(),
		},
	}, nil
}

// GetSellerScorecardApprovalStatus returns a seller scorecard.
func (s *SellerScorecardApprovalServer) GetSellerScorecardApprovalStatus(ctx context.Context, req *sellersv1.GetSellerScorecardApprovalStatusRequest) (*sellersv1.GetSellerScorecardApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSellerScorecardApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "get_seller_scorecard_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller scorecard approval status", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	pauseGiftCardTransactionWorkflowRequest := &giftcardsv1.PauseGiftCardTransactionWorkflowRequest{}
	pauseGiftCardTransactionWorkflowRequest.Context = req.GetContext()
	pauseGiftCardTransactionWorkflowRequest.Details = &giftcardsv1.PauseGiftCardTransactionWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{Email: (&decoded).GetActor().GetEmail()},
	}
	pauseGiftCardTransactionWorkflowReply, err := s.giftCardTransactionWorkflowClient.PauseGiftCardTransactionWorkflow(ctx, pauseGiftCardTransactionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/PauseGiftCardTransactionWorkflow")
	}
	return &sellersv1.GetSellerScorecardApprovalStatusResponse{
		SellerScorecard: &sellersv1.SellerScorecard{
			Number:            pauseGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetSubject().GetNumber(),
			SellerScorecardId: req.GetSellerScorecardId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentSellerScorecardApproval comments a seller scorecard.
func (s *SellerScorecardApprovalServer) CommentSellerScorecardApproval(ctx context.Context, req *sellersv1.CommentSellerScorecardApprovalRequest) (*sellersv1.CommentSellerScorecardApprovalResponse, error) {
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "comment seller scorecard approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	pushSellerScorecardRequest := buildPushSellerScorecardRequestForCommentSellerScorecardApproval(req)
	pushSellerScorecardReply, err := s.sellerScorecardSyncClient.PushSellerScorecard(ctx, pushSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardSyncService/PushSellerScorecard")
	}
	return &sellersv1.CommentSellerScorecardApprovalResponse{
		Result: &commonv1.OperationResult{
			Details: pushSellerScorecardReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			SellerScorecardId: req.GetSellerScorecardId(),
			Total:             req.GetSellerScorecard().GetTotal(),
		},
	}, nil
}
