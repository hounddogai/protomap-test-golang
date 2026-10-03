package sellers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	datasetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/datasets/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	mediav1 "github.com/hounddogai/protomap-test-golang/gen/acme/media/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerLifecycleServer implements `acme.sellers.v1.SellerLifecycleService`.
// Moves a seller between the states of its lifecycle.
type SellerLifecycleServer struct {
	sellersv1.UnimplementedSellerLifecycleServiceServer
	store                             *Store
	log                               *slog.Logger
	sellerCalculationClient           sellersv1.SellerCalculationServiceClient
	sellerCalculationClientFallback   sellersv1.SellerCalculationServiceClient
	sellerClient                      sellersv1.SellerServiceClient
	meteredGiftCardBatchApproval      meteredGiftCardBatchApproval
	searchIndexNotificationClient     searchv1.SearchIndexNotificationServiceClient
	batchGetDatasetsFn                func(context.Context, *datasetsv1.BatchGetDatasetsRequest, ...grpc.CallOption) (*datasetsv1.BatchGetDatasetsResponse, error)
	clients                           *registry.Registry
	meteredSellerListingQuery         meteredSellerListingQuery
	giftCardBatchWorkflowClient       giftcardsv1.GiftCardBatchWorkflowServiceClient
	conn                              *grpc.ClientConn
	mediaAssetQueryOnce               sync.Once
	mediaAssetQueryCached             mediav1.MediaAssetQueryServiceClient
	auditEventClient                  auditv1.AuditEventServiceClient
	consentRecordValidationClient     customersv1.ConsentRecordValidationServiceClient
	giftCardTransactionWorkflowClient giftcardsv1.GiftCardTransactionWorkflowServiceClient
}

// NewSellerLifecycleServer wires a SellerLifecycleServer.
func NewSellerLifecycleServer(deps platform.Deps) *SellerLifecycleServer {
	dataset := datasetsv1.NewDatasetServiceClient(deps.Conn)
	return &SellerLifecycleServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.sellers.v1.SellerLifecycleService"),
		sellerCalculationClient:           sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		sellerCalculationClientFallback:   sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		sellerClient:                      sellersv1.NewSellerServiceClient(deps.Conn),
		meteredGiftCardBatchApproval:      meteredGiftCardBatchApproval{GiftCardBatchApprovalServiceClient: giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		searchIndexNotificationClient:     searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		batchGetDatasetsFn:                dataset.BatchGetDatasets,
		clients:                           deps.Clients,
		meteredSellerListingQuery:         meteredSellerListingQuery{SellerListingQueryServiceClient: sellersv1.NewSellerListingQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		giftCardBatchWorkflowClient:       giftcardsv1.NewGiftCardBatchWorkflowServiceClient(deps.Conn),
		conn:                              deps.Conn,
		auditEventClient:                  auditv1.NewAuditEventServiceClient(deps.Conn),
		consentRecordValidationClient:     customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		giftCardTransactionWorkflowClient: giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn),
	}
}

// ActivateSeller activates a seller.
func (s *SellerLifecycleServer) ActivateSeller(ctx context.Context, req *sellersv1.ActivateSellerRequest) (*sellersv1.ActivateSellerResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "activate_seller"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate seller", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *sellersv1.GetSellerRatesRequest, ...grpc.CallOption) (*sellersv1.GetSellerRatesResponse, error){
		"primary":  s.sellerCalculationClient.GetSellerRates,
		"fallback": s.sellerCalculationClientFallback.GetSellerRates,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	getSellerRatesRequest := &sellersv1.GetSellerRatesRequest{
		Context:  req.GetContext(),
		SellerId: req.GetSellerId(),
	}
	getSellerRatesReply, err := route(ctx, getSellerRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/GetSellerRates")
	}
	watchSellersStream, err := s.sellerClient.WatchSellers(ctx, &sellersv1.WatchSellersRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/WatchSellers")
	}
	for {
		received, err := watchSellersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/WatchSellers")
		}
		_ = received
	}
	return &sellersv1.ActivateSellerResponse{
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				Region: getSellerRatesReply.GetSeller().GetPrimaryAddress().GetRegion(),
			},
			SellerId: req.GetSellerId(),
		},
		Outcome: &sellersv1.ActivateSellerResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SuspendSeller suspends a seller.
func (s *SellerLifecycleServer) SuspendSeller(ctx context.Context, req *sellersv1.SuspendSellerRequest) (*sellersv1.SuspendSellerResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "suspend_seller"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend seller", "tenant", req.GetContext().GetTenantId())
	delegateGiftCardBatchApprovalRequest := &giftcardsv1.DelegateGiftCardBatchApprovalRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.DelegateGiftCardBatchApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	delegateGiftCardBatchApprovalReply, err := s.meteredGiftCardBatchApproval.DelegateGiftCardBatchApproval(ctx, delegateGiftCardBatchApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendSearchIndexTestNoticeRequest := &searchv1.SendSearchIndexTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSearchIndexTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Email:            req.GetDetails().GetNotifyContact().GetEmail(),
			},
			Payload: packed,
		},
	}
	sendSearchIndexTestNoticeReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*searchv1.SendSearchIndexTestNoticeResponse, error) {
		return s.searchIndexNotificationClient.SendSearchIndexTestNotice(ctx, sendSearchIndexTestNoticeRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/SendSearchIndexTestNotice")
	}
	return &sellersv1.SuspendSellerResponse{
		Result: &commonv1.OperationResult{
			Message: delegateGiftCardBatchApprovalReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient:  sendSearchIndexTestNoticeReply.GetSearchIndex().GetAddress().GetRecipient(),
				PostalCode: sendSearchIndexTestNoticeReply.GetSearchIndex().GetAddress().GetPostalCode(),
			},
			SellerId: req.GetSellerId(),
		},
		Outcome: &sellersv1.SuspendSellerResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstateSeller reinstates a seller.
func (s *SellerLifecycleServer) ReinstateSeller(ctx context.Context, req *sellersv1.ReinstateSellerRequest) (*sellersv1.ReinstateSellerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "reinstate_seller"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate seller", "tenant", req.GetContext().GetTenantId())
	batchGetDatasetsRequest := &datasetsv1.BatchGetDatasetsRequest{Context: req.GetContext()}
	batchGetDatasetsReply, err := s.batchGetDatasetsFn(ctx, batchGetDatasetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.datasets.v1.DatasetService/BatchGetDatasets")
	}
	sellerValidation, err := registry.Lookup[sellersv1.SellerValidationServiceClient](s.clients, "acme.sellers.v1.SellerValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	explainSellerDecisionRequest := &sellersv1.ExplainSellerDecisionRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Details: &sellersv1.ExplainSellerDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	}
	explainSellerDecisionReply, err := sellerValidation.ExplainSellerDecision(ctx, explainSellerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/ExplainSellerDecision")
	}
	return &sellersv1.ReinstateSellerResponse{
		Result: &commonv1.OperationResult{
			Message: batchGetDatasetsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &sellersv1.ReinstateSellerResponse_Outcome{
			ReferenceId: explainSellerDecisionReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Seller: &sellersv1.Seller{SellerId: req.GetSellerId()},
	}, nil
}

// TerminateSeller terminates a seller.
func (s *SellerLifecycleServer) TerminateSeller(ctx context.Context, req *sellersv1.TerminateSellerRequest) (*sellersv1.TerminateSellerResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "terminate_seller"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate seller", "tenant", req.GetContext().GetTenantId())
	summarizeSellerListingsRequest := &sellersv1.SummarizeSellerListingsRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
	summarizeSellerListingsReply, err := s.meteredSellerListingQuery.SummarizeSellerListings(ctx, summarizeSellerListingsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/SummarizeSellerListings")
	}
	return &sellersv1.TerminateSellerResponse{
		Result: &commonv1.OperationResult{
			Status:  summarizeSellerListingsReply.GetResult().GetStatus(),
			Message: summarizeSellerListingsReply.GetResult().GetMessage(),
		},
		Seller:  &sellersv1.Seller{SellerId: req.GetSellerId()},
		Outcome: &sellersv1.TerminateSellerResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewSeller renews a seller.
func (s *SellerLifecycleServer) RenewSeller(ctx context.Context, req *sellersv1.RenewSellerRequest) (*sellersv1.RenewSellerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewSeller", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "renew_seller"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew seller", "tenant", req.GetContext().GetTenantId())
	signalGiftCardBatchWorkflowRequest := &giftcardsv1.SignalGiftCardBatchWorkflowRequest{}
	signalGiftCardBatchWorkflowRequest.Context = req.GetContext()
	signalGiftCardBatchWorkflowRequest.Details = &giftcardsv1.SignalGiftCardBatchWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
	}
	signalGiftCardBatchWorkflowReply, err := s.giftCardBatchWorkflowClient.SignalGiftCardBatchWorkflow(ctx, signalGiftCardBatchWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/SignalGiftCardBatchWorkflow")
	}
	approveGiftCardRequest := &giftcardsv1.ApproveGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{
				Token: signalGiftCardBatchWorkflowReply.GetGiftCardBatch().GetCard().GetToken(),
				Last4: signalGiftCardBatchWorkflowReply.GetGiftCardBatch().GetCard().GetLast4(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: signalGiftCardBatchWorkflowReply.GetGiftCardBatch().GetAudit().GetUpdatedAt(),
			},
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	approveGiftCardReply, err := grpcx.NewClient(s.conn, giftcardsv1.NewGiftCardApprovalServiceClient).ApproveGiftCard(ctx, approveGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/ApproveGiftCard")
	}
	return &sellersv1.RenewSellerResponse{
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: signalGiftCardBatchWorkflowReply.GetGiftCardBatch().GetBillingAddress().GetLocality(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: approveGiftCardReply.GetGiftCard().GetAudit().GetUpdatedBy(),
				Etag:      approveGiftCardReply.GetGiftCard().GetAudit().GetEtag(),
			},
			SellerId: req.GetSellerId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: signalGiftCardBatchWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &sellersv1.RenewSellerResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ExpireSeller expires a seller.
func (s *SellerLifecycleServer) ExpireSeller(ctx context.Context, req *sellersv1.ExpireSellerRequest) (*sellersv1.ExpireSellerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "expire_seller"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire seller", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeMediaAssetsRequest := &mediav1.SummarizeMediaAssetsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &mediav1.SummarizeMediaAssetsRequest_Details{Filters: req.GetContext().GetBaggage()},
	}
	summarizeMediaAssetsReply, err := s.mediaAssetQueryLazy().SummarizeMediaAssets(ctx, summarizeMediaAssetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetQueryService/SummarizeMediaAssets")
	}
	listAuditEventsRequest := &auditv1.ListAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.ListAuditEventsRequest_Filter{
			OwnerId:      summarizeMediaAssetsReply.GetResult().GetCorrelationId(),
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			Labels:       summarizeMediaAssetsReply.GetSummary(),
		},
	}
	listAuditEventsReply, err := grpcx.Call(ctx, s.auditEventClient.ListAuditEvents, listAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/ListAuditEvents")
	}
	return &sellersv1.ExpireSellerResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeMediaAssetsReply.GetResult().GetReferenceIds(),
			Status:       listAuditEventsReply.GetResult().GetStatus(),
		},
		Seller:  &sellersv1.Seller{SellerId: req.GetSellerId()},
		Outcome: &sellersv1.ExpireSellerResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetSellerLifecycle returns a seller.
func (s *SellerLifecycleServer) GetSellerLifecycle(ctx context.Context, req *sellersv1.GetSellerLifecycleRequest) (*sellersv1.GetSellerLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "get_seller_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller lifecycle", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.GetSellerLifecycleResponse{
		Seller: &sellersv1.Seller{SellerId: req.GetSellerId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleSellerExpiry schedules a seller.
func (s *SellerLifecycleServer) ScheduleSellerExpiry(ctx context.Context, req *sellersv1.ScheduleSellerExpiryRequest) (*sellersv1.ScheduleSellerExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "schedule seller expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	source := req.GetDetails().GetNotify().ProtoReflect()
	phone := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("phone"))).String()
	previewConsentRecordRequest := &customersv1.PreviewConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.PreviewConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetSeller().GetContact().GetEmail(),
				Phone:            phone,
				Name:             req.GetSeller().GetContact().GetName(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	send := s.previewConsentRecordSender(previewConsentRecordRequest)
	previewConsentRecordReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord")
	}
	sellerScorecardValidation, ok := s.clients.Get("acme.sellers.v1.SellerScorecardValidationService").(sellersv1.SellerScorecardValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.sellers.v1.SellerScorecardValidationService")
	}
	encoded, err := protojson.Marshal(req.GetSeller())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded sellersv1.Seller
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	validateSellerScorecardRequest := &sellersv1.ValidateSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetSeller().GetPrimaryAddress().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: (&decoded).GetAudit().GetCreatedBy()},
		},
	}
	validateSellerScorecardReply, err := sellerScorecardValidation.ValidateSellerScorecard(ctx, validateSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/ValidateSellerScorecard")
	}
	listSellersRequest := buildListSellersRequestForScheduleSellerExpiry(req, previewConsentRecordReply, validateSellerScorecardReply)
	listSellersReply, err := s.sellerClient.ListSellers(ctx, listSellersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/ListSellers")
	}
	packed, err := anypb.New(req.GetSeller())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	startGiftCardTransactionWorkflowReply, err := s.giftCardTransactionWorkflowClient.StartGiftCardTransactionWorkflow(ctx, &giftcardsv1.StartGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.StartGiftCardTransactionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/StartGiftCardTransactionWorkflow")
	}
	return &sellersv1.ScheduleSellerExpiryResponse{
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: previewConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
				Email:       validateSellerScorecardReply.GetSellerScorecard().GetCounterparty().GetEmail(),
			},
			SellerId:       req.GetSellerId(),
			PrimaryAddress: req.GetSeller().GetPrimaryAddress(),
		},
		Outcome: &sellersv1.ScheduleSellerExpiryResponse_Outcome{
			NextSteps: previewConsentRecordReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: listSellersReply.GetResult().GetReferenceIds(),
			Status:       startGiftCardTransactionWorkflowReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// mediaAssetQueryLazy dials acme.media.v1.MediaAssetQueryService the first time a handler needs it.
func (s *SellerLifecycleServer) mediaAssetQueryLazy() mediav1.MediaAssetQueryServiceClient {
	s.mediaAssetQueryOnce.Do(func() { s.mediaAssetQueryCached = mediav1.NewMediaAssetQueryServiceClient(s.conn) })
	return s.mediaAssetQueryCached
}

// previewConsentRecordSender returns a function that sends one request to
// acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord.
func (s *SellerLifecycleServer) previewConsentRecordSender(request *customersv1.PreviewConsentRecordRequest) func(context.Context) (*customersv1.PreviewConsentRecordResponse, error) {
	return func(ctx context.Context) (*customersv1.PreviewConsentRecordResponse, error) {
		return s.consentRecordValidationClient.PreviewConsentRecord(ctx, request)
	}
}
