package sellers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	contractsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/contracts/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	timekeepingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/timekeeping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerScorecardSyncServer implements `acme.sellers.v1.SellerScorecardSyncService`.
// Keeps seller scorecards in step with the systems of record in other regions.
type SellerScorecardSyncServer struct {
	sellersv1.UnsafeSellerScorecardSyncServiceServer
	store                            *Store
	log                              *slog.Logger
	productCalculationClient         catalogv1.ProductCalculationServiceClient
	giftCardBatchApprovalClient      giftcardsv1.GiftCardBatchApprovalServiceClient
	meteredHouseholdQuery            meteredHouseholdQuery
	consentRecordValidationClient    customersv1.ConsentRecordValidationServiceClient
	approveGiftCardTransactionFn     func(context.Context, *giftcardsv1.ApproveGiftCardTransactionRequest, ...grpc.CallOption) (*giftcardsv1.ApproveGiftCardTransactionResponse, error)
	sellerQueryClient                sellersv1.SellerQueryServiceClient
	timesheetQueryClient             timekeepingv1.TimesheetQueryServiceClient
	sellerListingReportingClient     sellersv1.SellerListingReportingServiceClient
	documentClient                   documentsv1.DocumentServiceClient
	clients                          *registry.Registry
	fraudSignalCalculationClient     fraudv1.FraudSignalCalculationServiceClient
	contractQueryClient              contractsv1.ContractQueryServiceClient
	sellerScorecardCalculationClient sellersv1.SellerScorecardCalculationServiceClient
}

// NewSellerScorecardSyncServer wires a SellerScorecardSyncServer.
func NewSellerScorecardSyncServer(deps platform.Deps) *SellerScorecardSyncServer {
	giftCardTransactionApproval := giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn)
	return &SellerScorecardSyncServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.sellers.v1.SellerScorecardSyncService"),
		productCalculationClient:         catalogv1.NewProductCalculationServiceClient(deps.Conn),
		giftCardBatchApprovalClient:      giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
		meteredHouseholdQuery:            meteredHouseholdQuery{HouseholdQueryServiceClient: customersv1.NewHouseholdQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		consentRecordValidationClient:    customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		approveGiftCardTransactionFn:     giftCardTransactionApproval.ApproveGiftCardTransaction,
		sellerQueryClient:                sellersv1.NewSellerQueryServiceClient(deps.Conn),
		timesheetQueryClient:             timekeepingv1.NewTimesheetQueryServiceClient(deps.Conn),
		sellerListingReportingClient:     sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		documentClient:                   documentsv1.NewDocumentServiceClient(deps.Conn),
		clients:                          deps.Clients,
		fraudSignalCalculationClient:     fraudv1.NewFraudSignalCalculationServiceClient(deps.Conn),
		contractQueryClient:              contractsv1.NewContractQueryServiceClient(deps.Conn),
		sellerScorecardCalculationClient: sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn),
	}
}

// SyncSellerScorecards syncs a seller scorecard.
func (s *SellerScorecardSyncServer) SyncSellerScorecards(ctx context.Context, req *sellersv1.SyncSellerScorecardsRequest) (*sellersv1.SyncSellerScorecardsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "sync seller scorecards", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetSellerScorecard()).(*sellersv1.SellerScorecard)
	explainProductCalculationRequest := &catalogv1.ExplainProductCalculationRequest{}
	explainProductCalculationRequest.Context = req.GetContext()
	explainProductCalculationRequest.Product = &catalogv1.Product{
		Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
			Region:    req.GetSellerScorecard().GetBillingAddress().GetRegion(),
			Recipient: cloned.GetBillingAddress().GetRecipient(),
		}},
	}
	explainProductCalculationReply, err := s.productCalculationClient.ExplainProductCalculation(ctx, explainProductCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/ExplainProductCalculation")
	}
	delegateGiftCardBatchApprovalReply, err := s.giftCardBatchApprovalClient.DelegateGiftCardBatchApproval(ctx, &giftcardsv1.DelegateGiftCardBatchApprovalRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: explainProductCalculationReply.GetProduct().GetOwner().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Locality: req.GetSellerScorecard().GetBillingAddress().GetLocality(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetSellerScorecard().GetAudit().GetEtag()},
			Settlement: &giftcardsv1.GiftCardBatch_Card{Card: &giftcardsv1.GiftCardBatchCardSettlement{
				HolderName: req.GetSellerScorecard().GetCard().GetHolderName(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval")
	}
	return &sellersv1.SyncSellerScorecardsResponse{
		Outcome: &sellersv1.SyncSellerScorecardsResponse_Outcome{
			Message: explainProductCalculationReply.GetResult().GetMessage(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				Lines: explainProductCalculationReply.GetProduct().GetAddress().GetLines(),
			},
			Number: delegateGiftCardBatchApprovalReply.GetGiftCardBatch().GetNumber(),
			Audit: &commonv1.AuditInfo{
				CreatedBy: delegateGiftCardBatchApprovalReply.GetGiftCardBatch().GetAudit().GetCreatedBy(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
			Notes:             req.GetSellerScorecard().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PushSellerScorecard pushs a seller scorecard.
func (s *SellerScorecardSyncServer) PushSellerScorecard(ctx context.Context, req *sellersv1.PushSellerScorecardRequest) (*sellersv1.PushSellerScorecardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "push seller scorecard", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	browseHouseholdsRequest := &customersv1.BrowseHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.BrowseHouseholdsRequest_Filter{
			Region: req.GetSellerScorecard().GetBillingAddress().GetRegion(),
		},
	}
	browseHouseholdsReply, err := s.meteredHouseholdQuery.BrowseHouseholds(ctx, browseHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/BrowseHouseholds")
	}
	ctx = tenancy.With(ctx, "recipient", req.GetSellerScorecard().GetBillingAddress().GetRecipient())
	packed, err := anypb.New(req.GetSellerScorecard())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreConsentRecordReply, err := s.consentRecordValidationClient.ScoreConsentRecord(ctx, &customersv1.ScoreConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetSellerScorecard().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &customersv1.ScoreConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: tenancy.Value(ctx, "recipient")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord")
	}
	_ = scoreConsentRecordReply
	return &sellersv1.PushSellerScorecardResponse{
		Result: &commonv1.OperationResult{
			Message: browseHouseholdsReply.GetResult().GetMessage(),
			Details: browseHouseholdsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			SellerScorecardId: req.GetSellerScorecardId(),
			Number:            req.GetSellerScorecard().GetNumber(),
		},
	}, nil
}

// ReconcileSellerScorecards reconciles a seller scorecard.
func (s *SellerScorecardSyncServer) ReconcileSellerScorecards(ctx context.Context, req *sellersv1.ReconcileSellerScorecardsRequest) (*sellersv1.ReconcileSellerScorecardsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "reconcile seller scorecards", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	approveGiftCardTransactionRequest := &giftcardsv1.ApproveGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: req.GetSellerScorecard().GetCounterparty().GetDisplayName(),
			},
		},
	}
	approveGiftCardTransactionReply, err := s.approveGiftCardTransactionFn(ctx, approveGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/ApproveGiftCardTransaction")
	}
	exportSellersStream, err := s.sellerQueryClient.ExportSellers(ctx, &sellersv1.ExportSellersRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ExportSellersRequest_Filter{
			Region: req.GetSellerScorecard().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/ExportSellers")
	}
	for {
		received, err := exportSellersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/ExportSellers")
		}
		_ = received
	}
	cloned := proto.Clone(req.GetSellerScorecard()).(*sellersv1.SellerScorecard)
	browseTimesheetsRequest := &timekeepingv1.BrowseTimesheetsRequest{}
	browseTimesheetsRequest.Context = req.GetContext()
	browseTimesheetsRequest.Filter = &timekeepingv1.BrowseTimesheetsRequest_Filter{
		Region:       cloned.GetBillingAddress().GetRegion(),
		CreatedAfter: req.GetSellerScorecard().GetAudit().GetCreatedAt(),
		Labels:       req.GetSellerScorecard().GetMetadata(),
	}
	browseTimesheetsReply, err := s.timesheetQueryClient.BrowseTimesheets(ctx, browseTimesheetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.timekeeping.v1.TimesheetQueryService/BrowseTimesheets")
	}
	streamSellerListingMetricsStream, err := s.sellerListingReportingClient.StreamSellerListingMetrics(ctx, &sellersv1.StreamSellerListingMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: req.GetSellerScorecard().GetCounterparty().GetEmail(),
		},
		Filter: &sellersv1.StreamSellerListingMetricsRequest_Filter{
			Region: req.GetSellerScorecard().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/StreamSellerListingMetrics")
	}
	for {
		received2, err := streamSellerListingMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/StreamSellerListingMetrics")
		}
		_ = received2
	}
	return &sellersv1.ReconcileSellerScorecardsResponse{
		Outcome: &sellersv1.ReconcileSellerScorecardsResponse_Outcome{
			Status:      approveGiftCardTransactionReply.GetResult().GetStatus(),
			EffectiveAt: approveGiftCardTransactionReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message: browseTimesheetsReply.GetResult().GetMessage(),
			Status:  browseTimesheetsReply.GetResult().GetStatus(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			SellerScorecardId: req.GetSellerScorecardId(),
			Notes:             req.GetSellerScorecard().GetNotes(),
		},
	}, nil
}

// StreamSellerScorecardChanges exchanges changes of seller scorecards with a peer.
func (s *SellerScorecardSyncServer) StreamSellerScorecardChanges(stream grpc.BidiStreamingServer[sellersv1.StreamSellerScorecardChangesRequest, sellersv1.StreamSellerScorecardChangesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *sellersv1.StreamSellerScorecardChangesRequest)
	failed := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			received, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					failed <- err
				}
				return
			}
			select {
			case incoming <- received:
			case <-ctx.Done():
				return
			}
		}
	}()
	for msg := range incoming {
		s.log.InfoContext(ctx, "stream seller scorecard changes", "tenant", msg.GetContext().GetTenantId())
		watchDocumentsStream, err := s.documentClient.WatchDocuments(ctx, &documentsv1.WatchDocumentsRequest{
			Context: msg.GetContext(),
			Filter: &documentsv1.WatchDocumentsRequest_Filter{
				Region:       msg.GetChange().GetBillingAddress().GetRegion(),
				CreatedAfter: msg.GetChange().GetAudit().GetCreatedAt(),
				OwnerId:      msg.GetChange().GetCounterparty().GetCustomerId(),
			},
			Subscriber: &commonv1.ContactInfo{Email: msg.GetChange().GetCounterparty().GetEmail()},
		})
		if err != nil {
			return grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
		}
		for {
			received, err := watchDocumentsStream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
			}
			_ = received
		}
		browseSellersRequest := &sellersv1.BrowseSellersRequest{}
		browseSellersRequest.Context = msg.GetContext()
		browseSellersRequest.Filter = &sellersv1.BrowseSellersRequest_Filter{
			Region:       msg.GetChange().GetBillingAddress().GetRegion(),
			CreatedAfter: msg.GetChange().GetAudit().GetUpdatedAt(),
			OwnerId:      msg.GetChange().GetCounterparty().GetCustomerId(),
		}
		if _, err := s.sellerQueryClient.BrowseSellers(ctx, browseSellersRequest); err != nil {
			return grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/BrowseSellers")
		}
		if err := stream.Send(&sellersv1.StreamSellerScorecardChangesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
			return err
		}
	}
	select {
	case err := <-failed:
		return err
	default:
		return nil
	}
}

// ResolveSellerScorecardConflict resolves a seller scorecard.
func (s *SellerScorecardSyncServer) ResolveSellerScorecardConflict(ctx context.Context, req *sellersv1.ResolveSellerScorecardConflictRequest) (*sellersv1.ResolveSellerScorecardConflictResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerScorecard(ctx, req.GetContext().GetTenantId(), req.GetSellerScorecard()); err != nil {
		return nil, grpcx.Wrap(err, "save seller scorecard")
	}
	s.log.InfoContext(ctx, "resolve seller scorecard conflict", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerScorecard().GetAudit().GetEtag())
	sellerScorecardCalculation, ok := s.clients.Get("acme.sellers.v1.SellerScorecardCalculationService").(sellersv1.SellerScorecardCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.sellers.v1.SellerScorecardCalculationService")
	}
	recalculateSellerScorecardsRequest := &sellersv1.RecalculateSellerScorecardsRequest{
		Context: req.GetContext(),
		Details: &sellersv1.RecalculateSellerScorecardsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: req.GetSellerScorecard().GetBillingAddress().GetLocality(),
			},
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			Settlement: &sellersv1.SellerScorecard_Card{Card: &sellersv1.SellerScorecardCardSettlement{Last4: req.GetSellerScorecard().GetCard().GetLast4()}},
			Counterparty: &customersv1.CustomerRef{
				CustomerId: req.GetSellerScorecard().GetCounterparty().GetCustomerId(),
			},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetSellerScorecard().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	recalculateSellerScorecardsReply, err := sellerScorecardCalculation.RecalculateSellerScorecards(ctx, recalculateSellerScorecardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/RecalculateSellerScorecards")
	}
	for _, line := range req.GetSellerScorecard().GetLines() {
		if _, err := s.fraudSignalCalculationClient.ExplainFraudSignalCalculation(ctx, &fraudv1.ExplainFraudSignalCalculationRequest{
			Context: req.GetContext(),
			FraudSignal: &fraudv1.FraudSignal{
				Attributes: line.GetAttributes(),
				Subject: &fraudv1.FraudRuleRef{
					DisplayName: recalculateSellerScorecardsReply.GetSellerScorecard().GetCounterparty().GetDisplayName(),
				},
			},
			Details: &fraudv1.ExplainFraudSignalCalculationRequest_Details{
				Contact: &commonv1.ContactInfo{
					Email: recalculateSellerScorecardsReply.GetSellerScorecard().GetCounterparty().GetEmail(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalCalculationService/ExplainFraudSignalCalculation")
		}
	}
	source := req.GetSellerScorecard().GetBillingAddress().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	exportContractsStream, err := s.contractQueryClient.ExportContracts(ctx, &contractsv1.ExportContractsRequest{
		Context: req.GetContext(),
		Filter:  &contractsv1.ExportContractsRequest_Filter{Region: region},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.contracts.v1.ContractQueryService/ExportContracts")
	}
	for {
		received, err := exportContractsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.contracts.v1.ContractQueryService/ExportContracts")
		}
		_ = received
	}
	return &sellersv1.ResolveSellerScorecardConflictResponse{
		Outcome: &sellersv1.ResolveSellerScorecardConflictResponse_Outcome{
			Status: recalculateSellerScorecardsReply.GetResult().GetStatus(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			SellerScorecardId: req.GetSellerScorecardId(),
			Counterparty:      req.GetSellerScorecard().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSellerScorecardSyncState returns a seller scorecard.
func (s *SellerScorecardSyncServer) GetSellerScorecardSyncState(ctx context.Context, req *sellersv1.GetSellerScorecardSyncStateRequest) (*sellersv1.GetSellerScorecardSyncStateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "get_seller_scorecard_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller scorecard sync state", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.GetSellerScorecardSyncStateResponse{
		SellerScorecard: &sellersv1.SellerScorecard{SellerScorecardId: req.GetSellerScorecardId()},
		Result:          &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSellerScorecardConflicts lists seller scorecards a page at a time.
func (s *SellerScorecardSyncServer) ListSellerScorecardConflicts(ctx context.Context, req *sellersv1.ListSellerScorecardConflictsRequest) (*sellersv1.ListSellerScorecardConflictsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list seller scorecard conflicts", "tenant", req.GetContext().GetTenantId())
	filterCache := cache.New[string, *sellersv1.ListSellerScorecardConflictsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	calculateSellerScorecardRequest := &sellersv1.CalculateSellerScorecardRequest{
		Context: req.GetContext(),
		Details: &sellersv1.CalculateSellerScorecardRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			Counterparty: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			Audit:        &commonv1.AuditInfo{CreatedAt: cachedFilter.GetCreatedAfter()},
			Lines:        []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{Attributes: req.GetFilter().GetLabels()}},
		},
	}
	calculateSellerScorecardReply, err := grpcx.Call(ctx, s.sellerScorecardCalculationClient.CalculateSellerScorecard, calculateSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/CalculateSellerScorecard")
	}
	return &sellersv1.ListSellerScorecardConflictsResponse{
		SellerScorecards: []*sellersv1.SellerScorecard{&sellersv1.SellerScorecard{
			Status: calculateSellerScorecardReply.GetSellerScorecard().GetStatus(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
