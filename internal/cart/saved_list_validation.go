package cart

import (
	"context"
	"log/slog"

	bankingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/banking/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	devicesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/devices/v1"
	experimentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/experiments/v1"
	invoicingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/invoicing/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SavedListValidationServer implements `acme.cart.v1.SavedListValidationService`.
// Checks saved lists against the rules before anything acts on them.
type SavedListValidationServer struct {
	cartv1.UnimplementedSavedListValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	conn                             *grpc.ClientConn
	parcelQueryClient                shippingv1.ParcelQueryServiceClient
	savedListCalculationClient       cartv1.SavedListCalculationServiceClient
	meteredSavedListCalculation      meteredSavedListCalculation
	cartValidationClient             cartv1.CartValidationServiceClient
	sellerScorecardCalculationClient sellersv1.SellerScorecardCalculationServiceClient
	pointsLedgerCalculationClient    loyaltyv1.PointsLedgerCalculationServiceClient
	managedDeviceCalculationClient   devicesv1.ManagedDeviceCalculationServiceClient
	invoiceBatchQueryClient          invoicingv1.InvoiceBatchQueryServiceClient
	invoiceBatchQueryClientFallback  invoicingv1.InvoiceBatchQueryServiceClient
	placementQueryClient             merchandisingv1.PlacementQueryServiceClient
	bankAccountClient                bankingv1.BankAccountServiceClient
}

// NewSavedListValidationServer wires a SavedListValidationServer.
func NewSavedListValidationServer(deps platform.Deps) *SavedListValidationServer {
	return &SavedListValidationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.cart.v1.SavedListValidationService"),
		conn:                             deps.Conn,
		parcelQueryClient:                shippingv1.NewParcelQueryServiceClient(deps.Conn),
		savedListCalculationClient:       cartv1.NewSavedListCalculationServiceClient(deps.Conn),
		meteredSavedListCalculation:      meteredSavedListCalculation{SavedListCalculationServiceClient: cartv1.NewSavedListCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		cartValidationClient:             cartv1.NewCartValidationServiceClient(deps.Conn),
		sellerScorecardCalculationClient: sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn),
		pointsLedgerCalculationClient:    loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		managedDeviceCalculationClient:   devicesv1.NewManagedDeviceCalculationServiceClient(deps.Conn),
		invoiceBatchQueryClient:          invoicingv1.NewInvoiceBatchQueryServiceClient(deps.Conn),
		invoiceBatchQueryClientFallback:  invoicingv1.NewInvoiceBatchQueryServiceClient(deps.Conn),
		placementQueryClient:             merchandisingv1.NewPlacementQueryServiceClient(deps.Conn),
		bankAccountClient:                bankingv1.NewBankAccountServiceClient(deps.Conn),
	}
}

// ValidateSavedList validates a saved list.
func (s *SavedListValidationServer) ValidateSavedList(ctx context.Context, req *cartv1.ValidateSavedListRequest) (*cartv1.ValidateSavedListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "validate saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	previewCartRequest := &cartv1.PreviewCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Settlement: &cartv1.Cart_Bank{Bank: &cartv1.CartBankSettlement{Iban: req.GetSavedList().GetBank().GetIban()}},
			Counterparty: &cartv1.SavedListRef{
				DisplayName: req.GetSavedList().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &cartv1.PreviewCartRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}
	previewCartReply := new(cartv1.PreviewCartResponse)
	if err := s.conn.Invoke(ctx, cartCartValidationServicePreviewCartMethod, previewCartRequest, previewCartReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/PreviewCart")
	}
	summarizeExperimentsRequest := &experimentsv1.SummarizeExperimentsRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details:     &experimentsv1.SummarizeExperimentsRequest_Details{Filters: req.GetSavedList().GetMetadata()},
	}
	summarizeExperimentsReply, err := summarizeExperimentsCall(ctx, summarizeExperimentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.experiments.v1.ExperimentQueryService/SummarizeExperiments")
	}
	resolveParcelsRequest := &shippingv1.ResolveParcelsRequest{Context: req.GetContext()}
	resolveParcelsReply, err := grpcx.Call(ctx, s.parcelQueryClient.ResolveParcels, resolveParcelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelQueryService/ResolveParcels")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*cartv1.QuoteSavedListResponse, len(req.GetSavedList().GetAttachments()))
	for i, attachment := range req.GetSavedList().GetAttachments() {
		group.Go(func() error {
			reply, err := s.savedListCalculationClient.QuoteSavedList(groupCtx, &cartv1.QuoteSavedListRequest{
				Context: req.GetContext(),
				SavedList: &cartv1.SavedList{
					Attachments: []*cartv1.SavedListAttachment{&cartv1.SavedListAttachment{
						SizeBytes: attachment.GetSizeBytes(),
						Checksum:  attachment.GetChecksum(),
					}},
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
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListCalculationService/QuoteSavedList")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &cartv1.ValidateSavedListResponse{
		Result: &commonv1.OperationResult{
			Message: previewCartReply.GetOutcome().GetMessage(),
			Details: summarizeExperimentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &cartv1.ValidateSavedListResponse_Outcome{Message: resolveParcelsReply.GetResult().GetMessage()},
		SavedList: &cartv1.SavedList{
			SavedListId: req.GetSavedListId(),
			Metadata:    req.GetSavedList().GetMetadata(),
		},
	}, nil
}

// PreviewSavedList previews a saved list.
func (s *SavedListValidationServer) PreviewSavedList(ctx context.Context, req *cartv1.PreviewSavedListRequest) (*cartv1.PreviewSavedListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "preview saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	quoteSavedListRequest := &cartv1.QuoteSavedListRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Counterparty: &cartv1.CartRuleRef{
				DisplayName: req.GetSavedList().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &cartv1.QuoteSavedListRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: req.GetSavedList().GetBillingAddress().GetLines()},
		},
	}
	quoteSavedListReply, err := s.meteredSavedListCalculation.QuoteSavedList(ctx, quoteSavedListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListCalculationService/QuoteSavedList")
	}
	compareSavedListOptionsReply, err := s.savedListCalculationClient.CompareSavedListOptions(ctx, &cartv1.CompareSavedListOptionsRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			BillingAddress: &commonv1.PostalAddress{
				Region: quoteSavedListReply.GetSavedList().GetBillingAddress().GetRegion(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListCalculationService/CompareSavedListOptions")
	}
	return &cartv1.PreviewSavedListResponse{
		SavedList: &cartv1.SavedList{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: quoteSavedListReply.GetSavedList().GetBillingAddress().GetPostalCode(),
			},
			Counterparty: quoteSavedListReply.GetSavedList().GetCounterparty(),
			Metadata:     compareSavedListOptionsReply.GetSavedList().GetMetadata(),
			SavedListId:  req.GetSavedListId(),
			Notes:        req.GetSavedList().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeSavedList normalizes a saved list.
func (s *SavedListValidationServer) NormalizeSavedList(ctx context.Context, req *cartv1.NormalizeSavedListRequest) (*cartv1.NormalizeSavedListResponse, error) {
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "normalize saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetSavedList())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded cartv1.SavedList
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	source := req.GetSavedList().GetAudit().ProtoReflect()
	createdBy := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("created_by"))).String()
	previewCartReply, err := s.cartValidationClient.PreviewCart(ctx, &cartv1.PreviewCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Settlement: &cartv1.Cart_Bank{Bank: &cartv1.CartBankSettlement{Iban: req.GetSavedList().GetBank().GetIban()}},
			Audit: &commonv1.AuditInfo{
				Etag:      (&decoded).GetAudit().GetEtag(),
				CreatedBy: createdBy,
			},
		},
		Details: &cartv1.PreviewCartRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/PreviewCart")
	}
	for _, attachment := range req.GetSavedList().GetAttachments() {
		if _, err := s.sellerScorecardCalculationClient.RecalculateSellerScorecards(ctx, &sellersv1.RecalculateSellerScorecardsRequest{
			Context: req.GetContext(),
			SellerScorecard: &sellersv1.SellerScorecard{
				Attachments: []*sellersv1.SellerScorecardAttachment{&sellersv1.SellerScorecardAttachment{FileName: attachment.GetFileName()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/RecalculateSellerScorecards")
		}
	}
	return &cartv1.NormalizeSavedListResponse{
		SavedList: &cartv1.SavedList{
			SavedListId:    previewCartReply.GetCart().GetCounterparty().GetSavedListId(),
			IssuedAt:       req.GetSavedList().GetIssuedAt(),
			BillingAddress: req.GetSavedList().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainSavedListDecision explains a saved list.
func (s *SavedListValidationServer) ExplainSavedListDecision(ctx context.Context, req *cartv1.ExplainSavedListDecisionRequest) (*cartv1.ExplainSavedListDecisionResponse, error) {
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "explain saved list decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	source := req.GetSavedList().GetAudit().ProtoReflect()
	etag := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("etag"))).String()
	encoded, err := protojson.Marshal(req.GetSavedList())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded cartv1.SavedList
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	explainPointsLedgerCalculationReply, err := s.pointsLedgerCalculationClient.ExplainPointsLedgerCalculation(ctx, &loyaltyv1.ExplainPointsLedgerCalculationRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Funding: &loyaltyv1.PointsLedger_Card{Card: &loyaltyv1.PointsLedgerCardFunding{Last4: req.GetSavedList().GetCard().GetLast4()}},
			Audit: &commonv1.AuditInfo{
				Etag:      etag,
				UpdatedAt: (&decoded).GetAudit().GetUpdatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/ExplainPointsLedgerCalculation")
	}
	estimateManagedDeviceRequest := &devicesv1.EstimateManagedDeviceRequest{
		Context: req.GetContext(),
		Details: &devicesv1.EstimateManagedDeviceRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetSavedList().GetBillingAddress().GetCountryCode(),
			},
		},
		ManagedDevice: &devicesv1.ManagedDevice{
			Locator: &devicesv1.ManagedDevice_Address{Address: &commonv1.PostalAddress{
				Locality: req.GetSavedList().GetBillingAddress().GetLocality(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag: explainPointsLedgerCalculationReply.GetPointsLedger().GetAudit().GetEtag(),
			},
		},
	}
	estimateManagedDeviceReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*devicesv1.EstimateManagedDeviceResponse, error) {
		return s.managedDeviceCalculationClient.EstimateManagedDevice(ctx, estimateManagedDeviceRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.devices.v1.ManagedDeviceCalculationService/EstimateManagedDevice")
	}
	return &cartv1.ExplainSavedListDecisionResponse{
		Outcome: &cartv1.ExplainSavedListDecisionResponse_Outcome{
			EffectiveAt: explainPointsLedgerCalculationReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message: explainPointsLedgerCalculationReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		SavedList: &cartv1.SavedList{
			Counterparty: &cartv1.CartRuleRef{
				DisplayName: estimateManagedDeviceReply.GetManagedDevice().GetOwner().GetDisplayName(),
			},
			Total:       estimateManagedDeviceReply.GetBreakdown().GetTotal(),
			SavedListId: req.GetSavedListId(),
			Notes:       req.GetSavedList().GetNotes(),
		},
	}, nil
}

// ScoreSavedList scores a saved list.
func (s *SavedListValidationServer) ScoreSavedList(ctx context.Context, req *cartv1.ScoreSavedListRequest) (*cartv1.ScoreSavedListResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "score saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *invoicingv1.BrowseInvoiceBatchesRequest, ...grpc.CallOption) (*invoicingv1.BrowseInvoiceBatchesResponse, error){
		"primary":  s.invoiceBatchQueryClient.BrowseInvoiceBatches,
		"fallback": s.invoiceBatchQueryClientFallback.BrowseInvoiceBatches,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	browseInvoiceBatchesRequest := &invoicingv1.BrowseInvoiceBatchesRequest{
		Context: req.GetContext(),
		Filter: &invoicingv1.BrowseInvoiceBatchesRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetSavedList().GetAudit().GetCreatedAt(),
		},
	}
	browseInvoiceBatchesReply, err := route(ctx, browseInvoiceBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.invoicing.v1.InvoiceBatchQueryService/BrowseInvoiceBatches")
	}
	return &cartv1.ScoreSavedListResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: browseInvoiceBatchesReply.GetResult().GetReferenceIds(),
			Status:       browseInvoiceBatchesReply.GetResult().GetStatus(),
		},
		SavedList: &cartv1.SavedList{
			SavedListId:    req.GetSavedListId(),
			BillingAddress: req.GetSavedList().GetBillingAddress(),
		},
	}, nil
}

// LintSavedLists lints a saved list.
func (s *SavedListValidationServer) LintSavedLists(ctx context.Context, req *cartv1.LintSavedListsRequest) (*cartv1.LintSavedListsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "lint saved lists", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetSavedList())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded cartv1.SavedList
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	previewCartRequest := &cartv1.PreviewCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{Region: (&decoded).GetBillingAddress().GetRegion()},
			Audit:          &commonv1.AuditInfo{UpdatedAt: req.GetSavedList().GetAudit().GetUpdatedAt()},
		},
	}
	previewCartReply := new(cartv1.PreviewCartResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.cart.v1.CartValidationService/PreviewCart", previewCartRequest, previewCartReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/PreviewCart")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizePlacementsRequest := &merchandisingv1.SummarizePlacementsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
	}
	summarizePlacementsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*merchandisingv1.SummarizePlacementsResponse, error) {
		return s.placementQueryClient.SummarizePlacements(ctx, summarizePlacementsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/SummarizePlacements")
	}
	batchGetBankAccountsRequest := &bankingv1.BatchGetBankAccountsRequest{}
	batchGetBankAccountsRequest.Context = req.GetContext()
	batchGetBankAccountsReply, err := s.bankAccountClient.BatchGetBankAccounts(ctx, batchGetBankAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.banking.v1.BankAccountService/BatchGetBankAccounts")
	}
	return &cartv1.LintSavedListsResponse{
		Outcome: &cartv1.LintSavedListsResponse_Outcome{Message: previewCartReply.GetOutcome().GetMessage()},
		Result: &commonv1.OperationResult{
			Status:  summarizePlacementsReply.GetResult().GetStatus(),
			Details: batchGetBankAccountsReply.GetResult().GetDetails(),
		},
		SavedList: &cartv1.SavedList{
			SavedListId: req.GetSavedListId(),
			Number:      req.GetSavedList().GetNumber(),
		},
	}, nil
}

// GetSavedListRules returns a saved list.
func (s *SavedListValidationServer) GetSavedListRules(ctx context.Context, req *cartv1.GetSavedListRulesRequest) (*cartv1.GetSavedListRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSavedListRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "saved_list", req.GetSavedListId(), "get_saved_list_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get saved list rules", "tenant", req.GetContext().GetTenantId())
	return &cartv1.GetSavedListRulesResponse{
		SavedList: &cartv1.SavedList{SavedListId: req.GetSavedListId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}
