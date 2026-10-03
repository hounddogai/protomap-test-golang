package orders

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// OrderAmendmentValidationServer implements `acme.orders.v1.OrderAmendmentValidationService`.
// Checks order amendments against the rules before anything acts on them.
type OrderAmendmentValidationServer struct {
	ordersv1.UnimplementedOrderAmendmentValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	synonymSyncClient                searchv1.SynonymSyncServiceClient
	orderAmendmentCalculationClient  ordersv1.OrderAmendmentCalculationServiceClient
	meteredWarehouseQuery            meteredWarehouseQuery
	conn                             *grpc.ClientConn
	featureFlagQueryClient           featureflagsv1.FeatureFlagQueryServiceClient
	documentValidationClient         documentsv1.DocumentValidationServiceClient
	orderCalculationPort             orderCalculationPort
	meteredOrderAmendmentCalculation meteredOrderAmendmentCalculation
	synonymNotificationClient        searchv1.SynonymNotificationServiceClient
	ledgerAccountValidationClient    ledgerv1.LedgerAccountValidationServiceClient
	accessPolicyClient               authorizationv1.AccessPolicyServiceClient
	sellerValidationClient           sellersv1.SellerValidationServiceClient
	clients                          *registry.Registry
}

// NewOrderAmendmentValidationServer wires an OrderAmendmentValidationServer.
func NewOrderAmendmentValidationServer(deps platform.Deps) *OrderAmendmentValidationServer {
	return &OrderAmendmentValidationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.orders.v1.OrderAmendmentValidationService"),
		synonymSyncClient:                searchv1.NewSynonymSyncServiceClient(deps.Conn),
		orderAmendmentCalculationClient:  ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn),
		meteredWarehouseQuery:            meteredWarehouseQuery{WarehouseQueryServiceClient: warehousingv1.NewWarehouseQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                             deps.Conn,
		featureFlagQueryClient:           featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		documentValidationClient:         documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		orderCalculationPort:             newOrderCalculationPort(deps),
		meteredOrderAmendmentCalculation: meteredOrderAmendmentCalculation{OrderAmendmentCalculationServiceClient: ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		synonymNotificationClient:        searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		ledgerAccountValidationClient:    ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		accessPolicyClient:               authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		sellerValidationClient:           sellersv1.NewSellerValidationServiceClient(deps.Conn),
		clients:                          deps.Clients,
	}
}

// ValidateOrderAmendment validates an order amendment.
func (s *OrderAmendmentValidationServer) ValidateOrderAmendment(ctx context.Context, req *ordersv1.ValidateOrderAmendmentRequest) (*ordersv1.ValidateOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "validate order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	pushSynonymReply, err := s.synonymSyncClient.PushSynonym(ctx, &searchv1.PushSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetOrderAmendment().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetOrderAmendment().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetOrderAmendment().GetCounterparty().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PushSynonym")
	}
	return &ordersv1.ValidateOrderAmendmentResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			Lines: []*ordersv1.OrderAmendmentLine{&ordersv1.OrderAmendmentLine{
				Amount: pushSynonymReply.GetSynonym().GetDiscount().GetAmount(),
			}},
			OrderAmendmentId: req.GetOrderAmendmentId(),
			IssuedAt:         req.GetOrderAmendment().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{Status: pushSynonymReply.GetOutcome().GetStatus()},
	}, nil
}

// PreviewOrderAmendment previews an order amendment.
func (s *OrderAmendmentValidationServer) PreviewOrderAmendment(ctx context.Context, req *ordersv1.PreviewOrderAmendmentRequest) (*ordersv1.PreviewOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "preview order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	getOrderAmendmentRatesRequest := &ordersv1.GetOrderAmendmentRatesRequest{
		Context:          req.GetContext(),
		OrderAmendmentId: req.GetOrderAmendment().GetOrderAmendmentId(),
	}
	getOrderAmendmentRatesReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*ordersv1.GetOrderAmendmentRatesResponse, error) {
		return s.orderAmendmentCalculationClient.GetOrderAmendmentRates(ctx, getOrderAmendmentRatesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/GetOrderAmendmentRates")
	}
	recalculateOrderAmendmentsRequest := buildRecalculateOrderAmendmentsRequestForPreviewOrderAmendment(req, getOrderAmendmentRatesReply)
	recalculateOrderAmendmentsReply, err := s.orderAmendmentCalculationClient.RecalculateOrderAmendments(ctx, recalculateOrderAmendmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/RecalculateOrderAmendments")
	}
	findWarehousesRequest := &warehousingv1.FindWarehousesRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.FindWarehousesRequest_Filter{
			Region: recalculateOrderAmendmentsReply.GetOrderAmendment().GetBillingAddress().GetRegion(),
		},
	}
	findWarehousesReply, err := s.meteredWarehouseQuery.FindWarehouses(ctx, findWarehousesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/FindWarehouses")
	}
	return &ordersv1.PreviewOrderAmendmentResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			Notes: getOrderAmendmentRatesReply.GetOrderAmendment().GetNotes(),
			BillingAddress: &commonv1.PostalAddress{
				Region: recalculateOrderAmendmentsReply.GetOrderAmendment().GetBillingAddress().GetRegion(),
			},
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Metadata:         req.GetOrderAmendment().GetMetadata(),
		},
		Result: &commonv1.OperationResult{
			Details:       findWarehousesReply.GetResult().GetDetails(),
			CorrelationId: findWarehousesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// NormalizeOrderAmendment normalizes an order amendment.
func (s *OrderAmendmentValidationServer) NormalizeOrderAmendment(ctx context.Context, req *ordersv1.NormalizeOrderAmendmentRequest) (*ordersv1.NormalizeOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "normalize order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetOrderAmendment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded ordersv1.OrderAmendment
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	source := req.GetDetails().GetContact().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	explainPayoutCalculationRequest := &paymentsv1.ExplainPayoutCalculationRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Settlement:     &paymentsv1.Payout_Card{Card: &paymentsv1.PayoutCardSettlement{Last4: req.GetOrderAmendment().GetCard().GetLast4()}},
			BillingAddress: &commonv1.PostalAddress{Lines: (&decoded).GetBillingAddress().GetLines()},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetOrderAmendment().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &paymentsv1.ExplainPayoutCalculationRequest_Details{Contact: &commonv1.ContactInfo{Email: email}},
	}
	explainPayoutCalculationReply := new(paymentsv1.ExplainPayoutCalculationResponse)
	if err := s.conn.Invoke(ctx, paymentsPayoutCalculationServiceExplainPayoutCalculationMethod, explainPayoutCalculationRequest, explainPayoutCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/ExplainPayoutCalculation")
	}
	suggestFeatureFlagsRequest := buildSuggestFeatureFlagsRequestForNormalizeOrderAmendment(req, explainPayoutCalculationReply)
	suggestFeatureFlagsReply, err := s.featureFlagQueryClient.SuggestFeatureFlags(ctx, suggestFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/SuggestFeatureFlags")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*documentsv1.CheckDocumentEligibilityResponse, len(req.GetOrderAmendment().GetLines()))
	for i, line := range req.GetOrderAmendment().GetLines() {
		group.Go(func() error {
			reply, err := s.documentValidationClient.CheckDocumentEligibility(groupCtx, &documentsv1.CheckDocumentEligibilityRequest{
				Context: req.GetContext(),
				Document: &documentsv1.Document{
					Lines: []*documentsv1.DocumentLine{&documentsv1.DocumentLine{
						Tax:    &documentsv1.DocumentLineTax{Rate: line.GetTax().GetRate()},
						Amount: line.GetTax().GetAmount(),
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
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/CheckDocumentEligibility")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &ordersv1.NormalizeOrderAmendmentResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: explainPayoutCalculationReply.GetPayout().GetBillingAddress().GetCountryCode(),
			},
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Notes:            req.GetOrderAmendment().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: suggestFeatureFlagsReply.GetResult().GetStatus()},
	}, nil
}

// ExplainOrderAmendmentDecision explains an order amendment.
func (s *OrderAmendmentValidationServer) ExplainOrderAmendmentDecision(ctx context.Context, req *ordersv1.ExplainOrderAmendmentDecisionRequest) (*ordersv1.ExplainOrderAmendmentDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "explain order amendment decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	estimateOrderRequest := &ordersv1.EstimateOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: req.GetOrderAmendment().GetBillingAddress().GetRecipient(),
			},
		},
	}
	estimateOrderReply, err := s.orderCalculationPort.EstimateOrder(ctx, estimateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/EstimateOrder")
	}
	var bic string
	switch choice := req.GetOrderAmendment().GetSettlement().(type) {
	case *ordersv1.OrderAmendment_Card:
		bic = choice.Card.GetHolderName()
	case *ordersv1.OrderAmendment_Bank:
		bic = choice.Bank.GetBic()
	case *ordersv1.OrderAmendment_VoucherCode:
		bic = choice.VoucherCode
	}
	recalculateOrderAmendmentsRequest := &ordersv1.RecalculateOrderAmendmentsRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Settlement: &ordersv1.OrderAmendment_Bank{Bank: &ordersv1.OrderAmendmentBankSettlement{Bic: bic}},
		},
	}
	recalculateOrderAmendmentsReply, err := s.meteredOrderAmendmentCalculation.RecalculateOrderAmendments(ctx, recalculateOrderAmendmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/RecalculateOrderAmendments")
	}
	return &ordersv1.ExplainOrderAmendmentDecisionResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			BillingAddress:   estimateOrderReply.GetOrder().GetBillingAddress(),
			Metadata:         recalculateOrderAmendmentsReply.GetOrderAmendment().GetMetadata(),
			OrderAmendmentId: req.GetOrderAmendmentId(),
			DueAt:            req.GetOrderAmendment().GetDueAt(),
		},
		Outcome: &ordersv1.ExplainOrderAmendmentDecisionResponse_Outcome{Status: estimateOrderReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreOrderAmendment scores an order amendment.
func (s *OrderAmendmentValidationServer) ScoreOrderAmendment(ctx context.Context, req *ordersv1.ScoreOrderAmendmentRequest) (*ordersv1.ScoreOrderAmendmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "score order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	subscribeSynonymUpdatesReply, err := s.synonymNotificationClient.SubscribeSynonymUpdates(ctx, &searchv1.SubscribeSynonymUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.SubscribeSynonymUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: req.GetOrderAmendment().GetMetadata(),
		},
		Annotations: annotations,
		Reason:      req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/SubscribeSynonymUpdates")
	}
	annotations2 := make(map[string]string, len(req.GetAnnotations()))
	for key2, value2 := range req.GetAnnotations() {
		annotations2[key2] = value2
	}
	summarizeFeatureFlagsReply, err := s.featureFlagQueryClient.SummarizeFeatureFlags(ctx, &featureflagsv1.SummarizeFeatureFlagsRequest{
		Context:     req.GetContext(),
		Reason:      subscribeSynonymUpdatesReply.GetSynonym().GetBlock().GetReason(),
		Annotations: annotations2,
		Details: &featureflagsv1.SummarizeFeatureFlagsRequest_Details{
			Period: subscribeSynonymUpdatesReply.GetSynonym().GetEffective(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/SummarizeFeatureFlags")
	}
	calculateOrderAmendmentReply, err := s.orderAmendmentCalculationClient.CalculateOrderAmendment(ctx, &ordersv1.CalculateOrderAmendmentRequest{
		Context: req.GetContext(),
		Details: &ordersv1.CalculateOrderAmendmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: subscribeSynonymUpdatesReply.GetSynonym().GetAudit().GetCreatedAt(),
				Etag:      subscribeSynonymUpdatesReply.GetSynonym().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Lines: req.GetOrderAmendment().GetBillingAddress().GetLines(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/CalculateOrderAmendment")
	}
	validateLedgerAccountReply, err := s.ledgerAccountValidationClient.ValidateLedgerAccount(ctx, &ledgerv1.ValidateLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Holder: &ledgerv1.PostingBatchRef{
				DisplayName: subscribeSynonymUpdatesReply.GetSynonym().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateOrderAmendmentReply.GetOrderAmendment().GetAudit().GetUpdatedBy(),
			},
			Contact: &commonv1.ContactInfo{
				Email: subscribeSynonymUpdatesReply.GetSynonym().GetOwner().GetEmail(),
			},
			Funding: &ledgerv1.LedgerAccount_Card{Card: &ledgerv1.LedgerAccountCardFunding{Token: req.GetOrderAmendment().GetCard().GetToken()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ValidateLedgerAccount")
	}
	return &ordersv1.ScoreOrderAmendmentResponse{
		Result: &commonv1.OperationResult{
			Message:       subscribeSynonymUpdatesReply.GetResult().GetMessage(),
			ReferenceIds:  summarizeFeatureFlagsReply.GetResult().GetReferenceIds(),
			CorrelationId: validateLedgerAccountReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				Etag: subscribeSynonymUpdatesReply.GetSynonym().GetAudit().GetEtag(),
			},
			Counterparty: &ordersv1.BackorderRef{
				DisplayName: calculateOrderAmendmentReply.GetOrderAmendment().GetCounterparty().GetDisplayName(),
			},
			Number:           validateLedgerAccountReply.GetLedgerAccount().GetHolder().GetNumber(),
			OrderAmendmentId: req.GetOrderAmendmentId(),
		},
		Breakdown: &ordersv1.ScoreOrderAmendmentResponse_Breakdown{
			Total:        calculateOrderAmendmentReply.GetBreakdown().GetTotal(),
			CurrencyCode: req.GetDetails().GetCurrencyCode(),
		},
	}, nil
}

// LintOrderAmendments lints an order amendment.
func (s *OrderAmendmentValidationServer) LintOrderAmendments(ctx context.Context, req *ordersv1.LintOrderAmendmentsRequest) (*ordersv1.LintOrderAmendmentsResponse, error) {
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "lint order amendments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded ordersv1.LintOrderAmendmentsRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	snapshot, err := protojson.Marshal(req.GetOrderAmendment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullSynonymsRequest := &searchv1.PullSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetOrderAmendment().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetOrderAmendment().GetAudit().GetCreatedBy(),
			},
			Priority: req.GetOrderAmendment().GetCounterparty().GetPriority(),
			Action:   &searchv1.Synonym_Block{Block: &searchv1.SynonymBlockAction{NotifyEmail: (&decoded).GetContact().GetEmail()}},
		},
		Details: &searchv1.PullSynonymsRequest_Details{Payload: snapshot},
	}
	pullSynonymsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*searchv1.PullSynonymsResponse, error) {
		return s.synonymSyncClient.PullSynonyms(ctx, pullSynonymsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PullSynonyms")
	}
	listAccessPoliciesReply, err := s.accessPolicyClient.ListAccessPolicies(ctx, &authorizationv1.ListAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.ListAccessPoliciesRequest_Filter{
			Region:       req.GetOrderAmendment().GetBillingAddress().GetRegion(),
			CreatedAfter: pullSynonymsReply.GetSynonym().GetAudit().GetCreatedAt(),
			Labels:       pullSynonymsReply.GetSynonym().GetParameters(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/ListAccessPolicies")
	}
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetContact().GetPhone())
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.LintOrderAmendmentsRequest_Details)
	source := req.GetOrderAmendment().GetAudit().ProtoReflect()
	updatedBy := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("updated_by"))).String()
	validateSellerRequest := &sellersv1.ValidateSellerRequest{
		Context: req.GetContext(),
		Details: &sellersv1.ValidateSellerRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone:            tenancy.Value(ctx, "phone"),
				PreferredChannel: cloned.GetContact().GetPreferredChannel(),
			},
		},
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{Email: pullSynonymsReply.GetSynonym().GetOwner().GetEmail()},
			Audit:               &commonv1.AuditInfo{UpdatedBy: updatedBy},
		},
	}
	validateSellerReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*sellersv1.ValidateSellerResponse, error) {
		return s.sellerValidationClient.ValidateSeller(ctx, validateSellerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/ValidateSeller")
	}
	warehouseQuery, err := registry.Lookup[warehousingv1.WarehouseQueryServiceClient](s.clients, "acme.warehousing.v1.WarehouseQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	findWarehousesRequest := &warehousingv1.FindWarehousesRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.FindWarehousesRequest_Filter{
			Region:       validateSellerReply.GetSeller().GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetOrderAmendment().GetAudit().GetCreatedAt(),
		},
	}
	findWarehousesReply, err := warehouseQuery.FindWarehouses(ctx, findWarehousesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/FindWarehouses")
	}
	return &ordersv1.LintOrderAmendmentsResponse{
		Outcome: &ordersv1.LintOrderAmendmentsResponse_Outcome{Status: pullSynonymsReply.GetOutcome().GetStatus()},
		Result: &commonv1.OperationResult{
			ReferenceIds:  pullSynonymsReply.GetResult().GetReferenceIds(),
			CorrelationId: listAccessPoliciesReply.GetResult().GetCorrelationId(),
			Message:       listAccessPoliciesReply.GetResult().GetMessage(),
			Details:       findWarehousesReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validateSellerReply.GetSeller().GetAudit().GetUpdatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: validateSellerReply.GetSeller().GetPrimaryAddress().GetRecipient(),
			},
			OrderAmendmentId: req.GetOrderAmendmentId(),
			DueAt:            req.GetOrderAmendment().GetDueAt(),
		},
	}, nil
}

// GetOrderAmendmentRules returns an order amendment.
func (s *OrderAmendmentValidationServer) GetOrderAmendmentRules(ctx context.Context, req *ordersv1.GetOrderAmendmentRulesRequest) (*ordersv1.GetOrderAmendmentRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "get_order_amendment_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order amendment rules", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.GetOrderAmendmentRulesResponse{
		OrderAmendment: &ordersv1.OrderAmendment{OrderAmendmentId: req.GetOrderAmendmentId()},
		Result:         &commonv1.OperationResult{Status: "ok"},
	}, nil
}
