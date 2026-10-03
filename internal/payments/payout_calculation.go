package payments

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// PayoutCalculationServer implements `acme.payments.v1.PayoutCalculationService`.
// Computes the figures of a payout, such as totals, taxes, and estimates.
type PayoutCalculationServer struct {
	*payoutCalculationHandlers
}

// payoutCalculationHandlers holds the handlers of PayoutCalculationServer, with what they use.
type payoutCalculationHandlers struct {
	paymentsv1.UnimplementedPayoutCalculationServiceServer
	store                              *Store
	log                                *slog.Logger
	paymentInstrumentCalculationClient paymentsv1.PaymentInstrumentCalculationServiceClient
	paymentValidationClient            paymentsv1.PaymentValidationServiceClient
	clients                            *registry.Registry
	refundValidationClient             paymentsv1.RefundValidationServiceClient
	collectionClient                   merchandisingv1.CollectionServiceClient
	sellerListingCalculationClient     sellersv1.SellerListingCalculationServiceClient
	conn                               *grpc.ClientConn
	collectionQueryOnce                sync.Once
	collectionQueryCached              merchandisingv1.CollectionQueryServiceClient
	paymentCalculationPort             paymentCalculationPort
	documentPort                       documentPort
	tenantNotificationClient           identityv1.TenantNotificationServiceClient
	bus                                *eventbus.Bus
	apiCredentialAdminClient           identityv1.ApiCredentialAdminServiceClient
	paymentCalculationClient           paymentsv1.PaymentCalculationServiceClient
}

// NewPayoutCalculationServer wires a PayoutCalculationServer.
func NewPayoutCalculationServer(deps platform.Deps) *PayoutCalculationServer {
	return &PayoutCalculationServer{payoutCalculationHandlers: &payoutCalculationHandlers{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.payments.v1.PayoutCalculationService"),
		paymentInstrumentCalculationClient: paymentsv1.NewPaymentInstrumentCalculationServiceClient(deps.Conn),
		paymentValidationClient:            paymentsv1.NewPaymentValidationServiceClient(deps.Conn),
		clients:                            deps.Clients,
		refundValidationClient:             paymentsv1.NewRefundValidationServiceClient(deps.Conn),
		collectionClient:                   merchandisingv1.NewCollectionServiceClient(deps.Conn),
		sellerListingCalculationClient:     sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
		conn:                               deps.Conn,
		paymentCalculationPort:             newPaymentCalculationPort(deps),
		documentPort:                       newDocumentPort(deps),
		tenantNotificationClient:           identityv1.NewTenantNotificationServiceClient(deps.Conn),
		bus:                                deps.Bus,
		apiCredentialAdminClient:           identityv1.NewApiCredentialAdminServiceClient(deps.Conn),
		paymentCalculationClient:           paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
	}}
}

// CalculatePayout calculates a payout.
func (s *payoutCalculationHandlers) CalculatePayout(ctx context.Context, req *paymentsv1.CalculatePayoutRequest) (*paymentsv1.CalculatePayoutResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "calculate payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	calculatePaymentInstrumentRequest := buildCalculatePaymentInstrumentRequestForCalculatePayout(req)
	calculatePaymentInstrumentReply, err := s.paymentInstrumentCalculationClient.CalculatePaymentInstrument(ctx, calculatePaymentInstrumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentCalculationService/CalculatePaymentInstrument")
	}
	checkPaymentEligibilityRequest := &paymentsv1.CheckPaymentEligibilityRequest{}
	checkPaymentEligibilityRequest.Context = req.GetContext()
	checkPaymentEligibilityRequest.Details = &paymentsv1.CheckPaymentEligibilityRequest_Details{
		Contact: &commonv1.ContactInfo{
			Phone: calculatePaymentInstrumentReply.GetPaymentInstrument().GetContact().GetPhone(),
		},
	}
	checkPaymentEligibilityReply, err := s.paymentValidationClient.CheckPaymentEligibility(ctx, checkPaymentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/CheckPaymentEligibility")
	}
	refundValidation, ok := s.clients.Get("acme.payments.v1.RefundValidationService").(paymentsv1.RefundValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.payments.v1.RefundValidationService")
	}
	checkRefundEligibilityRequest := &paymentsv1.CheckRefundEligibilityRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				Etag: checkPaymentEligibilityReply.GetPayment().GetAudit().GetEtag(),
			},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: checkPaymentEligibilityReply.GetPayment().GetCounterparty().GetDisplayName(),
			},
		},
	}
	checkRefundEligibilityReply, err := refundValidation.CheckRefundEligibility(ctx, checkRefundEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/CheckRefundEligibility")
	}
	explainRefundDecisionReply, err := s.refundValidationClient.ExplainRefundDecision(ctx, &paymentsv1.ExplainRefundDecisionRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetPayout().GetBillingAddress().GetCountryCode(),
			},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: calculatePaymentInstrumentReply.GetPaymentInstrument().GetHolder().GetDisplayName(),
				CustomerId:  calculatePaymentInstrumentReply.GetPaymentInstrument().GetHolder().GetCustomerId(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/ExplainRefundDecision")
	}
	return &paymentsv1.CalculatePayoutResponse{
		Breakdown: &paymentsv1.CalculatePayoutResponse_Breakdown{
			CurrencyCode: calculatePaymentInstrumentReply.GetBreakdown().GetCurrencyCode(),
		},
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: checkPaymentEligibilityReply.GetPayment().GetAudit().GetUpdatedBy(),
			},
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: checkRefundEligibilityReply.GetRefund().GetCounterparty().GetDisplayName(),
				Number:      req.GetPayout().GetNumber(),
			},
			Number:   explainRefundDecisionReply.GetRefund().GetNumber(),
			PayoutId: req.GetPayoutId(),
		},
		Result: &commonv1.OperationResult{
			Message: checkRefundEligibilityReply.GetResult().GetMessage(),
			Status:  explainRefundDecisionReply.GetResult().GetStatus(),
		},
	}, nil
}

// EstimatePayout estimates a payout.
func (s *payoutCalculationHandlers) EstimatePayout(ctx context.Context, req *paymentsv1.EstimatePayoutRequest) (*paymentsv1.EstimatePayoutResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "estimate payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	for _, line := range req.GetPayout().GetLines() {
		if _, err := s.collectionClient.CloneCollection(ctx, &merchandisingv1.CloneCollectionRequest{
			Context: req.GetContext(),
			Collection: &merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
						Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{Attributes: line.GetAttributes()}},
						Location: &commonv1.PostalAddress{Lines: req.GetDetails().GetDestination().GetLines()},
					}},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionService/CloneCollection")
		}
	}
	source := req.GetDetails().GetDestination().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	explainSellerListingCalculationReply, err := s.sellerListingCalculationClient.ExplainSellerListingCalculation(ctx, &sellersv1.ExplainSellerListingCalculationRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				Locality: req.GetDetails().GetDestination().GetLocality(),
				Region:   region,
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/ExplainSellerListingCalculation")
	}
	resolveCollectionsRequest := &merchandisingv1.ResolveCollectionsRequest{Context: req.GetContext()}
	resolveCollectionsReply, err := s.collectionQueryLazy().ResolveCollections(ctx, resolveCollectionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionQueryService/ResolveCollections")
	}
	cloned := proto.Clone(req.GetPayout()).(*paymentsv1.Payout)
	calculatePaymentRequest := &paymentsv1.CalculatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Audit:          &commonv1.AuditInfo{UpdatedAt: cloned.GetAudit().GetUpdatedAt()},
			BillingAddress: &commonv1.PostalAddress{Lines: req.GetPayout().GetBillingAddress().GetLines()},
		},
	}
	calculatePaymentReply, err := s.paymentCalculationPort.CalculatePayment(ctx, calculatePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/CalculatePayment")
	}
	return &paymentsv1.EstimatePayoutResponse{
		Payout: &paymentsv1.Payout{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: explainSellerListingCalculationReply.GetSellerListing().GetAddress().GetRecipient(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: calculatePaymentReply.GetPayment().GetAudit().GetEtag(),
			},
			PayoutId: req.GetPayoutId(),
			DueAt:    req.GetPayout().GetDueAt(),
		},
		Result: &commonv1.OperationResult{
			Status:        explainSellerListingCalculationReply.GetOutcome().GetStatus(),
			CorrelationId: resolveCollectionsReply.GetResult().GetCorrelationId(),
			Message:       resolveCollectionsReply.GetResult().GetMessage(),
		},
	}, nil
}

// ComparePayoutOptions compares a payout.
func (s *payoutCalculationHandlers) ComparePayoutOptions(ctx context.Context, req *paymentsv1.ComparePayoutOptionsRequest) (*paymentsv1.ComparePayoutOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "compare payout options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPayout())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.Payout
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	listDocumentsRequest := &documentsv1.ListDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.ListDocumentsRequest_Filter{
			Region:  (&decoded).GetBillingAddress().GetRegion(),
			OwnerId: req.GetPayout().GetCounterparty().GetRefundId(),
			Labels:  req.GetPayout().GetMetadata(),
		},
	}
	listDocumentsReply, err := s.documentPort.ListDocuments(ctx, listDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/ListDocuments")
	}
	return &paymentsv1.ComparePayoutOptionsResponse{
		Result: &commonv1.OperationResult{
			Message: listDocumentsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Payout:    &paymentsv1.Payout{PayoutId: req.GetPayoutId()},
		Breakdown: &paymentsv1.ComparePayoutOptionsResponse_Breakdown{Total: req.GetPayout().GetTotal()},
	}, nil
}

// RecalculatePayouts recalculates a payout.
func (s *payoutCalculationHandlers) RecalculatePayouts(ctx context.Context, req *paymentsv1.RecalculatePayoutsRequest) (*paymentsv1.RecalculatePayoutsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculatePayouts", "error", err)
		return nil, err
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "recalculate payouts", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	getTenantPreferencesReply, err := s.tenantNotificationClient.GetTenantPreferences(ctx, &identityv1.GetTenantPreferencesRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/GetTenantPreferences")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "payments.recalculate_payouts.calculate_payment",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.destination.postal_code": req.GetPayout().GetBillingAddress().GetPostalCode(),
			"context.tenant_id":               req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	repairApiCredentialRequest := &identityv1.RepairApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: getTenantPreferencesReply.GetTenant().GetAudit().GetUpdatedBy(),
			},
			Contact: &commonv1.ContactInfo{
				Email: getTenantPreferencesReply.GetTenant().GetManager().GetEmail(),
			},
		},
	}
	repairApiCredentialReply, err := grpcx.Call(ctx, s.apiCredentialAdminClient.RepairApiCredential, repairApiCredentialRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialAdminService/RepairApiCredential")
	}
	return &paymentsv1.RecalculatePayoutsResponse{
		Payout: &paymentsv1.Payout{
			BillingAddress: &commonv1.PostalAddress{
				Region: getTenantPreferencesReply.GetTenant().GetLocation().GetRegion(),
			},
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: getTenantPreferencesReply.GetTenant().GetManager().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: repairApiCredentialReply.GetApiCredential().GetAudit().GetEtag(),
			},
			PayoutId: req.GetPayoutId(),
		},
		Result: &commonv1.OperationResult{
			Message: repairApiCredentialReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Breakdown: &paymentsv1.RecalculatePayoutsResponse_Breakdown{Total: req.GetPayout().GetTotal()},
	}, nil
}

// SimulatePayout simulates a payout.
func (s *payoutCalculationHandlers) SimulatePayout(ctx context.Context, req *paymentsv1.SimulatePayoutRequest) (*paymentsv1.SimulatePayoutResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SimulatePayout", "error", err)
		return nil, err
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "simulate payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	apiCredentialNotification, err := registry.Lookup[identityv1.ApiCredentialNotificationServiceClient](s.clients, "acme.identity.v1.ApiCredentialNotificationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	sendApiCredentialTestNoticeRequest := &identityv1.SendApiCredentialTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendApiCredentialTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Reason:      req.GetReason(),
		Annotations: annotations,
	}
	sendApiCredentialTestNoticeReply, err := apiCredentialNotification.SendApiCredentialTestNotice(ctx, sendApiCredentialTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SendApiCredentialTestNotice")
	}
	ctx = tenancy.With(ctx, "country_code", req.GetDetails().GetDestination().GetCountryCode())
	explainSellerListingCalculationRequest := &sellersv1.ExplainSellerListingCalculationRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Audit:   &commonv1.AuditInfo{UpdatedBy: req.GetPayout().GetAudit().GetUpdatedBy()},
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{CountryCode: tenancy.Value(ctx, "country_code")}},
		},
		Details: &sellersv1.ExplainSellerListingCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: sendApiCredentialTestNoticeReply.GetApiCredential().GetContact().GetEmail(),
			},
		},
	}
	explainSellerListingCalculationReply := new(sellersv1.ExplainSellerListingCalculationResponse)
	if err := grpcx.Invoke(ctx, s.conn, sellersv1.SellerListingCalculationService_ExplainSellerListingCalculation_FullMethodName, explainSellerListingCalculationRequest, explainSellerListingCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/ExplainSellerListingCalculation")
	}
	return &paymentsv1.SimulatePayoutResponse{
		Result: &commonv1.OperationResult{
			Details: sendApiCredentialTestNoticeReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				CreatedBy: sendApiCredentialTestNoticeReply.GetApiCredential().GetAudit().GetCreatedBy(),
				Etag:      explainSellerListingCalculationReply.GetSellerListing().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: explainSellerListingCalculationReply.GetSellerListing().GetAddress().GetRecipient(),
			},
			PayoutId: req.GetPayoutId(),
			Status:   req.GetPayout().GetStatus(),
		},
	}, nil
}

// ExplainPayoutCalculation explains a payout.
func (s *payoutCalculationHandlers) ExplainPayoutCalculation(ctx context.Context, req *paymentsv1.ExplainPayoutCalculationRequest) (*paymentsv1.ExplainPayoutCalculationResponse, error) {
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "explain payout calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	jobs := make(chan *paymentsv1.PayoutAttachment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attachment := range jobs {
				if _, err := s.paymentCalculationClient.EstimatePayment(workCtx, &paymentsv1.EstimatePaymentRequest{
					Context: req.GetContext(),
					Payment: &paymentsv1.Payment{
						Attachments: []*paymentsv1.PaymentAttachment{&paymentsv1.PaymentAttachment{
							Checksum:    attachment.GetChecksum(),
							ContentType: attachment.GetContentType(),
							SizeBytes:   attachment.GetSizeBytes(),
						}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPayout().GetAttachments() {
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
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/EstimatePayment")
	}
	return &paymentsv1.ExplainPayoutCalculationResponse{
		Payout: &paymentsv1.Payout{
			PayoutId: req.GetPayoutId(),
			Status:   req.GetPayout().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// collectionQueryLazy dials acme.merchandising.v1.CollectionQueryService the first time a handler needs it.
func (s *payoutCalculationHandlers) collectionQueryLazy() merchandisingv1.CollectionQueryServiceClient {
	s.collectionQueryOnce.Do(func() { s.collectionQueryCached = merchandisingv1.NewCollectionQueryServiceClient(s.conn) })
	return s.collectionQueryCached
}
