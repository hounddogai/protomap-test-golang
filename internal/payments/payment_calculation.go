package payments

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	concessionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/concessions/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	personalizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/personalization/v1"
	recruitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recruiting/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PaymentCalculationServer implements `acme.payments.v1.PaymentCalculationService`.
// Computes the figures of a payment, such as totals, taxes, and estimates.
type PaymentCalculationServer struct {
	paymentsv1.UnimplementedPaymentCalculationServiceServer
	store                       *Store
	log                         *slog.Logger
	sessionValidationClient     identityv1.SessionValidationServiceClient
	meteredJobOpening           meteredJobOpening
	clients                     *registry.Registry
	sessionQueryClient          identityv1.SessionQueryServiceClient
	documentValidationClient    documentsv1.DocumentValidationServiceClient
	userValidationClient        identityv1.UserValidationServiceClient
	accessPolicyClient          authorizationv1.AccessPolicyServiceClient
	accessPolicyClientFallback  authorizationv1.AccessPolicyServiceClient
	fraudSignalValidationClient fraudv1.FraudSignalValidationServiceClient
	profileCalculationClient    personalizationv1.ProfileCalculationServiceClient
	concessionStandClient       concessionsv1.ConcessionStandServiceClient
	subscriptionPlanQueryClient subscriptionsv1.SubscriptionPlanQueryServiceClient
	countSessionsFn             func(context.Context, *identityv1.CountSessionsRequest, ...grpc.CallOption) (*identityv1.CountSessionsResponse, error)
}

// NewPaymentCalculationServer wires a PaymentCalculationServer.
func NewPaymentCalculationServer(deps platform.Deps) *PaymentCalculationServer {
	sessionQuery := identityv1.NewSessionQueryServiceClient(deps.Conn)
	return &PaymentCalculationServer{
		store:                       NewStore(deps.DB),
		log:                         deps.Logger.With("service", "acme.payments.v1.PaymentCalculationService"),
		sessionValidationClient:     identityv1.NewSessionValidationServiceClient(deps.Conn),
		meteredJobOpening:           meteredJobOpening{JobOpeningServiceClient: recruitingv1.NewJobOpeningServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                     deps.Clients,
		sessionQueryClient:          identityv1.NewSessionQueryServiceClient(deps.Conn),
		documentValidationClient:    documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		userValidationClient:        identityv1.NewUserValidationServiceClient(deps.Conn),
		accessPolicyClient:          authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		accessPolicyClientFallback:  authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		fraudSignalValidationClient: fraudv1.NewFraudSignalValidationServiceClient(deps.Conn),
		profileCalculationClient:    personalizationv1.NewProfileCalculationServiceClient(deps.Conn),
		concessionStandClient:       concessionsv1.NewConcessionStandServiceClient(deps.Conn),
		subscriptionPlanQueryClient: subscriptionsv1.NewSubscriptionPlanQueryServiceClient(deps.Conn),
		countSessionsFn:             sessionQuery.CountSessions,
	}
}

// CalculatePayment calculates a payment.
func (s *PaymentCalculationServer) CalculatePayment(ctx context.Context, req *paymentsv1.CalculatePaymentRequest) (*paymentsv1.CalculatePaymentResponse, error) {
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "calculate payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPayment()).(*paymentsv1.Payment)
	lintSessionsRequest := &identityv1.LintSessionsRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Audit: &commonv1.AuditInfo{
				CreatedBy: cloned.GetAudit().GetCreatedBy(),
				CreatedAt: req.GetPayment().GetAudit().GetCreatedAt(),
			},
			Origin: &identityv1.Session_Customer{Customer: &identityv1.ApiCredentialRef{
				DisplayName: req.GetPayment().GetCounterparty().GetDisplayName(),
			}},
			Sla: &identityv1.SessionSla{DueAt: req.GetPayment().GetDueAt()},
		},
	}
	type lintSessionsResult struct {
		reply *identityv1.LintSessionsResponse
		err   error
	}
	lintSessionsResults := make(chan lintSessionsResult, 1)
	go func() {
		reply, err := s.sessionValidationClient.LintSessions(ctx, lintSessionsRequest)
		lintSessionsResults <- lintSessionsResult{reply, err}
	}()
	var lintSessionsReply *identityv1.LintSessionsResponse
	select {
	case result := <-lintSessionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.SessionValidationService/LintSessions")
		}
		lintSessionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	var accountHolder string
	switch choice := req.GetPayment().GetSettlement().(type) {
	case *paymentsv1.Payment_Card:
		accountHolder = choice.Card.GetHolderName()
	case *paymentsv1.Payment_Bank:
		accountHolder = choice.Bank.GetAccountHolder()
	case *paymentsv1.Payment_VoucherCode:
		accountHolder = choice.VoucherCode
	}
	updateJobOpeningRequest := &recruitingv1.UpdateJobOpeningRequest{
		Context: req.GetContext(),
		JobOpening: &recruitingv1.JobOpening{
			BillingAddress: &commonv1.PostalAddress{
				Locality:    req.GetDetails().GetDestination().GetLocality(),
				CountryCode: req.GetPayment().GetBillingAddress().GetCountryCode(),
			},
			Settlement: &recruitingv1.JobOpening_Bank{Bank: &recruitingv1.JobOpeningBankSettlement{AccountHolder: accountHolder}},
		},
	}
	updateJobOpeningReply, err := s.meteredJobOpening.UpdateJobOpening(ctx, updateJobOpeningRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.recruiting.v1.JobOpeningService/UpdateJobOpening")
	}
	searchIndexNotification, err := registry.Lookup[searchv1.SearchIndexNotificationServiceClient](s.clients, "acme.search.v1.SearchIndexNotificationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	packed, err := anypb.New(req.GetPayment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	unsubscribeSearchIndexUpdatesRequest := &searchv1.UnsubscribeSearchIndexUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSearchIndexUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: lintSessionsReply.GetSession().GetRequester().GetPreferredChannel(),
			},
			Channel: lintSessionsReply.GetSession().GetChannel().GetChannel(),
			Payload: packed,
		},
	}
	unsubscribeSearchIndexUpdatesReply, err := searchIndexNotification.UnsubscribeSearchIndexUpdates(ctx, unsubscribeSearchIndexUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/UnsubscribeSearchIndexUpdates")
	}
	return &paymentsv1.CalculatePaymentResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: lintSessionsReply.GetResult().GetCorrelationId(),
			Details:       unsubscribeSearchIndexUpdatesReply.GetResult().GetDetails(),
			Message:       unsubscribeSearchIndexUpdatesReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Payment: &paymentsv1.Payment{
			Notes: updateJobOpeningReply.GetJobOpening().GetNotes(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: updateJobOpeningReply.GetJobOpening().GetAudit().GetUpdatedAt(),
			},
			PaymentId: req.GetPaymentId(),
		},
		Breakdown: &paymentsv1.CalculatePaymentResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// EstimatePayment estimates a payment.
func (s *PaymentCalculationServer) EstimatePayment(ctx context.Context, req *paymentsv1.EstimatePaymentRequest) (*paymentsv1.EstimatePaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "estimate payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*identityv1.SuggestSessionsResponse, len(req.GetPayment().GetLines()))
	for i, line := range req.GetPayment().GetLines() {
		group.Go(func() error {
			reply, err := s.sessionQueryClient.SuggestSessions(groupCtx, &identityv1.SuggestSessionsRequest{
				Context: req.GetContext(),
				Filter: &identityv1.SuggestSessionsRequest_Filter{
					Region:  req.GetPayment().GetBillingAddress().GetRegion(),
					OwnerId: line.GetLineId(),
					Labels:  line.GetAttributes(),
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
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/SuggestSessions")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	var last4 string
	switch choice := req.GetPayment().GetSettlement().(type) {
	case *paymentsv1.Payment_Card:
		last4 = choice.Card.GetLast4()
	case *paymentsv1.Payment_Bank:
		last4 = choice.Bank.GetIban()
	case *paymentsv1.Payment_VoucherCode:
		last4 = choice.VoucherCode
	}
	validateDocumentRequest := &documentsv1.ValidateDocumentRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPayment().GetAudit().GetUpdatedAt()},
			Settlement: &documentsv1.Document_Card{Card: &documentsv1.DocumentCardSettlement{
				Token: req.GetPayment().GetCard().GetToken(),
				Last4: last4,
			}},
			BillingAddress: &commonv1.PostalAddress{Lines: req.GetPayment().GetBillingAddress().GetLines()},
		},
	}
	send := s.validateDocumentSender(validateDocumentRequest)
	validateDocumentReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/ValidateDocument")
	}
	return &paymentsv1.EstimatePaymentResponse{
		Payment: &paymentsv1.Payment{
			IssuedAt: validateDocumentReply.GetDocument().GetIssuedAt(),
			BillingAddress: &commonv1.PostalAddress{
				Recipient: validateDocumentReply.GetDocument().GetBillingAddress().GetRecipient(),
			},
			PaymentId: req.GetPaymentId(),
			Notes:     req.GetPayment().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuotePayment quotes a payment.
func (s *PaymentCalculationServer) QuotePayment(ctx context.Context, req *paymentsv1.QuotePaymentRequest) (*paymentsv1.QuotePaymentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "quote payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*paymentsv1.QuotePaymentRequest_Details)
	normalizeUserRequest := &identityv1.NormalizeUserRequest{}
	normalizeUserRequest.Context = req.GetContext()
	normalizeUserRequest.User = &identityv1.User{
		PrimaryAddress: &commonv1.PostalAddress{
			Recipient: cloned.GetDestination().GetRecipient(),
			Lines:     req.GetPayment().GetBillingAddress().GetLines(),
		},
		IdentityDocument: &identityv1.User_Passport{Passport: &identityv1.UserPassport{Number: req.GetPayment().GetCounterparty().GetNumber()}},
		Audit:            &commonv1.AuditInfo{UpdatedAt: req.GetPayment().GetAudit().GetUpdatedAt()},
	}
	normalizeUserReply, err := s.userValidationClient.NormalizeUser(ctx, normalizeUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/NormalizeUser")
	}
	return &paymentsv1.QuotePaymentResponse{
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				Number: normalizeUserReply.GetUser().GetNationalId().GetNumber(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizeUserReply.GetUser().GetAudit().GetUpdatedAt(),
			},
			PaymentId: req.GetPaymentId(),
			Total:     req.GetPayment().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculatePayments recalculates a payment.
func (s *PaymentCalculationServer) RecalculatePayments(ctx context.Context, req *paymentsv1.RecalculatePaymentsRequest) (*paymentsv1.RecalculatePaymentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "recalculate payments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *authorizationv1.CloneAccessPolicyRequest, ...grpc.CallOption) (*authorizationv1.CloneAccessPolicyResponse, error){
		"primary":  s.accessPolicyClient.CloneAccessPolicy,
		"fallback": s.accessPolicyClientFallback.CloneAccessPolicy,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloneAccessPolicyRequest := &authorizationv1.CloneAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPayment().GetAudit().GetUpdatedBy()},
		},
	}
	cloneAccessPolicyReply, err := route(ctx, cloneAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/CloneAccessPolicy")
	}
	return &paymentsv1.RecalculatePaymentsResponse{
		Result: &commonv1.OperationResult{
			Message: cloneAccessPolicyReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cloneAccessPolicyReply.GetAccessPolicy().GetAudit().GetUpdatedBy(),
			},
			PaymentId:    req.GetPaymentId(),
			Counterparty: req.GetPayment().GetCounterparty(),
		},
	}, nil
}

// SimulatePayment simulates a payment.
func (s *PaymentCalculationServer) SimulatePayment(ctx context.Context, req *paymentsv1.SimulatePaymentRequest) (*paymentsv1.SimulatePaymentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "simulate payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	previewFraudSignalRequest := &fraudv1.PreviewFraudSignalRequest{}
	previewFraudSignalRequest.Context = req.GetContext()
	previewFraudSignalRequest.FraudSignal = &fraudv1.FraudSignal{
		Subject: &fraudv1.FraudRuleRef{
			DisplayName: req.GetPayment().GetCounterparty().GetDisplayName(),
		},
	}
	previewFraudSignalRequest.Reason = tenancy.Value(ctx, "reason")
	previewFraudSignalReply, err := s.fraudSignalValidationClient.PreviewFraudSignal(ctx, previewFraudSignalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalValidationService/PreviewFraudSignal")
	}
	encoded, err := protojson.Marshal(req.GetPayment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.Payment
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	estimateProfileRequest := &personalizationv1.EstimateProfileRequest{}
	estimateProfileRequest.Context = req.GetContext()
	estimateProfileRequest.Details = &personalizationv1.EstimateProfileRequest_Details{
		Destination: &commonv1.PostalAddress{
			Lines:      req.GetPayment().GetBillingAddress().GetLines(),
			PostalCode: req.GetPayment().GetBillingAddress().GetPostalCode(),
		},
	}
	estimateProfileRequest.Profile = &personalizationv1.Profile{
		Audit: &commonv1.AuditInfo{CreatedBy: (&decoded).GetAudit().GetCreatedBy()},
	}
	estimateProfileReply, err := s.profileCalculationClient.EstimateProfile(ctx, estimateProfileRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.personalization.v1.ProfileCalculationService/EstimateProfile")
	}
	encoded2, err := protojson.Marshal(req.GetPayment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 paymentsv1.Payment
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	updateConcessionStandReply, err := s.concessionStandClient.UpdateConcessionStand(ctx, &concessionsv1.UpdateConcessionStandRequest{
		Context: req.GetContext(),
		ConcessionStand: &concessionsv1.ConcessionStand{
			Audit: &commonv1.AuditInfo{UpdatedBy: (&decoded2).GetAudit().GetUpdatedBy()},
			Locator: &concessionsv1.ConcessionStand_Address{Address: &commonv1.PostalAddress{
				CountryCode: req.GetPayment().GetBillingAddress().GetCountryCode(),
				Locality:    req.GetPayment().GetBillingAddress().GetLocality(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.concessions.v1.ConcessionStandService/UpdateConcessionStand")
	}
	return &paymentsv1.SimulatePaymentResponse{
		Result: &commonv1.OperationResult{
			Status:       previewFraudSignalReply.GetOutcome().GetStatus(),
			ReferenceIds: estimateProfileReply.GetResult().GetReferenceIds(),
			Message:      updateConcessionStandReply.GetResult().GetMessage(),
		},
		Payment: &paymentsv1.Payment{
			Lines: []*paymentsv1.PaymentLine{&paymentsv1.PaymentLine{
				Attributes: previewFraudSignalReply.GetFraudSignal().GetAttributes(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: updateConcessionStandReply.GetConcessionStand().GetAddress().GetPostalCode(),
			},
			PaymentId: req.GetPaymentId(),
			Notes:     req.GetPayment().GetNotes(),
		},
		Breakdown: &paymentsv1.SimulatePaymentResponse_Breakdown{
			CurrencyCode: estimateProfileReply.GetBreakdown().GetCurrencyCode(),
		},
	}, nil
}

// ExplainPaymentCalculation explains a payment.
func (s *PaymentCalculationServer) ExplainPaymentCalculation(ctx context.Context, req *paymentsv1.ExplainPaymentCalculationRequest) (*paymentsv1.ExplainPaymentCalculationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "explain payment calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	for line := range iterx.Each(req.GetPayment().GetLines(), func(candidate *paymentsv1.PaymentLine) bool { return candidate != nil }) {
		if _, err := s.subscriptionPlanQueryClient.BrowseSubscriptionPlans(ctx, &subscriptionsv1.BrowseSubscriptionPlansRequest{
			Context: req.GetContext(),
			Filter: &subscriptionsv1.BrowseSubscriptionPlansRequest_Filter{
				Region:  req.GetPayment().GetBillingAddress().GetRegion(),
				Labels:  line.GetAttributes(),
				OwnerId: line.GetLineId(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/BrowseSubscriptionPlans")
		}
	}
	cloneAccessPolicyRequest := buildCloneAccessPolicyRequestForExplainPaymentCalculation(req)
	cloneAccessPolicyReply, err := s.accessPolicyClient.CloneAccessPolicy(ctx, cloneAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/CloneAccessPolicy")
	}
	paymentCache := cache.New[string, *paymentsv1.Payment](1)
	paymentCache.Put(req.GetContext().GetTraceId(), req.GetPayment())
	cachedPayment, _ := paymentCache.Get(req.GetContext().GetTraceId())
	countSessionsRequest := &identityv1.CountSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.CountSessionsRequest_Filter{
			Region:       cachedPayment.GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPayment().GetAudit().GetUpdatedAt(),
			OwnerId:      cloneAccessPolicyReply.GetAccessPolicy().GetOwner().GetUserId(),
		},
	}
	countSessionsReply, err := s.countSessionsFn(ctx, countSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/CountSessions")
	}
	return &paymentsv1.ExplainPaymentCalculationResponse{
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cloneAccessPolicyReply.GetAccessPolicy().GetAudit().GetUpdatedBy(),
			},
			PaymentId: req.GetPaymentId(),
			DueAt:     req.GetPayment().GetDueAt(),
		},
		Result: &commonv1.OperationResult{
			Message: countSessionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetPaymentRates returns a payment.
func (s *PaymentCalculationServer) GetPaymentRates(ctx context.Context, req *paymentsv1.GetPaymentRatesRequest) (*paymentsv1.GetPaymentRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "get_payment_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payment rates", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.GetPaymentRatesResponse{
		Payment: &paymentsv1.Payment{PaymentId: req.GetPaymentId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// validateDocumentSender returns a function that sends one request to
// acme.documents.v1.DocumentValidationService/ValidateDocument.
func (s *PaymentCalculationServer) validateDocumentSender(request *documentsv1.ValidateDocumentRequest) func(context.Context) (*documentsv1.ValidateDocumentResponse, error) {
	return func(ctx context.Context) (*documentsv1.ValidateDocumentResponse, error) {
		return s.documentValidationClient.ValidateDocument(ctx, request)
	}
}
