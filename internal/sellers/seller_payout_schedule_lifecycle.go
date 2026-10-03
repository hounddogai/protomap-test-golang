package sellers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	secretsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/secrets/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerPayoutScheduleLifecycleServer implements `acme.sellers.v1.SellerPayoutScheduleLifecycleService`.
// Moves a seller payout schedule between the states of its lifecycle.
type SellerPayoutScheduleLifecycleServer struct {
	sellersv1.UnimplementedSellerPayoutScheduleLifecycleServiceServer
	store                               *Store
	log                                 *slog.Logger
	userQueryClient                     identityv1.UserQueryServiceClient
	promotionQueryClient                promotionsv1.PromotionQueryServiceClient
	clients                             *registry.Registry
	giftCardBatchApprovalClient         giftcardsv1.GiftCardBatchApprovalServiceClient
	giftCardBatchApprovalClientFallback giftcardsv1.GiftCardBatchApprovalServiceClient
	preferenceCalculationClient         customersv1.PreferenceCalculationServiceClient
	customerSegmentValidationPort       customerSegmentValidationPort
	redemptionCalculationClient         promotionsv1.RedemptionCalculationServiceClient
	conn                                *grpc.ClientConn
	householdCalculationClient          customersv1.HouseholdCalculationServiceClient
	meteredHouseholdCalculation         meteredHouseholdCalculation
	giftCardApprovalClient              giftcardsv1.GiftCardApprovalServiceClient
	currencyRateCalculationPort         currencyRateCalculationPort
	sellerListingReportingClient        sellersv1.SellerListingReportingServiceClient
}

// NewSellerPayoutScheduleLifecycleServer wires a SellerPayoutScheduleLifecycleServer.
func NewSellerPayoutScheduleLifecycleServer(deps platform.Deps) *SellerPayoutScheduleLifecycleServer {
	return &SellerPayoutScheduleLifecycleServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.sellers.v1.SellerPayoutScheduleLifecycleService"),
		userQueryClient:                     identityv1.NewUserQueryServiceClient(deps.Conn),
		promotionQueryClient:                promotionsv1.NewPromotionQueryServiceClient(deps.Conn),
		clients:                             deps.Clients,
		giftCardBatchApprovalClient:         giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
		giftCardBatchApprovalClientFallback: giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
		preferenceCalculationClient:         customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		customerSegmentValidationPort:       newCustomerSegmentValidationPort(deps),
		redemptionCalculationClient:         promotionsv1.NewRedemptionCalculationServiceClient(deps.Conn),
		conn:                                deps.Conn,
		householdCalculationClient:          customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		meteredHouseholdCalculation:         meteredHouseholdCalculation{HouseholdCalculationServiceClient: customersv1.NewHouseholdCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		giftCardApprovalClient:              giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		currencyRateCalculationPort:         newCurrencyRateCalculationPort(deps),
		sellerListingReportingClient:        sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
	}
}

// ActivateSellerPayoutSchedule activates a seller payout schedule.
func (s *SellerPayoutScheduleLifecycleServer) ActivateSellerPayoutSchedule(ctx context.Context, req *sellersv1.ActivateSellerPayoutScheduleRequest) (*sellersv1.ActivateSellerPayoutScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ActivateSellerPayoutSchedule", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "activate_seller_payout_schedule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate seller payout schedule", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := sellerPayoutScheduleLifecycleServerPeekUserSteps[mode]
	var holder struct{ UserId string }
	if record := req.GetDetails().GetNewOwner(); record != nil {
		if err := mapper.CopyFields(&holder, record, "UserId"); err != nil {
			s.log.WarnContext(ctx, "copy user_id", "error", err)
		}
	}
	peekUserRequest := &identityv1.PeekUserRequest{
		Context: req.GetContext(),
		UserId:  holder.UserId,
	}
	peekUserReply, err := run(s, ctx, peekUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/PeekUser")
	}
	suggestPromotionsRequest := &promotionsv1.SuggestPromotionsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.SuggestPromotionsRequest_Filter{
			Region:       peekUserReply.GetUser().GetPrimaryAddress().GetRegion(),
			CreatedAfter: peekUserReply.GetUser().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		},
	}
	type suggestPromotionsResult struct {
		reply *promotionsv1.SuggestPromotionsResponse
		err   error
	}
	suggestPromotionsResults := make(chan suggestPromotionsResult, 1)
	go func() {
		reply, err := s.promotionQueryClient.SuggestPromotions(ctx, suggestPromotionsRequest)
		suggestPromotionsResults <- suggestPromotionsResult{reply, err}
	}()
	var suggestPromotionsReply *promotionsv1.SuggestPromotionsResponse
	select {
	case result := <-suggestPromotionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.promotions.v1.PromotionQueryService/SuggestPromotions")
		}
		suggestPromotionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	exportUsersStream, err := s.userQueryClient.ExportUsers(ctx, &identityv1.ExportUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ExportUsersRequest_Filter{
			Region:       peekUserReply.GetUser().GetPrimaryAddress().GetRegion(),
			CreatedAfter: peekUserReply.GetUser().GetPassport().GetExpiresAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/ExportUsers")
	}
	for {
		received, err := exportUsersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/ExportUsers")
		}
		_ = received
	}
	return &sellersv1.ActivateSellerPayoutScheduleResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Organizer: &identityv1.UserRef{
				UserId: peekUserReply.GetUser().GetRelationshipManager().GetUserId(),
			},
			Location: &commonv1.PostalAddress{
				Recipient: peekUserReply.GetUser().GetPrimaryAddress().GetRecipient(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestPromotionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &sellersv1.ActivateSellerPayoutScheduleResponse_Outcome{
			Message:     suggestPromotionsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// SuspendSellerPayoutSchedule suspends a seller payout schedule.
func (s *SellerPayoutScheduleLifecycleServer) SuspendSellerPayoutSchedule(ctx context.Context, req *sellersv1.SuspendSellerPayoutScheduleRequest) (*sellersv1.SuspendSellerPayoutScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendSellerPayoutSchedule", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "suspend_seller_payout_schedule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend seller payout schedule", "tenant", req.GetContext().GetTenantId())
	secret, err := registry.Lookup[secretsv1.SecretServiceClient](s.clients, "acme.secrets.v1.SecretService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	searchSecretsRequest := &secretsv1.SearchSecretsRequest{
		Context: req.GetContext(),
		Filter: &secretsv1.SearchSecretsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			Labels:       annotations,
		},
	}
	searchSecretsReply, err := secret.SearchSecrets(ctx, searchSecretsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.secrets.v1.SecretService/SearchSecrets")
	}
	routes := map[string]func(context.Context, *giftcardsv1.EscalateGiftCardBatchRequest, ...grpc.CallOption) (*giftcardsv1.EscalateGiftCardBatchResponse, error){
		"primary":  s.giftCardBatchApprovalClient.EscalateGiftCardBatch,
		"fallback": s.giftCardBatchApprovalClientFallback.EscalateGiftCardBatch,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	escalateGiftCardBatchRequest := &giftcardsv1.EscalateGiftCardBatchRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.EscalateGiftCardBatchRequest_Details{
			Approver: &identityv1.UserRef{
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}
	escalateGiftCardBatchReply, err := route(ctx, escalateGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/EscalateGiftCardBatch")
	}
	calculatePreferenceRequest := &customersv1.CalculatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
				Email:  req.GetDetails().GetNewOwner().GetEmail(),
			},
		},
		Details: &customersv1.CalculatePreferenceRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:      escalateGiftCardBatchReply.GetGiftCardBatch().GetBillingAddress().GetRegion(),
				CountryCode: escalateGiftCardBatchReply.GetGiftCardBatch().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	type calculatePreferenceResult struct {
		reply *customersv1.CalculatePreferenceResponse
		err   error
	}
	calculatePreferenceResults := make(chan calculatePreferenceResult, 1)
	go func() {
		reply, err := s.preferenceCalculationClient.CalculatePreference(ctx, calculatePreferenceRequest)
		calculatePreferenceResults <- calculatePreferenceResult{reply, err}
	}()
	var calculatePreferenceReply *customersv1.CalculatePreferenceResponse
	select {
	case result := <-calculatePreferenceResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.PreferenceCalculationService/CalculatePreference")
		}
		calculatePreferenceReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &sellersv1.SuspendSellerPayoutScheduleResponse{
		Outcome: &sellersv1.SuspendSellerPayoutScheduleResponse_Outcome{
			Status:      searchSecretsReply.GetResult().GetStatus(),
			Message:     searchSecretsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message: escalateGiftCardBatchReply.GetResult().GetMessage(),
			Details: calculatePreferenceReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{SellerPayoutScheduleId: req.GetSellerPayoutScheduleId()},
	}, nil
}

// TerminateSellerPayoutSchedule terminates a seller payout schedule.
func (s *SellerPayoutScheduleLifecycleServer) TerminateSellerPayoutSchedule(ctx context.Context, req *sellersv1.TerminateSellerPayoutScheduleRequest) (*sellersv1.TerminateSellerPayoutScheduleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "terminate_seller_payout_schedule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate seller payout schedule", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*sellersv1.TerminateSellerPayoutScheduleRequest_Details)
	explainCustomerSegmentDecisionRequest := &customersv1.ExplainCustomerSegmentDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainCustomerSegmentDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cloned.GetNotifyContact().GetEmail()},
		},
	}
	explainCustomerSegmentDecisionReply, err := s.customerSegmentValidationPort.ExplainCustomerSegmentDecision(ctx, explainCustomerSegmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ExplainCustomerSegmentDecision")
	}
	return &sellersv1.TerminateSellerPayoutScheduleResponse{
		Result: &commonv1.OperationResult{
			Status: explainCustomerSegmentDecisionReply.GetOutcome().GetStatus(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{SellerPayoutScheduleId: req.GetSellerPayoutScheduleId()},
		Outcome:              &sellersv1.TerminateSellerPayoutScheduleResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewSellerPayoutSchedule renews a seller payout schedule.
func (s *SellerPayoutScheduleLifecycleServer) RenewSellerPayoutSchedule(ctx context.Context, req *sellersv1.RenewSellerPayoutScheduleRequest) (*sellersv1.RenewSellerPayoutScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewSellerPayoutSchedule", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "renew_seller_payout_schedule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew seller payout schedule", "tenant", req.GetContext().GetTenantId())
	calculateRedemptionRequest := buildCalculateRedemptionRequestForRenewSellerPayoutSchedule(req)
	calculateRedemptionReply, err := s.redemptionCalculationClient.CalculateRedemption(ctx, calculateRedemptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionCalculationService/CalculateRedemption")
	}
	return &sellersv1.RenewSellerPayoutScheduleResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: calculateRedemptionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{SellerPayoutScheduleId: req.GetSellerPayoutScheduleId()},
		Outcome:              &sellersv1.RenewSellerPayoutScheduleResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ExpireSellerPayoutSchedule expires a seller payout schedule.
func (s *SellerPayoutScheduleLifecycleServer) ExpireSellerPayoutSchedule(ctx context.Context, req *sellersv1.ExpireSellerPayoutScheduleRequest) (*sellersv1.ExpireSellerPayoutScheduleResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "expire_seller_payout_schedule"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire seller payout schedule", "tenant", req.GetContext().GetTenantId())
	lookupUserRequest := &identityv1.LookupUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetDetails().GetNewOwner().GetUserId(),
	}
	type lookupUserResult struct {
		reply *identityv1.LookupUserResponse
		err   error
	}
	lookupUserResults := make(chan lookupUserResult, 1)
	go func() {
		reply, err := s.userQueryClient.LookupUser(ctx, lookupUserRequest)
		lookupUserResults <- lookupUserResult{reply, err}
	}()
	var lookupUserReply *identityv1.LookupUserResponse
	select {
	case result := <-lookupUserResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.UserQueryService/LookupUser")
		}
		lookupUserReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	generateSellerListingReportRequest := &sellersv1.GenerateSellerListingReportRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	generateSellerListingReportReply := new(sellersv1.GenerateSellerListingReportResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", sellersSellerListingReportingServiceName, "GenerateSellerListingReport"), generateSellerListingReportRequest, generateSellerListingReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/GenerateSellerListingReport")
	}
	simulateHouseholdRequest := &customersv1.SimulateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						DisplayName: lookupUserReply.GetUser().GetRelationshipManager().GetDisplayName(),
					},
				}},
			}},
		},
	}
	send := s.simulateHouseholdSender(simulateHouseholdRequest)
	simulateHouseholdReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/SimulateHousehold")
	}
	compareHouseholdOptionsRequest := &customersv1.CompareHouseholdOptionsRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						PostalCode: simulateHouseholdReply.GetHousehold().GetLocation().GetPostalCode(),
						Region:     lookupUserReply.GetUser().GetPrimaryAddress().GetRegion(),
						Locality:   simulateHouseholdReply.GetHousehold().GetLocation().GetLocality(),
					},
					Manager: &identityv1.UserRef{
						UserId: simulateHouseholdReply.GetHousehold().GetManager().GetUserId(),
					},
				}},
			}},
		},
	}
	compareHouseholdOptionsReply, err := s.meteredHouseholdCalculation.CompareHouseholdOptions(ctx, compareHouseholdOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions")
	}
	return &sellersv1.ExpireSellerPayoutScheduleResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Organizer: &identityv1.UserRef{
				DisplayName: lookupUserReply.GetUser().GetRelationshipManager().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: simulateHouseholdReply.GetHousehold().GetAudit().GetCreatedAt(),
			},
			Location: &commonv1.PostalAddress{
				Lines: compareHouseholdOptionsReply.GetHousehold().GetLocation().GetLines(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: generateSellerListingReportReply.GetResult().GetCorrelationId(),
			Message:       generateSellerListingReportReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Outcome: &sellersv1.ExpireSellerPayoutScheduleResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetSellerPayoutScheduleLifecycle returns a seller payout schedule.
func (s *SellerPayoutScheduleLifecycleServer) GetSellerPayoutScheduleLifecycle(ctx context.Context, req *sellersv1.GetSellerPayoutScheduleLifecycleRequest) (*sellersv1.GetSellerPayoutScheduleLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSellerPayoutScheduleLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "get_seller_payout_schedule_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller payout schedule lifecycle", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	rejectGiftCardRequest := &giftcardsv1.RejectGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	}
	send := s.rejectGiftCardSender(rejectGiftCardRequest)
	rejectGiftCardReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/RejectGiftCard")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	quoteCurrencyRateRequest := &pricingv1.QuoteCurrencyRateRequest{
		Context: req.GetContext(),
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				Email: rejectGiftCardReply.GetGiftCard().GetContact().GetEmail(),
			},
			Detail: packed,
		},
	}
	quoteCurrencyRateReply, err := s.currencyRateCalculationPort.QuoteCurrencyRate(ctx, quoteCurrencyRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateCalculationService/QuoteCurrencyRate")
	}
	return &sellersv1.GetSellerPayoutScheduleLifecycleResponse{
		Result: &commonv1.OperationResult{
			Message:      rejectGiftCardReply.GetResult().GetMessage(),
			ReferenceIds: quoteCurrencyRateReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Organizer: &identityv1.UserRef{
				DisplayName: quoteCurrencyRateReply.GetCurrencyRate().GetSubject().GetDisplayName(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
		},
	}, nil
}

// ScheduleSellerPayoutScheduleExpiry schedules a seller payout schedule.
func (s *SellerPayoutScheduleLifecycleServer) ScheduleSellerPayoutScheduleExpiry(ctx context.Context, req *sellersv1.ScheduleSellerPayoutScheduleExpiryRequest) (*sellersv1.ScheduleSellerPayoutScheduleExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleSellerPayoutScheduleExpiry", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "schedule seller payout schedule expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	generateSellerListingReportRequest := buildGenerateSellerListingReportRequestForScheduleSellerPayoutScheduleExpiry(req)
	generateSellerListingReportReply, err := s.sellerListingReportingClient.GenerateSellerListingReport(ctx, generateSellerListingReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/GenerateSellerListingReport")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeSellersRequest := &sellersv1.SummarizeSellersRequest{
		Context:     req.GetContext(),
		Details:     &sellersv1.SummarizeSellersRequest_Details{Period: generateSellerListingReportReply.GetPeriod()},
		Annotations: annotations,
		Reason:      req.GetReason(),
	}
	summarizeSellersReply := new(sellersv1.SummarizeSellersResponse)
	if err := grpcx.Invoke(ctx, s.conn, sellersv1.SellerQueryService_SummarizeSellers_FullMethodName, summarizeSellersRequest, summarizeSellersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/SummarizeSellers")
	}
	return &sellersv1.ScheduleSellerPayoutScheduleExpiryResponse{
		Result: &commonv1.OperationResult{
			Status: generateSellerListingReportReply.GetResult().GetStatus(),
		},
		Outcome: &sellersv1.ScheduleSellerPayoutScheduleExpiryResponse_Outcome{
			Status: generateSellerListingReportReply.GetResult().GetStatus(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Title:                  req.GetSellerPayoutSchedule().GetTitle(),
		},
	}, nil
}

// sellerPayoutScheduleLifecycleServerPeekUserSteps are the ways to run acme.identity.v1.UserQueryService/PeekUser, by
// name.
var sellerPayoutScheduleLifecycleServerPeekUserSteps = map[string]func(*SellerPayoutScheduleLifecycleServer, context.Context, *identityv1.PeekUserRequest) (*identityv1.PeekUserResponse, error){
	"default": (*SellerPayoutScheduleLifecycleServer).peekUserStep,
	"audited": (*SellerPayoutScheduleLifecycleServer).peekUserAuditedStep,
}

func (s *SellerPayoutScheduleLifecycleServer) peekUserStep(ctx context.Context, request *identityv1.PeekUserRequest) (*identityv1.PeekUserResponse, error) {
	return s.userQueryClient.PeekUser(ctx, request)
}

func (s *SellerPayoutScheduleLifecycleServer) peekUserAuditedStep(ctx context.Context, request *identityv1.PeekUserRequest) (*identityv1.PeekUserResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.UserQueryService/PeekUser")
	return s.userQueryClient.PeekUser(ctx, request)
}

// simulateHouseholdSender returns a function that sends one request to
// acme.customers.v1.HouseholdCalculationService/SimulateHousehold.
func (s *SellerPayoutScheduleLifecycleServer) simulateHouseholdSender(request *customersv1.SimulateHouseholdRequest) func(context.Context) (*customersv1.SimulateHouseholdResponse, error) {
	return func(ctx context.Context) (*customersv1.SimulateHouseholdResponse, error) {
		return s.householdCalculationClient.SimulateHousehold(ctx, request)
	}
}

// rejectGiftCardSender returns a function that sends one request to
// acme.giftcards.v1.GiftCardApprovalService/RejectGiftCard.
func (s *SellerPayoutScheduleLifecycleServer) rejectGiftCardSender(request *giftcardsv1.RejectGiftCardRequest) func(context.Context) (*giftcardsv1.RejectGiftCardResponse, error) {
	return func(ctx context.Context) (*giftcardsv1.RejectGiftCardResponse, error) {
		return s.giftCardApprovalClient.RejectGiftCard(ctx, request)
	}
}
