package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	broadcastsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/broadcasts/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	ratelimitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ratelimiting/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// RestockDecisionCalculationServer implements `acme.returns.v1.RestockDecisionCalculationService`.
// Computes the figures of a restock decision, such as totals, taxes, and estimates.
type RestockDecisionCalculationServer struct {
	returnsv1.UnimplementedRestockDecisionCalculationServiceServer
	store                               *Store
	log                                 *slog.Logger
	auditEventValidationClient          auditv1.AuditEventValidationServiceClient
	featureFlagQueryClient              featureflagsv1.FeatureFlagQueryServiceClient
	returnLabelCalculationClient        returnsv1.ReturnLabelCalculationServiceClient
	returnRequestCalculationClient      returnsv1.ReturnRequestCalculationServiceClient
	broadcastClient                     broadcastsv1.BroadcastServiceClient
	conn                                *grpc.ClientConn
	loyaltyMemberValidationClient       loyaltyv1.LoyaltyMemberValidationServiceClient
	rateLimitClient                     ratelimitingv1.RateLimitServiceClient
	meteredLoyaltyMemberCalculation     meteredLoyaltyMemberCalculation
	sellerListingQueryClient            sellersv1.SellerListingQueryServiceClient
	giftCardTransactionValidationClient giftcardsv1.GiftCardTransactionValidationServiceClient
	tenantExportClient                  identityv1.TenantExportServiceClient
	searchQueryNotificationClient       searchv1.SearchQueryNotificationServiceClient
}

// NewRestockDecisionCalculationServer wires a RestockDecisionCalculationServer.
func NewRestockDecisionCalculationServer(deps platform.Deps) *RestockDecisionCalculationServer {
	return &RestockDecisionCalculationServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.returns.v1.RestockDecisionCalculationService"),
		auditEventValidationClient:          auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		featureFlagQueryClient:              featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		returnLabelCalculationClient:        returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn),
		returnRequestCalculationClient:      returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn),
		broadcastClient:                     broadcastsv1.NewBroadcastServiceClient(deps.Conn),
		conn:                                deps.Conn,
		loyaltyMemberValidationClient:       loyaltyv1.NewLoyaltyMemberValidationServiceClient(deps.Conn),
		rateLimitClient:                     ratelimitingv1.NewRateLimitServiceClient(deps.Conn),
		meteredLoyaltyMemberCalculation:     meteredLoyaltyMemberCalculation{LoyaltyMemberCalculationServiceClient: loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerListingQueryClient:            sellersv1.NewSellerListingQueryServiceClient(deps.Conn),
		giftCardTransactionValidationClient: giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn),
		tenantExportClient:                  identityv1.NewTenantExportServiceClient(deps.Conn),
		searchQueryNotificationClient:       searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
	}
}

// CalculateRestockDecision calculates a restock decision.
func (s *RestockDecisionCalculationServer) CalculateRestockDecision(ctx context.Context, req *returnsv1.CalculateRestockDecisionRequest) (*returnsv1.CalculateRestockDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "calculate restock decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetRestockDecision()).(*returnsv1.RestockDecision)
	ctx = tenancy.With(ctx, "reason", req.GetRestockDecision().GetBlock().GetReason())
	previewAuditEventReply, err := s.auditEventValidationClient.PreviewAuditEvent(ctx, &auditv1.PreviewAuditEventRequest{
		Context: req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{
			Subject: &auditv1.AuditTrailRef{DisplayName: cloned.GetOwner().GetDisplayName()},
		},
		Details: &auditv1.PreviewAuditEventRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetRestockDecision().GetOwner().GetEmail()},
		},
		Reason:      tenancy.Value(ctx, "reason"),
		Annotations: req.GetAnnotations(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/PreviewAuditEvent")
	}
	exportFeatureFlagsStream, err := s.featureFlagQueryClient.ExportFeatureFlags(ctx, &featureflagsv1.ExportFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.ExportFeatureFlagsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetRestockDecision().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/ExportFeatureFlags")
	}
	for {
		received, err := exportFeatureFlagsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/ExportFeatureFlags")
		}
		_ = received
	}
	quoteReturnLabelRequest := &returnsv1.QuoteReturnLabelRequest{
		Context: req.GetContext(),
		Details: &returnsv1.QuoteReturnLabelRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
		},
	}
	quoteReturnLabelReply, err := grpcx.Call(ctx, s.returnLabelCalculationClient.QuoteReturnLabel, quoteReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/QuoteReturnLabel")
	}
	return &returnsv1.CalculateRestockDecisionResponse{
		Result: &commonv1.OperationResult{
			Message: previewAuditEventReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				DisplayName: previewAuditEventReply.GetAuditEvent().GetSubject().GetDisplayName(),
			},
			Priority:          quoteReturnLabelReply.GetReturnLabel().GetCounterparty().GetPriority(),
			RestockDecisionId: req.GetRestockDecisionId(),
			Effective:         req.GetRestockDecision().GetEffective(),
		},
		Breakdown: &returnsv1.CalculateRestockDecisionResponse_Breakdown{
			CurrencyCode: quoteReturnLabelReply.GetBreakdown().GetCurrencyCode(),
		},
	}, nil
}

// EstimateRestockDecision estimates a restock decision.
func (s *RestockDecisionCalculationServer) EstimateRestockDecision(ctx context.Context, req *returnsv1.EstimateRestockDecisionRequest) (*returnsv1.EstimateRestockDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "estimate restock decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetDetails().GetDestination().GetRegion())
	explainReturnRequestCalculationRequest := &returnsv1.ExplainReturnRequestCalculationRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Lines:          []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{Amount: req.GetRestockDecision().GetDiscount().GetAmount()}},
			BillingAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		},
	}
	explainReturnRequestCalculationReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*returnsv1.ExplainReturnRequestCalculationResponse, error) {
		return s.returnRequestCalculationClient.ExplainReturnRequestCalculation(ctx, explainReturnRequestCalculationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/ExplainReturnRequestCalculation")
	}
	cloneBroadcastReply, err := s.broadcastClient.CloneBroadcast(ctx, &broadcastsv1.CloneBroadcastRequest{
		Context: req.GetContext(),
		Broadcast: &broadcastsv1.Broadcast{
			Participants: []*broadcastsv1.BroadcastParticipant{&broadcastsv1.BroadcastParticipant{
				Contact: &commonv1.ContactInfo{
					Email: explainReturnRequestCalculationReply.GetReturnRequest().GetCounterparty().GetEmail(),
				},
			}},
			Location: &commonv1.PostalAddress{
				Region: explainReturnRequestCalculationReply.GetReturnRequest().GetBillingAddress().GetRegion(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.broadcasts.v1.BroadcastService/CloneBroadcast")
	}
	simulateReturnRequestRequest := &returnsv1.SimulateReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: cloneBroadcastReply.GetBroadcast().GetLocation().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetRestockDecision().GetAudit().GetEtag()},
			Settlement: &returnsv1.ReturnRequest_Card{Card: &returnsv1.ReturnRequestCardSettlement{
				Network: explainReturnRequestCalculationReply.GetReturnRequest().GetCard().GetNetwork(),
			}},
		},
	}
	simulateReturnRequestReply, err := grpcx.NewClient(s.conn, returnsv1.NewReturnRequestCalculationServiceClient).SimulateReturnRequest(ctx, simulateReturnRequestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/SimulateReturnRequest")
	}
	return &returnsv1.EstimateRestockDecisionResponse{
		Result: &commonv1.OperationResult{
			Message:       explainReturnRequestCalculationReply.GetOutcome().GetMessage(),
			CorrelationId: simulateReturnRequestReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				Etag: cloneBroadcastReply.GetBroadcast().GetAudit().GetEtag(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Effective:         req.GetRestockDecision().GetEffective(),
		},
	}, nil
}

// QuoteRestockDecision quotes a restock decision.
func (s *RestockDecisionCalculationServer) QuoteRestockDecision(ctx context.Context, req *returnsv1.QuoteRestockDecisionRequest) (*returnsv1.QuoteRestockDecisionResponse, error) {
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "quote restock decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	scoreLoyaltyMemberRequest := &loyaltyv1.ScoreLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Holder: &loyaltyv1.RewardRef{
				DisplayName: req.GetRestockDecision().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetRestockDecision().GetAudit().GetCreatedBy(),
			},
		},
	}
	send := s.scoreLoyaltyMemberSender(scoreLoyaltyMemberRequest)
	scoreLoyaltyMemberReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberValidationService/ScoreLoyaltyMember")
	}
	explainReturnRequestCalculationRequest := &returnsv1.ExplainReturnRequestCalculationRequest{}
	explainReturnRequestCalculationRequest.Context = req.GetContext()
	explainReturnRequestCalculationRequest.Details = &returnsv1.ExplainReturnRequestCalculationRequest_Details{
		Contact: &commonv1.ContactInfo{
			Email: scoreLoyaltyMemberReply.GetLoyaltyMember().GetContact().GetEmail(),
		},
	}
	explainReturnRequestCalculationRequest.ReturnRequest = &returnsv1.ReturnRequest{
		Counterparty: &customersv1.CustomerRef{
			DisplayName: scoreLoyaltyMemberReply.GetLoyaltyMember().GetHolder().GetDisplayName(),
		},
		Settlement: &returnsv1.ReturnRequest_Card{Card: &returnsv1.ReturnRequestCardSettlement{
			HolderName: scoreLoyaltyMemberReply.GetLoyaltyMember().GetCard().GetHolderName(),
			Token:      scoreLoyaltyMemberReply.GetLoyaltyMember().GetCard().GetToken(),
		}},
	}
	explainReturnRequestCalculationReply, err := s.returnRequestCalculationClient.ExplainReturnRequestCalculation(ctx, explainReturnRequestCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/ExplainReturnRequestCalculation")
	}
	searchRateLimitsReply, err := s.rateLimitClient.SearchRateLimits(ctx, &ratelimitingv1.SearchRateLimitsRequest{
		Context: req.GetContext(),
		Filter: &ratelimitingv1.SearchRateLimitsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			Labels:       scoreLoyaltyMemberReply.GetLoyaltyMember().GetLabels(),
			CreatedAfter: scoreLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ratelimiting.v1.RateLimitService/SearchRateLimits")
	}
	calculateLoyaltyMemberRequest := &loyaltyv1.CalculateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				Etag: scoreLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetEtag(),
			},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: scoreLoyaltyMemberReply.GetLoyaltyMember().GetContact().GetPreferredChannel(),
				Phone:            scoreLoyaltyMemberReply.GetLoyaltyMember().GetContact().GetPhone(),
			},
			Funding: &loyaltyv1.LoyaltyMember_Card{Card: &loyaltyv1.LoyaltyMemberCardFunding{
				Last4: explainReturnRequestCalculationReply.GetReturnRequest().GetCard().GetLast4(),
			}},
		},
	}
	calculateLoyaltyMemberReply, err := s.meteredLoyaltyMemberCalculation.CalculateLoyaltyMember(ctx, calculateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CalculateLoyaltyMember")
	}
	_ = calculateLoyaltyMemberReply
	return &returnsv1.QuoteRestockDecisionResponse{
		Breakdown: &returnsv1.QuoteRestockDecisionResponse_Breakdown{
			CurrencyCode: scoreLoyaltyMemberReply.GetLoyaltyMember().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{
			Status:       explainReturnRequestCalculationReply.GetResult().GetStatus(),
			Message:      searchRateLimitsReply.GetResult().GetMessage(),
			ReferenceIds: searchRateLimitsReply.GetResult().GetReferenceIds(),
		},
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainReturnRequestCalculationReply.GetReturnRequest().GetAudit().GetUpdatedBy(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Name:              req.GetRestockDecision().GetName(),
		},
	}, nil
}

// CompareRestockDecisionOptions compares a restock decision.
func (s *RestockDecisionCalculationServer) CompareRestockDecisionOptions(ctx context.Context, req *returnsv1.CompareRestockDecisionOptionsRequest) (*returnsv1.CompareRestockDecisionOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CompareRestockDecisionOptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "compare restock decision options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	simulateReturnRequestRequest := &returnsv1.SimulateReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
				Tax: &returnsv1.ReturnRequestLineTax{Amount: req.GetRestockDecision().GetDiscount().GetAmount()},
			}},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
	}
	type simulateReturnRequestResult struct {
		reply *returnsv1.SimulateReturnRequestResponse
		err   error
	}
	simulateReturnRequestResults := make(chan simulateReturnRequestResult, 1)
	go func() {
		reply, err := s.returnRequestCalculationClient.SimulateReturnRequest(ctx, simulateReturnRequestRequest)
		simulateReturnRequestResults <- simulateReturnRequestResult{reply, err}
	}()
	var simulateReturnRequestReply *returnsv1.SimulateReturnRequestResponse
	select {
	case result := <-simulateReturnRequestResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.returns.v1.ReturnRequestCalculationService/SimulateReturnRequest")
		}
		simulateReturnRequestReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	explainReturnLabelCalculationRequest := &returnsv1.ExplainReturnLabelCalculationRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{
				CreatedAt: simulateReturnRequestReply.GetReturnRequest().GetAudit().GetCreatedAt(),
				UpdatedAt: req.GetRestockDecision().GetAudit().GetUpdatedAt(),
			},
		},
	}
	explainReturnLabelCalculationReply, err := grpcx.Call(ctx, s.returnLabelCalculationClient.ExplainReturnLabelCalculation, explainReturnLabelCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/ExplainReturnLabelCalculation")
	}
	summarizeSellerListingsReply, err := s.sellerListingQueryClient.SummarizeSellerListings(ctx, &sellersv1.SummarizeSellerListingsRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/SummarizeSellerListings")
	}
	_ = explainReturnLabelCalculationReply
	return &returnsv1.CompareRestockDecisionOptionsResponse{
		Result: &commonv1.OperationResult{
			Message:      simulateReturnRequestReply.GetResult().GetMessage(),
			ReferenceIds: summarizeSellerListingsReply.GetResult().GetReferenceIds(),
			Status:       summarizeSellerListingsReply.GetResult().GetStatus(),
		},
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				CreatedBy: simulateReturnRequestReply.GetReturnRequest().GetAudit().GetCreatedBy(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Version:           req.GetRestockDecision().GetVersion(),
		},
	}, nil
}

// SimulateRestockDecision simulates a restock decision.
func (s *RestockDecisionCalculationServer) SimulateRestockDecision(ctx context.Context, req *returnsv1.SimulateRestockDecisionRequest) (*returnsv1.SimulateRestockDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "simulate restock decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetRestockDecision())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreGiftCardTransactionRequest := &giftcardsv1.ScoreGiftCardTransactionRequest{}
	scoreGiftCardTransactionRequest.Context = req.GetContext()
	scoreGiftCardTransactionRequest.Details = &giftcardsv1.ScoreGiftCardTransactionRequest_Details{
		Destination: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
	}
	scoreGiftCardTransactionRequest.GiftCardTransaction = &giftcardsv1.GiftCardTransaction{Detail: packed}
	scoreGiftCardTransactionReply, err := s.giftCardTransactionValidationClient.ScoreGiftCardTransaction(ctx, scoreGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/ScoreGiftCardTransaction")
	}
	return &returnsv1.SimulateRestockDecisionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: scoreGiftCardTransactionReply.GetResult().GetCorrelationId(),
			Message:       scoreGiftCardTransactionReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		RestockDecision: &returnsv1.RestockDecision{
			RestockDecisionId: req.GetRestockDecisionId(),
			Parameters:        req.GetRestockDecision().GetParameters(),
		},
	}, nil
}

// ExplainRestockDecisionCalculation explains a restock decision.
func (s *RestockDecisionCalculationServer) ExplainRestockDecisionCalculation(ctx context.Context, req *returnsv1.ExplainRestockDecisionCalculationRequest) (*returnsv1.ExplainRestockDecisionCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "explain restock decision calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	retryTenantExportRequest := &identityv1.RetryTenantExportRequest{
		Context: req.GetContext(),
		Tenant: &identityv1.Tenant{
			Children: []*identityv1.Tenant{&identityv1.Tenant{
				Audit: &commonv1.AuditInfo{
					CreatedAt: req.GetRestockDecision().GetAudit().GetCreatedAt(),
				},
				Children: []*identityv1.Tenant{&identityv1.Tenant{
					Manager: &identityv1.UserRef{Email: req.GetRestockDecision().GetOwner().GetEmail()},
				}},
			}},
		},
	}
	retryTenantExportReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*identityv1.RetryTenantExportResponse, error) {
		return s.tenantExportClient.RetryTenantExport(ctx, retryTenantExportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/RetryTenantExport")
	}
	step := &restockDecisionCalculationServerExplainRestockDecisionCalculationSimulateReturnLabelStep{request: &returnsv1.SimulateReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{Etag: req.GetRestockDecision().GetAudit().GetEtag()},
			Lines: []*returnsv1.ReturnLabelLine{&returnsv1.ReturnLabelLine{Amount: req.GetRestockDecision().GetDiscount().GetAmount()}},
		},
	}}
	if err := s.runExplainRestockDecisionCalculationSimulateReturnLabel(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/SimulateReturnLabel")
	}
	simulateReturnLabelReply := step.reply
	return &returnsv1.ExplainRestockDecisionCalculationResponse{
		Outcome: &returnsv1.ExplainRestockDecisionCalculationResponse_Outcome{Status: retryTenantExportReply.GetResult().GetStatus()},
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				Etag:      retryTenantExportReply.GetTenant().GetAudit().GetEtag(),
				CreatedBy: simulateReturnLabelReply.GetReturnLabel().GetAudit().GetCreatedBy(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Name:              req.GetRestockDecision().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetRestockDecisionRates returns a restock decision.
func (s *RestockDecisionCalculationServer) GetRestockDecisionRates(ctx context.Context, req *returnsv1.GetRestockDecisionRatesRequest) (*returnsv1.GetRestockDecisionRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetRestockDecisionRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "restock_decision", req.GetRestockDecisionId(), "get_restock_decision_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get restock decision rates", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifySearchQueryWatchersReply, err := s.searchQueryNotificationClient.NotifySearchQueryWatchers(ctx, &searchv1.NotifySearchQueryWatchersRequest{
		Context: req.GetContext(),
		Details: &searchv1.NotifySearchQueryWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: baggage,
			Payload:   packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/NotifySearchQueryWatchers")
	}
	explainReturnRequestCalculationRequest := &returnsv1.ExplainReturnRequestCalculationRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: notifySearchQueryWatchersReply.GetSearchQuery().GetSubject().GetDisplayName(),
			},
			Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
				Attributes: notifySearchQueryWatchersReply.GetSearchQuery().GetAttributes(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: notifySearchQueryWatchersReply.GetSearchQuery().GetOccurredAt(),
			},
		},
		Details: &returnsv1.ExplainReturnRequestCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	send := s.explainReturnRequestCalculationSender(explainReturnRequestCalculationRequest)
	explainReturnRequestCalculationReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/ExplainReturnRequestCalculation")
	}
	return &returnsv1.GetRestockDecisionRatesResponse{
		Result: &commonv1.OperationResult{
			Status:       notifySearchQueryWatchersReply.GetResult().GetStatus(),
			ReferenceIds: explainReturnRequestCalculationReply.GetResult().GetReferenceIds(),
		},
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainReturnRequestCalculationReply.GetReturnRequest().GetAudit().GetUpdatedBy(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
		},
	}, nil
}

// scoreLoyaltyMemberSender returns a function that sends one request to
// acme.loyalty.v1.LoyaltyMemberValidationService/ScoreLoyaltyMember.
func (s *RestockDecisionCalculationServer) scoreLoyaltyMemberSender(request *loyaltyv1.ScoreLoyaltyMemberRequest) func(context.Context) (*loyaltyv1.ScoreLoyaltyMemberResponse, error) {
	return func(ctx context.Context) (*loyaltyv1.ScoreLoyaltyMemberResponse, error) {
		return s.loyaltyMemberValidationClient.ScoreLoyaltyMember(ctx, request)
	}
}

// restockDecisionCalculationServerExplainRestockDecisionCalculationSimulateReturnLabelStep carries the request of one
// step of ExplainRestockDecisionCalculation and, once it ran, its reply.
type restockDecisionCalculationServerExplainRestockDecisionCalculationSimulateReturnLabelStep struct {
	request *returnsv1.SimulateReturnLabelRequest
	reply   *returnsv1.SimulateReturnLabelResponse
}

func (s *RestockDecisionCalculationServer) runExplainRestockDecisionCalculationSimulateReturnLabel(ctx context.Context, step *restockDecisionCalculationServerExplainRestockDecisionCalculationSimulateReturnLabelStep) error {
	reply, err := s.returnLabelCalculationClient.SimulateReturnLabel(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// explainReturnRequestCalculationSender returns a function that sends one request to
// acme.returns.v1.ReturnRequestCalculationService/ExplainReturnRequestCalculation.
func (s *RestockDecisionCalculationServer) explainReturnRequestCalculationSender(request *returnsv1.ExplainReturnRequestCalculationRequest) func(context.Context) (*returnsv1.ExplainReturnRequestCalculationResponse, error) {
	return func(ctx context.Context) (*returnsv1.ExplainReturnRequestCalculationResponse, error) {
		return s.returnRequestCalculationClient.ExplainReturnRequestCalculation(ctx, request)
	}
}
