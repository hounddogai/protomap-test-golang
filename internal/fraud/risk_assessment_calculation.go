package fraud

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	invoicingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/invoicing/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RiskAssessmentCalculationServer implements `acme.fraud.v1.RiskAssessmentCalculationService`.
// Computes the figures of a risk assessment, such as totals, taxes, and estimates.
type RiskAssessmentCalculationServer struct {
	*riskAssessmentCalculationHandlers
}

// riskAssessmentCalculationHandlers holds the handlers of RiskAssessmentCalculationServer, with what they use.
type riskAssessmentCalculationHandlers struct {
	fraudv1.UnimplementedRiskAssessmentCalculationServiceServer
	store                         *Store
	log                           *slog.Logger
	fulfillmentClient             fulfillmentv1.FulfillmentServiceClient
	roleHistoryClient             identityv1.RoleHistoryServiceClient
	ledgerAccountQueryClient      ledgerv1.LedgerAccountQueryServiceClient
	clients                       *registry.Registry
	invoiceBatchCalculationClient invoicingv1.InvoiceBatchCalculationServiceClient
	parcelQueryClient             shippingv1.ParcelQueryServiceClient
	conn                          *grpc.ClientConn
	meteredMetricSeriesQuery      meteredMetricSeriesQuery
	ledgerAccountClient           ledgerv1.LedgerAccountServiceClient
	userValidationClient          identityv1.UserValidationServiceClient
	searchIndexExportClient       searchv1.SearchIndexExportServiceClient
	sessionNotificationClient     identityv1.SessionNotificationServiceClient
	meteredLoyaltyMemberQuery     meteredLoyaltyMemberQuery
	loyaltyMemberQueryClient      loyaltyv1.LoyaltyMemberQueryServiceClient
}

// NewRiskAssessmentCalculationServer wires a RiskAssessmentCalculationServer.
func NewRiskAssessmentCalculationServer(deps platform.Deps) *RiskAssessmentCalculationServer {
	return &RiskAssessmentCalculationServer{riskAssessmentCalculationHandlers: &riskAssessmentCalculationHandlers{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.fraud.v1.RiskAssessmentCalculationService"),
		fulfillmentClient:             fulfillmentv1.NewFulfillmentServiceClient(deps.Conn),
		roleHistoryClient:             identityv1.NewRoleHistoryServiceClient(deps.Conn),
		ledgerAccountQueryClient:      ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		clients:                       deps.Clients,
		invoiceBatchCalculationClient: invoicingv1.NewInvoiceBatchCalculationServiceClient(deps.Conn),
		parcelQueryClient:             shippingv1.NewParcelQueryServiceClient(deps.Conn),
		conn:                          deps.Conn,
		meteredMetricSeriesQuery:      meteredMetricSeriesQuery{MetricSeriesQueryServiceClient: metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		ledgerAccountClient:           ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		userValidationClient:          identityv1.NewUserValidationServiceClient(deps.Conn),
		searchIndexExportClient:       searchv1.NewSearchIndexExportServiceClient(deps.Conn),
		sessionNotificationClient:     identityv1.NewSessionNotificationServiceClient(deps.Conn),
		meteredLoyaltyMemberQuery:     meteredLoyaltyMemberQuery{LoyaltyMemberQueryServiceClient: loyaltyv1.NewLoyaltyMemberQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		loyaltyMemberQueryClient:      loyaltyv1.NewLoyaltyMemberQueryServiceClient(deps.Conn),
	}}
}

// CalculateRiskAssessment calculates a risk assessment.
func (s *riskAssessmentCalculationHandlers) CalculateRiskAssessment(ctx context.Context, req *fraudv1.CalculateRiskAssessmentRequest) (*fraudv1.CalculateRiskAssessmentResponse, error) {
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "calculate risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	for comment := range iterx.Each(req.GetRiskAssessment().GetComments(), func(candidate *fraudv1.RiskAssessmentComment) bool { return candidate != nil }) {
		if _, err := s.fulfillmentClient.UpdateFulfillment(ctx, &fulfillmentv1.UpdateFulfillmentRequest{
			Context: req.GetContext(),
			Fulfillment: &fulfillmentv1.Fulfillment{
				Audit:        &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
				Counterparty: &customersv1.CustomerRef{Email: comment.GetAuthorEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentService/UpdateFulfillment")
		}
	}
	for _, comment2 := range req.GetRiskAssessment().GetComments() {
		if _, err := s.roleHistoryClient.RevertRole(ctx, &identityv1.RevertRoleRequest{
			Context: req.GetContext(),
			Role: &identityv1.Role{
				Audit: &commonv1.AuditInfo{
					CreatedAt: comment2.GetCreatedAt(),
					UpdatedBy: req.GetRiskAssessment().GetAudit().GetUpdatedBy(),
				},
			},
			Details: &identityv1.RevertRoleRequest_Details{AuthorEmail: comment2.GetAuthorEmail()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/RevertRole")
		}
	}
	findLedgerAccountsRequest := &ledgerv1.FindLedgerAccountsRequest{}
	findLedgerAccountsRequest.Context = req.GetContext()
	findLedgerAccountsReply, err := s.ledgerAccountQueryClient.FindLedgerAccounts(ctx, findLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/FindLedgerAccounts")
	}
	return &fraudv1.CalculateRiskAssessmentResponse{
		Result: &commonv1.OperationResult{
			Details: findLedgerAccountsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Requester:        req.GetRiskAssessment().GetRequester(),
		},
	}, nil
}

// EstimateRiskAssessment estimates a risk assessment.
func (s *riskAssessmentCalculationHandlers) EstimateRiskAssessment(ctx context.Context, req *fraudv1.EstimateRiskAssessmentRequest) (*fraudv1.EstimateRiskAssessmentResponse, error) {
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "estimate risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	giftCardCalculation, ok := s.clients.Get("acme.giftcards.v1.GiftCardCalculationService").(giftcardsv1.GiftCardCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardCalculationService")
	}
	recalculateGiftCardsRequest := &giftcardsv1.RecalculateGiftCardsRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetRiskAssessment().GetAudit().GetUpdatedBy(),
			},
		},
	}
	recalculateGiftCardsReply, err := giftCardCalculation.RecalculateGiftCards(ctx, recalculateGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/RecalculateGiftCards")
	}
	return &fraudv1.EstimateRiskAssessmentResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Comments: []*fraudv1.RiskAssessmentComment{&fraudv1.RiskAssessmentComment{
				CreatedAt: recalculateGiftCardsReply.GetGiftCard().GetAudit().GetCreatedAt(),
			}},
			RiskAssessmentId: req.GetRiskAssessmentId(),
		},
		Breakdown: &fraudv1.EstimateRiskAssessmentResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteRiskAssessment quotes a risk assessment.
func (s *riskAssessmentCalculationHandlers) QuoteRiskAssessment(ctx context.Context, req *fraudv1.QuoteRiskAssessmentRequest) (*fraudv1.QuoteRiskAssessmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteRiskAssessment", "error", err)
		return nil, err
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "quote risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*fraudv1.QuoteRiskAssessmentRequest_Details)
	ctx = tenancy.With(ctx, "display_name", req.GetRiskAssessment().GetAssignee().GetDisplayName())
	cloned2 := proto.Clone(req.GetDetails()).(*fraudv1.QuoteRiskAssessmentRequest_Details)
	cloned3 := proto.Clone(req.GetDetails()).(*fraudv1.QuoteRiskAssessmentRequest_Details)
	quoteInvoiceBatchRequest := &invoicingv1.QuoteInvoiceBatchRequest{}
	quoteInvoiceBatchRequest.Context = req.GetContext()
	quoteInvoiceBatchRequest.Details = &invoicingv1.QuoteInvoiceBatchRequest_Details{
		Destination: &commonv1.PostalAddress{
			CountryCode: cloned.GetDestination().GetCountryCode(),
			Recipient:   cloned2.GetDestination().GetRecipient(),
		},
	}
	quoteInvoiceBatchRequest.InvoiceBatch = &invoicingv1.InvoiceBatch{
		Counterparty:   &customersv1.CustomerRef{DisplayName: tenancy.Value(ctx, "display_name")},
		BillingAddress: &commonv1.PostalAddress{Region: cloned3.GetDestination().GetRegion()},
	}
	quoteInvoiceBatchReply, err := s.invoiceBatchCalculationClient.QuoteInvoiceBatch(ctx, quoteInvoiceBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.invoicing.v1.InvoiceBatchCalculationService/QuoteInvoiceBatch")
	}
	resolveParcelsRequest := &shippingv1.ResolveParcelsRequest{Context: req.GetContext()}
	resolveParcelsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*shippingv1.ResolveParcelsResponse, error) {
		return s.parcelQueryClient.ResolveParcels(ctx, resolveParcelsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelQueryService/ResolveParcels")
	}
	return &fraudv1.QuoteRiskAssessmentResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Comments: []*fraudv1.RiskAssessmentComment{&fraudv1.RiskAssessmentComment{
				CreatedAt: quoteInvoiceBatchReply.GetInvoiceBatch().GetAudit().GetCreatedAt(),
			}},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Priority:         req.GetRiskAssessment().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			Details: resolveParcelsReply.GetResult().GetDetails(),
			Status:  resolveParcelsReply.GetResult().GetStatus(),
		},
	}, nil
}

// CompareRiskAssessmentOptions compares a risk assessment.
func (s *riskAssessmentCalculationHandlers) CompareRiskAssessmentOptions(ctx context.Context, req *fraudv1.CompareRiskAssessmentOptionsRequest) (*fraudv1.CompareRiskAssessmentOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "compare risk assessment options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	var holder struct{ UserId string }
	if record := req.GetRiskAssessment().GetAssignee(); record != nil {
		if err := mapper.CopyFields(&holder, record, "UserId"); err != nil {
			s.log.WarnContext(ctx, "copy user_id", "error", err)
		}
	}
	recordRoleVersionRequest := &identityv1.RecordRoleVersionRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetRiskAssessment().GetCustomer().GetDisplayName(),
				Email:       req.GetRiskAssessment().GetCustomer().GetEmail(),
				UserId:      holder.UserId,
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetRiskAssessment().GetAudit().GetCreatedBy(),
			},
		},
	}
	var recordRoleVersionReply *identityv1.RecordRoleVersionResponse
	if err := pipeline.Run(ctx, "acme.identity.v1.RoleHistoryService/RecordRoleVersion", func(ctx context.Context) error {
		answer, err := s.roleHistoryClient.RecordRoleVersion(ctx, recordRoleVersionRequest)
		recordRoleVersionReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/RecordRoleVersion")
	}
	checkReturnRequestEligibilityRequest := &returnsv1.CheckReturnRequestEligibilityRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetRiskAssessment().GetAudit().GetUpdatedAt(),
				UpdatedBy: recordRoleVersionReply.GetRole().GetAudit().GetUpdatedBy(),
			},
			Counterparty: &customersv1.CustomerRef{
				CustomerId: req.GetRiskAssessment().GetCustomer().GetCustomerId(),
			},
		},
	}
	checkReturnRequestEligibilityReply := new(returnsv1.CheckReturnRequestEligibilityResponse)
	if err := grpcx.Invoke(ctx, s.conn, returnsv1.ReturnRequestValidationService_CheckReturnRequestEligibility_FullMethodName, checkReturnRequestEligibilityRequest, checkReturnRequestEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestValidationService/CheckReturnRequestEligibility")
	}
	countMetricSeriesesRequest := &metricsv1.CountMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.CountMetricSeriesesRequest_Filter{
			Region:       checkReturnRequestEligibilityReply.GetReturnRequest().GetBillingAddress().GetRegion(),
			CreatedAfter: recordRoleVersionReply.GetRole().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetRiskAssessment().GetCustomer().GetCustomerId(),
		},
	}
	countMetricSeriesesReply, err := s.meteredMetricSeriesQuery.CountMetricSerieses(ctx, countMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/CountMetricSerieses")
	}
	_ = countMetricSeriesesReply
	return &fraudv1.CompareRiskAssessmentOptionsResponse{
		Result: &commonv1.OperationResult{
			Status:  recordRoleVersionReply.GetResult().GetStatus(),
			Message: recordRoleVersionReply.GetResult().GetMessage(),
		},
		Breakdown: &fraudv1.CompareRiskAssessmentOptionsResponse_Breakdown{
			Total: checkReturnRequestEligibilityReply.GetReturnRequest().GetTotal(),
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: checkReturnRequestEligibilityReply.GetReturnRequest().GetAudit().GetCreatedAt(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Assignee:         req.GetRiskAssessment().GetAssignee(),
		},
	}, nil
}

// RecalculateRiskAssessments recalculates a risk assessment.
func (s *riskAssessmentCalculationHandlers) RecalculateRiskAssessments(ctx context.Context, req *fraudv1.RecalculateRiskAssessmentsRequest) (*fraudv1.RecalculateRiskAssessmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "recalculate risk assessments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	riskAssessmentCache := cache.New[string, *fraudv1.RiskAssessment](1)
	riskAssessmentCache.Put(req.GetContext().GetTraceId(), req.GetRiskAssessment())
	cachedRiskAssessment, _ := riskAssessmentCache.Get(req.GetContext().GetTraceId())
	cloneLedgerAccountRequest := &ledgerv1.CloneLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetRiskAssessment().GetAudit().GetCreatedAt(),
				CreatedBy: req.GetRiskAssessment().GetAudit().GetCreatedBy(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetRiskAssessment().GetRequester().GetEmail()},
			Holder: &ledgerv1.PostingBatchRef{
				DisplayName: cachedRiskAssessment.GetAssignee().GetDisplayName(),
			},
		},
	}
	cloneLedgerAccountReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*ledgerv1.CloneLedgerAccountResponse, error) {
		return s.ledgerAccountClient.CloneLedgerAccount(ctx, cloneLedgerAccountRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CloneLedgerAccount")
	}
	ctx = tenancy.With(ctx, "email", req.GetRiskAssessment().GetRequester().GetEmail())
	var email string
	switch choice := req.GetRiskAssessment().GetOrigin().(type) {
	case *fraudv1.RiskAssessment_Customer:
		email = choice.Customer.GetEmail()
	case *fraudv1.RiskAssessment_ExternalReference:
		email = choice.ExternalReference
	case *fraudv1.RiskAssessment_Channel:
		email = choice.Channel.GetChannel()
	}
	source := req.GetDetails().GetDestination().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	validateUserReply, err := s.userValidationClient.ValidateUser(ctx, &identityv1.ValidateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Contact:             &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
			RelationshipManager: &identityv1.UserRef{Email: email},
			PrimaryAddress: &commonv1.PostalAddress{
				Region:     region,
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ValidateUser")
	}
	return &fraudv1.RecalculateRiskAssessmentsResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Requester: &commonv1.ContactInfo{
				Email: cloneLedgerAccountReply.GetLedgerAccount().GetContact().GetEmail(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Subject:          req.GetRiskAssessment().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: validateUserReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// SimulateRiskAssessment simulates a risk assessment.
func (s *riskAssessmentCalculationHandlers) SimulateRiskAssessment(ctx context.Context, req *fraudv1.SimulateRiskAssessmentRequest) (*fraudv1.SimulateRiskAssessmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "simulate risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	normalizeUserRequest := buildNormalizeUserRequestForSimulateRiskAssessment(req)
	normalizeUserReply, err := s.userValidationClient.NormalizeUser(ctx, normalizeUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/NormalizeUser")
	}
	listSearchIndexExportsReply, err := s.searchIndexExportClient.ListSearchIndexExports(ctx, &searchv1.ListSearchIndexExportsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexExportsRequest_Filter{
			Region:       normalizeUserReply.GetUser().GetPrimaryAddress().GetRegion(),
			CreatedAfter: normalizeUserReply.GetUser().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ListSearchIndexExports")
	}
	streamSessionNoticesStream, err := s.sessionNotificationClient.StreamSessionNotices(ctx, &identityv1.StreamSessionNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email:            normalizeUserReply.GetUser().GetRelationshipManager().GetEmail(),
			Name:             normalizeUserReply.GetUser().GetContact().GetName(),
			PreferredChannel: req.GetRiskAssessment().GetRequester().GetPreferredChannel(),
		},
		Filter: &identityv1.StreamSessionNoticesRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
	}
	for {
		received, err := streamSessionNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
		}
		_ = received
	}
	return &fraudv1.SimulateRiskAssessmentResponse{
		Result: &commonv1.OperationResult{
			Details:      normalizeUserReply.GetResult().GetDetails(),
			ReferenceIds: listSearchIndexExportsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				UserId: normalizeUserReply.GetUser().GetRelationshipManager().GetUserId(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Priority:         req.GetRiskAssessment().GetPriority(),
		},
	}, nil
}

// ExplainRiskAssessmentCalculation explains a risk assessment.
func (s *riskAssessmentCalculationHandlers) ExplainRiskAssessmentCalculation(ctx context.Context, req *fraudv1.ExplainRiskAssessmentCalculationRequest) (*fraudv1.ExplainRiskAssessmentCalculationResponse, error) {
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "explain risk assessment calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetRiskAssessment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fraudv1.RiskAssessment
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	suggestLoyaltyMembersRequest := &loyaltyv1.SuggestLoyaltyMembersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.SuggestLoyaltyMembersRequest_Filter{
			CreatedAfter: req.GetRiskAssessment().GetAudit().GetCreatedAt(),
			OwnerId:      (&decoded).GetAssignee().GetUserId(),
			Labels:       annotations,
		},
	}
	suggestLoyaltyMembersReply, err := s.meteredLoyaltyMemberQuery.SuggestLoyaltyMembers(ctx, suggestLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/SuggestLoyaltyMembers")
	}
	var customerId string
	switch choice := req.GetRiskAssessment().GetOrigin().(type) {
	case *fraudv1.RiskAssessment_Customer:
		customerId = choice.Customer.GetCustomerId()
	case *fraudv1.RiskAssessment_ExternalReference:
		customerId = choice.ExternalReference
	case *fraudv1.RiskAssessment_Channel:
		customerId = choice.Channel.GetChannel()
	}
	exportLoyaltyMembersStream, err := s.loyaltyMemberQueryClient.ExportLoyaltyMembers(ctx, &loyaltyv1.ExportLoyaltyMembersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.ExportLoyaltyMembersRequest_Filter{
			CreatedAfter: req.GetRiskAssessment().GetSla().GetDueAt(),
			OwnerId:      customerId,
			Labels:       suggestLoyaltyMembersReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/ExportLoyaltyMembers")
	}
	for {
		received, err := exportLoyaltyMembersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/ExportLoyaltyMembers")
		}
		_ = received
	}
	return &fraudv1.ExplainRiskAssessmentCalculationResponse{
		Result: &commonv1.OperationResult{
			Message: suggestLoyaltyMembersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Subject:          req.GetRiskAssessment().GetSubject(),
		},
	}, nil
}

// GetRiskAssessmentRates returns a risk assessment.
func (s *riskAssessmentCalculationHandlers) GetRiskAssessmentRates(ctx context.Context, req *fraudv1.GetRiskAssessmentRatesRequest) (*fraudv1.GetRiskAssessmentRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "risk_assessment", req.GetRiskAssessmentId(), "get_risk_assessment_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get risk assessment rates", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.GetRiskAssessmentRatesResponse{
		RiskAssessment: &fraudv1.RiskAssessment{RiskAssessmentId: req.GetRiskAssessmentId()},
		Result:         &commonv1.OperationResult{Status: "ok"},
	}, nil
}
