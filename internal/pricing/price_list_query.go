package pricing

import (
	"context"
	"fmt"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// PriceListQueryServer implements `acme.pricing.v1.PriceListQueryService`.
// Answers read-only questions about price lists, for screens and reports.
type PriceListQueryServer struct {
	pricingv1.UnimplementedPriceListQueryServiceServer
	store                           *Store
	log                             *slog.Logger
	renewalScheduleValidationClient subscriptionsv1.RenewalScheduleValidationServiceClient
	conn                            *grpc.ClientConn
	userValidationClient            identityv1.UserValidationServiceClient
	priceOverrideCalculationClient  pricingv1.PriceOverrideCalculationServiceClient
	currencyRateValidationClient    pricingv1.CurrencyRateValidationServiceClient
	returnRequestCalculationClient  returnsv1.ReturnRequestCalculationServiceClient
	currencyRateCalculationClient   pricingv1.CurrencyRateCalculationServiceClient
}

// NewPriceListQueryServer wires a PriceListQueryServer.
func NewPriceListQueryServer(deps platform.Deps) *PriceListQueryServer {
	return &PriceListQueryServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.pricing.v1.PriceListQueryService"),
		renewalScheduleValidationClient: subscriptionsv1.NewRenewalScheduleValidationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		userValidationClient:            identityv1.NewUserValidationServiceClient(deps.Conn),
		priceOverrideCalculationClient:  pricingv1.NewPriceOverrideCalculationServiceClient(deps.Conn),
		currencyRateValidationClient:    pricingv1.NewCurrencyRateValidationServiceClient(deps.Conn),
		returnRequestCalculationClient:  returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn),
		currencyRateCalculationClient:   pricingv1.NewCurrencyRateCalculationServiceClient(deps.Conn),
	}
}

// LookupPriceList returns a price list.
func (s *PriceListQueryServer) LookupPriceList(ctx context.Context, req *pricingv1.LookupPriceListRequest) (*pricingv1.LookupPriceListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupPriceList", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_list", req.GetPriceListId(), "lookup_price_list"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup price list", "tenant", req.GetContext().GetTenantId())
	normalizeRenewalScheduleRequest := newNormalizeRenewalScheduleRequest(withNormalizeRenewalScheduleContext(req.GetContext()), withNormalizeRenewalScheduleRenewalSchedule(&subscriptionsv1.RenewalSchedule{
		Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		}},
	}))
	normalizeRenewalScheduleReply, err := s.renewalScheduleValidationClient.NormalizeRenewalSchedule(ctx, normalizeRenewalScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/NormalizeRenewalSchedule")
	}
	scorePriceQuoteRequest := &pricingv1.ScorePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{
				Locality: normalizeRenewalScheduleReply.GetRenewalSchedule().GetLocation().GetLocality(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: normalizeRenewalScheduleReply.GetRenewalSchedule().GetAudit().GetUpdatedBy(),
				CreatedAt: normalizeRenewalScheduleReply.GetRenewalSchedule().GetAudit().GetCreatedAt(),
			},
		},
		Details: &pricingv1.ScorePriceQuoteRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: normalizeRenewalScheduleReply.GetRenewalSchedule().GetLocation().GetRecipient(),
			},
		},
	}
	scorePriceQuoteReply := new(pricingv1.ScorePriceQuoteResponse)
	if err := grpcx.Invoke(ctx, s.conn, pricingv1.PriceQuoteValidationService_ScorePriceQuote_FullMethodName, scorePriceQuoteRequest, scorePriceQuoteReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/ScorePriceQuote")
	}
	return &pricingv1.LookupPriceListResponse{
		Result: &commonv1.OperationResult{
			Message: normalizeRenewalScheduleReply.GetResult().GetMessage(),
			Status:  scorePriceQuoteReply.GetResult().GetStatus(),
			Details: scorePriceQuoteReply.GetResult().GetDetails(),
		},
		PriceList: &pricingv1.PriceList{PriceListId: req.GetPriceListId()},
	}, nil
}

// BrowsePriceLists lists price lists a page at a time.
func (s *PriceListQueryServer) BrowsePriceLists(ctx context.Context, req *pricingv1.BrowsePriceListsRequest) (*pricingv1.BrowsePriceListsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BrowsePriceLists", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "browse price lists", "tenant", req.GetContext().GetTenantId())
	explainFraudRuleCalculationRequest := &fraudv1.ExplainFraudRuleCalculationRequest{
		Context: req.GetContext(),
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &fraudv1.ExplainFraudRuleCalculationRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	explainFraudRuleCalculationReply, err := grpcx.NewClient(s.conn, fraudv1.NewFraudRuleCalculationServiceClient).ExplainFraudRuleCalculation(ctx, explainFraudRuleCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleCalculationService/ExplainFraudRuleCalculation")
	}
	return &pricingv1.BrowsePriceListsResponse{
		PriceLists: []*pricingv1.PriceList{&pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				Etag: explainFraudRuleCalculationReply.GetFraudRule().GetAudit().GetEtag(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// FindPriceLists lists price lists a page at a time.
func (s *PriceListQueryServer) FindPriceLists(ctx context.Context, req *pricingv1.FindPriceListsRequest) (*pricingv1.FindPriceListsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "find price lists", "tenant", req.GetContext().GetTenantId())
	normalizeUserRequest := buildNormalizeUserRequestForFindPriceLists(req)
	normalizeUserReply, err := s.userValidationClient.NormalizeUser(ctx, normalizeUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/NormalizeUser")
	}
	return &pricingv1.FindPriceListsResponse{
		PriceLists: []*pricingv1.PriceList{&pricingv1.PriceList{
			Owner: &identityv1.UserRef{
				Email:  normalizeUserReply.GetUser().GetRelationshipManager().GetEmail(),
				UserId: normalizeUserReply.GetUser().GetUserId(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountPriceLists counts price lists by status.
func (s *PriceListQueryServer) CountPriceLists(ctx context.Context, req *pricingv1.CountPriceListsRequest) (*pricingv1.CountPriceListsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountPriceLists", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count price lists", "tenant", req.GetContext().GetTenantId())
	return &pricingv1.CountPriceListsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ResolvePriceLists returns several price lists at once.
func (s *PriceListQueryServer) ResolvePriceLists(ctx context.Context, req *pricingv1.ResolvePriceListsRequest) (*pricingv1.ResolvePriceListsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "resolve price lists", "tenant", req.GetContext().GetTenantId())
	estimateReturnRequestRequest := &returnsv1.EstimateReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	estimateReturnRequestReply := new(returnsv1.EstimateReturnRequestResponse)
	if err := grpcx.Invoke(ctx, s.conn, returnsv1.ReturnRequestCalculationService_EstimateReturnRequest_FullMethodName, estimateReturnRequestRequest, estimateReturnRequestReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/EstimateReturnRequest")
	}
	return &pricingv1.ResolvePriceListsResponse{
		Result: &commonv1.OperationResult{
			Details: estimateReturnRequestReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PriceLists: []*pricingv1.PriceList{&pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				CreatedAt: estimateReturnRequestReply.GetReturnRequest().GetAudit().GetCreatedAt(),
			},
		}},
	}, nil
}

// SummarizePriceLists summarizes a price list.
func (s *PriceListQueryServer) SummarizePriceLists(ctx context.Context, req *pricingv1.SummarizePriceListsRequest) (*pricingv1.SummarizePriceListsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SummarizePriceLists", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_list", req.GetPriceListId(), "summarize_price_lists"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize price lists", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	recalculatePriceOverridesRequest := &pricingv1.RecalculatePriceOverridesRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Action: &pricingv1.PriceOverride_Block{Block: &pricingv1.PriceOverrideBlockAction{Reason: tenancy.Value(ctx, "reason")}},
			Owner:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Annotations: req.GetAnnotations(),
	}
	recalculatePriceOverridesReply, err := grpcx.Call(ctx, s.priceOverrideCalculationClient.RecalculatePriceOverrides, recalculatePriceOverridesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideCalculationService/RecalculatePriceOverrides")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreCurrencyRateRequest := &pricingv1.ScoreCurrencyRateRequest{
		Context: req.GetContext(),
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				DisplayName: recalculatePriceOverridesReply.GetPriceOverride().GetOwner().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	scoreCurrencyRateReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*pricingv1.ScoreCurrencyRateResponse, error) {
		return s.currencyRateValidationClient.ScoreCurrencyRate(ctx, scoreCurrencyRateRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateValidationService/ScoreCurrencyRate")
	}
	calculateReturnRequestRequest := &returnsv1.CalculateReturnRequestRequest{}
	calculateReturnRequestRequest.Context = req.GetContext()
	calculateReturnRequestRequest.ReturnRequest = &returnsv1.ReturnRequest{
		Counterparty: &customersv1.CustomerRef{
			CustomerId:  scoreCurrencyRateReply.GetCurrencyRate().GetSubject().GetCustomerId(),
			DisplayName: recalculatePriceOverridesReply.GetPriceOverride().GetOwner().GetDisplayName(),
		},
		Audit: &commonv1.AuditInfo{
			Etag: recalculatePriceOverridesReply.GetPriceOverride().GetAudit().GetEtag(),
		},
		Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
			Tax: &returnsv1.ReturnRequestLineTax{
				Amount: recalculatePriceOverridesReply.GetPriceOverride().GetDiscount().GetAmount(),
			},
		}},
	}
	calculateReturnRequestReply, err := s.returnRequestCalculationClient.CalculateReturnRequest(ctx, calculateReturnRequestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/CalculateReturnRequest")
	}
	listTenantExportsRequest := &identityv1.ListTenantExportsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListTenantExportsRequest_Filter{
			Region: calculateReturnRequestReply.GetReturnRequest().GetBillingAddress().GetRegion(),
		},
	}
	listTenantExportsReply := new(identityv1.ListTenantExportsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", identityTenantExportServiceName, "ListTenantExports"), listTenantExportsRequest, listTenantExportsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/ListTenantExports")
	}
	return &pricingv1.SummarizePriceListsResponse{
		Result: &commonv1.OperationResult{
			Message:       recalculatePriceOverridesReply.GetResult().GetMessage(),
			CorrelationId: recalculatePriceOverridesReply.GetResult().GetCorrelationId(),
			Status:        scoreCurrencyRateReply.GetResult().GetStatus(),
			ReferenceIds:  calculateReturnRequestReply.GetResult().GetReferenceIds(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestPriceLists lists price lists a page at a time.
func (s *PriceListQueryServer) SuggestPriceLists(ctx context.Context, req *pricingv1.SuggestPriceListsRequest) (*pricingv1.SuggestPriceListsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "suggest price lists", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewCurrencyRateRequest := &pricingv1.PreviewCurrencyRateRequest{
		Context: req.GetContext(),
		Details: &pricingv1.PreviewCurrencyRateRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			Measurements: []*pricingv1.CurrencyRateMeasurement{&pricingv1.CurrencyRateMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Detail:       packed,
		},
	}
	var previewCurrencyRateReply *pricingv1.PreviewCurrencyRateResponse
	if err := pipeline.Run(ctx, "acme.pricing.v1.CurrencyRateValidationService/PreviewCurrencyRate", func(ctx context.Context) error {
		answer, err := s.currencyRateValidationClient.PreviewCurrencyRate(ctx, previewCurrencyRateRequest)
		previewCurrencyRateReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateValidationService/PreviewCurrencyRate")
	}
	return &pricingv1.SuggestPriceListsResponse{
		Result: &commonv1.OperationResult{
			Status:       previewCurrencyRateReply.GetOutcome().GetStatus(),
			ReferenceIds: previewCurrencyRateReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// PeekPriceList returns a price list.
func (s *PriceListQueryServer) PeekPriceList(ctx context.Context, req *pricingv1.PeekPriceListRequest) (*pricingv1.PeekPriceListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PeekPriceList", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_list", req.GetPriceListId(), "peek_price_list"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek price list", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	quoteCurrencyRateReply, err := s.currencyRateCalculationClient.QuoteCurrencyRate(ctx, &pricingv1.QuoteCurrencyRateRequest{
		Context: req.GetContext(),
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject:       &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			CorrelationId: req.GetPriceListId(),
			Detail:        packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateCalculationService/QuoteCurrencyRate")
	}
	return &pricingv1.PeekPriceListResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: quoteCurrencyRateReply.GetResult().GetCorrelationId(),
			ReferenceIds:  quoteCurrencyRateReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		PriceList: &pricingv1.PriceList{PriceListId: req.GetPriceListId()},
	}, nil
}
