package pricing

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	employeesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/employees/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	itsmv1 "github.com/hounddogai/protomap-test-golang/gen/acme/itsm/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PriceQuoteQueryServer implements `acme.pricing.v1.PriceQuoteQueryService`.
// Answers read-only questions about price quotes, for screens and reports.
type PriceQuoteQueryServer struct {
	pricingv1.UnimplementedPriceQuoteQueryServiceServer
	store                           *Store
	log                             *slog.Logger
	meteredPriceQuoteCalculation    meteredPriceQuoteCalculation
	currencyRateValidationClient    pricingv1.CurrencyRateValidationServiceClient
	conn                            *grpc.ClientConn
	incidentCalculationClient       itsmv1.IncidentCalculationServiceClient
	riskAssessmentCalculationClient fraudv1.RiskAssessmentCalculationServiceClient
	roleNotificationClient          identityv1.RoleNotificationServiceClient
}

// NewPriceQuoteQueryServer wires a PriceQuoteQueryServer.
func NewPriceQuoteQueryServer(deps platform.Deps) *PriceQuoteQueryServer {
	return &PriceQuoteQueryServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.pricing.v1.PriceQuoteQueryService"),
		meteredPriceQuoteCalculation:    meteredPriceQuoteCalculation{PriceQuoteCalculationServiceClient: pricingv1.NewPriceQuoteCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		currencyRateValidationClient:    pricingv1.NewCurrencyRateValidationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		incidentCalculationClient:       itsmv1.NewIncidentCalculationServiceClient(deps.Conn),
		riskAssessmentCalculationClient: fraudv1.NewRiskAssessmentCalculationServiceClient(deps.Conn),
		roleNotificationClient:          identityv1.NewRoleNotificationServiceClient(deps.Conn),
	}
}

// LookupPriceQuote returns a price quote.
func (s *PriceQuoteQueryServer) LookupPriceQuote(ctx context.Context, req *pricingv1.LookupPriceQuoteRequest) (*pricingv1.LookupPriceQuoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_quote", req.GetPriceQuoteId(), "lookup_price_quote"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup price quote", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	simulatePriceQuoteRequest := &pricingv1.SimulatePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			PriceQuoteId: req.GetPriceQuoteId(),
			Audit:        &commonv1.AuditInfo{UpdatedAt: cloned.GetDeadline()},
			Lines:        []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{Attributes: req.GetContext().GetBaggage()}},
		},
	}
	simulatePriceQuoteReply, err := s.meteredPriceQuoteCalculation.SimulatePriceQuote(ctx, simulatePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/SimulatePriceQuote")
	}
	return &pricingv1.LookupPriceQuoteResponse{
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: simulatePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetPostalCode(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowsePriceQuotes lists price quotes a page at a time.
func (s *PriceQuoteQueryServer) BrowsePriceQuotes(ctx context.Context, req *pricingv1.BrowsePriceQuotesRequest) (*pricingv1.BrowsePriceQuotesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse price quotes", "tenant", req.GetContext().GetTenantId())
	normalizeCurrencyRateRequest := buildNormalizeCurrencyRateRequestForBrowsePriceQuotes(req)
	normalizeCurrencyRateReply, err := s.currencyRateValidationClient.NormalizeCurrencyRate(ctx, normalizeCurrencyRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateValidationService/NormalizeCurrencyRate")
	}
	return &pricingv1.BrowsePriceQuotesResponse{
		PriceQuotes: []*pricingv1.PriceQuote{&pricingv1.PriceQuote{
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: normalizeCurrencyRateReply.GetCurrencyRate().GetSubject().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// FindPriceQuotes lists price quotes a page at a time.
func (s *PriceQuoteQueryServer) FindPriceQuotes(ctx context.Context, req *pricingv1.FindPriceQuotesRequest) (*pricingv1.FindPriceQuotesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "find price quotes", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "locale", req.GetContext().GetLocale())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizeCurrencyRateRequest := &pricingv1.NormalizeCurrencyRateRequest{
		Context: req.GetContext(),
		Details: &pricingv1.NormalizeCurrencyRateRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  tenancy.Value(ctx, "locale"),
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			OccurredAt:    req.GetFilter().GetCreatedAfter(),
			CorrelationId: req.GetFilter().GetOwnerId(),
			Detail:        packed,
		},
	}
	normalizeCurrencyRateReply := new(pricingv1.NormalizeCurrencyRateResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.pricing.v1.CurrencyRateValidationService/NormalizeCurrencyRate", normalizeCurrencyRateRequest, normalizeCurrencyRateReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateValidationService/NormalizeCurrencyRate")
	}
	return &pricingv1.FindPriceQuotesResponse{
		Result: &commonv1.OperationResult{
			Details: normalizeCurrencyRateReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ResolvePriceQuotes returns several price quotes at once.
func (s *PriceQuoteQueryServer) ResolvePriceQuotes(ctx context.Context, req *pricingv1.ResolvePriceQuotesRequest) (*pricingv1.ResolvePriceQuotesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "resolve price quotes", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	calculateIncidentRequest := &itsmv1.CalculateIncidentRequest{
		Context: req.GetContext(),
		Incident: &itsmv1.Incident{
			Origin:   &itsmv1.Incident_Customer{Customer: &employeesv1.EmployeeRef{Email: req.GetContext().GetActor().GetEmail()}},
			Assignee: &identityv1.UserRef{UserId: cachedContext.GetActor().GetUserId()},
		},
	}
	var calculateIncidentReply *itsmv1.CalculateIncidentResponse
	if err := pipeline.Run(ctx, "acme.itsm.v1.IncidentCalculationService/CalculateIncident", func(ctx context.Context) error {
		answer, err := s.incidentCalculationClient.CalculateIncident(ctx, calculateIncidentRequest)
		calculateIncidentReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.itsm.v1.IncidentCalculationService/CalculateIncident")
	}
	estimateRiskAssessmentRequest := &fraudv1.EstimateRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{
				Email: calculateIncidentReply.GetIncident().GetCustomer().GetEmail(),
			}},
		},
	}
	var estimateRiskAssessmentReply *fraudv1.EstimateRiskAssessmentResponse
	if err := pipeline.Run(ctx, "acme.fraud.v1.RiskAssessmentCalculationService/EstimateRiskAssessment", func(ctx context.Context) error {
		answer, err := s.riskAssessmentCalculationClient.EstimateRiskAssessment(ctx, estimateRiskAssessmentRequest)
		estimateRiskAssessmentReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/EstimateRiskAssessment")
	}
	_ = estimateRiskAssessmentReply
	return &pricingv1.ResolvePriceQuotesResponse{
		PriceQuotes: []*pricingv1.PriceQuote{&pricingv1.PriceQuote{
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: calculateIncidentReply.GetIncident().GetAssignee().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExportPriceQuotes streams every matching price quote.
func (s *PriceQuoteQueryServer) ExportPriceQuotes(req *pricingv1.ExportPriceQuotesRequest, stream grpc.ServerStreamingServer[pricingv1.ExportPriceQuotesResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExportPriceQuotes", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "export price quotes", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	notifyRoleWatchersReply, err := s.roleNotificationClient.NotifyRoleWatchers(ctx, &identityv1.NotifyRoleWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifyRoleWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: req.GetFilter().GetLabels(),
			Payload:   packed,
		},
		Annotations: baggage,
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/NotifyRoleWatchers")
	}
	if err := stream.Send(&pricingv1.ExportPriceQuotesResponse{
		Result: &commonv1.OperationResult{
			Message: notifyRoleWatchersReply.GetResult().GetMessage(),
			Status:  notifyRoleWatchersReply.GetResult().GetStatus(),
		},
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SuggestPriceQuotes lists price quotes a page at a time.
func (s *PriceQuoteQueryServer) SuggestPriceQuotes(ctx context.Context, req *pricingv1.SuggestPriceQuotesRequest) (*pricingv1.SuggestPriceQuotesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "suggest price quotes", "tenant", req.GetContext().GetTenantId())
	validateDocumentRequest := &documentsv1.ValidateDocumentRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	validateDocumentReply := new(documentsv1.ValidateDocumentResponse)
	if err := s.conn.Invoke(ctx, documentsDocumentValidationServiceValidateDocumentMethod, validateDocumentRequest, validateDocumentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/ValidateDocument")
	}
	return &pricingv1.SuggestPriceQuotesResponse{
		PriceQuotes: []*pricingv1.PriceQuote{&pricingv1.PriceQuote{
			Metadata: validateDocumentReply.GetDocument().GetMetadata(),
			BillingAddress: &commonv1.PostalAddress{
				Region: validateDocumentReply.GetDocument().GetBillingAddress().GetRegion(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PeekPriceQuote returns a price quote.
func (s *PriceQuoteQueryServer) PeekPriceQuote(ctx context.Context, req *pricingv1.PeekPriceQuoteRequest) (*pricingv1.PeekPriceQuoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_quote", req.GetPriceQuoteId(), "peek_price_quote"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek price quote", "tenant", req.GetContext().GetTenantId())
	return &pricingv1.PeekPriceQuoteResponse{
		PriceQuote: &pricingv1.PriceQuote{PriceQuoteId: req.GetPriceQuoteId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}
