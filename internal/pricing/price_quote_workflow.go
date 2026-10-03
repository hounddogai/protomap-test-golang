package pricing

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	patientsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patients/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// PriceQuoteWorkflowServer implements `acme.pricing.v1.PriceQuoteWorkflowService`.
// Runs the long-lived workflow that takes a price quote from start to finish.
type PriceQuoteWorkflowServer struct {
	pricingv1.UnimplementedPriceQuoteWorkflowServiceServer
	store                         *Store
	log                           *slog.Logger
	ledgerAccountClient           ledgerv1.LedgerAccountServiceClient
	priceListApprovalClient       pricingv1.PriceListApprovalServiceClient
	priceOverrideApprovalClient   pricingv1.PriceOverrideApprovalServiceClient
	priceListWorkflowClient       pricingv1.PriceListWorkflowServiceClient
	promotionReportingClient      promotionsv1.PromotionReportingServiceClient
	priceListValidationClient     pricingv1.PriceListValidationServiceClient
	priceQuoteApprovalClient      pricingv1.PriceQuoteApprovalServiceClient
	searchIndexValidationClient   searchv1.SearchIndexValidationServiceClient
	priceOverrideValidationClient pricingv1.PriceOverrideValidationServiceClient
	householdCalculationClient    customersv1.HouseholdCalculationServiceClient
	meteredPriceQuoteApproval     meteredPriceQuoteApproval
	priceQuoteQueryClient         pricingv1.PriceQuoteQueryServiceClient
	resumeFulfillmentWorkflowFn   func(context.Context, *fulfillmentv1.ResumeFulfillmentWorkflowRequest, ...grpc.CallOption) (*fulfillmentv1.ResumeFulfillmentWorkflowResponse, error)
	patientCalculationClient      patientsv1.PatientCalculationServiceClient
}

// NewPriceQuoteWorkflowServer wires a PriceQuoteWorkflowServer.
func NewPriceQuoteWorkflowServer(deps platform.Deps) *PriceQuoteWorkflowServer {
	fulfillmentWorkflow := fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn)
	return &PriceQuoteWorkflowServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.pricing.v1.PriceQuoteWorkflowService"),
		ledgerAccountClient:           ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		priceListApprovalClient:       pricingv1.NewPriceListApprovalServiceClient(deps.Conn),
		priceOverrideApprovalClient:   pricingv1.NewPriceOverrideApprovalServiceClient(deps.Conn),
		priceListWorkflowClient:       pricingv1.NewPriceListWorkflowServiceClient(deps.Conn),
		promotionReportingClient:      promotionsv1.NewPromotionReportingServiceClient(deps.Conn),
		priceListValidationClient:     pricingv1.NewPriceListValidationServiceClient(deps.Conn),
		priceQuoteApprovalClient:      pricingv1.NewPriceQuoteApprovalServiceClient(deps.Conn),
		searchIndexValidationClient:   searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		priceOverrideValidationClient: pricingv1.NewPriceOverrideValidationServiceClient(deps.Conn),
		householdCalculationClient:    customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		meteredPriceQuoteApproval:     meteredPriceQuoteApproval{PriceQuoteApprovalServiceClient: pricingv1.NewPriceQuoteApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		priceQuoteQueryClient:         pricingv1.NewPriceQuoteQueryServiceClient(deps.Conn),
		resumeFulfillmentWorkflowFn:   fulfillmentWorkflow.ResumeFulfillmentWorkflow,
		patientCalculationClient:      patientsv1.NewPatientCalculationServiceClient(deps.Conn),
	}
}

// StartPriceQuoteWorkflow starts a price quote.
func (s *PriceQuoteWorkflowServer) StartPriceQuoteWorkflow(ctx context.Context, req *pricingv1.StartPriceQuoteWorkflowRequest) (*pricingv1.StartPriceQuoteWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StartPriceQuoteWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "start price quote workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	var holder struct{ Region string }
	if record := req.GetPriceQuote().GetBillingAddress(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	priceQuoteCache := cache.New[string, *pricingv1.PriceQuote](1)
	priceQuoteCache.Put(req.GetContext().GetTraceId(), req.GetPriceQuote())
	cachedPriceQuote, _ := priceQuoteCache.Get(req.GetContext().GetTraceId())
	parameters := make(map[string]string, len(req.GetDetails().GetParameters()))
	for key, value := range req.GetDetails().GetParameters() {
		parameters[key] = value
	}
	searchLedgerAccountsReply, err := s.ledgerAccountClient.SearchLedgerAccounts(ctx, &ledgerv1.SearchLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.SearchLedgerAccountsRequest_Filter{
			Region:       holder.Region,
			CreatedAfter: cachedPriceQuote.GetAudit().GetCreatedAt(),
			Labels:       parameters,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/SearchLedgerAccounts")
	}
	approvePriceListRequest := newApprovePriceListRequest(withApprovePriceListContext(req.GetContext()), withApprovePriceListPriceList(&pricingv1.PriceList{
		Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPriceQuote().GetAudit().GetUpdatedBy()},
	}))
	approvePriceListReply, err := s.priceListApprovalClient.ApprovePriceList(ctx, approvePriceListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListApprovalService/ApprovePriceList")
	}
	approvePriceOverrideRequest := newApprovePriceOverrideRequest(withApprovePriceOverrideContext(req.GetContext()), withApprovePriceOverridePriceOverride(&pricingv1.PriceOverride{
		Action: &pricingv1.PriceOverride_Block{Block: &pricingv1.PriceOverrideBlockAction{
			NotifyEmail: approvePriceListReply.GetPriceList().GetBlock().GetNotifyEmail(),
		}},
		Owner: &identityv1.UserRef{
			DisplayName: approvePriceListReply.GetPriceList().GetOwner().GetDisplayName(),
			Email:       approvePriceListReply.GetPriceList().GetOwner().GetEmail(),
		},
		Audit: &commonv1.AuditInfo{CreatedAt: req.GetPriceQuote().GetAudit().GetCreatedAt()},
	}))
	approvePriceOverrideReply, err := s.priceOverrideApprovalClient.ApprovePriceOverride(ctx, approvePriceOverrideRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideApprovalService/ApprovePriceOverride")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := priceQuoteWorkflowServerRetryPriceListWorkflowSteps[mode]
	retryPriceListWorkflowRequest := &pricingv1.RetryPriceListWorkflowRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Action: &pricingv1.PriceList_Route{Route: &pricingv1.PriceListRouteAction{
				Target: approvePriceListReply.GetPriceList().GetRoute().GetTarget(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag: approvePriceOverrideReply.GetPriceOverride().GetAudit().GetEtag(),
			},
		},
		Details: &pricingv1.RetryPriceListWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: approvePriceOverrideReply.GetPriceOverride().GetOwner().GetEmail(),
				Phone: req.GetDetails().GetNotify().GetPhone(),
			},
		},
	}
	retryPriceListWorkflowReply, err := run(s, ctx, retryPriceListWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListWorkflowService/RetryPriceListWorkflow")
	}
	return &pricingv1.StartPriceQuoteWorkflowResponse{
		Outcome: &pricingv1.StartPriceQuoteWorkflowResponse_Outcome{
			Message:   searchLedgerAccountsReply.GetResult().GetMessage(),
			NextSteps: approvePriceListReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			Message: searchLedgerAccountsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PriceQuote: &pricingv1.PriceQuote{
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: approvePriceOverrideReply.GetPriceOverride().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: retryPriceListWorkflowReply.GetPriceList().GetAudit().GetUpdatedAt(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
			Metadata:     req.GetPriceQuote().GetMetadata(),
		},
	}, nil
}

// AdvancePriceQuoteWorkflow advances a price quote.
func (s *PriceQuoteWorkflowServer) AdvancePriceQuoteWorkflow(ctx context.Context, req *pricingv1.AdvancePriceQuoteWorkflowRequest) (*pricingv1.AdvancePriceQuoteWorkflowResponse, error) {
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "advance price quote workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	streamPromotionMetricsStream, err := s.promotionReportingClient.StreamPromotionMetrics(ctx, &promotionsv1.StreamPromotionMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone: req.GetDetails().GetNotify().GetPhone(),
			Email: req.GetDetails().GetNotify().GetEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/StreamPromotionMetrics")
	}
	for {
		received, err := streamPromotionMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/StreamPromotionMetrics")
		}
		_ = received
	}
	scorePriceListRequest := &pricingv1.ScorePriceListRequest{}
	scorePriceListRequest.Context = req.GetContext()
	scorePriceListRequest.PriceList = &pricingv1.PriceList{
		Audit: &commonv1.AuditInfo{CreatedAt: req.GetPriceQuote().GetAudit().GetCreatedAt()},
	}
	scorePriceListReply, err := s.priceListValidationClient.ScorePriceList(ctx, scorePriceListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/ScorePriceList")
	}
	return &pricingv1.AdvancePriceQuoteWorkflowResponse{
		Result: &commonv1.OperationResult{Status: scorePriceListReply.GetResult().GetStatus()},
		PriceQuote: &pricingv1.PriceQuote{
			PriceQuoteId: req.GetPriceQuoteId(),
			Total:        req.GetPriceQuote().GetTotal(),
		},
	}, nil
}

// CancelPriceQuoteWorkflow cancels a price quote.
func (s *PriceQuoteWorkflowServer) CancelPriceQuoteWorkflow(ctx context.Context, req *pricingv1.CancelPriceQuoteWorkflowRequest) (*pricingv1.CancelPriceQuoteWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CancelPriceQuoteWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "cancel price quote workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*pricingv1.SubmitPriceListResponse, len(req.GetPriceQuote().GetLines()))
	for i, line := range req.GetPriceQuote().GetLines() {
		group.Go(func() error {
			reply, err := s.priceListApprovalClient.SubmitPriceList(groupCtx, &pricingv1.SubmitPriceListRequest{
				Context: req.GetContext(),
				PriceList: &pricingv1.PriceList{
					Action: &pricingv1.PriceList_Discount{Discount: &pricingv1.PriceListDiscountAction{Amount: line.GetTax().GetAmount()}},
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
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListApprovalService/SubmitPriceList")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	recallPriceQuoteRequest := &pricingv1.RecallPriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetPriceQuote().GetBillingAddress().GetRegion()},
		},
	}
	recallPriceQuoteReply, err := grpcx.Call(ctx, s.priceQuoteApprovalClient.RecallPriceQuote, recallPriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteApprovalService/RecallPriceQuote")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := priceQuoteWorkflowServerApprovePriceListSteps[mode]
	approvePriceListRequest := &pricingv1.ApprovePriceListRequest{Context: req.GetContext()}
	approvePriceListReply, err := run(s, ctx, approvePriceListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListApprovalService/ApprovePriceList")
	}
	validateSearchIndexRequest := &searchv1.ValidateSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: approvePriceListReply.GetPriceList().GetAudit().GetUpdatedAt(),
			},
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{
				Recipient: recallPriceQuoteReply.GetPriceQuote().GetBillingAddress().GetRecipient(),
			}},
		},
	}
	validateSearchIndexReply, err := grpcx.Call(ctx, s.searchIndexValidationClient.ValidateSearchIndex, validateSearchIndexRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/ValidateSearchIndex")
	}
	_ = validateSearchIndexReply
	return &pricingv1.CancelPriceQuoteWorkflowResponse{
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: recallPriceQuoteReply.GetPriceQuote().GetBillingAddress().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: recallPriceQuoteReply.GetPriceQuote().GetAudit().GetCreatedAt(),
				UpdatedBy: approvePriceListReply.GetPriceList().GetAudit().GetUpdatedBy(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
			Metadata:     req.GetPriceQuote().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryPriceQuoteWorkflow retrys a price quote.
func (s *PriceQuoteWorkflowServer) RetryPriceQuoteWorkflow(ctx context.Context, req *pricingv1.RetryPriceQuoteWorkflowRequest) (*pricingv1.RetryPriceQuoteWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "retry price quote workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*pricingv1.NormalizePriceOverrideResponse, len(req.GetPriceQuote().GetLines()))
	for i, line := range req.GetPriceQuote().GetLines() {
		group.Go(func() error {
			reply, err := s.priceOverrideValidationClient.NormalizePriceOverride(groupCtx, &pricingv1.NormalizePriceOverrideRequest{
				Context: req.GetContext(),
				PriceOverride: &pricingv1.PriceOverride{
					Action:      &pricingv1.PriceOverride_Discount{Discount: &pricingv1.PriceOverrideDiscountAction{Amount: line.GetTax().GetAmount()}},
					Description: line.GetDescription(),
					Audit: &commonv1.AuditInfo{
						CreatedAt: req.GetPriceQuote().GetAudit().GetCreatedAt(),
						UpdatedBy: req.GetPriceQuote().GetAudit().GetUpdatedBy(),
					},
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
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideValidationService/NormalizePriceOverride")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	escalatePriceListRequest := &pricingv1.EscalatePriceListRequest{Context: req.GetContext()}
	escalatePriceListReply, err := escalatePriceListCall(ctx, escalatePriceListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListApprovalService/EscalatePriceList")
	}
	estimateHouseholdRequest := buildEstimateHouseholdRequestForRetryPriceQuoteWorkflow(req, escalatePriceListReply)
	estimateHouseholdReply, err := s.householdCalculationClient.EstimateHousehold(ctx, estimateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/EstimateHousehold")
	}
	recallPriceQuoteRequest := &pricingv1.RecallPriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{
				Tax: &pricingv1.PriceQuoteLineTax{
					Amount: escalatePriceListReply.GetPriceList().GetDiscount().GetAmount(),
				},
			}},
			Counterparty: &pricingv1.PriceListRef{Version: req.GetPriceQuote().GetCounterparty().GetVersion()},
		},
	}
	recallPriceQuoteReply, err := s.meteredPriceQuoteApproval.RecallPriceQuote(ctx, recallPriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteApprovalService/RecallPriceQuote")
	}
	return &pricingv1.RetryPriceQuoteWorkflowResponse{
		Outcome: &pricingv1.RetryPriceQuoteWorkflowResponse_Outcome{
			Status:  escalatePriceListReply.GetResult().GetStatus(),
			Message: recallPriceQuoteReply.GetResult().GetMessage(),
		},
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{
				Locality: estimateHouseholdReply.GetHousehold().GetLocation().GetLocality(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
			Status:       req.GetPriceQuote().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PausePriceQuoteWorkflow pauses a price quote.
func (s *PriceQuoteWorkflowServer) PausePriceQuoteWorkflow(ctx context.Context, req *pricingv1.PausePriceQuoteWorkflowRequest) (*pricingv1.PausePriceQuoteWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "pause price quote workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	for _, line := range req.GetPriceQuote().GetLines() {
		if _, err := s.householdCalculationClient.QuoteHousehold(ctx, &customersv1.QuoteHouseholdRequest{
			Context: req.GetContext(),
			Household: &customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Children: []*customersv1.Household{&customersv1.Household{
						Children: []*customersv1.Household{&customersv1.Household{Attributes: line.GetAttributes()}},
						Audit: &commonv1.AuditInfo{
							UpdatedAt: req.GetPriceQuote().GetAudit().GetUpdatedAt(),
							UpdatedBy: req.GetPriceQuote().GetAudit().GetUpdatedBy(),
						},
					}},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/QuoteHousehold")
		}
	}
	return &pricingv1.PausePriceQuoteWorkflowResponse{
		PriceQuote: &pricingv1.PriceQuote{
			PriceQuoteId: req.GetPriceQuoteId(),
			Metadata:     req.GetPriceQuote().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPriceQuoteWorkflows lists price quotes a page at a time.
func (s *PriceQuoteWorkflowServer) ListPriceQuoteWorkflows(ctx context.Context, req *pricingv1.ListPriceQuoteWorkflowsRequest) (*pricingv1.ListPriceQuoteWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list price quote workflows", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*pricingv1.ListPriceQuoteWorkflowsRequest_Filter)
	filterCache := cache.New[string, *pricingv1.ListPriceQuoteWorkflowsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	cloned2 := proto.Clone(req.GetPage()).(*commonv1.PageRequest)
	browsePriceQuotesRequest := &pricingv1.BrowsePriceQuotesRequest{}
	browsePriceQuotesRequest.Context = req.GetContext()
	browsePriceQuotesRequest.Filter = &pricingv1.BrowsePriceQuotesRequest_Filter{
		OwnerId:  req.GetFilter().GetOwnerId(),
		Statuses: cloned.GetStatuses(),
		Query:    cachedFilter.GetQuery(),
	}
	browsePriceQuotesRequest.Page = &commonv1.PageRequest{PageSize: cloned2.GetPageSize()}
	browsePriceQuotesReply, err := s.priceQuoteQueryClient.BrowsePriceQuotes(ctx, browsePriceQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteQueryService/BrowsePriceQuotes")
	}
	return &pricingv1.ListPriceQuoteWorkflowsResponse{
		Result: &commonv1.OperationResult{
			Details: browsePriceQuotesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PageInfo: &commonv1.PageInfo{
			NextPageToken: browsePriceQuotesReply.GetPageInfo().GetNextPageToken(),
		},
	}, nil
}

// SignalPriceQuoteWorkflow signals a price quote.
func (s *PriceQuoteWorkflowServer) SignalPriceQuoteWorkflow(ctx context.Context, req *pricingv1.SignalPriceQuoteWorkflowRequest) (*pricingv1.SignalPriceQuoteWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "signal price quote workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*pricingv1.PreviewPriceListResponse, len(req.GetPriceQuote().GetLines()))
	for i, line := range req.GetPriceQuote().GetLines() {
		group.Go(func() error {
			reply, err := s.priceListValidationClient.PreviewPriceList(groupCtx, &pricingv1.PreviewPriceListRequest{
				Context: req.GetContext(),
				PriceList: &pricingv1.PriceList{
					Action: &pricingv1.PriceList_Discount{Discount: &pricingv1.PriceListDiscountAction{Amount: line.GetAmount()}},
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
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/PreviewPriceList")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	resumeFulfillmentWorkflowRequest := &fulfillmentv1.ResumeFulfillmentWorkflowRequest{
		Context: req.GetContext(),
		Fulfillment: &fulfillmentv1.Fulfillment{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPriceQuote().GetAudit().GetUpdatedAt()},
		},
	}
	resumeFulfillmentWorkflowReply, err := s.resumeFulfillmentWorkflowFn(ctx, resumeFulfillmentWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/ResumeFulfillmentWorkflow")
	}
	for _, line2 := range req.GetPriceQuote().GetLines() {
		if _, err := s.patientCalculationClient.ExplainPatientCalculation(ctx, &patientsv1.ExplainPatientCalculationRequest{
			Context: req.GetContext(),
			Patient: &patientsv1.Patient{
				Attributes: line2.GetAttributes(),
				RelationshipManager: &identityv1.UserRef{
					DisplayName: resumeFulfillmentWorkflowReply.GetFulfillment().GetCounterparty().GetDisplayName(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.patients.v1.PatientCalculationService/ExplainPatientCalculation")
		}
	}
	watchLedgerAccountsStream, err := s.ledgerAccountClient.WatchLedgerAccounts(ctx, &ledgerv1.WatchLedgerAccountsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: resumeFulfillmentWorkflowReply.GetFulfillment().GetCounterparty().GetEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/WatchLedgerAccounts")
	}
	for {
		received, err := watchLedgerAccountsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/WatchLedgerAccounts")
		}
		_ = received
	}
	return &pricingv1.SignalPriceQuoteWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resumeFulfillmentWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &pricingv1.SignalPriceQuoteWorkflowResponse_Outcome{
			Message: resumeFulfillmentWorkflowReply.GetOutcome().GetMessage(),
		},
		PriceQuote: &pricingv1.PriceQuote{
			PriceQuoteId: req.GetPriceQuoteId(),
			DueAt:        req.GetPriceQuote().GetDueAt(),
		},
	}, nil
}

// priceQuoteWorkflowServerRetryPriceListWorkflowSteps are the ways to run
// acme.pricing.v1.PriceListWorkflowService/RetryPriceListWorkflow, by name.
var priceQuoteWorkflowServerRetryPriceListWorkflowSteps = map[string]func(*PriceQuoteWorkflowServer, context.Context, *pricingv1.RetryPriceListWorkflowRequest) (*pricingv1.RetryPriceListWorkflowResponse, error){
	"default": (*PriceQuoteWorkflowServer).retryPriceListWorkflowStep,
	"audited": (*PriceQuoteWorkflowServer).retryPriceListWorkflowAuditedStep,
}

func (s *PriceQuoteWorkflowServer) retryPriceListWorkflowStep(ctx context.Context, request *pricingv1.RetryPriceListWorkflowRequest) (*pricingv1.RetryPriceListWorkflowResponse, error) {
	return s.priceListWorkflowClient.RetryPriceListWorkflow(ctx, request)
}

func (s *PriceQuoteWorkflowServer) retryPriceListWorkflowAuditedStep(ctx context.Context, request *pricingv1.RetryPriceListWorkflowRequest) (*pricingv1.RetryPriceListWorkflowResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.pricing.v1.PriceListWorkflowService/RetryPriceListWorkflow")
	return s.priceListWorkflowClient.RetryPriceListWorkflow(ctx, request)
}

// priceQuoteWorkflowServerApprovePriceListSteps are the ways to run
// acme.pricing.v1.PriceListApprovalService/ApprovePriceList, by name.
var priceQuoteWorkflowServerApprovePriceListSteps = map[string]func(*PriceQuoteWorkflowServer, context.Context, *pricingv1.ApprovePriceListRequest) (*pricingv1.ApprovePriceListResponse, error){
	"default": (*PriceQuoteWorkflowServer).approvePriceListStep,
	"audited": (*PriceQuoteWorkflowServer).approvePriceListAuditedStep,
}

func (s *PriceQuoteWorkflowServer) approvePriceListStep(ctx context.Context, request *pricingv1.ApprovePriceListRequest) (*pricingv1.ApprovePriceListResponse, error) {
	return s.priceListApprovalClient.ApprovePriceList(ctx, request)
}

func (s *PriceQuoteWorkflowServer) approvePriceListAuditedStep(ctx context.Context, request *pricingv1.ApprovePriceListRequest) (*pricingv1.ApprovePriceListResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.pricing.v1.PriceListApprovalService/ApprovePriceList")
	return s.priceListApprovalClient.ApprovePriceList(ctx, request)
}
