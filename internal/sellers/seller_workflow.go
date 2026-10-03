package sellers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	modelregistryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/modelregistry/v1"
	ratelimitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ratelimiting/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// SellerWorkflowServer implements `acme.sellers.v1.SellerWorkflowService`.
// Runs the long-lived workflow that takes a seller from start to finish.
type SellerWorkflowServer struct {
	sellersv1.UnimplementedSellerWorkflowServiceServer
	store                               *Store
	log                                 *slog.Logger
	conn                                *grpc.ClientConn
	meteredRegisteredModel              meteredRegisteredModel
	userQueryClient                     identityv1.UserQueryServiceClient
	preferenceCalculationClient         customersv1.PreferenceCalculationServiceClient
	searchQueryNotificationClient       searchv1.SearchQueryNotificationServiceClient
	sellerPayoutScheduleReportingClient sellersv1.SellerPayoutScheduleReportingServiceClient
	rateLimitQueryClient                ratelimitingv1.RateLimitQueryServiceClient
	sellerApprovalClient                sellersv1.SellerApprovalServiceClient
	customerValidationClient            customersv1.CustomerValidationServiceClient
	metricSeriesQueryClient             metricsv1.MetricSeriesQueryServiceClient
	householdCalculationClient          customersv1.HouseholdCalculationServiceClient
}

// NewSellerWorkflowServer wires a SellerWorkflowServer.
func NewSellerWorkflowServer(deps platform.Deps) *SellerWorkflowServer {
	return &SellerWorkflowServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.sellers.v1.SellerWorkflowService"),
		conn:                                deps.Conn,
		meteredRegisteredModel:              meteredRegisteredModel{RegisteredModelServiceClient: modelregistryv1.NewRegisteredModelServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		userQueryClient:                     identityv1.NewUserQueryServiceClient(deps.Conn),
		preferenceCalculationClient:         customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		searchQueryNotificationClient:       searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		sellerPayoutScheduleReportingClient: sellersv1.NewSellerPayoutScheduleReportingServiceClient(deps.Conn),
		rateLimitQueryClient:                ratelimitingv1.NewRateLimitQueryServiceClient(deps.Conn),
		sellerApprovalClient:                sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		customerValidationClient:            customersv1.NewCustomerValidationServiceClient(deps.Conn),
		metricSeriesQueryClient:             metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		householdCalculationClient:          customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
	}
}

// StartSellerWorkflow starts a seller.
func (s *SellerWorkflowServer) StartSellerWorkflow(ctx context.Context, req *sellersv1.StartSellerWorkflowRequest) (*sellersv1.StartSellerWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "start seller workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetSeller().GetPrimaryAddress().GetRegion())
	findAuditEventsRequest := &auditv1.FindAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.FindAuditEventsRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: req.GetSeller().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetSeller().GetRelationshipManager().GetUserId(),
		},
	}
	findAuditEventsReply := new(auditv1.FindAuditEventsResponse)
	if err := grpcx.Invoke(ctx, s.conn, auditv1.AuditEventQueryService_FindAuditEvents_FullMethodName, findAuditEventsRequest, findAuditEventsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/FindAuditEvents")
	}
	updateRegisteredModelRequest := &modelregistryv1.UpdateRegisteredModelRequest{
		Context: req.GetContext(),
		RegisteredModel: &modelregistryv1.RegisteredModel{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetSeller().GetAudit().GetCreatedBy()},
		},
	}
	updateRegisteredModelReply, err := s.meteredRegisteredModel.UpdateRegisteredModel(ctx, updateRegisteredModelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.modelregistry.v1.RegisteredModelService/UpdateRegisteredModel")
	}
	return &sellersv1.StartSellerWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: findAuditEventsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &sellersv1.StartSellerWorkflowResponse_Outcome{Status: updateRegisteredModelReply.GetResult().GetStatus()},
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: updateRegisteredModelReply.GetRegisteredModel().GetAddress().GetPostalCode(),
			},
			SellerId: req.GetSellerId(),
			Status:   req.GetSeller().GetStatus(),
		},
	}, nil
}

// AdvanceSellerWorkflow advances a seller.
func (s *SellerWorkflowServer) AdvanceSellerWorkflow(ctx context.Context, req *sellersv1.AdvanceSellerWorkflowRequest) (*sellersv1.AdvanceSellerWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "advance seller workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	peekUserRequest := buildPeekUserRequestForAdvanceSellerWorkflow(req)
	peekUserReply, err := s.userQueryClient.PeekUser(ctx, peekUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/PeekUser")
	}
	return &sellersv1.AdvanceSellerWorkflowResponse{
		Seller: &sellersv1.Seller{
			PrimaryAddress: peekUserReply.GetUser().GetPrimaryAddress(),
			SellerId:       req.GetSellerId(),
			Attributes:     req.GetSeller().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelSellerWorkflow cancels a seller.
func (s *SellerWorkflowServer) CancelSellerWorkflow(ctx context.Context, req *sellersv1.CancelSellerWorkflowRequest) (*sellersv1.CancelSellerWorkflowResponse, error) {
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "cancel seller workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := sellerWorkflowServerSimulatePreferenceSteps[mode]
	simulatePreferenceRequest := &customersv1.SimulatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetSeller().GetAudit().GetCreatedAt()},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
		},
	}
	simulatePreferenceReply, err := run(s, ctx, simulatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/SimulatePreference")
	}
	notifySearchQueryWatchersRequest := &searchv1.NotifySearchQueryWatchersRequest{
		Context: req.GetContext(),
		Details: &searchv1.NotifySearchQueryWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetSeller().GetRelationshipManager().GetEmail()},
		},
	}
	notifySearchQueryWatchersReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*searchv1.NotifySearchQueryWatchersResponse, error) {
		return s.searchQueryNotificationClient.NotifySearchQueryWatchers(ctx, notifySearchQueryWatchersRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/NotifySearchQueryWatchers")
	}
	var holder struct{ CreatedBy string }
	if record := req.GetSeller().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "CreatedBy"); err != nil {
			s.log.WarnContext(ctx, "copy created_by", "error", err)
		}
	}
	calculateCartRuleRequest := &cartv1.CalculateCartRuleRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetSeller().GetAudit().GetEtag(),
				CreatedBy: holder.CreatedBy,
				UpdatedBy: req.GetSeller().GetAudit().GetUpdatedBy(),
			},
		},
	}
	calculateCartRuleReply := new(cartv1.CalculateCartRuleResponse)
	if err := s.conn.Invoke(ctx, cartCartRuleCalculationServiceCalculateCartRuleMethod, calculateCartRuleRequest, calculateCartRuleReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/CalculateCartRule")
	}
	return &sellersv1.CancelSellerWorkflowResponse{
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				Etag:      simulatePreferenceReply.GetPreference().GetAudit().GetEtag(),
				CreatedAt: calculateCartRuleReply.GetCartRule().GetAudit().GetCreatedAt(),
			},
			RelationshipManager: &identityv1.UserRef{
				UserId:      simulatePreferenceReply.GetPreference().GetOwner().GetUserId(),
				DisplayName: calculateCartRuleReply.GetCartRule().GetOwner().GetDisplayName(),
			},
			SellerId:   req.GetSellerId(),
			Attributes: req.GetSeller().GetAttributes(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: notifySearchQueryWatchersReply.GetResult().GetCorrelationId(),
			Message:       notifySearchQueryWatchersReply.GetOutcome().GetMessage(),
			Status:        "ok",
		},
	}, nil
}

// RetrySellerWorkflow retrys a seller.
func (s *SellerWorkflowServer) RetrySellerWorkflow(ctx context.Context, req *sellersv1.RetrySellerWorkflowRequest) (*sellersv1.RetrySellerWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetrySellerWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "retry seller workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	streamSellerPayoutScheduleMetricsStream, err := s.sellerPayoutScheduleReportingClient.StreamSellerPayoutScheduleMetrics(ctx, &sellersv1.StreamSellerPayoutScheduleMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Name: req.GetSeller().GetContact().GetName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/StreamSellerPayoutScheduleMetrics")
	}
	for {
		received, err := streamSellerPayoutScheduleMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/StreamSellerPayoutScheduleMetrics")
		}
		_ = received
	}
	return &sellersv1.RetrySellerWorkflowResponse{
		Seller: &sellersv1.Seller{
			SellerId:       req.GetSellerId(),
			PrimaryAddress: req.GetSeller().GetPrimaryAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseSellerWorkflow pauses a seller.
func (s *SellerWorkflowServer) PauseSellerWorkflow(ctx context.Context, req *sellersv1.PauseSellerWorkflowRequest) (*sellersv1.PauseSellerWorkflowResponse, error) {
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "pause seller workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	checkSellerListingEligibilityRequest := &sellersv1.CheckSellerListingEligibilityRequest{
		Context: req.GetContext(),
		Details: &sellersv1.CheckSellerListingEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	checkSellerListingEligibilityReply, err := checkSellerListingEligibilityCall(ctx, checkSellerListingEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/CheckSellerListingEligibility")
	}
	return &sellersv1.PauseSellerWorkflowResponse{
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient: checkSellerListingEligibilityReply.GetSellerListing().GetAddress().GetRecipient(),
			},
			SellerId:   req.GetSellerId(),
			Attributes: req.GetSeller().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeSellerWorkflow resumes a seller.
func (s *SellerWorkflowServer) ResumeSellerWorkflow(ctx context.Context, req *sellersv1.ResumeSellerWorkflowRequest) (*sellersv1.ResumeSellerWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeSellerWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "resume seller workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	suggestRateLimitsRequest := &ratelimitingv1.SuggestRateLimitsRequest{
		Context: req.GetContext(),
		Filter: &ratelimitingv1.SuggestRateLimitsRequest_Filter{
			Region:       req.GetSeller().GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetSeller().GetPassport().GetExpiresAt(),
		},
	}
	suggestRateLimitsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*ratelimitingv1.SuggestRateLimitsResponse, error) {
		return s.rateLimitQueryClient.SuggestRateLimits(ctx, suggestRateLimitsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ratelimiting.v1.RateLimitQueryService/SuggestRateLimits")
	}
	cloned := proto.Clone(req.GetSeller()).(*sellersv1.Seller)
	submitSellerRequest := &sellersv1.SubmitSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetSeller().GetContact().GetPhone(),
				Email: req.GetSeller().GetRelationshipManager().GetEmail(),
			},
			PrimaryAddress: &commonv1.PostalAddress{PostalCode: cloned.GetPrimaryAddress().GetPostalCode()},
		},
	}
	submitSellerReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*sellersv1.SubmitSellerResponse, error) {
		return s.sellerApprovalClient.SubmitSeller(ctx, submitSellerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/SubmitSeller")
	}
	return &sellersv1.ResumeSellerWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message:       suggestRateLimitsReply.GetResult().GetMessage(),
			CorrelationId: suggestRateLimitsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: submitSellerReply.GetSeller().GetAudit().GetUpdatedBy(),
			},
			SellerId: req.GetSellerId(),
			Contact:  &commonv1.ContactInfo{Name: req.GetSeller().GetName()},
		},
	}, nil
}

// InspectSellerWorkflow returns a seller.
func (s *SellerWorkflowServer) InspectSellerWorkflow(ctx context.Context, req *sellersv1.InspectSellerWorkflowRequest) (*sellersv1.InspectSellerWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "inspect_seller_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect seller workflow", "tenant", req.GetContext().GetTenantId())
	scoreCustomerRequest := &customersv1.ScoreCustomerRequest{}
	scoreCustomerRequest.Context = req.GetContext()
	scoreCustomerRequest.Customer = &customersv1.Customer{
		RelationshipManager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
	}
	scoreCustomerReply, err := s.customerValidationClient.ScoreCustomer(ctx, scoreCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ScoreCustomer")
	}
	return &sellersv1.InspectSellerWorkflowResponse{
		Seller: &sellersv1.Seller{
			Attributes: scoreCustomerReply.GetCustomer().GetAttributes(),
			RelationshipManager: &identityv1.UserRef{
				DisplayName: scoreCustomerReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
			SellerId: req.GetSellerId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSellerWorkflows lists sellers a page at a time.
func (s *SellerWorkflowServer) ListSellerWorkflows(ctx context.Context, req *sellersv1.ListSellerWorkflowsRequest) (*sellersv1.ListSellerWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list seller workflows", "tenant", req.GetContext().GetTenantId())
	countMetricSeriesesRequest := &metricsv1.CountMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.CountMetricSeriesesRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Labels:       req.GetFilter().GetLabels(),
		},
		OrderBy: req.GetOrderBy(),
	}
	type countMetricSeriesesResult struct {
		reply *metricsv1.CountMetricSeriesesResponse
		err   error
	}
	countMetricSeriesesResults := make(chan countMetricSeriesesResult, 1)
	go func() {
		reply, err := s.metricSeriesQueryClient.CountMetricSerieses(ctx, countMetricSeriesesRequest)
		countMetricSeriesesResults <- countMetricSeriesesResult{reply, err}
	}()
	var countMetricSeriesesReply *metricsv1.CountMetricSeriesesResponse
	select {
	case result := <-countMetricSeriesesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.metrics.v1.MetricSeriesQueryService/CountMetricSerieses")
		}
		countMetricSeriesesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &sellersv1.ListSellerWorkflowsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: countMetricSeriesesReply.GetResult().GetReferenceIds(),
			Status:       countMetricSeriesesReply.GetResult().GetStatus(),
		},
	}, nil
}

// SignalSellerWorkflow signals a seller.
func (s *SellerWorkflowServer) SignalSellerWorkflow(ctx context.Context, req *sellersv1.SignalSellerWorkflowRequest) (*sellersv1.SignalSellerWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SignalSellerWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "signal seller workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "etag", req.GetSeller().GetAudit().GetEtag())
	simulateHouseholdReply, err := s.householdCalculationClient.SimulateHousehold(ctx, &customersv1.SimulateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						CreatedAt: req.GetSeller().GetAudit().GetCreatedAt(),
						Etag:      tenancy.Value(ctx, "etag"),
					},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/SimulateHousehold")
	}
	quotePreferenceReply, err := s.preferenceCalculationClient.QuotePreference(ctx, &customersv1.QuotePreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.QuotePreferenceRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient:   req.GetSeller().GetPrimaryAddress().GetRecipient(),
				CountryCode: simulateHouseholdReply.GetHousehold().GetLocation().GetCountryCode(),
			},
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetSeller().GetAudit().GetCreatedAt(),
				Etag:      req.GetSeller().GetAudit().GetEtag(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/QuotePreference")
	}
	return &sellersv1.SignalSellerWorkflowResponse{
		Seller: &sellersv1.Seller{
			Attributes: simulateHouseholdReply.GetHousehold().GetAttributes(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulateHouseholdReply.GetHousehold().GetAudit().GetUpdatedAt(),
				CreatedBy: quotePreferenceReply.GetPreference().GetAudit().GetCreatedBy(),
			},
			SellerId:            req.GetSellerId(),
			RelationshipManager: req.GetSeller().GetRelationshipManager(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// sellerWorkflowServerSimulatePreferenceSteps are the ways to run
// acme.customers.v1.PreferenceCalculationService/SimulatePreference, by name.
var sellerWorkflowServerSimulatePreferenceSteps = map[string]func(*SellerWorkflowServer, context.Context, *customersv1.SimulatePreferenceRequest) (*customersv1.SimulatePreferenceResponse, error){
	"default": (*SellerWorkflowServer).simulatePreferenceStep,
	"audited": (*SellerWorkflowServer).simulatePreferenceAuditedStep,
}

func (s *SellerWorkflowServer) simulatePreferenceStep(ctx context.Context, request *customersv1.SimulatePreferenceRequest) (*customersv1.SimulatePreferenceResponse, error) {
	return s.preferenceCalculationClient.SimulatePreference(ctx, request)
}

func (s *SellerWorkflowServer) simulatePreferenceAuditedStep(ctx context.Context, request *customersv1.SimulatePreferenceRequest) (*customersv1.SimulatePreferenceResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.customers.v1.PreferenceCalculationService/SimulatePreference")
	return s.preferenceCalculationClient.SimulatePreference(ctx, request)
}
