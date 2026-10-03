package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	audiencesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audiences/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	onboardingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// BackorderValidationServer implements `acme.orders.v1.BackorderValidationService`.
// Checks backorders against the rules before anything acts on them.
type BackorderValidationServer struct {
	ordersv1.UnimplementedBackorderValidationServiceServer
	store                              *Store
	log                                *slog.Logger
	onboardingPlanQueryClient          onboardingv1.OnboardingPlanQueryServiceClient
	payoutCalculationClient            paymentsv1.PayoutCalculationServiceClient
	orderAmendmentValidationClient     ordersv1.OrderAmendmentValidationServiceClient
	bus                                *eventbus.Bus
	orderAmendmentCalculationClient    ordersv1.OrderAmendmentCalculationServiceClient
	orderValidationClient              ordersv1.OrderValidationServiceClient
	audienceClient                     audiencesv1.AudienceServiceClient
	tenantNotificationClient           identityv1.TenantNotificationServiceClient
	tenantNotificationClientFallback   identityv1.TenantNotificationServiceClient
	returnRequestQueryClient           returnsv1.ReturnRequestQueryServiceClient
	searchIndexClient                  searchv1.SearchIndexServiceClient
	parcelCalculationClient            shippingv1.ParcelCalculationServiceClient
	searchIndexNotificationClient      searchv1.SearchIndexNotificationServiceClient
	explainOrderAmendmentCalculationFn func(context.Context, *ordersv1.ExplainOrderAmendmentCalculationRequest, ...grpc.CallOption) (*ordersv1.ExplainOrderAmendmentCalculationResponse, error)
	orderValidationPort                orderValidationPort
	inspectionCalculationClient        returnsv1.InspectionCalculationServiceClient
	backorderCalculationClient         ordersv1.BackorderCalculationServiceClient
}

// NewBackorderValidationServer wires a BackorderValidationServer.
func NewBackorderValidationServer(deps platform.Deps) *BackorderValidationServer {
	orderAmendmentCalculation := ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn)
	return &BackorderValidationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.orders.v1.BackorderValidationService"),
		onboardingPlanQueryClient:          onboardingv1.NewOnboardingPlanQueryServiceClient(deps.Conn),
		payoutCalculationClient:            paymentsv1.NewPayoutCalculationServiceClient(deps.Conn),
		orderAmendmentValidationClient:     ordersv1.NewOrderAmendmentValidationServiceClient(deps.Conn),
		bus:                                deps.Bus,
		orderAmendmentCalculationClient:    ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn),
		orderValidationClient:              ordersv1.NewOrderValidationServiceClient(deps.Conn),
		audienceClient:                     audiencesv1.NewAudienceServiceClient(deps.Conn),
		tenantNotificationClient:           identityv1.NewTenantNotificationServiceClient(deps.Conn),
		tenantNotificationClientFallback:   identityv1.NewTenantNotificationServiceClient(deps.Conn),
		returnRequestQueryClient:           returnsv1.NewReturnRequestQueryServiceClient(deps.Conn),
		searchIndexClient:                  searchv1.NewSearchIndexServiceClient(deps.Conn),
		parcelCalculationClient:            shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		searchIndexNotificationClient:      searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		explainOrderAmendmentCalculationFn: orderAmendmentCalculation.ExplainOrderAmendmentCalculation,
		orderValidationPort:                newOrderValidationPort(deps),
		inspectionCalculationClient:        returnsv1.NewInspectionCalculationServiceClient(deps.Conn),
		backorderCalculationClient:         ordersv1.NewBackorderCalculationServiceClient(deps.Conn),
	}
}

// ValidateBackorder validates a backorder.
func (s *BackorderValidationServer) ValidateBackorder(ctx context.Context, req *ordersv1.ValidateBackorderRequest) (*ordersv1.ValidateBackorderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "validate backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	customFields := make(map[string]string, len(req.GetBackorder().GetCustomFields()))
	for key, value := range req.GetBackorder().GetCustomFields() {
		customFields[key] = value
	}
	exportOnboardingPlansStream, err := s.onboardingPlanQueryClient.ExportOnboardingPlans(ctx, &onboardingv1.ExportOnboardingPlansRequest{
		Context: req.GetContext(),
		Filter:  &onboardingv1.ExportOnboardingPlansRequest_Filter{Labels: customFields},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.onboarding.v1.OnboardingPlanQueryService/ExportOnboardingPlans")
	}
	for {
		received, err := exportOnboardingPlansStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.onboarding.v1.OnboardingPlanQueryService/ExportOnboardingPlans")
		}
		_ = received
	}
	cloned := proto.Clone(req.GetBackorder()).(*ordersv1.Backorder)
	recalculatePayoutsRequest := &paymentsv1.RecalculatePayoutsRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetBackorder().GetAssignee().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: cloned.GetAudit().GetCreatedBy(),
				UpdatedAt: req.GetBackorder().GetAudit().GetUpdatedAt(),
			},
		},
	}
	var recalculatePayoutsReply *paymentsv1.RecalculatePayoutsResponse
	if err := pipeline.Run(ctx, "acme.payments.v1.PayoutCalculationService/RecalculatePayouts", func(ctx context.Context) error {
		answer, err := s.payoutCalculationClient.RecalculatePayouts(ctx, recalculatePayoutsRequest)
		recalculatePayoutsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/RecalculatePayouts")
	}
	jobs := make(chan *ordersv1.BackorderComment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for comment := range jobs {
				if _, err := s.orderAmendmentValidationClient.ScoreOrderAmendment(workCtx, &ordersv1.ScoreOrderAmendmentRequest{
					Context: req.GetContext(),
					OrderAmendment: &ordersv1.OrderAmendment{
						Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
						BillingAddress: &commonv1.PostalAddress{
							CountryCode: recalculatePayoutsReply.GetPayout().GetBillingAddress().GetCountryCode(),
						},
					},
					Details: &ordersv1.ScoreOrderAmendmentRequest_Details{
						Destination: &commonv1.PostalAddress{
							PostalCode: recalculatePayoutsReply.GetPayout().GetBillingAddress().GetPostalCode(),
						},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetBackorder().GetComments() {
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
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/ScoreOrderAmendment")
	}
	return &ordersv1.ValidateBackorderResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recalculatePayoutsReply.GetPayout().GetAudit().GetUpdatedAt(),
				UpdatedBy: recalculatePayoutsReply.GetPayout().GetAudit().GetUpdatedBy(),
			},
			BackorderId: req.GetBackorderId(),
			Assignee:    req.GetBackorder().GetAssignee(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewBackorder previews a backorder.
func (s *BackorderValidationServer) PreviewBackorder(ctx context.Context, req *ordersv1.PreviewBackorderRequest) (*ordersv1.PreviewBackorderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewBackorder", "error", err)
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "preview backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	lintOrderAmendmentsRequest := &ordersv1.LintOrderAmendmentsRequest{}
	lintOrderAmendmentsRequest.Context = req.GetContext()
	lintOrderAmendmentsRequest.Details = &ordersv1.LintOrderAmendmentsRequest_Details{
		Contact: &commonv1.ContactInfo{Name: req.GetBackorder().GetRequester().GetName()},
	}
	lintOrderAmendmentsReply, err := s.orderAmendmentValidationClient.LintOrderAmendments(ctx, lintOrderAmendmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/LintOrderAmendments")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "orders.preview_backorder.validate_synonym",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.contact.phone":             req.GetDetails().GetContact().GetPhone(),
			"details.contact.preferred_channel": req.GetBackorder().GetRequester().GetPreferredChannel(),
			"details.contact.email":             req.GetBackorder().GetRequester().GetEmail(),
			"context.tenant_id":                 req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	for _, comment := range req.GetBackorder().GetComments() {
		if _, err := s.orderAmendmentCalculationClient.EstimateOrderAmendment(ctx, &ordersv1.EstimateOrderAmendmentRequest{
			Context: req.GetContext(),
			OrderAmendment: &ordersv1.OrderAmendment{
				Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/EstimateOrderAmendment")
		}
	}
	return &ordersv1.PreviewBackorderResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintOrderAmendmentsReply.GetOrderAmendment().GetAudit().GetCreatedBy(),
			},
			BackorderId: req.GetBackorderId(),
			Subject:     req.GetBackorder().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			Details: lintOrderAmendmentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// CheckBackorderEligibility checks a backorder.
func (s *BackorderValidationServer) CheckBackorderEligibility(ctx context.Context, req *ordersv1.CheckBackorderEligibilityRequest) (*ordersv1.CheckBackorderEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckBackorderEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "check backorder eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	lintOrdersRequest := &ordersv1.LintOrdersRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetBackorder().GetCustomer().GetDisplayName(),
				CustomerId:  req.GetBackorder().GetCustomer().GetCustomerId(),
			},
		},
		Details: &ordersv1.LintOrdersRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetDetails().GetContact().GetEmail(),
				Name:  req.GetDetails().GetContact().GetName(),
			},
		},
	}
	lintOrdersReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*ordersv1.LintOrdersResponse, error) {
		return s.orderValidationClient.LintOrders(ctx, lintOrdersRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/LintOrders")
	}
	updateAudienceRequest := &audiencesv1.UpdateAudienceRequest{
		Context: req.GetContext(),
		Audience: &audiencesv1.Audience{
			Owner: &identityv1.UserRef{
				Email:       req.GetBackorder().GetRequester().GetEmail(),
				DisplayName: lintOrdersReply.GetOrder().GetCounterparty().GetDisplayName(),
			},
		},
	}
	updateAudienceReply, err := grpcx.Call(ctx, s.audienceClient.UpdateAudience, updateAudienceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audiences.v1.AudienceService/UpdateAudience")
	}
	simulatePayoutReply, err := s.payoutCalculationClient.SimulatePayout(ctx, &paymentsv1.SimulatePayoutRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				Etag:      lintOrdersReply.GetOrder().GetAudit().GetEtag(),
				CreatedBy: lintOrdersReply.GetOrder().GetAudit().GetCreatedBy(),
			},
			Lines: []*paymentsv1.PayoutLine{&paymentsv1.PayoutLine{
				Tax: &paymentsv1.PayoutLineTax{
					Amount: updateAudienceReply.GetAudience().GetDiscount().GetAmount(),
				},
			}},
			Settlement: &paymentsv1.Payout_Bank{Bank: &paymentsv1.PayoutBankSettlement{Bic: lintOrdersReply.GetOrder().GetBank().GetBic()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/SimulatePayout")
	}
	routes := map[string]func(context.Context, *identityv1.NotifyTenantWatchersRequest, ...grpc.CallOption) (*identityv1.NotifyTenantWatchersResponse, error){
		"primary":  s.tenantNotificationClient.NotifyTenantWatchers,
		"fallback": s.tenantNotificationClientFallback.NotifyTenantWatchers,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	detailsCache := cache.New[string, *ordersv1.CheckBackorderEligibilityRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	notifyTenantWatchersRequest := &identityv1.NotifyTenantWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifyTenantWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            cachedDetails.GetContact().GetPhone(),
				PreferredChannel: req.GetBackorder().GetRequester().GetPreferredChannel(),
			},
		},
	}
	notifyTenantWatchersReply, err := route(ctx, notifyTenantWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/NotifyTenantWatchers")
	}
	return &ordersv1.CheckBackorderEligibilityResponse{
		Backorder: &ordersv1.Backorder{
			Sla:          &ordersv1.BackorderSla{DueAt: lintOrdersReply.GetOrder().GetDueAt()},
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{
			Status:  lintOrdersReply.GetResult().GetStatus(),
			Details: notifyTenantWatchersReply.GetResult().GetDetails(),
		},
		Outcome: &ordersv1.CheckBackorderEligibilityResponse_Outcome{
			Status:  updateAudienceReply.GetResult().GetStatus(),
			Message: simulatePayoutReply.GetResult().GetMessage(),
		},
	}, nil
}

// NormalizeBackorder normalizes a backorder.
func (s *BackorderValidationServer) NormalizeBackorder(ctx context.Context, req *ordersv1.NormalizeBackorderRequest) (*ordersv1.NormalizeBackorderResponse, error) {
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "normalize backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*returnsv1.FindReturnRequestsResponse, len(req.GetBackorder().GetHistory()))
	for i, item := range req.GetBackorder().GetHistory() {
		group.Go(func() error {
			reply, err := s.returnRequestQueryClient.FindReturnRequests(groupCtx, &returnsv1.FindReturnRequestsRequest{
				Context: req.GetContext(),
				Filter: &returnsv1.FindReturnRequestsRequest_Filter{
					CreatedAfter: item.GetChangedAt(),
					OwnerId:      req.GetBackorder().GetCustomer().GetCustomerId(),
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
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestQueryService/FindReturnRequests")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &ordersv1.NormalizeBackorderResponse{
		Backorder: &ordersv1.Backorder{
			BackorderId: req.GetBackorderId(),
			Assignee:    req.GetBackorder().GetAssignee(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainBackorderDecision explains a backorder.
func (s *BackorderValidationServer) ExplainBackorderDecision(ctx context.Context, req *ordersv1.ExplainBackorderDecisionRequest) (*ordersv1.ExplainBackorderDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "explain backorder decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	var displayName string
	switch choice := req.GetBackorder().GetOrigin().(type) {
	case *ordersv1.Backorder_Customer:
		displayName = choice.Customer.GetDisplayName()
	case *ordersv1.Backorder_ExternalReference:
		displayName = choice.ExternalReference
	case *ordersv1.Backorder_Channel:
		displayName = choice.Channel.GetChannel()
	}
	updateSearchIndexRequest := &searchv1.UpdateSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Owner: &identityv1.UserRef{
				DisplayName: displayName,
				Email:       req.GetBackorder().GetAssignee().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetBackorder().GetAudit().GetUpdatedAt()},
		},
	}
	updateSearchIndexReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*searchv1.UpdateSearchIndexResponse, error) {
		return s.searchIndexClient.UpdateSearchIndex(ctx, updateSearchIndexRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/UpdateSearchIndex")
	}
	calculatePayoutReply, err := s.payoutCalculationClient.CalculatePayout(ctx, &paymentsv1.CalculatePayoutRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Lines: []*paymentsv1.PayoutLine{&paymentsv1.PayoutLine{
				Description: updateSearchIndexReply.GetSearchIndex().GetComponents()["primary"].GetDescription(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: updateSearchIndexReply.GetSearchIndex().GetAudit().GetCreatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/CalculatePayout")
	}
	estimateParcelRequest := &shippingv1.EstimateParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				Etag: updateSearchIndexReply.GetSearchIndex().GetAudit().GetEtag(),
			},
		},
		Details: &shippingv1.EstimateParcelRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: updateSearchIndexReply.GetSearchIndex().GetAddress().GetRegion(),
			},
		},
	}
	estimateParcelReply, err := shippingv1.ParcelCalculationServiceClient.EstimateParcel(s.parcelCalculationClient, ctx, estimateParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/EstimateParcel")
	}
	return &ordersv1.ExplainBackorderDecisionResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				Etag: updateSearchIndexReply.GetSearchIndex().GetAudit().GetEtag(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: calculatePayoutReply.GetPayout().GetCounterparty().GetDisplayName(),
			},
			Requester: &commonv1.ContactInfo{
				Email: estimateParcelReply.GetParcel().GetOwner().GetEmail(),
			},
			BackorderId: req.GetBackorderId(),
			Priority:    req.GetBackorder().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: calculatePayoutReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ScoreBackorder scores a backorder.
func (s *BackorderValidationServer) ScoreBackorder(ctx context.Context, req *ordersv1.ScoreBackorderRequest) (*ordersv1.ScoreBackorderResponse, error) {
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "score backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	streamSearchIndexNoticesStream, err := s.searchIndexNotificationClient.StreamSearchIndexNotices(ctx, &searchv1.StreamSearchIndexNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: req.GetBackorder().GetAssignee().GetEmail(),
			Name:  req.GetBackorder().GetRequester().GetName(),
		},
		Filter: &searchv1.StreamSearchIndexNoticesRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
	}
	for {
		received, err := streamSearchIndexNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
		}
		_ = received
	}
	cloned := proto.Clone(req.GetBackorder()).(*ordersv1.Backorder)
	explainOrderAmendmentCalculationRequest := &ordersv1.ExplainOrderAmendmentCalculationRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			BillingAddress: &commonv1.PostalAddress{
				Locality:  req.GetDetails().GetDestination().GetLocality(),
				Recipient: req.GetDetails().GetDestination().GetRecipient(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: cloned.GetAudit().GetCreatedAt()},
		},
	}
	explainOrderAmendmentCalculationReply, err := s.explainOrderAmendmentCalculationFn(ctx, explainOrderAmendmentCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/ExplainOrderAmendmentCalculation")
	}
	checkOrderEligibilityRequest := &ordersv1.CheckOrderEligibilityRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{Email: req.GetBackorder().GetAssignee().GetEmail()},
			Audit:        &commonv1.AuditInfo{UpdatedBy: req.GetBackorder().GetAudit().GetUpdatedBy()},
			Settlement: &ordersv1.Order_Bank{Bank: &ordersv1.OrderBankSettlement{
				Bic:  explainOrderAmendmentCalculationReply.GetOrderAmendment().GetBank().GetBic(),
				Iban: explainOrderAmendmentCalculationReply.GetOrderAmendment().GetBank().GetIban(),
			}},
		},
	}
	checkOrderEligibilityReply, err := s.orderValidationPort.CheckOrderEligibility(ctx, checkOrderEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/CheckOrderEligibility")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*returnsv1.SimulateInspectionResponse, len(req.GetBackorder().GetComments()))
	for i, comment := range req.GetBackorder().GetComments() {
		group.Go(func() error {
			reply, err := s.inspectionCalculationClient.SimulateInspection(groupCtx, &returnsv1.SimulateInspectionRequest{
				Context: req.GetContext(),
				Inspection: &returnsv1.Inspection{
					Comments: []*returnsv1.InspectionComment{&returnsv1.InspectionComment{
						CommentId: comment.GetCommentId(),
						CreatedAt: comment.GetCreatedAt(),
						Body:      comment.GetBody(),
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
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/SimulateInspection")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &ordersv1.ScoreBackorderResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainOrderAmendmentCalculationReply.GetOrderAmendment().GetAudit().GetCreatedBy(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: checkOrderEligibilityReply.GetOrder().GetCounterparty().GetDisplayName(),
			},
			BackorderId: req.GetBackorderId(),
			Priority:    req.GetBackorder().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetBackorderRules returns a backorder.
func (s *BackorderValidationServer) GetBackorderRules(ctx context.Context, req *ordersv1.GetBackorderRulesRequest) (*ordersv1.GetBackorderRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "get_backorder_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get backorder rules", "tenant", req.GetContext().GetTenantId())
	explainBackorderCalculationRequest := &ordersv1.ExplainBackorderCalculationRequest{}
	explainBackorderCalculationRequest.Context = req.GetContext()
	explainBackorderCalculationRequest.Backorder = &ordersv1.Backorder{BackorderId: req.GetBackorderId()}
	explainBackorderCalculationReply, err := s.backorderCalculationClient.ExplainBackorderCalculation(ctx, explainBackorderCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/ExplainBackorderCalculation")
	}
	previewOrderAmendmentRequest := newPreviewOrderAmendmentRequest(withPreviewOrderAmendmentContext(req.GetContext()), withPreviewOrderAmendmentOrderAmendment(&ordersv1.OrderAmendment{
		Audit: &commonv1.AuditInfo{
			UpdatedBy: explainBackorderCalculationReply.GetBackorder().GetAudit().GetUpdatedBy(),
		},
		Counterparty: &ordersv1.BackorderRef{
			DisplayName: explainBackorderCalculationReply.GetBackorder().GetCustomer().GetDisplayName(),
		},
	}), withPreviewOrderAmendmentDetails(&ordersv1.PreviewOrderAmendmentRequest_Details{
		Contact: &commonv1.ContactInfo{
			Email: explainBackorderCalculationReply.GetBackorder().GetAssignee().GetEmail(),
		},
	}))
	previewOrderAmendmentReply, err := s.orderAmendmentValidationClient.PreviewOrderAmendment(ctx, previewOrderAmendmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/PreviewOrderAmendment")
	}
	return &ordersv1.GetBackorderRulesResponse{
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				DisplayName: explainBackorderCalculationReply.GetBackorder().GetAssignee().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewOrderAmendmentReply.GetOrderAmendment().GetAudit().GetCreatedBy(),
			},
			BackorderId: req.GetBackorderId(),
		},
		Result: &commonv1.OperationResult{
			Details: previewOrderAmendmentReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}
