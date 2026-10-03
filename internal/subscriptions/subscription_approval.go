package subscriptions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	contentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/content/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	lineagev1 "github.com/hounddogai/protomap-test-golang/gen/acme/lineage/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// SubscriptionApprovalServer implements `acme.subscriptions.v1.SubscriptionApprovalService`.
// Routes subscriptions through approval, with escalation and delegation.
type SubscriptionApprovalServer struct {
	subscriptionsv1.UnimplementedSubscriptionApprovalServiceServer
	store                         *Store
	log                           *slog.Logger
	synonymHistoryClient          searchv1.SynonymHistoryServiceClient
	clients                       *registry.Registry
	featureFlagQueryClient        featureflagsv1.FeatureFlagQueryServiceClient
	sellerScorecardApprovalClient sellersv1.SellerScorecardApprovalServiceClient
	conn                          *grpc.ClientConn
	articleQueryClient            contentv1.ArticleQueryServiceClient
	householdValidationClient     customersv1.HouseholdValidationServiceClient
	metricSeriesQueryClient       metricsv1.MetricSeriesQueryServiceClient
	preferenceCalculationPort     preferenceCalculationPort
	usageRecordCalculationClient  subscriptionsv1.UsageRecordCalculationServiceClient
	customerOnce                  sync.Once
	customerCached                customersv1.CustomerServiceClient
}

// NewSubscriptionApprovalServer wires a SubscriptionApprovalServer.
func NewSubscriptionApprovalServer(deps platform.Deps) *SubscriptionApprovalServer {
	return &SubscriptionApprovalServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.subscriptions.v1.SubscriptionApprovalService"),
		synonymHistoryClient:          searchv1.NewSynonymHistoryServiceClient(deps.Conn),
		clients:                       deps.Clients,
		featureFlagQueryClient:        featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		sellerScorecardApprovalClient: sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		conn:                          deps.Conn,
		articleQueryClient:            contentv1.NewArticleQueryServiceClient(deps.Conn),
		householdValidationClient:     customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		metricSeriesQueryClient:       metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		preferenceCalculationPort:     newPreferenceCalculationPort(deps),
		usageRecordCalculationClient:  subscriptionsv1.NewUsageRecordCalculationServiceClient(deps.Conn),
	}
}

// SubmitSubscription submits a subscription.
func (s *SubscriptionApprovalServer) SubmitSubscription(ctx context.Context, req *subscriptionsv1.SubmitSubscriptionRequest) (*subscriptionsv1.SubmitSubscriptionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "submit subscription", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	for _, limit := range req.GetSubscription().GetLimits() {
		if _, err := s.synonymHistoryClient.RevertSynonym(ctx, &searchv1.RevertSynonymRequest{
			Context: req.GetContext(),
			Synonym: &searchv1.Synonym{
				Action: &searchv1.Synonym_Discount{Discount: &searchv1.SynonymDiscountAction{Amount: limit.GetAmount()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/RevertSynonym")
		}
	}
	consentRecordValidation, err := registry.Lookup[customersv1.ConsentRecordValidationServiceClient](s.clients, "acme.customers.v1.ConsentRecordValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "email", req.GetSubscription().GetContact().GetEmail())
	packed, err := anypb.New(req.GetSubscription())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizeConsentRecordRequest := &customersv1.NormalizeConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.NormalizeConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: tenancy.Value(ctx, "email"),
				Name:  req.GetSubscription().GetContact().GetName(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	normalizeConsentRecordReply, err := consentRecordValidation.NormalizeConsentRecord(ctx, normalizeConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord")
	}
	return &subscriptionsv1.SubmitSubscriptionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: normalizeConsentRecordReply.GetConsentRecord().GetCorrelationId(),
			Status:        "ok",
		},
		Subscription: &subscriptionsv1.Subscription{
			SubscriptionId: req.GetSubscriptionId(),
			OpenedAt:       req.GetSubscription().GetOpenedAt(),
		},
	}, nil
}

// ApproveSubscription approves a subscription.
func (s *SubscriptionApprovalServer) ApproveSubscription(ctx context.Context, req *subscriptionsv1.ApproveSubscriptionRequest) (*subscriptionsv1.ApproveSubscriptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveSubscription", "error", err)
		return nil, err
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "approve subscription", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	suggestFeatureFlagsRequest := buildSuggestFeatureFlagsRequestForApproveSubscription(req)
	suggestFeatureFlagsReply, err := s.featureFlagQueryClient.SuggestFeatureFlags(ctx, suggestFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/SuggestFeatureFlags")
	}
	for _, limit := range req.GetSubscription().GetLimits() {
		if _, err := s.sellerScorecardApprovalClient.CommentSellerScorecardApproval(ctx, &sellersv1.CommentSellerScorecardApprovalRequest{
			Context: req.GetContext(),
			SellerScorecard: &sellersv1.SellerScorecard{
				Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
					Tax: &sellersv1.SellerScorecardLineTax{Amount: limit.GetAmount()},
				}},
				Audit: &commonv1.AuditInfo{
					Etag:      req.GetSubscription().GetAudit().GetEtag(),
					UpdatedBy: req.GetSubscription().GetAudit().GetUpdatedBy(),
				},
			},
			Details: &sellersv1.CommentSellerScorecardApprovalRequest_Details{
				Approver: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/CommentSellerScorecardApproval")
		}
	}
	return &subscriptionsv1.ApproveSubscriptionResponse{
		Outcome: &subscriptionsv1.ApproveSubscriptionResponse_Outcome{
			Status:      suggestFeatureFlagsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Subscription: &subscriptionsv1.Subscription{SubscriptionId: req.GetSubscriptionId()},
		Result:       &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalateSubscription escalates a subscription.
func (s *SubscriptionApprovalServer) EscalateSubscription(ctx context.Context, req *subscriptionsv1.EscalateSubscriptionRequest) (*subscriptionsv1.EscalateSubscriptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "escalate subscription", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	batchGetLineageNodesRequest := &lineagev1.BatchGetLineageNodesRequest{Context: req.GetContext()}
	batchGetLineageNodesReply, err := grpcx.NewClient(s.conn, lineagev1.NewLineageNodeServiceClient).BatchGetLineageNodes(ctx, batchGetLineageNodesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.lineage.v1.LineageNodeService/BatchGetLineageNodes")
	}
	return &subscriptionsv1.EscalateSubscriptionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: batchGetLineageNodesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Subscription: &subscriptionsv1.Subscription{
			SubscriptionId: req.GetSubscriptionId(),
			Holder:         req.GetSubscription().GetHolder(),
		},
	}, nil
}

// ListPendingSubscriptions lists subscriptions a page at a time.
func (s *SubscriptionApprovalServer) ListPendingSubscriptions(ctx context.Context, req *subscriptionsv1.ListPendingSubscriptionsRequest) (*subscriptionsv1.ListPendingSubscriptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingSubscriptions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending subscriptions", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	exportArticlesStream, err := s.articleQueryClient.ExportArticles(ctx, &contentv1.ExportArticlesRequest{
		Context: req.GetContext(),
		Filter: &contentv1.ExportArticlesRequest_Filter{
			Query:   req.GetFilter().GetQuery(),
			OwnerId: req.GetFilter().GetOwnerId(),
			Labels:  labels,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.content.v1.ArticleQueryService/ExportArticles")
	}
	for {
		received, err := exportArticlesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.content.v1.ArticleQueryService/ExportArticles")
		}
		_ = received
	}
	return &subscriptionsv1.ListPendingSubscriptionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateSubscriptionApproval delegates a subscription.
func (s *SubscriptionApprovalServer) DelegateSubscriptionApproval(ctx context.Context, req *subscriptionsv1.DelegateSubscriptionApprovalRequest) (*subscriptionsv1.DelegateSubscriptionApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "delegate subscription approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	for _, limit := range req.GetSubscription().GetLimits() {
		if _, err := s.householdValidationClient.PreviewHousehold(ctx, &customersv1.PreviewHouseholdRequest{
			Context: req.GetContext(),
			Household: &customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Children: []*customersv1.Household{&customersv1.Household{Name: limit.GetName()}},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/PreviewHousehold")
		}
	}
	browseMetricSeriesesRequest := buildBrowseMetricSeriesesRequestForDelegateSubscriptionApproval(req)
	browseMetricSeriesesReply, err := s.metricSeriesQueryClient.BrowseMetricSerieses(ctx, browseMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses")
	}
	return &subscriptionsv1.DelegateSubscriptionApprovalResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: browseMetricSeriesesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Subscription: &subscriptionsv1.Subscription{
			SubscriptionId: req.GetSubscriptionId(),
			Labels:         req.GetSubscription().GetLabels(),
		},
	}, nil
}

// RecallSubscription recalls a subscription.
func (s *SubscriptionApprovalServer) RecallSubscription(ctx context.Context, req *subscriptionsv1.RecallSubscriptionRequest) (*subscriptionsv1.RecallSubscriptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "recall subscription", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetSubscription()).(*subscriptionsv1.Subscription)
	encoded, err := protojson.Marshal(req.GetSubscription())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded subscriptionsv1.Subscription
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	calculatePreferenceRequest := &customersv1.CalculatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:       req.GetSubscription().GetHolder().GetEmail(),
				DisplayName: req.GetSubscription().GetHolder().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cloned.GetAudit().GetUpdatedBy(),
				CreatedBy: (&decoded).GetAudit().GetCreatedBy(),
			},
		},
	}
	calculatePreferenceReply, err := s.preferenceCalculationPort.CalculatePreference(ctx, calculatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/CalculatePreference")
	}
	ctx = tenancy.With(ctx, "email", req.GetSubscription().GetContact().GetEmail())
	encoded2, err := protojson.Marshal(req.GetSubscription())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 subscriptionsv1.Subscription
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetSubscription())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	simulateUsageRecordReply, err := s.usageRecordCalculationClient.SimulateUsageRecord(ctx, &subscriptionsv1.SimulateUsageRecordRequest{
		Context: req.GetContext(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{
				Email:       tenancy.Value(ctx, "email"),
				DisplayName: (&decoded2).GetHolder().GetDisplayName(),
			},
			Detail: packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordCalculationService/SimulateUsageRecord")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	generateSellerPayoutScheduleReportRequest := &sellersv1.GenerateSellerPayoutScheduleReportRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      calculatePreferenceReply.GetPreference().GetBlock().GetReason(),
	}
	generateSellerPayoutScheduleReportReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerPayoutScheduleReportingServiceClient).GenerateSellerPayoutScheduleReport(ctx, generateSellerPayoutScheduleReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/GenerateSellerPayoutScheduleReport")
	}
	return &subscriptionsv1.RecallSubscriptionResponse{
		Subscription: &subscriptionsv1.Subscription{
			Limits: []*subscriptionsv1.SubscriptionLimit{&subscriptionsv1.SubscriptionLimit{
				Amount: calculatePreferenceReply.GetPreference().GetDiscount().GetAmount(),
			}},
			SubscriptionId: req.GetSubscriptionId(),
			Contact:        req.GetSubscription().GetContact(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: simulateUsageRecordReply.GetUsageRecord().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &subscriptionsv1.RecallSubscriptionResponse_Outcome{
			Message: generateSellerPayoutScheduleReportReply.GetResult().GetMessage(),
		},
	}, nil
}

// CommentSubscriptionApproval comments a subscription.
func (s *SubscriptionApprovalServer) CommentSubscriptionApproval(ctx context.Context, req *subscriptionsv1.CommentSubscriptionApprovalRequest) (*subscriptionsv1.CommentSubscriptionApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentSubscriptionApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "comment subscription approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	searchCustomersRequest := &customersv1.SearchCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SearchCustomersRequest_Filter{
			Labels:       req.GetSubscription().GetLabels(),
			CreatedAfter: req.GetSubscription().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
		},
	}
	searchCustomersReply, err := s.customerLazy().SearchCustomers(ctx, searchCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/SearchCustomers")
	}
	ctx = tenancy.With(ctx, "customer_id", req.GetSubscription().GetHolder().GetCustomerId())
	countAuditEventsRequest := &auditv1.CountAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.CountAuditEventsRequest_Filter{
			Labels:  req.GetSubscription().GetLabels(),
			OwnerId: tenancy.Value(ctx, "customer_id"),
		},
	}
	countAuditEventsReply := new(auditv1.CountAuditEventsResponse)
	if err := s.conn.Invoke(ctx, auditAuditEventQueryServiceCountAuditEventsMethod, countAuditEventsRequest, countAuditEventsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/CountAuditEvents")
	}
	return &subscriptionsv1.CommentSubscriptionApprovalResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: searchCustomersReply.GetResult().GetCorrelationId(),
			Status:        searchCustomersReply.GetResult().GetStatus(),
		},
		Subscription: &subscriptionsv1.Subscription{
			SubscriptionId: req.GetSubscriptionId(),
			Balance:        req.GetSubscription().GetBalance(),
		},
	}, nil
}

// customerLazy dials acme.customers.v1.CustomerService the first time a handler needs it.
func (s *SubscriptionApprovalServer) customerLazy() customersv1.CustomerServiceClient {
	s.customerOnce.Do(func() { s.customerCached = customersv1.NewCustomerServiceClient(s.conn) })
	return s.customerCached
}
