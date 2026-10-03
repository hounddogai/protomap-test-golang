package warehousing

import (
	"context"
	"fmt"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	labelingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/labeling/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// PickListValidationServer implements `acme.warehousing.v1.PickListValidationService`.
// Checks pick lists against the rules before anything acts on them.
type PickListValidationServer struct {
	warehousingv1.UnimplementedPickListValidationServiceServer
	store                         *Store
	log                           *slog.Logger
	venueClient                   venuesv1.VenueServiceClient
	tierCalculationClient         loyaltyv1.TierCalculationServiceClient
	conn                          *grpc.ClientConn
	notificationQueryClient       notificationsv1.NotificationQueryServiceClient
	labelingTaskCalculationClient labelingv1.LabelingTaskCalculationServiceClient
	tenantExportClient            identityv1.TenantExportServiceClient
	metricSeriesClient            metricsv1.MetricSeriesServiceClient
	clients                       *registry.Registry
	parcelValidationClient        shippingv1.ParcelValidationServiceClient
	summarizeNotificationsFn      func(context.Context, *notificationsv1.SummarizeNotificationsRequest, ...grpc.CallOption) (*notificationsv1.SummarizeNotificationsResponse, error)
}

// NewPickListValidationServer wires a PickListValidationServer.
func NewPickListValidationServer(deps platform.Deps) *PickListValidationServer {
	notificationQuery := notificationsv1.NewNotificationQueryServiceClient(deps.Conn)
	return &PickListValidationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.warehousing.v1.PickListValidationService"),
		venueClient:                   venuesv1.NewVenueServiceClient(deps.Conn),
		tierCalculationClient:         loyaltyv1.NewTierCalculationServiceClient(deps.Conn),
		conn:                          deps.Conn,
		notificationQueryClient:       notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
		labelingTaskCalculationClient: labelingv1.NewLabelingTaskCalculationServiceClient(deps.Conn),
		tenantExportClient:            identityv1.NewTenantExportServiceClient(deps.Conn),
		metricSeriesClient:            metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		clients:                       deps.Clients,
		parcelValidationClient:        shippingv1.NewParcelValidationServiceClient(deps.Conn),
		summarizeNotificationsFn:      notificationQuery.SummarizeNotifications,
	}
}

// ValidatePickList validates a pick list.
func (s *PickListValidationServer) ValidatePickList(ctx context.Context, req *warehousingv1.ValidatePickListRequest) (*warehousingv1.ValidatePickListResponse, error) {
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "validate pick list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	updateVenueReply, err := s.venueClient.UpdateVenue(ctx, &venuesv1.UpdateVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{Email: req.GetPickList().GetCounterparty().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/UpdateVenue")
	}
	return &warehousingv1.ValidatePickListResponse{
		PickList: &warehousingv1.PickList{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: updateVenueReply.GetVenue().GetAddress().GetPostalCode(),
			},
			PickListId: req.GetPickListId(),
			Metadata:   req.GetPickList().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewPickList previews a pick list.
func (s *PickListValidationServer) PreviewPickList(ctx context.Context, req *warehousingv1.PreviewPickListRequest) (*warehousingv1.PreviewPickListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewPickList", "error", err)
		return nil, err
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "preview pick list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	step := &pickListValidationServerPreviewPickListSimulateTierStep{request: &loyaltyv1.SimulateTierRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetPickList().GetAudit().GetCreatedAt()},
		},
		Details: &loyaltyv1.SimulateTierRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:    req.GetPickList().GetBillingAddress().GetLines(),
				Locality: req.GetPickList().GetBillingAddress().GetLocality(),
			},
		},
	}}
	if err := s.runPreviewPickListSimulateTier(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierCalculationService/SimulateTier")
	}
	simulateTierReply := step.reply
	return &warehousingv1.PreviewPickListResponse{
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{
				CreatedAt: simulateTierReply.GetTier().GetAudit().GetCreatedAt(),
			},
			PickListId: req.GetPickListId(),
			Metadata:   req.GetPickList().GetMetadata(),
		},
		Outcome: &warehousingv1.PreviewPickListResponse_Outcome{Message: simulateTierReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckPickListEligibility checks a pick list.
func (s *PickListValidationServer) CheckPickListEligibility(ctx context.Context, req *warehousingv1.CheckPickListEligibilityRequest) (*warehousingv1.CheckPickListEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckPickListEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "check pick list eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	cloneVenueRequest := buildCloneVenueRequestForCheckPickListEligibility(req)
	cloneVenueReply, err := s.venueClient.CloneVenue(ctx, cloneVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/CloneVenue")
	}
	lintSynonymsRequest := &searchv1.LintSynonymsRequest{
		Context: req.GetContext(),
		Details: &searchv1.LintSynonymsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cloneVenueReply.GetVenue().GetOwner().GetEmail()},
		},
	}
	lintSynonymsReply := new(searchv1.LintSynonymsResponse)
	if err := grpcx.Invoke(ctx, s.conn, searchv1.SynonymValidationService_LintSynonyms_FullMethodName, lintSynonymsRequest, lintSynonymsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/LintSynonyms")
	}
	return &warehousingv1.CheckPickListEligibilityResponse{
		Outcome: &warehousingv1.CheckPickListEligibilityResponse_Outcome{
			Status:      cloneVenueReply.GetResult().GetStatus(),
			EffectiveAt: lintSynonymsReply.GetOutcome().GetEffectiveAt(),
		},
		PickList: &warehousingv1.PickList{
			PickListId: req.GetPickListId(),
			Total:      req.GetPickList().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizePickList normalizes a pick list.
func (s *PickListValidationServer) NormalizePickList(ctx context.Context, req *warehousingv1.NormalizePickListRequest) (*warehousingv1.NormalizePickListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizePickList", "error", err)
		return nil, err
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "normalize pick list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	findNotificationsRequest := buildFindNotificationsRequestForNormalizePickList(req)
	findNotificationsReply, err := s.notificationQueryClient.FindNotifications(ctx, findNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/FindNotifications")
	}
	var bic string
	switch choice := req.GetPickList().GetSettlement().(type) {
	case *warehousingv1.PickList_Card:
		bic = choice.Card.GetHolderName()
	case *warehousingv1.PickList_Bank:
		bic = choice.Bank.GetBic()
	case *warehousingv1.PickList_VoucherCode:
		bic = choice.VoucherCode
	}
	createSubscriptionRequest := &subscriptionsv1.CreateSubscriptionRequest{
		Context: req.GetContext(),
		Subscription: &subscriptionsv1.Subscription{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPickList().GetAudit().GetUpdatedAt()},
			Holder: &customersv1.CustomerRef{
				DisplayName: req.GetPickList().GetCounterparty().GetDisplayName(),
			},
			Funding: &subscriptionsv1.Subscription_Bank{Bank: &subscriptionsv1.SubscriptionBankFunding{Bic: bic}},
		},
	}
	createSubscriptionReply := new(subscriptionsv1.CreateSubscriptionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", subscriptionsSubscriptionServiceName, "CreateSubscription"), createSubscriptionRequest, createSubscriptionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionService/CreateSubscription")
	}
	calculateLabelingTaskRequest := &labelingv1.CalculateLabelingTaskRequest{
		Context: req.GetContext(),
		LabelingTask: &labelingv1.LabelingTask{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: createSubscriptionReply.GetSubscription().GetContact().GetPreferredChannel(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: createSubscriptionReply.GetSubscription().GetAudit().GetCreatedBy(),
			},
		},
	}
	calculateLabelingTaskReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*labelingv1.CalculateLabelingTaskResponse, error) {
		return s.labelingTaskCalculationClient.CalculateLabelingTask(ctx, calculateLabelingTaskRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.labeling.v1.LabelingTaskCalculationService/CalculateLabelingTask")
	}
	return &warehousingv1.NormalizePickListResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: findNotificationsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &warehousingv1.NormalizePickListResponse_Outcome{Message: findNotificationsReply.GetResult().GetMessage()},
		PickList: &warehousingv1.PickList{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: calculateLabelingTaskReply.GetLabelingTask().GetCustomer().GetDisplayName(),
			},
			PickListId: req.GetPickListId(),
			Total:      req.GetPickList().GetTotal(),
		},
	}, nil
}

// ExplainPickListDecision explains a pick list.
func (s *PickListValidationServer) ExplainPickListDecision(ctx context.Context, req *warehousingv1.ExplainPickListDecisionRequest) (*warehousingv1.ExplainPickListDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "explain pick list decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	source := req.GetPickList().GetCounterparty().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	cloned := proto.Clone(req.GetPickList()).(*warehousingv1.PickList)
	retryTenantExportRequest := &identityv1.RetryTenantExportRequest{
		Context: req.GetContext(),
		Tenant: &identityv1.Tenant{
			Children: []*identityv1.Tenant{&identityv1.Tenant{
				Children: []*identityv1.Tenant{&identityv1.Tenant{
					Manager: &identityv1.UserRef{Email: email},
					Location: &commonv1.PostalAddress{
						Lines:  cloned.GetBillingAddress().GetLines(),
						Region: req.GetPickList().GetBillingAddress().GetRegion(),
					},
					Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPickList().GetAudit().GetUpdatedBy()},
				}},
			}},
		},
	}
	retryTenantExportReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*identityv1.RetryTenantExportResponse, error) {
		return s.tenantExportClient.RetryTenantExport(ctx, retryTenantExportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/RetryTenantExport")
	}
	var holder struct{ CustomerId string }
	if record := req.GetPickList().GetCounterparty(); record != nil {
		if err := mapper.CopyFields(&holder, record, "CustomerId"); err != nil {
			s.log.WarnContext(ctx, "copy customer_id", "error", err)
		}
	}
	listMetricSeriesesRequest := &metricsv1.ListMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.ListMetricSeriesesRequest_Filter{
			Region:       retryTenantExportReply.GetTenant().GetLocation().GetRegion(),
			CreatedAfter: req.GetPickList().GetAudit().GetCreatedAt(),
			OwnerId:      holder.CustomerId,
		},
	}
	var listMetricSeriesesReply *metricsv1.ListMetricSeriesesResponse
	if err := pipeline.Run(ctx, "acme.metrics.v1.MetricSeriesService/ListMetricSerieses", func(ctx context.Context) error {
		answer, err := s.metricSeriesClient.ListMetricSerieses(ctx, listMetricSeriesesRequest)
		listMetricSeriesesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/ListMetricSerieses")
	}
	return &warehousingv1.ExplainPickListDecisionResponse{
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{
				CreatedAt: retryTenantExportReply.GetTenant().GetAudit().GetCreatedAt(),
			},
			PickListId: req.GetPickListId(),
			DueAt:      req.GetPickList().GetDueAt(),
		},
		Result: &commonv1.OperationResult{
			Details: listMetricSeriesesReply.GetResult().GetDetails(),
			Message: listMetricSeriesesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// LintPickLists lints a pick list.
func (s *PickListValidationServer) LintPickLists(ctx context.Context, req *warehousingv1.LintPickListsRequest) (*warehousingv1.LintPickListsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "lint pick lists", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	synonymValidation, err := registry.Lookup[searchv1.SynonymValidationServiceClient](s.clients, "acme.search.v1.SynonymValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "etag", req.GetPickList().GetAudit().GetEtag())
	validateSynonymRequest := &searchv1.ValidateSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{Etag: tenancy.Value(ctx, "etag")},
		},
	}
	validateSynonymReply, err := synonymValidation.ValidateSynonym(ctx, validateSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ValidateSynonym")
	}
	retryTenantExportReply, err := s.tenantExportClient.RetryTenantExport(ctx, &identityv1.RetryTenantExportRequest{
		Context: req.GetContext(),
		Tenant: &identityv1.Tenant{
			Children: []*identityv1.Tenant{&identityv1.Tenant{
				Children: []*identityv1.Tenant{&identityv1.Tenant{
					Audit: &commonv1.AuditInfo{
						CreatedBy: validateSynonymReply.GetSynonym().GetAudit().GetCreatedBy(),
					},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/RetryTenantExport")
	}
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetContact().GetPhone())
	lintParcelsRequest := &shippingv1.LintParcelsRequest{
		Context: req.GetContext(),
		Details: &shippingv1.LintParcelsRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: tenancy.Value(ctx, "phone")},
		},
	}
	var lintParcelsReply *shippingv1.LintParcelsResponse
	if err := pipeline.Run(ctx, "acme.shipping.v1.ParcelValidationService/LintParcels", func(ctx context.Context) error {
		answer, err := s.parcelValidationClient.LintParcels(ctx, lintParcelsRequest)
		lintParcelsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/LintParcels")
	}
	return &warehousingv1.LintPickListsResponse{
		Result: &commonv1.OperationResult{
			Message:       validateSynonymReply.GetOutcome().GetMessage(),
			CorrelationId: lintParcelsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &warehousingv1.LintPickListsResponse_Outcome{
			NextSteps: retryTenantExportReply.GetOutcome().GetNextSteps(),
		},
		PickList: &warehousingv1.PickList{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: retryTenantExportReply.GetTenant().GetLocation().GetPostalCode(),
			},
			PickListId: req.GetPickListId(),
			Number:     req.GetPickList().GetNumber(),
		},
	}, nil
}

// GetPickListRules returns a pick list.
func (s *PickListValidationServer) GetPickListRules(ctx context.Context, req *warehousingv1.GetPickListRulesRequest) (*warehousingv1.GetPickListRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetPickListRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "pick_list", req.GetPickListId(), "get_pick_list_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get pick list rules", "tenant", req.GetContext().GetTenantId())
	summarizeNotificationsRequest := &notificationsv1.SummarizeNotificationsRequest{
		Context:     req.GetContext(),
		Annotations: req.GetContext().GetBaggage(),
	}
	summarizeNotificationsReply, err := s.summarizeNotificationsFn(ctx, summarizeNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/SummarizeNotifications")
	}
	return &warehousingv1.GetPickListRulesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeNotificationsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  summarizeNotificationsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		PickList: &warehousingv1.PickList{PickListId: req.GetPickListId()},
	}, nil
}

// pickListValidationServerPreviewPickListSimulateTierStep carries the request of one step of PreviewPickList and, once
// it ran, its reply.
type pickListValidationServerPreviewPickListSimulateTierStep struct {
	request *loyaltyv1.SimulateTierRequest
	reply   *loyaltyv1.SimulateTierResponse
}

func (s *PickListValidationServer) runPreviewPickListSimulateTier(ctx context.Context, step *pickListValidationServerPreviewPickListSimulateTierStep) error {
	reply, err := s.tierCalculationClient.SimulateTier(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
