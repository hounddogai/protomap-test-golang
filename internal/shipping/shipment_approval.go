package shipping

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featurestorev1 "github.com/hounddogai/protomap-test-golang/gen/acme/featurestore/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// ShipmentApprovalServer implements `acme.shipping.v1.ShipmentApprovalService`.
// Routes shipments through approval, with escalation and delegation.
type ShipmentApprovalServer struct {
	*shipmentApprovalHandlers
}

// shipmentApprovalHandlers holds the handlers of ShipmentApprovalServer, with what they use.
type shipmentApprovalHandlers struct {
	shippingv1.UnimplementedShipmentApprovalServiceServer
	store                             *Store
	log                               *slog.Logger
	subscriptionPlanWorkflowClient    subscriptionsv1.SubscriptionPlanWorkflowServiceClient
	consentRecordValidationClient     customersv1.ConsentRecordValidationServiceClient
	customerValidationClient          customersv1.CustomerValidationServiceClient
	consentRecordCalculationClient    customersv1.ConsentRecordCalculationServiceClient
	shipmentValidationClient          shippingv1.ShipmentValidationServiceClient
	featureViewQueryClient            featurestorev1.FeatureViewQueryServiceClient
	synonymExportClient               searchv1.SynonymExportServiceClient
	customerCalculationClient         customersv1.CustomerCalculationServiceClient
	parcelQueryClient                 shippingv1.ParcelQueryServiceClient
	explainConsentRecordDecisionFn    func(context.Context, *customersv1.ExplainConsentRecordDecisionRequest, ...grpc.CallOption) (*customersv1.ExplainConsentRecordDecisionResponse, error)
	preferenceCalculationClient       customersv1.PreferenceCalculationServiceClient
	householdValidationClient         customersv1.HouseholdValidationServiceClient
	householdValidationClientFallback customersv1.HouseholdValidationServiceClient
	scheduleManifestReportFn          func(context.Context, *shippingv1.ScheduleManifestReportRequest, ...grpc.CallOption) (*shippingv1.ScheduleManifestReportResponse, error)
	manifestSyncClient                shippingv1.ManifestSyncServiceClient
	reviewModerationApprovalClient    reviewsv1.ReviewModerationApprovalServiceClient
	auditEventQueryClient             auditv1.AuditEventQueryServiceClient
}

// NewShipmentApprovalServer wires a ShipmentApprovalServer.
func NewShipmentApprovalServer(deps platform.Deps) *ShipmentApprovalServer {
	consentRecordValidation := customersv1.NewConsentRecordValidationServiceClient(deps.Conn)
	manifestReporting := shippingv1.NewManifestReportingServiceClient(deps.Conn)
	return &ShipmentApprovalServer{shipmentApprovalHandlers: &shipmentApprovalHandlers{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.shipping.v1.ShipmentApprovalService"),
		subscriptionPlanWorkflowClient:    subscriptionsv1.NewSubscriptionPlanWorkflowServiceClient(deps.Conn),
		consentRecordValidationClient:     customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		customerValidationClient:          customersv1.NewCustomerValidationServiceClient(deps.Conn),
		consentRecordCalculationClient:    customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		shipmentValidationClient:          shippingv1.NewShipmentValidationServiceClient(deps.Conn),
		featureViewQueryClient:            featurestorev1.NewFeatureViewQueryServiceClient(deps.Conn),
		synonymExportClient:               searchv1.NewSynonymExportServiceClient(deps.Conn),
		customerCalculationClient:         customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		parcelQueryClient:                 shippingv1.NewParcelQueryServiceClient(deps.Conn),
		explainConsentRecordDecisionFn:    consentRecordValidation.ExplainConsentRecordDecision,
		preferenceCalculationClient:       customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		householdValidationClient:         customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		householdValidationClientFallback: customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		scheduleManifestReportFn:          manifestReporting.ScheduleManifestReport,
		manifestSyncClient:                shippingv1.NewManifestSyncServiceClient(deps.Conn),
		reviewModerationApprovalClient:    reviewsv1.NewReviewModerationApprovalServiceClient(deps.Conn),
		auditEventQueryClient:             auditv1.NewAuditEventQueryServiceClient(deps.Conn),
	}}
}

// SubmitShipment submits a shipment.
func (s *shipmentApprovalHandlers) SubmitShipment(ctx context.Context, req *shippingv1.SubmitShipmentRequest) (*shippingv1.SubmitShipmentResponse, error) {
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "submit shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	explainCustomerDecisionRequest := &customersv1.ExplainCustomerDecisionRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress:      &commonv1.PostalAddress{Lines: req.GetShipment().GetBillingAddress().GetLines()},
			RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	explainCustomerDecisionReply, err := explainCustomerDecisionCall(ctx, explainCustomerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ExplainCustomerDecision")
	}
	pauseSubscriptionPlanWorkflowRequest := &subscriptionsv1.PauseSubscriptionPlanWorkflowRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetShipment().GetAudit().GetCreatedBy()},
		},
		Details: &subscriptionsv1.PauseSubscriptionPlanWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: explainCustomerDecisionReply.GetCustomer().GetContact().GetEmail(),
			},
		},
	}
	var pauseSubscriptionPlanWorkflowReply *subscriptionsv1.PauseSubscriptionPlanWorkflowResponse
	if err := pipeline.Run(ctx, "acme.subscriptions.v1.SubscriptionPlanWorkflowService/PauseSubscriptionPlanWorkflow", func(ctx context.Context) error {
		answer, err := s.subscriptionPlanWorkflowClient.PauseSubscriptionPlanWorkflow(ctx, pauseSubscriptionPlanWorkflowRequest)
		pauseSubscriptionPlanWorkflowReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanWorkflowService/PauseSubscriptionPlanWorkflow")
	}
	return &shippingv1.SubmitShipmentResponse{
		Outcome: &shippingv1.SubmitShipmentResponse_Outcome{
			EffectiveAt: explainCustomerDecisionReply.GetOutcome().GetEffectiveAt(),
		},
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: pauseSubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetAudit().GetCreatedBy(),
				Etag:      pauseSubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetAudit().GetEtag(),
			},
			ShipmentId:   req.GetShipmentId(),
			Counterparty: req.GetShipment().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveShipment approves a shipment.
func (s *shipmentApprovalHandlers) ApproveShipment(ctx context.Context, req *shippingv1.ApproveShipmentRequest) (*shippingv1.ApproveShipmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "approve shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetShipment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewConsentRecordRequest := &customersv1.PreviewConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	send := s.previewConsentRecordSender(previewConsentRecordRequest)
	previewConsentRecordReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord")
	}
	return &shippingv1.ApproveShipmentResponse{
		Outcome: &shippingv1.ApproveShipmentResponse_Outcome{Status: previewConsentRecordReply.GetOutcome().GetStatus()},
		Shipment: &shippingv1.Shipment{
			ShipmentId: req.GetShipmentId(),
			Total:      req.GetShipment().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectShipment rejects a shipment.
func (s *shipmentApprovalHandlers) RejectShipment(ctx context.Context, req *shippingv1.RejectShipmentRequest) (*shippingv1.RejectShipmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "reject shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	scoreCustomerReply, err := s.customerValidationClient.ScoreCustomer(ctx, &customersv1.ScoreCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{Etag: req.GetShipment().GetAudit().GetEtag()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ScoreCustomer")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*customersv1.QuoteConsentRecordResponse, len(req.GetShipment().GetLines()))
	for i, line := range req.GetShipment().GetLines() {
		group.Go(func() error {
			reply, err := s.consentRecordCalculationClient.QuoteConsentRecord(groupCtx, &customersv1.QuoteConsentRecordRequest{
				Context:       req.GetContext(),
				ConsentRecord: &customersv1.ConsentRecord{Attributes: line.GetAttributes()},
				Details: &customersv1.QuoteConsentRecordRequest_Details{
					Destination: &commonv1.PostalAddress{
						Lines:     req.GetShipment().GetBillingAddress().GetLines(),
						Recipient: scoreCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	cloned := proto.Clone(req.GetShipment()).(*shippingv1.Shipment)
	lintShipmentsReply, err := s.shipmentValidationClient.LintShipments(ctx, &shippingv1.LintShipmentsRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{ParcelId: req.GetShipment().GetCounterparty().GetParcelId()},
			BillingAddress: &commonv1.PostalAddress{
				Recipient:  cloned.GetBillingAddress().GetRecipient(),
				PostalCode: req.GetShipment().GetBillingAddress().GetPostalCode(),
				Region:     req.GetShipment().GetBillingAddress().GetRegion(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/LintShipments")
	}
	return &shippingv1.RejectShipmentResponse{
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: scoreCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: scoreCustomerReply.GetCustomer().GetAudit().GetCreatedAt(),
			},
			ShipmentId: req.GetShipmentId(),
		},
		Result: &commonv1.OperationResult{
			Details: lintShipmentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &shippingv1.RejectShipmentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// EscalateShipment escalates a shipment.
func (s *shipmentApprovalHandlers) EscalateShipment(ctx context.Context, req *shippingv1.EscalateShipmentRequest) (*shippingv1.EscalateShipmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "escalate shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeFeatureViewsRequest := &featurestorev1.SummarizeFeatureViewsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &featurestorev1.SummarizeFeatureViewsRequest_Details{Filters: req.GetShipment().GetMetadata()},
	}
	summarizeFeatureViewsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*featurestorev1.SummarizeFeatureViewsResponse, error) {
		return s.featureViewQueryClient.SummarizeFeatureViews(ctx, summarizeFeatureViewsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featurestore.v1.FeatureViewQueryService/SummarizeFeatureViews")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*searchv1.ScheduleSynonymExportResponse, len(req.GetShipment().GetLines()))
	for i, line := range req.GetShipment().GetLines() {
		group.Go(func() error {
			reply, err := s.synonymExportClient.ScheduleSynonymExport(groupCtx, &searchv1.ScheduleSynonymExportRequest{
				Context: req.GetContext(),
				Details: &searchv1.ScheduleSynonymExportRequest_Details{Filters: line.GetAttributes()},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ScheduleSynonymExport")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	ctx = tenancy.With(ctx, "display_name", req.GetShipment().GetCounterparty().GetDisplayName())
	quoteCustomerRequest := &customersv1.QuoteCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit:               &commonv1.AuditInfo{UpdatedAt: req.GetShipment().GetAudit().GetUpdatedAt()},
			RelationshipManager: &identityv1.UserRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
		Details: &customersv1.QuoteCustomerRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetShipment().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	quoteCustomerReply, err := grpcx.Call(ctx, s.customerCalculationClient.QuoteCustomer, quoteCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/QuoteCustomer")
	}
	lookupParcelRequest := buildLookupParcelRequestForEscalateShipment(req)
	lookupParcelReply, err := s.parcelQueryClient.LookupParcel(ctx, lookupParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelQueryService/LookupParcel")
	}
	return &shippingv1.EscalateShipmentResponse{
		Result: &commonv1.OperationResult{
			Message:      summarizeFeatureViewsReply.GetResult().GetMessage(),
			Details:      quoteCustomerReply.GetResult().GetDetails(),
			ReferenceIds: lookupParcelReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &shippingv1.EscalateShipmentResponse_Outcome{
			Message: summarizeFeatureViewsReply.GetResult().GetMessage(),
			Status:  lookupParcelReply.GetResult().GetStatus(),
		},
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: quoteCustomerReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
			},
			ShipmentId: req.GetShipmentId(),
			Status:     req.GetShipment().GetStatus(),
		},
	}, nil
}

// ListPendingShipments lists shipments a page at a time.
func (s *shipmentApprovalHandlers) ListPendingShipments(ctx context.Context, req *shippingv1.ListPendingShipmentsRequest) (*shippingv1.ListPendingShipmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending shipments", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainConsentRecordDecisionRequest := &customersv1.ExplainConsentRecordDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainConsentRecordDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
	}
	explainConsentRecordDecisionReply, err := s.explainConsentRecordDecisionFn(ctx, explainConsentRecordDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
	}
	comparePreferenceOptionsRequest := newComparePreferenceOptionsRequest(withComparePreferenceOptionsContext(req.GetContext()), withComparePreferenceOptionsPreference(&customersv1.Preference{
		Owner: &identityv1.UserRef{
			DisplayName: explainConsentRecordDecisionReply.GetConsentRecord().GetSubject().GetDisplayName(),
		},
	}), withComparePreferenceOptionsDetails(&customersv1.ComparePreferenceOptionsRequest_Details{
		Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
	}))
	comparePreferenceOptionsReply, err := s.preferenceCalculationClient.ComparePreferenceOptions(ctx, comparePreferenceOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions")
	}
	return &shippingv1.ListPendingShipmentsResponse{
		Shipments: []*shippingv1.Shipment{&shippingv1.Shipment{
			Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{
				Attributes: explainConsentRecordDecisionReply.GetConsentRecord().GetAttributes(),
			}},
			Total: comparePreferenceOptionsReply.GetBreakdown().GetTotal(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateShipmentApproval delegates a shipment.
func (s *shipmentApprovalHandlers) DelegateShipmentApproval(ctx context.Context, req *shippingv1.DelegateShipmentApprovalRequest) (*shippingv1.DelegateShipmentApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DelegateShipmentApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "delegate shipment approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *customersv1.ValidateHouseholdRequest, ...grpc.CallOption) (*customersv1.ValidateHouseholdResponse, error){
		"primary":  s.householdValidationClient.ValidateHousehold,
		"fallback": s.householdValidationClientFallback.ValidateHousehold,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloned := proto.Clone(req.GetDetails()).(*shippingv1.DelegateShipmentApprovalRequest_Details)
	source := req.GetShipment().GetBillingAddress().ProtoReflect()
	postalCode := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("postal_code"))).String()
	validateHouseholdRequest := &customersv1.ValidateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager:  &identityv1.UserRef{DisplayName: cloned.GetApprover().GetDisplayName()},
					Location: &commonv1.PostalAddress{PostalCode: postalCode},
				}},
				Location: &commonv1.PostalAddress{Region: req.GetShipment().GetBillingAddress().GetRegion()},
			}},
		},
	}
	validateHouseholdReply, err := route(ctx, validateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ValidateHousehold")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	scheduleManifestReportRequest := &shippingv1.ScheduleManifestReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details:     &shippingv1.ScheduleManifestReportRequest_Details{Filters: validateHouseholdReply.GetResult().GetDetails()},
	}
	scheduleManifestReportReply, err := s.scheduleManifestReportFn(ctx, scheduleManifestReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestReportingService/ScheduleManifestReport")
	}
	return &shippingv1.DelegateShipmentApprovalResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: validateHouseholdReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &shippingv1.DelegateShipmentApprovalResponse_Outcome{
			NextSteps: validateHouseholdReply.GetOutcome().GetNextSteps(),
			Status:    scheduleManifestReportReply.GetResult().GetStatus(),
		},
		Shipment: &shippingv1.Shipment{
			ShipmentId: req.GetShipmentId(),
			IssuedAt:   req.GetShipment().GetIssuedAt(),
		},
	}, nil
}

// RecallShipment recalls a shipment.
func (s *shipmentApprovalHandlers) RecallShipment(ctx context.Context, req *shippingv1.RecallShipmentRequest) (*shippingv1.RecallShipmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "recall shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	renewParcelRequest := &shippingv1.RenewParcelRequest{
		Context:  req.GetContext(),
		ParcelId: req.GetShipment().GetCounterparty().GetParcelId(),
		Details: &shippingv1.RenewParcelRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetShipment().GetCounterparty().GetDisplayName(),
				Email:       req.GetDetails().GetApprover().GetEmail(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
			},
		},
	}
	renewParcelReply, err := renewParcelCall(ctx, renewParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelLifecycleService/RenewParcel")
	}
	ctx = tenancy.With(ctx, "postal_code", req.GetShipment().GetBillingAddress().GetPostalCode())
	syncManifestsRequest := &shippingv1.SyncManifestsRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			BillingAddress: &commonv1.PostalAddress{PostalCode: tenancy.Value(ctx, "postal_code")},
		},
	}
	type syncManifestsResult struct {
		reply *shippingv1.SyncManifestsResponse
		err   error
	}
	syncManifestsResults := make(chan syncManifestsResult, 1)
	go func() {
		reply, err := s.manifestSyncClient.SyncManifests(ctx, syncManifestsRequest)
		syncManifestsResults <- syncManifestsResult{reply, err}
	}()
	var syncManifestsReply *shippingv1.SyncManifestsResponse
	select {
	case result := <-syncManifestsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.shipping.v1.ManifestSyncService/SyncManifests")
		}
		syncManifestsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	escalateReviewModerationRequest := &reviewsv1.EscalateReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			Origin:    &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{Email: renewParcelReply.GetParcel().GetOwner().GetEmail()}},
		},
		Details: &reviewsv1.EscalateReviewModerationRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetShipment().GetCounterparty().GetDisplayName(),
			},
		},
	}
	escalateReviewModerationReply, err := grpcx.Call(ctx, s.reviewModerationApprovalClient.EscalateReviewModeration, escalateReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationApprovalService/EscalateReviewModeration")
	}
	suggestAuditEventsRequest := newSuggestAuditEventsRequest(withSuggestAuditEventsContext(req.GetContext()), withSuggestAuditEventsFilter(&auditv1.SuggestAuditEventsRequest_Filter{
		Region:       req.GetShipment().GetBillingAddress().GetRegion(),
		CreatedAfter: syncManifestsReply.GetManifest().GetAudit().GetUpdatedAt(),
		OwnerId:      renewParcelReply.GetParcel().GetComponents()["primary"].GetComponentId(),
	}))
	suggestAuditEventsReply, err := s.auditEventQueryClient.SuggestAuditEvents(ctx, suggestAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/SuggestAuditEvents")
	}
	return &shippingv1.RecallShipmentResponse{
		Outcome: &shippingv1.RecallShipmentResponse_Outcome{Message: renewParcelReply.GetOutcome().GetMessage()},
		Shipment: &shippingv1.Shipment{
			DueAt:          syncManifestsReply.GetManifest().GetDueAt(),
			Notes:          syncManifestsReply.GetManifest().GetNotes(),
			ShipmentId:     req.GetShipmentId(),
			BillingAddress: req.GetShipment().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{
			Details:       escalateReviewModerationReply.GetResult().GetDetails(),
			CorrelationId: suggestAuditEventsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  suggestAuditEventsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
	}, nil
}

// GetShipmentApprovalStatus returns a shipment.
func (s *shipmentApprovalHandlers) GetShipmentApprovalStatus(ctx context.Context, req *shippingv1.GetShipmentApprovalStatusRequest) (*shippingv1.GetShipmentApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipment", req.GetShipmentId(), "get_shipment_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get shipment approval status", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.GetShipmentApprovalStatusResponse{
		Shipment: &shippingv1.Shipment{ShipmentId: req.GetShipmentId()},
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentShipmentApproval comments a shipment.
func (s *shipmentApprovalHandlers) CommentShipmentApproval(ctx context.Context, req *shippingv1.CommentShipmentApprovalRequest) (*shippingv1.CommentShipmentApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentShipmentApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "comment shipment approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	calculatePreferenceRequest := &customersv1.CalculatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetShipment().GetCounterparty().GetDisplayName(),
			},
		},
	}
	calculatePreferenceReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*customersv1.CalculatePreferenceResponse, error) {
		return s.preferenceCalculationClient.CalculatePreference(ctx, calculatePreferenceRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/CalculatePreference")
	}
	return &shippingv1.CommentShipmentApprovalResponse{
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculatePreferenceReply.GetPreference().GetAudit().GetCreatedAt(),
			},
			ShipmentId: req.GetShipmentId(),
			Metadata:   req.GetShipment().GetMetadata(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: calculatePreferenceReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// previewConsentRecordSender returns a function that sends one request to
// acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord.
func (s *shipmentApprovalHandlers) previewConsentRecordSender(request *customersv1.PreviewConsentRecordRequest) func(context.Context) (*customersv1.PreviewConsentRecordResponse, error) {
	return func(ctx context.Context) (*customersv1.PreviewConsentRecordResponse, error) {
		return s.consentRecordValidationClient.PreviewConsentRecord(ctx, request)
	}
}
