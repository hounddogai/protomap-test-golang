package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	analyticsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/analytics/v1"
	audiencesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audiences/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	yardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/yards/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// PreferenceImportServer implements `acme.customers.v1.PreferenceImportService`.
// Takes preferences in bulk from partners and legacy systems.
type PreferenceImportServer struct {
	customersv1.UnimplementedPreferenceImportServiceServer
	store                         *Store
	log                           *slog.Logger
	yardQueryClient               yardsv1.YardQueryServiceClient
	consentRecordValidationClient customersv1.ConsentRecordValidationServiceClient
	householdValidationClient     customersv1.HouseholdValidationServiceClient
	orderReportingClient          ordersv1.OrderReportingServiceClient
	audienceQueryClient           audiencesv1.AudienceQueryServiceClient
	householdValidationPort       householdValidationPort
	bus                           *eventbus.Bus
	cartRuleApprovalClient        cartv1.CartRuleApprovalServiceClient
	clients                       *registry.Registry
	consentRecordSyncClient       customersv1.ConsentRecordSyncServiceClient
	orderAmendmentLifecycleClient ordersv1.OrderAmendmentLifecycleServiceClient
	analyticsEventClient          analyticsv1.AnalyticsEventServiceClient
	quotePreferenceFn             func(context.Context, *customersv1.QuotePreferenceRequest, ...grpc.CallOption) (*customersv1.QuotePreferenceResponse, error)
	conn                          *grpc.ClientConn
	userClient                    identityv1.UserServiceClient
	customerSegmentImportClient   customersv1.CustomerSegmentImportServiceClient
}

// NewPreferenceImportServer wires a PreferenceImportServer.
func NewPreferenceImportServer(deps platform.Deps) *PreferenceImportServer {
	preferenceCalculation := customersv1.NewPreferenceCalculationServiceClient(deps.Conn)
	return &PreferenceImportServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.customers.v1.PreferenceImportService"),
		yardQueryClient:               yardsv1.NewYardQueryServiceClient(deps.Conn),
		consentRecordValidationClient: customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		householdValidationClient:     customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		orderReportingClient:          ordersv1.NewOrderReportingServiceClient(deps.Conn),
		audienceQueryClient:           audiencesv1.NewAudienceQueryServiceClient(deps.Conn),
		householdValidationPort:       newHouseholdValidationPort(deps),
		bus:                           deps.Bus,
		cartRuleApprovalClient:        cartv1.NewCartRuleApprovalServiceClient(deps.Conn),
		clients:                       deps.Clients,
		consentRecordSyncClient:       customersv1.NewConsentRecordSyncServiceClient(deps.Conn),
		orderAmendmentLifecycleClient: ordersv1.NewOrderAmendmentLifecycleServiceClient(deps.Conn),
		analyticsEventClient:          analyticsv1.NewAnalyticsEventServiceClient(deps.Conn),
		quotePreferenceFn:             preferenceCalculation.QuotePreference,
		conn:                          deps.Conn,
		userClient:                    identityv1.NewUserServiceClient(deps.Conn),
		customerSegmentImportClient:   customersv1.NewCustomerSegmentImportServiceClient(deps.Conn),
	}
}

// ImportPreferences takes a stream of preferences and answers once at the end.
func (s *PreferenceImportServer) ImportPreferences(stream grpc.ClientStreamingServer[customersv1.ImportPreferencesRequest, customersv1.ImportPreferencesResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "import preferences", "tenant", msg.GetContext().GetTenantId())
		browseYardsRequest := &yardsv1.BrowseYardsRequest{
			Context: msg.GetContext(),
			Filter:  &yardsv1.BrowseYardsRequest_Filter{CreatedAfter: msg.GetRecord().GetAudit().GetCreatedAt()},
		}
		if _, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*yardsv1.BrowseYardsResponse, error) {
			return s.yardQueryClient.BrowseYards(ctx, browseYardsRequest)
		}); err != nil {
			return grpcx.Wrap(err, "acme.yards.v1.YardQueryService/BrowseYards")
		}
		previewConsentRecordRequest := &customersv1.PreviewConsentRecordRequest{
			Context: msg.GetContext(),
			Details: &customersv1.PreviewConsentRecordRequest_Details{
				Contact: &commonv1.ContactInfo{
					Email:            msg.GetRecord().GetOwner().GetEmail(),
					PreferredChannel: msg.GetHeader().GetSubmittedBy().GetPreferredChannel(),
				},
			},
			ConsentRecord: &customersv1.ConsentRecord{
				Subject: &customersv1.HouseholdRef{DisplayName: msg.GetRecord().GetOwner().GetDisplayName()},
			},
		}
		if _, err := grpcx.Call(ctx, s.consentRecordValidationClient.PreviewConsentRecord, previewConsentRecordRequest); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord")
		}
		previewHouseholdRequest := &customersv1.PreviewHouseholdRequest{}
		previewHouseholdRequest.Context = msg.GetContext()
		previewHouseholdRequest.Household = &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{Email: msg.GetHeader().GetSubmittedBy().GetEmail()},
					Audit: &commonv1.AuditInfo{
						UpdatedBy: msg.GetRecord().GetAudit().GetUpdatedBy(),
						CreatedBy: msg.GetRecord().GetAudit().GetCreatedBy(),
					},
				}},
			}},
		}
		if _, err := s.householdValidationClient.PreviewHousehold(ctx, previewHouseholdRequest); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/PreviewHousehold")
		}
		streamOrderMetricsStream, err := s.orderReportingClient.StreamOrderMetrics(ctx, &ordersv1.StreamOrderMetricsRequest{
			Context: msg.GetContext(),
			Subscriber: &commonv1.ContactInfo{
				PreferredChannel: msg.GetHeader().GetSubmittedBy().GetPreferredChannel(),
			},
		})
		if err != nil {
			return grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/StreamOrderMetrics")
		}
		for {
			received, err := streamOrderMetricsStream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/StreamOrderMetrics")
			}
			_ = received
		}
		accepted++
	}
	return stream.SendAndClose(&customersv1.ImportPreferencesResponse{Accepted: accepted})
}

// GetPreferenceImportStatus returns a preference.
func (s *PreferenceImportServer) GetPreferenceImportStatus(ctx context.Context, req *customersv1.GetPreferenceImportStatusRequest) (*customersv1.GetPreferenceImportStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "get_preference_import_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get preference import status", "tenant", req.GetContext().GetTenantId())
	return &customersv1.GetPreferenceImportStatusResponse{
		Preference: &customersv1.Preference{PreferenceId: req.GetPreferenceId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelPreferenceImport cancels a preference.
func (s *PreferenceImportServer) CancelPreferenceImport(ctx context.Context, req *customersv1.CancelPreferenceImportRequest) (*customersv1.CancelPreferenceImportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "cancel_preference_import"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "cancel preference import", "tenant", req.GetContext().GetTenantId())
	exportAudiencesStream, err := s.audienceQueryClient.ExportAudiences(ctx, &audiencesv1.ExportAudiencesRequest{
		Context: req.GetContext(),
		Filter:  &audiencesv1.ExportAudiencesRequest_Filter{CreatedAfter: req.GetDetails().GetEffectiveAt()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audiences.v1.AudienceQueryService/ExportAudiences")
	}
	for {
		received, err := exportAudiencesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.audiences.v1.AudienceQueryService/ExportAudiences")
		}
		_ = received
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.CancelPreferenceImportRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	previewHouseholdRequest := &customersv1.PreviewHouseholdRequest{
		Context: req.GetContext(),
		Details: &customersv1.PreviewHouseholdRequest_Details{
			Contact: &commonv1.ContactInfo{Email: (&decoded).GetNotifyContact().GetEmail()},
		},
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
			}},
		},
	}
	previewHouseholdReply, err := s.householdValidationPort.PreviewHousehold(ctx, previewHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/PreviewHousehold")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "customers.cancel_preference_import.schedule_order_amendment_expiry",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"order_amendment.counterparty.display_name": previewHouseholdReply.GetHousehold().GetManager().GetDisplayName(),
			"details.notify.email":                      previewHouseholdReply.GetHousehold().GetManager().GetEmail(),
			"order_amendment.billing_address.locality":  previewHouseholdReply.GetHousehold().GetLocation().GetLocality(),
			"context.tenant_id":                         req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	step := &preferenceImportServerCancelPreferenceImportSubmitCartRuleStep{request: &cartv1.SubmitCartRuleRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	}}
	if err := s.runCancelPreferenceImportSubmitCartRule(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleApprovalService/SubmitCartRule")
	}
	submitCartRuleReply := step.reply
	return &customersv1.CancelPreferenceImportResponse{
		Outcome: &customersv1.CancelPreferenceImportResponse_Outcome{
			EffectiveAt: previewHouseholdReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: submitCartRuleReply.GetResult().GetStatus()},
		Preference: &customersv1.Preference{
			Description:  submitCartRuleReply.GetCartRule().GetDescription(),
			PreferenceId: req.GetPreferenceId(),
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}, nil
}

// ValidatePreferenceImport validates a preference.
func (s *PreferenceImportServer) ValidatePreferenceImport(ctx context.Context, req *customersv1.ValidatePreferenceImportRequest) (*customersv1.ValidatePreferenceImportResponse, error) {
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "validate preference import", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	consentRecordCalculation, err := registry.Lookup[customersv1.ConsentRecordCalculationServiceClient](s.clients, "acme.customers.v1.ConsentRecordCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	packed, err := anypb.New(req.GetPreference())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateConsentRecordsRequest := &customersv1.RecalculateConsentRecordsRequest{
		Context: req.GetContext(),
		Reason:  req.GetPreference().GetBlock().GetReason(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetPreference().GetOwner().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	recalculateConsentRecordsReply, err := consentRecordCalculation.RecalculateConsentRecords(ctx, recalculateConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords")
	}
	snapshot, err := protojson.Marshal(req.GetPreference())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveConsentRecordConflictRequest := &customersv1.ResolveConsentRecordConflictRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetPreference().GetOwner().GetDisplayName(),
			},
		},
		Details: &customersv1.ResolveConsentRecordConflictRequest_Details{Payload: snapshot},
	}
	resolveConsentRecordConflictReply, err := customersv1.ConsentRecordSyncServiceClient.ResolveConsentRecordConflict(s.consentRecordSyncClient, ctx, resolveConsentRecordConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/ResolveConsentRecordConflict")
	}
	ctx = tenancy.With(ctx, "updated_by", req.GetPreference().GetAudit().GetUpdatedBy())
	scheduleOrderAmendmentExpiryRequest := &ordersv1.ScheduleOrderAmendmentExpiryRequest{}
	scheduleOrderAmendmentExpiryRequest.Context = req.GetContext()
	scheduleOrderAmendmentExpiryRequest.OrderAmendment = &ordersv1.OrderAmendment{
		Lines: []*ordersv1.OrderAmendmentLine{&ordersv1.OrderAmendmentLine{
			Tax:        &ordersv1.OrderAmendmentLineTax{Amount: req.GetPreference().GetDiscount().GetAmount()},
			Attributes: recalculateConsentRecordsReply.GetConsentRecord().GetAttributes(),
		}},
		Audit: &commonv1.AuditInfo{UpdatedBy: tenancy.Value(ctx, "updated_by")},
	}
	scheduleOrderAmendmentExpiryRequest.Details = &ordersv1.ScheduleOrderAmendmentExpiryRequest_Details{
		Notify: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
	}
	scheduleOrderAmendmentExpiryReply, err := s.orderAmendmentLifecycleClient.ScheduleOrderAmendmentExpiry(ctx, scheduleOrderAmendmentExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentLifecycleService/ScheduleOrderAmendmentExpiry")
	}
	step := &preferenceImportServerValidatePreferenceImportSearchAnalyticsEventsStep{request: &analyticsv1.SearchAnalyticsEventsRequest{
		Context: req.GetContext(),
		Filter: &analyticsv1.SearchAnalyticsEventsRequest_Filter{
			Region:       scheduleOrderAmendmentExpiryReply.GetOrderAmendment().GetBillingAddress().GetRegion(),
			CreatedAfter: scheduleOrderAmendmentExpiryReply.GetOrderAmendment().GetIssuedAt(),
			OwnerId:      scheduleOrderAmendmentExpiryReply.GetOrderAmendment().GetCounterparty().GetBackorderId(),
			Labels:       req.GetPreference().GetParameters(),
		},
	}}
	if err := s.runValidatePreferenceImportSearchAnalyticsEvents(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.analytics.v1.AnalyticsEventService/SearchAnalyticsEvents")
	}
	searchAnalyticsEventsReply := step.reply
	_ = searchAnalyticsEventsReply
	return &customersv1.ValidatePreferenceImportResponse{
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: recalculateConsentRecordsReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: scheduleOrderAmendmentExpiryReply.GetOrderAmendment().GetAudit().GetCreatedAt(),
			},
			PreferenceId: req.GetPreferenceId(),
			Priority:     req.GetPreference().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: recalculateConsentRecordsReply.GetConsentRecord().GetCorrelationId(),
			Status:        scheduleOrderAmendmentExpiryReply.GetOutcome().GetStatus(),
		},
		Outcome: &customersv1.ValidatePreferenceImportResponse_Outcome{
			Message: resolveConsentRecordConflictReply.GetResult().GetMessage(),
		},
	}, nil
}

// ListPreferenceImports lists preferences a page at a time.
func (s *PreferenceImportServer) ListPreferenceImports(ctx context.Context, req *customersv1.ListPreferenceImportsRequest) (*customersv1.ListPreferenceImportsResponse, error) {
	s.log.InfoContext(ctx, "list preference imports", "tenant", req.GetContext().GetTenantId())
	return &customersv1.ListPreferenceImportsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// RetryPreferenceImport retrys a preference.
func (s *PreferenceImportServer) RetryPreferenceImport(ctx context.Context, req *customersv1.RetryPreferenceImportRequest) (*customersv1.RetryPreferenceImportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "retry preference import", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	preferenceCache := cache.New[string, *customersv1.Preference](1)
	preferenceCache.Put(req.GetContext().GetTraceId(), req.GetPreference())
	cachedPreference, _ := preferenceCache.Get(req.GetContext().GetTraceId())
	quotePreferenceRequest := &customersv1.QuotePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{UserId: cachedPreference.GetOwner().GetUserId()},
		},
	}
	quotePreferenceReply, err := s.quotePreferenceFn(ctx, quotePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/QuotePreference")
	}
	validateHouseholdRequest := &customersv1.ValidateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						UserId:      req.GetPreference().GetOwner().GetUserId(),
						DisplayName: quotePreferenceReply.GetPreference().GetOwner().GetDisplayName(),
					},
				}},
			}},
		},
	}
	validateHouseholdReply, err := s.householdValidationPort.ValidateHousehold(ctx, validateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ValidateHousehold")
	}
	checkHouseholdEligibilityRequest := &customersv1.CheckHouseholdEligibilityRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						Etag:      validateHouseholdReply.GetHousehold().GetAudit().GetEtag(),
						CreatedBy: quotePreferenceReply.GetPreference().GetAudit().GetCreatedBy(),
						UpdatedBy: quotePreferenceReply.GetPreference().GetAudit().GetUpdatedBy(),
					},
					Location: &commonv1.PostalAddress{
						Lines: validateHouseholdReply.GetHousehold().GetLocation().GetLines(),
					},
				}},
			}},
		},
	}
	checkHouseholdEligibilityReply, err := grpcx.NewClient(s.conn, customersv1.NewHouseholdValidationServiceClient).CheckHouseholdEligibility(ctx, checkHouseholdEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/CheckHouseholdEligibility")
	}
	ctx = tenancy.With(ctx, "preference_id", req.GetPreference().GetPreferenceId())
	listUsersRequest := &identityv1.ListUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListUsersRequest_Filter{
			Region:       checkHouseholdEligibilityReply.GetHousehold().GetLocation().GetRegion(),
			CreatedAfter: checkHouseholdEligibilityReply.GetHousehold().GetAudit().GetUpdatedAt(),
			OwnerId:      tenancy.Value(ctx, "preference_id"),
		},
	}
	send := s.listUsersSender(listUsersRequest)
	listUsersReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/ListUsers")
	}
	return &customersv1.RetryPreferenceImportResponse{
		Outcome: &customersv1.RetryPreferenceImportResponse_Outcome{
			Message: quotePreferenceReply.GetResult().GetMessage(),
			Status:  listUsersReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message:      validateHouseholdReply.GetOutcome().GetMessage(),
			ReferenceIds: listUsersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				UserId: checkHouseholdEligibilityReply.GetHousehold().GetManager().GetUserId(),
			},
			PreferenceId: req.GetPreferenceId(),
			Effective:    req.GetPreference().GetEffective(),
		},
	}, nil
}

// MapPreferenceImportColumns maps a preference.
func (s *PreferenceImportServer) MapPreferenceImportColumns(ctx context.Context, req *customersv1.MapPreferenceImportColumnsRequest) (*customersv1.MapPreferenceImportColumnsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "map preference import columns", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	listPendingPickListsRequest := &warehousingv1.ListPendingPickListsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.ListPendingPickListsRequest_Filter{
			CreatedAfter: req.GetPreference().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetPreference().GetOwner().GetUserId(),
		},
	}
	listPendingPickListsReply := new(warehousingv1.ListPendingPickListsResponse)
	if err := grpcx.Invoke(ctx, s.conn, warehousingv1.PickListApprovalService_ListPendingPickLists_FullMethodName, listPendingPickListsRequest, listPendingPickListsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListApprovalService/ListPendingPickLists")
	}
	checkPreferenceEligibilityRequest := &customersv1.CheckPreferenceEligibilityRequest{
		Context: req.GetContext(),
		Details: &customersv1.CheckPreferenceEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}
	checkPreferenceEligibilityReply, err := grpcx.NewClient(s.conn, customersv1.NewPreferenceValidationServiceClient).CheckPreferenceEligibility(ctx, checkPreferenceEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/CheckPreferenceEligibility")
	}
	for _, condition := range req.GetPreference().GetConditions() {
		if _, err := s.customerSegmentImportClient.ValidateCustomerSegmentImport(ctx, &customersv1.ValidateCustomerSegmentImportRequest{
			Context: req.GetContext(),
			CustomerSegment: &customersv1.CustomerSegment{
				Conditions: []*customersv1.CustomerSegmentCondition{&customersv1.CustomerSegmentCondition{
					Value:    condition.GetValue(),
					Operator: condition.GetOperator(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/ValidateCustomerSegmentImport")
		}
	}
	return &customersv1.MapPreferenceImportColumnsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listPendingPickListsReply.GetResult().GetCorrelationId(),
			Status:        listPendingPickListsReply.GetResult().GetStatus(),
		},
		Preference: &customersv1.Preference{
			Status:       checkPreferenceEligibilityReply.GetPreference().GetStatus(),
			PreferenceId: req.GetPreferenceId(),
			Parameters:   req.GetPreference().GetParameters(),
		},
	}, nil
}

// preferenceImportServerCancelPreferenceImportSubmitCartRuleStep carries the request of one step of
// CancelPreferenceImport and, once it ran, its reply.
type preferenceImportServerCancelPreferenceImportSubmitCartRuleStep struct {
	request *cartv1.SubmitCartRuleRequest
	reply   *cartv1.SubmitCartRuleResponse
}

func (s *PreferenceImportServer) runCancelPreferenceImportSubmitCartRule(ctx context.Context, step *preferenceImportServerCancelPreferenceImportSubmitCartRuleStep) error {
	reply, err := s.cartRuleApprovalClient.SubmitCartRule(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// preferenceImportServerValidatePreferenceImportSearchAnalyticsEventsStep carries the request of one step of
// ValidatePreferenceImport and, once it ran, its reply.
type preferenceImportServerValidatePreferenceImportSearchAnalyticsEventsStep struct {
	request *analyticsv1.SearchAnalyticsEventsRequest
	reply   *analyticsv1.SearchAnalyticsEventsResponse
}

func (s *PreferenceImportServer) runValidatePreferenceImportSearchAnalyticsEvents(ctx context.Context, step *preferenceImportServerValidatePreferenceImportSearchAnalyticsEventsStep) error {
	reply, err := s.analyticsEventClient.SearchAnalyticsEvents(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// listUsersSender returns a function that sends one request to acme.identity.v1.UserService/ListUsers.
func (s *PreferenceImportServer) listUsersSender(request *identityv1.ListUsersRequest) func(context.Context) (*identityv1.ListUsersResponse, error) {
	return func(ctx context.Context) (*identityv1.ListUsersResponse, error) {
		return s.userClient.ListUsers(ctx, request)
	}
}
