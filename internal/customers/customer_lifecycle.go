package customers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// CustomerLifecycleServer implements `acme.customers.v1.CustomerLifecycleService`.
// Moves a customer between the states of its lifecycle.
type CustomerLifecycleServer struct {
	customersv1.UnimplementedCustomerLifecycleServiceServer
	store                          *Store
	log                            *slog.Logger
	bus                            *eventbus.Bus
	preferenceValidationClient     customersv1.PreferenceValidationServiceClient
	householdValidationClient      customersv1.HouseholdValidationServiceClient
	conn                           *grpc.ClientConn
	customerValidationOnce         sync.Once
	customerValidationCached       customersv1.CustomerValidationServiceClient
	meteredShipmentApproval        meteredShipmentApproval
	meteredPickListApproval        meteredPickListApproval
	tenantExportClient             identityv1.TenantExportServiceClient
	preferenceCalculationClient    customersv1.PreferenceCalculationServiceClient
	tenantAdminClient              identityv1.TenantAdminServiceClient
	reviewModerationWorkflowClient reviewsv1.ReviewModerationWorkflowServiceClient
	featureFlagClient              featureflagsv1.FeatureFlagServiceClient
}

// NewCustomerLifecycleServer wires a CustomerLifecycleServer.
func NewCustomerLifecycleServer(deps platform.Deps) *CustomerLifecycleServer {
	return &CustomerLifecycleServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.customers.v1.CustomerLifecycleService"),
		bus:                            deps.Bus,
		preferenceValidationClient:     customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		householdValidationClient:      customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		conn:                           deps.Conn,
		meteredShipmentApproval:        meteredShipmentApproval{ShipmentApprovalServiceClient: shippingv1.NewShipmentApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		meteredPickListApproval:        meteredPickListApproval{PickListApprovalServiceClient: warehousingv1.NewPickListApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		tenantExportClient:             identityv1.NewTenantExportServiceClient(deps.Conn),
		preferenceCalculationClient:    customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		tenantAdminClient:              identityv1.NewTenantAdminServiceClient(deps.Conn),
		reviewModerationWorkflowClient: reviewsv1.NewReviewModerationWorkflowServiceClient(deps.Conn),
		featureFlagClient:              featureflagsv1.NewFeatureFlagServiceClient(deps.Conn),
	}
}

// ActivateCustomer activates a customer.
func (s *CustomerLifecycleServer) ActivateCustomer(ctx context.Context, req *customersv1.ActivateCustomerRequest) (*customersv1.ActivateCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ActivateCustomer", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer", req.GetCustomerId(), "activate_customer"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate customer", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "customers.activate_customer.get_customer_rules",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"customer_id":       req.GetCustomerId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	step := &customerLifecycleServerActivateCustomerNormalizePreferenceStep{request: &customersv1.NormalizePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}}
	if err := s.runActivateCustomerNormalizePreference(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/NormalizePreference")
	}
	normalizePreferenceReply := step.reply
	return &customersv1.ActivateCustomerResponse{
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				CreatedBy: normalizePreferenceReply.GetPreference().GetAudit().GetCreatedBy(),
			},
			RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
		Outcome: &customersv1.ActivateCustomerResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SuspendCustomer suspends a customer.
func (s *CustomerLifecycleServer) SuspendCustomer(ctx context.Context, req *customersv1.SuspendCustomerRequest) (*customersv1.SuspendCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer", req.GetCustomerId(), "suspend_customer"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend customer", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.SuspendCustomerRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	checkHouseholdEligibilityRequest := &customersv1.CheckHouseholdEligibilityRequest{}
	checkHouseholdEligibilityRequest.Context = req.GetContext()
	checkHouseholdEligibilityRequest.Household = &customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{Email: (&decoded).GetNotifyContact().GetEmail()},
			}},
		}},
	}
	checkHouseholdEligibilityReply, err := s.householdValidationClient.CheckHouseholdEligibility(ctx, checkHouseholdEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/CheckHouseholdEligibility")
	}
	checkCustomerEligibilityRequest := &customersv1.CheckCustomerEligibilityRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: checkHouseholdEligibilityReply.GetHousehold().GetAudit().GetUpdatedAt(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			PrimaryAddress: &commonv1.PostalAddress{
				Region: checkHouseholdEligibilityReply.GetHousehold().GetLocation().GetRegion(),
			},
		},
	}
	checkCustomerEligibilityReply, err := s.customerValidationLazy().CheckCustomerEligibility(ctx, checkCustomerEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/CheckCustomerEligibility")
	}
	approveShipmentRequest := &shippingv1.ApproveShipmentRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ApproveShipmentRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: checkHouseholdEligibilityReply.GetHousehold().GetAudit().GetUpdatedAt(),
			},
		},
	}
	approveShipmentReply, err := s.meteredShipmentApproval.ApproveShipment(ctx, approveShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentApprovalService/ApproveShipment")
	}
	return &customersv1.SuspendCustomerResponse{
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				Email:       checkHouseholdEligibilityReply.GetHousehold().GetManager().GetEmail(),
				DisplayName: approveShipmentReply.GetShipment().GetCounterparty().GetDisplayName(),
			},
			CustomerId: checkCustomerEligibilityReply.GetCustomer().GetCustomerId(),
			Contact:    &commonv1.ContactInfo{Phone: req.GetDetails().GetNotifyContact().GetPhone()},
		},
		Result: &commonv1.OperationResult{
			Details: checkCustomerEligibilityReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &customersv1.SuspendCustomerResponse_Outcome{
			Message:     approveShipmentReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// ReinstateCustomer reinstates a customer.
func (s *CustomerLifecycleServer) ReinstateCustomer(ctx context.Context, req *customersv1.ReinstateCustomerRequest) (*customersv1.ReinstateCustomerResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer", req.GetCustomerId(), "reinstate_customer"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate customer", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.ReinstateCustomerRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	submitPickListRequest := &warehousingv1.SubmitPickListRequest{
		Context: req.GetContext(),
		Details: &warehousingv1.SubmitPickListRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: (&decoded).GetNewOwner().GetDisplayName(),
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
			},
		},
		PickList: &warehousingv1.PickList{
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
	submitPickListReply, err := s.meteredPickListApproval.SubmitPickList(ctx, submitPickListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListApprovalService/SubmitPickList")
	}
	cancelTenantExportReply, err := s.tenantExportClient.CancelTenantExport(ctx, &identityv1.CancelTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.CancelTenantExportRequest_Details{
			NewOwner: &identityv1.UserRef{
				Email:       req.GetDetails().GetNewOwner().GetEmail(),
				DisplayName: submitPickListReply.GetPickList().GetCounterparty().GetDisplayName(),
			},
			NotifyContact: &commonv1.ContactInfo{
				Email: submitPickListReply.GetPickList().GetCounterparty().GetEmail(),
			},
			EffectiveAt: submitPickListReply.GetOutcome().GetEffectiveAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/CancelTenantExport")
	}
	return &customersv1.ReinstateCustomerResponse{
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Lines:       submitPickListReply.GetPickList().GetBillingAddress().GetLines(),
				CountryCode: cancelTenantExportReply.GetTenant().GetLocation().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cancelTenantExportReply.GetTenant().GetAudit().GetUpdatedAt(),
			},
			CustomerId: req.GetCustomerId(),
		},
		Result: &commonv1.OperationResult{
			Message: submitPickListReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &customersv1.ReinstateCustomerResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// TerminateCustomer terminates a customer.
func (s *CustomerLifecycleServer) TerminateCustomer(ctx context.Context, req *customersv1.TerminateCustomerRequest) (*customersv1.TerminateCustomerResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer", req.GetCustomerId(), "terminate_customer"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate customer", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	reconcileConsentRecordsRequest := &customersv1.ReconcileConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Detail: packed,
		},
		Annotations: annotations,
		Reason:      req.GetReason(),
	}
	reconcileConsentRecordsReply, err := grpcx.NewClient(s.conn, customersv1.NewConsentRecordSyncServiceClient).ReconcileConsentRecords(ctx, reconcileConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/ReconcileConsentRecords")
	}
	searchUsersRequest := &identityv1.SearchUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.SearchUsersRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			Labels:       reconcileConsentRecordsReply.GetResult().GetDetails(),
		},
	}
	searchUsersReply := new(identityv1.SearchUsersResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", identityUserServiceName, "SearchUsers"), searchUsersRequest, searchUsersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/SearchUsers")
	}
	return &customersv1.TerminateCustomerResponse{
		Result: &commonv1.OperationResult{
			Message: reconcileConsentRecordsReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Customer: &customersv1.Customer{
			Attributes: reconcileConsentRecordsReply.GetConsentRecord().GetAttributes(),
			CustomerId: req.GetCustomerId(),
		},
		Outcome: &customersv1.TerminateCustomerResponse_Outcome{
			Status:      searchUsersReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// ExpireCustomer expires a customer.
func (s *CustomerLifecycleServer) ExpireCustomer(ctx context.Context, req *customersv1.ExpireCustomerRequest) (*customersv1.ExpireCustomerResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer", req.GetCustomerId(), "expire_customer"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire customer", "tenant", req.GetContext().GetTenantId())
	quotePreferenceRequest := buildQuotePreferenceRequestForExpireCustomer(req)
	quotePreferenceReply, err := s.preferenceCalculationClient.QuotePreference(ctx, quotePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/QuotePreference")
	}
	archiveTenantRequest := buildArchiveTenantRequestForExpireCustomer(req, quotePreferenceReply)
	archiveTenantReply, err := s.tenantAdminClient.ArchiveTenant(ctx, archiveTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/ArchiveTenant")
	}
	return &customersv1.ExpireCustomerResponse{
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: quotePreferenceReply.GetPreference().GetOwner().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{
				Email: quotePreferenceReply.GetPreference().GetOwner().GetEmail(),
			},
			CustomerId: req.GetCustomerId(),
			Name:       req.GetDetails().GetNotifyContact().GetName(),
		},
		Outcome: &customersv1.ExpireCustomerResponse_Outcome{
			EffectiveAt: archiveTenantReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleCustomerExpiry schedules a customer.
func (s *CustomerLifecycleServer) ScheduleCustomerExpiry(ctx context.Context, req *customersv1.ScheduleCustomerExpiryRequest) (*customersv1.ScheduleCustomerExpiryResponse, error) {
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "schedule customer expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	pauseReviewModerationWorkflowReply, err := s.reviewModerationWorkflowClient.PauseReviewModerationWorkflow(ctx, &reviewsv1.PauseReviewModerationWorkflowRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.PauseReviewModerationWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationWorkflowService/PauseReviewModerationWorkflow")
	}
	retryCustomerSegmentImportRequest := &customersv1.RetryCustomerSegmentImportRequest{
		Context: req.GetContext(),
		Details: &customersv1.RetryCustomerSegmentImportRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name: pauseReviewModerationWorkflowReply.GetReviewModeration().GetRequester().GetName(),
			},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetCustomer().GetRelationshipManager().GetDisplayName(),
				Email:       req.GetCustomer().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetCustomer().GetAudit().GetUpdatedAt()},
		},
	}
	retryCustomerSegmentImportReply := new(customersv1.RetryCustomerSegmentImportResponse)
	if err := s.conn.Invoke(ctx, customersCustomerSegmentImportServiceRetryCustomerSegmentImportMethod, retryCustomerSegmentImportRequest, retryCustomerSegmentImportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/RetryCustomerSegmentImport")
	}
	batchGetFeatureFlagsRequest := &featureflagsv1.BatchGetFeatureFlagsRequest{Context: req.GetContext()}
	batchGetFeatureFlagsReply, err := grpcx.Call(ctx, s.featureFlagClient.BatchGetFeatureFlags, batchGetFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags")
	}
	return &customersv1.ScheduleCustomerExpiryResponse{
		Outcome: &customersv1.ScheduleCustomerExpiryResponse_Outcome{
			Status: pauseReviewModerationWorkflowReply.GetOutcome().GetStatus(),
		},
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				Email: retryCustomerSegmentImportReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
			CustomerId: req.GetCustomerId(),
			Contact:    &commonv1.ContactInfo{Name: req.GetCustomer().GetName()},
		},
		Result: &commonv1.OperationResult{
			Details: retryCustomerSegmentImportReply.GetResult().GetDetails(),
			Message: batchGetFeatureFlagsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// customerLifecycleServerActivateCustomerNormalizePreferenceStep carries the request of one step of ActivateCustomer
// and, once it ran, its reply.
type customerLifecycleServerActivateCustomerNormalizePreferenceStep struct {
	request *customersv1.NormalizePreferenceRequest
	reply   *customersv1.NormalizePreferenceResponse
}

func (s *CustomerLifecycleServer) runActivateCustomerNormalizePreference(ctx context.Context, step *customerLifecycleServerActivateCustomerNormalizePreferenceStep) error {
	reply, err := s.preferenceValidationClient.NormalizePreference(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// customerValidationLazy dials acme.customers.v1.CustomerValidationService the first time a handler needs it.
func (s *CustomerLifecycleServer) customerValidationLazy() customersv1.CustomerValidationServiceClient {
	s.customerValidationOnce.Do(func() { s.customerValidationCached = customersv1.NewCustomerValidationServiceClient(s.conn) })
	return s.customerValidationCached
}
