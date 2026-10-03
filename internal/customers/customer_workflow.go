package customers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	forecastingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forecasting/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// CustomerWorkflowServer implements `acme.customers.v1.CustomerWorkflowService`.
// Runs the long-lived workflow that takes a customer from start to finish.
type CustomerWorkflowServer struct {
	customersv1.UnimplementedCustomerWorkflowServiceServer
	store                          *Store
	log                            *slog.Logger
	roleNotificationClient         identityv1.RoleNotificationServiceClient
	customerSegmentReportingClient customersv1.CustomerSegmentReportingServiceClient
	collectionWorkflowClient       merchandisingv1.CollectionWorkflowServiceClient
	demandForecastClient           forecastingv1.DemandForecastServiceClient
	customerLifecycleClient        customersv1.CustomerLifecycleServiceClient
	paymentWorkflowClient          paymentsv1.PaymentWorkflowServiceClient
	preferenceSyncClient           customersv1.PreferenceSyncServiceClient
	conn                           *grpc.ClientConn
	householdApprovalClient        customersv1.HouseholdApprovalServiceClient
	tenantNotificationClient       identityv1.TenantNotificationServiceClient
	consentRecordLifecycleClient   customersv1.ConsentRecordLifecycleServiceClient
	preferenceSyncOnce             sync.Once
	preferenceSyncCached           customersv1.PreferenceSyncServiceClient
	countCurrencyRateTotalsFn      func(context.Context, *pricingv1.CountCurrencyRateTotalsRequest, ...grpc.CallOption) (*pricingv1.CountCurrencyRateTotalsResponse, error)
}

// NewCustomerWorkflowServer wires a CustomerWorkflowServer.
func NewCustomerWorkflowServer(deps platform.Deps) *CustomerWorkflowServer {
	currencyRateReporting := pricingv1.NewCurrencyRateReportingServiceClient(deps.Conn)
	return &CustomerWorkflowServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.customers.v1.CustomerWorkflowService"),
		roleNotificationClient:         identityv1.NewRoleNotificationServiceClient(deps.Conn),
		customerSegmentReportingClient: customersv1.NewCustomerSegmentReportingServiceClient(deps.Conn),
		collectionWorkflowClient:       merchandisingv1.NewCollectionWorkflowServiceClient(deps.Conn),
		demandForecastClient:           forecastingv1.NewDemandForecastServiceClient(deps.Conn),
		customerLifecycleClient:        customersv1.NewCustomerLifecycleServiceClient(deps.Conn),
		paymentWorkflowClient:          paymentsv1.NewPaymentWorkflowServiceClient(deps.Conn),
		preferenceSyncClient:           customersv1.NewPreferenceSyncServiceClient(deps.Conn),
		conn:                           deps.Conn,
		householdApprovalClient:        customersv1.NewHouseholdApprovalServiceClient(deps.Conn),
		tenantNotificationClient:       identityv1.NewTenantNotificationServiceClient(deps.Conn),
		consentRecordLifecycleClient:   customersv1.NewConsentRecordLifecycleServiceClient(deps.Conn),
		countCurrencyRateTotalsFn:      currencyRateReporting.CountCurrencyRateTotals,
	}
}

// StartCustomerWorkflow starts a customer.
func (s *CustomerWorkflowServer) StartCustomerWorkflow(ctx context.Context, req *customersv1.StartCustomerWorkflowRequest) (*customersv1.StartCustomerWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "start customer workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	unsubscribeRoleUpdatesRequest := buildUnsubscribeRoleUpdatesRequestForStartCustomerWorkflow(req)
	unsubscribeRoleUpdatesReply, err := s.roleNotificationClient.UnsubscribeRoleUpdates(ctx, unsubscribeRoleUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/UnsubscribeRoleUpdates")
	}
	return &customersv1.StartCustomerWorkflowResponse{
		Outcome: &customersv1.StartCustomerWorkflowResponse_Outcome{
			Message: unsubscribeRoleUpdatesReply.GetOutcome().GetMessage(),
		},
		Customer: &customersv1.Customer{
			CustomerId: req.GetCustomerId(),
			Attributes: req.GetCustomer().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvanceCustomerWorkflow advances a customer.
func (s *CustomerWorkflowServer) AdvanceCustomerWorkflow(ctx context.Context, req *customersv1.AdvanceCustomerWorkflowRequest) (*customersv1.AdvanceCustomerWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "advance customer workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	listCustomerSegmentReportsRequest := &customersv1.ListCustomerSegmentReportsRequest{
		Context: req.GetContext(),
		Filter:  &customersv1.ListCustomerSegmentReportsRequest_Filter{Region: req.GetCustomer().GetPrimaryAddress().GetRegion()},
	}
	send := s.listCustomerSegmentReportsSender(listCustomerSegmentReportsRequest)
	listCustomerSegmentReportsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentReportingService/ListCustomerSegmentReports")
	}
	return &customersv1.AdvanceCustomerWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listCustomerSegmentReportsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Customer: &customersv1.Customer{
			CustomerId:     req.GetCustomerId(),
			PrimaryAddress: req.GetCustomer().GetPrimaryAddress(),
		},
	}, nil
}

// RetryCustomerWorkflow retrys a customer.
func (s *CustomerWorkflowServer) RetryCustomerWorkflow(ctx context.Context, req *customersv1.RetryCustomerWorkflowRequest) (*customersv1.RetryCustomerWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "retry customer workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	signalCollectionWorkflowRequest := &merchandisingv1.SignalCollectionWorkflowRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Manager: &identityv1.UserRef{
						DisplayName: req.GetCustomer().GetRelationshipManager().GetDisplayName(),
					},
					Location: &commonv1.PostalAddress{
						PostalCode: req.GetCustomer().GetPrimaryAddress().GetPostalCode(),
					},
				}},
			}},
		},
	}
	signalCollectionWorkflowReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*merchandisingv1.SignalCollectionWorkflowResponse, error) {
		return s.collectionWorkflowClient.SignalCollectionWorkflow(ctx, signalCollectionWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/SignalCollectionWorkflow")
	}
	searchDemandForecastsRequest := &forecastingv1.SearchDemandForecastsRequest{}
	searchDemandForecastsRequest.Context = req.GetContext()
	searchDemandForecastsRequest.Filter = &forecastingv1.SearchDemandForecastsRequest_Filter{
		Region:       req.GetCustomer().GetPrimaryAddress().GetRegion(),
		CreatedAfter: signalCollectionWorkflowReply.GetCollection().GetAudit().GetUpdatedAt(),
		Labels:       signalCollectionWorkflowReply.GetResult().GetDetails(),
		OwnerId:      signalCollectionWorkflowReply.GetOutcome().GetReferenceId(),
	}
	searchDemandForecastsReply, err := s.demandForecastClient.SearchDemandForecasts(ctx, searchDemandForecastsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forecasting.v1.DemandForecastService/SearchDemandForecasts")
	}
	step := &customerWorkflowServerRetryCustomerWorkflowScheduleCustomerExpiryStep{request: &customersv1.ScheduleCustomerExpiryRequest{
		Context: req.GetContext(),
		Details: &customersv1.ScheduleCustomerExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetCustomer().GetContact().GetPhone()},
		},
		Customer: &customersv1.Customer{
			Preferences: &customersv1.CustomerPreferences{Channels: req.GetCustomer().GetPreferences().GetChannels()},
			Audit:       &commonv1.AuditInfo{CreatedAt: req.GetCustomer().GetAudit().GetCreatedAt()},
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: signalCollectionWorkflowReply.GetCollection().GetLocation().GetPostalCode(),
			},
		},
	}}
	if err := s.runRetryCustomerWorkflowScheduleCustomerExpiry(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerLifecycleService/ScheduleCustomerExpiry")
	}
	scheduleCustomerExpiryReply := step.reply
	listPaymentWorkflowsRequest := &paymentsv1.ListPaymentWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.ListPaymentWorkflowsRequest_Filter{
			Region:       req.GetCustomer().GetPrimaryAddress().GetRegion(),
			CreatedAfter: scheduleCustomerExpiryReply.GetCustomer().GetPassport().GetExpiresAt(),
		},
	}
	listPaymentWorkflowsReply, err := grpcx.Call(ctx, s.paymentWorkflowClient.ListPaymentWorkflows, listPaymentWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentWorkflowService/ListPaymentWorkflows")
	}
	return &customersv1.RetryCustomerWorkflowResponse{
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				UserId: signalCollectionWorkflowReply.GetCollection().GetManager().GetUserId(),
			},
			Name:       scheduleCustomerExpiryReply.GetCustomer().GetContact().GetName(),
			CustomerId: req.GetCustomerId(),
			Contact:    &commonv1.ContactInfo{Name: req.GetCustomer().GetName()},
		},
		Result: &commonv1.OperationResult{
			CorrelationId: searchDemandForecastsReply.GetResult().GetCorrelationId(),
			Status:        searchDemandForecastsReply.GetResult().GetStatus(),
			Details:       listPaymentWorkflowsReply.GetResult().GetDetails(),
		},
	}, nil
}

// PauseCustomerWorkflow pauses a customer.
func (s *CustomerWorkflowServer) PauseCustomerWorkflow(ctx context.Context, req *customersv1.PauseCustomerWorkflowRequest) (*customersv1.PauseCustomerWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "pause customer workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetCustomer())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullPreferencesRequest := &customersv1.PullPreferencesRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetCustomer().GetRelationshipManager().GetDisplayName(),
				Email:       req.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetCustomer().GetAudit().GetCreatedBy(),
				UpdatedBy: req.GetCustomer().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &customersv1.PullPreferencesRequest_Details{Payload: snapshot},
	}
	pullPreferencesReply, err := grpcx.Call(ctx, s.preferenceSyncClient.PullPreferences, pullPreferencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceSyncService/PullPreferences")
	}
	listApiCredentialVersionsRequest := &identityv1.ListApiCredentialVersionsRequest{
		Context: req.GetContext(),
		Filter:  &identityv1.ListApiCredentialVersionsRequest_Filter{Region: req.GetCustomer().GetPrimaryAddress().GetRegion()},
	}
	listApiCredentialVersionsReply, err := grpcx.NewClient(s.conn, identityv1.NewApiCredentialHistoryServiceClient).ListApiCredentialVersions(ctx, listApiCredentialVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/ListApiCredentialVersions")
	}
	createDocumentRequest := &documentsv1.CreateDocumentRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			Lines: []*documentsv1.DocumentLine{&documentsv1.DocumentLine{
				Tax: &documentsv1.DocumentLineTax{
					Amount: pullPreferencesReply.GetPreference().GetDiscount().GetAmount(),
				},
			}},
		},
	}
	createDocumentReply := new(documentsv1.CreateDocumentResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", documentsDocumentServiceName, "CreateDocument"), createDocumentRequest, createDocumentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/CreateDocument")
	}
	cloned := proto.Clone(req.GetDetails()).(*customersv1.PauseCustomerWorkflowRequest_Details)
	recallHouseholdReply, err := s.householdApprovalClient.RecallHousehold(ctx, &customersv1.RecallHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						Email:  cloned.GetNotify().GetEmail(),
						UserId: pullPreferencesReply.GetPreference().GetOwner().GetUserId(),
					},
					Location: &commonv1.PostalAddress{
						CountryCode: createDocumentReply.GetDocument().GetBillingAddress().GetCountryCode(),
					},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdApprovalService/RecallHousehold")
	}
	_ = recallHouseholdReply
	return &customersv1.PauseCustomerWorkflowResponse{
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: pullPreferencesReply.GetPreference().GetAudit().GetUpdatedBy(),
			},
			CustomerId: req.GetCustomerId(),
			Status:     req.GetCustomer().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message:       listApiCredentialVersionsReply.GetResult().GetMessage(),
			CorrelationId: createDocumentReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &customersv1.PauseCustomerWorkflowResponse_Outcome{Message: createDocumentReply.GetResult().GetMessage()},
	}, nil
}

// ResumeCustomerWorkflow resumes a customer.
func (s *CustomerWorkflowServer) ResumeCustomerWorkflow(ctx context.Context, req *customersv1.ResumeCustomerWorkflowRequest) (*customersv1.ResumeCustomerWorkflowResponse, error) {
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "resume customer workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotify().GetPhone())
	packed, err := anypb.New(req.GetCustomer())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendTenantTestNoticeRequest := &identityv1.SendTenantTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendTenantTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            tenancy.Value(ctx, "phone"),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Name:             req.GetCustomer().GetContact().GetName(),
			},
			Payload: packed,
		},
	}
	type sendTenantTestNoticeResult struct {
		reply *identityv1.SendTenantTestNoticeResponse
		err   error
	}
	sendTenantTestNoticeResults := make(chan sendTenantTestNoticeResult, 1)
	go func() {
		reply, err := s.tenantNotificationClient.SendTenantTestNotice(ctx, sendTenantTestNoticeRequest)
		sendTenantTestNoticeResults <- sendTenantTestNoticeResult{reply, err}
	}()
	var sendTenantTestNoticeReply *identityv1.SendTenantTestNoticeResponse
	select {
	case result := <-sendTenantTestNoticeResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.TenantNotificationService/SendTenantTestNotice")
		}
		sendTenantTestNoticeReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	pauseCollectionWorkflowReply, err := s.collectionWorkflowClient.PauseCollectionWorkflow(ctx, &merchandisingv1.PauseCollectionWorkflowRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Audit: &commonv1.AuditInfo{
						CreatedAt: sendTenantTestNoticeReply.GetTenant().GetAudit().GetCreatedAt(),
					},
					Location: &commonv1.PostalAddress{
						Region: sendTenantTestNoticeReply.GetTenant().GetLocation().GetRegion(),
					},
				}},
				Manager: &identityv1.UserRef{
					UserId: sendTenantTestNoticeReply.GetTenant().GetManager().GetUserId(),
				},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/PauseCollectionWorkflow")
	}
	return &customersv1.ResumeCustomerWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: sendTenantTestNoticeReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				CreatedBy: pauseCollectionWorkflowReply.GetCollection().GetAudit().GetCreatedBy(),
				UpdatedBy: pauseCollectionWorkflowReply.GetCollection().GetAudit().GetUpdatedBy(),
			},
			CustomerId: req.GetCustomerId(),
			Attributes: req.GetCustomer().GetAttributes(),
		},
	}, nil
}

// ListCustomerWorkflows lists customers a page at a time.
func (s *CustomerWorkflowServer) ListCustomerWorkflows(ctx context.Context, req *customersv1.ListCustomerWorkflowsRequest) (*customersv1.ListCustomerWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListCustomerWorkflows", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list customer workflows", "tenant", req.GetContext().GetTenantId())
	pauseCollectionWorkflowRequest := &merchandisingv1.PauseCollectionWorkflowRequest{}
	pauseCollectionWorkflowRequest.Context = req.GetContext()
	pauseCollectionWorkflowRequest.Collection = &merchandisingv1.Collection{
		Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			}},
		}},
	}
	pauseCollectionWorkflowReply, err := s.collectionWorkflowClient.PauseCollectionWorkflow(ctx, pauseCollectionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/PauseCollectionWorkflow")
	}
	return &customersv1.ListCustomerWorkflowsResponse{
		Customers: []*customersv1.Customer{&customersv1.Customer{
			Attributes: pauseCollectionWorkflowReply.GetCollection().GetAttributes(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SignalCustomerWorkflow signals a customer.
func (s *CustomerWorkflowServer) SignalCustomerWorkflow(ctx context.Context, req *customersv1.SignalCustomerWorkflowRequest) (*customersv1.SignalCustomerWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SignalCustomerWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "signal customer workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*customersv1.SignalCustomerWorkflowRequest_Details)
	packed, err := anypb.New(req.GetCustomer())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scheduleConsentRecordExpiryRequest := &customersv1.ScheduleConsentRecordExpiryRequest{
		Context: req.GetContext(),
		Details: &customersv1.ScheduleConsentRecordExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:             req.GetCustomer().GetContact().GetName(),
				Email:            cloned.GetNotify().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
	}
	type scheduleConsentRecordExpiryResult struct {
		reply *customersv1.ScheduleConsentRecordExpiryResponse
		err   error
	}
	scheduleConsentRecordExpiryResults := make(chan scheduleConsentRecordExpiryResult, 1)
	go func() {
		reply, err := s.consentRecordLifecycleClient.ScheduleConsentRecordExpiry(ctx, scheduleConsentRecordExpiryRequest)
		scheduleConsentRecordExpiryResults <- scheduleConsentRecordExpiryResult{reply, err}
	}()
	var scheduleConsentRecordExpiryReply *customersv1.ScheduleConsentRecordExpiryResponse
	select {
	case result := <-scheduleConsentRecordExpiryResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.ConsentRecordLifecycleService/ScheduleConsentRecordExpiry")
		}
		scheduleConsentRecordExpiryReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	ctx = tenancy.With(ctx, "created_by", req.GetCustomer().GetAudit().GetCreatedBy())
	snapshot, err := protojson.Marshal(req.GetCustomer())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	syncPreferencesRequest := &customersv1.SyncPreferencesRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:  req.GetCustomer().GetContact().GetEmail(),
				UserId: req.GetCustomer().GetRelationshipManager().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: tenancy.Value(ctx, "created_by")},
		},
		Details: &customersv1.SyncPreferencesRequest_Details{Payload: snapshot},
	}
	syncPreferencesReply, err := s.preferenceSyncLazy().SyncPreferences(ctx, syncPreferencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceSyncService/SyncPreferences")
	}
	streamPreferenceChangesStream, err := s.preferenceSyncClient.StreamPreferenceChanges(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceSyncService/StreamPreferenceChanges")
	}
	if err := streamPreferenceChangesStream.Send(&customersv1.StreamPreferenceChangesRequest{
		Context: req.GetContext(),
		Frame: &customersv1.StreamPreferenceChangesRequest_Change{Change: &customersv1.Preference{
			Action: &customersv1.Preference_Block{Block: &customersv1.PreferenceBlockAction{
				Reason: syncPreferencesReply.GetPreference().GetBlock().GetReason(),
			}},
			Owner: &identityv1.UserRef{
				DisplayName: scheduleConsentRecordExpiryReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceSyncService/StreamPreferenceChanges")
	}
	if err := streamPreferenceChangesStream.CloseSend(); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceSyncService/StreamPreferenceChanges")
	}
	acknowledged, err := streamPreferenceChangesStream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceSyncService/StreamPreferenceChanges")
	}
	_ = acknowledged
	ctx = tenancy.With(ctx, "region", req.GetCustomer().GetPrimaryAddress().GetRegion())
	countCurrencyRateTotalsRequest := &pricingv1.CountCurrencyRateTotalsRequest{
		Context: req.GetContext(),
		Filter:  &pricingv1.CountCurrencyRateTotalsRequest_Filter{Region: tenancy.Value(ctx, "region")},
	}
	countCurrencyRateTotalsReply, err := s.countCurrencyRateTotalsFn(ctx, countCurrencyRateTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateReportingService/CountCurrencyRateTotals")
	}
	return &customersv1.SignalCustomerWorkflowResponse{
		Customer: &customersv1.Customer{
			Attributes: scheduleConsentRecordExpiryReply.GetConsentRecord().GetAttributes(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: syncPreferencesReply.GetPreference().GetAudit().GetUpdatedAt(),
			},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: syncPreferencesReply.GetPreference().GetOwner().GetDisplayName(),
			},
			CustomerId: req.GetCustomerId(),
			Name:       req.GetCustomer().GetName(),
		},
		Outcome: &customersv1.SignalCustomerWorkflowResponse_Outcome{
			NextSteps: scheduleConsentRecordExpiryReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			Message:      countCurrencyRateTotalsReply.GetResult().GetMessage(),
			ReferenceIds: countCurrencyRateTotalsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// listCustomerSegmentReportsSender returns a function that sends one request to
// acme.customers.v1.CustomerSegmentReportingService/ListCustomerSegmentReports.
func (s *CustomerWorkflowServer) listCustomerSegmentReportsSender(request *customersv1.ListCustomerSegmentReportsRequest) func(context.Context) (*customersv1.ListCustomerSegmentReportsResponse, error) {
	return func(ctx context.Context) (*customersv1.ListCustomerSegmentReportsResponse, error) {
		return s.customerSegmentReportingClient.ListCustomerSegmentReports(ctx, request)
	}
}

// customerWorkflowServerRetryCustomerWorkflowScheduleCustomerExpiryStep carries the request of one step of
// RetryCustomerWorkflow and, once it ran, its reply.
type customerWorkflowServerRetryCustomerWorkflowScheduleCustomerExpiryStep struct {
	request *customersv1.ScheduleCustomerExpiryRequest
	reply   *customersv1.ScheduleCustomerExpiryResponse
}

func (s *CustomerWorkflowServer) runRetryCustomerWorkflowScheduleCustomerExpiry(ctx context.Context, step *customerWorkflowServerRetryCustomerWorkflowScheduleCustomerExpiryStep) error {
	reply, err := s.customerLifecycleClient.ScheduleCustomerExpiry(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// preferenceSyncLazy dials acme.customers.v1.PreferenceSyncService the first time a handler needs it.
func (s *CustomerWorkflowServer) preferenceSyncLazy() customersv1.PreferenceSyncServiceClient {
	s.preferenceSyncOnce.Do(func() { s.preferenceSyncCached = customersv1.NewPreferenceSyncServiceClient(s.conn) })
	return s.preferenceSyncCached
}
