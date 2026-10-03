package shipping

import (
	"context"
	"errors"
	"io"
	"log/slog"

	admissionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/admissions/v1"
	billingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/billing/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ParcelLifecycleServer implements `acme.shipping.v1.ParcelLifecycleService`.
// Moves a parcel between the states of its lifecycle.
type ParcelLifecycleServer struct {
	shippingv1.UnimplementedParcelLifecycleServiceServer
	store                              *Store
	log                                *slog.Logger
	conn                               *grpc.ClientConn
	preferenceCalculationClient        customersv1.PreferenceCalculationServiceClient
	consentRecordValidationClient      customersv1.ConsentRecordValidationServiceClient
	customerSegmentCalculationClient   customersv1.CustomerSegmentCalculationServiceClient
	customerValidationClient           customersv1.CustomerValidationServiceClient
	sellerPayoutScheduleApprovalClient sellersv1.SellerPayoutScheduleApprovalServiceClient
	ledgerAccountClient                ledgerv1.LedgerAccountServiceClient
	clients                            *registry.Registry
	householdCalculationClient         customersv1.HouseholdCalculationServiceClient
	parcelCalculationClient            shippingv1.ParcelCalculationServiceClient
	preferenceValidationPort           preferenceValidationPort
	parcelValidationClient             shippingv1.ParcelValidationServiceClient
	carrierRateReportingClient         shippingv1.CarrierRateReportingServiceClient
	parcelValidationPort               parcelValidationPort
}

// NewParcelLifecycleServer wires a ParcelLifecycleServer.
func NewParcelLifecycleServer(deps platform.Deps) *ParcelLifecycleServer {
	return &ParcelLifecycleServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.shipping.v1.ParcelLifecycleService"),
		conn:                               deps.Conn,
		preferenceCalculationClient:        customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		consentRecordValidationClient:      customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		customerSegmentCalculationClient:   customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		customerValidationClient:           customersv1.NewCustomerValidationServiceClient(deps.Conn),
		sellerPayoutScheduleApprovalClient: sellersv1.NewSellerPayoutScheduleApprovalServiceClient(deps.Conn),
		ledgerAccountClient:                ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		clients:                            deps.Clients,
		householdCalculationClient:         customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		parcelCalculationClient:            shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		preferenceValidationPort:           newPreferenceValidationPort(deps),
		parcelValidationClient:             shippingv1.NewParcelValidationServiceClient(deps.Conn),
		carrierRateReportingClient:         shippingv1.NewCarrierRateReportingServiceClient(deps.Conn),
		parcelValidationPort:               newParcelValidationPort(deps),
	}
}

// ActivateParcel activates a parcel.
func (s *ParcelLifecycleServer) ActivateParcel(ctx context.Context, req *shippingv1.ActivateParcelRequest) (*shippingv1.ActivateParcelResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "activate_parcel"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate parcel", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintTrackingEventsRequest := &shippingv1.LintTrackingEventsRequest{
		Context: req.GetContext(),
		Details: &shippingv1.LintTrackingEventsRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNewOwner().GetEmail(),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
			},
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	lintTrackingEventsReply, err := grpcx.NewClient(s.conn, shippingv1.NewTrackingEventValidationServiceClient).LintTrackingEvents(ctx, lintTrackingEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventValidationService/LintTrackingEvents")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	quotePreferenceRequest := &customersv1.QuotePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:       tenancy.Value(ctx, "email"),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
			},
		},
	}
	type quotePreferenceResult struct {
		reply *customersv1.QuotePreferenceResponse
		err   error
	}
	quotePreferenceResults := make(chan quotePreferenceResult, 1)
	go func() {
		reply, err := s.preferenceCalculationClient.QuotePreference(ctx, quotePreferenceRequest)
		quotePreferenceResults <- quotePreferenceResult{reply, err}
	}()
	var quotePreferenceReply *customersv1.QuotePreferenceResponse
	select {
	case result := <-quotePreferenceResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.PreferenceCalculationService/QuotePreference")
		}
		quotePreferenceReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	previewConsentRecordRequest := &customersv1.PreviewConsentRecordRequest{}
	previewConsentRecordRequest.Context = req.GetContext()
	previewConsentRecordRequest.Details = &customersv1.PreviewConsentRecordRequest_Details{
		Contact: &commonv1.ContactInfo{
			Name:  req.GetDetails().GetNotifyContact().GetName(),
			Phone: req.GetDetails().GetNotifyContact().GetPhone(),
			Email: req.GetDetails().GetNewOwner().GetEmail(),
		},
	}
	previewConsentRecordRequest.ConsentRecord = &customersv1.ConsentRecord{
		Subject: &customersv1.HouseholdRef{
			DisplayName: quotePreferenceReply.GetPreference().GetOwner().GetDisplayName(),
		},
	}
	previewConsentRecordReply, err := s.consentRecordValidationClient.PreviewConsentRecord(ctx, previewConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord")
	}
	return &shippingv1.ActivateParcelResponse{
		Result: &commonv1.OperationResult{
			Message:       lintTrackingEventsReply.GetResult().GetMessage(),
			CorrelationId: previewConsentRecordReply.GetConsentRecord().GetCorrelationId(),
			Status:        "ok",
		},
		Parcel: &shippingv1.Parcel{
			Name:     quotePreferenceReply.GetPreference().GetName(),
			ParcelId: req.GetParcelId(),
		},
		Outcome: &shippingv1.ActivateParcelResponse_Outcome{
			Message:     quotePreferenceReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// SuspendParcel suspends a parcel.
func (s *ParcelLifecycleServer) SuspendParcel(ctx context.Context, req *shippingv1.SuspendParcelRequest) (*shippingv1.SuspendParcelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "suspend_parcel"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend parcel", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*shippingv1.SuspendParcelRequest_Details)
	detailsCache := cache.New[string, *shippingv1.SuspendParcelRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	calculateCustomerSegmentRequest := &customersv1.CalculateCustomerSegmentRequest{}
	calculateCustomerSegmentRequest.Context = req.GetContext()
	calculateCustomerSegmentRequest.CustomerSegment = &customersv1.CustomerSegment{
		Owner: &identityv1.UserRef{
			UserId:      cloned.GetNewOwner().GetUserId(),
			Email:       cachedDetails.GetNewOwner().GetEmail(),
			DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
		},
	}
	calculateCustomerSegmentReply, err := s.customerSegmentCalculationClient.CalculateCustomerSegment(ctx, calculateCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/CalculateCustomerSegment")
	}
	lintCustomersRequest := &customersv1.LintCustomersRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
		Details: &customersv1.LintCustomersRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: calculateCustomerSegmentReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
		},
	}
	type lintCustomersResult struct {
		reply *customersv1.LintCustomersResponse
		err   error
	}
	lintCustomersResults := make(chan lintCustomersResult, 1)
	go func() {
		reply, err := s.customerValidationClient.LintCustomers(ctx, lintCustomersRequest)
		lintCustomersResults <- lintCustomersResult{reply, err}
	}()
	var lintCustomersReply *customersv1.LintCustomersResponse
	select {
	case result := <-lintCustomersResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.CustomerValidationService/LintCustomers")
		}
		lintCustomersReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	cloned2 := proto.Clone(req.GetDetails()).(*shippingv1.SuspendParcelRequest_Details)
	submitSellerPayoutScheduleRequest := newSubmitSellerPayoutScheduleRequest(withSubmitSellerPayoutScheduleContext(req.GetContext()), withSubmitSellerPayoutScheduleSellerPayoutSchedule(&sellersv1.SellerPayoutSchedule{
		Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Email:            cloned2.GetNotifyContact().GetEmail(),
			},
		}},
	}))
	submitSellerPayoutScheduleReply, err := s.sellerPayoutScheduleApprovalClient.SubmitSellerPayoutSchedule(ctx, submitSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleApprovalService/SubmitSellerPayoutSchedule")
	}
	lookupParcelRequest := &shippingv1.LookupParcelRequest{
		Context:  req.GetContext(),
		ParcelId: req.GetParcelId(),
	}
	lookupParcelReply, err := grpcx.NewClient(s.conn, shippingv1.NewParcelQueryServiceClient).LookupParcel(ctx, lookupParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelQueryService/LookupParcel")
	}
	return &shippingv1.SuspendParcelResponse{
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetUpdatedBy(),
				UpdatedAt: submitSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetUpdatedAt(),
				CreatedAt: lookupParcelReply.GetParcel().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				Email: lintCustomersReply.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			ParcelId: req.GetParcelId(),
		},
		Outcome: &shippingv1.SuspendParcelResponse_Outcome{
			Status:      calculateCustomerSegmentReply.GetResult().GetStatus(),
			Message:     submitSellerPayoutScheduleReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: lookupParcelReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ReinstateParcel reinstates a parcel.
func (s *ParcelLifecycleServer) ReinstateParcel(ctx context.Context, req *shippingv1.ReinstateParcelRequest) (*shippingv1.ReinstateParcelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "reinstate_parcel"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate parcel", "tenant", req.GetContext().GetTenantId())
	cloneLedgerAccountRequest := &ledgerv1.CloneLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Holder: &ledgerv1.PostingBatchRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	cloneLedgerAccountReply, err := ledgerv1.LedgerAccountServiceClient.CloneLedgerAccount(s.ledgerAccountClient, ctx, cloneLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CloneLedgerAccount")
	}
	admissionQuery, ok := s.clients.Get("acme.admissions.v1.AdmissionQueryService").(admissionsv1.AdmissionQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.admissions.v1.AdmissionQueryService")
	}
	resolveAdmissionsRequest := &admissionsv1.ResolveAdmissionsRequest{Context: req.GetContext()}
	resolveAdmissionsReply, err := admissionQuery.ResolveAdmissions(ctx, resolveAdmissionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.admissions.v1.AdmissionQueryService/ResolveAdmissions")
	}
	shareCarrierRateReportRequest := &shippingv1.ShareCarrierRateReportRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ShareCarrierRateReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:             cloneLedgerAccountReply.GetLedgerAccount().GetContact().GetName(),
				PreferredChannel: cloneLedgerAccountReply.GetLedgerAccount().GetContact().GetPreferredChannel(),
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
			},
		},
	}
	shareCarrierRateReportReply := new(shippingv1.ShareCarrierRateReportResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.CarrierRateReportingService_ShareCarrierRateReport_FullMethodName, shareCarrierRateReportRequest, shareCarrierRateReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateReportingService/ShareCarrierRateReport")
	}
	quoteHouseholdRequest := &customersv1.QuoteHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						UserId: req.GetDetails().GetNewOwner().GetUserId(),
						Email:  req.GetDetails().GetNotifyContact().GetEmail(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedBy: shareCarrierRateReportReply.GetCarrierRate().GetAudit().GetCreatedBy(),
					},
				}},
			}},
		},
	}
	send := s.quoteHouseholdSender(quoteHouseholdRequest)
	quoteHouseholdReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/QuoteHousehold")
	}
	_ = quoteHouseholdReply
	return &shippingv1.ReinstateParcelResponse{
		Outcome: &shippingv1.ReinstateParcelResponse_Outcome{
			Message:     cloneLedgerAccountReply.GetResult().GetMessage(),
			Status:      resolveAdmissionsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				Email: cloneLedgerAccountReply.GetLedgerAccount().GetContact().GetEmail(),
			},
			Components: map[string]*shippingv1.ParcelComponent{"primary": &shippingv1.ParcelComponent{
				Description: shareCarrierRateReportReply.GetCarrierRate().GetDescription(),
			}},
			ParcelId: req.GetParcelId(),
		},
		Result: &commonv1.OperationResult{
			Details: resolveAdmissionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// TerminateParcel terminates a parcel.
func (s *ParcelLifecycleServer) TerminateParcel(ctx context.Context, req *shippingv1.TerminateParcelRequest) (*shippingv1.TerminateParcelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected TerminateParcel", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "terminate_parcel"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate parcel", "tenant", req.GetContext().GetTenantId())
	comparePreferenceOptionsRequest := &customersv1.ComparePreferenceOptionsRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetNewOwner().GetEmail(),
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
			},
			Action: &customersv1.Preference_Block{Block: &customersv1.PreferenceBlockAction{Reason: req.GetReason()}},
		},
	}
	send := s.comparePreferenceOptionsSender(comparePreferenceOptionsRequest)
	comparePreferenceOptionsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions")
	}
	compareParcelOptionsRequest := buildCompareParcelOptionsRequestForTerminateParcel(req, comparePreferenceOptionsReply)
	compareParcelOptionsReply, err := s.parcelCalculationClient.CompareParcelOptions(ctx, compareParcelOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/CompareParcelOptions")
	}
	return &shippingv1.TerminateParcelResponse{
		Parcel: &shippingv1.Parcel{
			Owner:    comparePreferenceOptionsReply.GetPreference().GetOwner(),
			Code:     compareParcelOptionsReply.GetParcel().GetCode(),
			ParcelId: req.GetParcelId(),
		},
		Result: &commonv1.OperationResult{
			Details: compareParcelOptionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &shippingv1.TerminateParcelResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewParcel renews a parcel.
func (s *ParcelLifecycleServer) RenewParcel(ctx context.Context, req *shippingv1.RenewParcelRequest) (*shippingv1.RenewParcelResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "renew_parcel"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew parcel", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetNewOwner().GetDisplayName())
	createBillingAccountRequest := &billingv1.CreateBillingAccountRequest{
		Context: req.GetContext(),
		BillingAccount: &billingv1.BillingAccount{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
			Holder:  &billingv1.InvoiceRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
	}
	createBillingAccountReply := new(billingv1.CreateBillingAccountResponse)
	if err := s.conn.Invoke(ctx, billingBillingAccountServiceCreateBillingAccountMethod, createBillingAccountRequest, createBillingAccountReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.billing.v1.BillingAccountService/CreateBillingAccount")
	}
	previewPreferenceRequest := &customersv1.PreviewPreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
		},
		Details: &customersv1.PreviewPreferenceRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: createBillingAccountReply.GetBillingAccount().GetContact().GetEmail(),
				Phone: createBillingAccountReply.GetBillingAccount().GetContact().GetPhone(),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
			},
		},
	}
	previewPreferenceReply, err := s.preferenceValidationPort.PreviewPreference(ctx, previewPreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	normalizeParcelRequest := &shippingv1.NormalizeParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				Email:       previewPreferenceReply.GetPreference().GetOwner().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: previewPreferenceReply.GetPreference().GetAudit().GetUpdatedAt(),
				CreatedAt: previewPreferenceReply.GetPreference().GetAudit().GetCreatedAt(),
			},
		},
	}
	normalizeParcelReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*shippingv1.NormalizeParcelResponse, error) {
		return s.parcelValidationClient.NormalizeParcel(ctx, normalizeParcelRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/NormalizeParcel")
	}
	streamCarrierRateMetricsStream, err := s.carrierRateReportingClient.StreamCarrierRateMetrics(ctx, &shippingv1.StreamCarrierRateMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: previewPreferenceReply.GetPreference().GetOwner().GetEmail(),
			Name:  req.GetDetails().GetNotifyContact().GetName(),
			Phone: createBillingAccountReply.GetBillingAccount().GetContact().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateReportingService/StreamCarrierRateMetrics")
	}
	for {
		received, err := streamCarrierRateMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateReportingService/StreamCarrierRateMetrics")
		}
		_ = received
	}
	return &shippingv1.RenewParcelResponse{
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				Etag:      createBillingAccountReply.GetBillingAccount().GetAudit().GetEtag(),
				UpdatedBy: previewPreferenceReply.GetPreference().GetAudit().GetUpdatedBy(),
				CreatedBy: previewPreferenceReply.GetPreference().GetAudit().GetCreatedBy(),
			},
			Dimensions: &shippingv1.ParcelDimensions{
				Length: normalizeParcelReply.GetParcel().GetDimensions().GetLength(),
			},
			ParcelId: req.GetParcelId(),
			Owner:    &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
		Outcome: &shippingv1.RenewParcelResponse_Outcome{
			EffectiveAt: normalizeParcelReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireParcel expires a parcel.
func (s *ParcelLifecycleServer) ExpireParcel(ctx context.Context, req *shippingv1.ExpireParcelRequest) (*shippingv1.ExpireParcelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExpireParcel", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "expire_parcel"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire parcel", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "parcel_id", req.GetParcelId())
	getParcelRulesRequest := &shippingv1.GetParcelRulesRequest{
		Context:  req.GetContext(),
		ParcelId: tenancy.Value(ctx, "parcel_id"),
	}
	getParcelRulesReply, err := s.parcelValidationPort.GetParcelRules(ctx, getParcelRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/GetParcelRules")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareCarrierRateReportReply, err := s.carrierRateReportingClient.ShareCarrierRateReport(ctx, &shippingv1.ShareCarrierRateReportRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ShareCarrierRateReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotifyContact().GetPhone()},
			Payload:   packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateReportingService/ShareCarrierRateReport")
	}
	return &shippingv1.ExpireParcelResponse{
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: getParcelRulesReply.GetParcel().GetAudit().GetUpdatedBy(),
				CreatedBy: shareCarrierRateReportReply.GetCarrierRate().GetAudit().GetCreatedBy(),
				CreatedAt: shareCarrierRateReportReply.GetCarrierRate().GetAudit().GetCreatedAt(),
			},
			ParcelId: req.GetParcelId(),
		},
		Outcome: &shippingv1.ExpireParcelResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// quoteHouseholdSender returns a function that sends one request to
// acme.customers.v1.HouseholdCalculationService/QuoteHousehold.
func (s *ParcelLifecycleServer) quoteHouseholdSender(request *customersv1.QuoteHouseholdRequest) func(context.Context) (*customersv1.QuoteHouseholdResponse, error) {
	return func(ctx context.Context) (*customersv1.QuoteHouseholdResponse, error) {
		return s.householdCalculationClient.QuoteHousehold(ctx, request)
	}
}

// comparePreferenceOptionsSender returns a function that sends one request to
// acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions.
func (s *ParcelLifecycleServer) comparePreferenceOptionsSender(request *customersv1.ComparePreferenceOptionsRequest) func(context.Context) (*customersv1.ComparePreferenceOptionsResponse, error) {
	return func(ctx context.Context) (*customersv1.ComparePreferenceOptionsResponse, error) {
		return s.preferenceCalculationClient.ComparePreferenceOptions(ctx, request)
	}
}
