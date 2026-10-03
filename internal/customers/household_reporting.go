package customers

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	driversv1 "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// HouseholdReportingServer implements `acme.customers.v1.HouseholdReportingService`.
// Builds and schedules reports about households.
type HouseholdReportingServer struct {
	customersv1.UnimplementedHouseholdReportingServiceServer
	store                          *Store
	log                            *slog.Logger
	consentRecordCalculationClient customersv1.ConsentRecordCalculationServiceClient
	searchIndexValidationClient    searchv1.SearchIndexValidationServiceClient
	consentRecordValidationClient  customersv1.ConsentRecordValidationServiceClient
	customerSegmentImportClient    customersv1.CustomerSegmentImportServiceClient
	conn                           *grpc.ClientConn
	meteredCustomerCalculation     meteredCustomerCalculation
	customerCalculationClient      customersv1.CustomerCalculationServiceClient
	formCalculationClient          formsv1.FormCalculationServiceClient
	customerClient                 customersv1.CustomerServiceClient
}

// NewHouseholdReportingServer wires a HouseholdReportingServer.
func NewHouseholdReportingServer(deps platform.Deps) *HouseholdReportingServer {
	return &HouseholdReportingServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.customers.v1.HouseholdReportingService"),
		consentRecordCalculationClient: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		searchIndexValidationClient:    searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		consentRecordValidationClient:  customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		customerSegmentImportClient:    customersv1.NewCustomerSegmentImportServiceClient(deps.Conn),
		conn:                           deps.Conn,
		meteredCustomerCalculation:     meteredCustomerCalculation{CustomerCalculationServiceClient: customersv1.NewCustomerCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		customerCalculationClient:      customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		formCalculationClient:          formsv1.NewFormCalculationServiceClient(deps.Conn),
		customerClient:                 customersv1.NewCustomerServiceClient(deps.Conn),
	}
}

// GenerateHouseholdReport generates a household.
func (s *HouseholdReportingServer) GenerateHouseholdReport(ctx context.Context, req *customersv1.GenerateHouseholdReportRequest) (*customersv1.GenerateHouseholdReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GenerateHouseholdReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "generate_household_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate household report", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateConsentRecordsRequest := &customersv1.RecalculateConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject:    &customersv1.HouseholdRef{HouseholdId: req.GetHouseholdId()},
			Attributes: req.GetDetails().GetFilters(),
			Detail:     packed,
		},
		Annotations: annotations,
		Reason:      req.GetReason(),
	}
	recalculateConsentRecordsReply, err := grpcx.Call(ctx, s.consentRecordCalculationClient.RecalculateConsentRecords, recalculateConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords")
	}
	lintSearchIndexesReply, err := s.searchIndexValidationClient.LintSearchIndexes(ctx, &searchv1.LintSearchIndexesRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Owner: &identityv1.UserRef{
				DisplayName: recalculateConsentRecordsReply.GetConsentRecord().GetSubject().GetDisplayName(),
				Email:       req.GetContext().GetActor().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/LintSearchIndexes")
	}
	return &customersv1.GenerateHouseholdReportResponse{
		Result: &commonv1.OperationResult{
			Details:       recalculateConsentRecordsReply.GetResult().GetDetails(),
			CorrelationId: recalculateConsentRecordsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  lintSearchIndexesReply.GetResult().GetReferenceIds(),
			Message:       lintSearchIndexesReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchHouseholdReport returns a household.
func (s *HouseholdReportingServer) FetchHouseholdReport(ctx context.Context, req *customersv1.FetchHouseholdReportRequest) (*customersv1.FetchHouseholdReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FetchHouseholdReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "fetch_household_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch household report", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().ProtoReflect()
	locale := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("locale"))).String()
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainConsentRecordDecisionRequest := &customersv1.ExplainConsentRecordDecisionRequest{}
	explainConsentRecordDecisionRequest.Context = req.GetContext()
	explainConsentRecordDecisionRequest.ConsentRecord = &customersv1.ConsentRecord{
		Subject: &customersv1.HouseholdRef{HouseholdId: req.GetHouseholdId()},
		Detail:  packed,
	}
	explainConsentRecordDecisionRequest.Details = &customersv1.ExplainConsentRecordDecisionRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Locale:  locale,
	}
	explainConsentRecordDecisionReply, err := s.consentRecordValidationClient.ExplainConsentRecordDecision(ctx, explainConsentRecordDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
	}
	return &customersv1.FetchHouseholdReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: explainConsentRecordDecisionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Household: &customersv1.Household{HouseholdId: req.GetHouseholdId()},
	}, nil
}

// ScheduleHouseholdReport schedules a household.
func (s *HouseholdReportingServer) ScheduleHouseholdReport(ctx context.Context, req *customersv1.ScheduleHouseholdReportRequest) (*customersv1.ScheduleHouseholdReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "schedule_household_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule household report", "tenant", req.GetContext().GetTenantId())
	validateCustomerSegmentImportRequest := &customersv1.ValidateCustomerSegmentImportRequest{}
	validateCustomerSegmentImportRequest.Context = req.GetContext()
	validateCustomerSegmentImportRequest.Reason = req.GetReason()
	validateCustomerSegmentImportRequest.Annotations = req.GetAnnotations()
	validateCustomerSegmentImportReply, err := s.customerSegmentImportClient.ValidateCustomerSegmentImport(ctx, validateCustomerSegmentImportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/ValidateCustomerSegmentImport")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreConsentRecordRequest := &customersv1.ScoreConsentRecordRequest{
		Context: req.GetContext(),
		Reason:  validateCustomerSegmentImportReply.GetCustomerSegment().GetBlock().GetReason(),
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId: validateCustomerSegmentImportReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
	}
	scoreConsentRecordReply, err := grpcx.NewClient(s.conn, customersv1.NewConsentRecordValidationServiceClient).ScoreConsentRecord(ctx, scoreConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord")
	}
	listDriversRequest := &driversv1.ListDriversRequest{
		Context: req.GetContext(),
		Filter: &driversv1.ListDriversRequest_Filter{
			CreatedAfter: validateCustomerSegmentImportReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
			OwnerId:      validateCustomerSegmentImportReply.GetOutcome().GetReferenceId(),
			Labels:       req.GetDetails().GetFilters(),
		},
	}
	listDriversReply, err := listDriversCall(ctx, listDriversRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.drivers.v1.DriverService/ListDrivers")
	}
	_ = listDriversReply
	return &customersv1.ScheduleHouseholdReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: validateCustomerSegmentImportReply.GetResult().GetReferenceIds(),
			Message:      validateCustomerSegmentImportReply.GetOutcome().GetMessage(),
			Details:      scoreConsentRecordReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// ListHouseholdReports lists households a page at a time.
func (s *HouseholdReportingServer) ListHouseholdReports(ctx context.Context, req *customersv1.ListHouseholdReportsRequest) (*customersv1.ListHouseholdReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list household reports", "tenant", req.GetContext().GetTenantId())
	filterCache := cache.New[string, *customersv1.ListHouseholdReportsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	explainCustomerCalculationRequest := &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress:      &commonv1.PostalAddress{Region: cachedFilter.GetRegion()},
			RelationshipManager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &customersv1.ExplainCustomerCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	}
	explainCustomerCalculationReply, err := s.meteredCustomerCalculation.ExplainCustomerCalculation(ctx, explainCustomerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	return &customersv1.ListHouseholdReportsResponse{
		Households: []*customersv1.Household{&customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Location: &commonv1.PostalAddress{
					Region: explainCustomerCalculationReply.GetCustomer().GetPrimaryAddress().GetRegion(),
				},
			}},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ShareHouseholdReport shares a household.
func (s *HouseholdReportingServer) ShareHouseholdReport(ctx context.Context, req *customersv1.ShareHouseholdReportRequest) (*customersv1.ShareHouseholdReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "share_household_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share household report", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*customersv1.ShareHouseholdReportRequest_Details)
	var envelope customersv1.CustomerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	estimateCustomerRequest := &customersv1.EstimateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetRecipient().GetPhone(),
				Name:  req.GetDetails().GetRecipient().GetName(),
				Email: cloned.GetRecipient().GetEmail(),
			},
		},
		CustomerId: envelope.GetDisplayName(),
	}
	estimateCustomerReply, err := grpcx.Call(ctx, s.customerCalculationClient.EstimateCustomer, estimateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/EstimateCustomer")
	}
	var envelope2 customersv1.CustomerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	quoteFormRequest := &formsv1.QuoteFormRequest{}
	quoteFormRequest.Context = req.GetContext()
	quoteFormRequest.Form = &formsv1.Form{
		Audit: &commonv1.AuditInfo{
			CreatedBy: estimateCustomerReply.GetCustomer().GetAudit().GetCreatedBy(),
			UpdatedBy: estimateCustomerReply.GetCustomer().GetAudit().GetUpdatedBy(),
		},
		BillingAddress: &commonv1.PostalAddress{
			Lines:    estimateCustomerReply.GetCustomer().GetPrimaryAddress().GetLines(),
			Locality: estimateCustomerReply.GetCustomer().GetPrimaryAddress().GetLocality(),
		},
	}
	quoteFormRequest.FormId = envelope2.GetDisplayName()
	quoteFormReply, err := s.formCalculationClient.QuoteForm(ctx, quoteFormRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forms.v1.FormCalculationService/QuoteForm")
	}
	return &customersv1.ShareHouseholdReportResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Audit: &commonv1.AuditInfo{
					CreatedAt: estimateCustomerReply.GetCustomer().GetAudit().GetCreatedAt(),
					UpdatedBy: quoteFormReply.GetForm().GetAudit().GetUpdatedBy(),
				},
				Location: &commonv1.PostalAddress{
					PostalCode: quoteFormReply.GetForm().GetBillingAddress().GetPostalCode(),
				},
			}},
			Location: &commonv1.PostalAddress{
				Recipient: estimateCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
			},
			HouseholdId: req.GetHouseholdId(),
			Manager:     &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountHouseholdTotals counts households by status.
func (s *HouseholdReportingServer) CountHouseholdTotals(ctx context.Context, req *customersv1.CountHouseholdTotalsRequest) (*customersv1.CountHouseholdTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count household totals", "tenant", req.GetContext().GetTenantId())
	normalizeConsentRecordRequest := &customersv1.NormalizeConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.NormalizeConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		ConsentRecord: &customersv1.ConsentRecord{
			OccurredAt:    req.GetFilter().GetCreatedAfter(),
			CorrelationId: req.GetFilter().GetOwnerId(),
		},
	}
	normalizeConsentRecordReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*customersv1.NormalizeConsentRecordResponse, error) {
		return s.consentRecordValidationClient.NormalizeConsentRecord(ctx, normalizeConsentRecordRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord")
	}
	return &customersv1.CountHouseholdTotalsResponse{
		Result: &commonv1.OperationResult{
			Details: normalizeConsentRecordReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// DeleteHouseholdReport deletes a household.
func (s *HouseholdReportingServer) DeleteHouseholdReport(ctx context.Context, req *customersv1.DeleteHouseholdReportRequest) (*customersv1.DeleteHouseholdReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "delete_household_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete household report", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().ProtoReflect()
	locale := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("locale"))).String()
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizeConsentRecordRequest := &customersv1.NormalizeConsentRecordRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &customersv1.NormalizeConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  locale,
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{HouseholdId: req.GetHouseholdId()},
			Detail:  packed,
		},
	}
	send := s.normalizeConsentRecordSender(normalizeConsentRecordRequest)
	normalizeConsentRecordReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord")
	}
	searchCustomersRequest := &customersv1.SearchCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SearchCustomersRequest_Filter{
			CreatedAfter: normalizeConsentRecordReply.GetConsentRecord().GetOccurredAt(),
			Labels:       normalizeConsentRecordReply.GetResult().GetDetails(),
			OwnerId:      normalizeConsentRecordReply.GetConsentRecord().GetCorrelationId(),
		},
	}
	searchCustomersReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.SearchCustomersResponse, error) {
		return s.customerClient.SearchCustomers(ctx, searchCustomersRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/SearchCustomers")
	}
	return &customersv1.DeleteHouseholdReportResponse{
		Result: &commonv1.OperationResult{
			Details: normalizeConsentRecordReply.GetResult().GetDetails(),
			Status:  normalizeConsentRecordReply.GetOutcome().GetStatus(),
			Message: searchCustomersReply.GetResult().GetMessage(),
		},
	}, nil
}

// normalizeConsentRecordSender returns a function that sends one request to
// acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord.
func (s *HouseholdReportingServer) normalizeConsentRecordSender(request *customersv1.NormalizeConsentRecordRequest) func(context.Context) (*customersv1.NormalizeConsentRecordResponse, error) {
	return func(ctx context.Context) (*customersv1.NormalizeConsentRecordResponse, error) {
		return s.consentRecordValidationClient.NormalizeConsentRecord(ctx, request)
	}
}
