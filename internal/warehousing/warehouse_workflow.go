package warehousing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	surveysv1 "github.com/hounddogai/protomap-test-golang/gen/acme/surveys/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WarehouseWorkflowServer implements `acme.warehousing.v1.WarehouseWorkflowService`.
// Runs the long-lived workflow that takes a warehouse from start to finish.
type WarehouseWorkflowServer struct {
	warehousingv1.UnsafeWarehouseWorkflowServiceServer
	store                          *Store
	log                            *slog.Logger
	meteredVenue                   meteredVenue
	restockDecisionLifecycleClient returnsv1.RestockDecisionLifecycleServiceClient
	meteredPutawayTaskValidation   meteredPutawayTaskValidation
	conn                           *grpc.ClientConn
	warehouseQueryClient           warehousingv1.WarehouseQueryServiceClient
	preferenceCalculationClient    customersv1.PreferenceCalculationServiceClient
	storageBinQueryClient          warehousingv1.StorageBinQueryServiceClient
	householdQueryClient           customersv1.HouseholdQueryServiceClient
	preferenceValidationClient     customersv1.PreferenceValidationServiceClient
	loyaltyMemberWorkflowPort      loyaltyMemberWorkflowPort
	searchQueryExportClient        searchv1.SearchQueryExportServiceClient
}

// NewWarehouseWorkflowServer wires a WarehouseWorkflowServer.
func NewWarehouseWorkflowServer(deps platform.Deps) *WarehouseWorkflowServer {
	return &WarehouseWorkflowServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.warehousing.v1.WarehouseWorkflowService"),
		meteredVenue:                   meteredVenue{VenueServiceClient: venuesv1.NewVenueServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		restockDecisionLifecycleClient: returnsv1.NewRestockDecisionLifecycleServiceClient(deps.Conn),
		meteredPutawayTaskValidation:   meteredPutawayTaskValidation{PutawayTaskValidationServiceClient: warehousingv1.NewPutawayTaskValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                           deps.Conn,
		warehouseQueryClient:           warehousingv1.NewWarehouseQueryServiceClient(deps.Conn),
		preferenceCalculationClient:    customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		storageBinQueryClient:          warehousingv1.NewStorageBinQueryServiceClient(deps.Conn),
		householdQueryClient:           customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		preferenceValidationClient:     customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		loyaltyMemberWorkflowPort:      newLoyaltyMemberWorkflowPort(deps),
		searchQueryExportClient:        searchv1.NewSearchQueryExportServiceClient(deps.Conn),
	}
}

// StartWarehouseWorkflow starts a warehouse.
func (s *WarehouseWorkflowServer) StartWarehouseWorkflow(ctx context.Context, req *warehousingv1.StartWarehouseWorkflowRequest) (*warehousingv1.StartWarehouseWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveWarehouse(ctx, req.GetContext().GetTenantId(), req.GetWarehouse()); err != nil {
		return nil, grpcx.Wrap(err, "save warehouse")
	}
	s.log.InfoContext(ctx, "start warehouse workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWarehouse().GetAudit().GetEtag())
	updateVenueRequest := &venuesv1.UpdateVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetWarehouse().GetAudit().GetUpdatedAt()},
		},
	}
	updateVenueReply, err := s.meteredVenue.UpdateVenue(ctx, updateVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/UpdateVenue")
	}
	scheduleRestockDecisionExpiryRequest := &returnsv1.ScheduleRestockDecisionExpiryRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetWarehouse().GetAudit().GetUpdatedBy()},
		},
	}
	scheduleRestockDecisionExpiryReply, err := grpcx.Call(ctx, s.restockDecisionLifecycleClient.ScheduleRestockDecisionExpiry, scheduleRestockDecisionExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionLifecycleService/ScheduleRestockDecisionExpiry")
	}
	normalizePutawayTaskRequest := &warehousingv1.NormalizePutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Origin: &warehousingv1.PutawayTask_Customer{Customer: &warehousingv1.StorageBinRef{
				DisplayName: scheduleRestockDecisionExpiryReply.GetRestockDecision().GetOwner().GetDisplayName(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: scheduleRestockDecisionExpiryReply.GetRestockDecision().GetAudit().GetCreatedAt(),
				CreatedBy: updateVenueReply.GetVenue().GetAudit().GetCreatedBy(),
			},
			Assignee: &identityv1.UserRef{UserId: req.GetWarehouse().GetOwner().GetUserId()},
		},
	}
	normalizePutawayTaskReply, err := s.meteredPutawayTaskValidation.NormalizePutawayTask(ctx, normalizePutawayTaskRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/NormalizePutawayTask")
	}
	return &warehousingv1.StartWarehouseWorkflowResponse{
		Warehouse: &warehousingv1.Warehouse{
			Owner: updateVenueReply.GetVenue().GetOwner(),
			Dimensions: &warehousingv1.WarehouseDimensions{
				Width: updateVenueReply.GetVenue().GetDimensions().GetWidth(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: scheduleRestockDecisionExpiryReply.GetRestockDecision().GetAudit().GetCreatedAt(),
			},
			WarehouseId: req.GetWarehouseId(),
			Capacity:    req.GetWarehouse().GetCapacity(),
		},
		Outcome: &warehousingv1.StartWarehouseWorkflowResponse_Outcome{
			NextSteps: scheduleRestockDecisionExpiryReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{Status: normalizePutawayTaskReply.GetResult().GetStatus()},
	}, nil
}

// AdvanceWarehouseWorkflow advances a warehouse.
func (s *WarehouseWorkflowServer) AdvanceWarehouseWorkflow(ctx context.Context, req *warehousingv1.AdvanceWarehouseWorkflowRequest) (*warehousingv1.AdvanceWarehouseWorkflowResponse, error) {
	if err := s.store.SaveWarehouse(ctx, req.GetContext().GetTenantId(), req.GetWarehouse()); err != nil {
		return nil, grpcx.Wrap(err, "save warehouse")
	}
	s.log.InfoContext(ctx, "advance warehouse workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWarehouse().GetAudit().GetEtag())
	var region string
	switch choice := req.GetWarehouse().GetLocator().(type) {
	case *warehousingv1.Warehouse_Address:
		region = choice.Address.GetRegion()
	case *warehousingv1.Warehouse_FacilityCode:
		region = choice.FacilityCode
	}
	listCarrierRateReportsRequest := &shippingv1.ListCarrierRateReportsRequest{
		Context: req.GetContext(),
		Filter:  &shippingv1.ListCarrierRateReportsRequest_Filter{Region: region},
	}
	listCarrierRateReportsReply := new(shippingv1.ListCarrierRateReportsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", shippingCarrierRateReportingServiceName, "ListCarrierRateReports"), listCarrierRateReportsRequest, listCarrierRateReportsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateReportingService/ListCarrierRateReports")
	}
	return &warehousingv1.AdvanceWarehouseWorkflowResponse{
		Outcome: &warehousingv1.AdvanceWarehouseWorkflowResponse_Outcome{
			Message: listCarrierRateReportsReply.GetResult().GetMessage(),
		},
		Warehouse: &warehousingv1.Warehouse{
			WarehouseId: req.GetWarehouseId(),
			Owner:       req.GetWarehouse().GetOwner(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelWarehouseWorkflow cancels a warehouse.
func (s *WarehouseWorkflowServer) CancelWarehouseWorkflow(ctx context.Context, req *warehousingv1.CancelWarehouseWorkflowRequest) (*warehousingv1.CancelWarehouseWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveWarehouse(ctx, req.GetContext().GetTenantId(), req.GetWarehouse()); err != nil {
		return nil, grpcx.Wrap(err, "save warehouse")
	}
	s.log.InfoContext(ctx, "cancel warehouse workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWarehouse().GetAudit().GetEtag())
	var countryCode string
	switch choice := req.GetWarehouse().GetLocator().(type) {
	case *warehousingv1.Warehouse_Address:
		countryCode = choice.Address.GetCountryCode()
	case *warehousingv1.Warehouse_FacilityCode:
		countryCode = choice.FacilityCode
	}
	cloneSurveyRequest := &surveysv1.CloneSurveyRequest{
		Context: req.GetContext(),
		Survey: &surveysv1.Survey{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetWarehouse().GetAudit().GetCreatedBy(),
				UpdatedBy: req.GetWarehouse().GetAudit().GetUpdatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{CountryCode: countryCode},
		},
	}
	cloneSurveyReply := new(surveysv1.CloneSurveyResponse)
	if err := grpcx.Invoke(ctx, s.conn, surveysv1.SurveyService_CloneSurvey_FullMethodName, cloneSurveyRequest, cloneSurveyReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.surveys.v1.SurveyService/CloneSurvey")
	}
	normalizePickListRequest := &warehousingv1.NormalizePickListRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetWarehouse().GetAudit().GetCreatedBy()},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: cloneSurveyReply.GetSurvey().GetBillingAddress().GetRecipient(),
			},
			Settlement: &warehousingv1.PickList_Card{Card: &warehousingv1.PickListCardSettlement{
				Network: cloneSurveyReply.GetSurvey().GetCard().GetNetwork(),
			}},
		},
		Details: &warehousingv1.NormalizePickListRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetWarehouse().GetOwner().GetEmail()},
		},
	}
	normalizePickListReply := new(warehousingv1.NormalizePickListResponse)
	if err := grpcx.Invoke(ctx, s.conn, warehousingv1.PickListValidationService_NormalizePickList_FullMethodName, normalizePickListRequest, normalizePickListReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/NormalizePickList")
	}
	return &warehousingv1.CancelWarehouseWorkflowResponse{
		Outcome: &warehousingv1.CancelWarehouseWorkflowResponse_Outcome{Message: cloneSurveyReply.GetResult().GetMessage()},
		Warehouse: &warehousingv1.Warehouse{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizePickListReply.GetPickList().GetAudit().GetUpdatedAt(),
			},
			WarehouseId: req.GetWarehouseId(),
			Specs:       req.GetWarehouse().GetSpecs(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryWarehouseWorkflow retrys a warehouse.
func (s *WarehouseWorkflowServer) RetryWarehouseWorkflow(ctx context.Context, req *warehousingv1.RetryWarehouseWorkflowRequest) (*warehousingv1.RetryWarehouseWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryWarehouseWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveWarehouse(ctx, req.GetContext().GetTenantId(), req.GetWarehouse()); err != nil {
		return nil, grpcx.Wrap(err, "save warehouse")
	}
	s.log.InfoContext(ctx, "retry warehouse workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWarehouse().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetWarehouse().GetAddress().GetRegion())
	findWarehousesRequest := &warehousingv1.FindWarehousesRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.FindWarehousesRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: req.GetWarehouse().GetAudit().GetUpdatedAt(),
			Labels:       req.GetDetails().GetParameters(),
		},
	}
	findWarehousesReply, err := grpcx.Call(ctx, s.warehouseQueryClient.FindWarehouses, findWarehousesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/FindWarehouses")
	}
	return &warehousingv1.RetryWarehouseWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: findWarehousesReply.GetResult().GetCorrelationId(),
			Message:       findWarehousesReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Warehouse: &warehousingv1.Warehouse{
			WarehouseId: req.GetWarehouseId(),
			Status:      req.GetWarehouse().GetStatus(),
		},
	}, nil
}

// ResumeWarehouseWorkflow resumes a warehouse.
func (s *WarehouseWorkflowServer) ResumeWarehouseWorkflow(ctx context.Context, req *warehousingv1.ResumeWarehouseWorkflowRequest) (*warehousingv1.ResumeWarehouseWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeWarehouseWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveWarehouse(ctx, req.GetContext().GetTenantId(), req.GetWarehouse()); err != nil {
		return nil, grpcx.Wrap(err, "save warehouse")
	}
	s.log.InfoContext(ctx, "resume warehouse workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWarehouse().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "locality", req.GetWarehouse().GetAddress().GetLocality())
	estimatePreferenceReply, err := s.preferenceCalculationClient.EstimatePreference(ctx, &customersv1.EstimatePreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.EstimatePreferenceRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetWarehouse().GetAddress().GetCountryCode(),
				Locality:    tenancy.Value(ctx, "locality"),
			},
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetWarehouse().GetAudit().GetCreatedAt(),
				CreatedBy: req.GetWarehouse().GetAudit().GetCreatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/EstimatePreference")
	}
	suggestStorageBinsRequest := buildSuggestStorageBinsRequestForResumeWarehouseWorkflow(req)
	suggestStorageBinsReply, err := s.storageBinQueryClient.SuggestStorageBins(ctx, suggestStorageBinsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/SuggestStorageBins")
	}
	var region string
	switch choice := req.GetWarehouse().GetLocator().(type) {
	case *warehousingv1.Warehouse_Address:
		region = choice.Address.GetRegion()
	case *warehousingv1.Warehouse_FacilityCode:
		region = choice.FacilityCode
	}
	browseHouseholdsRequest := &customersv1.BrowseHouseholdsRequest{
		Context: req.GetContext(),
		Filter:  &customersv1.BrowseHouseholdsRequest_Filter{Region: region},
	}
	browseHouseholdsReply, err := grpcx.Call(ctx, s.householdQueryClient.BrowseHouseholds, browseHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/BrowseHouseholds")
	}
	explainPreferenceDecisionRequest := &customersv1.ExplainPreferenceDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainPreferenceDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: estimatePreferenceReply.GetPreference().GetOwner().GetEmail(),
			},
		},
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetWarehouse().GetAudit().GetUpdatedBy()},
		},
	}
	explainPreferenceDecisionReply, err := grpcx.Call(ctx, s.preferenceValidationClient.ExplainPreferenceDecision, explainPreferenceDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ExplainPreferenceDecision")
	}
	return &warehousingv1.ResumeWarehouseWorkflowResponse{
		Warehouse: &warehousingv1.Warehouse{
			Audit: &commonv1.AuditInfo{
				CreatedAt: estimatePreferenceReply.GetPreference().GetAudit().GetCreatedAt(),
				UpdatedAt: explainPreferenceDecisionReply.GetPreference().GetAudit().GetUpdatedAt(),
			},
			WarehouseId: req.GetWarehouseId(),
			Capacity:    req.GetWarehouse().GetCapacity(),
		},
		Outcome: &warehousingv1.ResumeWarehouseWorkflowResponse_Outcome{
			Status:  suggestStorageBinsReply.GetResult().GetStatus(),
			Message: suggestStorageBinsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message: browseHouseholdsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// InspectWarehouseWorkflow returns a warehouse.
func (s *WarehouseWorkflowServer) InspectWarehouseWorkflow(ctx context.Context, req *warehousingv1.InspectWarehouseWorkflowRequest) (*warehousingv1.InspectWarehouseWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "warehouse", req.GetWarehouseId(), "inspect_warehouse_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect warehouse workflow", "tenant", req.GetContext().GetTenantId())
	return &warehousingv1.InspectWarehouseWorkflowResponse{
		Warehouse: &warehousingv1.Warehouse{WarehouseId: req.GetWarehouseId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListWarehouseWorkflows lists warehouses a page at a time.
func (s *WarehouseWorkflowServer) ListWarehouseWorkflows(ctx context.Context, req *warehousingv1.ListWarehouseWorkflowsRequest) (*warehousingv1.ListWarehouseWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list warehouse workflows", "tenant", req.GetContext().GetTenantId())
	cancelLoyaltyMemberWorkflowRequest := &loyaltyv1.CancelLoyaltyMemberWorkflowRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Labels:  req.GetFilter().GetLabels(),
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
	}
	cancelLoyaltyMemberWorkflowReply, err := s.loyaltyMemberWorkflowPort.CancelLoyaltyMemberWorkflow(ctx, cancelLoyaltyMemberWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberWorkflowService/CancelLoyaltyMemberWorkflow")
	}
	lintAccessPoliciesRequest := &authorizationv1.LintAccessPoliciesRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cancelLoyaltyMemberWorkflowReply.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
			},
		},
	}
	lintAccessPoliciesReply, err := grpcx.NewClient(s.conn, authorizationv1.NewAccessPolicyValidationServiceClient).LintAccessPolicies(ctx, lintAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/LintAccessPolicies")
	}
	return &warehousingv1.ListWarehouseWorkflowsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: cancelLoyaltyMemberWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Warehouses: []*warehousingv1.Warehouse{&warehousingv1.Warehouse{
			Audit: &commonv1.AuditInfo{
				CreatedAt: cancelLoyaltyMemberWorkflowReply.GetLoyaltyMember().GetAudit().GetCreatedAt(),
				UpdatedAt: lintAccessPoliciesReply.GetAccessPolicy().GetAudit().GetUpdatedAt(),
			},
		}},
	}, nil
}

// SignalWarehouseWorkflow signals a warehouse.
func (s *WarehouseWorkflowServer) SignalWarehouseWorkflow(ctx context.Context, req *warehousingv1.SignalWarehouseWorkflowRequest) (*warehousingv1.SignalWarehouseWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveWarehouse(ctx, req.GetContext().GetTenantId(), req.GetWarehouse()); err != nil {
		return nil, grpcx.Wrap(err, "save warehouse")
	}
	s.log.InfoContext(ctx, "signal warehouse workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWarehouse().GetAudit().GetEtag())
	var region string
	switch choice := req.GetWarehouse().GetLocator().(type) {
	case *warehousingv1.Warehouse_Address:
		region = choice.Address.GetRegion()
	case *warehousingv1.Warehouse_FacilityCode:
		region = choice.FacilityCode
	}
	streamSearchQueryExportStream, err := s.searchQueryExportClient.StreamSearchQueryExport(ctx, &searchv1.StreamSearchQueryExportRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.StreamSearchQueryExportRequest_Filter{Region: region},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/StreamSearchQueryExport")
	}
	for {
		received, err := streamSearchQueryExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/StreamSearchQueryExport")
		}
		_ = received
	}
	return &warehousingv1.SignalWarehouseWorkflowResponse{
		Warehouse: &warehousingv1.Warehouse{
			WarehouseId: req.GetWarehouseId(),
			Specs:       req.GetWarehouse().GetSpecs(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
