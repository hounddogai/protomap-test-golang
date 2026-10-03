package warehousing

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// StorageBinWorkflowServer implements `acme.warehousing.v1.StorageBinWorkflowService`.
// Runs the long-lived workflow that takes a storage bin from start to finish.
type StorageBinWorkflowServer struct {
	warehousingv1.UnimplementedStorageBinWorkflowServiceServer
	store                       *Store
	log                         *slog.Logger
	venueClient                 venuesv1.VenueServiceClient
	storageBinQueryClient       warehousingv1.StorageBinQueryServiceClient
	venueQueryClient            venuesv1.VenueQueryServiceClient
	cloneWarehouseFn            func(context.Context, *warehousingv1.CloneWarehouseRequest, ...grpc.CallOption) (*warehousingv1.CloneWarehouseResponse, error)
	clients                     *registry.Registry
	pickListApprovalClient      warehousingv1.PickListApprovalServiceClient
	placementApprovalClient     merchandisingv1.PlacementApprovalServiceClient
	conn                        *grpc.ClientConn
	searchQueryHistoryClient    searchv1.SearchQueryHistoryServiceClient
	tenantExportOnce            sync.Once
	tenantExportCached          identityv1.TenantExportServiceClient
	householdValidationClient   customersv1.HouseholdValidationServiceClient
	putawayTaskValidationClient warehousingv1.PutawayTaskValidationServiceClient
	tenantExportClient          identityv1.TenantExportServiceClient
	customerValidationClient    customersv1.CustomerValidationServiceClient
}

// NewStorageBinWorkflowServer wires a StorageBinWorkflowServer.
func NewStorageBinWorkflowServer(deps platform.Deps) *StorageBinWorkflowServer {
	warehouse := warehousingv1.NewWarehouseServiceClient(deps.Conn)
	return &StorageBinWorkflowServer{
		store:                       NewStore(deps.DB),
		log:                         deps.Logger.With("service", "acme.warehousing.v1.StorageBinWorkflowService"),
		venueClient:                 venuesv1.NewVenueServiceClient(deps.Conn),
		storageBinQueryClient:       warehousingv1.NewStorageBinQueryServiceClient(deps.Conn),
		venueQueryClient:            venuesv1.NewVenueQueryServiceClient(deps.Conn),
		cloneWarehouseFn:            warehouse.CloneWarehouse,
		clients:                     deps.Clients,
		pickListApprovalClient:      warehousingv1.NewPickListApprovalServiceClient(deps.Conn),
		placementApprovalClient:     merchandisingv1.NewPlacementApprovalServiceClient(deps.Conn),
		conn:                        deps.Conn,
		searchQueryHistoryClient:    searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
		householdValidationClient:   customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		putawayTaskValidationClient: warehousingv1.NewPutawayTaskValidationServiceClient(deps.Conn),
		tenantExportClient:          identityv1.NewTenantExportServiceClient(deps.Conn),
		customerValidationClient:    customersv1.NewCustomerValidationServiceClient(deps.Conn),
	}
}

// StartStorageBinWorkflow starts a storage bin.
func (s *StorageBinWorkflowServer) StartStorageBinWorkflow(ctx context.Context, req *warehousingv1.StartStorageBinWorkflowRequest) (*warehousingv1.StartStorageBinWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StartStorageBinWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "start storage bin workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
	names := collectStorageBinNames(req.GetStorageBin(), nil)
	updateVenueRequest := &venuesv1.UpdateVenueRequest{}
	updateVenueRequest.Context = req.GetContext()
	updateVenueRequest.Venue = &venuesv1.Venue{
		Owner: &identityv1.UserRef{Email: req.GetStorageBin().GetManager().GetEmail()},
		Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{
			CountryCode: req.GetStorageBin().GetLocation().GetCountryCode(),
		}},
		Audit: &commonv1.AuditInfo{
			Etag:      req.GetStorageBin().GetAudit().GetEtag(),
			CreatedBy: req.GetStorageBin().GetAudit().GetCreatedBy(),
		},
		CategoryPath: names,
	}
	updateVenueReply, err := s.venueClient.UpdateVenue(ctx, updateVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/UpdateVenue")
	}
	exportStorageBinsStream, err := s.storageBinQueryClient.ExportStorageBins(ctx, &warehousingv1.ExportStorageBinsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.ExportStorageBinsRequest_Filter{
			Region:       req.GetStorageBin().GetLocation().GetRegion(),
			CreatedAfter: updateVenueReply.GetVenue().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/ExportStorageBins")
	}
	for {
		received, err := exportStorageBinsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/ExportStorageBins")
		}
		_ = received
	}
	return &warehousingv1.StartStorageBinWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: updateVenueReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		StorageBin: &warehousingv1.StorageBin{
			StorageBinId: req.GetStorageBinId(),
			Status:       req.GetStorageBin().GetStatus(),
		},
	}, nil
}

// AdvanceStorageBinWorkflow advances a storage bin.
func (s *StorageBinWorkflowServer) AdvanceStorageBinWorkflow(ctx context.Context, req *warehousingv1.AdvanceStorageBinWorkflowRequest) (*warehousingv1.AdvanceStorageBinWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvanceStorageBinWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "advance storage bin workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
	findVenuesRequest := &venuesv1.FindVenuesRequest{
		Context: req.GetContext(),
		Filter: &venuesv1.FindVenuesRequest_Filter{
			Region:       req.GetStorageBin().GetLocation().GetRegion(),
			CreatedAfter: req.GetStorageBin().GetAudit().GetCreatedAt(),
		},
	}
	findVenuesReply, err := grpcx.Call(ctx, s.venueQueryClient.FindVenues, findVenuesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/FindVenues")
	}
	cloneWarehouseRequest := &warehousingv1.CloneWarehouseRequest{
		Context: req.GetContext(),
		Warehouse: &warehousingv1.Warehouse{
			Locator: &warehousingv1.Warehouse_Address{Address: &commonv1.PostalAddress{
				PostalCode: req.GetStorageBin().GetLocation().GetPostalCode(),
			}},
		},
	}
	cloneWarehouseReply, err := s.cloneWarehouseFn(ctx, cloneWarehouseRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseService/CloneWarehouse")
	}
	storageBinQuery, err := registry.Lookup[warehousingv1.StorageBinQueryServiceClient](s.clients, "acme.warehousing.v1.StorageBinQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	lookupStorageBinRequest := &warehousingv1.LookupStorageBinRequest{
		Context:      req.GetContext(),
		StorageBinId: req.GetStorageBinId(),
	}
	lookupStorageBinReply, err := storageBinQuery.LookupStorageBin(ctx, lookupStorageBinRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/LookupStorageBin")
	}
	names := collectStorageBinNames(req.GetStorageBin(), nil)
	updateCustomerRequest := &customersv1.UpdateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetStorageBin().GetManager().GetDisplayName(),
			},
			PrimaryAddress: &commonv1.PostalAddress{Lines: names},
		},
	}
	updateCustomerReply, err := updateCustomerCall(ctx, updateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/UpdateCustomer")
	}
	_ = updateCustomerReply
	return &warehousingv1.AdvanceStorageBinWorkflowResponse{
		Result: &commonv1.OperationResult{Status: findVenuesReply.GetResult().GetStatus()},
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Audit: &commonv1.AuditInfo{
						CreatedAt: cloneWarehouseReply.GetWarehouse().GetAudit().GetCreatedAt(),
					},
					Manager: lookupStorageBinReply.GetStorageBin().GetManager(),
				}},
			}},
			StorageBinId: req.GetStorageBinId(),
			Attributes:   req.GetStorageBin().GetAttributes(),
		},
	}, nil
}

// CancelStorageBinWorkflow cancels a storage bin.
func (s *StorageBinWorkflowServer) CancelStorageBinWorkflow(ctx context.Context, req *warehousingv1.CancelStorageBinWorkflowRequest) (*warehousingv1.CancelStorageBinWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "cancel storage bin workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
	apiCredentialValidation, err := registry.Lookup[identityv1.ApiCredentialValidationServiceClient](s.clients, "acme.identity.v1.ApiCredentialValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	normalizeApiCredentialRequest := &identityv1.NormalizeApiCredentialRequest{
		Context: req.GetContext(),
		Details: &identityv1.NormalizeApiCredentialRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetStorageBin().GetManager().GetEmail()},
		},
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetStorageBin().GetAudit().GetUpdatedAt()},
		},
	}
	normalizeApiCredentialReply, err := apiCredentialValidation.NormalizeApiCredential(ctx, normalizeApiCredentialRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/NormalizeApiCredential")
	}
	ctx = tenancy.With(ctx, "region", req.GetStorageBin().GetLocation().GetRegion())
	suggestStorageBinsRequest := &warehousingv1.SuggestStorageBinsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.SuggestStorageBinsRequest_Filter{
			Region: tenancy.Value(ctx, "region"),
			Labels: normalizeApiCredentialReply.GetApiCredential().GetLabels(),
		},
	}
	suggestStorageBinsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*warehousingv1.SuggestStorageBinsResponse, error) {
		return s.storageBinQueryClient.SuggestStorageBins(ctx, suggestStorageBinsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/SuggestStorageBins")
	}
	names := collectStorageBinNames(req.GetStorageBin(), nil)
	approvePickListRequest := &warehousingv1.ApprovePickListRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Settlement: &warehousingv1.PickList_Card{Card: &warehousingv1.PickListCardSettlement{
				Network: normalizeApiCredentialReply.GetApiCredential().GetCard().GetNetwork(),
			}},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetStorageBin().GetManager().GetEmail()},
			Audit:          &commonv1.AuditInfo{Etag: req.GetStorageBin().GetAudit().GetEtag()},
			BillingAddress: &commonv1.PostalAddress{Lines: names},
		},
		Details: &warehousingv1.ApprovePickListRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: normalizeApiCredentialReply.GetApiCredential().GetHolder().GetDisplayName(),
			},
		},
	}
	approvePickListReply, err := grpcx.Call(ctx, s.pickListApprovalClient.ApprovePickList, approvePickListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListApprovalService/ApprovePickList")
	}
	source := req.GetStorageBin().GetLocation().ProtoReflect()
	locality := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("locality"))).String()
	approvePlacementReply, err := s.placementApprovalClient.ApprovePlacement(ctx, &merchandisingv1.ApprovePlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					Phone: normalizeApiCredentialReply.GetApiCredential().GetContact().GetPhone(),
				},
			}},
			Location: &commonv1.PostalAddress{Locality: locality},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementApprovalService/ApprovePlacement")
	}
	_ = approvePlacementReply
	return &warehousingv1.CancelStorageBinWorkflowResponse{
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Audit: &commonv1.AuditInfo{
					CreatedAt: normalizeApiCredentialReply.GetApiCredential().GetAudit().GetCreatedAt(),
				},
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Manager: &identityv1.UserRef{
						DisplayName: normalizeApiCredentialReply.GetApiCredential().GetHolder().GetDisplayName(),
					},
				}},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: approvePickListReply.GetPickList().GetAudit().GetCreatedBy(),
			},
			StorageBinId: req.GetStorageBinId(),
			Status:       req.GetStorageBin().GetStatus(),
		},
		Result:  &commonv1.OperationResult{Status: suggestStorageBinsReply.GetResult().GetStatus()},
		Outcome: &warehousingv1.CancelStorageBinWorkflowResponse_Outcome{Message: suggestStorageBinsReply.GetResult().GetMessage()},
	}, nil
}

// RetryStorageBinWorkflow retrys a storage bin.
func (s *StorageBinWorkflowServer) RetryStorageBinWorkflow(ctx context.Context, req *warehousingv1.RetryStorageBinWorkflowRequest) (*warehousingv1.RetryStorageBinWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryStorageBinWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "retry storage bin workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded warehousingv1.RetryStorageBinWorkflowRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	names := collectStorageBinNames(req.GetStorageBin(), nil)
	updateCustomerRequest := &customersv1.UpdateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact:             &commonv1.ContactInfo{Name: (&decoded).GetNotify().GetName()},
			RelationshipManager: &identityv1.UserRef{UserId: req.GetStorageBin().GetManager().GetUserId()},
			PrimaryAddress:      &commonv1.PostalAddress{Lines: names},
		},
	}
	updateCustomerReply := new(customersv1.UpdateCustomerResponse)
	if err := s.conn.Invoke(ctx, customersCustomerServiceUpdateCustomerMethod, updateCustomerRequest, updateCustomerReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/UpdateCustomer")
	}
	unsubscribeSessionUpdatesRequest := &identityv1.UnsubscribeSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetNotify().GetEmail(),
				Name:             updateCustomerReply.GetCustomer().GetContact().GetName(),
				PreferredChannel: updateCustomerReply.GetCustomer().GetContact().GetPreferredChannel(),
				Phone:            updateCustomerReply.GetCustomer().GetContact().GetPhone(),
			},
		},
	}
	unsubscribeSessionUpdatesReply := new(identityv1.UnsubscribeSessionUpdatesResponse)
	if err := s.conn.Invoke(ctx, identitySessionNotificationServiceUnsubscribeSessionUpdatesMethod, unsubscribeSessionUpdatesRequest, unsubscribeSessionUpdatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/UnsubscribeSessionUpdates")
	}
	return &warehousingv1.RetryStorageBinWorkflowResponse{
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Location: &commonv1.PostalAddress{
						Region: updateCustomerReply.GetCustomer().GetPrimaryAddress().GetRegion(),
					},
				}},
				Audit: &commonv1.AuditInfo{
					UpdatedBy: unsubscribeSessionUpdatesReply.GetSession().GetAudit().GetUpdatedBy(),
				},
			}},
			Manager: &identityv1.UserRef{
				Email: updateCustomerReply.GetCustomer().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: unsubscribeSessionUpdatesReply.GetSession().GetAudit().GetCreatedBy(),
			},
			StorageBinId: req.GetStorageBinId(),
			Depth:        req.GetStorageBin().GetDepth(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeStorageBinWorkflow resumes a storage bin.
func (s *StorageBinWorkflowServer) ResumeStorageBinWorkflow(ctx context.Context, req *warehousingv1.ResumeStorageBinWorkflowRequest) (*warehousingv1.ResumeStorageBinWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "resume storage bin workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetStorageBin().GetManager().GetDisplayName())
	snapshot, err := protojson.Marshal(req.GetStorageBin())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertSearchQueryRequest := &searchv1.RevertSearchQueryRequest{}
	revertSearchQueryRequest.Context = req.GetContext()
	revertSearchQueryRequest.SearchQuery = &searchv1.SearchQuery{
		Attributes: req.GetStorageBin().GetAttributes(),
		Subject:    &searchv1.SearchIndexRef{DisplayName: tenancy.Value(ctx, "display_name")},
	}
	revertSearchQueryRequest.Details = &searchv1.RevertSearchQueryRequest_Details{Snapshot: snapshot}
	revertSearchQueryReply, err := s.searchQueryHistoryClient.RevertSearchQuery(ctx, revertSearchQueryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/RevertSearchQuery")
	}
	ctx = tenancy.With(ctx, "tenant_id", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetStorageBin())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded warehousingv1.StorageBin
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cancelTenantExportRequest := &identityv1.CancelTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: tenancy.Value(ctx, "tenant_id"),
		Details: &identityv1.CancelTenantExportRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Email:            (&decoded).GetManager().GetEmail(),
			},
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetStorageBin().GetManager().GetDisplayName(),
			},
		},
	}
	cancelTenantExportReply, err := s.tenantExportLazy().CancelTenantExport(ctx, cancelTenantExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/CancelTenantExport")
	}
	names := collectStorageBinNames(req.GetStorageBin(), nil)
	checkHouseholdEligibilityRequest := newCheckHouseholdEligibilityRequest(withCheckHouseholdEligibilityContext(req.GetContext()), withCheckHouseholdEligibilityHousehold(&customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{
					DisplayName: cancelTenantExportReply.GetTenant().GetManager().GetDisplayName(),
				},
				Audit: &commonv1.AuditInfo{
					UpdatedAt: cancelTenantExportReply.GetTenant().GetAudit().GetUpdatedAt(),
					CreatedAt: req.GetStorageBin().GetAudit().GetCreatedAt(),
					UpdatedBy: req.GetStorageBin().GetAudit().GetUpdatedBy(),
				},
			}},
		}},
		Path: names,
	}))
	checkHouseholdEligibilityReply, err := s.householdValidationClient.CheckHouseholdEligibility(ctx, checkHouseholdEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/CheckHouseholdEligibility")
	}
	return &warehousingv1.ResumeStorageBinWorkflowResponse{
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Manager: &identityv1.UserRef{
					DisplayName: revertSearchQueryReply.GetSearchQuery().GetSubject().GetDisplayName(),
				},
				Attributes: revertSearchQueryReply.GetSearchQuery().GetAttributes(),
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Depth: checkHouseholdEligibilityReply.GetHousehold().GetDepth(),
				}},
			}},
			StorageBinId: req.GetStorageBinId(),
			Depth:        req.GetStorageBin().GetDepth(),
		},
		Outcome: &warehousingv1.ResumeStorageBinWorkflowResponse_Outcome{
			NextSteps: cancelTenantExportReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			Status: checkHouseholdEligibilityReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// ListStorageBinWorkflows lists storage bins a page at a time.
func (s *StorageBinWorkflowServer) ListStorageBinWorkflows(ctx context.Context, req *warehousingv1.ListStorageBinWorkflowsRequest) (*warehousingv1.ListStorageBinWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list storage bin workflows", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	explainPutawayTaskDecisionRequest := &warehousingv1.ExplainPutawayTaskDecisionRequest{}
	explainPutawayTaskDecisionRequest.Context = req.GetContext()
	explainPutawayTaskDecisionRequest.PutawayTask = &warehousingv1.PutawayTask{
		Assignee: &identityv1.UserRef{
			Email:  (&decoded).GetActor().GetEmail(),
			UserId: req.GetContext().GetActor().GetUserId(),
		},
		Audit: &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
	}
	explainPutawayTaskDecisionRequest.Details = &warehousingv1.ExplainPutawayTaskDecisionRequest_Details{Locale: cachedContext.GetLocale()}
	explainPutawayTaskDecisionReply, err := s.putawayTaskValidationClient.ExplainPutawayTaskDecision(ctx, explainPutawayTaskDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/ExplainPutawayTaskDecision")
	}
	suggestStorageBinsRequest := &warehousingv1.SuggestStorageBinsRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
	}
	suggestStorageBinsReply := new(warehousingv1.SuggestStorageBinsResponse)
	if err := grpcx.Invoke(ctx, s.conn, warehousingv1.StorageBinQueryService_SuggestStorageBins_FullMethodName, suggestStorageBinsRequest, suggestStorageBinsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/SuggestStorageBins")
	}
	return &warehousingv1.ListStorageBinWorkflowsResponse{
		StorageBins: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
			Audit: &commonv1.AuditInfo{
				CreatedAt: explainPutawayTaskDecisionReply.GetPutawayTask().GetAudit().GetCreatedAt(),
			},
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: explainPutawayTaskDecisionReply.GetPutawayTask().GetAudit().GetUpdatedAt(),
					},
				}},
			}},
		}},
		PageInfo: &commonv1.PageInfo{
			NextPageToken: suggestStorageBinsReply.GetPageInfo().GetNextPageToken(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SignalStorageBinWorkflow signals a storage bin.
func (s *StorageBinWorkflowServer) SignalStorageBinWorkflow(ctx context.Context, req *warehousingv1.SignalStorageBinWorkflowRequest) (*warehousingv1.SignalStorageBinWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SignalStorageBinWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "signal storage bin workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
	attributes := make(map[string]string, len(req.GetStorageBin().GetAttributes()))
	for key, value := range req.GetStorageBin().GetAttributes() {
		attributes[key] = value
	}
	streamTenantExportStream, err := s.tenantExportClient.StreamTenantExport(ctx, &identityv1.StreamTenantExportRequest{
		Context: req.GetContext(),
		Filter: &identityv1.StreamTenantExportRequest_Filter{
			Region:       req.GetStorageBin().GetLocation().GetRegion(),
			CreatedAfter: req.GetStorageBin().GetAudit().GetUpdatedAt(),
			Labels:       attributes,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
	}
	for {
		received, err := streamTenantExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
		}
		_ = received
	}
	ctx = tenancy.With(ctx, "recipient", req.GetStorageBin().GetLocation().GetRecipient())
	names := collectStorageBinNames(req.GetStorageBin(), nil)
	checkCustomerEligibilityRequest := &customersv1.CheckCustomerEligibilityRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient: tenancy.Value(ctx, "recipient"),
				Lines:     names,
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetStorageBin().GetAudit().GetUpdatedAt()},
		},
	}
	checkCustomerEligibilityReply, err := grpcx.Call(ctx, s.customerValidationClient.CheckCustomerEligibility, checkCustomerEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/CheckCustomerEligibility")
	}
	return &warehousingv1.SignalStorageBinWorkflowResponse{
		Outcome: &warehousingv1.SignalStorageBinWorkflowResponse_Outcome{
			NextSteps: checkCustomerEligibilityReply.GetOutcome().GetNextSteps(),
		},
		StorageBin: &warehousingv1.StorageBin{
			StorageBinId: req.GetStorageBinId(),
			Manager:      req.GetStorageBin().GetManager(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// tenantExportLazy dials acme.identity.v1.TenantExportService the first time a handler needs it.
func (s *StorageBinWorkflowServer) tenantExportLazy() identityv1.TenantExportServiceClient {
	s.tenantExportOnce.Do(func() { s.tenantExportCached = identityv1.NewTenantExportServiceClient(s.conn) })
	return s.tenantExportCached
}
