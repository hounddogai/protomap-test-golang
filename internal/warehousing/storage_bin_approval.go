package warehousing

import (
	"context"
	"fmt"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// StorageBinApprovalServer implements `acme.warehousing.v1.StorageBinApprovalService`.
// Routes storage bins through approval, with escalation and delegation.
type StorageBinApprovalServer struct {
	warehousingv1.UnimplementedStorageBinApprovalServiceServer
	store                             *Store
	log                               *slog.Logger
	conn                              *grpc.ClientConn
	pickListValidationClient          warehousingv1.PickListValidationServiceClient
	householdValidationClient         customersv1.HouseholdValidationServiceClient
	householdValidationClientFallback customersv1.HouseholdValidationServiceClient
	warehouseClient                   warehousingv1.WarehouseServiceClient
	putawayTaskValidationClient       warehousingv1.PutawayTaskValidationServiceClient
	accessPolicyValidationClient      authorizationv1.AccessPolicyValidationServiceClient
	findStorageBinsFn                 func(context.Context, *warehousingv1.FindStorageBinsRequest, ...grpc.CallOption) (*warehousingv1.FindStorageBinsResponse, error)
	storageBinQueryClient             warehousingv1.StorageBinQueryServiceClient
	customerCalculationClient         customersv1.CustomerCalculationServiceClient
	meteredReviewVoteWorkflow         meteredReviewVoteWorkflow
	userClient                        identityv1.UserServiceClient
	userClientFallback                identityv1.UserServiceClient
	preferenceValidationClient        customersv1.PreferenceValidationServiceClient
	trackingEventApprovalClient       shippingv1.TrackingEventApprovalServiceClient
	clients                           *registry.Registry
}

// NewStorageBinApprovalServer wires a StorageBinApprovalServer.
func NewStorageBinApprovalServer(deps platform.Deps) *StorageBinApprovalServer {
	storageBinQuery := warehousingv1.NewStorageBinQueryServiceClient(deps.Conn)
	return &StorageBinApprovalServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.warehousing.v1.StorageBinApprovalService"),
		conn:                              deps.Conn,
		pickListValidationClient:          warehousingv1.NewPickListValidationServiceClient(deps.Conn),
		householdValidationClient:         customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		householdValidationClientFallback: customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		warehouseClient:                   warehousingv1.NewWarehouseServiceClient(deps.Conn),
		putawayTaskValidationClient:       warehousingv1.NewPutawayTaskValidationServiceClient(deps.Conn),
		accessPolicyValidationClient:      authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		findStorageBinsFn:                 storageBinQuery.FindStorageBins,
		storageBinQueryClient:             warehousingv1.NewStorageBinQueryServiceClient(deps.Conn),
		customerCalculationClient:         customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		meteredReviewVoteWorkflow:         meteredReviewVoteWorkflow{ReviewVoteWorkflowServiceClient: reviewsv1.NewReviewVoteWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		userClient:                        identityv1.NewUserServiceClient(deps.Conn),
		userClientFallback:                identityv1.NewUserServiceClient(deps.Conn),
		preferenceValidationClient:        customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		trackingEventApprovalClient:       shippingv1.NewTrackingEventApprovalServiceClient(deps.Conn),
		clients:                           deps.Clients,
	}
}

// SubmitStorageBin submits a storage bin.
func (s *StorageBinApprovalServer) SubmitStorageBin(ctx context.Context, req *warehousingv1.SubmitStorageBinRequest) (*warehousingv1.SubmitStorageBinResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "submit storage bin", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	names := collectStorageBinNames(req.GetStorageBin(), nil)
	summarizeStorageBinsRequest := &warehousingv1.SummarizeStorageBinsRequest{
		Context:      req.GetContext(),
		StorageBinId: req.GetStorageBin().GetStorageBinId(),
		Reason:       req.GetReason(),
		Annotations:  annotations,
		Details: &warehousingv1.SummarizeStorageBinsRequest_Details{
			Filters:         req.GetStorageBin().GetAttributes(),
			RecipientEmails: names,
		},
	}
	summarizeStorageBinsReply := new(warehousingv1.SummarizeStorageBinsResponse)
	if err := grpc.Invoke(ctx, "/acme.warehousing.v1.StorageBinQueryService/SummarizeStorageBins", summarizeStorageBinsRequest, summarizeStorageBinsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/SummarizeStorageBins")
	}
	ctx = tenancy.With(ctx, "email", req.GetStorageBin().GetManager().GetEmail())
	source := req.GetStorageBin().GetLocation().ProtoReflect()
	recipient := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("recipient"))).String()
	names2 := collectStorageBinNames(req.GetStorageBin(), nil)
	step := &storageBinApprovalServerSubmitStorageBinPreviewPickListStep{request: &warehousingv1.PreviewPickListRequest{
		Context: req.GetContext(),
		Details: &warehousingv1.PreviewPickListRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
		PickList: &warehousingv1.PickList{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: recipient,
				Lines:     names2,
			},
		},
	}}
	if err := s.runSubmitStorageBinPreviewPickList(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/PreviewPickList")
	}
	previewPickListReply := step.reply
	return &warehousingv1.SubmitStorageBinResponse{
		Result: &commonv1.OperationResult{
			Message: summarizeStorageBinsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Manager: &identityv1.UserRef{
					Email: previewPickListReply.GetPickList().GetCounterparty().GetEmail(),
				},
			}},
			StorageBinId: req.GetStorageBinId(),
			Name:         req.GetStorageBin().GetName(),
		},
	}, nil
}

// ApproveStorageBin approves a storage bin.
func (s *StorageBinApprovalServer) ApproveStorageBin(ctx context.Context, req *warehousingv1.ApproveStorageBinRequest) (*warehousingv1.ApproveStorageBinResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveStorageBin", "error", err)
		return nil, err
	}
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "approve storage bin", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *customersv1.ScoreHouseholdRequest, ...grpc.CallOption) (*customersv1.ScoreHouseholdResponse, error){
		"primary":  s.householdValidationClient.ScoreHousehold,
		"fallback": s.householdValidationClientFallback.ScoreHousehold,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	source := req.GetStorageBin().GetManager().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	encoded, err := protojson.Marshal(req.GetStorageBin())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded warehousingv1.StorageBin
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	names := collectStorageBinNames(req.GetStorageBin(), nil)
	scoreHouseholdRequest := &customersv1.ScoreHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{Email: email},
					Location: &commonv1.PostalAddress{
						CountryCode: req.GetStorageBin().GetLocation().GetCountryCode(),
					},
					Audit: &commonv1.AuditInfo{Etag: req.GetStorageBin().GetAudit().GetEtag()},
				}},
				Audit: &commonv1.AuditInfo{UpdatedAt: (&decoded).GetAudit().GetUpdatedAt()},
			}},
			Path: names,
		},
	}
	scoreHouseholdReply, err := route(ctx, scoreHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ScoreHousehold")
	}
	searchWarehousesRequest := &warehousingv1.SearchWarehousesRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.SearchWarehousesRequest_Filter{
			Region: scoreHouseholdReply.GetHousehold().GetLocation().GetRegion(),
		},
	}
	var searchWarehousesReply *warehousingv1.SearchWarehousesResponse
	if err := pipeline.Run(ctx, "acme.warehousing.v1.WarehouseService/SearchWarehouses", func(ctx context.Context) error {
		answer, err := s.warehouseClient.SearchWarehouses(ctx, searchWarehousesRequest)
		searchWarehousesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseService/SearchWarehouses")
	}
	return &warehousingv1.ApproveStorageBinResponse{
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Attributes: scoreHouseholdReply.GetHousehold().GetAttributes(),
					Audit: &commonv1.AuditInfo{
						CreatedBy: scoreHouseholdReply.GetHousehold().GetAudit().GetCreatedBy(),
					},
				}},
			}},
			StorageBinId: req.GetStorageBinId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: searchWarehousesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &warehousingv1.ApproveStorageBinResponse_Outcome{
			Message:     searchWarehousesReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// RejectStorageBin rejects a storage bin.
func (s *StorageBinApprovalServer) RejectStorageBin(ctx context.Context, req *warehousingv1.RejectStorageBinRequest) (*warehousingv1.RejectStorageBinResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "reject storage bin", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetStorageBin())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded warehousingv1.StorageBin
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	names := collectStorageBinNames(req.GetStorageBin(), nil)
	validatePutawayTaskReply, err := s.putawayTaskValidationClient.ValidatePutawayTask(ctx, &warehousingv1.ValidatePutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Audit: &commonv1.AuditInfo{CreatedAt: (&decoded).GetAudit().GetCreatedAt()},
			Tags:  names,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/ValidatePutawayTask")
	}
	ctx = tenancy.With(ctx, "recipient", req.GetStorageBin().GetLocation().GetRecipient())
	scoreAccessPolicyRequest := &authorizationv1.ScoreAccessPolicyRequest{}
	scoreAccessPolicyRequest.Context = req.GetContext()
	scoreAccessPolicyRequest.Details = &authorizationv1.ScoreAccessPolicyRequest_Details{
		Destination: &commonv1.PostalAddress{
			Recipient:   tenancy.Value(ctx, "recipient"),
			CountryCode: req.GetStorageBin().GetLocation().GetCountryCode(),
		},
	}
	scoreAccessPolicyRequest.AccessPolicy = &authorizationv1.AccessPolicy{
		Audit: &commonv1.AuditInfo{
			CreatedBy: validatePutawayTaskReply.GetPutawayTask().GetAudit().GetCreatedBy(),
		},
	}
	scoreAccessPolicyReply, err := s.accessPolicyValidationClient.ScoreAccessPolicy(ctx, scoreAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/ScoreAccessPolicy")
	}
	suggestStorageBinsRequest := &warehousingv1.SuggestStorageBinsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.SuggestStorageBinsRequest_Filter{
			Region:       req.GetStorageBin().GetLocation().GetRegion(),
			CreatedAfter: validatePutawayTaskReply.GetPutawayTask().GetAudit().GetUpdatedAt(),
			OwnerId:      validatePutawayTaskReply.GetOutcome().GetReferenceId(),
		},
	}
	suggestStorageBinsReply := new(warehousingv1.SuggestStorageBinsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", warehousingStorageBinQueryServiceName, "SuggestStorageBins"), suggestStorageBinsRequest, suggestStorageBinsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/SuggestStorageBins")
	}
	return &warehousingv1.RejectStorageBinResponse{
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
						StorageBinId: validatePutawayTaskReply.GetPutawayTask().GetCustomer().GetStorageBinId(),
					}},
				}},
				Audit: &commonv1.AuditInfo{
					CreatedBy: scoreAccessPolicyReply.GetAccessPolicy().GetAudit().GetCreatedBy(),
					CreatedAt: scoreAccessPolicyReply.GetAccessPolicy().GetAudit().GetCreatedAt(),
				},
			}},
			StorageBinId: req.GetStorageBinId(),
			Depth:        req.GetStorageBin().GetDepth(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestStorageBinsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// EscalateStorageBin escalates a storage bin.
func (s *StorageBinApprovalServer) EscalateStorageBin(ctx context.Context, req *warehousingv1.EscalateStorageBinRequest) (*warehousingv1.EscalateStorageBinResponse, error) {
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "escalate storage bin", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
	storageBinCache := cache.New[string, *warehousingv1.StorageBin](1)
	storageBinCache.Put(req.GetContext().GetTraceId(), req.GetStorageBin())
	cachedStorageBin, _ := storageBinCache.Get(req.GetContext().GetTraceId())
	cloned := proto.Clone(req.GetDetails()).(*warehousingv1.EscalateStorageBinRequest_Details)
	findStorageBinsRequest := &warehousingv1.FindStorageBinsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.FindStorageBinsRequest_Filter{
			Region:       cachedStorageBin.GetLocation().GetRegion(),
			CreatedAfter: cloned.GetEffectiveAt(),
		},
	}
	findStorageBinsReply, err := s.findStorageBinsFn(ctx, findStorageBinsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/FindStorageBins")
	}
	return &warehousingv1.EscalateStorageBinResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: findStorageBinsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		StorageBin: &warehousingv1.StorageBin{
			StorageBinId: req.GetStorageBinId(),
			Attributes:   req.GetStorageBin().GetAttributes(),
		},
	}, nil
}

// ListPendingStorageBins lists storage bins a page at a time.
func (s *StorageBinApprovalServer) ListPendingStorageBins(ctx context.Context, req *warehousingv1.ListPendingStorageBinsRequest) (*warehousingv1.ListPendingStorageBinsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending storage bins", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	cloned := proto.Clone(req.GetFilter()).(*warehousingv1.ListPendingStorageBinsRequest_Filter)
	suggestStorageBinsRequest := &warehousingv1.SuggestStorageBinsRequest{}
	suggestStorageBinsRequest.Context = req.GetContext()
	suggestStorageBinsRequest.Filter = &warehousingv1.SuggestStorageBinsRequest_Filter{
		Labels:   labels,
		Statuses: cloned.GetStatuses(),
	}
	suggestStorageBinsReply, err := s.storageBinQueryClient.SuggestStorageBins(ctx, suggestStorageBinsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/SuggestStorageBins")
	}
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	recalculateCustomersRequest := &customersv1.RecalculateCustomersRequest{}
	recalculateCustomersRequest.Context = req.GetContext()
	recalculateCustomersRequest.Customer = &customersv1.Customer{
		PrimaryAddress:      &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		Contact:             &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		RelationshipManager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
	}
	recalculateCustomersReply, err := s.customerCalculationClient.RecalculateCustomers(ctx, recalculateCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	return &warehousingv1.ListPendingStorageBinsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: suggestStorageBinsReply.GetResult().GetCorrelationId(),
			Status:        suggestStorageBinsReply.GetResult().GetStatus(),
		},
		StorageBins: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Audit: &commonv1.AuditInfo{
					CreatedBy: recalculateCustomersReply.GetCustomer().GetAudit().GetCreatedBy(),
				},
			}},
		}},
	}, nil
}

// DelegateStorageBinApproval delegates a storage bin.
func (s *StorageBinApprovalServer) DelegateStorageBinApproval(ctx context.Context, req *warehousingv1.DelegateStorageBinApprovalRequest) (*warehousingv1.DelegateStorageBinApprovalResponse, error) {
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "delegate storage bin approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
	detailsCache := cache.New[string, *warehousingv1.DelegateStorageBinApprovalRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetStorageBin())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	signalReviewVoteWorkflowRequest := &reviewsv1.SignalReviewVoteWorkflowRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				Email:       cachedDetails.GetApprover().GetEmail(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	signalReviewVoteWorkflowReply, err := s.meteredReviewVoteWorkflow.SignalReviewVoteWorkflow(ctx, signalReviewVoteWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteWorkflowService/SignalReviewVoteWorkflow")
	}
	return &warehousingv1.DelegateStorageBinApprovalResponse{
		StorageBin: &warehousingv1.StorageBin{
			Manager: &identityv1.UserRef{
				Email: signalReviewVoteWorkflowReply.GetReviewVote().GetSubject().GetEmail(),
			},
			StorageBinId: req.GetStorageBinId(),
			ParentId:     req.GetStorageBin().GetParentId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallStorageBin recalls a storage bin.
func (s *StorageBinApprovalServer) RecallStorageBin(ctx context.Context, req *warehousingv1.RecallStorageBinRequest) (*warehousingv1.RecallStorageBinResponse, error) {
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "recall storage bin", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *identityv1.GetUserRequest, ...grpc.CallOption) (*identityv1.GetUserResponse, error){
		"primary":  s.userClient.GetUser,
		"fallback": s.userClientFallback.GetUser,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	getUserRequest := &identityv1.GetUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetDetails().GetApprover().GetUserId(),
	}
	getUserReply, err := route(ctx, getUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/GetUser")
	}
	browseWarehousesRequest := &warehousingv1.BrowseWarehousesRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.BrowseWarehousesRequest_Filter{
			Region: getUserReply.GetUser().GetPrimaryAddress().GetRegion(),
		},
	}
	browseWarehousesReply, err := browseWarehousesCall(ctx, browseWarehousesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/BrowseWarehouses")
	}
	step := &storageBinApprovalServerRecallStorageBinPreviewPreferenceStep{request: &customersv1.PreviewPreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.PreviewPreferenceRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: getUserReply.GetUser().GetContact().GetPhone()},
		},
	}}
	if err := s.runRecallStorageBinPreviewPreference(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	previewPreferenceReply := step.reply
	names := collectStorageBinNames(req.GetStorageBin(), nil)
	rejectTrackingEventRequest := newRejectTrackingEventRequest(withRejectTrackingEventContext(req.GetContext()), withRejectTrackingEventDetails(&shippingv1.RejectTrackingEventRequest_Details{
		Approver: &identityv1.UserRef{
			DisplayName: req.GetStorageBin().GetManager().GetDisplayName(),
		},
		Conditions: names,
	}))
	rejectTrackingEventReply, err := s.trackingEventApprovalClient.RejectTrackingEvent(ctx, rejectTrackingEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventApprovalService/RejectTrackingEvent")
	}
	_ = rejectTrackingEventReply
	return &warehousingv1.RecallStorageBinResponse{
		Outcome: &warehousingv1.RecallStorageBinResponse_Outcome{
			Status:    getUserReply.GetResult().GetStatus(),
			NextSteps: previewPreferenceReply.GetOutcome().GetNextSteps(),
		},
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{Attributes: getUserReply.GetUser().GetAttributes()}},
				}},
			}},
			StorageBinId: req.GetStorageBinId(),
			Status:       req.GetStorageBin().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: browseWarehousesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetStorageBinApprovalStatus returns a storage bin.
func (s *StorageBinApprovalServer) GetStorageBinApprovalStatus(ctx context.Context, req *warehousingv1.GetStorageBinApprovalStatusRequest) (*warehousingv1.GetStorageBinApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storage_bin", req.GetStorageBinId(), "get_storage_bin_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get storage bin approval status", "tenant", req.GetContext().GetTenantId())
	return &warehousingv1.GetStorageBinApprovalStatusResponse{
		StorageBin: &warehousingv1.StorageBin{StorageBinId: req.GetStorageBinId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentStorageBinApproval comments a storage bin.
func (s *StorageBinApprovalServer) CommentStorageBinApproval(ctx context.Context, req *warehousingv1.CommentStorageBinApprovalRequest) (*warehousingv1.CommentStorageBinApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStorageBin(ctx, req.GetContext().GetTenantId(), req.GetStorageBin()); err != nil {
		return nil, grpcx.Wrap(err, "save storage bin")
	}
	s.log.InfoContext(ctx, "comment storage bin approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorageBin().GetAudit().GetEtag())
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
	validatePickListRequest := &warehousingv1.ValidatePickListRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: lookupStorageBinReply.GetStorageBin().GetManager().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{Lines: req.GetStorageBin().GetLocation().GetLines()},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: lookupStorageBinReply.GetStorageBin().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &warehousingv1.ValidatePickListRequest_Details{Rules: names},
	}
	var validatePickListReply *warehousingv1.ValidatePickListResponse
	if err := pipeline.Run(ctx, "acme.warehousing.v1.PickListValidationService/ValidatePickList", func(ctx context.Context) error {
		answer, err := s.pickListValidationClient.ValidatePickList(ctx, validatePickListRequest)
		validatePickListReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/ValidatePickList")
	}
	return &warehousingv1.CommentStorageBinApprovalResponse{
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Manager: &identityv1.UserRef{
						UserId: lookupStorageBinReply.GetStorageBin().GetManager().GetUserId(),
					},
				}},
				Location: &commonv1.PostalAddress{
					CountryCode: validatePickListReply.GetPickList().GetBillingAddress().GetCountryCode(),
				},
			}},
			StorageBinId: req.GetStorageBinId(),
			Name:         req.GetStorageBin().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// storageBinApprovalServerSubmitStorageBinPreviewPickListStep carries the request of one step of SubmitStorageBin and,
// once it ran, its reply.
type storageBinApprovalServerSubmitStorageBinPreviewPickListStep struct {
	request *warehousingv1.PreviewPickListRequest
	reply   *warehousingv1.PreviewPickListResponse
}

func (s *StorageBinApprovalServer) runSubmitStorageBinPreviewPickList(ctx context.Context, step *storageBinApprovalServerSubmitStorageBinPreviewPickListStep) error {
	reply, err := s.pickListValidationClient.PreviewPickList(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// storageBinApprovalServerRecallStorageBinPreviewPreferenceStep carries the request of one step of RecallStorageBin
// and, once it ran, its reply.
type storageBinApprovalServerRecallStorageBinPreviewPreferenceStep struct {
	request *customersv1.PreviewPreferenceRequest
	reply   *customersv1.PreviewPreferenceResponse
}

func (s *StorageBinApprovalServer) runRecallStorageBinPreviewPreference(ctx context.Context, step *storageBinApprovalServerRecallStorageBinPreviewPreferenceStep) error {
	reply, err := s.preferenceValidationClient.PreviewPreference(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
