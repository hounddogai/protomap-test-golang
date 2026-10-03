package warehousing

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// PickListWorkflowServer implements `acme.warehousing.v1.PickListWorkflowService`.
// Runs the long-lived workflow that takes a pick list from start to finish.
type PickListWorkflowServer struct {
	warehousingv1.UnimplementedPickListWorkflowServiceServer
	store                          *Store
	log                            *slog.Logger
	storageBinQueryClient          warehousingv1.StorageBinQueryServiceClient
	userClient                     identityv1.UserServiceClient
	preferenceCalculationClient    customersv1.PreferenceCalculationServiceClient
	conn                           *grpc.ClientConn
	warehouseWorkflowClient        warehousingv1.WarehouseWorkflowServiceClient
	recallRewardFn                 func(context.Context, *loyaltyv1.RecallRewardRequest, ...grpc.CallOption) (*loyaltyv1.RecallRewardResponse, error)
	giftCardBatchWorkflowClient    giftcardsv1.GiftCardBatchWorkflowServiceClient
	consentRecordCalculationClient customersv1.ConsentRecordCalculationServiceClient
	customerSegmentValidationPort  customerSegmentValidationPort
	pickListApprovalClient         warehousingv1.PickListApprovalServiceClient
	customerQueryClient            customersv1.CustomerQueryServiceClient
	storageBinApprovalClient       warehousingv1.StorageBinApprovalServiceClient
}

// NewPickListWorkflowServer wires a PickListWorkflowServer.
func NewPickListWorkflowServer(deps platform.Deps) PickListWorkflowServer {
	rewardApproval := loyaltyv1.NewRewardApprovalServiceClient(deps.Conn)
	return PickListWorkflowServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.warehousing.v1.PickListWorkflowService"),
		storageBinQueryClient:          warehousingv1.NewStorageBinQueryServiceClient(deps.Conn),
		userClient:                     identityv1.NewUserServiceClient(deps.Conn),
		preferenceCalculationClient:    customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		conn:                           deps.Conn,
		warehouseWorkflowClient:        warehousingv1.NewWarehouseWorkflowServiceClient(deps.Conn),
		recallRewardFn:                 rewardApproval.RecallReward,
		giftCardBatchWorkflowClient:    giftcardsv1.NewGiftCardBatchWorkflowServiceClient(deps.Conn),
		consentRecordCalculationClient: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		customerSegmentValidationPort:  newCustomerSegmentValidationPort(deps),
		pickListApprovalClient:         warehousingv1.NewPickListApprovalServiceClient(deps.Conn),
		customerQueryClient:            customersv1.NewCustomerQueryServiceClient(deps.Conn),
		storageBinApprovalClient:       warehousingv1.NewStorageBinApprovalServiceClient(deps.Conn),
	}
}

// StartPickListWorkflow starts a pick list.
func (s PickListWorkflowServer) StartPickListWorkflow(ctx context.Context, req *warehousingv1.StartPickListWorkflowRequest) (*warehousingv1.StartPickListWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StartPickListWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "start pick list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	var holder struct{ Region string }
	if record := req.GetPickList().GetBillingAddress(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	exportStorageBinsStream, err := s.storageBinQueryClient.ExportStorageBins(ctx, &warehousingv1.ExportStorageBinsRequest{
		Context: req.GetContext(),
		Filter:  &warehousingv1.ExportStorageBinsRequest_Filter{Region: holder.Region},
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
	metadata2 := make(map[string]string, len(req.GetPickList().GetMetadata()))
	for key, value := range req.GetPickList().GetMetadata() {
		metadata2[key] = value
	}
	summarizeStorageBinsRequest := &warehousingv1.SummarizeStorageBinsRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details:     &warehousingv1.SummarizeStorageBinsRequest_Details{Filters: metadata2},
	}
	summarizeStorageBinsReply, err := grpcx.Call(ctx, s.storageBinQueryClient.SummarizeStorageBins, summarizeStorageBinsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/SummarizeStorageBins")
	}
	return &warehousingv1.StartPickListWorkflowResponse{
		Outcome: &warehousingv1.StartPickListWorkflowResponse_Outcome{Message: summarizeStorageBinsReply.GetResult().GetMessage()},
		PickList: &warehousingv1.PickList{
			PickListId:   req.GetPickListId(),
			Counterparty: req.GetPickList().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvancePickListWorkflow advances a pick list.
func (s PickListWorkflowServer) AdvancePickListWorkflow(ctx context.Context, req *warehousingv1.AdvancePickListWorkflowRequest) (*warehousingv1.AdvancePickListWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "advance pick list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	searchUsersReply, err := s.userClient.SearchUsers(ctx, &identityv1.SearchUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.SearchUsersRequest_Filter{
			Region:       req.GetPickList().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPickList().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetPickList().GetPickListId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/SearchUsers")
	}
	exportStorageBinsStream, err := s.storageBinQueryClient.ExportStorageBins(ctx, &warehousingv1.ExportStorageBinsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.ExportStorageBinsRequest_Filter{
			Region:       req.GetPickList().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPickList().GetAudit().GetCreatedAt(),
			OwnerId:      searchUsersReply.GetResult().GetCorrelationId(),
			Labels:       searchUsersReply.GetResult().GetDetails(),
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
	return &warehousingv1.AdvancePickListWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: searchUsersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PickList: &warehousingv1.PickList{
			PickListId: req.GetPickListId(),
			Number:     req.GetPickList().GetNumber(),
		},
	}, nil
}

// CancelPickListWorkflow cancels a pick list.
func (s PickListWorkflowServer) CancelPickListWorkflow(ctx context.Context, req *warehousingv1.CancelPickListWorkflowRequest) (*warehousingv1.CancelPickListWorkflowResponse, error) {
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "cancel pick list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetPickList().GetCounterparty().GetDisplayName())
	ctx = tenancy.With(ctx, "email", req.GetPickList().GetCounterparty().GetEmail())
	escalateRewardRequest := &loyaltyv1.EscalateRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{DisplayName: tenancy.Value(ctx, "display_name")},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPickList().GetAudit().GetUpdatedAt()},
		},
		Details: &loyaltyv1.EscalateRewardRequest_Details{
			Approver: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
	}
	escalateRewardReply, err := escalateRewardCall(ctx, escalateRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/EscalateReward")
	}
	return &warehousingv1.CancelPickListWorkflowResponse{
		PickList: &warehousingv1.PickList{
			Lines: []*warehousingv1.PickListLine{&warehousingv1.PickListLine{
				Tax: &warehousingv1.PickListLineTax{
					Amount: escalateRewardReply.GetReward().GetDiscount().GetAmount(),
				},
			}},
			PickListId: req.GetPickListId(),
			Total:      req.GetPickList().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryPickListWorkflow retrys a pick list.
func (s PickListWorkflowServer) RetryPickListWorkflow(ctx context.Context, req *warehousingv1.RetryPickListWorkflowRequest) (*warehousingv1.RetryPickListWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryPickListWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "retry pick list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	resolveStorageBinsRequest := &warehousingv1.ResolveStorageBinsRequest{Context: req.GetContext()}
	resolveStorageBinsReply, err := resolveStorageBinsCall(ctx, resolveStorageBinsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/ResolveStorageBins")
	}
	comparePreferenceOptionsReply, err := s.preferenceCalculationClient.ComparePreferenceOptions(ctx, &customersv1.ComparePreferenceOptionsRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
			Audit: &commonv1.AuditInfo{Etag: req.GetPickList().GetAudit().GetEtag()},
		},
		Details: &customersv1.ComparePreferenceOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient:  req.GetPickList().GetBillingAddress().GetRecipient(),
				PostalCode: req.GetPickList().GetBillingAddress().GetPostalCode(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions")
	}
	ctx = tenancy.With(ctx, "customer_id", req.GetPickList().GetCounterparty().GetCustomerId())
	summarizeCustomersRequest := &customersv1.SummarizeCustomersRequest{
		Context:     req.GetContext(),
		CustomerId:  tenancy.Value(ctx, "customer_id"),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
	}
	summarizeCustomersReply, err := grpcx.NewClient(s.conn, customersv1.NewCustomerQueryServiceClient).SummarizeCustomers(ctx, summarizeCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SummarizeCustomers")
	}
	signalWarehouseWorkflowReply, err := s.warehouseWorkflowClient.SignalWarehouseWorkflow(ctx, &warehousingv1.SignalWarehouseWorkflowRequest{
		Context: req.GetContext(),
		Details: &warehousingv1.SignalWarehouseWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
		Warehouse: &warehousingv1.Warehouse{
			Locator: &warehousingv1.Warehouse_Address{Address: &commonv1.PostalAddress{Region: req.GetPickList().GetBillingAddress().GetRegion()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseWorkflowService/SignalWarehouseWorkflow")
	}
	return &warehousingv1.RetryPickListWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:  resolveStorageBinsReply.GetResult().GetStatus(),
			Message: summarizeCustomersReply.GetResult().GetMessage(),
		},
		Outcome: &warehousingv1.RetryPickListWorkflowResponse_Outcome{Message: resolveStorageBinsReply.GetResult().GetMessage()},
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: comparePreferenceOptionsReply.GetPreference().GetAudit().GetUpdatedBy(),
			},
			Lines: []*warehousingv1.PickListLine{&warehousingv1.PickListLine{
				Description: signalWarehouseWorkflowReply.GetWarehouse().GetComponents()["primary"].GetDescription(),
			}},
			PickListId: req.GetPickListId(),
			IssuedAt:   req.GetPickList().GetIssuedAt(),
		},
	}, nil
}

// ResumePickListWorkflow resumes a pick list.
func (s PickListWorkflowServer) ResumePickListWorkflow(ctx context.Context, req *warehousingv1.ResumePickListWorkflowRequest) (*warehousingv1.ResumePickListWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumePickListWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "resume pick list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	recallRewardRequest := &loyaltyv1.RecallRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPickList().GetAudit().GetEtag(),
				UpdatedAt: req.GetPickList().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{Email: req.GetPickList().GetCounterparty().GetEmail()},
		},
	}
	recallRewardReply, err := s.recallRewardFn(ctx, recallRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/RecallReward")
	}
	advanceGiftCardBatchWorkflowRequest := buildAdvanceGiftCardBatchWorkflowRequestForResumePickListWorkflow(req)
	advanceGiftCardBatchWorkflowReply, err := s.giftCardBatchWorkflowClient.AdvanceGiftCardBatchWorkflow(ctx, advanceGiftCardBatchWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/AdvanceGiftCardBatchWorkflow")
	}
	calculateConsentRecordReply, err := s.consentRecordCalculationClient.CalculateConsentRecord(ctx, &customersv1.CalculateConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.CalculateConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: req.GetPickList().GetBillingAddress().GetLocality(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CalculateConsentRecord")
	}
	checkCustomerSegmentEligibilityRequest := &customersv1.CheckCustomerSegmentEligibilityRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				Etag:      recallRewardReply.GetReward().GetAudit().GetEtag(),
				CreatedAt: advanceGiftCardBatchWorkflowReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
			},
		},
	}
	checkCustomerSegmentEligibilityReply, err := s.customerSegmentValidationPort.CheckCustomerSegmentEligibility(ctx, checkCustomerSegmentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/CheckCustomerSegmentEligibility")
	}
	_ = calculateConsentRecordReply
	_ = checkCustomerSegmentEligibilityReply
	return &warehousingv1.ResumePickListWorkflowResponse{
		Outcome: &warehousingv1.ResumePickListWorkflowResponse_Outcome{
			Message: recallRewardReply.GetOutcome().GetMessage(),
			Status:  advanceGiftCardBatchWorkflowReply.GetResult().GetStatus(),
		},
		PickList: &warehousingv1.PickList{
			Lines: []*warehousingv1.PickListLine{&warehousingv1.PickListLine{
				Amount: recallRewardReply.GetReward().GetDiscount().GetAmount(),
			}},
			PickListId:     req.GetPickListId(),
			BillingAddress: req.GetPickList().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectPickListWorkflow returns a pick list.
func (s PickListWorkflowServer) InspectPickListWorkflow(ctx context.Context, req *warehousingv1.InspectPickListWorkflowRequest) (*warehousingv1.InspectPickListWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "pick_list", req.GetPickListId(), "inspect_pick_list_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect pick list workflow", "tenant", req.GetContext().GetTenantId())
	watchUsersStream, err := s.userClient.WatchUsers(ctx, &identityv1.WatchUsersRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter:     &identityv1.WatchUsersRequest_Filter{OwnerId: req.GetPickListId()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/WatchUsers")
	}
	for {
		received, err := watchUsersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/WatchUsers")
		}
		_ = received
	}
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	listPendingPickListsRequest := &warehousingv1.ListPendingPickListsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.ListPendingPickListsRequest_Filter{
			OwnerId:      req.GetPickListId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       baggage,
		},
	}
	listPendingPickListsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*warehousingv1.ListPendingPickListsResponse, error) {
		return s.pickListApprovalClient.ListPendingPickLists(ctx, listPendingPickListsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListApprovalService/ListPendingPickLists")
	}
	return &warehousingv1.InspectPickListWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  listPendingPickListsReply.GetResult().GetReferenceIds(),
			CorrelationId: listPendingPickListsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PickList: &warehousingv1.PickList{PickListId: req.GetPickListId()},
	}, nil
}

// ListPickListWorkflows lists pick lists a page at a time.
func (s PickListWorkflowServer) ListPickListWorkflows(ctx context.Context, req *warehousingv1.ListPickListWorkflowsRequest) (*warehousingv1.ListPickListWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pick list workflows", "tenant", req.GetContext().GetTenantId())
	return &warehousingv1.ListPickListWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalPickListWorkflow signals a pick list.
func (s PickListWorkflowServer) SignalPickListWorkflow(ctx context.Context, req *warehousingv1.SignalPickListWorkflowRequest) (*warehousingv1.SignalPickListWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "signal pick list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	jobs := make(chan *warehousingv1.PickListLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.customerQueryClient.CountCustomers(workCtx, &customersv1.CountCustomersRequest{
					Context: req.GetContext(),
					Filter: &customersv1.CountCustomersRequest_Filter{
						OwnerId:      line.GetLineId(),
						Region:       req.GetPickList().GetBillingAddress().GetRegion(),
						Labels:       line.GetAttributes(),
						CreatedAfter: req.GetPickList().GetAudit().GetCreatedAt(),
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPickList().GetLines() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/CountCustomers")
	}
	cloned := proto.Clone(req.GetPickList()).(*warehousingv1.PickList)
	cloned2 := proto.Clone(req.GetPickList()).(*warehousingv1.PickList)
	listPendingStorageBinsRequest := &warehousingv1.ListPendingStorageBinsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.ListPendingStorageBinsRequest_Filter{
			Region:       req.GetPickList().GetBillingAddress().GetRegion(),
			CreatedAfter: cloned.GetDueAt(),
			OwnerId:      cloned2.GetCounterparty().GetCustomerId(),
		},
	}
	listPendingStorageBinsReply, err := grpcx.Call(ctx, s.storageBinApprovalClient.ListPendingStorageBins, listPendingStorageBinsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinApprovalService/ListPendingStorageBins")
	}
	return &warehousingv1.SignalPickListWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: listPendingStorageBinsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PickList: &warehousingv1.PickList{
			PickListId: req.GetPickListId(),
			Metadata:   req.GetPickList().GetMetadata(),
		},
	}, nil
}
