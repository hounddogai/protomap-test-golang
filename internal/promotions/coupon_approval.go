package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	admissionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/admissions/v1"
	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	driversv1 "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	timekeepingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/timekeeping/v1"
	workplacesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/workplaces/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
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

// CouponApprovalServer implements `acme.promotions.v1.CouponApprovalService`.
// Routes coupons through approval, with escalation and delegation.
type CouponApprovalServer struct {
	promotionsv1.UnimplementedCouponApprovalServiceServer
	store                         *Store
	log                           *slog.Logger
	clients                       *registry.Registry
	timesheetClient               timekeepingv1.TimesheetServiceClient
	conn                          *grpc.ClientConn
	buildingOnce                  sync.Once
	buildingCached                workplacesv1.BuildingServiceClient
	promotionApprovalClient       promotionsv1.PromotionApprovalServiceClient
	packingSlipWorkflowClient     fulfillmentv1.PackingSlipWorkflowServiceClient
	couponQueryClient             promotionsv1.CouponQueryServiceClient
	searchQueryValidationClient   searchv1.SearchQueryValidationServiceClient
	couponReportingClient         promotionsv1.CouponReportingServiceClient
	merchandisingRuleWorkflowPort merchandisingRuleWorkflowPort
}

// NewCouponApprovalServer wires a CouponApprovalServer.
func NewCouponApprovalServer(deps platform.Deps) *CouponApprovalServer {
	return &CouponApprovalServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.promotions.v1.CouponApprovalService"),
		clients:                       deps.Clients,
		timesheetClient:               timekeepingv1.NewTimesheetServiceClient(deps.Conn),
		conn:                          deps.Conn,
		promotionApprovalClient:       promotionsv1.NewPromotionApprovalServiceClient(deps.Conn),
		packingSlipWorkflowClient:     fulfillmentv1.NewPackingSlipWorkflowServiceClient(deps.Conn),
		couponQueryClient:             promotionsv1.NewCouponQueryServiceClient(deps.Conn),
		searchQueryValidationClient:   searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
		couponReportingClient:         promotionsv1.NewCouponReportingServiceClient(deps.Conn),
		merchandisingRuleWorkflowPort: newMerchandisingRuleWorkflowPort(deps),
	}
}

// SubmitCoupon submits a coupon.
func (s *CouponApprovalServer) SubmitCoupon(ctx context.Context, req *promotionsv1.SubmitCouponRequest) (*promotionsv1.SubmitCouponResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "submit coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	documentValidation, err := registry.Lookup[documentsv1.DocumentValidationServiceClient](s.clients, "acme.documents.v1.DocumentValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "phone", req.GetCoupon().GetContact().GetPhone())
	previewDocumentRequest := &documentsv1.PreviewDocumentRequest{
		Context: req.GetContext(),
		Details: &documentsv1.PreviewDocumentRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: tenancy.Value(ctx, "phone")},
		},
		Document: &documentsv1.Document{
			Counterparty: &documentsv1.DocumentFolderRef{DisplayName: req.GetCoupon().GetHolder().GetDisplayName()},
			Settlement: &documentsv1.Document_Card{Card: &documentsv1.DocumentCardSettlement{
				Last4: req.GetCoupon().GetCard().GetLast4(),
				Token: req.GetCoupon().GetCard().GetToken(),
			}},
		},
	}
	previewDocumentReply, err := documentValidation.PreviewDocument(ctx, previewDocumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/PreviewDocument")
	}
	return &promotionsv1.SubmitCouponResponse{
		Outcome: &promotionsv1.SubmitCouponResponse_Outcome{
			ReferenceId: previewDocumentReply.GetOutcome().GetReferenceId(),
		},
		Coupon: &promotionsv1.Coupon{
			CouponId: req.GetCouponId(),
			Labels:   req.GetCoupon().GetLabels(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveCoupon approves a coupon.
func (s *CouponApprovalServer) ApproveCoupon(ctx context.Context, req *promotionsv1.ApproveCouponRequest) (*promotionsv1.ApproveCouponResponse, error) {
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "approve coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	watchTimesheetsStream, err := s.timesheetClient.WatchTimesheets(ctx, &timekeepingv1.WatchTimesheetsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email:            req.GetDetails().GetApprover().GetEmail(),
			PreferredChannel: req.GetCoupon().GetContact().GetPreferredChannel(),
			Name:             req.GetCoupon().GetContact().GetName(),
			Phone:            req.GetCoupon().GetContact().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.timekeeping.v1.TimesheetService/WatchTimesheets")
	}
	for {
		received, err := watchTimesheetsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.timekeeping.v1.TimesheetService/WatchTimesheets")
		}
		_ = received
	}
	ctx = tenancy.With(ctx, "created_by", req.GetCoupon().GetAudit().GetCreatedBy())
	calculateDriverRequest := &driversv1.CalculateDriverRequest{
		Context: req.GetContext(),
		Driver: &driversv1.Driver{
			Audit: &commonv1.AuditInfo{CreatedBy: tenancy.Value(ctx, "created_by")},
		},
	}
	calculateDriverReply, err := grpcx.NewClient(s.conn, driversv1.NewDriverCalculationServiceClient).CalculateDriver(ctx, calculateDriverRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.drivers.v1.DriverCalculationService/CalculateDriver")
	}
	batchGetBuildingsRequest := &workplacesv1.BatchGetBuildingsRequest{Context: req.GetContext()}
	batchGetBuildingsReply, err := s.buildingLazy().BatchGetBuildings(ctx, batchGetBuildingsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.workplaces.v1.BuildingService/BatchGetBuildings")
	}
	backorderSync, err := registry.Lookup[ordersv1.BackorderSyncServiceClient](s.clients, "acme.orders.v1.BackorderSyncService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	pushBackorderRequest := &ordersv1.PushBackorderRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetCoupon().GetAudit().GetEtag(),
				UpdatedAt: calculateDriverReply.GetDriver().GetAudit().GetUpdatedAt(),
			},
			Origin: &ordersv1.Backorder_Customer{Customer: &customersv1.CustomerRef{DisplayName: req.GetCoupon().GetHolder().GetDisplayName()}},
		},
	}
	pushBackorderReply, err := backorderSync.PushBackorder(ctx, pushBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/PushBackorder")
	}
	_ = batchGetBuildingsReply
	return &promotionsv1.ApproveCouponResponse{
		Result: &commonv1.OperationResult{Status: calculateDriverReply.GetResult().GetStatus()},
		Coupon: &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{
				DisplayName: calculateDriverReply.GetDriver().GetRelationshipManager().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{
				Email: pushBackorderReply.GetBackorder().GetRequester().GetEmail(),
			},
			CouponId: req.GetCouponId(),
			Balance:  req.GetCoupon().GetBalance(),
		},
	}, nil
}

// RejectCoupon rejects a coupon.
func (s *CouponApprovalServer) RejectCoupon(ctx context.Context, req *promotionsv1.RejectCouponRequest) (*promotionsv1.RejectCouponResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "reject coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	listRedemptionConflictsRequest := &promotionsv1.ListRedemptionConflictsRequest{
		Context: req.GetContext(),
		Filter:  &promotionsv1.ListRedemptionConflictsRequest_Filter{Labels: req.GetCoupon().GetLabels()},
	}
	listRedemptionConflictsReply, err := listRedemptionConflictsCall(ctx, listRedemptionConflictsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionSyncService/ListRedemptionConflicts")
	}
	return &promotionsv1.RejectCouponResponse{
		Result: &commonv1.OperationResult{
			Message:       listRedemptionConflictsReply.GetResult().GetMessage(),
			CorrelationId: listRedemptionConflictsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Coupon:  &promotionsv1.Coupon{CouponId: req.GetCouponId()},
		Outcome: &promotionsv1.RejectCouponResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// EscalateCoupon escalates a coupon.
func (s *CouponApprovalServer) EscalateCoupon(ctx context.Context, req *promotionsv1.EscalateCouponRequest) (*promotionsv1.EscalateCouponResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateCoupon", "error", err)
		return nil, err
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "escalate coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	var holderName string
	switch choice := req.GetCoupon().GetFunding().(type) {
	case *promotionsv1.Coupon_Card:
		holderName = choice.Card.GetHolderName()
	case *promotionsv1.Coupon_Bank:
		holderName = choice.Bank.GetIban()
	case *promotionsv1.Coupon_ExternalAccountId:
		holderName = choice.ExternalAccountId
	}
	startPackingSlipWorkflowRequest := &fulfillmentv1.StartPackingSlipWorkflowRequest{
		Context: req.GetContext(),
		PackingSlip: &fulfillmentv1.PackingSlip{
			Counterparty: &fulfillmentv1.FulfillmentRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Settlement: &fulfillmentv1.PackingSlip_Card{Card: &fulfillmentv1.PackingSlipCardSettlement{HolderName: holderName}},
		},
	}
	startPackingSlipWorkflowReply := new(fulfillmentv1.StartPackingSlipWorkflowResponse)
	if err := s.conn.Invoke(ctx, fulfillmentPackingSlipWorkflowServiceStartPackingSlipWorkflowMethod, startPackingSlipWorkflowRequest, startPackingSlipWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipWorkflowService/StartPackingSlipWorkflow")
	}
	return &promotionsv1.EscalateCouponResponse{
		Outcome: &promotionsv1.EscalateCouponResponse_Outcome{
			Message: startPackingSlipWorkflowReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Status: startPackingSlipWorkflowReply.GetResult().GetStatus(),
		},
		Coupon: &promotionsv1.Coupon{
			CouponId: req.GetCouponId(),
			Contact:  req.GetCoupon().GetContact(),
		},
	}, nil
}

// ListPendingCoupons lists coupons a page at a time.
func (s *CouponApprovalServer) ListPendingCoupons(ctx context.Context, req *promotionsv1.ListPendingCouponsRequest) (*promotionsv1.ListPendingCouponsResponse, error) {
	s.log.InfoContext(ctx, "list pending coupons", "tenant", req.GetContext().GetTenantId())
	admission, ok := s.clients.Get("acme.admissions.v1.AdmissionService").(admissionsv1.AdmissionServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.admissions.v1.AdmissionService")
	}
	source := req.GetFilter().ProtoReflect()
	query := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("query"))).String()
	listAdmissionsRequest := &admissionsv1.ListAdmissionsRequest{
		Context: req.GetContext(),
		Filter: &admissionsv1.ListAdmissionsRequest_Filter{
			Query:   query,
			OwnerId: req.GetFilter().GetOwnerId(),
		},
		Page: &commonv1.PageRequest{
			PageToken: req.GetPage().GetPageToken(),
			PageSize:  req.GetPage().GetPageSize(),
		},
	}
	listAdmissionsReply, err := admission.ListAdmissions(ctx, listAdmissionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.admissions.v1.AdmissionService/ListAdmissions")
	}
	return &promotionsv1.ListPendingCouponsResponse{
		Result: &commonv1.OperationResult{
			Details: listAdmissionsReply.GetResult().GetDetails(),
			Status:  listAdmissionsReply.GetResult().GetStatus(),
		},
	}, nil
}

// DelegateCouponApproval delegates a coupon.
func (s *CouponApprovalServer) DelegateCouponApproval(ctx context.Context, req *promotionsv1.DelegateCouponApprovalRequest) (*promotionsv1.DelegateCouponApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DelegateCouponApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "delegate coupon approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	browseAuditEventsRequest := &auditv1.BrowseAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.BrowseAuditEventsRequest_Filter{
			Labels:       req.GetCoupon().GetLabels(),
			CreatedAfter: req.GetCoupon().GetOpenedAt(),
			OwnerId:      req.GetCoupon().GetHolder().GetPromotionId(),
		},
	}
	browseAuditEventsReply := new(auditv1.BrowseAuditEventsResponse)
	if err := grpcx.Invoke(ctx, s.conn, auditv1.AuditEventQueryService_BrowseAuditEvents_FullMethodName, browseAuditEventsRequest, browseAuditEventsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/BrowseAuditEvents")
	}
	snapshot, err := protojson.Marshal(req.GetCoupon())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullRedemptionsRequest := &promotionsv1.PullRedemptionsRequest{
		Context: req.GetContext(),
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		Details: &promotionsv1.PullRedemptionsRequest_Details{Payload: snapshot},
	}
	pullRedemptionsReply := new(promotionsv1.PullRedemptionsResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.promotions.v1.RedemptionSyncService/PullRedemptions", pullRedemptionsRequest, pullRedemptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionSyncService/PullRedemptions")
	}
	getPromotionApprovalStatusRequest := &promotionsv1.GetPromotionApprovalStatusRequest{}
	getPromotionApprovalStatusRequest.Context = req.GetContext()
	getPromotionApprovalStatusRequest.PromotionId = req.GetCoupon().GetHolder().GetPromotionId()
	getPromotionApprovalStatusReply, err := s.promotionApprovalClient.GetPromotionApprovalStatus(ctx, getPromotionApprovalStatusRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionApprovalService/GetPromotionApprovalStatus")
	}
	resumePackingSlipWorkflowRequest := buildResumePackingSlipWorkflowRequestForDelegateCouponApproval(req, getPromotionApprovalStatusReply)
	resumePackingSlipWorkflowReply, err := s.packingSlipWorkflowClient.ResumePackingSlipWorkflow(ctx, resumePackingSlipWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipWorkflowService/ResumePackingSlipWorkflow")
	}
	return &promotionsv1.DelegateCouponApprovalResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: browseAuditEventsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &promotionsv1.DelegateCouponApprovalResponse_Outcome{
			EffectiveAt: pullRedemptionsReply.GetOutcome().GetEffectiveAt(),
			Message:     pullRedemptionsReply.GetOutcome().GetMessage(),
		},
		Coupon: &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{
				PromotionId: getPromotionApprovalStatusReply.GetPromotion().GetPromotionId(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: resumePackingSlipWorkflowReply.GetPackingSlip().GetAudit().GetCreatedAt(),
			},
			CouponId: req.GetCouponId(),
			OpenedAt: req.GetCoupon().GetOpenedAt(),
		},
	}, nil
}

// RecallCoupon recalls a coupon.
func (s *CouponApprovalServer) RecallCoupon(ctx context.Context, req *promotionsv1.RecallCouponRequest) (*promotionsv1.RecallCouponResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallCoupon", "error", err)
		return nil, err
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "recall coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	step := &couponApprovalServerRecallCouponSuggestCouponsStep{request: &promotionsv1.SuggestCouponsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.SuggestCouponsRequest_Filter{
			Labels:       req.GetCoupon().GetLabels(),
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetCoupon().GetHolder().GetPromotionId(),
		},
	}}
	if err := s.runRecallCouponSuggestCoupons(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponQueryService/SuggestCoupons")
	}
	suggestCouponsReply := step.reply
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetApprover().GetEmail())
	packed, err := anypb.New(req.GetCoupon())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizeSearchQueryReply, err := s.searchQueryValidationClient.NormalizeSearchQuery(ctx, &searchv1.NormalizeSearchQueryRequest{
		Context: req.GetContext(),
		Details: &searchv1.NormalizeSearchQueryRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
		SearchQuery: &searchv1.SearchQuery{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/NormalizeSearchQuery")
	}
	return &promotionsv1.RecallCouponResponse{
		Outcome: &promotionsv1.RecallCouponResponse_Outcome{
			Message:   suggestCouponsReply.GetResult().GetMessage(),
			Status:    normalizeSearchQueryReply.GetOutcome().GetStatus(),
			NextSteps: normalizeSearchQueryReply.GetOutcome().GetNextSteps(),
		},
		Coupon: &promotionsv1.Coupon{
			CouponId:     req.GetCouponId(),
			CurrencyCode: req.GetCoupon().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetCouponApprovalStatus returns a coupon.
func (s *CouponApprovalServer) GetCouponApprovalStatus(ctx context.Context, req *promotionsv1.GetCouponApprovalStatusRequest) (*promotionsv1.GetCouponApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetCouponApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "get_coupon_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get coupon approval status", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	listCouponReportsRequest := &promotionsv1.ListCouponReportsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.ListCouponReportsRequest_Filter{
			OwnerId:      req.GetCouponId(),
			CreatedAfter: cloned.GetDeadline(),
			Labels:       baggage,
		},
	}
	listCouponReportsReply, err := grpcx.Call(ctx, s.couponReportingClient.ListCouponReports, listCouponReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponReportingService/ListCouponReports")
	}
	signalMerchandisingRuleWorkflowRequest := &merchandisingv1.SignalMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	signalMerchandisingRuleWorkflowReply, err := s.merchandisingRuleWorkflowPort.SignalMerchandisingRuleWorkflow(ctx, signalMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/SignalMerchandisingRuleWorkflow")
	}
	return &promotionsv1.GetCouponApprovalStatusResponse{
		Result: &commonv1.OperationResult{Status: listCouponReportsReply.GetResult().GetStatus()},
		Coupon: &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{
				DisplayName: signalMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetOwner().GetDisplayName(),
			},
			CouponId: req.GetCouponId(),
		},
	}, nil
}

// buildingLazy dials acme.workplaces.v1.BuildingService the first time a handler needs it.
func (s *CouponApprovalServer) buildingLazy() workplacesv1.BuildingServiceClient {
	s.buildingOnce.Do(func() { s.buildingCached = workplacesv1.NewBuildingServiceClient(s.conn) })
	return s.buildingCached
}

// couponApprovalServerRecallCouponSuggestCouponsStep carries the request of one step of RecallCoupon and, once it ran,
// its reply.
type couponApprovalServerRecallCouponSuggestCouponsStep struct {
	request *promotionsv1.SuggestCouponsRequest
	reply   *promotionsv1.SuggestCouponsResponse
}

func (s *CouponApprovalServer) runRecallCouponSuggestCoupons(ctx context.Context, step *couponApprovalServerRecallCouponSuggestCouponsStep) error {
	reply, err := s.couponQueryClient.SuggestCoupons(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
