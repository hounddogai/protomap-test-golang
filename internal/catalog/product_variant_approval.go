package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// ProductVariantApprovalServer implements `acme.catalog.v1.ProductVariantApprovalService`.
// Routes product variants through approval, with escalation and delegation.
type ProductVariantApprovalServer struct {
	catalogv1.UnimplementedProductVariantApprovalServiceServer
	store                        *Store
	log                          *slog.Logger
	brandReportingClient         catalogv1.BrandReportingServiceClient
	sessionValidationClient      identityv1.SessionValidationServiceClient
	conn                         *grpc.ClientConn
	carrierRateWorkflowOnce      sync.Once
	carrierRateWorkflowCached    shippingv1.CarrierRateWorkflowServiceClient
	pickListWorkflowClient       warehousingv1.PickListWorkflowServiceClient
	productWorkflowClient        catalogv1.ProductWorkflowServiceClient
	meteredGiftCardBatchApproval meteredGiftCardBatchApproval
	auditEventClient             auditv1.AuditEventServiceClient
}

// NewProductVariantApprovalServer wires a ProductVariantApprovalServer.
func NewProductVariantApprovalServer(deps platform.Deps) *ProductVariantApprovalServer {
	return &ProductVariantApprovalServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.catalog.v1.ProductVariantApprovalService"),
		brandReportingClient:         catalogv1.NewBrandReportingServiceClient(deps.Conn),
		sessionValidationClient:      identityv1.NewSessionValidationServiceClient(deps.Conn),
		conn:                         deps.Conn,
		pickListWorkflowClient:       warehousingv1.NewPickListWorkflowServiceClient(deps.Conn),
		productWorkflowClient:        catalogv1.NewProductWorkflowServiceClient(deps.Conn),
		meteredGiftCardBatchApproval: meteredGiftCardBatchApproval{GiftCardBatchApprovalServiceClient: giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		auditEventClient:             auditv1.NewAuditEventServiceClient(deps.Conn),
	}
}

// SubmitProductVariant submits a product variant.
func (s *ProductVariantApprovalServer) SubmitProductVariant(ctx context.Context, req *catalogv1.SubmitProductVariantRequest) (*catalogv1.SubmitProductVariantResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitProductVariant", "error", err)
		return nil, err
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "submit product variant", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetProductVariant().GetOwner().GetEmail())
	streamBrandMetricsStream, err := s.brandReportingClient.StreamBrandMetrics(ctx, &catalogv1.StreamBrandMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		Filter:     &catalogv1.StreamBrandMetricsRequest_Filter{Region: req.GetProductVariant().GetAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/StreamBrandMetrics")
	}
	for {
		received, err := streamBrandMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/StreamBrandMetrics")
		}
		_ = received
	}
	previewSessionRequest := buildPreviewSessionRequestForSubmitProductVariant(req)
	previewSessionReply, err := s.sessionValidationClient.PreviewSession(ctx, previewSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/PreviewSession")
	}
	return &catalogv1.SubmitProductVariantResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: previewSessionReply.GetResult().GetReferenceIds(),
			Status:       previewSessionReply.GetOutcome().GetStatus(),
		},
		ProductVariant: &catalogv1.ProductVariant{
			ProductVariantId: req.GetProductVariantId(),
			Capacity:         req.GetProductVariant().GetCapacity(),
		},
	}, nil
}

// ApproveProductVariant approves a product variant.
func (s *ProductVariantApprovalServer) ApproveProductVariant(ctx context.Context, req *catalogv1.ApproveProductVariantRequest) (*catalogv1.ApproveProductVariantResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveProductVariant", "error", err)
		return nil, err
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "approve product variant", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	startCarrierRateWorkflowRequest := &shippingv1.StartCarrierRateWorkflowRequest{
		Context: req.GetContext(),
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetProductVariant().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{Email: req.GetProductVariant().GetOwner().GetEmail()},
		},
	}
	startCarrierRateWorkflowReply, err := s.carrierRateWorkflowLazy().StartCarrierRateWorkflow(ctx, startCarrierRateWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateWorkflowService/StartCarrierRateWorkflow")
	}
	return &catalogv1.ApproveProductVariantResponse{
		Outcome: &catalogv1.ApproveProductVariantResponse_Outcome{
			Status:    startCarrierRateWorkflowReply.GetOutcome().GetStatus(),
			NextSteps: startCarrierRateWorkflowReply.GetOutcome().GetNextSteps(),
		},
		ProductVariant: &catalogv1.ProductVariant{
			ProductVariantId: req.GetProductVariantId(),
			Name:             req.GetProductVariant().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectProductVariant rejects a product variant.
func (s *ProductVariantApprovalServer) RejectProductVariant(ctx context.Context, req *catalogv1.RejectProductVariantRequest) (*catalogv1.RejectProductVariantResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "reject product variant", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	startPickListWorkflowReply, err := s.pickListWorkflowClient.StartPickListWorkflow(ctx, &warehousingv1.StartPickListWorkflowRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetProductVariant().GetAudit().GetCreatedAt(),
				Etag:      req.GetProductVariant().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetProductVariant().GetAddress().GetPostalCode(),
			},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListWorkflowService/StartPickListWorkflow")
	}
	return &catalogv1.RejectProductVariantResponse{
		Outcome: &catalogv1.RejectProductVariantResponse_Outcome{
			Message: startPickListWorkflowReply.GetResult().GetMessage(),
		},
		ProductVariant: &catalogv1.ProductVariant{
			Audit: &commonv1.AuditInfo{
				Etag: startPickListWorkflowReply.GetPickList().GetAudit().GetEtag(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Code:             req.GetProductVariant().GetCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingProductVariants lists product variants a page at a time.
func (s *ProductVariantApprovalServer) ListPendingProductVariants(ctx context.Context, req *catalogv1.ListPendingProductVariantsRequest) (*catalogv1.ListPendingProductVariantsResponse, error) {
	s.log.InfoContext(ctx, "list pending product variants", "tenant", req.GetContext().GetTenantId())
	countBrandTotalsRequest := &catalogv1.CountBrandTotalsRequest{
		Context: req.GetContext(),
		Page: &commonv1.PageRequest{
			PageSize:  req.GetPage().GetPageSize(),
			PageToken: req.GetPage().GetPageToken(),
		},
		Filter: &catalogv1.CountBrandTotalsRequest_Filter{
			Query:  req.GetFilter().GetQuery(),
			Region: req.GetFilter().GetRegion(),
		},
	}
	countBrandTotalsReply, err := countBrandTotalsCall(ctx, countBrandTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/CountBrandTotals")
	}
	return &catalogv1.ListPendingProductVariantsResponse{
		Result: &commonv1.OperationResult{Status: countBrandTotalsReply.GetResult().GetStatus()},
	}, nil
}

// RecallProductVariant recalls a product variant.
func (s *ProductVariantApprovalServer) RecallProductVariant(ctx context.Context, req *catalogv1.RecallProductVariantRequest) (*catalogv1.RecallProductVariantResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "recall product variant", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	var locality string
	switch choice := req.GetProductVariant().GetLocator().(type) {
	case *catalogv1.ProductVariant_Address:
		locality = choice.Address.GetLocality()
	case *catalogv1.ProductVariant_FacilityCode:
		locality = choice.FacilityCode
	}
	startPickListWorkflowRequest := &warehousingv1.StartPickListWorkflowRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Lines: []*warehousingv1.PickListLine{&warehousingv1.PickListLine{
				Description: req.GetProductVariant().GetComponents()["primary"].GetDescription(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetProductVariant().GetAddress().GetCountryCode(),
				Locality:    locality,
			},
		},
	}
	startPickListWorkflowReply, err := startPickListWorkflowCall(ctx, startPickListWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListWorkflowService/StartPickListWorkflow")
	}
	listProductWorkflowsRequest := &catalogv1.ListProductWorkflowsRequest{}
	listProductWorkflowsRequest.Context = req.GetContext()
	listProductWorkflowsRequest.Filter = &catalogv1.ListProductWorkflowsRequest_Filter{
		Region:       startPickListWorkflowReply.GetPickList().GetBillingAddress().GetRegion(),
		CreatedAfter: startPickListWorkflowReply.GetPickList().GetAudit().GetUpdatedAt(),
		OwnerId:      startPickListWorkflowReply.GetPickList().GetCounterparty().GetCustomerId(),
		Labels:       req.GetAnnotations(),
	}
	listProductWorkflowsReply, err := s.productWorkflowClient.ListProductWorkflows(ctx, listProductWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductWorkflowService/ListProductWorkflows")
	}
	var region string
	switch choice2 := req.GetProductVariant().GetLocator().(type) {
	case *catalogv1.ProductVariant_Address:
		region = choice2.Address.GetRegion()
	case *catalogv1.ProductVariant_FacilityCode:
		region = choice2.FacilityCode
	}
	delegateGiftCardBatchApprovalRequest := &giftcardsv1.DelegateGiftCardBatchApprovalRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{Region: region},
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	}
	delegateGiftCardBatchApprovalReply, err := s.meteredGiftCardBatchApproval.DelegateGiftCardBatchApproval(ctx, delegateGiftCardBatchApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval")
	}
	return &catalogv1.RecallProductVariantResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: startPickListWorkflowReply.GetResult().GetCorrelationId(),
			Status:        startPickListWorkflowReply.GetResult().GetStatus(),
			ReferenceIds:  delegateGiftCardBatchApprovalReply.GetResult().GetReferenceIds(),
		},
		Outcome: &catalogv1.RecallProductVariantResponse_Outcome{
			Message:     listProductWorkflowsReply.GetResult().GetMessage(),
			EffectiveAt: delegateGiftCardBatchApprovalReply.GetOutcome().GetEffectiveAt(),
		},
		ProductVariant: &catalogv1.ProductVariant{
			ProductVariantId: req.GetProductVariantId(),
			Name:             req.GetProductVariant().GetName(),
		},
	}, nil
}

// CommentProductVariantApproval comments a product variant.
func (s *ProductVariantApprovalServer) CommentProductVariantApproval(ctx context.Context, req *catalogv1.CommentProductVariantApprovalRequest) (*catalogv1.CommentProductVariantApprovalResponse, error) {
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "comment product variant approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetProductVariant())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	updateAuditEventReply, err := s.auditEventClient.UpdateAuditEvent(ctx, &auditv1.UpdateAuditEventRequest{
		Context: req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{
			Measurements: []*auditv1.AuditEventMeasurement{&auditv1.AuditEventMeasurement{Unit: req.GetProductVariant().GetDimensions().GetUnit()}},
			Detail:       packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/UpdateAuditEvent")
	}
	return &catalogv1.CommentProductVariantApprovalResponse{
		Outcome: &catalogv1.CommentProductVariantApprovalResponse_Outcome{Status: updateAuditEventReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Message: updateAuditEventReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		ProductVariant: &catalogv1.ProductVariant{
			ProductVariantId: req.GetProductVariantId(),
			Owner:            req.GetProductVariant().GetOwner(),
		},
	}, nil
}

// carrierRateWorkflowLazy dials acme.shipping.v1.CarrierRateWorkflowService the first time a handler needs it.
func (s *ProductVariantApprovalServer) carrierRateWorkflowLazy() shippingv1.CarrierRateWorkflowServiceClient {
	s.carrierRateWorkflowOnce.Do(func() { s.carrierRateWorkflowCached = shippingv1.NewCarrierRateWorkflowServiceClient(s.conn) })
	return s.carrierRateWorkflowCached
}
