package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	billingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/billing/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ProductVariantWorkflowServer implements `acme.catalog.v1.ProductVariantWorkflowService`.
// Runs the long-lived workflow that takes a product variant from start to finish.
type ProductVariantWorkflowServer struct {
	catalogv1.UnimplementedProductVariantWorkflowServiceServer
	store                             *Store
	log                               *slog.Logger
	categoryApprovalClient            catalogv1.CategoryApprovalServiceClient
	bus                               *eventbus.Bus
	scheduleCategoryExpiryFn          func(context.Context, *catalogv1.ScheduleCategoryExpiryRequest, ...grpc.CallOption) (*catalogv1.ScheduleCategoryExpiryResponse, error)
	riskAssessmentWorkflowClient      fraudv1.RiskAssessmentWorkflowServiceClient
	giftCardTransactionWorkflowClient giftcardsv1.GiftCardTransactionWorkflowServiceClient
	recallProductFn                   func(context.Context, *catalogv1.RecallProductRequest, ...grpc.CallOption) (*catalogv1.RecallProductResponse, error)
	brandLifecycleClient              catalogv1.BrandLifecycleServiceClient
	productApprovalClient             catalogv1.ProductApprovalServiceClient
	promotionReportingClient          promotionsv1.PromotionReportingServiceClient
	paymentInstrumentLifecyclePort    paymentInstrumentLifecyclePort
	conn                              *grpc.ClientConn
	productApprovalOnce               sync.Once
	productApprovalCached             catalogv1.ProductApprovalServiceClient
	billingAccountQueryClient         billingv1.BillingAccountQueryServiceClient
}

// NewProductVariantWorkflowServer wires a ProductVariantWorkflowServer.
func NewProductVariantWorkflowServer(deps platform.Deps) *ProductVariantWorkflowServer {
	categoryLifecycle := catalogv1.NewCategoryLifecycleServiceClient(deps.Conn)
	productApproval := catalogv1.NewProductApprovalServiceClient(deps.Conn)
	return &ProductVariantWorkflowServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.catalog.v1.ProductVariantWorkflowService"),
		categoryApprovalClient:            catalogv1.NewCategoryApprovalServiceClient(deps.Conn),
		bus:                               deps.Bus,
		scheduleCategoryExpiryFn:          categoryLifecycle.ScheduleCategoryExpiry,
		riskAssessmentWorkflowClient:      fraudv1.NewRiskAssessmentWorkflowServiceClient(deps.Conn),
		giftCardTransactionWorkflowClient: giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn),
		recallProductFn:                   productApproval.RecallProduct,
		brandLifecycleClient:              catalogv1.NewBrandLifecycleServiceClient(deps.Conn),
		productApprovalClient:             catalogv1.NewProductApprovalServiceClient(deps.Conn),
		promotionReportingClient:          promotionsv1.NewPromotionReportingServiceClient(deps.Conn),
		paymentInstrumentLifecyclePort:    newPaymentInstrumentLifecyclePort(deps),
		conn:                              deps.Conn,
		billingAccountQueryClient:         billingv1.NewBillingAccountQueryServiceClient(deps.Conn),
	}
}

// StartProductVariantWorkflow starts a product variant.
func (s *ProductVariantWorkflowServer) StartProductVariantWorkflow(ctx context.Context, req *catalogv1.StartProductVariantWorkflowRequest) (*catalogv1.StartProductVariantWorkflowResponse, error) {
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "start product variant workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	approveCategoryRequest := &catalogv1.ApproveCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Location: &commonv1.PostalAddress{
						CountryCode: req.GetProductVariant().GetAddress().GetCountryCode(),
					},
					Manager: &identityv1.UserRef{
						DisplayName: req.GetProductVariant().GetOwner().GetDisplayName(),
						UserId:      req.GetProductVariant().GetOwner().GetUserId(),
					},
				}},
			}},
		},
	}
	approveCategoryReply, err := grpcx.Call(ctx, s.categoryApprovalClient.ApproveCategory, approveCategoryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryApprovalService/ApproveCategory")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "catalog.start_product_variant_workflow.recall_product",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.approver.display_name": approveCategoryReply.GetCategory().GetManager().GetDisplayName(),
			"product.address.recipient":     approveCategoryReply.GetCategory().GetLocation().GetRecipient(),
			"context.tenant_id":             req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	scheduleCategoryExpiryRequest := &catalogv1.ScheduleCategoryExpiryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{Etag: req.GetProductVariant().GetAudit().GetEtag()},
				}},
			}},
		},
	}
	scheduleCategoryExpiryReply, err := s.scheduleCategoryExpiryFn(ctx, scheduleCategoryExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryLifecycleService/ScheduleCategoryExpiry")
	}
	return &catalogv1.StartProductVariantWorkflowResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: approveCategoryReply.GetCategory().GetAudit().GetUpdatedBy(),
				CreatedAt: scheduleCategoryExpiryReply.GetCategory().GetAudit().GetCreatedAt(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Owner:            req.GetProductVariant().GetOwner(),
		},
		Result: &commonv1.OperationResult{
			Details: scheduleCategoryExpiryReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// AdvanceProductVariantWorkflow advances a product variant.
func (s *ProductVariantWorkflowServer) AdvanceProductVariantWorkflow(ctx context.Context, req *catalogv1.AdvanceProductVariantWorkflowRequest) (*catalogv1.AdvanceProductVariantWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "advance product variant workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*catalogv1.AdvanceProductVariantWorkflowRequest_Details)
	signalRiskAssessmentWorkflowRequest := &fraudv1.SignalRiskAssessmentWorkflowRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
		Details: &fraudv1.SignalRiskAssessmentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: cloned.GetNotify().GetEmail(),
				Name:  req.GetDetails().GetNotify().GetName(),
			},
		},
	}
	signalRiskAssessmentWorkflowReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*fraudv1.SignalRiskAssessmentWorkflowResponse, error) {
		return s.riskAssessmentWorkflowClient.SignalRiskAssessmentWorkflow(ctx, signalRiskAssessmentWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentWorkflowService/SignalRiskAssessmentWorkflow")
	}
	return &catalogv1.AdvanceProductVariantWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: signalRiskAssessmentWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &catalogv1.AdvanceProductVariantWorkflowResponse_Outcome{
			ReferenceId: signalRiskAssessmentWorkflowReply.GetOutcome().GetReferenceId(),
		},
		ProductVariant: &catalogv1.ProductVariant{
			ProductVariantId: req.GetProductVariantId(),
			Specs:            req.GetProductVariant().GetSpecs(),
		},
	}, nil
}

// CancelProductVariantWorkflow cancels a product variant.
func (s *ProductVariantWorkflowServer) CancelProductVariantWorkflow(ctx context.Context, req *catalogv1.CancelProductVariantWorkflowRequest) (*catalogv1.CancelProductVariantWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "cancel product variant workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	cancelGiftCardTransactionWorkflowReply, err := s.giftCardTransactionWorkflowClient.CancelGiftCardTransactionWorkflow(ctx, &giftcardsv1.CancelGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CancelGiftCardTransactionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNotify().GetEmail(),
				Phone: req.GetDetails().GetNotify().GetPhone(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/CancelGiftCardTransactionWorkflow")
	}
	var countryCode string
	switch choice := req.GetProductVariant().GetLocator().(type) {
	case *catalogv1.ProductVariant_Address:
		countryCode = choice.Address.GetCountryCode()
	case *catalogv1.ProductVariant_FacilityCode:
		countryCode = choice.FacilityCode
	}
	recallProductRequest := &catalogv1.RecallProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Dimensions: &catalogv1.ProductDimensions{
				Height: req.GetProductVariant().GetDimensions().GetHeight(),
				Length: req.GetProductVariant().GetDimensions().GetLength(),
			},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				PostalCode:  req.GetProductVariant().GetAddress().GetPostalCode(),
				CountryCode: countryCode,
			}},
		},
	}
	recallProductReply, err := s.recallProductFn(ctx, recallProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductApprovalService/RecallProduct")
	}
	return &catalogv1.CancelProductVariantWorkflowResponse{
		Outcome: &catalogv1.CancelProductVariantWorkflowResponse_Outcome{
			EffectiveAt: cancelGiftCardTransactionWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		ProductVariant: &catalogv1.ProductVariant{
			Tags: recallProductReply.GetProduct().GetTags(),
			Dimensions: &catalogv1.ProductVariantDimensions{
				Weight: recallProductReply.GetProduct().GetDimensions().GetWeight(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Status:           req.GetProductVariant().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryProductVariantWorkflow retrys a product variant.
func (s *ProductVariantWorkflowServer) RetryProductVariantWorkflow(ctx context.Context, req *catalogv1.RetryProductVariantWorkflowRequest) (*catalogv1.RetryProductVariantWorkflowResponse, error) {
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "retry product variant workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	scheduleBrandExpiryReply, err := s.brandLifecycleClient.ScheduleBrandExpiry(ctx, &catalogv1.ScheduleBrandExpiryRequest{
		Context: req.GetContext(),
		Brand: &catalogv1.Brand{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Name:             req.GetDetails().GetNotify().GetName(),
			},
			RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandLifecycleService/ScheduleBrandExpiry")
	}
	rejectProductReply, err := s.productApprovalClient.RejectProduct(ctx, &catalogv1.RejectProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				Locality: req.GetProductVariant().GetAddress().GetLocality(),
				Lines:    scheduleBrandExpiryReply.GetBrand().GetPrimaryAddress().GetLines(),
			}},
			Owner: &identityv1.UserRef{
				DisplayName: scheduleBrandExpiryReply.GetBrand().GetRelationshipManager().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductApprovalService/RejectProduct")
	}
	return &catalogv1.RetryProductVariantWorkflowResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				DisplayName: scheduleBrandExpiryReply.GetBrand().GetRelationshipManager().GetDisplayName(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Code:             req.GetProductVariant().GetCode(),
		},
		Outcome: &catalogv1.RetryProductVariantWorkflowResponse_Outcome{
			EffectiveAt: rejectProductReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseProductVariantWorkflow pauses a product variant.
func (s *ProductVariantWorkflowServer) PauseProductVariantWorkflow(ctx context.Context, req *catalogv1.PauseProductVariantWorkflowRequest) (*catalogv1.PauseProductVariantWorkflowResponse, error) {
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "pause product variant workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := productVariantWorkflowServerCommentProductApprovalSteps[mode]
	commentProductApprovalRequest := &catalogv1.CommentProductApprovalRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetProductVariant().GetOwner().GetDisplayName(),
			},
		},
	}
	commentProductApprovalReply, err := run(s, ctx, commentProductApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductApprovalService/CommentProductApproval")
	}
	cloned := proto.Clone(req.GetDetails()).(*catalogv1.PauseProductVariantWorkflowRequest_Details)
	renewProductVariantRequest := &catalogv1.RenewProductVariantRequest{
		Context:          req.GetContext(),
		ProductVariantId: req.GetProductVariantId(),
		Details: &catalogv1.RenewProductVariantRequest_Details{
			NewOwner: &identityv1.UserRef{
				UserId: commentProductApprovalReply.GetProduct().GetOwner().GetUserId(),
			},
			NotifyContact: &commonv1.ContactInfo{
				Email:            req.GetProductVariant().GetOwner().GetEmail(),
				Phone:            cloned.GetNotify().GetPhone(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	renewProductVariantReply, err := renewProductVariantCall(ctx, renewProductVariantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantLifecycleService/RenewProductVariant")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "catalog.pause_product_variant_workflow.recall_product",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"product.audit.updated_by":    renewProductVariantReply.GetProductVariant().GetAudit().GetUpdatedBy(),
			"product.address.postal_code": req.GetProductVariant().GetAddress().GetPostalCode(),
			"context.tenant_id":           req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &catalogv1.PauseProductVariantWorkflowResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Code:             commentProductApprovalReply.GetProduct().GetCode(),
			ProductVariantId: req.GetProductVariantId(),
			Capacity:         req.GetProductVariant().GetCapacity(),
		},
		Outcome: &catalogv1.PauseProductVariantWorkflowResponse_Outcome{
			EffectiveAt: renewProductVariantReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeProductVariantWorkflow resumes a product variant.
func (s *ProductVariantWorkflowServer) ResumeProductVariantWorkflow(ctx context.Context, req *catalogv1.ResumeProductVariantWorkflowRequest) (*catalogv1.ResumeProductVariantWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeProductVariantWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "resume product variant workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	streamPromotionMetricsStream, err := s.promotionReportingClient.StreamPromotionMetrics(ctx, &promotionsv1.StreamPromotionMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:             req.GetDetails().GetNotify().GetName(),
			PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
		},
		Filter: &promotionsv1.StreamPromotionMetricsRequest_Filter{Region: req.GetProductVariant().GetAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/StreamPromotionMetrics")
	}
	for {
		received, err := streamPromotionMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/StreamPromotionMetrics")
		}
		_ = received
	}
	return &catalogv1.ResumeProductVariantWorkflowResponse{
		ProductVariant: &catalogv1.ProductVariant{
			ProductVariantId: req.GetProductVariantId(),
			Name:             req.GetProductVariant().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectProductVariantWorkflow returns a product variant.
func (s *ProductVariantWorkflowServer) InspectProductVariantWorkflow(ctx context.Context, req *catalogv1.InspectProductVariantWorkflowRequest) (*catalogv1.InspectProductVariantWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "inspect_product_variant_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect product variant workflow", "tenant", req.GetContext().GetTenantId())
	recallProductRequest := &catalogv1.RecallProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner:      &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
			Components: map[string]*catalogv1.ProductComponent{"primary": &catalogv1.ProductComponent{ComponentId: req.GetProductVariantId()}},
		},
		Details: &catalogv1.RecallProductRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	type recallProductResult struct {
		reply *catalogv1.RecallProductResponse
		err   error
	}
	recallProductResults := make(chan recallProductResult, 1)
	go func() {
		reply, err := s.productApprovalClient.RecallProduct(ctx, recallProductRequest)
		recallProductResults <- recallProductResult{reply, err}
	}()
	var recallProductReply *catalogv1.RecallProductResponse
	select {
	case result := <-recallProductResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.catalog.v1.ProductApprovalService/RecallProduct")
		}
		recallProductReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	schedulePaymentInstrumentExpiryRequest := &paymentsv1.SchedulePaymentInstrumentExpiryRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder: &customersv1.CustomerRef{
				DisplayName: recallProductReply.GetProduct().GetOwner().GetDisplayName(),
				Email:       recallProductReply.GetProduct().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: recallProductReply.GetProduct().GetAudit().GetCreatedBy(),
			},
		},
	}
	schedulePaymentInstrumentExpiryReply, err := s.paymentInstrumentLifecyclePort.SchedulePaymentInstrumentExpiry(ctx, schedulePaymentInstrumentExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentLifecycleService/SchedulePaymentInstrumentExpiry")
	}
	return &catalogv1.InspectProductVariantWorkflowResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Audit: &commonv1.AuditInfo{
				CreatedAt: recallProductReply.GetProduct().GetAudit().GetCreatedAt(),
			},
			ProductVariantId: req.GetProductVariantId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: schedulePaymentInstrumentExpiryReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ListProductVariantWorkflows lists product variants a page at a time.
func (s *ProductVariantWorkflowServer) ListProductVariantWorkflows(ctx context.Context, req *catalogv1.ListProductVariantWorkflowsRequest) (*catalogv1.ListProductVariantWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list product variant workflows", "tenant", req.GetContext().GetTenantId())
	return &catalogv1.ListProductVariantWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalProductVariantWorkflow signals a product variant.
func (s *ProductVariantWorkflowServer) SignalProductVariantWorkflow(ctx context.Context, req *catalogv1.SignalProductVariantWorkflowRequest) (*catalogv1.SignalProductVariantWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "signal product variant workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	scheduleAttributeReportRequest := &catalogv1.ScheduleAttributeReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
	}
	scheduleAttributeReportReply, err := grpcx.NewClient(s.conn, catalogv1.NewAttributeReportingServiceClient).ScheduleAttributeReport(ctx, scheduleAttributeReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeReportingService/ScheduleAttributeReport")
	}
	recallProductRequest := &catalogv1.RecallProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetProductVariant().GetAudit().GetUpdatedBy(),
			},
			Dimensions: &catalogv1.ProductDimensions{Height: req.GetProductVariant().GetDimensions().GetHeight()},
		},
		Details: &catalogv1.RecallProductRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  req.GetProductVariant().GetOwner().GetEmail(),
				UserId: req.GetProductVariant().GetOwner().GetUserId(),
			},
		},
	}
	recallProductReply, err := s.productApprovalLazy().RecallProduct(ctx, recallProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductApprovalService/RecallProduct")
	}
	var region string
	switch choice := req.GetProductVariant().GetLocator().(type) {
	case *catalogv1.ProductVariant_Address:
		region = choice.Address.GetRegion()
	case *catalogv1.ProductVariant_FacilityCode:
		region = choice.FacilityCode
	}
	browseBillingAccountsRequest := newBrowseBillingAccountsRequest(withBrowseBillingAccountsContext(req.GetContext()), withBrowseBillingAccountsFilter(&billingv1.BrowseBillingAccountsRequest_Filter{
		Region:       region,
		CreatedAfter: req.GetProductVariant().GetAudit().GetCreatedAt(),
		OwnerId:      req.GetProductVariant().GetComponents()["primary"].GetComponentId(),
		Labels:       recallProductReply.GetResult().GetDetails(),
	}))
	browseBillingAccountsReply, err := s.billingAccountQueryClient.BrowseBillingAccounts(ctx, browseBillingAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.billing.v1.BillingAccountQueryService/BrowseBillingAccounts")
	}
	delegateCategoryApprovalRequest := &catalogv1.DelegateCategoryApprovalRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Location: &commonv1.PostalAddress{
						Region:    recallProductReply.GetProduct().GetAddress().GetRegion(),
						Recipient: req.GetProductVariant().GetAddress().GetRecipient(),
					},
					Manager: &identityv1.UserRef{UserId: req.GetProductVariant().GetOwner().GetUserId()},
				}},
			}},
		},
	}
	delegateCategoryApprovalReply, err := catalogv1.CategoryApprovalServiceClient.DelegateCategoryApproval(s.categoryApprovalClient, ctx, delegateCategoryApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryApprovalService/DelegateCategoryApproval")
	}
	return &catalogv1.SignalProductVariantWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: scheduleAttributeReportReply.GetResult().GetCorrelationId(),
			ReferenceIds:  browseBillingAccountsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		ProductVariant: &catalogv1.ProductVariant{
			Owner:    recallProductReply.GetProduct().GetOwner(),
			Capacity: recallProductReply.GetProduct().GetCapacity(),
			Audit: &commonv1.AuditInfo{
				CreatedAt: delegateCategoryApprovalReply.GetCategory().GetAudit().GetCreatedAt(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Status:           req.GetProductVariant().GetStatus(),
		},
		Outcome: &catalogv1.SignalProductVariantWorkflowResponse_Outcome{
			ReferenceId: delegateCategoryApprovalReply.GetOutcome().GetReferenceId(),
		},
	}, nil
}

// productVariantWorkflowServerCommentProductApprovalSteps are the ways to run
// acme.catalog.v1.ProductApprovalService/CommentProductApproval, by name.
var productVariantWorkflowServerCommentProductApprovalSteps = map[string]func(*ProductVariantWorkflowServer, context.Context, *catalogv1.CommentProductApprovalRequest) (*catalogv1.CommentProductApprovalResponse, error){
	"default": (*ProductVariantWorkflowServer).commentProductApprovalStep,
	"audited": (*ProductVariantWorkflowServer).commentProductApprovalAuditedStep,
}

func (s *ProductVariantWorkflowServer) commentProductApprovalStep(ctx context.Context, request *catalogv1.CommentProductApprovalRequest) (*catalogv1.CommentProductApprovalResponse, error) {
	return s.productApprovalClient.CommentProductApproval(ctx, request)
}

func (s *ProductVariantWorkflowServer) commentProductApprovalAuditedStep(ctx context.Context, request *catalogv1.CommentProductApprovalRequest) (*catalogv1.CommentProductApprovalResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.catalog.v1.ProductApprovalService/CommentProductApproval")
	return s.productApprovalClient.CommentProductApproval(ctx, request)
}

// productApprovalLazy dials acme.catalog.v1.ProductApprovalService the first time a handler needs it.
func (s *ProductVariantWorkflowServer) productApprovalLazy() catalogv1.ProductApprovalServiceClient {
	s.productApprovalOnce.Do(func() { s.productApprovalCached = catalogv1.NewProductApprovalServiceClient(s.conn) })
	return s.productApprovalCached
}
