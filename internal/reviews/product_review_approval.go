package reviews

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// ProductReviewApprovalServer implements `acme.reviews.v1.ProductReviewApprovalService`.
// Routes product reviews through approval, with escalation and delegation.
type ProductReviewApprovalServer struct {
	reviewsv1.UnimplementedProductReviewApprovalServiceServer
	store                              *Store
	log                                *slog.Logger
	productReviewQueryClient           reviewsv1.ProductReviewQueryServiceClient
	giftCardBatchApprovalClient        giftcardsv1.GiftCardBatchApprovalServiceClient
	clients                            *registry.Registry
	productReviewClient                reviewsv1.ProductReviewServiceClient
	householdValidationClient          customersv1.HouseholdValidationServiceClient
	accessPolicyQueryPort              accessPolicyQueryPort
	customerCalculationPort            customerCalculationPort
	sellerApprovalPort                 sellerApprovalPort
	reviewModerationQueryClient        reviewsv1.ReviewModerationQueryServiceClient
	meteredGiftCardTransactionApproval meteredGiftCardTransactionApproval
	userValidationClient               identityv1.UserValidationServiceClient
	userValidationClientFallback       identityv1.UserValidationServiceClient
	householdQueryClient               customersv1.HouseholdQueryServiceClient
	getProductReviewFn                 func(context.Context, *reviewsv1.GetProductReviewRequest, ...grpc.CallOption) (*reviewsv1.GetProductReviewResponse, error)
}

// NewProductReviewApprovalServer wires a ProductReviewApprovalServer.
func NewProductReviewApprovalServer(deps platform.Deps) *ProductReviewApprovalServer {
	productReview := reviewsv1.NewProductReviewServiceClient(deps.Conn)
	return &ProductReviewApprovalServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.reviews.v1.ProductReviewApprovalService"),
		productReviewQueryClient:           reviewsv1.NewProductReviewQueryServiceClient(deps.Conn),
		giftCardBatchApprovalClient:        giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
		clients:                            deps.Clients,
		productReviewClient:                reviewsv1.NewProductReviewServiceClient(deps.Conn),
		householdValidationClient:          customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		accessPolicyQueryPort:              newAccessPolicyQueryPort(deps),
		customerCalculationPort:            newCustomerCalculationPort(deps),
		sellerApprovalPort:                 newSellerApprovalPort(deps),
		reviewModerationQueryClient:        reviewsv1.NewReviewModerationQueryServiceClient(deps.Conn),
		meteredGiftCardTransactionApproval: meteredGiftCardTransactionApproval{GiftCardTransactionApprovalServiceClient: giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		userValidationClient:               identityv1.NewUserValidationServiceClient(deps.Conn),
		userValidationClientFallback:       identityv1.NewUserValidationServiceClient(deps.Conn),
		householdQueryClient:               customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		getProductReviewFn:                 productReview.GetProductReview,
	}
}

// SubmitProductReview submits a product review.
func (s *ProductReviewApprovalServer) SubmitProductReview(ctx context.Context, req *reviewsv1.SubmitProductReviewRequest) (*reviewsv1.SubmitProductReviewResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "submit product review", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	lookupProductReviewRequest := &reviewsv1.LookupProductReviewRequest{}
	lookupProductReviewRequest.Context = req.GetContext()
	lookupProductReviewRequest.ProductReviewId = req.GetProductReviewId()
	lookupProductReviewReply, err := s.productReviewQueryClient.LookupProductReview(ctx, lookupProductReviewRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewQueryService/LookupProductReview")
	}
	listPendingGiftCardBatchesRequest := &giftcardsv1.ListPendingGiftCardBatchesRequest{}
	listPendingGiftCardBatchesRequest.Context = req.GetContext()
	listPendingGiftCardBatchesRequest.Filter = &giftcardsv1.ListPendingGiftCardBatchesRequest_Filter{
		Region:       lookupProductReviewReply.GetProductReview().GetBillingAddress().GetRegion(),
		CreatedAfter: lookupProductReviewReply.GetProductReview().GetAudit().GetCreatedAt(),
		OwnerId:      req.GetDetails().GetApprover().GetUserId(),
		Labels:       lookupProductReviewReply.GetResult().GetDetails(),
	}
	listPendingGiftCardBatchesReply, err := s.giftCardBatchApprovalClient.ListPendingGiftCardBatches(ctx, listPendingGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/ListPendingGiftCardBatches")
	}
	return &reviewsv1.SubmitProductReviewResponse{
		ProductReview: &reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: lookupProductReviewReply.GetProductReview().GetAudit().GetUpdatedAt(),
			},
			ProductReviewId: req.GetProductReviewId(),
			Number:          req.GetProductReview().GetNumber(),
		},
		Result: &commonv1.OperationResult{
			Status: listPendingGiftCardBatchesReply.GetResult().GetStatus(),
		},
		Outcome: &reviewsv1.SubmitProductReviewResponse_Outcome{
			Message: listPendingGiftCardBatchesReply.GetResult().GetMessage(),
		},
	}, nil
}

// ApproveProductReview approves a product review.
func (s *ProductReviewApprovalServer) ApproveProductReview(ctx context.Context, req *reviewsv1.ApproveProductReviewRequest) (*reviewsv1.ApproveProductReviewResponse, error) {
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "approve product review", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	customerSegmentValidation, ok := s.clients.Get("acme.customers.v1.CustomerSegmentValidationService").(customersv1.CustomerSegmentValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.CustomerSegmentValidationService")
	}
	previewCustomerSegmentRequest := &customersv1.PreviewCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{Etag: req.GetProductReview().GetAudit().GetEtag()},
			Owner: &identityv1.UserRef{Email: req.GetProductReview().GetCounterparty().GetEmail()},
		},
	}
	previewCustomerSegmentReply, err := customerSegmentValidation.PreviewCustomerSegment(ctx, previewCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/PreviewCustomerSegment")
	}
	getProductReviewReply, err := s.productReviewClient.GetProductReview(ctx, &reviewsv1.GetProductReviewRequest{
		Context:         req.GetContext(),
		ProductReviewId: req.GetProductReview().GetProductReviewId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/GetProductReview")
	}
	explainHouseholdDecisionRequest := buildExplainHouseholdDecisionRequestForApproveProductReview(req, getProductReviewReply)
	explainHouseholdDecisionReply, err := s.householdValidationClient.ExplainHouseholdDecision(ctx, explainHouseholdDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision")
	}
	findAccessPoliciesRequest := &authorizationv1.FindAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.FindAccessPoliciesRequest_Filter{
			Region:       req.GetProductReview().GetBillingAddress().GetRegion(),
			CreatedAfter: explainHouseholdDecisionReply.GetHousehold().GetAudit().GetUpdatedAt(),
		},
	}
	findAccessPoliciesReply, err := s.accessPolicyQueryPort.FindAccessPolicies(ctx, findAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/FindAccessPolicies")
	}
	return &reviewsv1.ApproveProductReviewResponse{
		Outcome: &reviewsv1.ApproveProductReviewResponse_Outcome{
			Message: previewCustomerSegmentReply.GetOutcome().GetMessage(),
		},
		ProductReview: &reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				Etag: previewCustomerSegmentReply.GetCustomerSegment().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: getProductReviewReply.GetProductReview().GetBillingAddress().GetCountryCode(),
				Region:      explainHouseholdDecisionReply.GetHousehold().GetLocation().GetRegion(),
			},
			ProductReviewId: req.GetProductReviewId(),
			Notes:           req.GetProductReview().GetNotes(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: findAccessPoliciesReply.GetResult().GetCorrelationId(),
			Message:       findAccessPoliciesReply.GetResult().GetMessage(),
			Status:        "ok",
		},
	}, nil
}

// RejectProductReview rejects a product review.
func (s *ProductReviewApprovalServer) RejectProductReview(ctx context.Context, req *reviewsv1.RejectProductReviewRequest) (*reviewsv1.RejectProductReviewResponse, error) {
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "reject product review", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	quoteCustomerRequest := &customersv1.QuoteCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetProductReview().GetAudit().GetCreatedAt()},
		},
	}
	quoteCustomerReply, err := s.customerCalculationPort.QuoteCustomer(ctx, quoteCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/QuoteCustomer")
	}
	recallSellerRequest := &sellersv1.RecallSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetProductReview().GetAudit().GetCreatedBy()},
			RelationshipManager: &identityv1.UserRef{
				Email: quoteCustomerReply.GetCustomer().GetContact().GetEmail(),
			},
		},
	}
	recallSellerReply, err := s.sellerApprovalPort.RecallSeller(ctx, recallSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/RecallSeller")
	}
	_ = recallSellerReply
	return &reviewsv1.RejectProductReviewResponse{
		ProductReview: &reviewsv1.ProductReview{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: quoteCustomerReply.GetCustomer().GetPrimaryAddress().GetPostalCode(),
			},
			ProductReviewId: req.GetProductReviewId(),
			Counterparty:    req.GetProductReview().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalateProductReview escalates a product review.
func (s *ProductReviewApprovalServer) EscalateProductReview(ctx context.Context, req *reviewsv1.EscalateProductReviewRequest) (*reviewsv1.EscalateProductReviewResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "escalate product review", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	browseReviewModerationsRequest := &reviewsv1.BrowseReviewModerationsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.BrowseReviewModerationsRequest_Filter{
			Region:       req.GetProductReview().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetProductReview().GetAudit().GetUpdatedAt(),
		},
	}
	browseReviewModerationsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*reviewsv1.BrowseReviewModerationsResponse, error) {
		return s.reviewModerationQueryClient.BrowseReviewModerations(ctx, browseReviewModerationsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/BrowseReviewModerations")
	}
	watchProductReviewsStream, err := s.productReviewClient.WatchProductReviews(ctx, &reviewsv1.WatchProductReviewsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		Filter: &reviewsv1.WatchProductReviewsRequest_Filter{
			Region: req.GetProductReview().GetBillingAddress().GetRegion(),
		},
		Since: req.GetProductReview().GetAudit().GetCreatedAt(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/WatchProductReviews")
	}
	for {
		received, err := watchProductReviewsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/WatchProductReviews")
		}
		_ = received
	}
	return &reviewsv1.EscalateProductReviewResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: browseReviewModerationsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ProductReview: &reviewsv1.ProductReview{
			ProductReviewId: req.GetProductReviewId(),
			Status:          req.GetProductReview().GetStatus(),
		},
	}, nil
}

// ListPendingProductReviews lists product reviews a page at a time.
func (s *ProductReviewApprovalServer) ListPendingProductReviews(ctx context.Context, req *reviewsv1.ListPendingProductReviewsRequest) (*reviewsv1.ListPendingProductReviewsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending product reviews", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	submitGiftCardTransactionRequest := &giftcardsv1.SubmitGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.SubmitGiftCardTransactionRequest_Details{
			ApproverEmail: req.GetContext().GetActor().GetEmail(),
			Approver:      &identityv1.UserRef{UserId: userId},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	submitGiftCardTransactionReply, err := s.meteredGiftCardTransactionApproval.SubmitGiftCardTransaction(ctx, submitGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/SubmitGiftCardTransaction")
	}
	routes := map[string]func(context.Context, *identityv1.CheckUserEligibilityRequest, ...grpc.CallOption) (*identityv1.CheckUserEligibilityResponse, error){
		"primary":  s.userValidationClient.CheckUserEligibility,
		"fallback": s.userValidationClientFallback.CheckUserEligibility,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	checkUserEligibilityRequest := &identityv1.CheckUserEligibilityRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: submitGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
		},
	}
	checkUserEligibilityReply, err := route(ctx, checkUserEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/CheckUserEligibility")
	}
	return &reviewsv1.ListPendingProductReviewsResponse{
		ProductReviews: []*reviewsv1.ProductReview{&reviewsv1.ProductReview{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: submitGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Region: checkUserEligibilityReply.GetUser().GetPrimaryAddress().GetRegion(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallProductReview recalls a product review.
func (s *ProductReviewApprovalServer) RecallProductReview(ctx context.Context, req *reviewsv1.RecallProductReviewRequest) (*reviewsv1.RecallProductReviewResponse, error) {
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "recall product review", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	for line := range iterx.Each(req.GetProductReview().GetLines(), func(candidate *reviewsv1.ProductReviewLine) bool { return candidate != nil }) {
		if _, err := s.householdQueryClient.SummarizeHouseholds(ctx, &customersv1.SummarizeHouseholdsRequest{
			Context:     req.GetContext(),
			Annotations: line.GetAttributes(),
			Reason:      req.GetReason(),
			Details:     &customersv1.SummarizeHouseholdsRequest_Details{Filters: req.GetProductReview().GetMetadata()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/SummarizeHouseholds")
		}
	}
	exportReviewModerationsStream, err := s.reviewModerationQueryClient.ExportReviewModerations(ctx, &reviewsv1.ExportReviewModerationsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.ExportReviewModerationsRequest_Filter{
			Region:       req.GetProductReview().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetProductReview().GetIssuedAt(),
			Labels:       req.GetProductReview().GetMetadata(),
			OwnerId:      req.GetProductReview().GetCounterparty().GetCustomerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/ExportReviewModerations")
	}
	for {
		received, err := exportReviewModerationsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/ExportReviewModerations")
		}
		_ = received
	}
	getProductReviewRequest := &reviewsv1.GetProductReviewRequest{
		Context:         req.GetContext(),
		ProductReviewId: req.GetProductReview().GetProductReviewId(),
	}
	getProductReviewReply, err := s.getProductReviewFn(ctx, getProductReviewRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/GetProductReview")
	}
	return &reviewsv1.RecallProductReviewResponse{
		ProductReview: &reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: getProductReviewReply.GetProductReview().GetAudit().GetUpdatedAt(),
			},
			ProductReviewId: req.GetProductReviewId(),
			Number:          req.GetProductReview().GetNumber(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetProductReviewApprovalStatus returns a product review.
func (s *ProductReviewApprovalServer) GetProductReviewApprovalStatus(ctx context.Context, req *reviewsv1.GetProductReviewApprovalStatusRequest) (*reviewsv1.GetProductReviewApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_review", req.GetProductReviewId(), "get_product_review_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get product review approval status", "tenant", req.GetContext().GetTenantId())
	return &reviewsv1.GetProductReviewApprovalStatusResponse{
		ProductReview: &reviewsv1.ProductReview{ProductReviewId: req.GetProductReviewId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}
