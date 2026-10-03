package promotions

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CouponQueryServer implements `acme.promotions.v1.CouponQueryService`.
// Answers read-only questions about coupons, for screens and reports.
type CouponQueryServer struct {
	promotionsv1.UnimplementedCouponQueryServiceServer
	store                    *Store
	log                      *slog.Logger
	promotionQueryPort       promotionQueryPort
	conn                     *grpc.ClientConn
	documentValidationClient documentsv1.DocumentValidationServiceClient
	clients                  *registry.Registry
	couponCalculationClient  promotionsv1.CouponCalculationServiceClient
}

// NewCouponQueryServer wires a CouponQueryServer.
func NewCouponQueryServer(deps platform.Deps) *CouponQueryServer {
	return &CouponQueryServer{
		store:                    NewStore(deps.DB),
		log:                      deps.Logger.With("service", "acme.promotions.v1.CouponQueryService"),
		promotionQueryPort:       newPromotionQueryPort(deps),
		conn:                     deps.Conn,
		documentValidationClient: documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		clients:                  deps.Clients,
		couponCalculationClient:  promotionsv1.NewCouponCalculationServiceClient(deps.Conn),
	}
}

// LookupCoupon returns a coupon.
func (s *CouponQueryServer) LookupCoupon(ctx context.Context, req *promotionsv1.LookupCouponRequest) (*promotionsv1.LookupCouponResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupCoupon", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "lookup_coupon"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup coupon", "tenant", req.GetContext().GetTenantId())
	suggestPromotionsRequest := &promotionsv1.SuggestPromotionsRequest{
		Context: req.GetContext(),
		Filter:  &promotionsv1.SuggestPromotionsRequest_Filter{OwnerId: req.GetCouponId()},
	}
	suggestPromotionsReply, err := s.promotionQueryPort.SuggestPromotions(ctx, suggestPromotionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionQueryService/SuggestPromotions")
	}
	return &promotionsv1.LookupCouponResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestPromotionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Coupon: &promotionsv1.Coupon{CouponId: req.GetCouponId()},
	}, nil
}

// BrowseCoupons lists coupons a page at a time.
func (s *CouponQueryServer) BrowseCoupons(ctx context.Context, req *promotionsv1.BrowseCouponsRequest) (*promotionsv1.BrowseCouponsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BrowseCoupons", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "browse coupons", "tenant", req.GetContext().GetTenantId())
	resolvePromotionsRequest := &promotionsv1.ResolvePromotionsRequest{Context: req.GetContext()}
	resolvePromotionsReply := new(promotionsv1.ResolvePromotionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, promotionsv1.PromotionQueryService_ResolvePromotions_FullMethodName, resolvePromotionsRequest, resolvePromotionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionQueryService/ResolvePromotions")
	}
	return &promotionsv1.BrowseCouponsResponse{
		PageInfo: &commonv1.PageInfo{
			TotalSize: resolvePromotionsReply.GetPageInfo().GetTotalSize(),
		},
		Result: &commonv1.OperationResult{Status: resolvePromotionsReply.GetResult().GetStatus()},
	}, nil
}

// FindCoupons lists coupons a page at a time.
func (s *CouponQueryServer) FindCoupons(ctx context.Context, req *promotionsv1.FindCouponsRequest) (*promotionsv1.FindCouponsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "find coupons", "tenant", req.GetContext().GetTenantId())
	var holder struct{ UserId string }
	if record := req.GetContext().GetActor(); record != nil {
		if err := mapper.CopyFields(&holder, record, "UserId"); err != nil {
			s.log.WarnContext(ctx, "copy user_id", "error", err)
		}
	}
	normalizeUserRequest := &identityv1.NormalizeUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			RelationshipManager: &identityv1.UserRef{
				UserId: holder.UserId,
				Email:  req.GetContext().GetActor().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
	}
	normalizeUserReply := new(identityv1.NormalizeUserResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.identity.v1.UserValidationService/NormalizeUser", normalizeUserRequest, normalizeUserReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/NormalizeUser")
	}
	countPromotionsRequest := &promotionsv1.CountPromotionsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.CountPromotionsRequest_Filter{
			Labels:       req.GetFilter().GetLabels(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
	}
	countPromotionsReply := new(promotionsv1.CountPromotionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, promotionsv1.PromotionQueryService_CountPromotions_FullMethodName, countPromotionsRequest, countPromotionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionQueryService/CountPromotions")
	}
	return &promotionsv1.FindCouponsResponse{
		Coupons: []*promotionsv1.Coupon{&promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{Name: normalizeUserReply.GetUser().GetName()},
		}},
		Result: &commonv1.OperationResult{
			CorrelationId: countPromotionsReply.GetResult().GetCorrelationId(),
			Details:       countPromotionsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
	}, nil
}

// CountCoupons counts coupons by status.
func (s *CouponQueryServer) CountCoupons(ctx context.Context, req *promotionsv1.CountCouponsRequest) (*promotionsv1.CountCouponsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count coupons", "tenant", req.GetContext().GetTenantId())
	scoreBackorderRequest := &ordersv1.ScoreBackorderRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ScoreBackorderRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Backorder: &ordersv1.Backorder{
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Assignee:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Sla:       &ordersv1.BackorderSla{DueAt: req.GetFilter().GetCreatedAfter()},
		},
	}
	scoreBackorderReply, err := grpcx.NewClient(s.conn, ordersv1.NewBackorderValidationServiceClient).ScoreBackorder(ctx, scoreBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderValidationService/ScoreBackorder")
	}
	return &promotionsv1.CountCouponsResponse{
		Result: &commonv1.OperationResult{Status: scoreBackorderReply.GetResult().GetStatus()},
	}, nil
}

// ResolveCoupons returns several coupons at once.
func (s *CouponQueryServer) ResolveCoupons(ctx context.Context, req *promotionsv1.ResolveCouponsRequest) (*promotionsv1.ResolveCouponsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve coupons", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	validateDocumentRequest := &documentsv1.ValidateDocumentRequest{
		Context: req.GetContext(),
		Details: &documentsv1.ValidateDocumentRequest_Details{
			Locale:  req.GetContext().GetLocale(),
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Document: &documentsv1.Document{
			IssuedAt: req.GetContext().GetDeadline(),
			Metadata: baggage,
		},
	}
	validateDocumentReply, err := documentsv1.DocumentValidationServiceClient.ValidateDocument(s.documentValidationClient, ctx, validateDocumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/ValidateDocument")
	}
	return &promotionsv1.ResolveCouponsResponse{
		Result: &commonv1.OperationResult{
			Details: validateDocumentReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// SummarizeCoupons summarizes a coupon.
func (s *CouponQueryServer) SummarizeCoupons(ctx context.Context, req *promotionsv1.SummarizeCouponsRequest) (*promotionsv1.SummarizeCouponsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "summarize_coupons"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize coupons", "tenant", req.GetContext().GetTenantId())
	countPromotionsRequest := &promotionsv1.CountPromotionsRequest{
		Context: req.GetContext(),
		Filter:  &promotionsv1.CountPromotionsRequest_Filter{OwnerId: req.GetCouponId()},
	}
	countPromotionsReply, err := grpcx.NewClient(s.conn, promotionsv1.NewPromotionQueryServiceClient).CountPromotions(ctx, countPromotionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionQueryService/CountPromotions")
	}
	pickListValidation, ok := s.clients.Get("acme.warehousing.v1.PickListValidationService").(warehousingv1.PickListValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.warehousing.v1.PickListValidationService")
	}
	normalizePickListRequest := &warehousingv1.NormalizePickListRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Details: &warehousingv1.NormalizePickListRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		Reason: req.GetReason(),
	}
	normalizePickListReply, err := pickListValidation.NormalizePickList(ctx, normalizePickListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/NormalizePickList")
	}
	return &promotionsv1.SummarizeCouponsResponse{
		Result: &commonv1.OperationResult{
			Status:        countPromotionsReply.GetResult().GetStatus(),
			CorrelationId: countPromotionsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  normalizePickListReply.GetResult().GetReferenceIds(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestCoupons lists coupons a page at a time.
func (s *CouponQueryServer) SuggestCoupons(ctx context.Context, req *promotionsv1.SuggestCouponsRequest) (*promotionsv1.SuggestCouponsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest coupons", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*promotionsv1.SuggestCouponsRequest_Filter)
	calculateCouponRequest := &promotionsv1.CalculateCouponRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.CalculateCouponRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Coupon: &promotionsv1.Coupon{
			Labels:  req.GetFilter().GetLabels(),
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Audit:   &commonv1.AuditInfo{UpdatedAt: cloned.GetCreatedAfter()},
		},
	}
	type calculateCouponResult struct {
		reply *promotionsv1.CalculateCouponResponse
		err   error
	}
	calculateCouponResults := make(chan calculateCouponResult, 1)
	go func() {
		reply, err := s.couponCalculationClient.CalculateCoupon(ctx, calculateCouponRequest)
		calculateCouponResults <- calculateCouponResult{reply, err}
	}()
	var calculateCouponReply *promotionsv1.CalculateCouponResponse
	select {
	case result := <-calculateCouponResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.promotions.v1.CouponCalculationService/CalculateCoupon")
		}
		calculateCouponReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &promotionsv1.SuggestCouponsResponse{
		Coupons: []*promotionsv1.Coupon{&promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{
				Email: calculateCouponReply.GetCoupon().GetContact().GetEmail(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
