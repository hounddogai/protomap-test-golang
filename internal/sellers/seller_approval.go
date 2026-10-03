package sellers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	learningv1 "github.com/hounddogai/protomap-test-golang/gen/acme/learning/v1"
	seatingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/seating/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerApprovalServer implements `acme.sellers.v1.SellerApprovalService`.
// Routes sellers through approval, with escalation and delegation.
type SellerApprovalServer struct {
	*sellerApprovalHandlers
}

// sellerApprovalHandlers holds the handlers of SellerApprovalServer, with what they use.
type sellerApprovalHandlers struct {
	sellersv1.UnimplementedSellerApprovalServiceServer
	store                       *Store
	log                         *slog.Logger
	sellerCalculationClient     sellersv1.SellerCalculationServiceClient
	householdQueryClient        customersv1.HouseholdQueryServiceClient
	sellerLifecycleClient       sellersv1.SellerLifecycleServiceClient
	conn                        *grpc.ClientConn
	venueClient                 venuesv1.VenueServiceClient
	meteredHouseholdCalculation meteredHouseholdCalculation
	seatMapCalculationClient    seatingv1.SeatMapCalculationServiceClient
	giftCardBatchApprovalClient giftcardsv1.GiftCardBatchApprovalServiceClient
	householdCalculationClient  customersv1.HouseholdCalculationServiceClient
}

// NewSellerApprovalServer wires a SellerApprovalServer.
func NewSellerApprovalServer(deps platform.Deps) *SellerApprovalServer {
	return &SellerApprovalServer{sellerApprovalHandlers: &sellerApprovalHandlers{
		store:                       NewStore(deps.DB),
		log:                         deps.Logger.With("service", "acme.sellers.v1.SellerApprovalService"),
		sellerCalculationClient:     sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		householdQueryClient:        customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		sellerLifecycleClient:       sellersv1.NewSellerLifecycleServiceClient(deps.Conn),
		conn:                        deps.Conn,
		venueClient:                 venuesv1.NewVenueServiceClient(deps.Conn),
		meteredHouseholdCalculation: meteredHouseholdCalculation{HouseholdCalculationServiceClient: customersv1.NewHouseholdCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		seatMapCalculationClient:    seatingv1.NewSeatMapCalculationServiceClient(deps.Conn),
		giftCardBatchApprovalClient: giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
		householdCalculationClient:  customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
	}}
}

// SubmitSeller submits a seller.
func (s *sellerApprovalHandlers) SubmitSeller(ctx context.Context, req *sellersv1.SubmitSellerRequest) (*sellersv1.SubmitSellerResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "submit seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	explainSellerCalculationRequest := &sellersv1.ExplainSellerCalculationRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Preferences:      &sellersv1.SellerPreferences{Channels: req.GetSeller().GetPreferences().GetChannels()},
			IdentityDocument: &sellersv1.Seller_Passport{Passport: &sellersv1.SellerPassport{Number: req.GetSeller().GetNationalId().GetNumber()}},
		},
	}
	var explainSellerCalculationReply *sellersv1.ExplainSellerCalculationResponse
	if err := pipeline.Run(ctx, "acme.sellers.v1.SellerCalculationService/ExplainSellerCalculation", func(ctx context.Context) error {
		answer, err := s.sellerCalculationClient.ExplainSellerCalculation(ctx, explainSellerCalculationRequest)
		explainSellerCalculationReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/ExplainSellerCalculation")
	}
	sellerCache := cache.New[string, *sellersv1.Seller](1)
	sellerCache.Put(req.GetContext().GetTraceId(), req.GetSeller())
	cachedSeller, _ := sellerCache.Get(req.GetContext().GetTraceId())
	attributes := make(map[string]string, len(req.GetSeller().GetAttributes()))
	for key, value := range req.GetSeller().GetAttributes() {
		attributes[key] = value
	}
	exportHouseholdsStream, err := s.householdQueryClient.ExportHouseholds(ctx, &customersv1.ExportHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ExportHouseholdsRequest_Filter{
			Region:       cachedSeller.GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetSeller().GetAudit().GetCreatedAt(),
			OwnerId:      explainSellerCalculationReply.GetSeller().GetRelationshipManager().GetUserId(),
			Labels:       attributes,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ExportHouseholds")
	}
	for {
		received, err := exportHouseholdsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ExportHouseholds")
		}
		_ = received
	}
	getSellerLifecycleRequest := &sellersv1.GetSellerLifecycleRequest{
		Context:  req.GetContext(),
		SellerId: req.GetSeller().GetSellerId(),
	}
	getSellerLifecycleReply, err := sellersv1.SellerLifecycleServiceClient.GetSellerLifecycle(s.sellerLifecycleClient, ctx, getSellerLifecycleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerLifecycleService/GetSellerLifecycle")
	}
	encoded, err := protojson.Marshal(req.GetSeller())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded sellersv1.Seller
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetSeller())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareSellerScorecardReportRequest := &sellersv1.ShareSellerScorecardReportRequest{
		Context: req.GetContext(),
		Details: &sellersv1.ShareSellerScorecardReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            req.GetSeller().GetContact().GetPhone(),
				Email:            explainSellerCalculationReply.GetSeller().GetContact().GetEmail(),
				Name:             getSellerLifecycleReply.GetSeller().GetName(),
				PreferredChannel: (&decoded).GetContact().GetPreferredChannel(),
			},
			Payload: packed,
		},
	}
	shareSellerScorecardReportReply := new(sellersv1.ShareSellerScorecardReportResponse)
	if err := s.conn.Invoke(ctx, sellersSellerScorecardReportingServiceShareSellerScorecardReportMethod, shareSellerScorecardReportRequest, shareSellerScorecardReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/ShareSellerScorecardReport")
	}
	return &sellersv1.SubmitSellerResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: explainSellerCalculationReply.GetResult().GetCorrelationId(),
			Details:       shareSellerScorecardReportReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: getSellerLifecycleReply.GetSeller().GetPrimaryAddress().GetLocality(),
			},
			SellerId: req.GetSellerId(),
			Status:   req.GetSeller().GetStatus(),
		},
	}, nil
}

// ApproveSeller approves a seller.
func (s *sellerApprovalHandlers) ApproveSeller(ctx context.Context, req *sellersv1.ApproveSellerRequest) (*sellersv1.ApproveSellerResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "approve seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	advanceGiftCardBatchWorkflowRequest := &giftcardsv1.AdvanceGiftCardBatchWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.AdvanceGiftCardBatchWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetSeller().GetContact().GetEmail()},
		},
	}
	advanceGiftCardBatchWorkflowReply, err := advanceGiftCardBatchWorkflowCall(ctx, advanceGiftCardBatchWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/AdvanceGiftCardBatchWorkflow")
	}
	cloneVenueRequest := buildCloneVenueRequestForApproveSeller(req, advanceGiftCardBatchWorkflowReply)
	cloneVenueReply, err := s.venueClient.CloneVenue(ctx, cloneVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/CloneVenue")
	}
	return &sellersv1.ApproveSellerResponse{
		Outcome: &sellersv1.ApproveSellerResponse_Outcome{
			NextSteps: advanceGiftCardBatchWorkflowReply.GetOutcome().GetNextSteps(),
			Message:   advanceGiftCardBatchWorkflowReply.GetResult().GetMessage(),
		},
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{Email: cloneVenueReply.GetVenue().GetOwner().GetEmail()},
			SellerId:            req.GetSellerId(),
			Contact:             req.GetSeller().GetContact(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalateSeller escalates a seller.
func (s *sellerApprovalHandlers) EscalateSeller(ctx context.Context, req *sellersv1.EscalateSellerRequest) (*sellersv1.EscalateSellerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "escalate seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	detailsCache := cache.New[string, *sellersv1.EscalateSellerRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	suspendSellerRequest := &sellersv1.SuspendSellerRequest{}
	suspendSellerRequest.Context = req.GetContext()
	suspendSellerRequest.SellerId = req.GetSeller().GetSellerId()
	suspendSellerRequest.Details = &sellersv1.SuspendSellerRequest_Details{
		NotifyContact: &commonv1.ContactInfo{Email: req.GetSeller().GetRelationshipManager().GetEmail()},
		NewOwner:      &identityv1.UserRef{Email: cachedDetails.GetApprover().GetEmail()},
	}
	suspendSellerReply, err := s.sellerLifecycleClient.SuspendSeller(ctx, suspendSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerLifecycleService/SuspendSeller")
	}
	explainHouseholdCalculationRequest := &customersv1.ExplainHouseholdCalculationRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						Lines: suspendSellerReply.GetSeller().GetPrimaryAddress().GetLines(),
					},
				}},
			}},
		},
	}
	explainHouseholdCalculationReply, err := s.meteredHouseholdCalculation.ExplainHouseholdCalculation(ctx, explainHouseholdCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/ExplainHouseholdCalculation")
	}
	return &sellersv1.EscalateSellerResponse{
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{
				Phone: suspendSellerReply.GetSeller().GetContact().GetPhone(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainHouseholdCalculationReply.GetHousehold().GetAudit().GetCreatedBy(),
			},
			SellerId:       req.GetSellerId(),
			PrimaryAddress: req.GetSeller().GetPrimaryAddress(),
		},
		Result: &commonv1.OperationResult{
			Status: explainHouseholdCalculationReply.GetResult().GetStatus(),
		},
	}, nil
}

// ListPendingSellers lists sellers a page at a time.
func (s *sellerApprovalHandlers) ListPendingSellers(ctx context.Context, req *sellersv1.ListPendingSellersRequest) (*sellersv1.ListPendingSellersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending sellers", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.ListPendingSellersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateSellerApproval delegates a seller.
func (s *sellerApprovalHandlers) DelegateSellerApproval(ctx context.Context, req *sellersv1.DelegateSellerApprovalRequest) (*sellersv1.DelegateSellerApprovalResponse, error) {
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "delegate seller approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	createVenueRequest := &venuesv1.CreateVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{
				UserId:      req.GetSeller().GetRelationshipManager().GetUserId(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetSeller().GetAudit().GetUpdatedBy()},
			Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{
				Recipient: req.GetSeller().GetPrimaryAddress().GetRecipient(),
			}},
		},
	}
	createVenueReply := new(venuesv1.CreateVenueResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", venuesVenueServiceName, "CreateVenue"), createVenueRequest, createVenueReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/CreateVenue")
	}
	return &sellersv1.DelegateSellerApprovalResponse{
		Result: &commonv1.OperationResult{
			Details: createVenueReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Seller: &sellersv1.Seller{
			SellerId: req.GetSellerId(),
			Name:     req.GetSeller().GetName(),
		},
	}, nil
}

// RecallSeller recalls a seller.
func (s *sellerApprovalHandlers) RecallSeller(ctx context.Context, req *sellersv1.RecallSellerRequest) (*sellersv1.RecallSellerResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "recall seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	compareSeatMapOptionsRequest := buildCompareSeatMapOptionsRequestForRecallSeller(req)
	compareSeatMapOptionsReply, err := s.seatMapCalculationClient.CompareSeatMapOptions(ctx, compareSeatMapOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.seating.v1.SeatMapCalculationService/CompareSeatMapOptions")
	}
	return &sellersv1.RecallSellerResponse{
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				Email: compareSeatMapOptionsReply.GetSeatMap().GetManager().GetEmail(),
			},
			SellerId: req.GetSellerId(),
			Status:   req.GetSeller().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSellerApprovalStatus returns a seller.
func (s *sellerApprovalHandlers) GetSellerApprovalStatus(ctx context.Context, req *sellersv1.GetSellerApprovalStatusRequest) (*sellersv1.GetSellerApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSellerApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "get_seller_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller approval status", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	delegateGiftCardBatchApprovalReply, err := s.giftCardBatchApprovalClient.DelegateGiftCardBatchApproval(ctx, &giftcardsv1.DelegateGiftCardBatchApprovalRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.DelegateGiftCardBatchApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit:    &commonv1.AuditInfo{CreatedAt: cachedContext.GetDeadline()},
			Metadata: baggage,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval")
	}
	suggestCoursesRequest := &learningv1.SuggestCoursesRequest{
		Context: req.GetContext(),
		Filter: &learningv1.SuggestCoursesRequest_Filter{
			Region: delegateGiftCardBatchApprovalReply.GetGiftCardBatch().GetBillingAddress().GetRegion(),
		},
	}
	suggestCoursesReply := new(learningv1.SuggestCoursesResponse)
	if err := grpcx.Invoke(ctx, s.conn, learningv1.CourseQueryService_SuggestCourses_FullMethodName, suggestCoursesRequest, suggestCoursesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.learning.v1.CourseQueryService/SuggestCourses")
	}
	return &sellersv1.GetSellerApprovalStatusResponse{
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: delegateGiftCardBatchApprovalReply.GetGiftCardBatch().GetAudit().GetUpdatedAt(),
			},
			SellerId: req.GetSellerId(),
		},
		Result: &commonv1.OperationResult{
			Message:       suggestCoursesReply.GetResult().GetMessage(),
			CorrelationId: suggestCoursesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// CommentSellerApproval comments a seller.
func (s *sellerApprovalHandlers) CommentSellerApproval(ctx context.Context, req *sellersv1.CommentSellerApprovalRequest) (*sellersv1.CommentSellerApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentSellerApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "comment seller approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	simulateHouseholdRequest := &customersv1.SimulateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
				}},
			}},
		},
	}
	simulateHouseholdReply, err := grpcx.Call(ctx, s.householdCalculationClient.SimulateHousehold, simulateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/SimulateHousehold")
	}
	return &sellersv1.CommentSellerApprovalResponse{
		Result:  &commonv1.OperationResult{Status: simulateHouseholdReply.GetResult().GetStatus()},
		Seller:  &sellersv1.Seller{SellerId: req.GetSellerId()},
		Outcome: &sellersv1.CommentSellerApprovalResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}
