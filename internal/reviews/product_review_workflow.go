package reviews

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
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	workplacesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/workplaces/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
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
	"google.golang.org/protobuf/types/known/anypb"
)

// ProductReviewWorkflowServer implements `acme.reviews.v1.ProductReviewWorkflowService`.
// Runs the long-lived workflow that takes a product review from start to finish.
type ProductReviewWorkflowServer struct {
	reviewsv1.UnimplementedProductReviewWorkflowServiceServer
	store                             *Store
	log                               *slog.Logger
	customerSegmentCalculationClient  customersv1.CustomerSegmentCalculationServiceClient
	ledgerAccountClient               ledgerv1.LedgerAccountServiceClient
	conn                              *grpc.ClientConn
	giftCardWorkflowClient            giftcardsv1.GiftCardWorkflowServiceClient
	giftCardTransactionApprovalClient giftcardsv1.GiftCardTransactionApprovalServiceClient
	productReviewClient               reviewsv1.ProductReviewServiceClient
	merchandisingRuleApprovalPort     merchandisingRuleApprovalPort
	apiCredentialNotificationClient   identityv1.ApiCredentialNotificationServiceClient
	householdValidationClient         customersv1.HouseholdValidationServiceClient
	reviewVoteCalculationPort         reviewVoteCalculationPort
	reviewVoteCalculationClient       reviewsv1.ReviewVoteCalculationServiceClient
	productReviewPort                 productReviewPort
	placementWorkflowClient           merchandisingv1.PlacementWorkflowServiceClient
	clients                           *registry.Registry
	reviewModerationCalculationClient reviewsv1.ReviewModerationCalculationServiceClient
	meteredProductReview              meteredProductReview
}

// NewProductReviewWorkflowServer wires a ProductReviewWorkflowServer.
func NewProductReviewWorkflowServer(deps platform.Deps) *ProductReviewWorkflowServer {
	return &ProductReviewWorkflowServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.reviews.v1.ProductReviewWorkflowService"),
		customerSegmentCalculationClient:  customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		ledgerAccountClient:               ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		conn:                              deps.Conn,
		giftCardWorkflowClient:            giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
		giftCardTransactionApprovalClient: giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn),
		productReviewClient:               reviewsv1.NewProductReviewServiceClient(deps.Conn),
		merchandisingRuleApprovalPort:     newMerchandisingRuleApprovalPort(deps),
		apiCredentialNotificationClient:   identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		householdValidationClient:         customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		reviewVoteCalculationPort:         newReviewVoteCalculationPort(deps),
		reviewVoteCalculationClient:       reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn),
		productReviewPort:                 newProductReviewPort(deps),
		placementWorkflowClient:           merchandisingv1.NewPlacementWorkflowServiceClient(deps.Conn),
		clients:                           deps.Clients,
		reviewModerationCalculationClient: reviewsv1.NewReviewModerationCalculationServiceClient(deps.Conn),
		meteredProductReview:              meteredProductReview{ProductReviewServiceClient: reviewsv1.NewProductReviewServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// StartProductReviewWorkflow starts a product review.
func (s *ProductReviewWorkflowServer) StartProductReviewWorkflow(ctx context.Context, req *reviewsv1.StartProductReviewWorkflowRequest) (*reviewsv1.StartProductReviewWorkflowResponse, error) {
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "start product review workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetProductReview())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded reviewsv1.ProductReview
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	step := &productReviewWorkflowServerStartProductReviewWorkflowQuoteCustomerSegmentStep{request: &customersv1.QuoteCustomerSegmentRequest{
		Context: req.GetContext(),
		Details: &customersv1.QuoteCustomerSegmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: (&decoded).GetBillingAddress().GetCountryCode(),
				PostalCode:  req.GetProductReview().GetBillingAddress().GetPostalCode(),
				Region:      req.GetProductReview().GetBillingAddress().GetRegion(),
			},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetProductReview().GetAudit().GetCreatedBy()},
		},
	}}
	if err := s.runStartProductReviewWorkflowQuoteCustomerSegment(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/QuoteCustomerSegment")
	}
	quoteCustomerSegmentReply := step.reply
	return &reviewsv1.StartProductReviewWorkflowResponse{
		ProductReview: &reviewsv1.ProductReview{
			Total: quoteCustomerSegmentReply.GetBreakdown().GetTotal(),
			Lines: []*reviewsv1.ProductReviewLine{&reviewsv1.ProductReviewLine{
				Amount: quoteCustomerSegmentReply.GetCustomerSegment().GetDiscount().GetAmount(),
			}},
			ProductReviewId: req.GetProductReviewId(),
			BillingAddress:  req.GetProductReview().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvanceProductReviewWorkflow advances a product review.
func (s *ProductReviewWorkflowServer) AdvanceProductReviewWorkflow(ctx context.Context, req *reviewsv1.AdvanceProductReviewWorkflowRequest) (*reviewsv1.AdvanceProductReviewWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvanceProductReviewWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "advance product review workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	watchLedgerAccountsStream, err := s.ledgerAccountClient.WatchLedgerAccounts(ctx, &ledgerv1.WatchLedgerAccountsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:  req.GetDetails().GetNotify().GetName(),
			Phone: req.GetDetails().GetNotify().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/WatchLedgerAccounts")
	}
	for {
		received, err := watchLedgerAccountsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/WatchLedgerAccounts")
		}
		_ = received
	}
	calculateBuildingRequest := &workplacesv1.CalculateBuildingRequest{
		Context: req.GetContext(),
		Building: &workplacesv1.Building{
			Children: []*workplacesv1.Building{&workplacesv1.Building{
				Children: []*workplacesv1.Building{&workplacesv1.Building{
					Location: &commonv1.PostalAddress{
						Region:    req.GetProductReview().GetBillingAddress().GetRegion(),
						Recipient: req.GetProductReview().GetBillingAddress().GetRecipient(),
					},
					Manager: &identityv1.UserRef{Email: req.GetProductReview().GetCounterparty().GetEmail()},
					Audit:   &commonv1.AuditInfo{Etag: req.GetProductReview().GetAudit().GetEtag()},
				}},
			}},
		},
	}
	calculateBuildingReply := new(workplacesv1.CalculateBuildingResponse)
	if err := grpcx.Invoke(ctx, s.conn, workplacesv1.BuildingCalculationService_CalculateBuilding_FullMethodName, calculateBuildingRequest, calculateBuildingReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.workplaces.v1.BuildingCalculationService/CalculateBuilding")
	}
	jobs := make(chan *reviewsv1.ProductReviewLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.giftCardWorkflowClient.SignalGiftCardWorkflow(workCtx, &giftcardsv1.SignalGiftCardWorkflowRequest{
					Context: req.GetContext(),
					GiftCard: &giftcardsv1.GiftCard{
						Limits: []*giftcardsv1.GiftCardLimit{&giftcardsv1.GiftCardLimit{Amount: line.GetTax().GetAmount()}},
						Audit: &commonv1.AuditInfo{
							CreatedBy: req.GetProductReview().GetAudit().GetCreatedBy(),
							Etag:      calculateBuildingReply.GetBuilding().GetAudit().GetEtag(),
						},
					},
					Details: &giftcardsv1.SignalGiftCardWorkflowRequest_Details{
						Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetProductReview().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/SignalGiftCardWorkflow")
	}
	cloned := proto.Clone(req.GetProductReview()).(*reviewsv1.ProductReview)
	packed, err := anypb.New(req.GetProductReview())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	step := &productReviewWorkflowServerAdvanceProductReviewWorkflowCommentGiftCardTransactionApprovalStep{request: &giftcardsv1.CommentGiftCardTransactionApprovalRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CommentGiftCardTransactionApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetProductReview().GetCounterparty().GetDisplayName(),
				Email:       cloned.GetCounterparty().GetEmail(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{Number: req.GetProductReview().GetNumber()},
			Detail:  packed,
		},
	}}
	if err := s.runAdvanceProductReviewWorkflowCommentGiftCardTransactionApproval(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/CommentGiftCardTransactionApproval")
	}
	commentGiftCardTransactionApprovalReply := step.reply
	return &reviewsv1.AdvanceProductReviewWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:  calculateBuildingReply.GetResult().GetStatus(),
			Details: commentGiftCardTransactionApprovalReply.GetResult().GetDetails(),
		},
		ProductReview: &reviewsv1.ProductReview{
			Counterparty: &customersv1.CustomerRef{
				Email: calculateBuildingReply.GetBuilding().GetManager().GetEmail(),
			},
			ProductReviewId: req.GetProductReviewId(),
			Total:           req.GetProductReview().GetTotal(),
		},
	}, nil
}

// CancelProductReviewWorkflow cancels a product review.
func (s *ProductReviewWorkflowServer) CancelProductReviewWorkflow(ctx context.Context, req *reviewsv1.CancelProductReviewWorkflowRequest) (*reviewsv1.CancelProductReviewWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "cancel product review workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	watchProductReviewsStream, err := s.productReviewClient.WatchProductReviews(ctx, &reviewsv1.WatchProductReviewsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: req.GetDetails().GetNotify().GetEmail(),
			Name:  req.GetDetails().GetNotify().GetName(),
		},
		Filter: &reviewsv1.WatchProductReviewsRequest_Filter{
			Region: req.GetProductReview().GetBillingAddress().GetRegion(),
		},
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
	commentMerchandisingRuleApprovalRequest := &merchandisingv1.CommentMerchandisingRuleApprovalRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetProductReview().GetAudit().GetCreatedBy()},
		},
		Details: &merchandisingv1.CommentMerchandisingRuleApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetProductReview().GetCounterparty().GetEmail()},
		},
	}
	commentMerchandisingRuleApprovalReply, err := s.merchandisingRuleApprovalPort.CommentMerchandisingRuleApproval(ctx, commentMerchandisingRuleApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleApprovalService/CommentMerchandisingRuleApproval")
	}
	return &reviewsv1.CancelProductReviewWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: commentMerchandisingRuleApprovalReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		ProductReview: &reviewsv1.ProductReview{
			Lines: []*reviewsv1.ProductReviewLine{&reviewsv1.ProductReviewLine{
				Tax: &reviewsv1.ProductReviewLineTax{
					Amount: commentMerchandisingRuleApprovalReply.GetMerchandisingRule().GetDiscount().GetAmount(),
				},
			}},
			ProductReviewId: req.GetProductReviewId(),
			Number:          req.GetProductReview().GetNumber(),
		},
	}, nil
}

// RetryProductReviewWorkflow retrys a product review.
func (s *ProductReviewWorkflowServer) RetryProductReviewWorkflow(ctx context.Context, req *reviewsv1.RetryProductReviewWorkflowRequest) (*reviewsv1.RetryProductReviewWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "retry product review workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	sendApiCredentialTestNoticeRequest := &identityv1.SendApiCredentialTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendApiCredentialTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotify().GetPhone(),
				Name:  req.GetDetails().GetNotify().GetName(),
			},
		},
	}
	send := s.sendApiCredentialTestNoticeSender(sendApiCredentialTestNoticeRequest)
	sendApiCredentialTestNoticeReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SendApiCredentialTestNotice")
	}
	return &reviewsv1.RetryProductReviewWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: sendApiCredentialTestNoticeReply.GetResult().GetDetails(),
			Message: sendApiCredentialTestNoticeReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		ProductReview: &reviewsv1.ProductReview{
			ProductReviewId: req.GetProductReviewId(),
			Number:          req.GetProductReview().GetNumber(),
		},
	}, nil
}

// PauseProductReviewWorkflow pauses a product review.
func (s *ProductReviewWorkflowServer) PauseProductReviewWorkflow(ctx context.Context, req *reviewsv1.PauseProductReviewWorkflowRequest) (*reviewsv1.PauseProductReviewWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "pause product review workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	normalizeHouseholdRequest := buildNormalizeHouseholdRequestForPauseProductReviewWorkflow(req)
	normalizeHouseholdReply, err := s.householdValidationClient.NormalizeHousehold(ctx, normalizeHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/NormalizeHousehold")
	}
	startPlacementWorkflowRequest := &merchandisingv1.StartPlacementWorkflowRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					Email:            normalizeHouseholdReply.GetHousehold().GetManager().GetEmail(),
					PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				},
			}},
		},
		Details: &merchandisingv1.StartPlacementWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetProductReview().GetCounterparty().GetEmail()},
		},
	}
	startPlacementWorkflowReply := new(merchandisingv1.StartPlacementWorkflowResponse)
	if err := grpc.Invoke(ctx, "/acme.merchandising.v1.PlacementWorkflowService/StartPlacementWorkflow", startPlacementWorkflowRequest, startPlacementWorkflowReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/StartPlacementWorkflow")
	}
	packed, err := anypb.New(req.GetProductReview())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateReviewVoteRequest := &reviewsv1.CalculateReviewVoteRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.CalculateReviewVoteRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetProductReview().GetBillingAddress().GetPostalCode(),
				Lines:      normalizeHouseholdReply.GetHousehold().GetLocation().GetLines(),
				Region:     normalizeHouseholdReply.GetHousehold().GetLocation().GetRegion(),
			},
		},
		ReviewVote: &reviewsv1.ReviewVote{Detail: packed},
	}
	calculateReviewVoteReply, err := s.reviewVoteCalculationPort.CalculateReviewVote(ctx, calculateReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/CalculateReviewVote")
	}
	return &reviewsv1.PauseProductReviewWorkflowResponse{
		Outcome: &reviewsv1.PauseProductReviewWorkflowResponse_Outcome{
			Message: normalizeHouseholdReply.GetResult().GetMessage(),
			Status:  startPlacementWorkflowReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: startPlacementWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		ProductReview: &reviewsv1.ProductReview{
			Counterparty: &customersv1.CustomerRef{
				CustomerId: calculateReviewVoteReply.GetReviewVote().GetSubject().GetCustomerId(),
				Email:      calculateReviewVoteReply.GetReviewVote().GetSubject().GetEmail(),
			},
			ProductReviewId: req.GetProductReviewId(),
			DueAt:           req.GetProductReview().GetDueAt(),
		},
	}, nil
}

// ResumeProductReviewWorkflow resumes a product review.
func (s *ProductReviewWorkflowServer) ResumeProductReviewWorkflow(ctx context.Context, req *reviewsv1.ResumeProductReviewWorkflowRequest) (*reviewsv1.ResumeProductReviewWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "resume product review workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	cloneProductReviewRequest := &reviewsv1.CloneProductReviewRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: req.GetProductReview().GetBillingAddress().GetRecipient(),
			},
			Counterparty: &customersv1.CustomerRef{
				CustomerId: req.GetProductReview().GetCounterparty().GetCustomerId(),
			},
		},
	}
	cloneProductReviewReply := new(reviewsv1.CloneProductReviewResponse)
	if err := grpcx.Invoke(ctx, s.conn, reviewsv1.ProductReviewService_CloneProductReview_FullMethodName, cloneProductReviewRequest, cloneProductReviewReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/CloneProductReview")
	}
	ctx = tenancy.With(ctx, "recipient", req.GetProductReview().GetBillingAddress().GetRecipient())
	cloned := proto.Clone(req.GetProductReview()).(*reviewsv1.ProductReview)
	packed, err := anypb.New(req.GetProductReview())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareReviewVoteOptionsRequest := &reviewsv1.CompareReviewVoteOptionsRequest{}
	compareReviewVoteOptionsRequest.Context = req.GetContext()
	compareReviewVoteOptionsRequest.ReviewVote = &reviewsv1.ReviewVote{
		Subject: &customersv1.CustomerRef{
			CustomerId: cloneProductReviewReply.GetProductReview().GetCounterparty().GetCustomerId(),
		},
		Detail: packed,
	}
	compareReviewVoteOptionsRequest.Details = &reviewsv1.CompareReviewVoteOptionsRequest_Details{
		Destination: &commonv1.PostalAddress{
			Recipient:   tenancy.Value(ctx, "recipient"),
			CountryCode: cloned.GetBillingAddress().GetCountryCode(),
		},
	}
	compareReviewVoteOptionsReply, err := s.reviewVoteCalculationClient.CompareReviewVoteOptions(ctx, compareReviewVoteOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/CompareReviewVoteOptions")
	}
	listProductReviewsRequest := &reviewsv1.ListProductReviewsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.ListProductReviewsRequest_Filter{
			Region:       cloneProductReviewReply.GetProductReview().GetBillingAddress().GetRegion(),
			CreatedAfter: cloneProductReviewReply.GetProductReview().GetAudit().GetCreatedAt(),
			Labels:       compareReviewVoteOptionsReply.GetResult().GetDetails(),
			OwnerId:      compareReviewVoteOptionsReply.GetReviewVote().GetSubject().GetCustomerId(),
		},
	}
	listProductReviewsReply, err := s.productReviewPort.ListProductReviews(ctx, listProductReviewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/ListProductReviews")
	}
	listPlacementWorkflowsRequest := &merchandisingv1.ListPlacementWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &merchandisingv1.ListPlacementWorkflowsRequest_Filter{
			Region:       req.GetProductReview().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetProductReview().GetAudit().GetCreatedAt(),
			OwnerId:      cloneProductReviewReply.GetProductReview().GetCounterparty().GetCustomerId(),
		},
	}
	var listPlacementWorkflowsReply *merchandisingv1.ListPlacementWorkflowsResponse
	if err := pipeline.Run(ctx, "acme.merchandising.v1.PlacementWorkflowService/ListPlacementWorkflows", func(ctx context.Context) error {
		answer, err := s.placementWorkflowClient.ListPlacementWorkflows(ctx, listPlacementWorkflowsRequest)
		listPlacementWorkflowsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/ListPlacementWorkflows")
	}
	return &reviewsv1.ResumeProductReviewWorkflowResponse{
		ProductReview: &reviewsv1.ProductReview{
			BillingAddress:  cloneProductReviewReply.GetProductReview().GetBillingAddress(),
			ProductReviewId: cloneProductReviewReply.GetProductReview().GetProductReviewId(),
			Total:           req.GetProductReview().GetTotal(),
			Number:          req.GetProductReview().GetNumber(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  compareReviewVoteOptionsReply.GetResult().GetReferenceIds(),
			CorrelationId: listProductReviewsReply.GetResult().GetCorrelationId(),
			Details:       listPlacementWorkflowsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Outcome: &reviewsv1.ResumeProductReviewWorkflowResponse_Outcome{
			Status: compareReviewVoteOptionsReply.GetResult().GetStatus(),
		},
	}, nil
}

// InspectProductReviewWorkflow returns a product review.
func (s *ProductReviewWorkflowServer) InspectProductReviewWorkflow(ctx context.Context, req *reviewsv1.InspectProductReviewWorkflowRequest) (*reviewsv1.InspectProductReviewWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_review", req.GetProductReviewId(), "inspect_product_review_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect product review workflow", "tenant", req.GetContext().GetTenantId())
	reviewModerationValidation, ok := s.clients.Get("acme.reviews.v1.ReviewModerationValidationService").(reviewsv1.ReviewModerationValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.reviews.v1.ReviewModerationValidationService")
	}
	validateReviewModerationRequest := &reviewsv1.ValidateReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		Details: &reviewsv1.ValidateReviewModerationRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	validateReviewModerationReply, err := reviewModerationValidation.ValidateReviewModeration(ctx, validateReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationValidationService/ValidateReviewModeration")
	}
	return &reviewsv1.InspectProductReviewWorkflowResponse{
		ProductReview: &reviewsv1.ProductReview{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: validateReviewModerationReply.GetReviewModeration().GetAssignee().GetDisplayName(),
			},
			ProductReviewId: req.GetProductReviewId(),
		},
		Result: &commonv1.OperationResult{
			Status: validateReviewModerationReply.GetResult().GetStatus(),
		},
	}, nil
}

// ListProductReviewWorkflows lists product reviews a page at a time.
func (s *ProductReviewWorkflowServer) ListProductReviewWorkflows(ctx context.Context, req *reviewsv1.ListProductReviewWorkflowsRequest) (*reviewsv1.ListProductReviewWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListProductReviewWorkflows", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list product review workflows", "tenant", req.GetContext().GetTenantId())
	recalculateReviewModerationsRequest := &reviewsv1.RecalculateReviewModerationsRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.RecalculateReviewModerationsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	recalculateReviewModerationsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*reviewsv1.RecalculateReviewModerationsResponse, error) {
		return s.reviewModerationCalculationClient.RecalculateReviewModerations(ctx, recalculateReviewModerationsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/RecalculateReviewModerations")
	}
	return &reviewsv1.ListProductReviewWorkflowsResponse{
		ProductReviews: []*reviewsv1.ProductReview{&reviewsv1.ProductReview{
			Total: recalculateReviewModerationsReply.GetBreakdown().GetTotal(),
		}},
		Result: &commonv1.OperationResult{
			ReferenceIds: recalculateReviewModerationsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// SignalProductReviewWorkflow signals a product review.
func (s *ProductReviewWorkflowServer) SignalProductReviewWorkflow(ctx context.Context, req *reviewsv1.SignalProductReviewWorkflowRequest) (*reviewsv1.SignalProductReviewWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "signal product review workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	createProductReviewRequest := &reviewsv1.CreateProductReviewRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetProductReview().GetBillingAddress().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetProductReview().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetProductReview().GetAudit().GetCreatedBy(),
				Etag:      req.GetProductReview().GetAudit().GetEtag(),
			},
		},
	}
	createProductReviewReply, err := s.meteredProductReview.CreateProductReview(ctx, createProductReviewRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/CreateProductReview")
	}
	return &reviewsv1.SignalProductReviewWorkflowResponse{
		ProductReview: &reviewsv1.ProductReview{
			Counterparty: &customersv1.CustomerRef{
				CustomerId: createProductReviewReply.GetProductReview().GetCounterparty().GetCustomerId(),
			},
			ProductReviewId: req.GetProductReviewId(),
			Status:          req.GetProductReview().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// productReviewWorkflowServerStartProductReviewWorkflowQuoteCustomerSegmentStep carries the request of one step of
// StartProductReviewWorkflow and, once it ran, its reply.
type productReviewWorkflowServerStartProductReviewWorkflowQuoteCustomerSegmentStep struct {
	request *customersv1.QuoteCustomerSegmentRequest
	reply   *customersv1.QuoteCustomerSegmentResponse
}

func (s *ProductReviewWorkflowServer) runStartProductReviewWorkflowQuoteCustomerSegment(ctx context.Context, step *productReviewWorkflowServerStartProductReviewWorkflowQuoteCustomerSegmentStep) error {
	reply, err := s.customerSegmentCalculationClient.QuoteCustomerSegment(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// productReviewWorkflowServerAdvanceProductReviewWorkflowCommentGiftCardTransactionApprovalStep carries the request of
// one step of AdvanceProductReviewWorkflow and, once it ran, its reply.
type productReviewWorkflowServerAdvanceProductReviewWorkflowCommentGiftCardTransactionApprovalStep struct {
	request *giftcardsv1.CommentGiftCardTransactionApprovalRequest
	reply   *giftcardsv1.CommentGiftCardTransactionApprovalResponse
}

func (s *ProductReviewWorkflowServer) runAdvanceProductReviewWorkflowCommentGiftCardTransactionApproval(ctx context.Context, step *productReviewWorkflowServerAdvanceProductReviewWorkflowCommentGiftCardTransactionApprovalStep) error {
	reply, err := s.giftCardTransactionApprovalClient.CommentGiftCardTransactionApproval(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// sendApiCredentialTestNoticeSender returns a function that sends one request to
// acme.identity.v1.ApiCredentialNotificationService/SendApiCredentialTestNotice.
func (s *ProductReviewWorkflowServer) sendApiCredentialTestNoticeSender(request *identityv1.SendApiCredentialTestNoticeRequest) func(context.Context) (*identityv1.SendApiCredentialTestNoticeResponse, error) {
	return func(ctx context.Context) (*identityv1.SendApiCredentialTestNoticeResponse, error) {
		return s.apiCredentialNotificationClient.SendApiCredentialTestNotice(ctx, request)
	}
}
