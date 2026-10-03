package cart

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	telehealthv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telehealth/v1"
	yardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/yards/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SavedListWorkflowServer implements `acme.cart.v1.SavedListWorkflowService`.
// Runs the long-lived workflow that takes a saved list from start to finish.
type SavedListWorkflowServer struct {
	cartv1.UnimplementedSavedListWorkflowServiceServer
	store                          *Store
	log                            *slog.Logger
	cartRuleApprovalClient         cartv1.CartRuleApprovalServiceClient
	conn                           *grpc.ClientConn
	cartValidationClient           cartv1.CartValidationServiceClient
	clients                        *registry.Registry
	quoteCartRuleFn                func(context.Context, *cartv1.QuoteCartRuleRequest, ...grpc.CallOption) (*cartv1.QuoteCartRuleResponse, error)
	savedListValidationClient      cartv1.SavedListValidationServiceClient
	yardCalculationPort            yardCalculationPort
	giftCardApprovalClient         giftcardsv1.GiftCardApprovalServiceClient
	meteredApiCredentialValidation meteredApiCredentialValidation
	userQueryClient                identityv1.UserQueryServiceClient
	userQueryClientFallback        identityv1.UserQueryServiceClient
	cartWorkflowClient             cartv1.CartWorkflowServiceClient
	cartRuleValidationClient       cartv1.CartRuleValidationServiceClient
	meteredUsageRecordApproval     meteredUsageRecordApproval
	cartRuleCalculationClient      cartv1.CartRuleCalculationServiceClient
	sponsorClient                  sponsorshipsv1.SponsorServiceClient
}

// NewSavedListWorkflowServer wires a SavedListWorkflowServer.
func NewSavedListWorkflowServer(deps platform.Deps) *SavedListWorkflowServer {
	cartRuleCalculation := cartv1.NewCartRuleCalculationServiceClient(deps.Conn)
	return &SavedListWorkflowServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.cart.v1.SavedListWorkflowService"),
		cartRuleApprovalClient:         cartv1.NewCartRuleApprovalServiceClient(deps.Conn),
		conn:                           deps.Conn,
		cartValidationClient:           cartv1.NewCartValidationServiceClient(deps.Conn),
		clients:                        deps.Clients,
		quoteCartRuleFn:                cartRuleCalculation.QuoteCartRule,
		savedListValidationClient:      cartv1.NewSavedListValidationServiceClient(deps.Conn),
		yardCalculationPort:            newYardCalculationPort(deps),
		giftCardApprovalClient:         giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		meteredApiCredentialValidation: meteredApiCredentialValidation{ApiCredentialValidationServiceClient: identityv1.NewApiCredentialValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		userQueryClient:                identityv1.NewUserQueryServiceClient(deps.Conn),
		userQueryClientFallback:        identityv1.NewUserQueryServiceClient(deps.Conn),
		cartWorkflowClient:             cartv1.NewCartWorkflowServiceClient(deps.Conn),
		cartRuleValidationClient:       cartv1.NewCartRuleValidationServiceClient(deps.Conn),
		meteredUsageRecordApproval:     meteredUsageRecordApproval{UsageRecordApprovalServiceClient: subscriptionsv1.NewUsageRecordApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		cartRuleCalculationClient:      cartv1.NewCartRuleCalculationServiceClient(deps.Conn),
		sponsorClient:                  sponsorshipsv1.NewSponsorServiceClient(deps.Conn),
	}
}

// StartSavedListWorkflow starts a saved list.
func (s *SavedListWorkflowServer) StartSavedListWorkflow(ctx context.Context, req *cartv1.StartSavedListWorkflowRequest) (*cartv1.StartSavedListWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "start saved list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	source := req.GetSavedList().GetCounterparty().ProtoReflect()
	cartRuleId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("cart_rule_id"))).String()
	recallCartRuleRequest := &cartv1.RecallCartRuleRequest{
		Context: req.GetContext(),
		Details: &cartv1.RecallCartRuleRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetSavedList().GetCounterparty().GetDisplayName(),
			},
			Parameters: req.GetDetails().GetParameters(),
		},
		CartRuleId: cartRuleId,
	}
	type recallCartRuleResult struct {
		reply *cartv1.RecallCartRuleResponse
		err   error
	}
	recallCartRuleResults := make(chan recallCartRuleResult, 1)
	go func() {
		reply, err := s.cartRuleApprovalClient.RecallCartRule(ctx, recallCartRuleRequest)
		recallCartRuleResults <- recallCartRuleResult{reply, err}
	}()
	var recallCartRuleReply *cartv1.RecallCartRuleResponse
	select {
	case result := <-recallCartRuleResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.cart.v1.CartRuleApprovalService/RecallCartRule")
		}
		recallCartRuleReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	var holderName string
	switch choice := req.GetSavedList().GetSettlement().(type) {
	case *cartv1.SavedList_Card:
		holderName = choice.Card.GetHolderName()
	case *cartv1.SavedList_Bank:
		holderName = choice.Bank.GetIban()
	case *cartv1.SavedList_VoucherCode:
		holderName = choice.VoucherCode
	}
	validateSavedListRequest := &cartv1.ValidateSavedListRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Settlement: &cartv1.SavedList_Card{Card: &cartv1.SavedListCardSettlement{HolderName: holderName}},
			Counterparty: &cartv1.CartRuleRef{
				DisplayName: recallCartRuleReply.GetCartRule().GetOwner().GetDisplayName(),
			},
		},
	}
	validateSavedListReply := new(cartv1.ValidateSavedListResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.cart.v1.SavedListValidationService/ValidateSavedList", validateSavedListRequest, validateSavedListReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListValidationService/ValidateSavedList")
	}
	submitCartRuleRequest := buildSubmitCartRuleRequestForStartSavedListWorkflow(req, recallCartRuleReply)
	submitCartRuleReply, err := s.cartRuleApprovalClient.SubmitCartRule(ctx, submitCartRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleApprovalService/SubmitCartRule")
	}
	for _, attachment := range req.GetSavedList().GetAttachments() {
		if _, err := s.cartValidationClient.ScoreCart(ctx, &cartv1.ScoreCartRequest{
			Context: req.GetContext(),
			Cart: &cartv1.Cart{
				Attachments: []*cartv1.CartAttachment{&cartv1.CartAttachment{
					ContentType: attachment.GetContentType(),
					Checksum:    attachment.GetChecksum(),
					SizeBytes:   attachment.GetSizeBytes(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/ScoreCart")
		}
	}
	return &cartv1.StartSavedListWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details:      recallCartRuleReply.GetResult().GetDetails(),
			ReferenceIds: validateSavedListReply.GetResult().GetReferenceIds(),
			Status:       submitCartRuleReply.GetOutcome().GetStatus(),
		},
		SavedList: &cartv1.SavedList{
			SavedListId: req.GetSavedListId(),
			Notes:       req.GetSavedList().GetNotes(),
		},
	}, nil
}

// AdvanceSavedListWorkflow advances a saved list.
func (s *SavedListWorkflowServer) AdvanceSavedListWorkflow(ctx context.Context, req *cartv1.AdvanceSavedListWorkflowRequest) (*cartv1.AdvanceSavedListWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "advance saved list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	notification, ok := s.clients.Get("acme.notifications.v1.NotificationService").(notificationsv1.NotificationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.notifications.v1.NotificationService")
	}
	listNotificationsRequest := &notificationsv1.ListNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.ListNotificationsRequest_Filter{
			Region:       req.GetSavedList().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetSavedList().GetAudit().GetUpdatedAt(),
		},
	}
	listNotificationsReply, err := notification.ListNotifications(ctx, listNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/ListNotifications")
	}
	return &cartv1.AdvanceSavedListWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:       listNotificationsReply.GetResult().GetStatus(),
			ReferenceIds: listNotificationsReply.GetResult().GetReferenceIds(),
		},
		SavedList: &cartv1.SavedList{
			SavedListId:  req.GetSavedListId(),
			Counterparty: req.GetSavedList().GetCounterparty(),
		},
	}, nil
}

// CancelSavedListWorkflow cancels a saved list.
func (s *SavedListWorkflowServer) CancelSavedListWorkflow(ctx context.Context, req *cartv1.CancelSavedListWorkflowRequest) (*cartv1.CancelSavedListWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CancelSavedListWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "cancel saved list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetSavedList()).(*cartv1.SavedList)
	quoteCartRuleRequest := &cartv1.QuoteCartRuleRequest{
		Context: req.GetContext(),
		Details: &cartv1.QuoteCartRuleRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:     req.GetSavedList().GetBillingAddress().GetRegion(),
				PostalCode: cloned.GetBillingAddress().GetPostalCode(),
			},
		},
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetSavedList().GetCounterparty().GetDisplayName(),
			},
		},
	}
	quoteCartRuleReply, err := s.quoteCartRuleFn(ctx, quoteCartRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/QuoteCartRule")
	}
	for _, attachment := range req.GetSavedList().GetAttachments() {
		if _, err := s.savedListValidationClient.ValidateSavedList(ctx, &cartv1.ValidateSavedListRequest{
			Context: req.GetContext(),
			SavedList: &cartv1.SavedList{
				Attachments: []*cartv1.SavedListAttachment{&cartv1.SavedListAttachment{ContentType: attachment.GetContentType()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListValidationService/ValidateSavedList")
		}
	}
	explainYardCalculationRequest := &yardsv1.ExplainYardCalculationRequest{
		Context: req.GetContext(),
		Yard: &yardsv1.Yard{
			Owner: &identityv1.UserRef{
				DisplayName: quoteCartRuleReply.GetCartRule().GetOwner().GetDisplayName(),
			},
		},
		Details: &yardsv1.ExplainYardCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: quoteCartRuleReply.GetCartRule().GetOwner().GetEmail(),
			},
		},
	}
	explainYardCalculationReply, err := s.yardCalculationPort.ExplainYardCalculation(ctx, explainYardCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.yards.v1.YardCalculationService/ExplainYardCalculation")
	}
	return &cartv1.CancelSavedListWorkflowResponse{
		Outcome: &cartv1.CancelSavedListWorkflowResponse_Outcome{Status: quoteCartRuleReply.GetResult().GetStatus()},
		SavedList: &cartv1.SavedList{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: explainYardCalculationReply.GetYard().GetAddress().GetCountryCode(),
				Locality:    explainYardCalculationReply.GetYard().GetAddress().GetLocality(),
			},
			SavedListId:  req.GetSavedListId(),
			Counterparty: req.GetSavedList().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetrySavedListWorkflow retrys a saved list.
func (s *SavedListWorkflowServer) RetrySavedListWorkflow(ctx context.Context, req *cartv1.RetrySavedListWorkflowRequest) (*cartv1.RetrySavedListWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "retry saved list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	for _, line := range req.GetSavedList().GetLines() {
		if _, err := s.giftCardApprovalClient.RecallGiftCard(ctx, &giftcardsv1.RecallGiftCardRequest{
			Context: req.GetContext(),
			GiftCard: &giftcardsv1.GiftCard{
				Limits:  []*giftcardsv1.GiftCardLimit{&giftcardsv1.GiftCardLimit{Amount: line.GetAmount()}},
				Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/RecallGiftCard")
		}
	}
	recalculateVideoVisitsRequest := &telehealthv1.RecalculateVideoVisitsRequest{Context: req.GetContext()}
	recalculateVideoVisitsReply := new(telehealthv1.RecalculateVideoVisitsResponse)
	if err := s.conn.Invoke(ctx, telehealthVideoVisitCalculationServiceRecalculateVideoVisitsMethod, recalculateVideoVisitsRequest, recalculateVideoVisitsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.telehealth.v1.VideoVisitCalculationService/RecalculateVideoVisits")
	}
	return &cartv1.RetrySavedListWorkflowResponse{
		Outcome: &cartv1.RetrySavedListWorkflowResponse_Outcome{Status: recalculateVideoVisitsReply.GetResult().GetStatus()},
		SavedList: &cartv1.SavedList{
			Audit: &commonv1.AuditInfo{
				Etag: recalculateVideoVisitsReply.GetVideoVisit().GetAudit().GetEtag(),
			},
			SavedListId: req.GetSavedListId(),
			Total:       req.GetSavedList().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseSavedListWorkflow pauses a saved list.
func (s *SavedListWorkflowServer) PauseSavedListWorkflow(ctx context.Context, req *cartv1.PauseSavedListWorkflowRequest) (*cartv1.PauseSavedListWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "pause saved list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	var holder struct{ Phone string }
	if record := req.GetDetails().GetNotify(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Phone"); err != nil {
			s.log.WarnContext(ctx, "copy phone", "error", err)
		}
	}
	previewApiCredentialRequest := &identityv1.PreviewApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Funding: &identityv1.ApiCredential_Card{Card: &identityv1.ApiCredentialCardFunding{
				HolderName: req.GetSavedList().GetCard().GetHolderName(),
				Last4:      req.GetSavedList().GetCard().GetLast4(),
			}},
		},
		Details: &identityv1.PreviewApiCredentialRequest_Details{Contact: &commonv1.ContactInfo{Phone: holder.Phone}},
	}
	previewApiCredentialReply, err := s.meteredApiCredentialValidation.PreviewApiCredential(ctx, previewApiCredentialRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/PreviewApiCredential")
	}
	routes := map[string]func(context.Context, *identityv1.SuggestUsersRequest, ...grpc.CallOption) (*identityv1.SuggestUsersResponse, error){
		"primary":  s.userQueryClient.SuggestUsers,
		"fallback": s.userQueryClientFallback.SuggestUsers,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	suggestUsersRequest := &identityv1.SuggestUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.SuggestUsersRequest_Filter{
			Region:       req.GetSavedList().GetBillingAddress().GetRegion(),
			Labels:       previewApiCredentialReply.GetApiCredential().GetLabels(),
			CreatedAfter: previewApiCredentialReply.GetOutcome().GetEffectiveAt(),
		},
	}
	suggestUsersReply, err := route(ctx, suggestUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/SuggestUsers")
	}
	return &cartv1.PauseSavedListWorkflowResponse{
		Outcome: &cartv1.PauseSavedListWorkflowResponse_Outcome{
			ReferenceId: previewApiCredentialReply.GetOutcome().GetReferenceId(),
			Status:      suggestUsersReply.GetResult().GetStatus(),
			Message:     suggestUsersReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: previewApiCredentialReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		SavedList: &cartv1.SavedList{
			SavedListId: req.GetSavedListId(),
			Metadata:    req.GetSavedList().GetMetadata(),
		},
	}, nil
}

// ResumeSavedListWorkflow resumes a saved list.
func (s *SavedListWorkflowServer) ResumeSavedListWorkflow(ctx context.Context, req *cartv1.ResumeSavedListWorkflowRequest) (*cartv1.ResumeSavedListWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "resume saved list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	approveCartRuleRequest := buildApproveCartRuleRequestForResumeSavedListWorkflow(req)
	approveCartRuleReply, err := s.cartRuleApprovalClient.ApproveCartRule(ctx, approveCartRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleApprovalService/ApproveCartRule")
	}
	listApiCredentialSubscriptionsRequest := &identityv1.ListApiCredentialSubscriptionsRequest{
		Context: req.GetContext(),
		Filter:  &identityv1.ListApiCredentialSubscriptionsRequest_Filter{Region: req.GetSavedList().GetBillingAddress().GetRegion()},
	}
	listApiCredentialSubscriptionsReply, err := listApiCredentialSubscriptionsCall(ctx, listApiCredentialSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/ListApiCredentialSubscriptions")
	}
	step := &savedListWorkflowServerResumeSavedListWorkflowResumeCartWorkflowStep{request: &cartv1.ResumeCartWorkflowRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetSavedList().GetAudit().GetCreatedAt()},
		},
	}}
	if err := s.runResumeSavedListWorkflowResumeCartWorkflow(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartWorkflowService/ResumeCartWorkflow")
	}
	resumeCartWorkflowReply := step.reply
	jobs := make(chan *cartv1.SavedListLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.cartRuleValidationClient.LintCartRules(workCtx, &cartv1.LintCartRulesRequest{
					Context: req.GetContext(),
					CartRule: &cartv1.CartRule{
						Action: &cartv1.CartRule_Discount{Discount: &cartv1.CartRuleDiscountAction{Amount: line.GetAmount()}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetSavedList().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleValidationService/LintCartRules")
	}
	_ = listApiCredentialSubscriptionsReply
	return &cartv1.ResumeSavedListWorkflowResponse{
		Outcome: &cartv1.ResumeSavedListWorkflowResponse_Outcome{Status: approveCartRuleReply.GetResult().GetStatus()},
		SavedList: &cartv1.SavedList{
			BillingAddress: &commonv1.PostalAddress{
				Recipient:   resumeCartWorkflowReply.GetCart().GetBillingAddress().GetRecipient(),
				CountryCode: resumeCartWorkflowReply.GetCart().GetBillingAddress().GetCountryCode(),
			},
			SavedListId: req.GetSavedListId(),
			IssuedAt:    req.GetSavedList().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectSavedListWorkflow returns a saved list.
func (s *SavedListWorkflowServer) InspectSavedListWorkflow(ctx context.Context, req *cartv1.InspectSavedListWorkflowRequest) (*cartv1.InspectSavedListWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectSavedListWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "saved_list", req.GetSavedListId(), "inspect_saved_list_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect saved list workflow", "tenant", req.GetContext().GetTenantId())
	commentUsageRecordApprovalRequest := &subscriptionsv1.CommentUsageRecordApprovalRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.CommentUsageRecordApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		UsageRecord: &subscriptionsv1.UsageRecord{Source: req.GetSavedListId()},
	}
	commentUsageRecordApprovalReply, err := s.meteredUsageRecordApproval.CommentUsageRecordApproval(ctx, commentUsageRecordApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordApprovalService/CommentUsageRecordApproval")
	}
	return &cartv1.InspectSavedListWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: commentUsageRecordApprovalReply.GetUsageRecord().GetCorrelationId(),
			Status:        "ok",
		},
		SavedList: &cartv1.SavedList{SavedListId: req.GetSavedListId()},
	}, nil
}

// ListSavedListWorkflows lists saved lists a page at a time.
func (s *SavedListWorkflowServer) ListSavedListWorkflows(ctx context.Context, req *cartv1.ListSavedListWorkflowsRequest) (*cartv1.ListSavedListWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list saved list workflows", "tenant", req.GetContext().GetTenantId())
	lookupUserRequest := &identityv1.LookupUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetContext().GetActor().GetUserId(),
	}
	send := s.lookupUserSender(lookupUserRequest)
	lookupUserReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/LookupUser")
	}
	pauseSellerWorkflowRequest := &sellersv1.PauseSellerWorkflowRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: lookupUserReply.GetUser().GetPrimaryAddress().GetPostalCode(),
				Lines:      lookupUserReply.GetUser().GetPrimaryAddress().GetLines(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: lookupUserReply.GetUser().GetAudit().GetCreatedAt(),
			},
			Contact: &commonv1.ContactInfo{
				Email: lookupUserReply.GetUser().GetRelationshipManager().GetEmail(),
			},
		},
	}
	pauseSellerWorkflowReply := new(sellersv1.PauseSellerWorkflowResponse)
	if err := s.conn.Invoke(ctx, sellersSellerWorkflowServicePauseSellerWorkflowMethod, pauseSellerWorkflowRequest, pauseSellerWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerWorkflowService/PauseSellerWorkflow")
	}
	return &cartv1.ListSavedListWorkflowsResponse{
		Result: &commonv1.OperationResult{
			Message: lookupUserReply.GetResult().GetMessage(),
			Details: pauseSellerWorkflowReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SavedLists: []*cartv1.SavedList{&cartv1.SavedList{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: pauseSellerWorkflowReply.GetSeller().GetPrimaryAddress().GetRecipient(),
			},
		}},
	}, nil
}

// SignalSavedListWorkflow signals a saved list.
func (s *SavedListWorkflowServer) SignalSavedListWorkflow(ctx context.Context, req *cartv1.SignalSavedListWorkflowRequest) (*cartv1.SignalSavedListWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "signal saved list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*cartv1.ExplainCartRuleCalculationResponse, len(req.GetSavedList().GetLines()))
	for i, line := range req.GetSavedList().GetLines() {
		group.Go(func() error {
			reply, err := s.cartRuleCalculationClient.ExplainCartRuleCalculation(groupCtx, &cartv1.ExplainCartRuleCalculationRequest{
				Context: req.GetContext(),
				CartRule: &cartv1.CartRule{
					Description: line.GetDescription(),
					Action:      &cartv1.CartRule_Discount{Discount: &cartv1.CartRuleDiscountAction{Amount: line.GetAmount()}},
					Audit:       &commonv1.AuditInfo{Etag: req.GetSavedList().GetAudit().GetEtag()},
				},
				Details: &cartv1.ExplainCartRuleCalculationRequest_Details{
					Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/ExplainCartRuleCalculation")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	detailsCache := cache.New[string, *cartv1.SignalSavedListWorkflowRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	watchSponsorsStream, err := s.sponsorClient.WatchSponsors(ctx, &sponsorshipsv1.WatchSponsorsRequest{
		Context: req.GetContext(),
		Filter:  &sponsorshipsv1.WatchSponsorsRequest_Filter{Region: req.GetSavedList().GetBillingAddress().GetRegion()},
		Subscriber: &commonv1.ContactInfo{
			Name:             cachedDetails.GetNotify().GetName(),
			PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			Phone:            req.GetDetails().GetNotify().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sponsorships.v1.SponsorService/WatchSponsors")
	}
	for {
		received, err := watchSponsorsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sponsorships.v1.SponsorService/WatchSponsors")
		}
		_ = received
	}
	return &cartv1.SignalSavedListWorkflowResponse{
		SavedList: &cartv1.SavedList{
			SavedListId: req.GetSavedListId(),
			DueAt:       req.GetSavedList().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// savedListWorkflowServerResumeSavedListWorkflowResumeCartWorkflowStep carries the request of one step of
// ResumeSavedListWorkflow and, once it ran, its reply.
type savedListWorkflowServerResumeSavedListWorkflowResumeCartWorkflowStep struct {
	request *cartv1.ResumeCartWorkflowRequest
	reply   *cartv1.ResumeCartWorkflowResponse
}

func (s *SavedListWorkflowServer) runResumeSavedListWorkflowResumeCartWorkflow(ctx context.Context, step *savedListWorkflowServerResumeSavedListWorkflowResumeCartWorkflowStep) error {
	reply, err := s.cartWorkflowClient.ResumeCartWorkflow(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// lookupUserSender returns a function that sends one request to acme.identity.v1.UserQueryService/LookupUser.
func (s *SavedListWorkflowServer) lookupUserSender(request *identityv1.LookupUserRequest) func(context.Context) (*identityv1.LookupUserResponse, error) {
	return func(ctx context.Context) (*identityv1.LookupUserResponse, error) {
		return s.userQueryClient.LookupUser(ctx, request)
	}
}
