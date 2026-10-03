package partnerapi

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fanportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fanportal/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	patientappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patientapp/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerOrderApprovalServer implements `acme.partnerapi.v1.PartnerOrderApprovalService`.
// Routes partner orders through approval, with escalation and delegation.
type PartnerOrderApprovalServer struct {
	partnerapiv1.UnimplementedPartnerOrderApprovalServiceServer
	store                          *Store
	log                            *slog.Logger
	sellerApprovalClient           sellersv1.SellerApprovalServiceClient
	partnerWebhookValidationClient partnerapiv1.PartnerWebhookValidationServiceClient
	collectionWorkflowClient       merchandisingv1.CollectionWorkflowServiceClient
	cartRuleApprovalClient         cartv1.CartRuleApprovalServiceClient
	conn                           *grpc.ClientConn
	patientHomeClient              patientappv1.PatientHomeServiceClient
	savedListApprovalClient        cartv1.SavedListApprovalServiceClient
	meteredSellerLifecycle         meteredSellerLifecycle
	householdApprovalClient        customersv1.HouseholdApprovalServiceClient
	sellerLifecycleClient          sellersv1.SellerLifecycleServiceClient
	synonymValidationPort          synonymValidationPort
	fanSessionQueryClient          fanportalv1.FanSessionQueryServiceClient
}

// NewPartnerOrderApprovalServer wires a PartnerOrderApprovalServer.
func NewPartnerOrderApprovalServer(deps platform.Deps) *PartnerOrderApprovalServer {
	return &PartnerOrderApprovalServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.partnerapi.v1.PartnerOrderApprovalService"),
		sellerApprovalClient:           sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		partnerWebhookValidationClient: partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn),
		collectionWorkflowClient:       merchandisingv1.NewCollectionWorkflowServiceClient(deps.Conn),
		cartRuleApprovalClient:         cartv1.NewCartRuleApprovalServiceClient(deps.Conn),
		conn:                           deps.Conn,
		patientHomeClient:              patientappv1.NewPatientHomeServiceClient(deps.Conn),
		savedListApprovalClient:        cartv1.NewSavedListApprovalServiceClient(deps.Conn),
		meteredSellerLifecycle:         meteredSellerLifecycle{SellerLifecycleServiceClient: sellersv1.NewSellerLifecycleServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		householdApprovalClient:        customersv1.NewHouseholdApprovalServiceClient(deps.Conn),
		sellerLifecycleClient:          sellersv1.NewSellerLifecycleServiceClient(deps.Conn),
		synonymValidationPort:          newSynonymValidationPort(deps),
		fanSessionQueryClient:          fanportalv1.NewFanSessionQueryServiceClient(deps.Conn),
	}
}

// SubmitPartnerOrder submits a partner order.
func (s *PartnerOrderApprovalServer) SubmitPartnerOrder(ctx context.Context, req *partnerapiv1.SubmitPartnerOrderRequest) (*partnerapiv1.SubmitPartnerOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "submit partner order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	step := &partnerOrderApprovalServerSubmitPartnerOrderRecallSellerStep{request: &sellersv1.RecallSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: req.GetPartnerOrder().GetBillingAddress().GetLocality(),
			},
		},
	}}
	if err := s.runSubmitPartnerOrderRecallSeller(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/RecallSeller")
	}
	recallSellerReply := step.reply
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := partnerOrderApprovalServerValidatePartnerWebhookSteps[mode]
	packed, err := anypb.New(req.GetPartnerOrder())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validatePartnerWebhookRequest := &partnerapiv1.ValidatePartnerWebhookRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: recallSellerReply.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &partnerapiv1.ValidatePartnerWebhookRequest_Details{
			Contact: &commonv1.ContactInfo{Name: recallSellerReply.GetSeller().GetContact().GetName()},
		},
	}
	validatePartnerWebhookReply, err := run(s, ctx, validatePartnerWebhookRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ValidatePartnerWebhook")
	}
	pauseCollectionWorkflowReply, err := s.collectionWorkflowClient.PauseCollectionWorkflow(ctx, &merchandisingv1.PauseCollectionWorkflowRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Location: &commonv1.PostalAddress{
						Recipient: recallSellerReply.GetSeller().GetPrimaryAddress().GetRecipient(),
					},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/PauseCollectionWorkflow")
	}
	normalizePartnerWebhookRequest := &partnerapiv1.NormalizePartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.NormalizePartnerWebhookRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetApprover().GetEmail(),
				Name:             recallSellerReply.GetSeller().GetContact().GetName(),
				PreferredChannel: recallSellerReply.GetSeller().GetContact().GetPreferredChannel(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				CurrencyCode: validatePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetCurrencyCode(),
			},
		},
	}
	send := s.normalizePartnerWebhookSender(normalizePartnerWebhookRequest)
	normalizePartnerWebhookReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/NormalizePartnerWebhook")
	}
	return &partnerapiv1.SubmitPartnerOrderResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Lines: []*partnerapiv1.PartnerOrderLine{&partnerapiv1.PartnerOrderLine{Attributes: recallSellerReply.GetSeller().GetAttributes()}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: pauseCollectionWorkflowReply.GetCollection().GetAudit().GetUpdatedAt(),
			},
			PartnerOrderId: req.GetPartnerOrderId(),
			Notes:          req.GetPartnerOrder().GetNotes(),
		},
		Result: &commonv1.OperationResult{
			Message:       validatePartnerWebhookReply.GetResult().GetMessage(),
			CorrelationId: validatePartnerWebhookReply.GetPartnerWebhook().GetCorrelationId(),
			ReferenceIds:  normalizePartnerWebhookReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Outcome: &partnerapiv1.SubmitPartnerOrderResponse_Outcome{
			Status: pauseCollectionWorkflowReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// ApprovePartnerOrder approves a partner order.
func (s *PartnerOrderApprovalServer) ApprovePartnerOrder(ctx context.Context, req *partnerapiv1.ApprovePartnerOrderRequest) (*partnerapiv1.ApprovePartnerOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "approve partner order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	source := req.GetPartnerOrder().GetCounterparty().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	approveCartRuleRequest := &cartv1.ApproveCartRuleRequest{}
	approveCartRuleRequest.Context = req.GetContext()
	approveCartRuleRequest.CartRule = &cartv1.CartRule{
		Owner: &identityv1.UserRef{
			DisplayName: displayName,
			Email:       req.GetPartnerOrder().GetCounterparty().GetEmail(),
			UserId:      req.GetDetails().GetApprover().GetUserId(),
		},
	}
	approveCartRuleReply, err := s.cartRuleApprovalClient.ApproveCartRule(ctx, approveCartRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleApprovalService/ApproveCartRule")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*partnerapiv1.ScorePartnerWebhookResponse, len(req.GetPartnerOrder().GetLines()))
	for i, line := range req.GetPartnerOrder().GetLines() {
		group.Go(func() error {
			reply, err := s.partnerWebhookValidationClient.ScorePartnerWebhook(groupCtx, &partnerapiv1.ScorePartnerWebhookRequest{
				Context:        req.GetContext(),
				PartnerWebhook: &partnerapiv1.PartnerWebhook{Attributes: line.GetAttributes()},
				Details: &partnerapiv1.ScorePartnerWebhookRequest_Details{
					Destination: &commonv1.PostalAddress{
						CountryCode: req.GetPartnerOrder().GetBillingAddress().GetCountryCode(),
					},
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
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ScorePartnerWebhook")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &partnerapiv1.ApprovePartnerOrderResponse{
		Result: &commonv1.OperationResult{
			Message: approveCartRuleReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			PartnerOrderId: req.GetPartnerOrderId(),
			Status:         req.GetPartnerOrder().GetStatus(),
		},
	}, nil
}

// RejectPartnerOrder rejects a partner order.
func (s *PartnerOrderApprovalServer) RejectPartnerOrder(ctx context.Context, req *partnerapiv1.RejectPartnerOrderRequest) (*partnerapiv1.RejectPartnerOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "reject partner order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	pauseCollectionWorkflowRequest := &merchandisingv1.PauseCollectionWorkflowRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Location: &commonv1.PostalAddress{
						Locality: req.GetPartnerOrder().GetBillingAddress().GetLocality(),
						Lines:    req.GetPartnerOrder().GetBillingAddress().GetLines(),
					},
					Manager: &identityv1.UserRef{
						DisplayName: req.GetPartnerOrder().GetCounterparty().GetDisplayName(),
						UserId:      req.GetDetails().GetApprover().GetUserId(),
					},
				}},
			}},
		},
	}
	pauseCollectionWorkflowReply := new(merchandisingv1.PauseCollectionWorkflowResponse)
	if err := grpcx.Invoke(ctx, s.conn, merchandisingv1.CollectionWorkflowService_PauseCollectionWorkflow_FullMethodName, pauseCollectionWorkflowRequest, pauseCollectionWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/PauseCollectionWorkflow")
	}
	batchGetPatientHomesReply, err := s.patientHomeClient.BatchGetPatientHomes(ctx, &patientappv1.BatchGetPatientHomesRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.patientapp.v1.PatientHomeService/BatchGetPatientHomes")
	}
	recallSavedListRequest := &cartv1.RecallSavedListRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			BillingAddress: &commonv1.PostalAddress{
				Locality: req.GetPartnerOrder().GetBillingAddress().GetLocality(),
			},
		},
	}
	recallSavedListReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*cartv1.RecallSavedListResponse, error) {
		return s.savedListApprovalClient.RecallSavedList(ctx, recallSavedListRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListApprovalService/RecallSavedList")
	}
	expireSellerRequest := &sellersv1.ExpireSellerRequest{
		Context:  req.GetContext(),
		SellerId: req.GetPartnerOrder().GetCounterparty().GetSellerId(),
		Details: &sellersv1.ExpireSellerRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
		},
	}
	expireSellerReply, err := s.meteredSellerLifecycle.ExpireSeller(ctx, expireSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerLifecycleService/ExpireSeller")
	}
	return &partnerapiv1.RejectPartnerOrderResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: pauseCollectionWorkflowReply.GetCollection().GetLocation().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: recallSavedListReply.GetSavedList().GetAudit().GetEtag(),
			},
			PartnerOrderId: req.GetPartnerOrderId(),
			Number:         req.GetPartnerOrder().GetNumber(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetPatientHomesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &partnerapiv1.RejectPartnerOrderResponse_Outcome{
			ReferenceId: expireSellerReply.GetOutcome().GetReferenceId(),
		},
	}, nil
}

// EscalatePartnerOrder escalates a partner order.
func (s *PartnerOrderApprovalServer) EscalatePartnerOrder(ctx context.Context, req *partnerapiv1.EscalatePartnerOrderRequest) (*partnerapiv1.EscalatePartnerOrderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "escalate partner order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	escalateHouseholdReply, err := s.householdApprovalClient.EscalateHousehold(ctx, &customersv1.EscalateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						CountryCode: req.GetPartnerOrder().GetBillingAddress().GetCountryCode(),
					},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdApprovalService/EscalateHousehold")
	}
	encoded, err := protojson.Marshal(req.GetPartnerOrder())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.PartnerOrder
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetPartnerOrder())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintPartnerWebhooksRequest := &partnerapiv1.LintPartnerWebhooksRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: req.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
			CorrelationId: escalateHouseholdReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
		Details: &partnerapiv1.LintPartnerWebhooksRequest_Details{
			Contact: &commonv1.ContactInfo{Email: (&decoded).GetCounterparty().GetEmail()},
		},
	}
	lintPartnerWebhooksReply, err := grpcx.Call(ctx, s.partnerWebhookValidationClient.LintPartnerWebhooks, lintPartnerWebhooksRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/LintPartnerWebhooks")
	}
	packed2, err := anypb.New(req.GetPartnerOrder())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewPartnerWebhookReply, err := s.partnerWebhookValidationClient.PreviewPartnerWebhook(ctx, &partnerapiv1.PreviewPartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.PreviewPartnerWebhookRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetPartnerOrder().GetCounterparty().GetEmail()},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				CurrencyCode: lintPartnerWebhooksReply.GetPartnerWebhook().GetSubject().GetCurrencyCode(),
				DisplayName:  req.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
			Kind:   lintPartnerWebhooksReply.GetPartnerWebhook().GetKind(),
			Detail: packed2,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/PreviewPartnerWebhook")
	}
	return &partnerapiv1.EscalatePartnerOrderResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: escalateHouseholdReply.GetHousehold().GetLocation().GetPostalCode(),
			},
			PartnerOrderId: req.GetPartnerOrderId(),
			Counterparty:   req.GetPartnerOrder().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: escalateHouseholdReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &partnerapiv1.EscalatePartnerOrderResponse_Outcome{Status: previewPartnerWebhookReply.GetResult().GetStatus()},
	}, nil
}

// ListPendingPartnerOrders lists partner orders a page at a time.
func (s *PartnerOrderApprovalServer) ListPendingPartnerOrders(ctx context.Context, req *partnerapiv1.ListPendingPartnerOrdersRequest) (*partnerapiv1.ListPendingPartnerOrdersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingPartnerOrders", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending partner orders", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.ListPendingPartnerOrdersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegatePartnerOrderApproval delegates a partner order.
func (s *PartnerOrderApprovalServer) DelegatePartnerOrderApproval(ctx context.Context, req *partnerapiv1.DelegatePartnerOrderApprovalRequest) (*partnerapiv1.DelegatePartnerOrderApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DelegatePartnerOrderApproval", "error", err)
		return nil, err
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "delegate partner order approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	var holder struct{ DisplayName string }
	if record := req.GetDetails().GetApprover(); record != nil {
		if err := mapper.CopyFields(&holder, record, "DisplayName"); err != nil {
			s.log.WarnContext(ctx, "copy display_name", "error", err)
		}
	}
	lintPartnerWebhooksRequest := &partnerapiv1.LintPartnerWebhooksRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.LintPartnerWebhooksRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetPartnerOrder().GetCounterparty().GetEmail()},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{DisplayName: holder.DisplayName},
		},
	}
	send := s.lintPartnerWebhooksSender(lintPartnerWebhooksRequest)
	lintPartnerWebhooksReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/LintPartnerWebhooks")
	}
	cloned := proto.Clone(req.GetPartnerOrder()).(*partnerapiv1.PartnerOrder)
	scorePartnerWebhookRequest := newScorePartnerWebhookRequest(withScorePartnerWebhookContext(req.GetContext()), withScorePartnerWebhookDetails(&partnerapiv1.ScorePartnerWebhookRequest_Details{
		Destination: &commonv1.PostalAddress{
			CountryCode: req.GetPartnerOrder().GetBillingAddress().GetCountryCode(),
			Locality:    cloned.GetBillingAddress().GetLocality(),
		},
	}))
	scorePartnerWebhookReply, err := s.partnerWebhookValidationClient.ScorePartnerWebhook(ctx, scorePartnerWebhookRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ScorePartnerWebhook")
	}
	terminateSellerRequest := &sellersv1.TerminateSellerRequest{
		Context:  req.GetContext(),
		SellerId: req.GetPartnerOrder().GetCounterparty().GetSellerId(),
		Details: &sellersv1.TerminateSellerRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: lintPartnerWebhooksReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
			NotifyContact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	terminateSellerReply, err := sellersv1.SellerLifecycleServiceClient.TerminateSeller(s.sellerLifecycleClient, ctx, terminateSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerLifecycleService/TerminateSeller")
	}
	_ = terminateSellerReply
	return &partnerapiv1.DelegatePartnerOrderApprovalResponse{
		Outcome: &partnerapiv1.DelegatePartnerOrderApprovalResponse_Outcome{
			ReferenceId: lintPartnerWebhooksReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: lintPartnerWebhooksReply.GetResult().GetCorrelationId(),
			Details:       scorePartnerWebhookReply.GetResult().GetDetails(),
			Message:       scorePartnerWebhookReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			PartnerOrderId: req.GetPartnerOrderId(),
			IssuedAt:       req.GetPartnerOrder().GetIssuedAt(),
		},
	}, nil
}

// RecallPartnerOrder recalls a partner order.
func (s *PartnerOrderApprovalServer) RecallPartnerOrder(ctx context.Context, req *partnerapiv1.RecallPartnerOrderRequest) (*partnerapiv1.RecallPartnerOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "recall partner order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetPartnerOrder())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizePartnerWebhookRequest := &partnerapiv1.NormalizePartnerWebhookRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &partnerapiv1.NormalizePartnerWebhookRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApproverEmail()},
		},
	}
	type normalizePartnerWebhookResult struct {
		reply *partnerapiv1.NormalizePartnerWebhookResponse
		err   error
	}
	normalizePartnerWebhookResults := make(chan normalizePartnerWebhookResult, 1)
	go func() {
		reply, err := s.partnerWebhookValidationClient.NormalizePartnerWebhook(ctx, normalizePartnerWebhookRequest)
		normalizePartnerWebhookResults <- normalizePartnerWebhookResult{reply, err}
	}()
	var normalizePartnerWebhookReply *partnerapiv1.NormalizePartnerWebhookResponse
	select {
	case result := <-normalizePartnerWebhookResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.partnerapi.v1.PartnerWebhookValidationService/NormalizePartnerWebhook")
		}
		normalizePartnerWebhookReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	batchGetAccessPoliciesRequest := &authorizationv1.BatchGetAccessPoliciesRequest{Context: req.GetContext()}
	batchGetAccessPoliciesReply := new(authorizationv1.BatchGetAccessPoliciesResponse)
	if err := s.conn.Invoke(ctx, authorizationAccessPolicyServiceBatchGetAccessPoliciesMethod, batchGetAccessPoliciesRequest, batchGetAccessPoliciesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/BatchGetAccessPolicies")
	}
	packed2, err := anypb.New(req.GetPartnerOrder())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainPartnerWebhookDecisionRequest := &partnerapiv1.ExplainPartnerWebhookDecisionRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				CurrencyCode: normalizePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetCurrencyCode(),
				DisplayName:  normalizePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
			Detail: packed2,
		},
		Details: &partnerapiv1.ExplainPartnerWebhookDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetPartnerOrder().GetCounterparty().GetEmail()},
		},
	}
	explainPartnerWebhookDecisionReply, err := partnerapiv1.PartnerWebhookValidationServiceClient.ExplainPartnerWebhookDecision(s.partnerWebhookValidationClient, ctx, explainPartnerWebhookDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ExplainPartnerWebhookDecision")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetApprover().GetEmail())
	previewPartnerWebhookReply, err := s.partnerWebhookValidationClient.PreviewPartnerWebhook(ctx, &partnerapiv1.PreviewPartnerWebhookRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				CurrencyCode:         explainPartnerWebhookDecisionReply.GetPartnerWebhook().GetSubject().GetCurrencyCode(),
				DisplayName:          req.GetDetails().GetApprover().GetDisplayName(),
				PartnerIntegrationId: normalizePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetPartnerIntegrationId(),
			},
		},
		Details: &partnerapiv1.PreviewPartnerWebhookRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/PreviewPartnerWebhook")
	}
	_ = previewPartnerWebhookReply
	return &partnerapiv1.RecallPartnerOrderResponse{
		Outcome: &partnerapiv1.RecallPartnerOrderResponse_Outcome{
			Message: normalizePartnerWebhookReply.GetResult().GetMessage(),
			Status:  normalizePartnerWebhookReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: batchGetAccessPoliciesReply.GetResult().GetReferenceIds(),
			Status:       explainPartnerWebhookDecisionReply.GetOutcome().GetStatus(),
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Lines: []*partnerapiv1.PartnerOrderLine{&partnerapiv1.PartnerOrderLine{
				Attributes: explainPartnerWebhookDecisionReply.GetPartnerWebhook().GetAttributes(),
			}},
			PartnerOrderId: req.GetPartnerOrderId(),
			Total:          req.GetPartnerOrder().GetTotal(),
		},
	}, nil
}

// GetPartnerOrderApprovalStatus returns a partner order.
func (s *PartnerOrderApprovalServer) GetPartnerOrderApprovalStatus(ctx context.Context, req *partnerapiv1.GetPartnerOrderApprovalStatusRequest) (*partnerapiv1.GetPartnerOrderApprovalStatusResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "get_partner_order_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get partner order approval status", "tenant", req.GetContext().GetTenantId())
	validateSynonymRequest := &searchv1.ValidateSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
		Details: &searchv1.ValidateSynonymRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	validateSynonymReply, err := s.synonymValidationPort.ValidateSynonym(ctx, validateSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ValidateSynonym")
	}
	return &partnerapiv1.GetPartnerOrderApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Message: validateSynonymReply.GetResult().GetMessage(),
			Status:  validateSynonymReply.GetOutcome().GetStatus(),
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{PartnerOrderId: req.GetPartnerOrderId()},
	}, nil
}

// CommentPartnerOrderApproval comments a partner order.
func (s *PartnerOrderApprovalServer) CommentPartnerOrderApproval(ctx context.Context, req *partnerapiv1.CommentPartnerOrderApprovalRequest) (*partnerapiv1.CommentPartnerOrderApprovalResponse, error) {
	if err := s.store.SavePartnerOrder(ctx, req.GetContext().GetTenantId(), req.GetPartnerOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save partner order")
	}
	s.log.InfoContext(ctx, "comment partner order approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPartnerOrder().GetAudit().GetEtag())
	source := req.GetDetails().GetApprover().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	scheduleSellerExpiryRequest := &sellersv1.ScheduleSellerExpiryRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{DisplayName: displayName},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPartnerOrder().GetAudit().GetEtag(),
				CreatedAt: req.GetPartnerOrder().GetAudit().GetCreatedAt(),
			},
		},
		Details: &sellersv1.ScheduleSellerExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	scheduleSellerExpiryReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerLifecycleServiceClient).ScheduleSellerExpiry(ctx, scheduleSellerExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerLifecycleService/ScheduleSellerExpiry")
	}
	browseFanSessionsReply, err := s.fanSessionQueryClient.BrowseFanSessions(ctx, &fanportalv1.BrowseFanSessionsRequest{
		Context: req.GetContext(),
		Filter: &fanportalv1.BrowseFanSessionsRequest_Filter{
			Region: scheduleSellerExpiryReply.GetSeller().GetPrimaryAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fanportal.v1.FanSessionQueryService/BrowseFanSessions")
	}
	return &partnerapiv1.CommentPartnerOrderApprovalResponse{
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Number:         scheduleSellerExpiryReply.GetSeller().GetNationalId().GetNumber(),
			PartnerOrderId: req.GetPartnerOrderId(),
			IssuedAt:       req.GetPartnerOrder().GetIssuedAt(),
		},
		Outcome: &partnerapiv1.CommentPartnerOrderApprovalResponse_Outcome{Status: browseFanSessionsReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// partnerOrderApprovalServerSubmitPartnerOrderRecallSellerStep carries the request of one step of SubmitPartnerOrder
// and, once it ran, its reply.
type partnerOrderApprovalServerSubmitPartnerOrderRecallSellerStep struct {
	request *sellersv1.RecallSellerRequest
	reply   *sellersv1.RecallSellerResponse
}

func (s *PartnerOrderApprovalServer) runSubmitPartnerOrderRecallSeller(ctx context.Context, step *partnerOrderApprovalServerSubmitPartnerOrderRecallSellerStep) error {
	reply, err := s.sellerApprovalClient.RecallSeller(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// partnerOrderApprovalServerValidatePartnerWebhookSteps are the ways to run
// acme.partnerapi.v1.PartnerWebhookValidationService/ValidatePartnerWebhook, by name.
var partnerOrderApprovalServerValidatePartnerWebhookSteps = map[string]func(*PartnerOrderApprovalServer, context.Context, *partnerapiv1.ValidatePartnerWebhookRequest) (*partnerapiv1.ValidatePartnerWebhookResponse, error){
	"default": (*PartnerOrderApprovalServer).validatePartnerWebhookStep,
	"audited": (*PartnerOrderApprovalServer).validatePartnerWebhookAuditedStep,
}

func (s *PartnerOrderApprovalServer) validatePartnerWebhookStep(ctx context.Context, request *partnerapiv1.ValidatePartnerWebhookRequest) (*partnerapiv1.ValidatePartnerWebhookResponse, error) {
	return s.partnerWebhookValidationClient.ValidatePartnerWebhook(ctx, request)
}

func (s *PartnerOrderApprovalServer) validatePartnerWebhookAuditedStep(ctx context.Context, request *partnerapiv1.ValidatePartnerWebhookRequest) (*partnerapiv1.ValidatePartnerWebhookResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.partnerapi.v1.PartnerWebhookValidationService/ValidatePartnerWebhook")
	return s.partnerWebhookValidationClient.ValidatePartnerWebhook(ctx, request)
}

// normalizePartnerWebhookSender returns a function that sends one request to
// acme.partnerapi.v1.PartnerWebhookValidationService/NormalizePartnerWebhook.
func (s *PartnerOrderApprovalServer) normalizePartnerWebhookSender(request *partnerapiv1.NormalizePartnerWebhookRequest) func(context.Context) (*partnerapiv1.NormalizePartnerWebhookResponse, error) {
	return func(ctx context.Context) (*partnerapiv1.NormalizePartnerWebhookResponse, error) {
		return s.partnerWebhookValidationClient.NormalizePartnerWebhook(ctx, request)
	}
}

// lintPartnerWebhooksSender returns a function that sends one request to
// acme.partnerapi.v1.PartnerWebhookValidationService/LintPartnerWebhooks.
func (s *PartnerOrderApprovalServer) lintPartnerWebhooksSender(request *partnerapiv1.LintPartnerWebhooksRequest) func(context.Context) (*partnerapiv1.LintPartnerWebhooksResponse, error) {
	return func(ctx context.Context) (*partnerapiv1.LintPartnerWebhooksResponse, error) {
		return s.partnerWebhookValidationClient.LintPartnerWebhooks(ctx, request)
	}
}
