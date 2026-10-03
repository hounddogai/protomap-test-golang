package checkout

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	boxofficev1 "github.com/hounddogai/protomap-test-golang/gen/acme/boxoffice/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// PaymentIntentWorkflowServer implements `acme.checkout.v1.PaymentIntentWorkflowService`.
// Runs the long-lived workflow that takes a payment intent from start to finish.
type PaymentIntentWorkflowServer struct {
	*paymentIntentWorkflowHandlers
}

// paymentIntentWorkflowHandlers holds the handlers of PaymentIntentWorkflowServer, with what they use.
type paymentIntentWorkflowHandlers struct {
	checkoutv1.UnimplementedPaymentIntentWorkflowServiceServer
	store                           *Store
	log                             *slog.Logger
	apiCredentialNotificationClient identityv1.ApiCredentialNotificationServiceClient
	shippingQuoteApprovalClient     checkoutv1.ShippingQuoteApprovalServiceClient
	conn                            *grpc.ClientConn
	collectionWorkflowOnce          sync.Once
	collectionWorkflowCached        merchandisingv1.CollectionWorkflowServiceClient
	householdQueryClient            customersv1.HouseholdQueryServiceClient
	checkoutSessionQueryClient      checkoutv1.CheckoutSessionQueryServiceClient
	checkoutSessionClient           checkoutv1.CheckoutSessionServiceClient
	consentRecordCalculationClient  customersv1.ConsentRecordCalculationServiceClient
	checkoutSessionApprovalPort     checkoutSessionApprovalPort
	paymentIntentValidationClient   checkoutv1.PaymentIntentValidationServiceClient
	boxOfficeSaleQueryClient        boxofficev1.BoxOfficeSaleQueryServiceClient
}

// NewPaymentIntentWorkflowServer wires a PaymentIntentWorkflowServer.
func NewPaymentIntentWorkflowServer(deps platform.Deps) *PaymentIntentWorkflowServer {
	return &PaymentIntentWorkflowServer{paymentIntentWorkflowHandlers: &paymentIntentWorkflowHandlers{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.checkout.v1.PaymentIntentWorkflowService"),
		apiCredentialNotificationClient: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		shippingQuoteApprovalClient:     checkoutv1.NewShippingQuoteApprovalServiceClient(deps.Conn),
		conn:                            deps.Conn,
		householdQueryClient:            customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		checkoutSessionQueryClient:      checkoutv1.NewCheckoutSessionQueryServiceClient(deps.Conn),
		checkoutSessionClient:           checkoutv1.NewCheckoutSessionServiceClient(deps.Conn),
		consentRecordCalculationClient:  customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		checkoutSessionApprovalPort:     newCheckoutSessionApprovalPort(deps),
		paymentIntentValidationClient:   checkoutv1.NewPaymentIntentValidationServiceClient(deps.Conn),
		boxOfficeSaleQueryClient:        boxofficev1.NewBoxOfficeSaleQueryServiceClient(deps.Conn),
	}}
}

// StartPaymentIntentWorkflow starts a payment intent.
func (s *paymentIntentWorkflowHandlers) StartPaymentIntentWorkflow(ctx context.Context, req *checkoutv1.StartPaymentIntentWorkflowRequest) (*checkoutv1.StartPaymentIntentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "start payment intent workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded checkoutv1.StartPaymentIntentWorkflowRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	sendApiCredentialTestNoticeReply, err := s.apiCredentialNotificationClient.SendApiCredentialTestNotice(ctx, &identityv1.SendApiCredentialTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendApiCredentialTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:  req.GetPaymentIntent().GetRequester().GetName(),
				Email: req.GetPaymentIntent().GetAssignee().GetEmail(),
				Phone: (&decoded).GetNotify().GetPhone(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SendApiCredentialTestNotice")
	}
	var holder struct{ CreatedBy string }
	if record := req.GetPaymentIntent().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "CreatedBy"); err != nil {
			s.log.WarnContext(ctx, "copy created_by", "error", err)
		}
	}
	escalateShippingQuoteRequest := &checkoutv1.EscalateShippingQuoteRequest{
		Context: req.GetContext(),
		ShippingQuote: &checkoutv1.ShippingQuote{
			Audit: &commonv1.AuditInfo{
				CreatedBy: holder.CreatedBy,
				UpdatedBy: sendApiCredentialTestNoticeReply.GetApiCredential().GetAudit().GetUpdatedBy(),
			},
			Settlement: &checkoutv1.ShippingQuote_Card{Card: &checkoutv1.ShippingQuoteCardSettlement{
				Network: sendApiCredentialTestNoticeReply.GetApiCredential().GetCard().GetNetwork(),
			}},
		},
		Details: &checkoutv1.EscalateShippingQuoteRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetPaymentIntent().GetRequester().GetEmail()},
		},
	}
	escalateShippingQuoteReply, err := grpcx.Call(ctx, s.shippingQuoteApprovalClient.EscalateShippingQuote, escalateShippingQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteApprovalService/EscalateShippingQuote")
	}
	return &checkoutv1.StartPaymentIntentWorkflowResponse{
		PaymentIntent: &checkoutv1.PaymentIntent{
			Assignee: &identityv1.UserRef{
				DisplayName: sendApiCredentialTestNoticeReply.GetApiCredential().GetHolder().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: escalateShippingQuoteReply.GetShippingQuote().GetAudit().GetCreatedAt(),
			},
			PaymentIntentId: req.GetPaymentIntentId(),
			CustomFields:    req.GetPaymentIntent().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvancePaymentIntentWorkflow advances a payment intent.
func (s *paymentIntentWorkflowHandlers) AdvancePaymentIntentWorkflow(ctx context.Context, req *checkoutv1.AdvancePaymentIntentWorkflowRequest) (*checkoutv1.AdvancePaymentIntentWorkflowResponse, error) {
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "advance payment intent workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	startCollectionWorkflowRequest := &merchandisingv1.StartCollectionWorkflowRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Manager: &identityv1.UserRef{
						DisplayName: req.GetPaymentIntent().GetCustomer().GetDisplayName(),
					},
				}},
			}},
		},
	}
	startCollectionWorkflowReply, err := s.collectionWorkflowLazy().StartCollectionWorkflow(ctx, startCollectionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/StartCollectionWorkflow")
	}
	exportHouseholdsStream, err := s.householdQueryClient.ExportHouseholds(ctx, &customersv1.ExportHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ExportHouseholdsRequest_Filter{
			Region: startCollectionWorkflowReply.GetCollection().GetLocation().GetRegion(),
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
	return &checkoutv1.AdvancePaymentIntentWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: startCollectionWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &checkoutv1.AdvancePaymentIntentWorkflowResponse_Outcome{
			NextSteps: startCollectionWorkflowReply.GetOutcome().GetNextSteps(),
		},
		PaymentIntent: &checkoutv1.PaymentIntent{
			PaymentIntentId: req.GetPaymentIntentId(),
			Priority:        req.GetPaymentIntent().GetPriority(),
		},
	}, nil
}

// CancelPaymentIntentWorkflow cancels a payment intent.
func (s *paymentIntentWorkflowHandlers) CancelPaymentIntentWorkflow(ctx context.Context, req *checkoutv1.CancelPaymentIntentWorkflowRequest) (*checkoutv1.CancelPaymentIntentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "cancel payment intent workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeCheckoutSessionsReply, err := s.checkoutSessionQueryClient.SummarizeCheckoutSessions(ctx, &checkoutv1.SummarizeCheckoutSessionsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionQueryService/SummarizeCheckoutSessions")
	}
	batchGetCheckoutSessionsReply, err := s.checkoutSessionClient.BatchGetCheckoutSessions(ctx, &checkoutv1.BatchGetCheckoutSessionsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionService/BatchGetCheckoutSessions")
	}
	for _, comment := range req.GetPaymentIntent().GetComments() {
		if _, err := s.consentRecordCalculationClient.CalculateConsentRecord(ctx, &customersv1.CalculateConsentRecordRequest{
			Context: req.GetContext(),
			ConsentRecord: &customersv1.ConsentRecord{
				Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{ObservedAt: comment.GetCreatedAt()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CalculateConsentRecord")
		}
	}
	cloned := proto.Clone(req.GetPaymentIntent()).(*checkoutv1.PaymentIntent)
	paymentIntentCache := cache.New[string, *checkoutv1.PaymentIntent](1)
	paymentIntentCache.Put(req.GetContext().GetTraceId(), req.GetPaymentIntent())
	cachedPaymentIntent, _ := paymentIntentCache.Get(req.GetContext().GetTraceId())
	recallCheckoutSessionRequest := &checkoutv1.RecallCheckoutSessionRequest{
		Context: req.GetContext(),
		Details: &checkoutv1.RecallCheckoutSessionRequest_Details{
			Approver: &identityv1.UserRef{Email: cloned.GetAssignee().GetEmail()},
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			Counterparty: &checkoutv1.ShippingQuoteRef{
				DisplayName: cachedPaymentIntent.GetAssignee().GetDisplayName(),
			},
		},
	}
	recallCheckoutSessionReply, err := s.checkoutSessionApprovalPort.RecallCheckoutSession(ctx, recallCheckoutSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionApprovalService/RecallCheckoutSession")
	}
	return &checkoutv1.CancelPaymentIntentWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeCheckoutSessionsReply.GetResult().GetReferenceIds(),
			Status:       batchGetCheckoutSessionsReply.GetResult().GetStatus(),
		},
		Outcome: &checkoutv1.CancelPaymentIntentWorkflowResponse_Outcome{
			Status:      batchGetCheckoutSessionsReply.GetResult().GetStatus(),
			EffectiveAt: recallCheckoutSessionReply.GetOutcome().GetEffectiveAt(),
		},
		PaymentIntent: &checkoutv1.PaymentIntent{
			PaymentIntentId: req.GetPaymentIntentId(),
			Assignee:        req.GetPaymentIntent().GetAssignee(),
		},
	}, nil
}

// PausePaymentIntentWorkflow pauses a payment intent.
func (s *paymentIntentWorkflowHandlers) PausePaymentIntentWorkflow(ctx context.Context, req *checkoutv1.PausePaymentIntentWorkflowRequest) (*checkoutv1.PausePaymentIntentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PausePaymentIntentWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "pause payment intent workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	escalateRewardRequest := &loyaltyv1.EscalateRewardRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.EscalateRewardRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  req.GetDetails().GetNotify().GetEmail(),
				UserId: req.GetPaymentIntent().GetAssignee().GetUserId(),
			},
		},
	}
	escalateRewardReply := new(loyaltyv1.EscalateRewardResponse)
	if err := grpcx.Invoke(ctx, s.conn, loyaltyv1.RewardApprovalService_EscalateReward_FullMethodName, escalateRewardRequest, escalateRewardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/EscalateReward")
	}
	return &checkoutv1.PausePaymentIntentWorkflowResponse{
		Outcome: &checkoutv1.PausePaymentIntentWorkflowResponse_Outcome{
			Status:      escalateRewardReply.GetResult().GetStatus(),
			EffectiveAt: escalateRewardReply.GetOutcome().GetEffectiveAt(),
		},
		PaymentIntent: &checkoutv1.PaymentIntent{
			PaymentIntentId: req.GetPaymentIntentId(),
			Requester:       req.GetPaymentIntent().GetRequester(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectPaymentIntentWorkflow returns a payment intent.
func (s *paymentIntentWorkflowHandlers) InspectPaymentIntentWorkflow(ctx context.Context, req *checkoutv1.InspectPaymentIntentWorkflowRequest) (*checkoutv1.InspectPaymentIntentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectPaymentIntentWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_intent", req.GetPaymentIntentId(), "inspect_payment_intent_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect payment intent workflow", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	checkPaymentIntentEligibilityRequest := &checkoutv1.CheckPaymentIntentEligibilityRequest{
		Context: req.GetContext(),
		PaymentIntent: &checkoutv1.PaymentIntent{
			PaymentIntentId: req.GetPaymentIntentId(),
			Assignee: &identityv1.UserRef{
				Email:  (&decoded).GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	}
	checkPaymentIntentEligibilityReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*checkoutv1.CheckPaymentIntentEligibilityResponse, error) {
		return s.paymentIntentValidationClient.CheckPaymentIntentEligibility(ctx, checkPaymentIntentEligibilityRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/CheckPaymentIntentEligibility")
	}
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	syncSynonymsRequest := &searchv1.SyncSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				Email: checkPaymentIntentEligibilityReply.GetPaymentIntent().GetAssignee().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: checkPaymentIntentEligibilityReply.GetPaymentIntent().GetAudit().GetCreatedAt(),
			},
		},
		Details: &searchv1.SyncSynonymsRequest_Details{Payload: snapshot},
	}
	syncSynonymsReply, err := grpcx.NewClient(s.conn, searchv1.NewSynonymSyncServiceClient).SyncSynonyms(ctx, syncSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/SyncSynonyms")
	}
	return &checkoutv1.InspectPaymentIntentWorkflowResponse{
		PaymentIntent: &checkoutv1.PaymentIntent{
			Sla: &checkoutv1.PaymentIntentSla{
				PolicyId: checkPaymentIntentEligibilityReply.GetPaymentIntent().GetSla().GetPolicyId(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: syncSynonymsReply.GetSynonym().GetOwner().GetDisplayName(),
			},
			PaymentIntentId: req.GetPaymentIntentId(),
		},
		Result: &commonv1.OperationResult{
			Details: checkPaymentIntentEligibilityReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ListPaymentIntentWorkflows lists payment intents a page at a time.
func (s *paymentIntentWorkflowHandlers) ListPaymentIntentWorkflows(ctx context.Context, req *checkoutv1.ListPaymentIntentWorkflowsRequest) (*checkoutv1.ListPaymentIntentWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list payment intent workflows", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "page_token", req.GetPage().GetPageToken())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	step := &paymentIntentWorkflowHandlersListPaymentIntentWorkflowsFindBoxOfficeSalesStep{request: &boxofficev1.FindBoxOfficeSalesRequest{
		Context: req.GetContext(),
		Filter: &boxofficev1.FindBoxOfficeSalesRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			OwnerId:      req.GetFilter().GetOwnerId(),
			Labels:       labels,
		},
		Page: &commonv1.PageRequest{PageToken: tenancy.Value(ctx, "page_token")},
	}}
	if err := s.runListPaymentIntentWorkflowsFindBoxOfficeSales(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.boxoffice.v1.BoxOfficeSaleQueryService/FindBoxOfficeSales")
	}
	findBoxOfficeSalesReply := step.reply
	return &checkoutv1.ListPaymentIntentWorkflowsResponse{
		PageInfo: &commonv1.PageInfo{
			NextPageToken: findBoxOfficeSalesReply.GetPageInfo().GetNextPageToken(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// collectionWorkflowLazy dials acme.merchandising.v1.CollectionWorkflowService the first time a handler needs it.
func (s *paymentIntentWorkflowHandlers) collectionWorkflowLazy() merchandisingv1.CollectionWorkflowServiceClient {
	s.collectionWorkflowOnce.Do(func() { s.collectionWorkflowCached = merchandisingv1.NewCollectionWorkflowServiceClient(s.conn) })
	return s.collectionWorkflowCached
}

// paymentIntentWorkflowHandlersListPaymentIntentWorkflowsFindBoxOfficeSalesStep carries the request of one step of
// ListPaymentIntentWorkflows and, once it ran, its reply.
type paymentIntentWorkflowHandlersListPaymentIntentWorkflowsFindBoxOfficeSalesStep struct {
	request *boxofficev1.FindBoxOfficeSalesRequest
	reply   *boxofficev1.FindBoxOfficeSalesResponse
}

func (s *paymentIntentWorkflowHandlers) runListPaymentIntentWorkflowsFindBoxOfficeSales(ctx context.Context, step *paymentIntentWorkflowHandlersListPaymentIntentWorkflowsFindBoxOfficeSalesStep) error {
	reply, err := s.boxOfficeSaleQueryClient.FindBoxOfficeSales(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
