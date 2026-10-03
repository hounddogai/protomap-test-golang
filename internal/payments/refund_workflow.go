package payments

import (
	"context"
	"fmt"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// RefundWorkflowServer implements `acme.payments.v1.RefundWorkflowService`.
// Runs the long-lived workflow that takes a refund from start to finish.
type RefundWorkflowServer struct {
	*refundWorkflowHandlers
}

// refundWorkflowHandlers holds the handlers of RefundWorkflowServer, with what they use.
type refundWorkflowHandlers struct {
	paymentsv1.UnimplementedRefundWorkflowServiceServer
	store                              *Store
	log                                *slog.Logger
	customerCalculationClient          customersv1.CustomerCalculationServiceClient
	putawayTaskApprovalClient          warehousingv1.PutawayTaskApprovalServiceClient
	customerClient                     customersv1.CustomerServiceClient
	bus                                *eventbus.Bus
	reviewModerationApprovalClient     reviewsv1.ReviewModerationApprovalServiceClient
	conn                               *grpc.ClientConn
	venueClient                        venuesv1.VenueServiceClient
	paymentInstrumentSyncClient        paymentsv1.PaymentInstrumentSyncServiceClient
	accessPolicyQueryClient            authorizationv1.AccessPolicyQueryServiceClient
	sellerPayoutScheduleWorkflowClient sellersv1.SellerPayoutScheduleWorkflowServiceClient
	validateConsentRecordFn            func(context.Context, *customersv1.ValidateConsentRecordRequest, ...grpc.CallOption) (*customersv1.ValidateConsentRecordResponse, error)
}

// NewRefundWorkflowServer wires a RefundWorkflowServer.
func NewRefundWorkflowServer(deps platform.Deps) *RefundWorkflowServer {
	consentRecordValidation := customersv1.NewConsentRecordValidationServiceClient(deps.Conn)
	return &RefundWorkflowServer{refundWorkflowHandlers: &refundWorkflowHandlers{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.payments.v1.RefundWorkflowService"),
		customerCalculationClient:          customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		putawayTaskApprovalClient:          warehousingv1.NewPutawayTaskApprovalServiceClient(deps.Conn),
		customerClient:                     customersv1.NewCustomerServiceClient(deps.Conn),
		bus:                                deps.Bus,
		reviewModerationApprovalClient:     reviewsv1.NewReviewModerationApprovalServiceClient(deps.Conn),
		conn:                               deps.Conn,
		venueClient:                        venuesv1.NewVenueServiceClient(deps.Conn),
		paymentInstrumentSyncClient:        paymentsv1.NewPaymentInstrumentSyncServiceClient(deps.Conn),
		accessPolicyQueryClient:            authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn),
		sellerPayoutScheduleWorkflowClient: sellersv1.NewSellerPayoutScheduleWorkflowServiceClient(deps.Conn),
		validateConsentRecordFn:            consentRecordValidation.ValidateConsentRecord,
	}}
}

// StartRefundWorkflow starts a refund.
func (s *refundWorkflowHandlers) StartRefundWorkflow(ctx context.Context, req *paymentsv1.StartRefundWorkflowRequest) (*paymentsv1.StartRefundWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StartRefundWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "start refund workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetRefund()).(*paymentsv1.Refund)
	getCustomerRatesReply, err := s.customerCalculationClient.GetCustomerRates(ctx, &customersv1.GetCustomerRatesRequest{
		Context:    req.GetContext(),
		CustomerId: cloned.GetCounterparty().GetCustomerId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/GetCustomerRates")
	}
	delegatePutawayTaskApprovalRequest := &warehousingv1.DelegatePutawayTaskApprovalRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Requester: &commonv1.ContactInfo{
				Email: getCustomerRatesReply.GetCustomer().GetContact().GetEmail(),
			},
			Assignee: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: getCustomerRatesReply.GetCustomer().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &warehousingv1.DelegatePutawayTaskApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: getCustomerRatesReply.GetCustomer().GetRelationshipManager().GetUserId(),
			},
		},
	}
	delegatePutawayTaskApprovalReply, err := warehousingv1.PutawayTaskApprovalServiceClient.DelegatePutawayTaskApproval(s.putawayTaskApprovalClient, ctx, delegatePutawayTaskApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskApprovalService/DelegatePutawayTaskApproval")
	}
	for line := range iterx.Each(req.GetRefund().GetLines(), func(candidate *paymentsv1.RefundLine) bool { return candidate != nil }) {
		if _, err := s.customerClient.CloneCustomer(ctx, &customersv1.CloneCustomerRequest{
			Context: req.GetContext(),
			Customer: &customersv1.Customer{
				Attributes:     line.GetAttributes(),
				PrimaryAddress: &commonv1.PostalAddress{Region: req.GetRefund().GetBillingAddress().GetRegion()},
				Contact: &commonv1.ContactInfo{
					Email: delegatePutawayTaskApprovalReply.GetPutawayTask().GetAssignee().GetEmail(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CloneCustomer")
		}
	}
	return &paymentsv1.StartRefundWorkflowResponse{
		Outcome: &paymentsv1.StartRefundWorkflowResponse_Outcome{Status: getCustomerRatesReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			ReferenceIds: delegatePutawayTaskApprovalReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Refund: &paymentsv1.Refund{
			RefundId: req.GetRefundId(),
			Number:   req.GetRefund().GetNumber(),
		},
	}, nil
}

// AdvanceRefundWorkflow advances a refund.
func (s *refundWorkflowHandlers) AdvanceRefundWorkflow(ctx context.Context, req *paymentsv1.AdvanceRefundWorkflowRequest) (*paymentsv1.AdvanceRefundWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "advance refund workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "payments.advance_refund_workflow.preview_preference",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.contact.email":         req.GetDetails().GetNotify().GetEmail(),
			"preference.owner.display_name": req.GetRefund().GetCounterparty().GetDisplayName(),
			"context.tenant_id":             req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotify().GetEmail())
	submitReviewModerationReply, err := s.reviewModerationApprovalClient.SubmitReviewModeration(ctx, &reviewsv1.SubmitReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
			Origin: &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{
				CustomerId: req.GetRefund().GetCounterparty().GetCustomerId(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationApprovalService/SubmitReviewModeration")
	}
	return &paymentsv1.AdvanceRefundWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: submitReviewModerationReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Refund: &paymentsv1.Refund{
			RefundId: req.GetRefundId(),
			IssuedAt: req.GetRefund().GetIssuedAt(),
		},
	}, nil
}

// CancelRefundWorkflow cancels a refund.
func (s *refundWorkflowHandlers) CancelRefundWorkflow(ctx context.Context, req *paymentsv1.CancelRefundWorkflowRequest) (*paymentsv1.CancelRefundWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "cancel refund workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	pullPaymentInstrumentsRequest := &paymentsv1.PullPaymentInstrumentsRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Funding: &paymentsv1.PaymentInstrument_Card{Card: &paymentsv1.PaymentInstrumentCardFunding{Network: req.GetRefund().GetCard().GetNetwork()}},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetRefund().GetAudit().GetUpdatedAt()},
			Holder: &customersv1.CustomerRef{
				Email:       req.GetRefund().GetCounterparty().GetEmail(),
				DisplayName: req.GetRefund().GetCounterparty().GetDisplayName(),
			},
		},
	}
	pullPaymentInstrumentsReply := new(paymentsv1.PullPaymentInstrumentsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", paymentsPaymentInstrumentSyncServiceName, "PullPaymentInstruments"), pullPaymentInstrumentsRequest, pullPaymentInstrumentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/PullPaymentInstruments")
	}
	return &paymentsv1.CancelRefundWorkflowResponse{
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				Etag: pullPaymentInstrumentsReply.GetPaymentInstrument().GetAudit().GetEtag(),
			},
			RefundId: req.GetRefundId(),
			Number:   req.GetRefund().GetNumber(),
		},
		Outcome: &paymentsv1.CancelRefundWorkflowResponse_Outcome{
			Message: pullPaymentInstrumentsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryRefundWorkflow retrys a refund.
func (s *refundWorkflowHandlers) RetryRefundWorkflow(ctx context.Context, req *paymentsv1.RetryRefundWorkflowRequest) (*paymentsv1.RetryRefundWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "retry refund workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	createVenueRequest := &venuesv1.CreateVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetRefund().GetCounterparty().GetDisplayName(),
			},
			Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{Region: req.GetRefund().GetBillingAddress().GetRegion()}},
		},
	}
	var createVenueReply *venuesv1.CreateVenueResponse
	if err := pipeline.Run(ctx, "acme.venues.v1.VenueService/CreateVenue", func(ctx context.Context) error {
		answer, err := s.venueClient.CreateVenue(ctx, createVenueRequest)
		createVenueReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/CreateVenue")
	}
	return &paymentsv1.RetryRefundWorkflowResponse{
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: createVenueReply.GetVenue().GetAudit().GetUpdatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{Lines: createVenueReply.GetVenue().GetAddress().GetLines()},
			RefundId:       req.GetRefundId(),
			Number:         req.GetRefund().GetNumber(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseRefundWorkflow pauses a refund.
func (s *refundWorkflowHandlers) PauseRefundWorkflow(ctx context.Context, req *paymentsv1.PauseRefundWorkflowRequest) (*paymentsv1.PauseRefundWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "pause refund workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetRefund())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	syncPaymentInstrumentsReply, err := s.paymentInstrumentSyncClient.SyncPaymentInstruments(ctx, &paymentsv1.SyncPaymentInstrumentsRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{Email: req.GetRefund().GetCounterparty().GetEmail()},
			Funding: &paymentsv1.PaymentInstrument_Bank{Bank: &paymentsv1.PaymentInstrumentBankFunding{
				Bic:  req.GetRefund().GetBank().GetBic(),
				Iban: req.GetRefund().GetBank().GetIban(),
			}},
		},
		Details: &paymentsv1.SyncPaymentInstrumentsRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/SyncPaymentInstruments")
	}
	snapshot2, err := protojson.Marshal(req.GetRefund())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullPaymentInstrumentsRequest := &paymentsv1.PullPaymentInstrumentsRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
		Details: &paymentsv1.PullPaymentInstrumentsRequest_Details{Payload: snapshot2},
	}
	pullPaymentInstrumentsReply, err := grpcx.NewClient(s.conn, paymentsv1.NewPaymentInstrumentSyncServiceClient).PullPaymentInstruments(ctx, pullPaymentInstrumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/PullPaymentInstruments")
	}
	browseAccessPoliciesRequest := buildBrowseAccessPoliciesRequestForPauseRefundWorkflow(req, pullPaymentInstrumentsReply)
	browseAccessPoliciesReply, err := s.accessPolicyQueryClient.BrowseAccessPolicies(ctx, browseAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/BrowseAccessPolicies")
	}
	pauseSellerPayoutScheduleWorkflowRequest := &sellersv1.PauseSellerPayoutScheduleWorkflowRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email: req.GetDetails().GetNotify().GetEmail(),
					Phone: req.GetDetails().GetNotify().GetPhone(),
				},
			}},
		},
	}
	pauseSellerPayoutScheduleWorkflowReply, err := sellersv1.SellerPayoutScheduleWorkflowServiceClient.PauseSellerPayoutScheduleWorkflow(s.sellerPayoutScheduleWorkflowClient, ctx, pauseSellerPayoutScheduleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleWorkflowService/PauseSellerPayoutScheduleWorkflow")
	}
	return &paymentsv1.PauseRefundWorkflowResponse{
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				CreatedBy: syncPaymentInstrumentsReply.GetPaymentInstrument().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient:   pauseSellerPayoutScheduleWorkflowReply.GetSellerPayoutSchedule().GetLocation().GetRecipient(),
				CountryCode: pauseSellerPayoutScheduleWorkflowReply.GetSellerPayoutSchedule().GetLocation().GetCountryCode(),
			},
			RefundId: req.GetRefundId(),
			Number:   req.GetRefund().GetNumber(),
		},
		Result: &commonv1.OperationResult{
			Details: browseAccessPoliciesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// InspectRefundWorkflow returns a refund.
func (s *refundWorkflowHandlers) InspectRefundWorkflow(ctx context.Context, req *paymentsv1.InspectRefundWorkflowRequest) (*paymentsv1.InspectRefundWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "inspect_refund_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect refund workflow", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateConsentRecordRequest := &customersv1.ValidateConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.ValidateConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Source:     req.GetContext().GetActor().GetUserId(),
			OccurredAt: req.GetContext().GetDeadline(),
			Detail:     packed,
		},
	}
	validateConsentRecordReply, err := s.validateConsentRecordFn(ctx, validateConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ValidateConsentRecord")
	}
	return &paymentsv1.InspectRefundWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: validateConsentRecordReply.GetResult().GetDetails(),
			Status:  validateConsentRecordReply.GetResult().GetStatus(),
		},
		Refund: &paymentsv1.Refund{RefundId: req.GetRefundId()},
	}, nil
}
