package payments

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	seatingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/seating/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
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

// PaymentApprovalServer implements `acme.payments.v1.PaymentApprovalService`.
// Routes payments through approval, with escalation and delegation.
type PaymentApprovalServer struct {
	paymentsv1.UnimplementedPaymentApprovalServiceServer
	store                     *Store
	log                       *slog.Logger
	refundValidationClient    paymentsv1.RefundValidationServiceClient
	searchQueryQueryClient    searchv1.SearchQueryQueryServiceClient
	putawayTaskApprovalPort   putawayTaskApprovalPort
	customerValidationClient  customersv1.CustomerValidationServiceClient
	conn                      *grpc.ClientConn
	clients                   *registry.Registry
	paymentCalculationClient  paymentsv1.PaymentCalculationServiceClient
	venueQueryClient          venuesv1.VenueQueryServiceClient
	venueQueryClientFallback  venuesv1.VenueQueryServiceClient
	payoutLifecycleClient     paymentsv1.PayoutLifecycleServiceClient
	searchIndexAdminClient    searchv1.SearchIndexAdminServiceClient
	meteredRefundLifecycle    meteredRefundLifecycle
	putawayTaskApprovalClient warehousingv1.PutawayTaskApprovalServiceClient
	customerCalculationClient customersv1.CustomerCalculationServiceClient
	auditEventClient          auditv1.AuditEventServiceClient
	tenantNotificationClient  identityv1.TenantNotificationServiceClient
	disputeSyncClient         paymentsv1.DisputeSyncServiceClient
	seatMapQueryClient        seatingv1.SeatMapQueryServiceClient
	meteredPickListApproval   meteredPickListApproval
}

// NewPaymentApprovalServer wires a PaymentApprovalServer.
func NewPaymentApprovalServer(deps platform.Deps) *PaymentApprovalServer {
	return &PaymentApprovalServer{
		store:                     NewStore(deps.DB),
		log:                       deps.Logger.With("service", "acme.payments.v1.PaymentApprovalService"),
		refundValidationClient:    paymentsv1.NewRefundValidationServiceClient(deps.Conn),
		searchQueryQueryClient:    searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		putawayTaskApprovalPort:   newPutawayTaskApprovalPort(deps),
		customerValidationClient:  customersv1.NewCustomerValidationServiceClient(deps.Conn),
		conn:                      deps.Conn,
		clients:                   deps.Clients,
		paymentCalculationClient:  paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
		venueQueryClient:          venuesv1.NewVenueQueryServiceClient(deps.Conn),
		venueQueryClientFallback:  venuesv1.NewVenueQueryServiceClient(deps.Conn),
		payoutLifecycleClient:     paymentsv1.NewPayoutLifecycleServiceClient(deps.Conn),
		searchIndexAdminClient:    searchv1.NewSearchIndexAdminServiceClient(deps.Conn),
		meteredRefundLifecycle:    meteredRefundLifecycle{RefundLifecycleServiceClient: paymentsv1.NewRefundLifecycleServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		putawayTaskApprovalClient: warehousingv1.NewPutawayTaskApprovalServiceClient(deps.Conn),
		customerCalculationClient: customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		auditEventClient:          auditv1.NewAuditEventServiceClient(deps.Conn),
		tenantNotificationClient:  identityv1.NewTenantNotificationServiceClient(deps.Conn),
		disputeSyncClient:         paymentsv1.NewDisputeSyncServiceClient(deps.Conn),
		seatMapQueryClient:        seatingv1.NewSeatMapQueryServiceClient(deps.Conn),
		meteredPickListApproval:   meteredPickListApproval{PickListApprovalServiceClient: warehousingv1.NewPickListApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// SubmitPayment submits a payment.
func (s *PaymentApprovalServer) SubmitPayment(ctx context.Context, req *paymentsv1.SubmitPaymentRequest) (*paymentsv1.SubmitPaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "submit payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	for _, attachment := range req.GetPayment().GetAttachments() {
		if _, err := s.refundValidationClient.NormalizeRefund(ctx, &paymentsv1.NormalizeRefundRequest{
			Context: req.GetContext(),
			Refund: &paymentsv1.Refund{
				Attachments: []*paymentsv1.RefundAttachment{&paymentsv1.RefundAttachment{
					Checksum:    attachment.GetChecksum(),
					ContentType: attachment.GetContentType(),
					Uri:         attachment.GetUri(),
					FileName:    attachment.GetFileName(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/NormalizeRefund")
		}
	}
	cloned := proto.Clone(req.GetPayment()).(*paymentsv1.Payment)
	countSearchQueriesReply, err := s.searchQueryQueryClient.CountSearchQueries(ctx, &searchv1.CountSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.CountSearchQueriesRequest_Filter{
			CreatedAfter: cloned.GetAudit().GetCreatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/CountSearchQueries")
	}
	recallPutawayTaskRequest := &warehousingv1.RecallPutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Assignee: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
	recallPutawayTaskReply, err := s.putawayTaskApprovalPort.RecallPutawayTask(ctx, recallPutawayTaskRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskApprovalService/RecallPutawayTask")
	}
	cloned2 := proto.Clone(req.GetPayment()).(*paymentsv1.Payment)
	previewCustomerRequest := &customersv1.PreviewCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			IdentityDocument: &customersv1.Customer_Passport{Passport: &customersv1.CustomerPassport{Number: cloned2.GetCounterparty().GetNumber()}},
			Contact: &commonv1.ContactInfo{
				Name: recallPutawayTaskReply.GetPutawayTask().GetRequester().GetName(),
			},
		},
		Details: &customersv1.PreviewCustomerRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: recallPutawayTaskReply.GetPutawayTask().GetRequester().GetEmail(),
			},
		},
	}
	type previewCustomerResult struct {
		reply *customersv1.PreviewCustomerResponse
		err   error
	}
	previewCustomerResults := make(chan previewCustomerResult, 1)
	go func() {
		reply, err := s.customerValidationClient.PreviewCustomer(ctx, previewCustomerRequest)
		previewCustomerResults <- previewCustomerResult{reply, err}
	}()
	var previewCustomerReply *customersv1.PreviewCustomerResponse
	select {
	case result := <-previewCustomerResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.CustomerValidationService/PreviewCustomer")
		}
		previewCustomerReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &paymentsv1.SubmitPaymentResponse{
		Result: &commonv1.OperationResult{
			Message: countSearchQueriesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &paymentsv1.SubmitPaymentResponse_Outcome{
			Status:      recallPutawayTaskReply.GetOutcome().GetStatus(),
			ReferenceId: previewCustomerReply.GetOutcome().GetReferenceId(),
		},
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: previewCustomerReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
			},
			PaymentId: req.GetPaymentId(),
			DueAt:     req.GetPayment().GetDueAt(),
		},
	}, nil
}

// ApprovePayment approves a payment.
func (s *PaymentApprovalServer) ApprovePayment(ctx context.Context, req *paymentsv1.ApprovePaymentRequest) (*paymentsv1.ApprovePaymentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "approve payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	var holder struct{ Region string }
	if record := req.GetPayment().GetBillingAddress(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	listSellerListingReportsRequest := &sellersv1.ListSellerListingReportsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListSellerListingReportsRequest_Filter{
			Region:       holder.Region,
			CreatedAfter: req.GetPayment().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
		},
	}
	listSellerListingReportsReply := new(sellersv1.ListSellerListingReportsResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.sellers.v1.SellerListingReportingService/ListSellerListingReports", listSellerListingReportsRequest, listSellerListingReportsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/ListSellerListingReports")
	}
	packingSlipWorkflow, ok := s.clients.Get("acme.fulfillment.v1.PackingSlipWorkflowService").(fulfillmentv1.PackingSlipWorkflowServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.fulfillment.v1.PackingSlipWorkflowService")
	}
	cancelPackingSlipWorkflowRequest := &fulfillmentv1.CancelPackingSlipWorkflowRequest{
		Context: req.GetContext(),
		PackingSlip: &fulfillmentv1.PackingSlip{
			Counterparty: &fulfillmentv1.FulfillmentRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{Lines: req.GetPayment().GetBillingAddress().GetLines()},
		},
	}
	cancelPackingSlipWorkflowReply, err := packingSlipWorkflow.CancelPackingSlipWorkflow(ctx, cancelPackingSlipWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipWorkflowService/CancelPackingSlipWorkflow")
	}
	source := req.GetPayment().GetBillingAddress().ProtoReflect()
	countryCode := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("country_code"))).String()
	quotePaymentRequest := &paymentsv1.QuotePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{CountryCode: countryCode},
		},
	}
	quotePaymentReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*paymentsv1.QuotePaymentResponse, error) {
		return s.paymentCalculationClient.QuotePayment(ctx, quotePaymentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/QuotePayment")
	}
	routes := map[string]func(context.Context, *venuesv1.CountVenuesRequest, ...grpc.CallOption) (*venuesv1.CountVenuesResponse, error){
		"primary":  s.venueQueryClient.CountVenues,
		"fallback": s.venueQueryClientFallback.CountVenues,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	countVenuesRequest := &venuesv1.CountVenuesRequest{
		Context: req.GetContext(),
		Filter: &venuesv1.CountVenuesRequest_Filter{
			Region:       cancelPackingSlipWorkflowReply.GetPackingSlip().GetBillingAddress().GetRegion(),
			CreatedAfter: cancelPackingSlipWorkflowReply.GetOutcome().GetEffectiveAt(),
			OwnerId:      cancelPackingSlipWorkflowReply.GetPackingSlip().GetCounterparty().GetFulfillmentId(),
			Labels:       quotePaymentReply.GetResult().GetDetails(),
		},
	}
	countVenuesReply, err := route(ctx, countVenuesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/CountVenues")
	}
	return &paymentsv1.ApprovePaymentResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listSellerListingReportsReply.GetResult().GetReferenceIds(),
			Details:      cancelPackingSlipWorkflowReply.GetResult().GetDetails(),
			Message:      countVenuesReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Payment: &paymentsv1.Payment{
			BillingAddress: quotePaymentReply.GetPayment().GetBillingAddress(),
			PaymentId:      req.GetPaymentId(),
			IssuedAt:       req.GetPayment().GetIssuedAt(),
		},
	}, nil
}

// EscalatePayment escalates a payment.
func (s *PaymentApprovalServer) EscalatePayment(ctx context.Context, req *paymentsv1.EscalatePaymentRequest) (*paymentsv1.EscalatePaymentResponse, error) {
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "escalate payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	source := req.GetPayment().GetCounterparty().ProtoReflect()
	refundId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("refund_id"))).String()
	schedulePayoutExpiryRequest := &paymentsv1.SchedulePayoutExpiryRequest{}
	schedulePayoutExpiryRequest.Context = req.GetContext()
	schedulePayoutExpiryRequest.Payout = &paymentsv1.Payout{
		Settlement: &paymentsv1.Payout_Card{Card: &paymentsv1.PayoutCardSettlement{
			HolderName: req.GetPayment().GetCard().GetHolderName(),
			Last4:      req.GetPayment().GetCard().GetLast4(),
		}},
		Counterparty: &paymentsv1.RefundRef{RefundId: refundId},
	}
	schedulePayoutExpiryReply, err := s.payoutLifecycleClient.SchedulePayoutExpiry(ctx, schedulePayoutExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutLifecycleService/SchedulePayoutExpiry")
	}
	repairSearchIndexRequest := &searchv1.RepairSearchIndexRequest{}
	repairSearchIndexRequest.Context = req.GetContext()
	repairSearchIndexRequest.SearchIndex = &searchv1.SearchIndex{
		Owner: &identityv1.UserRef{
			DisplayName: req.GetPayment().GetCounterparty().GetDisplayName(),
		},
		Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{
			Recipient: req.GetPayment().GetBillingAddress().GetRecipient(),
		}},
	}
	repairSearchIndexReply, err := s.searchIndexAdminClient.RepairSearchIndex(ctx, repairSearchIndexRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexAdminService/RepairSearchIndex")
	}
	return &paymentsv1.EscalatePaymentResponse{
		Outcome: &paymentsv1.EscalatePaymentResponse_Outcome{
			ReferenceId: schedulePayoutExpiryReply.GetOutcome().GetReferenceId(),
		},
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: schedulePayoutExpiryReply.GetPayout().GetBillingAddress().GetPostalCode(),
			},
			PaymentId:    req.GetPaymentId(),
			Counterparty: req.GetPayment().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: repairSearchIndexReply.GetResult().GetStatus()},
	}, nil
}

// DelegatePaymentApproval delegates a payment.
func (s *PaymentApprovalServer) DelegatePaymentApproval(ctx context.Context, req *paymentsv1.DelegatePaymentApprovalRequest) (*paymentsv1.DelegatePaymentApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "delegate payment approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	renewRefundRequest := &paymentsv1.RenewRefundRequest{
		Context:  req.GetContext(),
		RefundId: req.GetPayment().GetCounterparty().GetRefundId(),
		Details: &paymentsv1.RenewRefundRequest_Details{
			NotifyContact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	renewRefundReply, err := s.meteredRefundLifecycle.RenewRefund(ctx, renewRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundLifecycleService/RenewRefund")
	}
	submitPutawayTaskRequest := &warehousingv1.SubmitPutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Audit: &commonv1.AuditInfo{Etag: req.GetPayment().GetAudit().GetEtag()},
			Origin: &warehousingv1.PutawayTask_Customer{Customer: &warehousingv1.StorageBinRef{
				DisplayName: req.GetPayment().GetCounterparty().GetDisplayName(),
			}},
			Requester: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	send := s.submitPutawayTaskSender(submitPutawayTaskRequest)
	submitPutawayTaskReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskApprovalService/SubmitPutawayTask")
	}
	return &paymentsv1.DelegatePaymentApprovalResponse{
		Payment: &paymentsv1.Payment{
			BillingAddress: renewRefundReply.GetRefund().GetBillingAddress(),
			PaymentId:      req.GetPaymentId(),
			Metadata:       req.GetPayment().GetMetadata(),
		},
		Outcome: &paymentsv1.DelegatePaymentApprovalResponse_Outcome{Status: renewRefundReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Details: submitPutawayTaskReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// RecallPayment recalls a payment.
func (s *PaymentApprovalServer) RecallPayment(ctx context.Context, req *paymentsv1.RecallPaymentRequest) (*paymentsv1.RecallPaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallPayment", "error", err)
		return nil, err
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "recall payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	paymentCache := cache.New[string, *paymentsv1.Payment](1)
	paymentCache.Put(req.GetContext().GetTraceId(), req.GetPayment())
	cachedPayment, _ := paymentCache.Get(req.GetContext().GetTraceId())
	estimateCustomerRequest := &customersv1.EstimateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetPayment().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &customersv1.EstimateCustomerRequest_Details{
			Destination: &commonv1.PostalAddress{Region: cachedPayment.GetBillingAddress().GetRegion()},
		},
	}
	estimateCustomerReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.EstimateCustomerResponse, error) {
		return s.customerCalculationClient.EstimateCustomer(ctx, estimateCustomerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/EstimateCustomer")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*customersv1.RecalculateCustomersResponse, len(req.GetPayment().GetLines()))
	for i, line := range req.GetPayment().GetLines() {
		group.Go(func() error {
			reply, err := s.customerCalculationClient.RecalculateCustomers(groupCtx, &customersv1.RecalculateCustomersRequest{
				Context:  req.GetContext(),
				Customer: &customersv1.Customer{Attributes: line.GetAttributes()},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	for _, line2 := range req.GetPayment().GetLines() {
		if _, err := s.auditEventClient.ListAuditEvents(ctx, &auditv1.ListAuditEventsRequest{
			Context: req.GetContext(),
			Filter: &auditv1.ListAuditEventsRequest_Filter{
				Region:  req.GetPayment().GetBillingAddress().GetRegion(),
				OwnerId: line2.GetLineId(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/ListAuditEvents")
		}
	}
	return &paymentsv1.RecallPaymentResponse{
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: estimateCustomerReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: estimateCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
			},
			PaymentId: req.GetPaymentId(),
			Notes:     req.GetPayment().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPaymentApprovalStatus returns a payment.
func (s *PaymentApprovalServer) GetPaymentApprovalStatus(ctx context.Context, req *paymentsv1.GetPaymentApprovalStatusRequest) (*paymentsv1.GetPaymentApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetPaymentApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "get_payment_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payment approval status", "tenant", req.GetContext().GetTenantId())
	notifyTenantWatchersRequest := buildNotifyTenantWatchersRequestForGetPaymentApprovalStatus(req)
	notifyTenantWatchersReply, err := s.tenantNotificationClient.NotifyTenantWatchers(ctx, notifyTenantWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/NotifyTenantWatchers")
	}
	return &paymentsv1.GetPaymentApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Message:      notifyTenantWatchersReply.GetResult().GetMessage(),
			ReferenceIds: notifyTenantWatchersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Payment: &paymentsv1.Payment{PaymentId: req.GetPaymentId()},
	}, nil
}

// CommentPaymentApproval comments a payment.
func (s *PaymentApprovalServer) CommentPaymentApproval(ctx context.Context, req *paymentsv1.CommentPaymentApprovalRequest) (*paymentsv1.CommentPaymentApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "comment payment approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	for attachment := range iterx.Each(req.GetPayment().GetAttachments(), func(candidate *paymentsv1.PaymentAttachment) bool { return candidate != nil }) {
		if _, err := s.disputeSyncClient.ResolveDisputeConflict(ctx, &paymentsv1.ResolveDisputeConflictRequest{
			Context: req.GetContext(),
			Details: &paymentsv1.ResolveDisputeConflictRequest_Details{Checksum: attachment.GetChecksum()},
			Dispute: &paymentsv1.Dispute{
				Assignee: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeSyncService/ResolveDisputeConflict")
		}
	}
	resolveSeatMapsReply, err := s.seatMapQueryClient.ResolveSeatMaps(ctx, &seatingv1.ResolveSeatMapsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.seating.v1.SeatMapQueryService/ResolveSeatMaps")
	}
	lintCustomerSegmentsRequest := &customersv1.LintCustomerSegmentsRequest{Context: req.GetContext()}
	lintCustomerSegmentsReply := new(customersv1.LintCustomerSegmentsResponse)
	if err := s.conn.Invoke(ctx, customersCustomerSegmentValidationServiceLintCustomerSegmentsMethod, lintCustomerSegmentsRequest, lintCustomerSegmentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	recallPickListRequest := &warehousingv1.RecallPickListRequest{
		Context: req.GetContext(),
		Details: &warehousingv1.RecallPickListRequest_Details{
			Approver: &identityv1.UserRef{
				Email: lintCustomerSegmentsReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
		},
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: lintCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetUpdatedAt(),
				UpdatedBy: lintCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetUpdatedBy(),
			},
		},
	}
	recallPickListReply, err := s.meteredPickListApproval.RecallPickList(ctx, recallPickListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListApprovalService/RecallPickList")
	}
	return &paymentsv1.CommentPaymentApprovalResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolveSeatMapsReply.GetResult().GetCorrelationId(),
			Status:        resolveSeatMapsReply.GetResult().GetStatus(),
			Details:       recallPickListReply.GetResult().GetDetails(),
		},
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: lintCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetUpdatedAt(),
			},
			Counterparty: &paymentsv1.RefundRef{Number: recallPickListReply.GetPickList().GetNumber()},
			PaymentId:    req.GetPaymentId(),
			Status:       req.GetPayment().GetStatus(),
		},
	}, nil
}

// submitPutawayTaskSender returns a function that sends one request to
// acme.warehousing.v1.PutawayTaskApprovalService/SubmitPutawayTask.
func (s *PaymentApprovalServer) submitPutawayTaskSender(request *warehousingv1.SubmitPutawayTaskRequest) func(context.Context) (*warehousingv1.SubmitPutawayTaskResponse, error) {
	return func(ctx context.Context) (*warehousingv1.SubmitPutawayTaskResponse, error) {
		return s.putawayTaskApprovalClient.SubmitPutawayTask(ctx, request)
	}
}
