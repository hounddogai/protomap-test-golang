package payments

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	corporateeventsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/corporateevents/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	payrollv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	telematicsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PaymentQueryServer implements `acme.payments.v1.PaymentQueryService`.
// Answers read-only questions about payments, for screens and reports.
type PaymentQueryServer struct {
	paymentsv1.UnsafePaymentQueryServiceServer
	store                     *Store
	log                       *slog.Logger
	telemetryReadingClient    telematicsv1.TelemetryReadingServiceClient
	tierValidationClient      loyaltyv1.TierValidationServiceClient
	giftCardBatchQueryClient  giftcardsv1.GiftCardBatchQueryServiceClient
	corporateEventQueryClient corporateeventsv1.CorporateEventQueryServiceClient
	conn                      *grpc.ClientConn
	storageBinQueryClient     warehousingv1.StorageBinQueryServiceClient
	refundValidationClient    paymentsv1.RefundValidationServiceClient
	clients                   *registry.Registry
	payRunQueryClient         payrollv1.PayRunQueryServiceClient
}

// NewPaymentQueryServer wires a PaymentQueryServer.
func NewPaymentQueryServer(deps platform.Deps) *PaymentQueryServer {
	return &PaymentQueryServer{
		store:                     NewStore(deps.DB),
		log:                       deps.Logger.With("service", "acme.payments.v1.PaymentQueryService"),
		telemetryReadingClient:    telematicsv1.NewTelemetryReadingServiceClient(deps.Conn),
		tierValidationClient:      loyaltyv1.NewTierValidationServiceClient(deps.Conn),
		giftCardBatchQueryClient:  giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn),
		corporateEventQueryClient: corporateeventsv1.NewCorporateEventQueryServiceClient(deps.Conn),
		conn:                      deps.Conn,
		storageBinQueryClient:     warehousingv1.NewStorageBinQueryServiceClient(deps.Conn),
		refundValidationClient:    paymentsv1.NewRefundValidationServiceClient(deps.Conn),
		clients:                   deps.Clients,
		payRunQueryClient:         payrollv1.NewPayRunQueryServiceClient(deps.Conn),
	}
}

// LookupPayment returns a payment.
func (s *PaymentQueryServer) LookupPayment(ctx context.Context, req *paymentsv1.LookupPaymentRequest) (*paymentsv1.LookupPaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupPayment", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "lookup_payment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup payment", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.LookupPaymentResponse{
		Payment: &paymentsv1.Payment{PaymentId: req.GetPaymentId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowsePayments lists payments a page at a time.
func (s *PaymentQueryServer) BrowsePayments(ctx context.Context, req *paymentsv1.BrowsePaymentsRequest) (*paymentsv1.BrowsePaymentsResponse, error) {
	s.log.InfoContext(ctx, "browse payments", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	updateTelemetryReadingRequest := &telematicsv1.UpdateTelemetryReadingRequest{
		Context: req.GetContext(),
		TelemetryReading: &telematicsv1.TelemetryReading{
			OccurredAt: req.GetFilter().GetCreatedAfter(),
			Attributes: req.GetFilter().GetLabels(),
			Source:     req.GetFilter().GetOwnerId(),
			Detail:     packed,
		},
	}
	updateTelemetryReadingReply, err := telematicsv1.TelemetryReadingServiceClient.UpdateTelemetryReading(s.telemetryReadingClient, ctx, updateTelemetryReadingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telematics.v1.TelemetryReadingService/UpdateTelemetryReading")
	}
	return &paymentsv1.BrowsePaymentsResponse{
		Result: &commonv1.OperationResult{
			Message: updateTelemetryReadingReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Payments: []*paymentsv1.Payment{&paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: updateTelemetryReadingReply.GetTelemetryReading().GetSubject().GetDisplayName(),
			},
		}},
	}, nil
}

// FindPayments lists payments a page at a time.
func (s *PaymentQueryServer) FindPayments(ctx context.Context, req *paymentsv1.FindPaymentsRequest) (*paymentsv1.FindPaymentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "find payments", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.FindPaymentsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ResolvePayments returns several payments at once.
func (s *PaymentQueryServer) ResolvePayments(ctx context.Context, req *paymentsv1.ResolvePaymentsRequest) (*paymentsv1.ResolvePaymentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve payments", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	normalizeTierRequest := &loyaltyv1.NormalizeTierRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.NormalizeTierRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cachedContext.GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	normalizeTierReply, err := grpcx.Call(ctx, s.tierValidationClient.NormalizeTier, normalizeTierRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/NormalizeTier")
	}
	return &paymentsv1.ResolvePaymentsResponse{
		Result: &commonv1.OperationResult{
			Message: normalizeTierReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Payments: []*paymentsv1.Payment{&paymentsv1.Payment{
			Lines: []*paymentsv1.PaymentLine{&paymentsv1.PaymentLine{
				Amount: normalizeTierReply.GetTier().GetDiscount().GetAmount(),
			}},
		}},
	}, nil
}

// ExportPayments streams every matching payment.
func (s *PaymentQueryServer) ExportPayments(req *paymentsv1.ExportPaymentsRequest, stream grpc.ServerStreamingServer[paymentsv1.ExportPaymentsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExportPayments", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "export payments", "tenant", req.GetContext().GetTenantId())
	browseGiftCardBatchesReply, err := s.giftCardBatchQueryClient.BrowseGiftCardBatches(ctx, &giftcardsv1.BrowseGiftCardBatchesRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.BrowseGiftCardBatchesRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Query:        req.GetFilter().GetQuery(),
			OwnerId:      req.GetFilter().GetOwnerId(),
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/BrowseGiftCardBatches")
	}
	if err := stream.Send(&paymentsv1.ExportPaymentsResponse{
		Result: &commonv1.OperationResult{Status: browseGiftCardBatchesReply.GetResult().GetStatus()},
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizePayments summarizes a payment.
func (s *PaymentQueryServer) SummarizePayments(ctx context.Context, req *paymentsv1.SummarizePaymentsRequest) (*paymentsv1.SummarizePaymentsResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "summarize_payments"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize payments", "tenant", req.GetContext().GetTenantId())
	findCorporateEventsRequest := &corporateeventsv1.FindCorporateEventsRequest{}
	findCorporateEventsRequest.Context = req.GetContext()
	findCorporateEventsRequest.Filter = &corporateeventsv1.FindCorporateEventsRequest_Filter{
		OwnerId: req.GetPaymentId(),
		Labels:  req.GetDetails().GetFilters(),
	}
	findCorporateEventsReply, err := s.corporateEventQueryClient.FindCorporateEvents(ctx, findCorporateEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.corporateevents.v1.CorporateEventQueryService/FindCorporateEvents")
	}
	countCorporateEventsRequest := &corporateeventsv1.CountCorporateEventsRequest{
		Context: req.GetContext(),
		Filter: &corporateeventsv1.CountCorporateEventsRequest_Filter{
			Labels:  findCorporateEventsReply.GetResult().GetDetails(),
			OwnerId: findCorporateEventsReply.GetResult().GetCorrelationId(),
		},
	}
	countCorporateEventsReply := new(corporateeventsv1.CountCorporateEventsResponse)
	if err := grpcx.Invoke(ctx, s.conn, corporateeventsv1.CorporateEventQueryService_CountCorporateEvents_FullMethodName, countCorporateEventsRequest, countCorporateEventsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.corporateevents.v1.CorporateEventQueryService/CountCorporateEvents")
	}
	return &paymentsv1.SummarizePaymentsResponse{
		Result: &commonv1.OperationResult{
			Status:  findCorporateEventsReply.GetResult().GetStatus(),
			Message: countCorporateEventsReply.GetResult().GetMessage(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestPayments lists payments a page at a time.
func (s *PaymentQueryServer) SuggestPayments(ctx context.Context, req *paymentsv1.SuggestPaymentsRequest) (*paymentsv1.SuggestPaymentsResponse, error) {
	s.log.InfoContext(ctx, "suggest payments", "tenant", req.GetContext().GetTenantId())
	findStorageBinsRequest := buildFindStorageBinsRequestForSuggestPayments(req)
	findStorageBinsReply, err := s.storageBinQueryClient.FindStorageBins(ctx, findStorageBinsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/FindStorageBins")
	}
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	step := &paymentQueryServerSuggestPaymentsScoreRefundStep{request: &paymentsv1.ScoreRefundRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.ScoreRefundRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Refund: &paymentsv1.Refund{
			Counterparty: &customersv1.CustomerRef{Email: email},
			Audit:        &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
			Lines:        []*paymentsv1.RefundLine{&paymentsv1.RefundLine{Attributes: req.GetFilter().GetLabels()}},
		},
	}}
	if err := s.runSuggestPaymentsScoreRefund(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/ScoreRefund")
	}
	scoreRefundReply := step.reply
	return &paymentsv1.SuggestPaymentsResponse{
		PageInfo: &commonv1.PageInfo{
			TotalSize: findStorageBinsReply.GetPageInfo().GetTotalSize(),
		},
		Result: &commonv1.OperationResult{
			Details: findStorageBinsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Payments: []*paymentsv1.Payment{&paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{
				Lines: scoreRefundReply.GetRefund().GetBillingAddress().GetLines(),
			},
		}},
	}, nil
}

// PeekPayment returns a payment.
func (s *PaymentQueryServer) PeekPayment(ctx context.Context, req *paymentsv1.PeekPaymentRequest) (*paymentsv1.PeekPaymentResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "peek_payment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek payment", "tenant", req.GetContext().GetTenantId())
	synonymValidation, ok := s.clients.Get("acme.search.v1.SynonymValidationService").(searchv1.SynonymValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SynonymValidationService")
	}
	scoreSynonymRequest := &searchv1.ScoreSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	scoreSynonymReply, err := synonymValidation.ScoreSynonym(ctx, scoreSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ScoreSynonym")
	}
	resolvePayRunsReply, err := s.payRunQueryClient.ResolvePayRuns(ctx, &payrollv1.ResolvePayRunsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payroll.v1.PayRunQueryService/ResolvePayRuns")
	}
	return &paymentsv1.PeekPaymentResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  scoreSynonymReply.GetResult().GetReferenceIds(),
			CorrelationId: resolvePayRunsReply.GetResult().GetCorrelationId(),
			Details:       resolvePayRunsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Payment: &paymentsv1.Payment{
			Total:     scoreSynonymReply.GetBreakdown().GetTotal(),
			PaymentId: req.GetPaymentId(),
		},
	}, nil
}

// paymentQueryServerSuggestPaymentsScoreRefundStep carries the request of one step of SuggestPayments and, once it ran,
// its reply.
type paymentQueryServerSuggestPaymentsScoreRefundStep struct {
	request *paymentsv1.ScoreRefundRequest
	reply   *paymentsv1.ScoreRefundResponse
}

func (s *PaymentQueryServer) runSuggestPaymentsScoreRefund(ctx context.Context, step *paymentQueryServerSuggestPaymentsScoreRefundStep) error {
	reply, err := s.refundValidationClient.ScoreRefund(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
