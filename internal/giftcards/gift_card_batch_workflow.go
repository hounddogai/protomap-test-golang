package giftcards

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	appointmentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/appointments/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	schedulingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/scheduling/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
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

// GiftCardBatchWorkflowServer implements `acme.giftcards.v1.GiftCardBatchWorkflowService`.
// Runs the long-lived workflow that takes a gift card batch from start to finish.
type GiftCardBatchWorkflowServer struct {
	giftcardsv1.UnimplementedGiftCardBatchWorkflowServiceServer
	store                               *Store
	log                                 *slog.Logger
	appointmentQueryClient              appointmentsv1.AppointmentQueryServiceClient
	tenantAdminClient                   identityv1.TenantAdminServiceClient
	giftCardTransactionApprovalClient   giftcardsv1.GiftCardTransactionApprovalServiceClient
	shiftQueryClient                    schedulingv1.ShiftQueryServiceClient
	conn                                *grpc.ClientConn
	meteredPointsLedgerQuery            meteredPointsLedgerQuery
	searchIndexHistoryClient            searchv1.SearchIndexHistoryServiceClient
	giftCardTransactionValidationClient giftcardsv1.GiftCardTransactionValidationServiceClient
	clients                             *registry.Registry
	metricSeriesQueryClient             metricsv1.MetricSeriesQueryServiceClient
	roleNotificationClient              identityv1.RoleNotificationServiceClient
	pickListValidationClient            warehousingv1.PickListValidationServiceClient
	pointsLedgerQueryClient             loyaltyv1.PointsLedgerQueryServiceClient
	giftCardBatchApprovalClient         giftcardsv1.GiftCardBatchApprovalServiceClient
}

// NewGiftCardBatchWorkflowServer wires a GiftCardBatchWorkflowServer.
func NewGiftCardBatchWorkflowServer(deps platform.Deps) *GiftCardBatchWorkflowServer {
	return &GiftCardBatchWorkflowServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.giftcards.v1.GiftCardBatchWorkflowService"),
		appointmentQueryClient:              appointmentsv1.NewAppointmentQueryServiceClient(deps.Conn),
		tenantAdminClient:                   identityv1.NewTenantAdminServiceClient(deps.Conn),
		giftCardTransactionApprovalClient:   giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn),
		shiftQueryClient:                    schedulingv1.NewShiftQueryServiceClient(deps.Conn),
		conn:                                deps.Conn,
		meteredPointsLedgerQuery:            meteredPointsLedgerQuery{PointsLedgerQueryServiceClient: loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		searchIndexHistoryClient:            searchv1.NewSearchIndexHistoryServiceClient(deps.Conn),
		giftCardTransactionValidationClient: giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn),
		clients:                             deps.Clients,
		metricSeriesQueryClient:             metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		roleNotificationClient:              identityv1.NewRoleNotificationServiceClient(deps.Conn),
		pickListValidationClient:            warehousingv1.NewPickListValidationServiceClient(deps.Conn),
		pointsLedgerQueryClient:             loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn),
		giftCardBatchApprovalClient:         giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
	}
}

// StartGiftCardBatchWorkflow starts a gift card batch.
func (s *GiftCardBatchWorkflowServer) StartGiftCardBatchWorkflow(ctx context.Context, req *giftcardsv1.StartGiftCardBatchWorkflowRequest) (*giftcardsv1.StartGiftCardBatchWorkflowResponse, error) {
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "start gift card batch workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	exportAppointmentsStream, err := s.appointmentQueryClient.ExportAppointments(ctx, &appointmentsv1.ExportAppointmentsRequest{
		Context: req.GetContext(),
		Filter: &appointmentsv1.ExportAppointmentsRequest_Filter{
			Region:       req.GetGiftCardBatch().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetGiftCardBatch().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.appointments.v1.AppointmentQueryService/ExportAppointments")
	}
	for {
		received, err := exportAppointmentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.appointments.v1.AppointmentQueryService/ExportAppointments")
		}
		_ = received
	}
	var holder struct{ Phone string }
	if record := req.GetDetails().GetNotify(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Phone"); err != nil {
			s.log.WarnContext(ctx, "copy phone", "error", err)
		}
	}
	lockTenantRequest := &identityv1.LockTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.LockTenantRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				Phone: holder.Phone,
				Name:  req.GetDetails().GetNotify().GetName(),
			},
			NewOwner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
	lockTenantReply, err := grpcx.Call(ctx, s.tenantAdminClient.LockTenant, lockTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/LockTenant")
	}
	return &giftcardsv1.StartGiftCardBatchWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: lockTenantReply.GetResult().GetReferenceIds(),
			Status:       lockTenantReply.GetResult().GetStatus(),
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			GiftCardBatchId: req.GetGiftCardBatchId(),
			BillingAddress:  req.GetGiftCardBatch().GetBillingAddress(),
		},
	}, nil
}

// AdvanceGiftCardBatchWorkflow advances a gift card batch.
func (s *GiftCardBatchWorkflowServer) AdvanceGiftCardBatchWorkflow(ctx context.Context, req *giftcardsv1.AdvanceGiftCardBatchWorkflowRequest) (*giftcardsv1.AdvanceGiftCardBatchWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "advance gift card batch workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	delegateGiftCardTransactionApprovalRequest := &giftcardsv1.DelegateGiftCardTransactionApprovalRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: req.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
		},
	}
	send := s.delegateGiftCardTransactionApprovalSender(delegateGiftCardTransactionApprovalRequest)
	delegateGiftCardTransactionApprovalReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/DelegateGiftCardTransactionApproval")
	}
	var holder struct{ Region string }
	if record := req.GetGiftCardBatch().GetBillingAddress(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	exportShiftsStream, err := s.shiftQueryClient.ExportShifts(ctx, &schedulingv1.ExportShiftsRequest{
		Context: req.GetContext(),
		Filter:  &schedulingv1.ExportShiftsRequest_Filter{Region: holder.Region},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.scheduling.v1.ShiftQueryService/ExportShifts")
	}
	for {
		received, err := exportShiftsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.scheduling.v1.ShiftQueryService/ExportShifts")
		}
		_ = received
	}
	var holderName string
	switch choice := req.GetGiftCardBatch().GetSettlement().(type) {
	case *giftcardsv1.GiftCardBatch_Card:
		holderName = choice.Card.GetHolderName()
	case *giftcardsv1.GiftCardBatch_Bank:
		holderName = choice.Bank.GetIban()
	case *giftcardsv1.GiftCardBatch_VoucherCode:
		holderName = choice.VoucherCode
	}
	signalGiftCardWorkflowRequest := &giftcardsv1.SignalGiftCardWorkflowRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{HolderName: holderName}},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetGiftCardBatch().GetAudit().GetCreatedAt()},
		},
	}
	signalGiftCardWorkflowReply, err := grpcx.NewClient(s.conn, giftcardsv1.NewGiftCardWorkflowServiceClient).SignalGiftCardWorkflow(ctx, signalGiftCardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/SignalGiftCardWorkflow")
	}
	return &giftcardsv1.AdvanceGiftCardBatchWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: delegateGiftCardTransactionApprovalReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				GiftCardId: signalGiftCardWorkflowReply.GetGiftCard().GetGiftCardId(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Number:          req.GetGiftCardBatch().GetNumber(),
		},
	}, nil
}

// CancelGiftCardBatchWorkflow cancels a gift card batch.
func (s *GiftCardBatchWorkflowServer) CancelGiftCardBatchWorkflow(ctx context.Context, req *giftcardsv1.CancelGiftCardBatchWorkflowRequest) (*giftcardsv1.CancelGiftCardBatchWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "cancel gift card batch workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "gift_card_batch_id", req.GetGiftCardBatch().GetGiftCardBatchId())
	parameters := make(map[string]string, len(req.GetDetails().GetParameters()))
	for key, value := range req.GetDetails().GetParameters() {
		parameters[key] = value
	}
	countPointsLedgersRequest := &loyaltyv1.CountPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.CountPointsLedgersRequest_Filter{
			Region:       req.GetGiftCardBatch().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetGiftCardBatch().GetDueAt(),
			OwnerId:      tenancy.Value(ctx, "gift_card_batch_id"),
			Labels:       parameters,
		},
	}
	countPointsLedgersReply, err := s.meteredPointsLedgerQuery.CountPointsLedgers(ctx, countPointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/CountPointsLedgers")
	}
	resolveGiftCardBatchesRequest := &giftcardsv1.ResolveGiftCardBatchesRequest{Context: req.GetContext()}
	resolveGiftCardBatchesReply, err := resolveGiftCardBatchesCall(ctx, resolveGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/ResolveGiftCardBatches")
	}
	for _, line := range req.GetGiftCardBatch().GetLines() {
		if _, err := s.searchIndexHistoryClient.CompareSearchIndexVersions(ctx, &searchv1.CompareSearchIndexVersionsRequest{
			Context: req.GetContext(),
			SearchIndex: &searchv1.SearchIndex{
				Components: map[string]*searchv1.SearchIndexComponent{"primary": &searchv1.SearchIndexComponent{
					Quantity:    line.GetQuantity(),
					Description: line.GetDescription(),
				}},
				Audit: &commonv1.AuditInfo{Etag: req.GetGiftCardBatch().GetAudit().GetEtag()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/CompareSearchIndexVersions")
		}
	}
	return &giftcardsv1.CancelGiftCardBatchWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details:      countPointsLedgersReply.GetResult().GetDetails(),
			ReferenceIds: resolveGiftCardBatchesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &giftcardsv1.CancelGiftCardBatchWorkflowResponse_Outcome{
			Message: countPointsLedgersReply.GetResult().GetMessage(),
			Status:  resolveGiftCardBatchesReply.GetResult().GetStatus(),
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Number:          req.GetGiftCardBatch().GetNumber(),
		},
	}, nil
}

// RetryGiftCardBatchWorkflow retrys a gift card batch.
func (s *GiftCardBatchWorkflowServer) RetryGiftCardBatchWorkflow(ctx context.Context, req *giftcardsv1.RetryGiftCardBatchWorkflowRequest) (*giftcardsv1.RetryGiftCardBatchWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "retry gift card batch workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*giftcardsv1.RetryGiftCardBatchWorkflowRequest_Details)
	packed, err := anypb.New(req.GetGiftCardBatch())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintGiftCardTransactionsRequest := &giftcardsv1.LintGiftCardTransactionsRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.LintGiftCardTransactionsRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone:            cloned.GetNotify().GetPhone(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	lintGiftCardTransactionsReply, err := giftcardsv1.GiftCardTransactionValidationServiceClient.LintGiftCardTransactions(s.giftCardTransactionValidationClient, ctx, lintGiftCardTransactionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/LintGiftCardTransactions")
	}
	searchIndexNotification, err := registry.Lookup[searchv1.SearchIndexNotificationServiceClient](s.clients, "acme.search.v1.SearchIndexNotificationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded giftcardsv1.RetryGiftCardBatchWorkflowRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	notifySearchIndexWatchersRequest := &searchv1.NotifySearchIndexWatchersRequest{
		Context: req.GetContext(),
		Details: &searchv1.NotifySearchIndexWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetNotify().GetPhone(),
				PreferredChannel: (&decoded).GetNotify().GetPreferredChannel(),
				Email:            req.GetDetails().GetNotify().GetEmail(),
			},
		},
	}
	notifySearchIndexWatchersReply, err := searchIndexNotification.NotifySearchIndexWatchers(ctx, notifySearchIndexWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/NotifySearchIndexWatchers")
	}
	browseMetricSeriesesRequest := &metricsv1.BrowseMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.BrowseMetricSeriesesRequest_Filter{
			Region:       notifySearchIndexWatchersReply.GetSearchIndex().GetAddress().GetRegion(),
			CreatedAfter: notifySearchIndexWatchersReply.GetSearchIndex().GetAudit().GetUpdatedAt(),
		},
	}
	browseMetricSeriesesReply, err := grpcx.Call(ctx, s.metricSeriesQueryClient.BrowseMetricSerieses, browseMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses")
	}
	return &giftcardsv1.RetryGiftCardBatchWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: lintGiftCardTransactionsReply.GetGiftCardTransaction().GetCorrelationId(),
			Status:        lintGiftCardTransactionsReply.GetOutcome().GetStatus(),
			Details:       browseMetricSeriesesReply.GetResult().GetDetails(),
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: notifySearchIndexWatchersReply.GetSearchIndex().GetAddress().GetRecipient(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Number:          req.GetGiftCardBatch().GetNumber(),
		},
		Outcome: &giftcardsv1.RetryGiftCardBatchWorkflowResponse_Outcome{
			Status: notifySearchIndexWatchersReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// PauseGiftCardBatchWorkflow pauses a gift card batch.
func (s *GiftCardBatchWorkflowServer) PauseGiftCardBatchWorkflow(ctx context.Context, req *giftcardsv1.PauseGiftCardBatchWorkflowRequest) (*giftcardsv1.PauseGiftCardBatchWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseGiftCardBatchWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "pause gift card batch workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	notifyRoleWatchersRequest := &identityv1.NotifyRoleWatchersRequest{}
	notifyRoleWatchersRequest.Context = req.GetContext()
	notifyRoleWatchersRequest.Details = &identityv1.NotifyRoleWatchersRequest_Details{
		Recipient: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
	}
	notifyRoleWatchersReply, err := s.roleNotificationClient.NotifyRoleWatchers(ctx, notifyRoleWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/NotifyRoleWatchers")
	}
	giftCardBatchValidation, err := registry.Lookup[giftcardsv1.GiftCardBatchValidationServiceClient](s.clients, "acme.giftcards.v1.GiftCardBatchValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cloned := proto.Clone(req.GetDetails()).(*giftcardsv1.PauseGiftCardBatchWorkflowRequest_Details)
	lintGiftCardBatchesRequest := &giftcardsv1.LintGiftCardBatchesRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.LintGiftCardBatchesRequest_Details{
			Contact: &commonv1.ContactInfo{PreferredChannel: cloned.GetNotify().GetPreferredChannel()},
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetGiftCardBatch().GetBillingAddress().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: notifyRoleWatchersReply.GetRole().GetAudit().GetCreatedBy(),
			},
		},
	}
	lintGiftCardBatchesReply, err := giftCardBatchValidation.LintGiftCardBatches(ctx, lintGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/LintGiftCardBatches")
	}
	resolveGiftCardBatchesRequest := &giftcardsv1.ResolveGiftCardBatchesRequest{Context: req.GetContext()}
	resolveGiftCardBatchesReply := new(giftcardsv1.ResolveGiftCardBatchesResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardBatchQueryService_ResolveGiftCardBatches_FullMethodName, resolveGiftCardBatchesRequest, resolveGiftCardBatchesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/ResolveGiftCardBatches")
	}
	packed, err := anypb.New(req.GetGiftCardBatch())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreGiftCardTransactionRequest := &giftcardsv1.ScoreGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ScoreGiftCardTransactionRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: lintGiftCardBatchesReply.GetGiftCardBatch().GetBillingAddress().GetCountryCode(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	scoreGiftCardTransactionReply := new(giftcardsv1.ScoreGiftCardTransactionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", giftcardsGiftCardTransactionValidationServiceName, "ScoreGiftCardTransaction"), scoreGiftCardTransactionRequest, scoreGiftCardTransactionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/ScoreGiftCardTransaction")
	}
	return &giftcardsv1.PauseGiftCardBatchWorkflowResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: notifyRoleWatchersReply.GetRole().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintGiftCardBatchesReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
			},
			DueAt:           lintGiftCardBatchesReply.GetGiftCardBatch().GetDueAt(),
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Number:          req.GetGiftCardBatch().GetNumber(),
		},
		Outcome: &giftcardsv1.PauseGiftCardBatchWorkflowResponse_Outcome{Message: notifyRoleWatchersReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Details:       resolveGiftCardBatchesReply.GetResult().GetDetails(),
			CorrelationId: scoreGiftCardTransactionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ResumeGiftCardBatchWorkflow resumes a gift card batch.
func (s *GiftCardBatchWorkflowServer) ResumeGiftCardBatchWorkflow(ctx context.Context, req *giftcardsv1.ResumeGiftCardBatchWorkflowRequest) (*giftcardsv1.ResumeGiftCardBatchWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeGiftCardBatchWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "resume gift card batch workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetGiftCardBatch())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	delegateGiftCardTransactionApprovalRequest := &giftcardsv1.DelegateGiftCardTransactionApprovalRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.DelegateGiftCardTransactionApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       req.GetDetails().GetNotify().GetEmail(),
				DisplayName: req.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	delegateGiftCardTransactionApprovalReply := new(giftcardsv1.DelegateGiftCardTransactionApprovalResponse)
	if err := s.conn.Invoke(ctx, giftcardsGiftCardTransactionApprovalServiceDelegateGiftCardTransactionApprovalMethod, delegateGiftCardTransactionApprovalRequest, delegateGiftCardTransactionApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/DelegateGiftCardTransactionApproval")
	}
	var holder struct{ CreatedBy string }
	if record := req.GetGiftCardBatch().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "CreatedBy"); err != nil {
			s.log.WarnContext(ctx, "copy created_by", "error", err)
		}
	}
	validatePickListRequest := &warehousingv1.ValidatePickListRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{CreatedBy: holder.CreatedBy},
			Settlement: &warehousingv1.PickList_Bank{Bank: &warehousingv1.PickListBankSettlement{
				AccountHolder: req.GetGiftCardBatch().GetBank().GetAccountHolder(),
			}},
		},
	}
	type validatePickListResult struct {
		reply *warehousingv1.ValidatePickListResponse
		err   error
	}
	validatePickListResults := make(chan validatePickListResult, 1)
	go func() {
		reply, err := s.pickListValidationClient.ValidatePickList(ctx, validatePickListRequest)
		validatePickListResults <- validatePickListResult{reply, err}
	}()
	var validatePickListReply *warehousingv1.ValidatePickListResponse
	select {
	case result := <-validatePickListResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.warehousing.v1.PickListValidationService/ValidatePickList")
		}
		validatePickListReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &giftcardsv1.ResumeGiftCardBatchWorkflowResponse{
		Outcome: &giftcardsv1.ResumeGiftCardBatchWorkflowResponse_Outcome{
			Status:    delegateGiftCardTransactionApprovalReply.GetResult().GetStatus(),
			NextSteps: delegateGiftCardTransactionApprovalReply.GetOutcome().GetNextSteps(),
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{
				Lines: validatePickListReply.GetPickList().GetBillingAddress().GetLines(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Metadata:        req.GetGiftCardBatch().GetMetadata(),
		},
		Result: &commonv1.OperationResult{
			Details: validatePickListReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// InspectGiftCardBatchWorkflow returns a gift card batch.
func (s *GiftCardBatchWorkflowServer) InspectGiftCardBatchWorkflow(ctx context.Context, req *giftcardsv1.InspectGiftCardBatchWorkflowRequest) (*giftcardsv1.InspectGiftCardBatchWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectGiftCardBatchWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_batch", req.GetGiftCardBatchId(), "inspect_gift_card_batch_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect gift card batch workflow", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeTenantUpdatesRequest := &identityv1.SubscribeTenantUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeTenantUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: cachedContext.GetActor().GetEmail()},
			Payload:   packed,
		},
		TenantId:    req.GetContext().GetTenantId(),
		Annotations: baggage,
	}
	subscribeTenantUpdatesReply := new(identityv1.SubscribeTenantUpdatesResponse)
	if err := s.conn.Invoke(ctx, identityTenantNotificationServiceSubscribeTenantUpdatesMethod, subscribeTenantUpdatesRequest, subscribeTenantUpdatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/SubscribeTenantUpdates")
	}
	countPointsLedgersRequest := &loyaltyv1.CountPointsLedgersRequest{}
	countPointsLedgersRequest.Context = req.GetContext()
	countPointsLedgersRequest.Filter = &loyaltyv1.CountPointsLedgersRequest_Filter{
		Region:       subscribeTenantUpdatesReply.GetTenant().GetLocation().GetRegion(),
		CreatedAfter: subscribeTenantUpdatesReply.GetTenant().GetAudit().GetCreatedAt(),
	}
	countPointsLedgersReply, err := s.pointsLedgerQueryClient.CountPointsLedgers(ctx, countPointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/CountPointsLedgers")
	}
	return &giftcardsv1.InspectGiftCardBatchWorkflowResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: subscribeTenantUpdatesReply.GetTenant().GetAudit().GetUpdatedBy(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
		},
		Result: &commonv1.OperationResult{
			Status:  countPointsLedgersReply.GetResult().GetStatus(),
			Message: countPointsLedgersReply.GetResult().GetMessage(),
		},
	}, nil
}

// ListGiftCardBatchWorkflows lists gift card batches a page at a time.
func (s *GiftCardBatchWorkflowServer) ListGiftCardBatchWorkflows(ctx context.Context, req *giftcardsv1.ListGiftCardBatchWorkflowsRequest) (*giftcardsv1.ListGiftCardBatchWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list gift card batch workflows", "tenant", req.GetContext().GetTenantId())
	normalizeGiftCardTransactionRequest := newNormalizeGiftCardTransactionRequest(withNormalizeGiftCardTransactionContext(req.GetContext()), withNormalizeGiftCardTransactionDetails(&giftcardsv1.NormalizeGiftCardTransactionRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Locale:  req.GetContext().GetLocale(),
	}), withNormalizeGiftCardTransactionGiftCardTransaction(&giftcardsv1.GiftCardTransaction{
		Measurements: []*giftcardsv1.GiftCardTransactionMeasurement{&giftcardsv1.GiftCardTransactionMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
		Source:       req.GetFilter().GetOwnerId(),
	}))
	normalizeGiftCardTransactionReply, err := s.giftCardTransactionValidationClient.NormalizeGiftCardTransaction(ctx, normalizeGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/NormalizeGiftCardTransaction")
	}
	return &giftcardsv1.ListGiftCardBatchWorkflowsResponse{
		Result: &commonv1.OperationResult{
			Details:       normalizeGiftCardTransactionReply.GetResult().GetDetails(),
			CorrelationId: normalizeGiftCardTransactionReply.GetGiftCardTransaction().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// SignalGiftCardBatchWorkflow signals a gift card batch.
func (s *GiftCardBatchWorkflowServer) SignalGiftCardBatchWorkflow(ctx context.Context, req *giftcardsv1.SignalGiftCardBatchWorkflowRequest) (*giftcardsv1.SignalGiftCardBatchWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "signal gift card batch workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	recallGiftCardBatchRequest := &giftcardsv1.RecallGiftCardBatchRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.RecallGiftCardBatchRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{Etag: req.GetGiftCardBatch().GetAudit().GetEtag()},
		},
	}
	var recallGiftCardBatchReply *giftcardsv1.RecallGiftCardBatchResponse
	if err := pipeline.Run(ctx, "acme.giftcards.v1.GiftCardBatchApprovalService/RecallGiftCardBatch", func(ctx context.Context) error {
		answer, err := s.giftCardBatchApprovalClient.RecallGiftCardBatch(ctx, recallGiftCardBatchRequest)
		recallGiftCardBatchReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/RecallGiftCardBatch")
	}
	previewPickListRequest := &warehousingv1.PreviewPickListRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			BillingAddress: &commonv1.PostalAddress{
				Region: recallGiftCardBatchReply.GetGiftCardBatch().GetBillingAddress().GetRegion(),
			},
			Settlement: &warehousingv1.PickList_Bank{Bank: &warehousingv1.PickListBankSettlement{
				Iban:          recallGiftCardBatchReply.GetGiftCardBatch().GetBank().GetIban(),
				AccountHolder: recallGiftCardBatchReply.GetGiftCardBatch().GetBank().GetAccountHolder(),
			}},
		},
	}
	previewPickListReply := new(warehousingv1.PreviewPickListResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.warehousing.v1.PickListValidationService/PreviewPickList", previewPickListRequest, previewPickListReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/PreviewPickList")
	}
	giftCardTransactionApproval, ok := s.clients.Get("acme.giftcards.v1.GiftCardTransactionApprovalService").(giftcardsv1.GiftCardTransactionApprovalServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardTransactionApprovalService")
	}
	cloned := proto.Clone(req.GetDetails()).(*giftcardsv1.SignalGiftCardBatchWorkflowRequest_Details)
	submitGiftCardTransactionRequest := &giftcardsv1.SubmitGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.SubmitGiftCardTransactionRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: previewPickListReply.GetPickList().GetCounterparty().GetDisplayName(),
				Email:       cloned.GetNotify().GetEmail(),
			},
		},
	}
	submitGiftCardTransactionReply, err := giftCardTransactionApproval.SubmitGiftCardTransaction(ctx, submitGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/SubmitGiftCardTransaction")
	}
	return &giftcardsv1.SignalGiftCardBatchWorkflowResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: recallGiftCardBatchReply.GetGiftCardBatch().GetBillingAddress(),
			Counterparty: &giftcardsv1.GiftCardRef{
				GiftCardId: recallGiftCardBatchReply.GetGiftCardBatch().GetCounterparty().GetGiftCardId(),
			},
			GiftCardBatchId: submitGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetGiftCardBatchId(),
			IssuedAt:        req.GetGiftCardBatch().GetIssuedAt(),
			Total:           req.GetGiftCardBatch().GetTotal(),
		},
		Result: &commonv1.OperationResult{
			Message: previewPickListReply.GetResult().GetMessage(),
			Status:  previewPickListReply.GetResult().GetStatus(),
		},
	}, nil
}

// delegateGiftCardTransactionApprovalSender returns a function that sends one request to
// acme.giftcards.v1.GiftCardTransactionApprovalService/DelegateGiftCardTransactionApproval.
func (s *GiftCardBatchWorkflowServer) delegateGiftCardTransactionApprovalSender(request *giftcardsv1.DelegateGiftCardTransactionApprovalRequest) func(context.Context) (*giftcardsv1.DelegateGiftCardTransactionApprovalResponse, error) {
	return func(ctx context.Context) (*giftcardsv1.DelegateGiftCardTransactionApprovalResponse, error) {
		return s.giftCardTransactionApprovalClient.DelegateGiftCardTransactionApproval(ctx, request)
	}
}
