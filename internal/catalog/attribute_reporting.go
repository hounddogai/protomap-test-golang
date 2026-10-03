package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	lendingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/lending/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// AttributeReportingServer implements `acme.catalog.v1.AttributeReportingService`.
// Builds and schedules reports about attributes.
type AttributeReportingServer struct {
	catalogv1.UnimplementedAttributeReportingServiceServer
	store                        *Store
	log                          *slog.Logger
	meteredForm                  meteredForm
	userClient                   identityv1.UserServiceClient
	brandValidationClient        catalogv1.BrandValidationServiceClient
	packingSlipApprovalClient    fulfillmentv1.PackingSlipApprovalServiceClient
	metricSeriesValidationClient metricsv1.MetricSeriesValidationServiceClient
	clients                      *registry.Registry
	loanApplicationQueryClient   lendingv1.LoanApplicationQueryServiceClient
	brandReportingClient         catalogv1.BrandReportingServiceClient
	sessionNotificationClient    identityv1.SessionNotificationServiceClient
	productValidationClient      catalogv1.ProductValidationServiceClient
	returnRequestApprovalClient  returnsv1.ReturnRequestApprovalServiceClient
}

// NewAttributeReportingServer wires an AttributeReportingServer.
func NewAttributeReportingServer(deps platform.Deps) *AttributeReportingServer {
	return &AttributeReportingServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.catalog.v1.AttributeReportingService"),
		meteredForm:                  meteredForm{FormServiceClient: formsv1.NewFormServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		userClient:                   identityv1.NewUserServiceClient(deps.Conn),
		brandValidationClient:        catalogv1.NewBrandValidationServiceClient(deps.Conn),
		packingSlipApprovalClient:    fulfillmentv1.NewPackingSlipApprovalServiceClient(deps.Conn),
		metricSeriesValidationClient: metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		clients:                      deps.Clients,
		loanApplicationQueryClient:   lendingv1.NewLoanApplicationQueryServiceClient(deps.Conn),
		brandReportingClient:         catalogv1.NewBrandReportingServiceClient(deps.Conn),
		sessionNotificationClient:    identityv1.NewSessionNotificationServiceClient(deps.Conn),
		productValidationClient:      catalogv1.NewProductValidationServiceClient(deps.Conn),
		returnRequestApprovalClient:  returnsv1.NewReturnRequestApprovalServiceClient(deps.Conn),
	}
}

// GenerateAttributeReport generates an attribute.
func (s *AttributeReportingServer) GenerateAttributeReport(ctx context.Context, req *catalogv1.GenerateAttributeReportRequest) (*catalogv1.GenerateAttributeReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "generate_attribute_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate attribute report", "tenant", req.GetContext().GetTenantId())
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	createFormRequest := &formsv1.CreateFormRequest{
		Context: req.GetContext(),
		Form: &formsv1.Form{
			Counterparty: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			Lines:        []*formsv1.FormLine{&formsv1.FormLine{Attributes: filters}},
		},
	}
	createFormReply, err := s.meteredForm.CreateForm(ctx, createFormRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forms.v1.FormService/CreateForm")
	}
	return &catalogv1.GenerateAttributeReportResponse{
		Result: &commonv1.OperationResult{
			Details: createFormReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchAttributeReport returns an attribute.
func (s *AttributeReportingServer) FetchAttributeReport(ctx context.Context, req *catalogv1.FetchAttributeReportRequest) (*catalogv1.FetchAttributeReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "fetch_attribute_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch attribute report", "tenant", req.GetContext().GetTenantId())
	batchGetUsersRequest := &identityv1.BatchGetUsersRequest{Context: req.GetContext()}
	send := s.batchGetUsersSender(batchGetUsersRequest)
	batchGetUsersReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/BatchGetUsers")
	}
	return &catalogv1.FetchAttributeReportResponse{
		Result: &commonv1.OperationResult{
			Message: batchGetUsersReply.GetResult().GetMessage(),
			Status:  batchGetUsersReply.GetResult().GetStatus(),
		},
		Attribute: &catalogv1.Attribute{AttributeId: req.GetAttributeId()},
	}, nil
}

// ScheduleAttributeReport schedules an attribute.
func (s *AttributeReportingServer) ScheduleAttributeReport(ctx context.Context, req *catalogv1.ScheduleAttributeReportRequest) (*catalogv1.ScheduleAttributeReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "schedule_attribute_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule attribute report", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	normalizeBrandRequest := &catalogv1.NormalizeBrandRequest{
		Context: req.GetContext(),
		Reason:  tenancy.Value(ctx, "reason"),
	}
	normalizeBrandReply, err := grpcx.Call(ctx, s.brandValidationClient.NormalizeBrand, normalizeBrandRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandValidationService/NormalizeBrand")
	}
	listPendingPackingSlipsRequest := &fulfillmentv1.ListPendingPackingSlipsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.ListPendingPackingSlipsRequest_Filter{
			Region:       normalizeBrandReply.GetBrand().GetPrimaryAddress().GetRegion(),
			CreatedAfter: normalizeBrandReply.GetBrand().GetAudit().GetCreatedAt(),
			OwnerId:      normalizeBrandReply.GetBrand().GetRelationshipManager().GetUserId(),
		},
	}
	listPendingPackingSlipsReply, err := grpcx.Call(ctx, s.packingSlipApprovalClient.ListPendingPackingSlips, listPendingPackingSlipsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipApprovalService/ListPendingPackingSlips")
	}
	return &catalogv1.ScheduleAttributeReportResponse{
		Result: &commonv1.OperationResult{
			Details:       normalizeBrandReply.GetResult().GetDetails(),
			CorrelationId: normalizeBrandReply.GetResult().GetCorrelationId(),
			Status:        listPendingPackingSlipsReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamAttributeMetrics streams the changes of attributes as they happen.
func (s *AttributeReportingServer) StreamAttributeMetrics(req *catalogv1.StreamAttributeMetricsRequest, stream grpc.ServerStreamingServer[catalogv1.StreamAttributeMetricsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream attribute metrics", "tenant", req.GetContext().GetTenantId())
	validateMetricSeriesReply, err := s.metricSeriesValidationClient.ValidateMetricSeries(ctx, &metricsv1.ValidateMetricSeriesRequest{
		Context: req.GetContext(),
		Details: &metricsv1.ValidateMetricSeriesRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetSubscriber().GetEmail(),
				Phone: req.GetSubscriber().GetPhone(),
			},
		},
		MetricSeries: &metricsv1.MetricSeries{
			Locator: &metricsv1.MetricSeries_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/ValidateMetricSeries")
	}
	scoreMetricSeriesReply, err := s.metricSeriesValidationClient.ScoreMetricSeries(ctx, &metricsv1.ScoreMetricSeriesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Dimensions: &metricsv1.MetricSeriesDimensions{
				Unit: validateMetricSeriesReply.GetMetricSeries().GetDimensions().GetUnit(),
			},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/ScoreMetricSeries")
	}
	_ = scoreMetricSeriesReply
	if err := stream.Send(&catalogv1.StreamAttributeMetricsResponse{
		Attribute: &catalogv1.Attribute{
			Owner: validateMetricSeriesReply.GetMetricSeries().GetOwner(),
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateMetricSeriesReply.GetMetricSeries().GetAudit().GetCreatedBy(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// ListAttributeReports lists attributes a page at a time.
func (s *AttributeReportingServer) ListAttributeReports(ctx context.Context, req *catalogv1.ListAttributeReportsRequest) (*catalogv1.ListAttributeReportsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list attribute reports", "tenant", req.GetContext().GetTenantId())
	packingSlipApproval, err := registry.Lookup[fulfillmentv1.PackingSlipApprovalServiceClient](s.clients, "acme.fulfillment.v1.PackingSlipApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	recallPackingSlipRequest := &fulfillmentv1.RecallPackingSlipRequest{
		Context: req.GetContext(),
		PackingSlip: &fulfillmentv1.PackingSlip{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	recallPackingSlipReply, err := packingSlipApproval.RecallPackingSlip(ctx, recallPackingSlipRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipApprovalService/RecallPackingSlip")
	}
	return &catalogv1.ListAttributeReportsResponse{
		Attributes: []*catalogv1.Attribute{&catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				CreatedAt: recallPackingSlipReply.GetPackingSlip().GetAudit().GetCreatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ShareAttributeReport shares an attribute.
func (s *AttributeReportingServer) ShareAttributeReport(ctx context.Context, req *catalogv1.ShareAttributeReportRequest) (*catalogv1.ShareAttributeReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ShareAttributeReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "share_attribute_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share attribute report", "tenant", req.GetContext().GetTenantId())
	var envelope catalogv1.ProductRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	summarizeLoanApplicationsRequest := &lendingv1.SummarizeLoanApplicationsRequest{
		Context:           req.GetContext(),
		Reason:            req.GetReason(),
		LoanApplicationId: envelope.GetDisplayName(),
	}
	type summarizeLoanApplicationsResult struct {
		reply *lendingv1.SummarizeLoanApplicationsResponse
		err   error
	}
	summarizeLoanApplicationsResults := make(chan summarizeLoanApplicationsResult, 1)
	go func() {
		reply, err := s.loanApplicationQueryClient.SummarizeLoanApplications(ctx, summarizeLoanApplicationsRequest)
		summarizeLoanApplicationsResults <- summarizeLoanApplicationsResult{reply, err}
	}()
	var summarizeLoanApplicationsReply *lendingv1.SummarizeLoanApplicationsResponse
	select {
	case result := <-summarizeLoanApplicationsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.lending.v1.LoanApplicationQueryService/SummarizeLoanApplications")
		}
		summarizeLoanApplicationsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	variables := make(map[string]string, len(req.GetDetails().GetVariables()))
	for key, value := range req.GetDetails().GetVariables() {
		variables[key] = value
	}
	countBrandTotalsRequest := &catalogv1.CountBrandTotalsRequest{
		Context: req.GetContext(),
		Filter: &catalogv1.CountBrandTotalsRequest_Filter{
			Labels:  variables,
			OwnerId: req.GetDetails().GetTemplateId(),
		},
	}
	countBrandTotalsReply, err := grpcx.Call(ctx, s.brandReportingClient.CountBrandTotals, countBrandTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/CountBrandTotals")
	}
	cloned := proto.Clone(req.GetDetails()).(*catalogv1.ShareAttributeReportRequest_Details)
	var envelope2 catalogv1.ProductRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	streamSessionNoticesStream, err := s.sessionNotificationClient.StreamSessionNotices(ctx, &identityv1.StreamSessionNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone:            cloned.GetRecipient().GetPhone(),
			Email:            req.GetDetails().GetRecipient().GetEmail(),
			PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
		},
		Filter: &identityv1.StreamSessionNoticesRequest_Filter{Query: envelope2.GetDisplayName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
	}
	for {
		received, err := streamSessionNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
		}
		_ = received
	}
	var envelope3 catalogv1.ProductRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateProductRequest := &catalogv1.ValidateProductRequest{
		Context: req.GetContext(),
		Details: &catalogv1.ValidateProductRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetRecipient().GetEmail(),
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
				Name:             req.GetDetails().GetRecipient().GetName(),
				Phone:            req.GetDetails().GetRecipient().GetPhone(),
			},
		},
		ProductId: envelope3.GetDisplayName(),
	}
	validateProductReply, err := grpcx.Call(ctx, s.productValidationClient.ValidateProduct, validateProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/ValidateProduct")
	}
	return &catalogv1.ShareAttributeReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeLoanApplicationsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &catalogv1.ShareAttributeReportResponse_Outcome{
			Status:  countBrandTotalsReply.GetResult().GetStatus(),
			Message: validateProductReply.GetOutcome().GetMessage(),
		},
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: validateProductReply.GetProduct().GetAudit().GetUpdatedBy(),
			},
			AttributeId: req.GetAttributeId(),
			Owner:       &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}, nil
}

// DeleteAttributeReport deletes an attribute.
func (s *AttributeReportingServer) DeleteAttributeReport(ctx context.Context, req *catalogv1.DeleteAttributeReportRequest) (*catalogv1.DeleteAttributeReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DeleteAttributeReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "delete_attribute_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete attribute report", "tenant", req.GetContext().GetTenantId())
	delegateReturnRequestApprovalRequest := &returnsv1.DelegateReturnRequestApprovalRequest{
		Context:       req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{Audit: &commonv1.AuditInfo{Etag: req.GetEtag()}},
		Details: &returnsv1.DelegateReturnRequestApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	type delegateReturnRequestApprovalResult struct {
		reply *returnsv1.DelegateReturnRequestApprovalResponse
		err   error
	}
	delegateReturnRequestApprovalResults := make(chan delegateReturnRequestApprovalResult, 1)
	go func() {
		reply, err := s.returnRequestApprovalClient.DelegateReturnRequestApproval(ctx, delegateReturnRequestApprovalRequest)
		delegateReturnRequestApprovalResults <- delegateReturnRequestApprovalResult{reply, err}
	}()
	var delegateReturnRequestApprovalReply *returnsv1.DelegateReturnRequestApprovalResponse
	select {
	case result := <-delegateReturnRequestApprovalResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.returns.v1.ReturnRequestApprovalService/DelegateReturnRequestApproval")
		}
		delegateReturnRequestApprovalReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &catalogv1.DeleteAttributeReportResponse{
		Result: &commonv1.OperationResult{
			Message: delegateReturnRequestApprovalReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// batchGetUsersSender returns a function that sends one request to acme.identity.v1.UserService/BatchGetUsers.
func (s *AttributeReportingServer) batchGetUsersSender(request *identityv1.BatchGetUsersRequest) func(context.Context) (*identityv1.BatchGetUsersResponse, error) {
	return func(ctx context.Context) (*identityv1.BatchGetUsersResponse, error) {
		return s.userClient.BatchGetUsers(ctx, request)
	}
}
