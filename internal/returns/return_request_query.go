package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fleetv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fleet/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	procurementv1 "github.com/hounddogai/protomap-test-golang/gen/acme/procurement/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReturnRequestQueryServer implements `acme.returns.v1.ReturnRequestQueryService`.
// Answers read-only questions about return requests, for screens and reports.
type ReturnRequestQueryServer struct {
	returnsv1.UnimplementedReturnRequestQueryServiceServer
	store                                *Store
	log                                  *slog.Logger
	restockDecisionCalculationPort       restockDecisionCalculationPort
	vehicleClient                        fleetv1.VehicleServiceClient
	inspectionValidationClient           returnsv1.InspectionValidationServiceClient
	returnLabelValidationPort            returnLabelValidationPort
	restockDecisionCalculationClient     returnsv1.RestockDecisionCalculationServiceClient
	conn                                 *grpc.ClientConn
	purchaseRequisitionCalculationClient procurementv1.PurchaseRequisitionCalculationServiceClient
	tenantExportClient                   identityv1.TenantExportServiceClient
	accessPolicyValidationClient         authorizationv1.AccessPolicyValidationServiceClient
}

// NewReturnRequestQueryServer wires a ReturnRequestQueryServer.
func NewReturnRequestQueryServer(deps platform.Deps) ReturnRequestQueryServer {
	return ReturnRequestQueryServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.returns.v1.ReturnRequestQueryService"),
		restockDecisionCalculationPort:       newRestockDecisionCalculationPort(deps),
		vehicleClient:                        fleetv1.NewVehicleServiceClient(deps.Conn),
		inspectionValidationClient:           returnsv1.NewInspectionValidationServiceClient(deps.Conn),
		returnLabelValidationPort:            newReturnLabelValidationPort(deps),
		restockDecisionCalculationClient:     returnsv1.NewRestockDecisionCalculationServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		purchaseRequisitionCalculationClient: procurementv1.NewPurchaseRequisitionCalculationServiceClient(deps.Conn),
		tenantExportClient:                   identityv1.NewTenantExportServiceClient(deps.Conn),
		accessPolicyValidationClient:         authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
	}
}

// LookupReturnRequest returns a return request.
func (s ReturnRequestQueryServer) LookupReturnRequest(ctx context.Context, req *returnsv1.LookupReturnRequestRequest) (*returnsv1.LookupReturnRequestResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_request", req.GetReturnRequestId(), "lookup_return_request"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup return request", "tenant", req.GetContext().GetTenantId())
	explainRestockDecisionCalculationRequest := &returnsv1.ExplainRestockDecisionCalculationRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	explainRestockDecisionCalculationReply, err := s.restockDecisionCalculationPort.ExplainRestockDecisionCalculation(ctx, explainRestockDecisionCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/ExplainRestockDecisionCalculation")
	}
	return &returnsv1.LookupReturnRequestResponse{
		Result: &commonv1.OperationResult{
			Status: explainRestockDecisionCalculationReply.GetOutcome().GetStatus(),
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
				Tax: &returnsv1.ReturnRequestLineTax{
					Amount: explainRestockDecisionCalculationReply.GetRestockDecision().GetDiscount().GetAmount(),
				},
			}},
			ReturnRequestId: req.GetReturnRequestId(),
		},
	}, nil
}

// BrowseReturnRequests lists return requests a page at a time.
func (s ReturnRequestQueryServer) BrowseReturnRequests(ctx context.Context, req *returnsv1.BrowseReturnRequestsRequest) (*returnsv1.BrowseReturnRequestsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "browse return requests", "tenant", req.GetContext().GetTenantId())
	listVehiclesRequest := &fleetv1.ListVehiclesRequest{
		Context: req.GetContext(),
		Filter:  &fleetv1.ListVehiclesRequest_Filter{CreatedAfter: req.GetFilter().GetCreatedAfter()},
	}
	type listVehiclesResult struct {
		reply *fleetv1.ListVehiclesResponse
		err   error
	}
	listVehiclesResults := make(chan listVehiclesResult, 1)
	go func() {
		reply, err := s.vehicleClient.ListVehicles(ctx, listVehiclesRequest)
		listVehiclesResults <- listVehiclesResult{reply, err}
	}()
	var listVehiclesReply *fleetv1.ListVehiclesResponse
	select {
	case result := <-listVehiclesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fleet.v1.VehicleService/ListVehicles")
		}
		listVehiclesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	checkInspectionEligibilityRequest := &returnsv1.CheckInspectionEligibilityRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Origin: &returnsv1.Inspection_Customer{Customer: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()}},
		},
	}
	var checkInspectionEligibilityReply *returnsv1.CheckInspectionEligibilityResponse
	if err := pipeline.Run(ctx, "acme.returns.v1.InspectionValidationService/CheckInspectionEligibility", func(ctx context.Context) error {
		answer, err := s.inspectionValidationClient.CheckInspectionEligibility(ctx, checkInspectionEligibilityRequest)
		checkInspectionEligibilityReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionValidationService/CheckInspectionEligibility")
	}
	return &returnsv1.BrowseReturnRequestsResponse{
		PageInfo: &commonv1.PageInfo{TotalSize: listVehiclesReply.GetPageInfo().GetTotalSize()},
		ReturnRequests: []*returnsv1.ReturnRequest{&returnsv1.ReturnRequest{
			Audit: &commonv1.AuditInfo{
				CreatedAt: checkInspectionEligibilityReply.GetInspection().GetAudit().GetCreatedAt(),
				UpdatedBy: checkInspectionEligibilityReply.GetInspection().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// FindReturnRequests lists return requests a page at a time.
func (s ReturnRequestQueryServer) FindReturnRequests(ctx context.Context, req *returnsv1.FindReturnRequestsRequest) (*returnsv1.FindReturnRequestsResponse, error) {
	s.log.InfoContext(ctx, "find return requests", "tenant", req.GetContext().GetTenantId())
	return &returnsv1.FindReturnRequestsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ResolveReturnRequests returns several return requests at once.
func (s ReturnRequestQueryServer) ResolveReturnRequests(ctx context.Context, req *returnsv1.ResolveReturnRequestsRequest) (*returnsv1.ResolveReturnRequestsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "resolve return requests", "tenant", req.GetContext().GetTenantId())
	previewReturnLabelRequest := &returnsv1.PreviewReturnLabelRequest{
		Context: req.GetContext(),
		Details: &returnsv1.PreviewReturnLabelRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
			Lines: []*returnsv1.ReturnLabelLine{&returnsv1.ReturnLabelLine{Attributes: req.GetContext().GetBaggage()}},
		},
	}
	previewReturnLabelReply, err := s.returnLabelValidationPort.PreviewReturnLabel(ctx, previewReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelValidationService/PreviewReturnLabel")
	}
	return &returnsv1.ResolveReturnRequestsResponse{
		ReturnRequests: []*returnsv1.ReturnRequest{&returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: previewReturnLabelReply.GetReturnLabel().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: previewReturnLabelReply.GetReturnLabel().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExportReturnRequests streams every matching return request.
func (s ReturnRequestQueryServer) ExportReturnRequests(req *returnsv1.ExportReturnRequestsRequest, stream grpc.ServerStreamingServer[returnsv1.ExportReturnRequestsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export return requests", "tenant", req.GetContext().GetTenantId())
	quoteRestockDecisionReply, err := s.restockDecisionCalculationClient.QuoteRestockDecision(ctx, &returnsv1.QuoteRestockDecisionRequest{
		Context: req.GetContext(),
		Details: &returnsv1.QuoteRestockDecisionRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/QuoteRestockDecision")
	}
	explainRestockDecisionCalculationRequest := &returnsv1.ExplainRestockDecisionCalculationRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				UserId: quoteRestockDecisionReply.GetRestockDecision().GetOwner().GetUserId(),
			},
			Action: &returnsv1.RestockDecision_Block{Block: &returnsv1.RestockDecisionBlockAction{
				Reason: quoteRestockDecisionReply.GetRestockDecision().GetBlock().GetReason(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: quoteRestockDecisionReply.GetRestockDecision().GetAudit().GetUpdatedBy(),
			},
		},
	}
	explainRestockDecisionCalculationReply := new(returnsv1.ExplainRestockDecisionCalculationResponse)
	if err := s.conn.Invoke(ctx, returnsRestockDecisionCalculationServiceExplainRestockDecisionCalculationMethod, explainRestockDecisionCalculationRequest, explainRestockDecisionCalculationReply); err != nil {
		return grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/ExplainRestockDecisionCalculation")
	}
	if err := stream.Send(&returnsv1.ExportReturnRequestsResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: quoteRestockDecisionReply.GetRestockDecision().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainRestockDecisionCalculationReply.GetRestockDecision().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{
			CorrelationId: quoteRestockDecisionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeReturnRequests summarizes a return request.
func (s ReturnRequestQueryServer) SummarizeReturnRequests(ctx context.Context, req *returnsv1.SummarizeReturnRequestsRequest) (*returnsv1.SummarizeReturnRequestsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_request", req.GetReturnRequestId(), "summarize_return_requests"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize return requests", "tenant", req.GetContext().GetTenantId())
	quotePurchaseRequisitionRequest := &procurementv1.QuotePurchaseRequisitionRequest{}
	quotePurchaseRequisitionRequest.Context = req.GetContext()
	quotePurchaseRequisitionRequest.Annotations = req.GetAnnotations()
	quotePurchaseRequisitionRequest.Reason = req.GetReason()
	quotePurchaseRequisitionReply, err := s.purchaseRequisitionCalculationClient.QuotePurchaseRequisition(ctx, quotePurchaseRequisitionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.procurement.v1.PurchaseRequisitionCalculationService/QuotePurchaseRequisition")
	}
	return &returnsv1.SummarizeReturnRequestsResponse{
		Result: &commonv1.OperationResult{
			Details: quotePurchaseRequisitionReply.GetResult().GetDetails(),
			Status:  quotePurchaseRequisitionReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestReturnRequests lists return requests a page at a time.
func (s ReturnRequestQueryServer) SuggestReturnRequests(ctx context.Context, req *returnsv1.SuggestReturnRequestsRequest) (*returnsv1.SuggestReturnRequestsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "suggest return requests", "tenant", req.GetContext().GetTenantId())
	streamTenantExportStream, err := s.tenantExportClient.StreamTenantExport(ctx, &identityv1.StreamTenantExportRequest{
		Context: req.GetContext(),
		Filter: &identityv1.StreamTenantExportRequest_Filter{
			Labels: req.GetFilter().GetLabels(),
			Region: req.GetFilter().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
	}
	for {
		received, err := streamTenantExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
		}
		_ = received
	}
	checkAccessPolicyEligibilityReply, err := s.accessPolicyValidationClient.CheckAccessPolicyEligibility(ctx, &authorizationv1.CheckAccessPolicyEligibilityRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		Details: &authorizationv1.CheckAccessPolicyEligibilityRequest_Details{Locale: req.GetContext().GetLocale()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/CheckAccessPolicyEligibility")
	}
	return &returnsv1.SuggestReturnRequestsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: checkAccessPolicyEligibilityReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// PeekReturnRequest returns a return request.
func (s ReturnRequestQueryServer) PeekReturnRequest(ctx context.Context, req *returnsv1.PeekReturnRequestRequest) (*returnsv1.PeekReturnRequestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_request", req.GetReturnRequestId(), "peek_return_request"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek return request", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	quoteRestockDecisionRequest := &returnsv1.QuoteRestockDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  tenancy.Value(ctx, "email"),
			},
		},
	}
	quoteRestockDecisionReply := new(returnsv1.QuoteRestockDecisionResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.returns.v1.RestockDecisionCalculationService/QuoteRestockDecision", quoteRestockDecisionRequest, quoteRestockDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/QuoteRestockDecision")
	}
	return &returnsv1.PeekReturnRequestResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			Audit: &commonv1.AuditInfo{
				CreatedAt: quoteRestockDecisionReply.GetRestockDecision().GetAudit().GetCreatedAt(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: quoteRestockDecisionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}
