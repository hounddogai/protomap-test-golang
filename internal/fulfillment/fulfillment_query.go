package fulfillment

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// FulfillmentQueryServer implements `acme.fulfillment.v1.FulfillmentQueryService`.
// Answers read-only questions about fulfillments, for screens and reports.
type FulfillmentQueryServer struct {
	fulfillmentv1.UnimplementedFulfillmentQueryServiceServer
	store                                *Store
	log                                  *slog.Logger
	fulfillmentExceptionValidationClient fulfillmentv1.FulfillmentExceptionValidationServiceClient
	conn                                 *grpc.ClientConn
	meteredAccessPolicyValidation        meteredAccessPolicyValidation
	accessPolicyValidationClient         authorizationv1.AccessPolicyValidationServiceClient
	accessPolicyValidationClientFallback authorizationv1.AccessPolicyValidationServiceClient
	carrierRateValidationClient          shippingv1.CarrierRateValidationServiceClient
	metricSeriesClient                   metricsv1.MetricSeriesServiceClient
}

// NewFulfillmentQueryServer wires a FulfillmentQueryServer.
func NewFulfillmentQueryServer(deps platform.Deps) FulfillmentQueryServer {
	return FulfillmentQueryServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.fulfillment.v1.FulfillmentQueryService"),
		fulfillmentExceptionValidationClient: fulfillmentv1.NewFulfillmentExceptionValidationServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		meteredAccessPolicyValidation:        meteredAccessPolicyValidation{AccessPolicyValidationServiceClient: authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		accessPolicyValidationClient:         authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		accessPolicyValidationClientFallback: authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		carrierRateValidationClient:          shippingv1.NewCarrierRateValidationServiceClient(deps.Conn),
		metricSeriesClient:                   metricsv1.NewMetricSeriesServiceClient(deps.Conn),
	}
}

// LookupFulfillment returns a fulfillment.
func (s FulfillmentQueryServer) LookupFulfillment(ctx context.Context, req *fulfillmentv1.LookupFulfillmentRequest) (*fulfillmentv1.LookupFulfillmentResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fulfillment", req.GetFulfillmentId(), "lookup_fulfillment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup fulfillment", "tenant", req.GetContext().GetTenantId())
	validateFulfillmentExceptionRequest := &fulfillmentv1.ValidateFulfillmentExceptionRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Assignee:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &fulfillmentv1.ValidateFulfillmentExceptionRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	var validateFulfillmentExceptionReply *fulfillmentv1.ValidateFulfillmentExceptionResponse
	if err := pipeline.Run(ctx, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ValidateFulfillmentException", func(ctx context.Context) error {
		answer, err := s.fulfillmentExceptionValidationClient.ValidateFulfillmentException(ctx, validateFulfillmentExceptionRequest)
		validateFulfillmentExceptionReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ValidateFulfillmentException")
	}
	return &fulfillmentv1.LookupFulfillmentResponse{
		Result: &commonv1.OperationResult{
			Message: validateFulfillmentExceptionReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Fulfillment: &fulfillmentv1.Fulfillment{FulfillmentId: req.GetFulfillmentId()},
	}, nil
}

// BrowseFulfillments lists fulfillments a page at a time.
func (s FulfillmentQueryServer) BrowseFulfillments(ctx context.Context, req *fulfillmentv1.BrowseFulfillmentsRequest) (*fulfillmentv1.BrowseFulfillmentsResponse, error) {
	s.log.InfoContext(ctx, "browse fulfillments", "tenant", req.GetContext().GetTenantId())
	return &fulfillmentv1.BrowseFulfillmentsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindFulfillments lists fulfillments a page at a time.
func (s FulfillmentQueryServer) FindFulfillments(ctx context.Context, req *fulfillmentv1.FindFulfillmentsRequest) (*fulfillmentv1.FindFulfillmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "find fulfillments", "tenant", req.GetContext().GetTenantId())
	step := &fulfillmentQueryServerFindFulfillmentsValidateFulfillmentExceptionStep{request: &fulfillmentv1.ValidateFulfillmentExceptionRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Assignee: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}}
	if err := s.runFindFulfillmentsValidateFulfillmentException(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ValidateFulfillmentException")
	}
	validateFulfillmentExceptionReply := step.reply
	return &fulfillmentv1.FindFulfillmentsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: validateFulfillmentExceptionReply.GetResult().GetCorrelationId(),
			Details:       validateFulfillmentExceptionReply.GetResult().GetDetails(),
			Status:        "ok",
		},
	}, nil
}

// CountFulfillments counts fulfillments by status.
func (s FulfillmentQueryServer) CountFulfillments(ctx context.Context, req *fulfillmentv1.CountFulfillmentsRequest) (*fulfillmentv1.CountFulfillmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count fulfillments", "tenant", req.GetContext().GetTenantId())
	scoreFulfillmentExceptionRequest := &fulfillmentv1.ScoreFulfillmentExceptionRequest{}
	scoreFulfillmentExceptionRequest.Context = req.GetContext()
	scoreFulfillmentExceptionRequest.Details = &fulfillmentv1.ScoreFulfillmentExceptionRequest_Details{
		Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
	}
	scoreFulfillmentExceptionReply, err := s.fulfillmentExceptionValidationClient.ScoreFulfillmentException(ctx, scoreFulfillmentExceptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ScoreFulfillmentException")
	}
	return &fulfillmentv1.CountFulfillmentsResponse{
		Result: &commonv1.OperationResult{
			Details: scoreFulfillmentExceptionReply.GetResult().GetDetails(),
			Message: scoreFulfillmentExceptionReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ResolveFulfillments returns several fulfillments at once.
func (s FulfillmentQueryServer) ResolveFulfillments(ctx context.Context, req *fulfillmentv1.ResolveFulfillmentsRequest) (*fulfillmentv1.ResolveFulfillmentsResponse, error) {
	s.log.InfoContext(ctx, "resolve fulfillments", "tenant", req.GetContext().GetTenantId())
	validateFulfillmentExceptionReply, err := s.fulfillmentExceptionValidationClient.ValidateFulfillmentException(ctx, &fulfillmentv1.ValidateFulfillmentExceptionRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ValidateFulfillmentException")
	}
	scoreFulfillmentExceptionReply, err := s.fulfillmentExceptionValidationClient.ScoreFulfillmentException(ctx, &fulfillmentv1.ScoreFulfillmentExceptionRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Origin: &fulfillmentv1.FulfillmentException_Channel{Channel: &fulfillmentv1.FulfillmentExceptionChannelOrigin{
				Address: validateFulfillmentExceptionReply.GetFulfillmentException().GetChannel().GetAddress(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: validateFulfillmentExceptionReply.GetFulfillmentException().GetAudit().GetCreatedAt(),
				UpdatedBy: validateFulfillmentExceptionReply.GetFulfillmentException().GetAudit().GetUpdatedBy(),
			},
			Requester: &commonv1.ContactInfo{
				Name: validateFulfillmentExceptionReply.GetFulfillmentException().GetRequester().GetName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/ScoreFulfillmentException")
	}
	return &fulfillmentv1.ResolveFulfillmentsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: validateFulfillmentExceptionReply.GetResult().GetReferenceIds(),
			Status:       validateFulfillmentExceptionReply.GetResult().GetStatus(),
		},
		Fulfillments: []*fulfillmentv1.Fulfillment{&fulfillmentv1.Fulfillment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: scoreFulfillmentExceptionReply.GetFulfillmentException().GetAudit().GetCreatedAt(),
			},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: scoreFulfillmentExceptionReply.GetFulfillmentException().GetAssignee().GetDisplayName(),
			},
		}},
	}, nil
}

// ExportFulfillments streams every matching fulfillment.
func (s FulfillmentQueryServer) ExportFulfillments(req *fulfillmentv1.ExportFulfillmentsRequest, stream grpc.ServerStreamingServer[fulfillmentv1.ExportFulfillmentsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "export fulfillments", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*fulfillmentv1.ExportFulfillmentsRequest_Filter)
	cloned2 := proto.Clone(req.GetFilter()).(*fulfillmentv1.ExportFulfillmentsRequest_Filter)
	countSponsorsRequest := &sponsorshipsv1.CountSponsorsRequest{
		Context: req.GetContext(),
		Filter: &sponsorshipsv1.CountSponsorsRequest_Filter{
			Region:       cloned.GetRegion(),
			Labels:       req.GetFilter().GetLabels(),
			Query:        req.GetFilter().GetQuery(),
			CreatedAfter: cloned2.GetCreatedAfter(),
		},
	}
	countSponsorsReply, err := grpcx.NewClient(s.conn, sponsorshipsv1.NewSponsorQueryServiceClient).CountSponsors(ctx, countSponsorsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.sponsorships.v1.SponsorQueryService/CountSponsors")
	}
	lintFulfillmentExceptionsRequest := &fulfillmentv1.LintFulfillmentExceptionsRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Assignee: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		Details: &fulfillmentv1.LintFulfillmentExceptionsRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	lintFulfillmentExceptionsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*fulfillmentv1.LintFulfillmentExceptionsResponse, error) {
		return s.fulfillmentExceptionValidationClient.LintFulfillmentExceptions(ctx, lintFulfillmentExceptionsRequest)
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionValidationService/LintFulfillmentExceptions")
	}
	if err := stream.Send(&fulfillmentv1.ExportFulfillmentsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: countSponsorsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Fulfillment: &fulfillmentv1.Fulfillment{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: lintFulfillmentExceptionsReply.GetFulfillmentException().GetCustomer().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeFulfillments summarizes a fulfillment.
func (s FulfillmentQueryServer) SummarizeFulfillments(ctx context.Context, req *fulfillmentv1.SummarizeFulfillmentsRequest) (*fulfillmentv1.SummarizeFulfillmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fulfillment", req.GetFulfillmentId(), "summarize_fulfillments"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize fulfillments", "tenant", req.GetContext().GetTenantId())
	validateAccessPolicyRequest := &authorizationv1.ValidateAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Action: &authorizationv1.AccessPolicy_Block{Block: &authorizationv1.AccessPolicyBlockAction{Reason: req.GetReason()}},
			Owner:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Annotations: req.GetAnnotations(),
	}
	validateAccessPolicyReply, err := s.meteredAccessPolicyValidation.ValidateAccessPolicy(ctx, validateAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/ValidateAccessPolicy")
	}
	routes := map[string]func(context.Context, *authorizationv1.ScoreAccessPolicyRequest, ...grpc.CallOption) (*authorizationv1.ScoreAccessPolicyResponse, error){
		"primary":  s.accessPolicyValidationClient.ScoreAccessPolicy,
		"fallback": s.accessPolicyValidationClientFallback.ScoreAccessPolicy,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	scoreAccessPolicyRequest := &authorizationv1.ScoreAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Action: &authorizationv1.AccessPolicy_Discount{Discount: &authorizationv1.AccessPolicyDiscountAction{
				Percent: validateAccessPolicyReply.GetAccessPolicy().GetDiscount().GetPercent(),
			}},
		},
	}
	scoreAccessPolicyReply, err := route(ctx, scoreAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/ScoreAccessPolicy")
	}
	normalizeCarrierRateRequest := &shippingv1.NormalizeCarrierRateRequest{}
	normalizeCarrierRateRequest.Context = req.GetContext()
	normalizeCarrierRateRequest.CarrierRate = &shippingv1.CarrierRate{
		Action: &shippingv1.CarrierRate_Block{Block: &shippingv1.CarrierRateBlockAction{
			Reason: scoreAccessPolicyReply.GetAccessPolicy().GetBlock().GetReason(),
		}},
	}
	normalizeCarrierRateReply, err := s.carrierRateValidationClient.NormalizeCarrierRate(ctx, normalizeCarrierRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateValidationService/NormalizeCarrierRate")
	}
	watchMetricSeriesesStream, err := s.metricSeriesClient.WatchMetricSerieses(ctx, &metricsv1.WatchMetricSeriesesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: normalizeCarrierRateReply.GetCarrierRate().GetOwner().GetEmail(),
		},
		Filter: &metricsv1.WatchMetricSeriesesRequest_Filter{
			CreatedAfter: validateAccessPolicyReply.GetAccessPolicy().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/WatchMetricSerieses")
	}
	for {
		received, err := watchMetricSeriesesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/WatchMetricSerieses")
		}
		_ = received
	}
	return &fulfillmentv1.SummarizeFulfillmentsResponse{
		Result: &commonv1.OperationResult{
			Status:        validateAccessPolicyReply.GetOutcome().GetStatus(),
			ReferenceIds:  validateAccessPolicyReply.GetResult().GetReferenceIds(),
			Details:       scoreAccessPolicyReply.GetResult().GetDetails(),
			CorrelationId: normalizeCarrierRateReply.GetResult().GetCorrelationId(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// PeekFulfillment returns a fulfillment.
func (s FulfillmentQueryServer) PeekFulfillment(ctx context.Context, req *fulfillmentv1.PeekFulfillmentRequest) (*fulfillmentv1.PeekFulfillmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fulfillment", req.GetFulfillmentId(), "peek_fulfillment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek fulfillment", "tenant", req.GetContext().GetTenantId())
	return &fulfillmentv1.PeekFulfillmentResponse{
		Fulfillment: &fulfillmentv1.Fulfillment{FulfillmentId: req.GetFulfillmentId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// fulfillmentQueryServerFindFulfillmentsValidateFulfillmentExceptionStep carries the request of one step of
// FindFulfillments and, once it ran, its reply.
type fulfillmentQueryServerFindFulfillmentsValidateFulfillmentExceptionStep struct {
	request *fulfillmentv1.ValidateFulfillmentExceptionRequest
	reply   *fulfillmentv1.ValidateFulfillmentExceptionResponse
}

func (s FulfillmentQueryServer) runFindFulfillmentsValidateFulfillmentException(ctx context.Context, step *fulfillmentQueryServerFindFulfillmentsValidateFulfillmentExceptionStep) error {
	reply, err := s.fulfillmentExceptionValidationClient.ValidateFulfillmentException(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
