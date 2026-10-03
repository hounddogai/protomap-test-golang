package fulfillment

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fuelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fuel/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// PackingSlipQueryServer implements `acme.fulfillment.v1.PackingSlipQueryService`.
// Answers read-only questions about packing slips, for screens and reports.
type PackingSlipQueryServer struct {
	fulfillmentv1.UnimplementedPackingSlipQueryServiceServer
	store                *Store
	log                  *slog.Logger
	conn                 *grpc.ClientConn
	userValidationClient identityv1.UserValidationServiceClient
	sessionHistoryClient identityv1.SessionHistoryServiceClient
	fuelCardQueryClient  fuelv1.FuelCardQueryServiceClient
}

// NewPackingSlipQueryServer wires a PackingSlipQueryServer.
func NewPackingSlipQueryServer(deps platform.Deps) *PackingSlipQueryServer {
	return &PackingSlipQueryServer{
		store:                NewStore(deps.DB),
		log:                  deps.Logger.With("service", "acme.fulfillment.v1.PackingSlipQueryService"),
		conn:                 deps.Conn,
		userValidationClient: identityv1.NewUserValidationServiceClient(deps.Conn),
		sessionHistoryClient: identityv1.NewSessionHistoryServiceClient(deps.Conn),
		fuelCardQueryClient:  fuelv1.NewFuelCardQueryServiceClient(deps.Conn),
	}
}

// LookupPackingSlip returns a packing slip.
func (s *PackingSlipQueryServer) LookupPackingSlip(ctx context.Context, req *fulfillmentv1.LookupPackingSlipRequest) (*fulfillmentv1.LookupPackingSlipResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "packing_slip", req.GetPackingSlipId(), "lookup_packing_slip"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup packing slip", "tenant", req.GetContext().GetTenantId())
	resolveParcelsRequest := &shippingv1.ResolveParcelsRequest{Context: req.GetContext()}
	resolveParcelsReply := new(shippingv1.ResolveParcelsResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.ParcelQueryService_ResolveParcels_FullMethodName, resolveParcelsRequest, resolveParcelsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelQueryService/ResolveParcels")
	}
	return &fulfillmentv1.LookupPackingSlipResponse{
		Result: &commonv1.OperationResult{
			Status:        resolveParcelsReply.GetResult().GetStatus(),
			CorrelationId: resolveParcelsReply.GetResult().GetCorrelationId(),
		},
		PackingSlip: &fulfillmentv1.PackingSlip{PackingSlipId: req.GetPackingSlipId()},
	}, nil
}

// BrowsePackingSlips lists packing slips a page at a time.
func (s *PackingSlipQueryServer) BrowsePackingSlips(ctx context.Context, req *fulfillmentv1.BrowsePackingSlipsRequest) (*fulfillmentv1.BrowsePackingSlipsResponse, error) {
	s.log.InfoContext(ctx, "browse packing slips", "tenant", req.GetContext().GetTenantId())
	return &fulfillmentv1.BrowsePackingSlipsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindPackingSlips lists packing slips a page at a time.
func (s *PackingSlipQueryServer) FindPackingSlips(ctx context.Context, req *fulfillmentv1.FindPackingSlipsRequest) (*fulfillmentv1.FindPackingSlipsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "find packing slips", "tenant", req.GetContext().GetTenantId())
	getUserRulesRequest := &identityv1.GetUserRulesRequest{
		Context: req.GetContext(),
		UserId:  req.GetContext().GetActor().GetUserId(),
	}
	type getUserRulesResult struct {
		reply *identityv1.GetUserRulesResponse
		err   error
	}
	getUserRulesResults := make(chan getUserRulesResult, 1)
	go func() {
		reply, err := s.userValidationClient.GetUserRules(ctx, getUserRulesRequest)
		getUserRulesResults <- getUserRulesResult{reply, err}
	}()
	var getUserRulesReply *identityv1.GetUserRulesResponse
	select {
	case result := <-getUserRulesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.UserValidationService/GetUserRules")
		}
		getUserRulesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	step := &packingSlipQueryServerFindPackingSlipsCompareSessionVersionsStep{request: &identityv1.CompareSessionVersionsRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Assignee: &identityv1.UserRef{
				Email: getUserRulesReply.GetUser().GetRelationshipManager().GetEmail(),
			},
		},
		Details: &identityv1.CompareSessionVersionsRequest_Details{Snapshot: snapshot},
	}}
	if err := s.runFindPackingSlipsCompareSessionVersions(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/CompareSessionVersions")
	}
	compareSessionVersionsReply := step.reply
	return &fulfillmentv1.FindPackingSlipsResponse{
		PackingSlips: []*fulfillmentv1.PackingSlip{&fulfillmentv1.PackingSlip{
			Audit: &commonv1.AuditInfo{
				CreatedBy: getUserRulesReply.GetUser().GetAudit().GetCreatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{
			Message: compareSessionVersionsReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// CountPackingSlips counts packing slips by status.
func (s *PackingSlipQueryServer) CountPackingSlips(ctx context.Context, req *fulfillmentv1.CountPackingSlipsRequest) (*fulfillmentv1.CountPackingSlipsResponse, error) {
	s.log.InfoContext(ctx, "count packing slips", "tenant", req.GetContext().GetTenantId())
	browseFulfillmentsRequest := &fulfillmentv1.BrowseFulfillmentsRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
	}
	browseFulfillmentsReply := new(fulfillmentv1.BrowseFulfillmentsResponse)
	if err := grpc.Invoke(ctx, "/acme.fulfillment.v1.FulfillmentQueryService/BrowseFulfillments", browseFulfillmentsRequest, browseFulfillmentsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/BrowseFulfillments")
	}
	return &fulfillmentv1.CountPackingSlipsResponse{
		Result: &commonv1.OperationResult{
			Details:       browseFulfillmentsReply.GetResult().GetDetails(),
			CorrelationId: browseFulfillmentsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ResolvePackingSlips returns several packing slips at once.
func (s *PackingSlipQueryServer) ResolvePackingSlips(ctx context.Context, req *fulfillmentv1.ResolvePackingSlipsRequest) (*fulfillmentv1.ResolvePackingSlipsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResolvePackingSlips", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve packing slips", "tenant", req.GetContext().GetTenantId())
	return &fulfillmentv1.ResolvePackingSlipsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SummarizePackingSlips summarizes a packing slip.
func (s *PackingSlipQueryServer) SummarizePackingSlips(ctx context.Context, req *fulfillmentv1.SummarizePackingSlipsRequest) (*fulfillmentv1.SummarizePackingSlipsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SummarizePackingSlips", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "packing_slip", req.GetPackingSlipId(), "summarize_packing_slips"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize packing slips", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := packingSlipQueryServerSummarizeFuelCardsSteps[mode]
	cloned := proto.Clone(req.GetDetails()).(*fulfillmentv1.SummarizePackingSlipsRequest_Details)
	summarizeFuelCardsRequest := &fuelv1.SummarizeFuelCardsRequest{
		Context: req.GetContext(),
		Details: &fuelv1.SummarizeFuelCardsRequest_Details{
			Period: req.GetDetails().GetPeriod(),
			Format: cloned.GetFormat(),
		},
	}
	summarizeFuelCardsReply, err := run(s, ctx, summarizeFuelCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardQueryService/SummarizeFuelCards")
	}
	return &fulfillmentv1.SummarizePackingSlipsResponse{
		DownloadUri: summarizeFuelCardsReply.GetDownloadUri(),
		ReportId:    summarizeFuelCardsReply.GetReportId(),
		Period:      req.GetDetails().GetPeriod(),
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// packingSlipQueryServerFindPackingSlipsCompareSessionVersionsStep carries the request of one step of FindPackingSlips
// and, once it ran, its reply.
type packingSlipQueryServerFindPackingSlipsCompareSessionVersionsStep struct {
	request *identityv1.CompareSessionVersionsRequest
	reply   *identityv1.CompareSessionVersionsResponse
}

func (s *PackingSlipQueryServer) runFindPackingSlipsCompareSessionVersions(ctx context.Context, step *packingSlipQueryServerFindPackingSlipsCompareSessionVersionsStep) error {
	reply, err := s.sessionHistoryClient.CompareSessionVersions(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// packingSlipQueryServerSummarizeFuelCardsSteps are the ways to run
// acme.fuel.v1.FuelCardQueryService/SummarizeFuelCards, by name.
var packingSlipQueryServerSummarizeFuelCardsSteps = map[string]func(*PackingSlipQueryServer, context.Context, *fuelv1.SummarizeFuelCardsRequest) (*fuelv1.SummarizeFuelCardsResponse, error){
	"default": (*PackingSlipQueryServer).summarizeFuelCardsStep,
	"audited": (*PackingSlipQueryServer).summarizeFuelCardsAuditedStep,
}

func (s *PackingSlipQueryServer) summarizeFuelCardsStep(ctx context.Context, request *fuelv1.SummarizeFuelCardsRequest) (*fuelv1.SummarizeFuelCardsResponse, error) {
	return s.fuelCardQueryClient.SummarizeFuelCards(ctx, request)
}

func (s *PackingSlipQueryServer) summarizeFuelCardsAuditedStep(ctx context.Context, request *fuelv1.SummarizeFuelCardsRequest) (*fuelv1.SummarizeFuelCardsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.fuel.v1.FuelCardQueryService/SummarizeFuelCards")
	return s.fuelCardQueryClient.SummarizeFuelCards(ctx, request)
}
