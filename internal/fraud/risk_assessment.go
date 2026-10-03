package fraud

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RiskAssessmentServer implements `acme.fraud.v1.RiskAssessmentService`.
// Manages risk assessments: creates, reads, changes, and lists them.
type RiskAssessmentServer struct {
	fraudv1.UnimplementedRiskAssessmentServiceServer
	store                             *Store
	log                               *slog.Logger
	bus                               *eventbus.Bus
	conn                              *grpc.ClientConn
	merchandisingRuleValidationClient merchandisingv1.MerchandisingRuleValidationServiceClient
	watchlistValidationClient         fraudv1.WatchlistValidationServiceClient
	riskAssessmentValidationClient    fraudv1.RiskAssessmentValidationServiceClient
	roleHistoryClient                 identityv1.RoleHistoryServiceClient
}

// NewRiskAssessmentServer wires a RiskAssessmentServer.
func NewRiskAssessmentServer(deps platform.Deps) *RiskAssessmentServer {
	return &RiskAssessmentServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.fraud.v1.RiskAssessmentService"),
		bus:                               deps.Bus,
		conn:                              deps.Conn,
		merchandisingRuleValidationClient: merchandisingv1.NewMerchandisingRuleValidationServiceClient(deps.Conn),
		watchlistValidationClient:         fraudv1.NewWatchlistValidationServiceClient(deps.Conn),
		riskAssessmentValidationClient:    fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn),
		roleHistoryClient:                 identityv1.NewRoleHistoryServiceClient(deps.Conn),
	}
}

// CreateRiskAssessment creates a risk assessment.
func (s *RiskAssessmentServer) CreateRiskAssessment(ctx context.Context, req *fraudv1.CreateRiskAssessmentRequest) (*fraudv1.CreateRiskAssessmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "create risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "fraud.create_risk_assessment.score_watchlist",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"watchlist.children.children.manager.email": req.GetRiskAssessment().GetCustomer().GetEmail(),
			"watchlist.children.manager.display_name":   req.GetRiskAssessment().GetCustomer().GetDisplayName(),
			"context.tenant_id":                         req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &fraudv1.CreateRiskAssessmentResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Status:           req.GetRiskAssessment().GetStatus(),
			RiskAssessmentId: req.GetRiskAssessment().GetRiskAssessmentId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetRiskAssessment returns a risk assessment.
func (s *RiskAssessmentServer) GetRiskAssessment(ctx context.Context, req *fraudv1.GetRiskAssessmentRequest) (*fraudv1.GetRiskAssessmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "risk_assessment", req.GetRiskAssessmentId(), "get_risk_assessment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get risk assessment", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	var cachedSubject string
	if entry, ok := recentRiskAssessments.Load(recordKey{req.GetContext().GetTenantId(), req.GetRiskAssessmentId()}); ok {
		if previous, ok := entry.(*fraudv1.RiskAssessment); ok {
			cachedSubject = previous.GetSubject()
		}
	}
	explainWatchlistCalculationRequest := &fraudv1.ExplainWatchlistCalculationRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Manager: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
			}},
			Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Reason: cachedSubject,
	}
	explainWatchlistCalculationReply := new(fraudv1.ExplainWatchlistCalculationResponse)
	if err := s.conn.Invoke(ctx, fraudWatchlistCalculationServiceExplainWatchlistCalculationMethod, explainWatchlistCalculationRequest, explainWatchlistCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/ExplainWatchlistCalculation")
	}
	return &fraudv1.GetRiskAssessmentResponse{
		Result: &commonv1.OperationResult{
			Status: explainWatchlistCalculationReply.GetOutcome().GetStatus(),
		},
		RiskAssessment: &fraudv1.RiskAssessment{RiskAssessmentId: req.GetRiskAssessmentId()},
	}, nil
}

// UpdateRiskAssessment changes a risk assessment.
func (s *RiskAssessmentServer) UpdateRiskAssessment(ctx context.Context, req *fraudv1.UpdateRiskAssessmentRequest) (*fraudv1.UpdateRiskAssessmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "update risk assessment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetRiskAssessment().GetCustomer().GetEmail())
	source := req.GetRiskAssessment().GetAudit().ProtoReflect()
	etag := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("etag"))).String()
	normalizeMerchandisingRuleReply, err := s.merchandisingRuleValidationClient.NormalizeMerchandisingRule(ctx, &merchandisingv1.NormalizeMerchandisingRuleRequest{
		Context: req.GetContext(),
		Details: &merchandisingv1.NormalizeMerchandisingRuleRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetRiskAssessment().GetRequester().GetPhone()},
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				Email:       tenancy.Value(ctx, "email"),
				DisplayName: req.GetRiskAssessment().GetCustomer().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{Etag: etag},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/NormalizeMerchandisingRule")
	}
	cloned := proto.Clone(req.GetRiskAssessment()).(*fraudv1.RiskAssessment)
	ctx = tenancy.With(ctx, "email", req.GetRiskAssessment().GetCustomer().GetEmail())
	step := &riskAssessmentServerUpdateRiskAssessmentScoreWatchlistStep{request: &fraudv1.ScoreWatchlistRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						CreatedBy: cloned.GetAudit().GetCreatedBy(),
						UpdatedBy: req.GetRiskAssessment().GetAudit().GetUpdatedBy(),
					},
					Manager: &identityv1.UserRef{
						DisplayName: req.GetRiskAssessment().GetAssignee().GetDisplayName(),
						Email:       tenancy.Value(ctx, "email"),
					},
				}},
			}},
		},
	}}
	if err := s.runUpdateRiskAssessmentScoreWatchlist(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistValidationService/ScoreWatchlist")
	}
	scoreWatchlistReply := step.reply
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "fraud.update_risk_assessment.validate_watchlist",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"watchlist.children.children.manager.user_id":  req.GetRiskAssessment().GetAssignee().GetUserId(),
			"watchlist.children.children.audit.updated_by": req.GetRiskAssessment().GetAudit().GetUpdatedBy(),
			"context.tenant_id":                            req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	recentRiskAssessments.Store(recordKey{req.GetContext().GetTenantId(), req.GetRiskAssessment().GetRiskAssessmentId()}, req.GetRiskAssessment())
	return &fraudv1.UpdateRiskAssessmentResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizeMerchandisingRuleReply.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
			},
			Requester: &commonv1.ContactInfo{
				Email: scoreWatchlistReply.GetWatchlist().GetManager().GetEmail(),
			},
			Status:           req.GetRiskAssessment().GetStatus(),
			RiskAssessmentId: req.GetRiskAssessment().GetRiskAssessmentId(),
		},
		Result: &commonv1.OperationResult{Status: scoreWatchlistReply.GetResult().GetStatus()},
	}, nil
}

// BatchGetRiskAssessments returns several risk assessments at once.
func (s *RiskAssessmentServer) BatchGetRiskAssessments(ctx context.Context, req *fraudv1.BatchGetRiskAssessmentsRequest) (*fraudv1.BatchGetRiskAssessmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "batch get risk assessments", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.BatchGetRiskAssessmentsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SearchRiskAssessments lists risk assessments a page at a time.
func (s *RiskAssessmentServer) SearchRiskAssessments(ctx context.Context, req *fraudv1.SearchRiskAssessmentsRequest) (*fraudv1.SearchRiskAssessmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "search risk assessments", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetContext().GetActor(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	validateRiskAssessmentRequest := &fraudv1.ValidateRiskAssessmentRequest{}
	validateRiskAssessmentRequest.Context = req.GetContext()
	validateRiskAssessmentRequest.RiskAssessment = &fraudv1.RiskAssessment{
		Assignee: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		Comments: []*fraudv1.RiskAssessmentComment{&fraudv1.RiskAssessmentComment{CreatedAt: req.GetFilter().GetCreatedAfter()}},
	}
	validateRiskAssessmentRequest.Details = &fraudv1.ValidateRiskAssessmentRequest_Details{
		Contact: &commonv1.ContactInfo{Email: holder.Email},
		Locale:  req.GetContext().GetLocale(),
	}
	validateRiskAssessmentReply, err := s.riskAssessmentValidationClient.ValidateRiskAssessment(ctx, validateRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/ValidateRiskAssessment")
	}
	return &fraudv1.SearchRiskAssessmentsResponse{
		RiskAssessments: []*fraudv1.RiskAssessment{&fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: validateRiskAssessmentReply.GetRiskAssessment().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BatchCreateRiskAssessments takes a stream of risk assessments and answers once at the end.
func (s *RiskAssessmentServer) BatchCreateRiskAssessments(stream grpc.ClientStreamingServer[fraudv1.BatchCreateRiskAssessmentsRequest, fraudv1.BatchCreateRiskAssessmentsResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "batch create risk assessments", "tenant", msg.GetContext().GetTenantId())
		validateRiskAssessmentRequest := &fraudv1.ValidateRiskAssessmentRequest{
			Context: msg.GetContext(),
			RiskAssessment: &fraudv1.RiskAssessment{
				Assignee: &identityv1.UserRef{DisplayName: msg.GetRecord().GetCustomer().GetDisplayName()},
			},
			Details: &fraudv1.ValidateRiskAssessmentRequest_Details{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: msg.GetHeader().GetSubmittedBy().GetPreferredChannel(),
					Email:            msg.GetRecord().GetRequester().GetEmail(),
					Phone:            msg.GetRecord().GetRequester().GetPhone(),
				},
			},
		}
		if _, err := grpcx.Call(ctx, s.riskAssessmentValidationClient.ValidateRiskAssessment, validateRiskAssessmentRequest); err != nil {
			return grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/ValidateRiskAssessment")
		}
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(8)
		replies := make([]*identityv1.ListRoleVersionsResponse, len(msg.GetRecord().GetHistory()))
		for i, item := range msg.GetRecord().GetHistory() {
			group.Go(func() error {
				reply, err := s.roleHistoryClient.ListRoleVersions(groupCtx, &identityv1.ListRoleVersionsRequest{
					Context: msg.GetContext(),
					Filter:  &identityv1.ListRoleVersionsRequest_Filter{CreatedAfter: item.GetChangedAt()},
				})
				if err != nil {
					return err
				}
				replies[i] = reply
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			return grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/ListRoleVersions")
		}
		s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
		accepted++
	}
	return stream.SendAndClose(&fraudv1.BatchCreateRiskAssessmentsResponse{Accepted: accepted})
}

// riskAssessmentServerUpdateRiskAssessmentScoreWatchlistStep carries the request of one step of UpdateRiskAssessment
// and, once it ran, its reply.
type riskAssessmentServerUpdateRiskAssessmentScoreWatchlistStep struct {
	request *fraudv1.ScoreWatchlistRequest
	reply   *fraudv1.ScoreWatchlistResponse
}

func (s *RiskAssessmentServer) runUpdateRiskAssessmentScoreWatchlist(ctx context.Context, step *riskAssessmentServerUpdateRiskAssessmentScoreWatchlistStep) error {
	reply, err := s.watchlistValidationClient.ScoreWatchlist(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
