package fraud

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	employeesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/employees/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RiskAssessmentQueryServer implements `acme.fraud.v1.RiskAssessmentQueryService`.
// Answers read-only questions about risk assessments, for screens and reports.
type RiskAssessmentQueryServer struct {
	fraudv1.UnimplementedRiskAssessmentQueryServiceServer
	store                            *Store
	log                              *slog.Logger
	ledgerAccountPort                ledgerAccountPort
	watchlistCalculationClient       fraudv1.WatchlistCalculationServiceClient
	subscriptionPlanValidationClient subscriptionsv1.SubscriptionPlanValidationServiceClient
	riskAssessmentCalculationClient  fraudv1.RiskAssessmentCalculationServiceClient
	employeeQueryClient              employeesv1.EmployeeQueryServiceClient
	clients                          *registry.Registry
}

// NewRiskAssessmentQueryServer wires a RiskAssessmentQueryServer.
func NewRiskAssessmentQueryServer(deps platform.Deps) *RiskAssessmentQueryServer {
	return &RiskAssessmentQueryServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.fraud.v1.RiskAssessmentQueryService"),
		ledgerAccountPort:                newLedgerAccountPort(deps),
		watchlistCalculationClient:       fraudv1.NewWatchlistCalculationServiceClient(deps.Conn),
		subscriptionPlanValidationClient: subscriptionsv1.NewSubscriptionPlanValidationServiceClient(deps.Conn),
		riskAssessmentCalculationClient:  fraudv1.NewRiskAssessmentCalculationServiceClient(deps.Conn),
		employeeQueryClient:              employeesv1.NewEmployeeQueryServiceClient(deps.Conn),
		clients:                          deps.Clients,
	}
}

// LookupRiskAssessment returns a risk assessment.
func (s *RiskAssessmentQueryServer) LookupRiskAssessment(ctx context.Context, req *fraudv1.LookupRiskAssessmentRequest) (*fraudv1.LookupRiskAssessmentResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "risk_assessment", req.GetRiskAssessmentId(), "lookup_risk_assessment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup risk assessment", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	createLedgerAccountRequest := &ledgerv1.CreateLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Contact: &commonv1.ContactInfo{Email: email},
			Funding: &ledgerv1.LedgerAccount_ExternalAccountId{ExternalAccountId: req.GetRiskAssessmentId()},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Labels:  baggage,
		},
	}
	createLedgerAccountReply, err := s.ledgerAccountPort.CreateLedgerAccount(ctx, createLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CreateLedgerAccount")
	}
	recalculateWatchlistsReply, err := s.watchlistCalculationClient.RecalculateWatchlists(ctx, &fraudv1.RecalculateWatchlistsRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: createLedgerAccountReply.GetLedgerAccount().GetAudit().GetUpdatedAt(),
						UpdatedBy: createLedgerAccountReply.GetLedgerAccount().GetAudit().GetUpdatedBy(),
					},
					Manager: &identityv1.UserRef{
						Email: createLedgerAccountReply.GetLedgerAccount().GetContact().GetEmail(),
					},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/RecalculateWatchlists")
	}
	return &fraudv1.LookupRiskAssessmentResponse{
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: createLedgerAccountReply.GetLedgerAccount().GetAudit().GetUpdatedBy(),
				Etag:      createLedgerAccountReply.GetLedgerAccount().GetAudit().GetEtag(),
			},
			Comments: []*fraudv1.RiskAssessmentComment{&fraudv1.RiskAssessmentComment{
				CreatedAt: recalculateWatchlistsReply.GetWatchlist().GetAudit().GetCreatedAt(),
			}},
			RiskAssessmentId: req.GetRiskAssessmentId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseRiskAssessments lists risk assessments a page at a time.
func (s *RiskAssessmentQueryServer) BrowseRiskAssessments(ctx context.Context, req *fraudv1.BrowseRiskAssessmentsRequest) (*fraudv1.BrowseRiskAssessmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "browse risk assessments", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.BrowseRiskAssessmentsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindRiskAssessments lists risk assessments a page at a time.
func (s *RiskAssessmentQueryServer) FindRiskAssessments(ctx context.Context, req *fraudv1.FindRiskAssessmentsRequest) (*fraudv1.FindRiskAssessmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FindRiskAssessments", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "find risk assessments", "tenant", req.GetContext().GetTenantId())
	step := &riskAssessmentQueryServerFindRiskAssessmentsPreviewSubscriptionPlanStep{request: &subscriptionsv1.PreviewSubscriptionPlanRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
	}}
	if err := s.runFindRiskAssessmentsPreviewSubscriptionPlan(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/PreviewSubscriptionPlan")
	}
	previewSubscriptionPlanReply := step.reply
	return &fraudv1.FindRiskAssessmentsResponse{
		Result: &commonv1.OperationResult{
			Status: previewSubscriptionPlanReply.GetOutcome().GetStatus(),
		},
		RiskAssessments: []*fraudv1.RiskAssessment{&fraudv1.RiskAssessment{
			Comments: []*fraudv1.RiskAssessmentComment{&fraudv1.RiskAssessmentComment{
				CreatedAt: previewSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetCreatedAt(),
			}},
		}},
	}, nil
}

// ResolveRiskAssessments returns several risk assessments at once.
func (s *RiskAssessmentQueryServer) ResolveRiskAssessments(ctx context.Context, req *fraudv1.ResolveRiskAssessmentsRequest) (*fraudv1.ResolveRiskAssessmentsResponse, error) {
	s.log.InfoContext(ctx, "resolve risk assessments", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.ResolveRiskAssessmentsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SummarizeRiskAssessments summarizes a risk assessment.
func (s *RiskAssessmentQueryServer) SummarizeRiskAssessments(ctx context.Context, req *fraudv1.SummarizeRiskAssessmentsRequest) (*fraudv1.SummarizeRiskAssessmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SummarizeRiskAssessments", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "risk_assessment", req.GetRiskAssessmentId(), "summarize_risk_assessments"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize risk assessments", "tenant", req.GetContext().GetTenantId())
	quoteRiskAssessmentRequest := &fraudv1.QuoteRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()}},
		},
	}
	quoteRiskAssessmentReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*fraudv1.QuoteRiskAssessmentResponse, error) {
		return s.riskAssessmentCalculationClient.QuoteRiskAssessment(ctx, quoteRiskAssessmentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/QuoteRiskAssessment")
	}
	return &fraudv1.SummarizeRiskAssessmentsResponse{
		Result: &commonv1.OperationResult{Status: quoteRiskAssessmentReply.GetResult().GetStatus()},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestRiskAssessments lists risk assessments a page at a time.
func (s *RiskAssessmentQueryServer) SuggestRiskAssessments(ctx context.Context, req *fraudv1.SuggestRiskAssessmentsRequest) (*fraudv1.SuggestRiskAssessmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest risk assessments", "tenant", req.GetContext().GetTenantId())
	exportEmployeesStream, err := s.employeeQueryClient.ExportEmployees(ctx, &employeesv1.ExportEmployeesRequest{
		Context: req.GetContext(),
		Filter: &employeesv1.ExportEmployeesRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			OwnerId:      req.GetFilter().GetOwnerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.employees.v1.EmployeeQueryService/ExportEmployees")
	}
	for {
		received, err := exportEmployeesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.employees.v1.EmployeeQueryService/ExportEmployees")
		}
		_ = received
	}
	reviewModerationQuery, err := registry.Lookup[reviewsv1.ReviewModerationQueryServiceClient](s.clients, "acme.reviews.v1.ReviewModerationQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cloned := proto.Clone(req.GetFilter()).(*fraudv1.SuggestRiskAssessmentsRequest_Filter)
	findReviewModerationsRequest := &reviewsv1.FindReviewModerationsRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
		Filter: &reviewsv1.FindReviewModerationsRequest_Filter{
			Query:        cloned.GetQuery(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Labels:       req.GetFilter().GetLabels(),
		},
	}
	findReviewModerationsReply, err := reviewModerationQuery.FindReviewModerations(ctx, findReviewModerationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/FindReviewModerations")
	}
	return &fraudv1.SuggestRiskAssessmentsResponse{
		Result: &commonv1.OperationResult{
			Status:  findReviewModerationsReply.GetResult().GetStatus(),
			Message: findReviewModerationsReply.GetResult().GetMessage(),
		},
	}, nil
}

// PeekRiskAssessment returns a risk assessment.
func (s *RiskAssessmentQueryServer) PeekRiskAssessment(ctx context.Context, req *fraudv1.PeekRiskAssessmentRequest) (*fraudv1.PeekRiskAssessmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "risk_assessment", req.GetRiskAssessmentId(), "peek_risk_assessment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek risk assessment", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.PeekRiskAssessmentResponse{
		RiskAssessment: &fraudv1.RiskAssessment{RiskAssessmentId: req.GetRiskAssessmentId()},
		Result:         &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// riskAssessmentQueryServerFindRiskAssessmentsPreviewSubscriptionPlanStep carries the request of one step of
// FindRiskAssessments and, once it ran, its reply.
type riskAssessmentQueryServerFindRiskAssessmentsPreviewSubscriptionPlanStep struct {
	request *subscriptionsv1.PreviewSubscriptionPlanRequest
	reply   *subscriptionsv1.PreviewSubscriptionPlanResponse
}

func (s *RiskAssessmentQueryServer) runFindRiskAssessmentsPreviewSubscriptionPlan(ctx context.Context, step *riskAssessmentQueryServerFindRiskAssessmentsPreviewSubscriptionPlanStep) error {
	reply, err := s.subscriptionPlanValidationClient.PreviewSubscriptionPlan(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
