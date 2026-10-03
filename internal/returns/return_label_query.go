package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReturnLabelQueryServer implements `acme.returns.v1.ReturnLabelQueryService`.
// Answers read-only questions about return labels, for screens and reports.
type ReturnLabelQueryServer struct {
	returnsv1.UnimplementedReturnLabelQueryServiceServer
	store                            *Store
	log                              *slog.Logger
	inspectionValidationClient       returnsv1.InspectionValidationServiceClient
	returnRequestCalculationClient   returnsv1.ReturnRequestCalculationServiceClient
	clients                          *registry.Registry
	accessPolicyClient               authorizationv1.AccessPolicyServiceClient
	restockDecisionValidationClient  returnsv1.RestockDecisionValidationServiceClient
	returnLabelCalculationClient     returnsv1.ReturnLabelCalculationServiceClient
	restockDecisionCalculationClient returnsv1.RestockDecisionCalculationServiceClient
}

// NewReturnLabelQueryServer wires a ReturnLabelQueryServer.
func NewReturnLabelQueryServer(deps platform.Deps) *ReturnLabelQueryServer {
	return &ReturnLabelQueryServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.returns.v1.ReturnLabelQueryService"),
		inspectionValidationClient:       returnsv1.NewInspectionValidationServiceClient(deps.Conn),
		returnRequestCalculationClient:   returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn),
		clients:                          deps.Clients,
		accessPolicyClient:               authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		restockDecisionValidationClient:  returnsv1.NewRestockDecisionValidationServiceClient(deps.Conn),
		returnLabelCalculationClient:     returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn),
		restockDecisionCalculationClient: returnsv1.NewRestockDecisionCalculationServiceClient(deps.Conn),
	}
}

// LookupReturnLabel returns a return label.
func (s *ReturnLabelQueryServer) LookupReturnLabel(ctx context.Context, req *returnsv1.LookupReturnLabelRequest) (*returnsv1.LookupReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "lookup_return_label"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup return label", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	step := &returnLabelQueryServerLookupReturnLabelLintInspectionsStep{request: &returnsv1.LintInspectionsRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Details: &returnsv1.LintInspectionsRequest_Details{Locale: cachedContext.GetLocale()},
	}}
	if err := s.runLookupReturnLabelLintInspections(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionValidationService/LintInspections")
	}
	lintInspectionsReply := step.reply
	explainReturnRequestCalculationReply, err := s.returnRequestCalculationClient.ExplainReturnRequestCalculation(ctx, &returnsv1.ExplainReturnRequestCalculationRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				Email:       lintInspectionsReply.GetInspection().GetAssignee().GetEmail(),
				DisplayName: lintInspectionsReply.GetInspection().GetCustomer().GetDisplayName(),
			},
		},
		Details: &returnsv1.ExplainReturnRequestCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: lintInspectionsReply.GetInspection().GetRequester().GetPhone(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/ExplainReturnRequestCalculation")
	}
	return &returnsv1.LookupReturnLabelResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{
				DisplayName: lintInspectionsReply.GetInspection().GetAssignee().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: explainReturnRequestCalculationReply.GetReturnRequest().GetBillingAddress().GetCountryCode(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
		},
		Result: &commonv1.OperationResult{
			Message: explainReturnRequestCalculationReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// BrowseReturnLabels lists return labels a page at a time.
func (s *ReturnLabelQueryServer) BrowseReturnLabels(ctx context.Context, req *returnsv1.BrowseReturnLabelsRequest) (*returnsv1.BrowseReturnLabelsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BrowseReturnLabels", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "browse return labels", "tenant", req.GetContext().GetTenantId())
	renewalScheduleValidation, err := registry.Lookup[subscriptionsv1.RenewalScheduleValidationServiceClient](s.clients, "acme.subscriptions.v1.RenewalScheduleValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	scoreRenewalScheduleRequest := &subscriptionsv1.ScoreRenewalScheduleRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			}},
			Location:  &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Organizer: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	scoreRenewalScheduleReply, err := renewalScheduleValidation.ScoreRenewalSchedule(ctx, scoreRenewalScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/ScoreRenewalSchedule")
	}
	return &returnsv1.BrowseReturnLabelsResponse{
		ReturnLabels: []*returnsv1.ReturnLabel{&returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: scoreRenewalScheduleReply.GetRenewalSchedule().GetLocation().GetPostalCode(),
				Locality:   scoreRenewalScheduleReply.GetRenewalSchedule().GetLocation().GetLocality(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// FindReturnLabels lists return labels a page at a time.
func (s *ReturnLabelQueryServer) FindReturnLabels(ctx context.Context, req *returnsv1.FindReturnLabelsRequest) (*returnsv1.FindReturnLabelsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "find return labels", "tenant", req.GetContext().GetTenantId())
	return &returnsv1.FindReturnLabelsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountReturnLabels counts return labels by status.
func (s *ReturnLabelQueryServer) CountReturnLabels(ctx context.Context, req *returnsv1.CountReturnLabelsRequest) (*returnsv1.CountReturnLabelsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountReturnLabels", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count return labels", "tenant", req.GetContext().GetTenantId())
	watchAccessPoliciesStream, err := s.accessPolicyClient.WatchAccessPolicies(ctx, &authorizationv1.WatchAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.WatchAccessPoliciesRequest_Filter{
			Labels: req.GetFilter().GetLabels(),
			Query:  req.GetFilter().GetQuery(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
	}
	for {
		received, err := watchAccessPoliciesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
		}
		_ = received
	}
	return &returnsv1.CountReturnLabelsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportReturnLabels streams every matching return label.
func (s *ReturnLabelQueryServer) ExportReturnLabels(req *returnsv1.ExportReturnLabelsRequest, stream grpc.ServerStreamingServer[returnsv1.ExportReturnLabelsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExportReturnLabels", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "export return labels", "tenant", req.GetContext().GetTenantId())
	checkRestockDecisionEligibilityRequest := buildCheckRestockDecisionEligibilityRequestForExportReturnLabels(req)
	checkRestockDecisionEligibilityReply, err := s.restockDecisionValidationClient.CheckRestockDecisionEligibility(ctx, checkRestockDecisionEligibilityRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.returns.v1.RestockDecisionValidationService/CheckRestockDecisionEligibility")
	}
	calculateReturnLabelReply, err := s.returnLabelCalculationClient.CalculateReturnLabel(ctx, &returnsv1.CalculateReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Lines: []*returnsv1.ReturnLabelLine{&returnsv1.ReturnLabelLine{
				Tax: &returnsv1.ReturnLabelLineTax{
					Amount: checkRestockDecisionEligibilityReply.GetRestockDecision().GetDiscount().GetAmount(),
				},
			}},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/CalculateReturnLabel")
	}
	if err := stream.Send(&returnsv1.ExportReturnLabelsResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{
				Priority:    checkRestockDecisionEligibilityReply.GetRestockDecision().GetPriority(),
				DisplayName: calculateReturnLabelReply.GetReturnLabel().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: checkRestockDecisionEligibilityReply.GetRestockDecision().GetAudit().GetUpdatedBy(),
			},
			Notes:          calculateReturnLabelReply.GetReturnLabel().GetNotes(),
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeReturnLabels summarizes a return label.
func (s *ReturnLabelQueryServer) SummarizeReturnLabels(ctx context.Context, req *returnsv1.SummarizeReturnLabelsRequest) (*returnsv1.SummarizeReturnLabelsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "summarize_return_labels"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize return labels", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	explainInspectionDecisionRequest := &returnsv1.ExplainInspectionDecisionRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		Annotations: annotations,
	}
	explainInspectionDecisionReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*returnsv1.ExplainInspectionDecisionResponse, error) {
		return s.inspectionValidationClient.ExplainInspectionDecision(ctx, explainInspectionDecisionRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionValidationService/ExplainInspectionDecision")
	}
	quoteReturnRequestRequest := &returnsv1.QuoteReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				Email: explainInspectionDecisionReply.GetInspection().GetCustomer().GetEmail(),
			},
		},
	}
	quoteReturnRequestReply, err := returnsv1.ReturnRequestCalculationServiceClient.QuoteReturnRequest(s.returnRequestCalculationClient, ctx, quoteReturnRequestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/QuoteReturnRequest")
	}
	calculateRestockDecisionRequest := &returnsv1.CalculateRestockDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: quoteReturnRequestReply.GetReturnRequest().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				Email: quoteReturnRequestReply.GetReturnRequest().GetCounterparty().GetEmail(),
			},
		},
		Details: &returnsv1.CalculateRestockDecisionRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality:   quoteReturnRequestReply.GetReturnRequest().GetBillingAddress().GetLocality(),
				PostalCode: quoteReturnRequestReply.GetReturnRequest().GetBillingAddress().GetPostalCode(),
			},
		},
	}
	type calculateRestockDecisionResult struct {
		reply *returnsv1.CalculateRestockDecisionResponse
		err   error
	}
	calculateRestockDecisionResults := make(chan calculateRestockDecisionResult, 1)
	go func() {
		reply, err := s.restockDecisionCalculationClient.CalculateRestockDecision(ctx, calculateRestockDecisionRequest)
		calculateRestockDecisionResults <- calculateRestockDecisionResult{reply, err}
	}()
	var calculateRestockDecisionReply *returnsv1.CalculateRestockDecisionResponse
	select {
	case result := <-calculateRestockDecisionResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.returns.v1.RestockDecisionCalculationService/CalculateRestockDecision")
		}
		calculateRestockDecisionReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	lintRestockDecisionsRequest := &returnsv1.LintRestockDecisionsRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: quoteReturnRequestReply.GetReturnRequest().GetAudit().GetUpdatedBy(),
				UpdatedAt: quoteReturnRequestReply.GetReturnRequest().GetAudit().GetUpdatedAt(),
			},
		},
	}
	lintRestockDecisionsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*returnsv1.LintRestockDecisionsResponse, error) {
		return s.restockDecisionValidationClient.LintRestockDecisions(ctx, lintRestockDecisionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionValidationService/LintRestockDecisions")
	}
	_ = calculateRestockDecisionReply
	return &returnsv1.SummarizeReturnLabelsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: explainInspectionDecisionReply.GetResult().GetCorrelationId(),
			ReferenceIds:  quoteReturnRequestReply.GetResult().GetReferenceIds(),
			Message:       lintRestockDecisionsReply.GetOutcome().GetMessage(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestReturnLabels lists return labels a page at a time.
func (s *ReturnLabelQueryServer) SuggestReturnLabels(ctx context.Context, req *returnsv1.SuggestReturnLabelsRequest) (*returnsv1.SuggestReturnLabelsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest return labels", "tenant", req.GetContext().GetTenantId())
	return &returnsv1.SuggestReturnLabelsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// PeekReturnLabel returns a return label.
func (s *ReturnLabelQueryServer) PeekReturnLabel(ctx context.Context, req *returnsv1.PeekReturnLabelRequest) (*returnsv1.PeekReturnLabelResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "peek_return_label"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek return label", "tenant", req.GetContext().GetTenantId())
	return &returnsv1.PeekReturnLabelResponse{
		ReturnLabel: &returnsv1.ReturnLabel{ReturnLabelId: req.GetReturnLabelId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// returnLabelQueryServerLookupReturnLabelLintInspectionsStep carries the request of one step of LookupReturnLabel and,
// once it ran, its reply.
type returnLabelQueryServerLookupReturnLabelLintInspectionsStep struct {
	request *returnsv1.LintInspectionsRequest
	reply   *returnsv1.LintInspectionsResponse
}

func (s *ReturnLabelQueryServer) runLookupReturnLabelLintInspections(ctx context.Context, step *returnLabelQueryServerLookupReturnLabelLintInspectionsStep) error {
	reply, err := s.inspectionValidationClient.LintInspections(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
