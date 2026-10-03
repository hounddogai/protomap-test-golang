package subscriptions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	contractsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/contracts/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	workplacesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/workplaces/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// RenewalScheduleValidationServer implements `acme.subscriptions.v1.RenewalScheduleValidationService`.
// Checks renewal schedules against the rules before anything acts on them.
type RenewalScheduleValidationServer struct {
	subscriptionsv1.UnimplementedRenewalScheduleValidationServiceServer
	store                        *Store
	log                          *slog.Logger
	reviewVoteValidationClient   reviewsv1.ReviewVoteValidationServiceClient
	usageRecordCalculationClient subscriptionsv1.UsageRecordCalculationServiceClient
	collectionClient             merchandisingv1.CollectionServiceClient
	buildingClient               workplacesv1.BuildingServiceClient
	documentQueryClient          documentsv1.DocumentQueryServiceClient
	meteredMetricSeries          meteredMetricSeries
	conn                         *grpc.ClientConn
	usageRecordCalculationPort   usageRecordCalculationPort
	contractQueryClient          contractsv1.ContractQueryServiceClient
}

// NewRenewalScheduleValidationServer wires a RenewalScheduleValidationServer.
func NewRenewalScheduleValidationServer(deps platform.Deps) *RenewalScheduleValidationServer {
	return &RenewalScheduleValidationServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.subscriptions.v1.RenewalScheduleValidationService"),
		reviewVoteValidationClient:   reviewsv1.NewReviewVoteValidationServiceClient(deps.Conn),
		usageRecordCalculationClient: subscriptionsv1.NewUsageRecordCalculationServiceClient(deps.Conn),
		collectionClient:             merchandisingv1.NewCollectionServiceClient(deps.Conn),
		buildingClient:               workplacesv1.NewBuildingServiceClient(deps.Conn),
		documentQueryClient:          documentsv1.NewDocumentQueryServiceClient(deps.Conn),
		meteredMetricSeries:          meteredMetricSeries{MetricSeriesServiceClient: metricsv1.NewMetricSeriesServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                         deps.Conn,
		usageRecordCalculationPort:   newUsageRecordCalculationPort(deps),
		contractQueryClient:          contractsv1.NewContractQueryServiceClient(deps.Conn),
	}
}

// ValidateRenewalSchedule validates a renewal schedule.
func (s *RenewalScheduleValidationServer) ValidateRenewalSchedule(ctx context.Context, req *subscriptionsv1.ValidateRenewalScheduleRequest) (*subscriptionsv1.ValidateRenewalScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "validate renewal schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	for participant := range iterx.Each(req.GetRenewalSchedule().GetParticipants(), func(candidate *subscriptionsv1.RenewalScheduleParticipant) bool { return candidate != nil }) {
		if _, err := s.reviewVoteValidationClient.CheckReviewVoteEligibility(ctx, &reviewsv1.CheckReviewVoteEligibilityRequest{
			Context: req.GetContext(),
			ReviewVote: &reviewsv1.ReviewVote{
				Subject: &customersv1.CustomerRef{Email: participant.GetContact().GetEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteValidationService/CheckReviewVoteEligibility")
		}
	}
	for _, participant2 := range req.GetRenewalSchedule().GetParticipants() {
		if _, err := s.usageRecordCalculationClient.RecalculateUsageRecords(ctx, &subscriptionsv1.RecalculateUsageRecordsRequest{
			Context: req.GetContext(),
			UsageRecord: &subscriptionsv1.UsageRecord{
				Subject: &customersv1.CustomerRef{Email: participant2.GetContact().GetEmail()},
			},
			Details: &subscriptionsv1.RecalculateUsageRecordsRequest_Details{
				Destination: &commonv1.PostalAddress{
					Recipient: req.GetRenewalSchedule().GetLocation().GetRecipient(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordCalculationService/RecalculateUsageRecords")
		}
	}
	createCollectionRequest := &merchandisingv1.CreateCollectionRequest{}
	createCollectionRequest.Context = req.GetContext()
	createCollectionReply, err := s.collectionClient.CreateCollection(ctx, createCollectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionService/CreateCollection")
	}
	return &subscriptionsv1.ValidateRenewalScheduleResponse{
		Result: &commonv1.OperationResult{Status: createCollectionReply.GetResult().GetStatus()},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: createCollectionReply.GetCollection().GetAudit().GetCreatedBy(),
			},
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Status:            req.GetRenewalSchedule().GetStatus(),
		},
	}, nil
}

// PreviewRenewalSchedule previews a renewal schedule.
func (s *RenewalScheduleValidationServer) PreviewRenewalSchedule(ctx context.Context, req *subscriptionsv1.PreviewRenewalScheduleRequest) (*subscriptionsv1.PreviewRenewalScheduleResponse, error) {
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "preview renewal schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetRenewalSchedule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	estimateUsageRecordReply, err := s.usageRecordCalculationClient.EstimateUsageRecord(ctx, &subscriptionsv1.EstimateUsageRecordRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.EstimateUsageRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetRenewalSchedule().GetLocation().GetPostalCode(),
			},
		},
		UsageRecord: &subscriptionsv1.UsageRecord{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordCalculationService/EstimateUsageRecord")
	}
	searchBuildingsRequest := &workplacesv1.SearchBuildingsRequest{
		Context: req.GetContext(),
		Filter:  &workplacesv1.SearchBuildingsRequest_Filter{Region: req.GetRenewalSchedule().GetLocation().GetRegion()},
	}
	type searchBuildingsResult struct {
		reply *workplacesv1.SearchBuildingsResponse
		err   error
	}
	searchBuildingsResults := make(chan searchBuildingsResult, 1)
	go func() {
		reply, err := s.buildingClient.SearchBuildings(ctx, searchBuildingsRequest)
		searchBuildingsResults <- searchBuildingsResult{reply, err}
	}()
	var searchBuildingsReply *workplacesv1.SearchBuildingsResponse
	select {
	case result := <-searchBuildingsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.workplaces.v1.BuildingService/SearchBuildings")
		}
		searchBuildingsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &subscriptionsv1.PreviewRenewalScheduleResponse{
		Outcome: &subscriptionsv1.PreviewRenewalScheduleResponse_Outcome{
			Message: estimateUsageRecordReply.GetResult().GetMessage(),
			Status:  searchBuildingsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: estimateUsageRecordReply.GetResult().GetStatus()},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Window:            req.GetRenewalSchedule().GetWindow(),
		},
	}, nil
}

// CheckRenewalScheduleEligibility checks a renewal schedule.
func (s *RenewalScheduleValidationServer) CheckRenewalScheduleEligibility(ctx context.Context, req *subscriptionsv1.CheckRenewalScheduleEligibilityRequest) (*subscriptionsv1.CheckRenewalScheduleEligibilityResponse, error) {
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "check renewal schedule eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	jobs := make(chan *subscriptionsv1.RenewalScheduleParticipant)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for participant := range jobs {
				if _, err := s.documentQueryClient.FindDocuments(workCtx, &documentsv1.FindDocumentsRequest{
					Context: req.GetContext(),
					Filter: &documentsv1.FindDocumentsRequest_Filter{
						Region:  req.GetRenewalSchedule().GetLocation().GetRegion(),
						OwnerId: participant.GetParticipantId(),
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetRenewalSchedule().GetParticipants() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/FindDocuments")
	}
	return &subscriptionsv1.CheckRenewalScheduleEligibilityResponse{
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Organizer:         req.GetRenewalSchedule().GetOrganizer(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeRenewalSchedule normalizes a renewal schedule.
func (s *RenewalScheduleValidationServer) NormalizeRenewalSchedule(ctx context.Context, req *subscriptionsv1.NormalizeRenewalScheduleRequest) (*subscriptionsv1.NormalizeRenewalScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizeRenewalSchedule", "error", err)
		return nil, err
	}
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "normalize renewal schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	listMetricSeriesesRequest := &metricsv1.ListMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter:  &metricsv1.ListMetricSeriesesRequest_Filter{Region: req.GetRenewalSchedule().GetLocation().GetRegion()},
	}
	listMetricSeriesesReply, err := s.meteredMetricSeries.ListMetricSerieses(ctx, listMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/ListMetricSerieses")
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetContact().GetPreferredChannel())
	packed, err := anypb.New(req.GetRenewalSchedule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewReviewVoteRequest := &reviewsv1.PreviewReviewVoteRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.PreviewReviewVoteRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetContact().GetPhone(),
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
				Email:            req.GetRenewalSchedule().GetOrganizer().GetEmail(),
			},
		},
		ReviewVote: &reviewsv1.ReviewVote{Detail: packed},
	}
	previewReviewVoteReply, err := grpcx.NewClient(s.conn, reviewsv1.NewReviewVoteValidationServiceClient).PreviewReviewVote(ctx, previewReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteValidationService/PreviewReviewVote")
	}
	return &subscriptionsv1.NormalizeRenewalScheduleResponse{
		Result: &commonv1.OperationResult{
			Details:       listMetricSeriesesReply.GetResult().GetDetails(),
			CorrelationId: previewReviewVoteReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Title:             req.GetRenewalSchedule().GetTitle(),
		},
	}, nil
}

// ExplainRenewalScheduleDecision explains a renewal schedule.
func (s *RenewalScheduleValidationServer) ExplainRenewalScheduleDecision(ctx context.Context, req *subscriptionsv1.ExplainRenewalScheduleDecisionRequest) (*subscriptionsv1.ExplainRenewalScheduleDecisionResponse, error) {
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "explain renewal schedule decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	estimateUsageRecordRequest := &subscriptionsv1.EstimateUsageRecordRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.EstimateUsageRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetRenewalSchedule().GetLocation().GetCountryCode(),
				Region:      req.GetRenewalSchedule().GetLocation().GetRegion(),
			},
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetRenewalSchedule().GetOrganizer().GetDisplayName(),
				Email:       req.GetRenewalSchedule().GetOrganizer().GetEmail(),
			},
		},
	}
	estimateUsageRecordReply, err := s.usageRecordCalculationPort.EstimateUsageRecord(ctx, estimateUsageRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordCalculationService/EstimateUsageRecord")
	}
	return &subscriptionsv1.ExplainRenewalScheduleDecisionResponse{
		Outcome: &subscriptionsv1.ExplainRenewalScheduleDecisionResponse_Outcome{Status: estimateUsageRecordReply.GetResult().GetStatus()},
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Status:            req.GetRenewalSchedule().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreRenewalSchedule scores a renewal schedule.
func (s *RenewalScheduleValidationServer) ScoreRenewalSchedule(ctx context.Context, req *subscriptionsv1.ScoreRenewalScheduleRequest) (*subscriptionsv1.ScoreRenewalScheduleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRenewalSchedule(ctx, req.GetContext().GetTenantId(), req.GetRenewalSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save renewal schedule")
	}
	s.log.InfoContext(ctx, "score renewal schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRenewalSchedule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*subscriptionsv1.ScoreRenewalScheduleRequest_Details)
	exportContractsStream, err := s.contractQueryClient.ExportContracts(ctx, &contractsv1.ExportContractsRequest{
		Context: req.GetContext(),
		Filter: &contractsv1.ExportContractsRequest_Filter{
			Region:       cloned.GetDestination().GetRegion(),
			CreatedAfter: req.GetRenewalSchedule().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.contracts.v1.ContractQueryService/ExportContracts")
	}
	for {
		received, err := exportContractsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.contracts.v1.ContractQueryService/ExportContracts")
		}
		_ = received
	}
	return &subscriptionsv1.ScoreRenewalScheduleResponse{
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			RenewalScheduleId: req.GetRenewalScheduleId(),
			Metadata:          req.GetRenewalSchedule().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetRenewalScheduleRules returns a renewal schedule.
func (s *RenewalScheduleValidationServer) GetRenewalScheduleRules(ctx context.Context, req *subscriptionsv1.GetRenewalScheduleRulesRequest) (*subscriptionsv1.GetRenewalScheduleRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetRenewalScheduleRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "renewal_schedule", req.GetRenewalScheduleId(), "get_renewal_schedule_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get renewal schedule rules", "tenant", req.GetContext().GetTenantId())
	return &subscriptionsv1.GetRenewalScheduleRulesResponse{
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{RenewalScheduleId: req.GetRenewalScheduleId()},
		Result:          &commonv1.OperationResult{Status: "ok"},
	}, nil
}
