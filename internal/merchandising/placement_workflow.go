package merchandising

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	expensesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/expenses/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	learningv1 "github.com/hounddogai/protomap-test-golang/gen/acme/learning/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	surveysv1 "github.com/hounddogai/protomap-test-golang/gen/acme/surveys/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// PlacementWorkflowServer implements `acme.merchandising.v1.PlacementWorkflowService`.
// Runs the long-lived workflow that takes a placement from start to finish.
type PlacementWorkflowServer struct {
	merchandisingv1.UnsafePlacementWorkflowServiceServer
	store                              *Store
	log                                *slog.Logger
	searchIndexClient                  searchv1.SearchIndexServiceClient
	placementQueryClient               merchandisingv1.PlacementQueryServiceClient
	expenseReportQueryClient           expensesv1.ExpenseReportQueryServiceClient
	sellerListingWorkflowClient        sellersv1.SellerListingWorkflowServiceClient
	userHistoryPort                    userHistoryPort
	metricSeriesClient                 metricsv1.MetricSeriesServiceClient
	collectionApprovalClient           merchandisingv1.CollectionApprovalServiceClient
	merchandisingRuleApprovalClient    merchandisingv1.MerchandisingRuleApprovalServiceClient
	apiCredentialValidationClient      identityv1.ApiCredentialValidationServiceClient
	cloneCollectionFn                  func(context.Context, *merchandisingv1.CloneCollectionRequest, ...grpc.CallOption) (*merchandisingv1.CloneCollectionResponse, error)
	sellerWorkflowClient               sellersv1.SellerWorkflowServiceClient
	courseQueryPort                    courseQueryPort
	sellerPayoutScheduleApprovalClient sellersv1.SellerPayoutScheduleApprovalServiceClient
	conn                               *grpc.ClientConn
	collectionClient                   merchandisingv1.CollectionServiceClient
	collectionWorkflowClient           merchandisingv1.CollectionWorkflowServiceClient
	searchQueryExportClient            searchv1.SearchQueryExportServiceClient
	sellerScorecardApprovalClient      sellersv1.SellerScorecardApprovalServiceClient
	surveyQueryClient                  surveysv1.SurveyQueryServiceClient
	notificationValidationClient       notificationsv1.NotificationValidationServiceClient
}

// NewPlacementWorkflowServer wires a PlacementWorkflowServer.
func NewPlacementWorkflowServer(deps platform.Deps) *PlacementWorkflowServer {
	collection := merchandisingv1.NewCollectionServiceClient(deps.Conn)
	return &PlacementWorkflowServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.merchandising.v1.PlacementWorkflowService"),
		searchIndexClient:                  searchv1.NewSearchIndexServiceClient(deps.Conn),
		placementQueryClient:               merchandisingv1.NewPlacementQueryServiceClient(deps.Conn),
		expenseReportQueryClient:           expensesv1.NewExpenseReportQueryServiceClient(deps.Conn),
		sellerListingWorkflowClient:        sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn),
		userHistoryPort:                    newUserHistoryPort(deps),
		metricSeriesClient:                 metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		collectionApprovalClient:           merchandisingv1.NewCollectionApprovalServiceClient(deps.Conn),
		merchandisingRuleApprovalClient:    merchandisingv1.NewMerchandisingRuleApprovalServiceClient(deps.Conn),
		apiCredentialValidationClient:      identityv1.NewApiCredentialValidationServiceClient(deps.Conn),
		cloneCollectionFn:                  collection.CloneCollection,
		sellerWorkflowClient:               sellersv1.NewSellerWorkflowServiceClient(deps.Conn),
		courseQueryPort:                    newCourseQueryPort(deps),
		sellerPayoutScheduleApprovalClient: sellersv1.NewSellerPayoutScheduleApprovalServiceClient(deps.Conn),
		conn:                               deps.Conn,
		collectionClient:                   merchandisingv1.NewCollectionServiceClient(deps.Conn),
		collectionWorkflowClient:           merchandisingv1.NewCollectionWorkflowServiceClient(deps.Conn),
		searchQueryExportClient:            searchv1.NewSearchQueryExportServiceClient(deps.Conn),
		sellerScorecardApprovalClient:      sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		surveyQueryClient:                  surveysv1.NewSurveyQueryServiceClient(deps.Conn),
		notificationValidationClient:       notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
	}
}

// StartPlacementWorkflow starts a placement.
func (s *PlacementWorkflowServer) StartPlacementWorkflow(ctx context.Context, req *merchandisingv1.StartPlacementWorkflowRequest) (*merchandisingv1.StartPlacementWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "start placement workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	batchCreateSearchIndexesStream, err := s.searchIndexClient.BatchCreateSearchIndexes(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/BatchCreateSearchIndexes")
	}
	if err := batchCreateSearchIndexesStream.Send(&searchv1.BatchCreateSearchIndexesRequest{
		Context: req.GetContext(),
		Chunk: &searchv1.BatchCreateSearchIndexesRequest_Record{Record: &searchv1.SearchIndex{
			Audit:   &commonv1.AuditInfo{CreatedBy: req.GetPlacement().GetAudit().GetCreatedBy()},
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{Lines: req.GetPlacement().GetLocation().GetLines()}},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/BatchCreateSearchIndexes")
	}
	if _, err := batchCreateSearchIndexesStream.CloseAndRecv(); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/BatchCreateSearchIndexes")
	}
	summarizePlacementsReply, err := s.placementQueryClient.SummarizePlacements(ctx, &merchandisingv1.SummarizePlacementsRequest{
		Context:     req.GetContext(),
		PlacementId: req.GetPlacementId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/SummarizePlacements")
	}
	exportExpenseReportsStream, err := s.expenseReportQueryClient.ExportExpenseReports(ctx, &expensesv1.ExportExpenseReportsRequest{
		Context: req.GetContext(),
		Filter:  &expensesv1.ExportExpenseReportsRequest_Filter{Region: req.GetPlacement().GetLocation().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.expenses.v1.ExpenseReportQueryService/ExportExpenseReports")
	}
	for {
		received, err := exportExpenseReportsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.expenses.v1.ExpenseReportQueryService/ExportExpenseReports")
		}
		_ = received
	}
	return &merchandisingv1.StartPlacementWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizePlacementsReply.GetResult().GetReferenceIds(),
			Status:       summarizePlacementsReply.GetResult().GetStatus(),
		},
		Placement: &merchandisingv1.Placement{
			PlacementId: req.GetPlacementId(),
			Window:      req.GetPlacement().GetWindow(),
		},
	}, nil
}

// AdvancePlacementWorkflow advances a placement.
func (s *PlacementWorkflowServer) AdvancePlacementWorkflow(ctx context.Context, req *merchandisingv1.AdvancePlacementWorkflowRequest) (*merchandisingv1.AdvancePlacementWorkflowResponse, error) {
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "advance placement workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	resumeSellerListingWorkflowRequest := &sellersv1.ResumeSellerListingWorkflowRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				CountryCode: req.GetPlacement().GetLocation().GetCountryCode(),
			}},
		},
	}
	send := s.resumeSellerListingWorkflowSender(resumeSellerListingWorkflowRequest)
	resumeSellerListingWorkflowReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/ResumeSellerListingWorkflow")
	}
	getUserVersionRequest := &identityv1.GetUserVersionRequest{
		Context: req.GetContext(),
		UserId:  req.GetPlacement().GetOrganizer().GetUserId(),
	}
	getUserVersionReply, err := s.userHistoryPort.GetUserVersion(ctx, getUserVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/GetUserVersion")
	}
	watchMetricSeriesesStream, err := s.metricSeriesClient.WatchMetricSerieses(ctx, &metricsv1.WatchMetricSeriesesRequest{Context: req.GetContext()})
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
	return &merchandisingv1.AdvancePlacementWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: resumeSellerListingWorkflowReply.GetOutcome().GetMessage(),
			Status:  getUserVersionReply.GetResult().GetStatus(),
		},
		Outcome: &merchandisingv1.AdvancePlacementWorkflowResponse_Outcome{Status: getUserVersionReply.GetResult().GetStatus()},
		Placement: &merchandisingv1.Placement{
			PlacementId: req.GetPlacementId(),
			Status:      req.GetPlacement().GetStatus(),
		},
	}, nil
}

// CancelPlacementWorkflow cancels a placement.
func (s *PlacementWorkflowServer) CancelPlacementWorkflow(ctx context.Context, req *merchandisingv1.CancelPlacementWorkflowRequest) (*merchandisingv1.CancelPlacementWorkflowResponse, error) {
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "cancel placement workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*merchandisingv1.CommentCollectionApprovalResponse, len(req.GetPlacement().GetParticipants()))
	for i, participant := range req.GetPlacement().GetParticipants() {
		group.Go(func() error {
			reply, err := s.collectionApprovalClient.CommentCollectionApproval(groupCtx, &merchandisingv1.CommentCollectionApprovalRequest{
				Context: req.GetContext(),
				Details: &merchandisingv1.CommentCollectionApprovalRequest_Details{
					Approver: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
				},
				Collection: &merchandisingv1.Collection{
					Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
						Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
							Manager: &identityv1.UserRef{Email: req.GetPlacement().GetOrganizer().GetEmail()},
							Audit:   &commonv1.AuditInfo{Etag: req.GetPlacement().GetAudit().GetEtag()},
						}},
					}},
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionApprovalService/CommentCollectionApproval")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	approveMerchandisingRuleReply, err := s.merchandisingRuleApprovalClient.ApproveMerchandisingRule(ctx, &merchandisingv1.ApproveMerchandisingRuleRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleApprovalService/ApproveMerchandisingRule")
	}
	for participant2 := range iterx.Each(req.GetPlacement().GetParticipants(), func(candidate *merchandisingv1.PlacementParticipant) bool { return candidate != nil }) {
		if _, err := s.apiCredentialValidationClient.LintApiCredentials(ctx, &identityv1.LintApiCredentialsRequest{
			Context: req.GetContext(),
			ApiCredential: &identityv1.ApiCredential{
				Contact: &commonv1.ContactInfo{
					Name:             participant2.GetContact().GetName(),
					Email:            participant2.GetContact().GetEmail(),
					PreferredChannel: participant2.GetContact().GetPreferredChannel(),
				},
			},
			Details: &identityv1.LintApiCredentialsRequest_Details{
				Contact: &commonv1.ContactInfo{Phone: participant2.GetContact().GetPhone()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/LintApiCredentials")
		}
	}
	return &merchandisingv1.CancelPlacementWorkflowResponse{
		Outcome: &merchandisingv1.CancelPlacementWorkflowResponse_Outcome{
			EffectiveAt: approveMerchandisingRuleReply.GetOutcome().GetEffectiveAt(),
		},
		Placement: &merchandisingv1.Placement{
			Audit: &commonv1.AuditInfo{
				CreatedBy: approveMerchandisingRuleReply.GetMerchandisingRule().GetAudit().GetCreatedBy(),
			},
			PlacementId: req.GetPlacementId(),
			Window:      req.GetPlacement().GetWindow(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryPlacementWorkflow retrys a placement.
func (s *PlacementWorkflowServer) RetryPlacementWorkflow(ctx context.Context, req *merchandisingv1.RetryPlacementWorkflowRequest) (*merchandisingv1.RetryPlacementWorkflowResponse, error) {
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "retry placement workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	cloneCollectionRequest := &merchandisingv1.CloneCollectionRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Location: &commonv1.PostalAddress{
						CountryCode: req.GetPlacement().GetLocation().GetCountryCode(),
					},
					Manager: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
				}},
			}},
		},
	}
	cloneCollectionReply, err := s.cloneCollectionFn(ctx, cloneCollectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionService/CloneCollection")
	}
	signalSellerWorkflowRequest := &sellersv1.SignalSellerWorkflowRequest{}
	signalSellerWorkflowRequest.Context = req.GetContext()
	signalSellerWorkflowRequest.Seller = &sellersv1.Seller{
		PrimaryAddress: &commonv1.PostalAddress{
			CountryCode: cloneCollectionReply.GetCollection().GetLocation().GetCountryCode(),
			PostalCode:  req.GetPlacement().GetLocation().GetPostalCode(),
		},
		Audit: &commonv1.AuditInfo{CreatedAt: req.GetPlacement().GetAudit().GetCreatedAt()},
	}
	signalSellerWorkflowRequest.Details = &sellersv1.SignalSellerWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{
			Email: cloneCollectionReply.GetCollection().GetManager().GetEmail(),
		},
	}
	signalSellerWorkflowReply, err := s.sellerWorkflowClient.SignalSellerWorkflow(ctx, signalSellerWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerWorkflowService/SignalSellerWorkflow")
	}
	findCoursesRequest := &learningv1.FindCoursesRequest{
		Context: req.GetContext(),
		Filter: &learningv1.FindCoursesRequest_Filter{
			Region:       signalSellerWorkflowReply.GetSeller().GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetPlacement().GetAudit().GetUpdatedAt(),
			Labels:       signalSellerWorkflowReply.GetSeller().GetAttributes(),
		},
	}
	findCoursesReply, err := s.courseQueryPort.FindCourses(ctx, findCoursesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.learning.v1.CourseQueryService/FindCourses")
	}
	submitCollectionRequest := &merchandisingv1.SubmitCollectionRequest{}
	submitCollectionRequest.Context = req.GetContext()
	submitCollectionRequest.Collection = &merchandisingv1.Collection{
		Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Audit: &commonv1.AuditInfo{
					UpdatedAt: cloneCollectionReply.GetCollection().GetAudit().GetUpdatedAt(),
				},
				Manager: &identityv1.UserRef{
					Email: cloneCollectionReply.GetCollection().GetManager().GetEmail(),
				},
				Location: &commonv1.PostalAddress{
					Locality: cloneCollectionReply.GetCollection().GetLocation().GetLocality(),
				},
			}},
		}},
	}
	submitCollectionReply, err := s.collectionApprovalClient.SubmitCollection(ctx, submitCollectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionApprovalService/SubmitCollection")
	}
	return &merchandisingv1.RetryPlacementWorkflowResponse{
		Placement: &merchandisingv1.Placement{
			Location: &commonv1.PostalAddress{
				PostalCode: cloneCollectionReply.GetCollection().GetLocation().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: cloneCollectionReply.GetCollection().GetAudit().GetCreatedBy(),
			},
			Organizer: &identityv1.UserRef{
				Email: submitCollectionReply.GetCollection().GetManager().GetEmail(),
			},
			PlacementId: req.GetPlacementId(),
			Title:       req.GetPlacement().GetTitle(),
		},
		Result: &commonv1.OperationResult{
			Status:  signalSellerWorkflowReply.GetOutcome().GetStatus(),
			Details: findCoursesReply.GetResult().GetDetails(),
		},
		Outcome: &merchandisingv1.RetryPlacementWorkflowResponse_Outcome{
			Message:     findCoursesReply.GetResult().GetMessage(),
			EffectiveAt: submitCollectionReply.GetOutcome().GetEffectiveAt(),
		},
	}, nil
}

// PausePlacementWorkflow pauses a placement.
func (s *PlacementWorkflowServer) PausePlacementWorkflow(ctx context.Context, req *merchandisingv1.PausePlacementWorkflowRequest) (*merchandisingv1.PausePlacementWorkflowResponse, error) {
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "pause placement workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*merchandisingv1.PausePlacementWorkflowRequest_Details)
	submitSellerPayoutScheduleReply, err := s.sellerPayoutScheduleApprovalClient.SubmitSellerPayoutSchedule(ctx, &sellersv1.SubmitSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Phone: cloned.GetNotify().GetPhone()},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleApprovalService/SubmitSellerPayoutSchedule")
	}
	resolveCollectionsRequest := &merchandisingv1.ResolveCollectionsRequest{Context: req.GetContext()}
	resolveCollectionsReply := new(merchandisingv1.ResolveCollectionsResponse)
	if err := grpc.Invoke(ctx, "/acme.merchandising.v1.CollectionQueryService/ResolveCollections", resolveCollectionsRequest, resolveCollectionsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionQueryService/ResolveCollections")
	}
	return &merchandisingv1.PausePlacementWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: submitSellerPayoutScheduleReply.GetResult().GetCorrelationId(),
			Message:       resolveCollectionsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Placement: &merchandisingv1.Placement{
			PlacementId: req.GetPlacementId(),
			Status:      req.GetPlacement().GetStatus(),
		},
	}, nil
}

// ResumePlacementWorkflow resumes a placement.
func (s *PlacementWorkflowServer) ResumePlacementWorkflow(ctx context.Context, req *merchandisingv1.ResumePlacementWorkflowRequest) (*merchandisingv1.ResumePlacementWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "resume placement workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	for _, participant := range req.GetPlacement().GetParticipants() {
		if _, err := s.collectionClient.CreateCollection(ctx, &merchandisingv1.CreateCollectionRequest{
			Context: req.GetContext(),
			Collection: &merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Manager: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
					Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
						Location: &commonv1.PostalAddress{Region: req.GetPlacement().GetLocation().GetRegion()},
						Audit:    &commonv1.AuditInfo{UpdatedAt: req.GetPlacement().GetAudit().GetUpdatedAt()},
						Manager:  &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
					}},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionService/CreateCollection")
		}
	}
	return &merchandisingv1.ResumePlacementWorkflowResponse{
		Placement: &merchandisingv1.Placement{
			PlacementId: req.GetPlacementId(),
			Location:    req.GetPlacement().GetLocation(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPlacementWorkflows lists placements a page at a time.
func (s *PlacementWorkflowServer) ListPlacementWorkflows(ctx context.Context, req *merchandisingv1.ListPlacementWorkflowsRequest) (*merchandisingv1.ListPlacementWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPlacementWorkflows", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list placement workflows", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*merchandisingv1.ListPlacementWorkflowsRequest_Filter)
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	signalCollectionWorkflowRequest := &merchandisingv1.SignalCollectionWorkflowRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Location: &commonv1.PostalAddress{Region: cloned.GetRegion()},
				}},
				Manager: &identityv1.UserRef{
					Email:  (&decoded).GetActor().GetEmail(),
					UserId: req.GetContext().GetActor().GetUserId(),
				},
			}},
		},
	}
	var signalCollectionWorkflowReply *merchandisingv1.SignalCollectionWorkflowResponse
	if err := pipeline.Run(ctx, "acme.merchandising.v1.CollectionWorkflowService/SignalCollectionWorkflow", func(ctx context.Context) error {
		answer, err := s.collectionWorkflowClient.SignalCollectionWorkflow(ctx, signalCollectionWorkflowRequest)
		signalCollectionWorkflowReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/SignalCollectionWorkflow")
	}
	listSearchQueryExportsRequest := &searchv1.ListSearchQueryExportsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchQueryExportsRequest_Filter{
			Region:       signalCollectionWorkflowReply.GetCollection().GetLocation().GetRegion(),
			Query:        req.GetFilter().GetQuery(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
	}
	listSearchQueryExportsReply, err := searchv1.SearchQueryExportServiceClient.ListSearchQueryExports(s.searchQueryExportClient, ctx, listSearchQueryExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/ListSearchQueryExports")
	}
	return &merchandisingv1.ListPlacementWorkflowsResponse{
		Placements: []*merchandisingv1.Placement{&merchandisingv1.Placement{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: signalCollectionWorkflowReply.GetCollection().GetAudit().GetUpdatedAt(),
				UpdatedBy: signalCollectionWorkflowReply.GetCollection().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: listSearchQueryExportsReply.GetResult().GetStatus()},
		PageInfo: &commonv1.PageInfo{
			NextPageToken: listSearchQueryExportsReply.GetPageInfo().GetNextPageToken(),
		},
	}, nil
}

// SignalPlacementWorkflow signals a placement.
func (s *PlacementWorkflowServer) SignalPlacementWorkflow(ctx context.Context, req *merchandisingv1.SignalPlacementWorkflowRequest) (*merchandisingv1.SignalPlacementWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "signal placement workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*sellersv1.SubmitSellerScorecardResponse, len(req.GetPlacement().GetParticipants()))
	for i, participant := range req.GetPlacement().GetParticipants() {
		group.Go(func() error {
			reply, err := s.sellerScorecardApprovalClient.SubmitSellerScorecard(groupCtx, &sellersv1.SubmitSellerScorecardRequest{
				Context: req.GetContext(),
				SellerScorecard: &sellersv1.SellerScorecard{
					Counterparty: &customersv1.CustomerRef{Email: participant.GetContact().GetEmail()},
					Audit:        &commonv1.AuditInfo{CreatedAt: req.GetPlacement().GetAudit().GetCreatedAt()},
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/SubmitSellerScorecard")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeCoursesRequest := &learningv1.SummarizeCoursesRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &learningv1.SummarizeCoursesRequest_Details{Period: req.GetPlacement().GetWindow()},
	}
	summarizeCoursesReply := new(learningv1.SummarizeCoursesResponse)
	if err := grpcx.Invoke(ctx, s.conn, learningv1.CourseQueryService_SummarizeCourses_FullMethodName, summarizeCoursesRequest, summarizeCoursesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.learning.v1.CourseQueryService/SummarizeCourses")
	}
	metadata2 := make(map[string]string, len(req.GetPlacement().GetMetadata()))
	for key2, value2 := range req.GetPlacement().GetMetadata() {
		metadata2[key2] = value2
	}
	browseSurveysRequest := newBrowseSurveysRequest(withBrowseSurveysContext(req.GetContext()), withBrowseSurveysFilter(&surveysv1.BrowseSurveysRequest_Filter{
		Region: req.GetPlacement().GetLocation().GetRegion(),
		Labels: metadata2,
	}))
	browseSurveysReply, err := s.surveyQueryClient.BrowseSurveys(ctx, browseSurveysRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.surveys.v1.SurveyQueryService/BrowseSurveys")
	}
	checkNotificationEligibilityRequest := &notificationsv1.CheckNotificationEligibilityRequest{Context: req.GetContext()}
	checkNotificationEligibilityReply, err := notificationsv1.NotificationValidationServiceClient.CheckNotificationEligibility(s.notificationValidationClient, ctx, checkNotificationEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/CheckNotificationEligibility")
	}
	_ = browseSurveysReply
	return &merchandisingv1.SignalPlacementWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: summarizeCoursesReply.GetResult().GetMessage(),
			Status:  checkNotificationEligibilityReply.GetOutcome().GetStatus(),
		},
		Outcome: &merchandisingv1.SignalPlacementWorkflowResponse_Outcome{Status: summarizeCoursesReply.GetResult().GetStatus()},
		Placement: &merchandisingv1.Placement{
			Organizer: &identityv1.UserRef{
				DisplayName: checkNotificationEligibilityReply.GetNotification().GetAssignee().GetDisplayName(),
			},
			PlacementId: req.GetPlacementId(),
			Title:       req.GetPlacement().GetTitle(),
		},
	}, nil
}

// resumeSellerListingWorkflowSender returns a function that sends one request to
// acme.sellers.v1.SellerListingWorkflowService/ResumeSellerListingWorkflow.
func (s *PlacementWorkflowServer) resumeSellerListingWorkflowSender(request *sellersv1.ResumeSellerListingWorkflowRequest) func(context.Context) (*sellersv1.ResumeSellerListingWorkflowResponse, error) {
	return func(ctx context.Context) (*sellersv1.ResumeSellerListingWorkflowResponse, error) {
		return s.sellerListingWorkflowClient.ResumeSellerListingWorkflow(ctx, request)
	}
}
