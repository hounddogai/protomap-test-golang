package fulfillment

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	compliancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/compliance/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// FulfillmentExceptionValidationServer implements `acme.fulfillment.v1.FulfillmentExceptionValidationService`.
// Checks fulfillment exceptions against the rules before anything acts on them.
type FulfillmentExceptionValidationServer struct {
	fulfillmentv1.UnimplementedFulfillmentExceptionValidationServiceServer
	store                           *Store
	log                             *slog.Logger
	parcelValidationClient          shippingv1.ParcelValidationServiceClient
	parcelCalculationClient         shippingv1.ParcelCalculationServiceClient
	sessionQueryClient              identityv1.SessionQueryServiceClient
	complianceCaseCalculationClient compliancev1.ComplianceCaseCalculationServiceClient
	parcelValidationPort            parcelValidationPort
	conn                            *grpc.ClientConn
	roleHistoryClient               identityv1.RoleHistoryServiceClient
	searchIndexExportClient         searchv1.SearchIndexExportServiceClient
	userQueryOnce                   sync.Once
	userQueryCached                 identityv1.UserQueryServiceClient
	searchIndexValidationClient     searchv1.SearchIndexValidationServiceClient
	returnLabelQueryClient          returnsv1.ReturnLabelQueryServiceClient
	venueValidationClient           venuesv1.VenueValidationServiceClient
	documentQueryClient             documentsv1.DocumentQueryServiceClient
	documentClient                  documentsv1.DocumentServiceClient
}

// NewFulfillmentExceptionValidationServer wires a FulfillmentExceptionValidationServer.
func NewFulfillmentExceptionValidationServer(deps platform.Deps) *FulfillmentExceptionValidationServer {
	return &FulfillmentExceptionValidationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.fulfillment.v1.FulfillmentExceptionValidationService"),
		parcelValidationClient:          shippingv1.NewParcelValidationServiceClient(deps.Conn),
		parcelCalculationClient:         shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		sessionQueryClient:              identityv1.NewSessionQueryServiceClient(deps.Conn),
		complianceCaseCalculationClient: compliancev1.NewComplianceCaseCalculationServiceClient(deps.Conn),
		parcelValidationPort:            newParcelValidationPort(deps),
		conn:                            deps.Conn,
		roleHistoryClient:               identityv1.NewRoleHistoryServiceClient(deps.Conn),
		searchIndexExportClient:         searchv1.NewSearchIndexExportServiceClient(deps.Conn),
		searchIndexValidationClient:     searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		returnLabelQueryClient:          returnsv1.NewReturnLabelQueryServiceClient(deps.Conn),
		venueValidationClient:           venuesv1.NewVenueValidationServiceClient(deps.Conn),
		documentQueryClient:             documentsv1.NewDocumentQueryServiceClient(deps.Conn),
		documentClient:                  documentsv1.NewDocumentServiceClient(deps.Conn),
	}
}

// ValidateFulfillmentException validates a fulfillment exception.
func (s *FulfillmentExceptionValidationServer) ValidateFulfillmentException(ctx context.Context, req *fulfillmentv1.ValidateFulfillmentExceptionRequest) (*fulfillmentv1.ValidateFulfillmentExceptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidateFulfillmentException", "error", err)
		return nil, err
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "validate fulfillment exception", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	previewParcelRequest := buildPreviewParcelRequestForValidateFulfillmentException(req)
	previewParcelReply, err := s.parcelValidationClient.PreviewParcel(ctx, previewParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/PreviewParcel")
	}
	calculateParcelRequest := newCalculateParcelRequest(withCalculateParcelContext(req.GetContext()), withCalculateParcelParcel(&shippingv1.Parcel{
		Audit: &commonv1.AuditInfo{
			CreatedAt: req.GetFulfillmentException().GetAudit().GetCreatedAt(),
		},
	}))
	calculateParcelReply, err := s.parcelCalculationClient.CalculateParcel(ctx, calculateParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/CalculateParcel")
	}
	customFields := make(map[string]string, len(req.GetFulfillmentException().GetCustomFields()))
	for key, value := range req.GetFulfillmentException().GetCustomFields() {
		customFields[key] = value
	}
	exportSessionsStream, err := s.sessionQueryClient.ExportSessions(ctx, &identityv1.ExportSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ExportSessionsRequest_Filter{
			Region:       previewParcelReply.GetParcel().GetAddress().GetRegion(),
			CreatedAfter: calculateParcelReply.GetParcel().GetAudit().GetCreatedAt(),
			OwnerId:      calculateParcelReply.GetParcel().GetComponents()["primary"].GetComponentId(),
			Labels:       customFields,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
	}
	for {
		received, err := exportSessionsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
		}
		_ = received
	}
	var displayName string
	switch choice := req.GetFulfillmentException().GetOrigin().(type) {
	case *fulfillmentv1.FulfillmentException_Customer:
		displayName = choice.Customer.GetDisplayName()
	case *fulfillmentv1.FulfillmentException_ExternalReference:
		displayName = choice.ExternalReference
	case *fulfillmentv1.FulfillmentException_Channel:
		displayName = choice.Channel.GetChannel()
	}
	recalculateComplianceCasesRequest := &compliancev1.RecalculateComplianceCasesRequest{
		Context: req.GetContext(),
		ComplianceCase: &compliancev1.ComplianceCase{
			Origin: &compliancev1.ComplianceCase_Customer{Customer: &compliancev1.AttestationRef{DisplayName: displayName}},
			Sla: &compliancev1.ComplianceCaseSla{
				PausedFor: req.GetFulfillmentException().GetSla().GetPausedFor(),
			},
			Assignee: &identityv1.UserRef{
				Email: calculateParcelReply.GetParcel().GetOwner().GetEmail(),
			},
			Requester: &commonv1.ContactInfo{
				Phone: req.GetFulfillmentException().GetRequester().GetPhone(),
			},
		},
	}
	recalculateComplianceCasesReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*compliancev1.RecalculateComplianceCasesResponse, error) {
		return s.complianceCaseCalculationClient.RecalculateComplianceCases(ctx, recalculateComplianceCasesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.compliance.v1.ComplianceCaseCalculationService/RecalculateComplianceCases")
	}
	return &fulfillmentv1.ValidateFulfillmentExceptionResponse{
		Result: &commonv1.OperationResult{Status: previewParcelReply.GetResult().GetStatus()},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Assignee: &identityv1.UserRef{
				Email:       calculateParcelReply.GetParcel().GetOwner().GetEmail(),
				DisplayName: calculateParcelReply.GetParcel().GetOwner().GetDisplayName(),
			},
			Sla: &fulfillmentv1.FulfillmentExceptionSla{
				PolicyId: recalculateComplianceCasesReply.GetComplianceCase().GetSla().GetPolicyId(),
			},
			CustomFields:           recalculateComplianceCasesReply.GetComplianceCase().GetCustomFields(),
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Subject:                req.GetFulfillmentException().GetSubject(),
		},
	}, nil
}

// PreviewFulfillmentException previews a fulfillment exception.
func (s *FulfillmentExceptionValidationServer) PreviewFulfillmentException(ctx context.Context, req *fulfillmentv1.PreviewFulfillmentExceptionRequest) (*fulfillmentv1.PreviewFulfillmentExceptionResponse, error) {
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "preview fulfillment exception", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*fulfillmentv1.PreviewFulfillmentExceptionRequest_Details)
	validateParcelRequest := &shippingv1.ValidateParcelRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ValidateParcelRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Phone:            cloned.GetContact().GetPhone(),
				Name:             req.GetDetails().GetContact().GetName(),
			},
		},
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetFulfillmentException().GetAudit().GetUpdatedAt(),
			},
		},
	}
	validateParcelReply, err := s.parcelValidationPort.ValidateParcel(ctx, validateParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/ValidateParcel")
	}
	browseDocumentsRequest := &documentsv1.BrowseDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.BrowseDocumentsRequest_Filter{
			Region: validateParcelReply.GetParcel().GetAddress().GetRegion(),
		},
	}
	browseDocumentsReply, err := browseDocumentsCall(ctx, browseDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/BrowseDocuments")
	}
	return &fulfillmentv1.PreviewFulfillmentExceptionResponse{
		Result: &commonv1.OperationResult{
			Message: validateParcelReply.GetResult().GetMessage(),
			Status:  browseDocumentsReply.GetResult().GetStatus(),
		},
		Outcome: &fulfillmentv1.PreviewFulfillmentExceptionResponse_Outcome{
			NextSteps: validateParcelReply.GetOutcome().GetNextSteps(),
			Status:    browseDocumentsReply.GetResult().GetStatus(),
		},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Requester:              req.GetFulfillmentException().GetRequester(),
		},
	}, nil
}

// CheckFulfillmentExceptionEligibility checks a fulfillment exception.
func (s *FulfillmentExceptionValidationServer) CheckFulfillmentExceptionEligibility(ctx context.Context, req *fulfillmentv1.CheckFulfillmentExceptionEligibilityRequest) (*fulfillmentv1.CheckFulfillmentExceptionEligibilityResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "check fulfillment exception eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	checkSubscriptionPlanEligibilityRequest := &subscriptionsv1.CheckSubscriptionPlanEligibilityRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetFulfillmentException().GetAudit().GetCreatedBy(),
				CreatedAt: req.GetFulfillmentException().GetAudit().GetCreatedAt(),
			},
		},
		Details: &subscriptionsv1.CheckSubscriptionPlanEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name: req.GetFulfillmentException().GetRequester().GetName(),
			},
		},
	}
	checkSubscriptionPlanEligibilityReply := new(subscriptionsv1.CheckSubscriptionPlanEligibilityResponse)
	if err := s.conn.Invoke(ctx, subscriptionsSubscriptionPlanValidationServiceCheckSubscriptionPlanEligibilityMethod, checkSubscriptionPlanEligibilityRequest, checkSubscriptionPlanEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/CheckSubscriptionPlanEligibility")
	}
	calculateComplianceCaseRequest := &compliancev1.CalculateComplianceCaseRequest{
		Context: req.GetContext(),
		ComplianceCase: &compliancev1.ComplianceCase{
			Requester: &commonv1.ContactInfo{
				Name:  req.GetFulfillmentException().GetRequester().GetName(),
				Email: req.GetFulfillmentException().GetAssignee().GetEmail(),
			},
		},
	}
	calculateComplianceCaseReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*compliancev1.CalculateComplianceCaseResponse, error) {
		return s.complianceCaseCalculationClient.CalculateComplianceCase(ctx, calculateComplianceCaseRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase")
	}
	for _, item := range req.GetFulfillmentException().GetHistory() {
		if _, err := s.sessionQueryClient.FindSessions(ctx, &identityv1.FindSessionsRequest{
			Context: req.GetContext(),
			Filter: &identityv1.FindSessionsRequest_Filter{
				CreatedAfter: item.GetChangedAt(),
				OwnerId:      checkSubscriptionPlanEligibilityReply.GetSubscriptionPlan().GetOwner().GetUserId(),
				Labels:       req.GetFulfillmentException().GetCustomFields(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/FindSessions")
		}
	}
	encoded, err := protojson.Marshal(req.GetFulfillmentException())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fulfillmentv1.FulfillmentException
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	normalizeRestockDecisionRequest := &returnsv1.NormalizeRestockDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				Email:  calculateComplianceCaseReply.GetComplianceCase().GetRequester().GetEmail(),
				UserId: (&decoded).GetAssignee().GetUserId(),
			},
			Action: &returnsv1.RestockDecision_Discount{Discount: &returnsv1.RestockDecisionDiscountAction{
				Amount: checkSubscriptionPlanEligibilityReply.GetSubscriptionPlan().GetDiscount().GetAmount(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetFulfillmentException().GetAudit().GetUpdatedAt(),
			},
		},
	}
	normalizeRestockDecisionReply := new(returnsv1.NormalizeRestockDecisionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", returnsRestockDecisionValidationServiceName, "NormalizeRestockDecision"), normalizeRestockDecisionRequest, normalizeRestockDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionValidationService/NormalizeRestockDecision")
	}
	return &fulfillmentv1.CheckFulfillmentExceptionEligibilityResponse{
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkSubscriptionPlanEligibilityReply.GetSubscriptionPlan().GetAudit().GetCreatedBy(),
			},
			Subject:                calculateComplianceCaseReply.GetComplianceCase().GetSubject(),
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			CustomFields:           req.GetFulfillmentException().GetCustomFields(),
		},
		Outcome: &fulfillmentv1.CheckFulfillmentExceptionEligibilityResponse_Outcome{
			ReferenceId: checkSubscriptionPlanEligibilityReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: calculateComplianceCaseReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// NormalizeFulfillmentException normalizes a fulfillment exception.
func (s *FulfillmentExceptionValidationServer) NormalizeFulfillmentException(ctx context.Context, req *fulfillmentv1.NormalizeFulfillmentExceptionRequest) (*fulfillmentv1.NormalizeFulfillmentExceptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "normalize fulfillment exception", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	jobs := make(chan *fulfillmentv1.FulfillmentExceptionStatusChange)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				if _, err := s.roleHistoryClient.RevertRole(workCtx, &identityv1.RevertRoleRequest{
					Context: req.GetContext(),
					Role: &identityv1.Role{
						Audit: &commonv1.AuditInfo{
							CreatedAt: item.GetChangedAt(),
							Etag:      req.GetFulfillmentException().GetAudit().GetEtag(),
						},
						Owner: &identityv1.UserRef{
							DisplayName: req.GetFulfillmentException().GetCustomer().GetDisplayName(),
						},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetFulfillmentException().GetHistory() {
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
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/RevertRole")
	}
	scheduleSearchIndexExportRequest := buildScheduleSearchIndexExportRequestForNormalizeFulfillmentException(req)
	scheduleSearchIndexExportReply, err := s.searchIndexExportClient.ScheduleSearchIndexExport(ctx, scheduleSearchIndexExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ScheduleSearchIndexExport")
	}
	findUsersRequest := &identityv1.FindUsersRequest{Context: req.GetContext()}
	findUsersReply, err := s.userQueryLazy().FindUsers(ctx, findUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/FindUsers")
	}
	ctx = tenancy.With(ctx, "packing_slip_id", req.GetFulfillmentException().GetCustomer().GetPackingSlipId())
	listSearchQuerySubscriptionsRequest := &searchv1.ListSearchQuerySubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchQuerySubscriptionsRequest_Filter{
			CreatedAfter: req.GetFulfillmentException().GetSla().GetDueAt(),
			OwnerId:      tenancy.Value(ctx, "packing_slip_id"),
		},
	}
	listSearchQuerySubscriptionsReply := new(searchv1.ListSearchQuerySubscriptionsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", searchSearchQueryNotificationServiceName, "ListSearchQuerySubscriptions"), listSearchQuerySubscriptionsRequest, listSearchQuerySubscriptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/ListSearchQuerySubscriptions")
	}
	return &fulfillmentv1.NormalizeFulfillmentExceptionResponse{
		Result: &commonv1.OperationResult{
			Details:       scheduleSearchIndexExportReply.GetResult().GetDetails(),
			Message:       scheduleSearchIndexExportReply.GetResult().GetMessage(),
			CorrelationId: findUsersReply.GetResult().GetCorrelationId(),
			Status:        listSearchQuerySubscriptionsReply.GetResult().GetStatus(),
		},
		Outcome: &fulfillmentv1.NormalizeFulfillmentExceptionResponse_Outcome{Status: findUsersReply.GetResult().GetStatus()},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Requester:              req.GetFulfillmentException().GetRequester(),
		},
	}, nil
}

// ExplainFulfillmentExceptionDecision explains a fulfillment exception.
func (s *FulfillmentExceptionValidationServer) ExplainFulfillmentExceptionDecision(ctx context.Context, req *fulfillmentv1.ExplainFulfillmentExceptionDecisionRequest) (*fulfillmentv1.ExplainFulfillmentExceptionDecisionResponse, error) {
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "explain fulfillment exception decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetFulfillmentException()).(*fulfillmentv1.FulfillmentException)
	var displayName string
	switch choice := req.GetFulfillmentException().GetOrigin().(type) {
	case *fulfillmentv1.FulfillmentException_Customer:
		displayName = choice.Customer.GetDisplayName()
	case *fulfillmentv1.FulfillmentException_ExternalReference:
		displayName = choice.ExternalReference
	case *fulfillmentv1.FulfillmentException_Channel:
		displayName = choice.Channel.GetChannel()
	}
	scoreSearchIndexReply, err := s.searchIndexValidationClient.ScoreSearchIndex(ctx, &searchv1.ScoreSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetFulfillmentException().GetAudit().GetEtag(),
				UpdatedAt: req.GetFulfillmentException().GetAudit().GetUpdatedAt(),
				CreatedBy: cloned.GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{DisplayName: displayName},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/ScoreSearchIndex")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*returnsv1.SuggestReturnLabelsResponse, len(req.GetFulfillmentException().GetComments()))
	for i, comment := range req.GetFulfillmentException().GetComments() {
		group.Go(func() error {
			reply, err := s.returnLabelQueryClient.SuggestReturnLabels(groupCtx, &returnsv1.SuggestReturnLabelsRequest{
				Context: req.GetContext(),
				Filter: &returnsv1.SuggestReturnLabelsRequest_Filter{
					CreatedAfter: comment.GetCreatedAt(),
					Region:       scoreSearchIndexReply.GetSearchIndex().GetAddress().GetRegion(),
					OwnerId:      comment.GetCommentId(),
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
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelQueryService/SuggestReturnLabels")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &fulfillmentv1.ExplainFulfillmentExceptionDecisionResponse{
		Outcome: &fulfillmentv1.ExplainFulfillmentExceptionDecisionResponse_Outcome{Message: scoreSearchIndexReply.GetResult().GetMessage()},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Comments: []*fulfillmentv1.FulfillmentExceptionComment{&fulfillmentv1.FulfillmentExceptionComment{
				CreatedAt: scoreSearchIndexReply.GetSearchIndex().GetAudit().GetCreatedAt(),
			}},
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Requester:              req.GetFulfillmentException().GetRequester(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreFulfillmentException scores a fulfillment exception.
func (s *FulfillmentExceptionValidationServer) ScoreFulfillmentException(ctx context.Context, req *fulfillmentv1.ScoreFulfillmentExceptionRequest) (*fulfillmentv1.ScoreFulfillmentExceptionResponse, error) {
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "score fulfillment exception", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	jobs := make(chan *fulfillmentv1.FulfillmentExceptionComment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for comment := range jobs {
				if _, err := s.venueValidationClient.LintVenues(workCtx, &venuesv1.LintVenuesRequest{
					Context: req.GetContext(),
					Venue: &venuesv1.Venue{
						Audit:   &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
						Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()}},
					},
					Details: &venuesv1.LintVenuesRequest_Details{
						Contact: &commonv1.ContactInfo{Email: comment.GetAuthorEmail()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetFulfillmentException().GetComments() {
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
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/LintVenues")
	}
	resolveDocumentsRequest := &documentsv1.ResolveDocumentsRequest{Context: req.GetContext()}
	resolveDocumentsReply := new(documentsv1.ResolveDocumentsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", documentsDocumentQueryServiceName, "ResolveDocuments"), resolveDocumentsRequest, resolveDocumentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/ResolveDocuments")
	}
	validateVenueRequest := buildValidateVenueRequestForScoreFulfillmentException(req)
	validateVenueReply, err := s.venueValidationClient.ValidateVenue(ctx, validateVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/ValidateVenue")
	}
	return &fulfillmentv1.ScoreFulfillmentExceptionResponse{
		Result: &commonv1.OperationResult{Status: resolveDocumentsReply.GetResult().GetStatus()},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Assignee: &identityv1.UserRef{
				UserId: validateVenueReply.GetVenue().GetOwner().GetUserId(),
			},
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Subject:                req.GetFulfillmentException().GetSubject(),
		},
	}, nil
}

// LintFulfillmentExceptions lints a fulfillment exception.
func (s *FulfillmentExceptionValidationServer) LintFulfillmentExceptions(ctx context.Context, req *fulfillmentv1.LintFulfillmentExceptionsRequest) (*fulfillmentv1.LintFulfillmentExceptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFulfillmentException(ctx, req.GetContext().GetTenantId(), req.GetFulfillmentException()); err != nil {
		return nil, grpcx.Wrap(err, "save fulfillment exception")
	}
	s.log.InfoContext(ctx, "lint fulfillment exceptions", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFulfillmentException().GetAudit().GetEtag())
	customFields := make(map[string]string, len(req.GetFulfillmentException().GetCustomFields()))
	for key, value := range req.GetFulfillmentException().GetCustomFields() {
		customFields[key] = value
	}
	findSessionsRequest := &identityv1.FindSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.FindSessionsRequest_Filter{
			CreatedAfter: req.GetFulfillmentException().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetFulfillmentException().GetSla().GetPolicyId(),
			Labels:       customFields,
		},
	}
	findSessionsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*identityv1.FindSessionsResponse, error) {
		return s.sessionQueryClient.FindSessions(ctx, findSessionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/FindSessions")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*documentsv1.FindDocumentsResponse, len(req.GetFulfillmentException().GetHistory()))
	for i, item := range req.GetFulfillmentException().GetHistory() {
		group.Go(func() error {
			reply, err := s.documentQueryClient.FindDocuments(groupCtx, &documentsv1.FindDocumentsRequest{
				Context: req.GetContext(),
				Filter:  &documentsv1.FindDocumentsRequest_Filter{CreatedAfter: item.GetChangedAt()},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/FindDocuments")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	watchDocumentsStream, err := s.documentClient.WatchDocuments(ctx, &documentsv1.WatchDocumentsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
	}
	for {
		received, err := watchDocumentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
		}
		_ = received
	}
	cloned := proto.Clone(req.GetFulfillmentException()).(*fulfillmentv1.FulfillmentException)
	estimateComplianceCaseRequest := &compliancev1.EstimateComplianceCaseRequest{
		Context: req.GetContext(),
		ComplianceCase: &compliancev1.ComplianceCase{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetFulfillmentException().GetRequester().GetPreferredChannel(),
				Email:            req.GetDetails().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{UpdatedBy: cloned.GetAudit().GetUpdatedBy()},
			Origin: &compliancev1.ComplianceCase_Customer{Customer: &compliancev1.AttestationRef{
				DisplayName: req.GetFulfillmentException().GetCustomer().GetDisplayName(),
			}},
		},
	}
	estimateComplianceCaseReply, err := compliancev1.ComplianceCaseCalculationServiceClient.EstimateComplianceCase(s.complianceCaseCalculationClient, ctx, estimateComplianceCaseRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.compliance.v1.ComplianceCaseCalculationService/EstimateComplianceCase")
	}
	return &fulfillmentv1.LintFulfillmentExceptionsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: findSessionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &fulfillmentv1.LintFulfillmentExceptionsResponse_Outcome{Status: findSessionsReply.GetResult().GetStatus()},
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Requester: &commonv1.ContactInfo{
				Name: estimateComplianceCaseReply.GetComplianceCase().GetRequester().GetName(),
			},
			FulfillmentExceptionId: req.GetFulfillmentExceptionId(),
			Assignee:               req.GetFulfillmentException().GetAssignee(),
		},
	}, nil
}

// GetFulfillmentExceptionRules returns a fulfillment exception.
func (s *FulfillmentExceptionValidationServer) GetFulfillmentExceptionRules(ctx context.Context, req *fulfillmentv1.GetFulfillmentExceptionRulesRequest) (*fulfillmentv1.GetFulfillmentExceptionRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetFulfillmentExceptionRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fulfillment_exception", req.GetFulfillmentExceptionId(), "get_fulfillment_exception_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get fulfillment exception rules", "tenant", req.GetContext().GetTenantId())
	return &fulfillmentv1.GetFulfillmentExceptionRulesResponse{
		FulfillmentException: &fulfillmentv1.FulfillmentException{FulfillmentExceptionId: req.GetFulfillmentExceptionId()},
		Result:               &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// userQueryLazy dials acme.identity.v1.UserQueryService the first time a handler needs it.
func (s *FulfillmentExceptionValidationServer) userQueryLazy() identityv1.UserQueryServiceClient {
	s.userQueryOnce.Do(func() { s.userQueryCached = identityv1.NewUserQueryServiceClient(s.conn) })
	return s.userQueryCached
}
