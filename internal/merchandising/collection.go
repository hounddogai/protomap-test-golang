package merchandising

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CollectionServer implements `acme.merchandising.v1.CollectionService`.
// Manages collections: creates, reads, changes, and lists them.
type CollectionServer struct {
	merchandisingv1.UnimplementedCollectionServiceServer
	store                                 *Store
	log                                   *slog.Logger
	placementValidationClient             merchandisingv1.PlacementValidationServiceClient
	clients                               *registry.Registry
	conn                                  *grpc.ClientConn
	bus                                   *eventbus.Bus
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	carePlanQueryClient                   careplansv1.CarePlanQueryServiceClient
}

// NewCollectionServer wires a CollectionServer.
func NewCollectionServer(deps platform.Deps) *CollectionServer {
	return &CollectionServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.merchandising.v1.CollectionService"),
		placementValidationClient:             merchandisingv1.NewPlacementValidationServiceClient(deps.Conn),
		clients:                               deps.Clients,
		conn:                                  deps.Conn,
		bus:                                   deps.Bus,
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		carePlanQueryClient:                   careplansv1.NewCarePlanQueryServiceClient(deps.Conn),
	}
}

// CreateCollection creates a collection.
func (s *CollectionServer) CreateCollection(ctx context.Context, req *merchandisingv1.CreateCollectionRequest) (*merchandisingv1.CreateCollectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "create collection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "etag", req.GetCollection().GetAudit().GetEtag())
	names := collectCollectionNames(req.GetCollection(), nil)
	previewPlacementReply, err := s.placementValidationClient.PreviewPlacement(ctx, &merchandisingv1.PreviewPlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Audit:    &commonv1.AuditInfo{Etag: tenancy.Value(ctx, "etag")},
			Location: &commonv1.PostalAddress{Lines: names},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/PreviewPlacement")
	}
	placementValidation, err := registry.Lookup[merchandisingv1.PlacementValidationServiceClient](s.clients, "acme.merchandising.v1.PlacementValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	names2 := collectCollectionNames(req.GetCollection(), nil)
	checkPlacementEligibilityRequest := &merchandisingv1.CheckPlacementEligibilityRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Location: &commonv1.PostalAddress{
				Locality: previewPlacementReply.GetPlacement().GetLocation().GetLocality(),
				Lines:    names2,
			},
		},
	}
	checkPlacementEligibilityReply, err := placementValidation.CheckPlacementEligibility(ctx, checkPlacementEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/CheckPlacementEligibility")
	}
	names3 := collectCollectionNames(req.GetCollection(), nil)
	explainPlacementDecisionRequest := &merchandisingv1.ExplainPlacementDecisionRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: checkPlacementEligibilityReply.GetPlacement().GetAudit().GetUpdatedBy(),
				CreatedAt: checkPlacementEligibilityReply.GetPlacement().GetAudit().GetCreatedAt(),
				CreatedBy: previewPlacementReply.GetPlacement().GetAudit().GetCreatedBy(),
			},
			Recurrence: &merchandisingv1.Placement_Weekly{Weekly: &merchandisingv1.PlacementWeeklyRule{
				Interval: checkPlacementEligibilityReply.GetPlacement().GetWeekly().GetInterval(),
			}},
			Location: &commonv1.PostalAddress{Lines: names3},
		},
	}
	explainPlacementDecisionReply := new(merchandisingv1.ExplainPlacementDecisionResponse)
	if err := grpc.Invoke(ctx, "/acme.merchandising.v1.PlacementValidationService/ExplainPlacementDecision", explainPlacementDecisionRequest, explainPlacementDecisionReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/ExplainPlacementDecision")
	}
	return &merchandisingv1.CreateCollectionResponse{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
						Location: previewPlacementReply.GetPlacement().GetLocation(),
					}},
					Audit: &commonv1.AuditInfo{
						CreatedBy: explainPlacementDecisionReply.GetPlacement().GetAudit().GetCreatedBy(),
					},
				}},
			}},
			Location: &commonv1.PostalAddress{
				Recipient: checkPlacementEligibilityReply.GetPlacement().GetLocation().GetRecipient(),
			},
			Name:   req.GetCollection().GetName(),
			Status: req.GetCollection().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetCollection returns a collection.
func (s *CollectionServer) GetCollection(ctx context.Context, req *merchandisingv1.GetCollectionRequest) (*merchandisingv1.GetCollectionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "collection", req.GetCollectionId(), "get_collection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get collection", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	checkPlacementEligibilityRequest := &merchandisingv1.CheckPlacementEligibilityRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact:       &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
				ParticipantId: req.GetCollectionId(),
			}},
			Organizer: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
		Details: &merchandisingv1.CheckPlacementEligibilityRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	checkPlacementEligibilityReply, err := grpcx.Call(ctx, s.placementValidationClient.CheckPlacementEligibility, checkPlacementEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/CheckPlacementEligibility")
	}
	return &merchandisingv1.GetCollectionResponse{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Audit: &commonv1.AuditInfo{
					CreatedAt: checkPlacementEligibilityReply.GetPlacement().GetAudit().GetCreatedAt(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				Etag: checkPlacementEligibilityReply.GetPlacement().GetAudit().GetEtag(),
			},
			CollectionId: req.GetCollectionId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UpdateCollection changes a collection.
func (s *CollectionServer) UpdateCollection(ctx context.Context, req *merchandisingv1.UpdateCollectionRequest) (*merchandisingv1.UpdateCollectionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "update collection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	names := collectCollectionNames(req.GetCollection(), nil)
	validatePlacementRequest := &merchandisingv1.ValidatePlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetCollection().GetManager().GetEmail()},
			}},
			Location: &commonv1.PostalAddress{
				Region: req.GetCollection().GetLocation().GetRegion(),
				Lines:  names,
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetCollection().GetAudit().GetUpdatedAt()},
		},
	}
	validatePlacementReply, err := grpcx.Call(ctx, s.placementValidationClient.ValidatePlacement, validatePlacementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/ValidatePlacement")
	}
	return &merchandisingv1.UpdateCollectionResponse{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Location: validatePlacementReply.GetPlacement().GetLocation(),
				}},
			}},
			Manager: &identityv1.UserRef{
				DisplayName: validatePlacementReply.GetPlacement().GetOrganizer().GetDisplayName(),
			},
			Location:     req.GetCollection().GetLocation(),
			CollectionId: req.GetCollection().GetCollectionId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DeleteCollection deletes a collection.
func (s *CollectionServer) DeleteCollection(ctx context.Context, req *merchandisingv1.DeleteCollectionRequest) (*merchandisingv1.DeleteCollectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "collection", req.GetCollectionId(), "delete_collection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete collection", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "merchandising.delete_collection.check_placement_eligibility",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"reason":                req.GetReason(),
			"placement.audit.etag":  req.GetEtag(),
			"details.contact.email": req.GetContext().GetActor().GetEmail(),
			"context.tenant_id":     req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &merchandisingv1.DeleteCollectionResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// BatchGetCollections returns several collections at once.
func (s *CollectionServer) BatchGetCollections(ctx context.Context, req *merchandisingv1.BatchGetCollectionsRequest) (*merchandisingv1.BatchGetCollectionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "batch get collections", "tenant", req.GetContext().GetTenantId())
	updateAccessPolicyRequest := &authorizationv1.UpdateAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	}
	updateAccessPolicyReply, err := grpcx.NewClient(s.conn, authorizationv1.NewAccessPolicyServiceClient).UpdateAccessPolicy(ctx, updateAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/UpdateAccessPolicy")
	}
	previewPlacementRequest := &merchandisingv1.PreviewPlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Organizer: &identityv1.UserRef{
				UserId: updateAccessPolicyReply.GetAccessPolicy().GetOwner().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: updateAccessPolicyReply.GetAccessPolicy().GetAudit().GetUpdatedBy(),
				CreatedAt: updateAccessPolicyReply.GetAccessPolicy().GetAudit().GetCreatedAt(),
			},
		},
	}
	previewPlacementReply, err := grpcx.Call(ctx, s.placementValidationClient.PreviewPlacement, previewPlacementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/PreviewPlacement")
	}
	return &merchandisingv1.BatchGetCollectionsResponse{
		Collections: []*merchandisingv1.Collection{&merchandisingv1.Collection{
			Manager: &identityv1.UserRef{
				DisplayName: updateAccessPolicyReply.GetAccessPolicy().GetOwner().GetDisplayName(),
				Email:       updateAccessPolicyReply.GetAccessPolicy().GetOwner().GetEmail(),
			},
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: previewPlacementReply.GetPlacement().GetAudit().GetUpdatedBy(),
					},
				}},
			}},
		}},
		Result: &commonv1.OperationResult{
			CorrelationId: previewPlacementReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// BatchCreateCollections takes a stream of collections and answers once at the end.
func (s *CollectionServer) BatchCreateCollections(stream grpc.ClientStreamingServer[merchandisingv1.BatchCreateCollectionsRequest, merchandisingv1.BatchCreateCollectionsResponse]) error {
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
		s.log.InfoContext(ctx, "batch create collections", "tenant", msg.GetContext().GetTenantId())
		lintPlacementsRequest := &merchandisingv1.LintPlacementsRequest{
			Context: msg.GetContext(),
			Placement: &merchandisingv1.Placement{
				Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
					Contact: &commonv1.ContactInfo{Email: msg.GetRecord().GetManager().GetEmail()},
				}},
			},
			Details: &merchandisingv1.LintPlacementsRequest_Details{
				Contact: &commonv1.ContactInfo{Name: msg.GetHeader().GetSubmittedBy().GetName()},
			},
		}
		if _, err := grpcx.Call(ctx, s.placementValidationClient.LintPlacements, lintPlacementsRequest); err != nil {
			return grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/LintPlacements")
		}
		estimateSellerPayoutScheduleRequest := &sellersv1.EstimateSellerPayoutScheduleRequest{Context: msg.GetContext()}
		if _, err := grpcx.Call(ctx, s.sellerPayoutScheduleCalculationClient.EstimateSellerPayoutSchedule, estimateSellerPayoutScheduleRequest); err != nil {
			return grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/EstimateSellerPayoutSchedule")
		}
		accepted++
	}
	return stream.SendAndClose(&merchandisingv1.BatchCreateCollectionsResponse{Accepted: accepted})
}

// CloneCollection creates a collection.
func (s *CollectionServer) CloneCollection(ctx context.Context, req *merchandisingv1.CloneCollectionRequest) (*merchandisingv1.CloneCollectionResponse, error) {
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "clone collection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	names := collectCollectionNames(req.GetCollection(), nil)
	checkNotificationEligibilityRequest := &notificationsv1.CheckNotificationEligibilityRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetCollection().GetManager().GetDisplayName(),
			},
			Tags: names,
		},
	}
	checkNotificationEligibilityReply := new(notificationsv1.CheckNotificationEligibilityResponse)
	if err := grpcx.Invoke(ctx, s.conn, notificationsv1.NotificationValidationService_CheckNotificationEligibility_FullMethodName, checkNotificationEligibilityRequest, checkNotificationEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/CheckNotificationEligibility")
	}
	step := &collectionServerCloneCollectionFindCarePlansStep{request: &careplansv1.FindCarePlansRequest{
		Context: req.GetContext(),
		Filter: &careplansv1.FindCarePlansRequest_Filter{
			Region:       req.GetCollection().GetLocation().GetRegion(),
			CreatedAfter: checkNotificationEligibilityReply.GetNotification().GetAudit().GetUpdatedAt(),
		},
	}}
	if err := s.runCloneCollectionFindCarePlans(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.careplans.v1.CarePlanQueryService/FindCarePlans")
	}
	findCarePlansReply := step.reply
	return &merchandisingv1.CloneCollectionResponse{
		Result: &commonv1.OperationResult{
			Status:       checkNotificationEligibilityReply.GetResult().GetStatus(),
			ReferenceIds: findCarePlansReply.GetResult().GetReferenceIds(),
		},
		Collection: &merchandisingv1.Collection{
			Manager: req.GetCollection().GetManager(),
			Depth:   req.GetCollection().GetDepth(),
		},
	}, nil
}

// collectionServerCloneCollectionFindCarePlansStep carries the request of one step of CloneCollection and, once it ran,
// its reply.
type collectionServerCloneCollectionFindCarePlansStep struct {
	request *careplansv1.FindCarePlansRequest
	reply   *careplansv1.FindCarePlansResponse
}

func (s *CollectionServer) runCloneCollectionFindCarePlans(ctx context.Context, step *collectionServerCloneCollectionFindCarePlansStep) error {
	reply, err := s.carePlanQueryClient.FindCarePlans(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
