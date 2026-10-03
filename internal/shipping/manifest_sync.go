package shipping

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ManifestSyncServer implements `acme.shipping.v1.ManifestSyncService`.
// Keeps manifests in step with the systems of record in other regions.
type ManifestSyncServer struct {
	shippingv1.UnimplementedManifestSyncServiceServer
	store                          *Store
	log                            *slog.Logger
	loyaltyMemberApprovalClient    loyaltyv1.LoyaltyMemberApprovalServiceClient
	preferenceCalculationClient    customersv1.PreferenceCalculationServiceClient
	shipmentClient                 shippingv1.ShipmentServiceClient
	manifestCalculationClient      shippingv1.ManifestCalculationServiceClient
	manifestValidationClient       shippingv1.ManifestValidationServiceClient
	reviewVoteWorkflowClient       reviewsv1.ReviewVoteWorkflowServiceClient
	shipmentValidationClient       shippingv1.ShipmentValidationServiceClient
	documentClient                 documentsv1.DocumentServiceClient
	conn                           *grpc.ClientConn
	userClient                     identityv1.UserServiceClient
	consentRecordCalculationClient customersv1.ConsentRecordCalculationServiceClient
}

// NewManifestSyncServer wires a ManifestSyncServer.
func NewManifestSyncServer(deps platform.Deps) *ManifestSyncServer {
	return &ManifestSyncServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.shipping.v1.ManifestSyncService"),
		loyaltyMemberApprovalClient:    loyaltyv1.NewLoyaltyMemberApprovalServiceClient(deps.Conn),
		preferenceCalculationClient:    customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		shipmentClient:                 shippingv1.NewShipmentServiceClient(deps.Conn),
		manifestCalculationClient:      shippingv1.NewManifestCalculationServiceClient(deps.Conn),
		manifestValidationClient:       shippingv1.NewManifestValidationServiceClient(deps.Conn),
		reviewVoteWorkflowClient:       reviewsv1.NewReviewVoteWorkflowServiceClient(deps.Conn),
		shipmentValidationClient:       shippingv1.NewShipmentValidationServiceClient(deps.Conn),
		documentClient:                 documentsv1.NewDocumentServiceClient(deps.Conn),
		conn:                           deps.Conn,
		userClient:                     identityv1.NewUserServiceClient(deps.Conn),
		consentRecordCalculationClient: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
	}
}

// SyncManifests syncs a manifest.
func (s *ManifestSyncServer) SyncManifests(ctx context.Context, req *shippingv1.SyncManifestsRequest) (*shippingv1.SyncManifestsResponse, error) {
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "sync manifests", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	getManifestRulesRequest := &shippingv1.GetManifestRulesRequest{
		Context:    req.GetContext(),
		ManifestId: req.GetManifest().GetManifestId(),
	}
	getManifestRulesReply, err := getManifestRulesCall(ctx, getManifestRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestValidationService/GetManifestRules")
	}
	for _, line := range req.GetManifest().GetLines() {
		if _, err := s.loyaltyMemberApprovalClient.EscalateLoyaltyMember(ctx, &loyaltyv1.EscalateLoyaltyMemberRequest{
			Context: req.GetContext(),
			LoyaltyMember: &loyaltyv1.LoyaltyMember{
				Limits: []*loyaltyv1.LoyaltyMemberLimit{&loyaltyv1.LoyaltyMemberLimit{Amount: line.GetTax().GetAmount()}},
				Audit: &commonv1.AuditInfo{
					CreatedBy: getManifestRulesReply.GetManifest().GetAudit().GetCreatedBy(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberApprovalService/EscalateLoyaltyMember")
		}
	}
	cloned := proto.Clone(req.GetManifest()).(*shippingv1.Manifest)
	listDocumentsRequest := &documentsv1.ListDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.ListDocumentsRequest_Filter{
			Region:  cloned.GetBillingAddress().GetRegion(),
			OwnerId: getManifestRulesReply.GetManifest().GetCounterparty().GetCustomerId(),
		},
	}
	listDocumentsReply, err := listDocumentsCall(ctx, listDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/ListDocuments")
	}
	quotePreferenceRequest := &customersv1.QuotePreferenceRequest{}
	quotePreferenceRequest.Context = req.GetContext()
	quotePreferenceRequest.Details = &customersv1.QuotePreferenceRequest_Details{
		Destination: &commonv1.PostalAddress{
			Locality:   getManifestRulesReply.GetManifest().GetBillingAddress().GetLocality(),
			PostalCode: getManifestRulesReply.GetManifest().GetBillingAddress().GetPostalCode(),
		},
	}
	quotePreferenceRequest.Preference = &customersv1.Preference{
		Audit: &commonv1.AuditInfo{UpdatedAt: req.GetManifest().GetAudit().GetUpdatedAt()},
	}
	quotePreferenceReply, err := s.preferenceCalculationClient.QuotePreference(ctx, quotePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/QuotePreference")
	}
	return &shippingv1.SyncManifestsResponse{
		Outcome: &shippingv1.SyncManifestsResponse_Outcome{Status: getManifestRulesReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Status:       listDocumentsReply.GetResult().GetStatus(),
			ReferenceIds: listDocumentsReply.GetResult().GetReferenceIds(),
		},
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{
				Etag:      quotePreferenceReply.GetPreference().GetAudit().GetEtag(),
				CreatedBy: quotePreferenceReply.GetPreference().GetAudit().GetCreatedBy(),
			},
			ManifestId: req.GetManifestId(),
			Metadata:   req.GetManifest().GetMetadata(),
		},
	}, nil
}

// PushManifest pushs a manifest.
func (s *ManifestSyncServer) PushManifest(ctx context.Context, req *shippingv1.PushManifestRequest) (*shippingv1.PushManifestResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "push manifest", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*shippingv1.ListShipmentsResponse, len(req.GetManifest().GetLines()))
	for i, line := range req.GetManifest().GetLines() {
		group.Go(func() error {
			reply, err := s.shipmentClient.ListShipments(groupCtx, &shippingv1.ListShipmentsRequest{
				Context: req.GetContext(),
				Filter:  &shippingv1.ListShipmentsRequest_Filter{OwnerId: line.GetLineId()},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentService/ListShipments")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	for attachment := range iterx.Each(req.GetManifest().GetAttachments(), func(candidate *shippingv1.ManifestAttachment) bool { return candidate != nil }) {
		if _, err := s.manifestCalculationClient.EstimateManifest(ctx, &shippingv1.EstimateManifestRequest{
			Context: req.GetContext(),
			Manifest: &shippingv1.Manifest{
				Attachments: []*shippingv1.ManifestAttachment{&shippingv1.ManifestAttachment{
					Checksum: attachment.GetChecksum(),
					FileName: attachment.GetFileName(),
					Uri:      attachment.GetUri(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/EstimateManifest")
		}
	}
	ctx = tenancy.With(ctx, "manifest_id", req.GetManifest().GetManifestId())
	step := &manifestSyncServerPushManifestGetManifestRulesStep{request: &shippingv1.GetManifestRulesRequest{
		Context:    req.GetContext(),
		ManifestId: tenancy.Value(ctx, "manifest_id"),
	}}
	if err := s.runPushManifestGetManifestRules(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestValidationService/GetManifestRules")
	}
	getManifestRulesReply := step.reply
	pauseReviewVoteWorkflowReply, err := s.reviewVoteWorkflowClient.PauseReviewVoteWorkflow(ctx, &reviewsv1.PauseReviewVoteWorkflowRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteWorkflowService/PauseReviewVoteWorkflow")
	}
	return &shippingv1.PushManifestResponse{
		Outcome: &shippingv1.PushManifestResponse_Outcome{Status: getManifestRulesReply.GetResult().GetStatus()},
		Manifest: &shippingv1.Manifest{
			BillingAddress: &commonv1.PostalAddress{
				Locality: getManifestRulesReply.GetManifest().GetBillingAddress().GetLocality(),
			},
			ManifestId: req.GetManifestId(),
			Status:     req.GetManifest().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Status: pauseReviewVoteWorkflowReply.GetResult().GetStatus(),
		},
	}, nil
}

// PullManifests pulls a manifest.
func (s *ManifestSyncServer) PullManifests(ctx context.Context, req *shippingv1.PullManifestsRequest) (*shippingv1.PullManifestsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PullManifests", "error", err)
		return nil, err
	}
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "pull manifests", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	previewShipmentReply, err := s.shipmentValidationClient.PreviewShipment(ctx, &shippingv1.PreviewShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: req.GetManifest().GetBillingAddress().GetRecipient(),
				Region:    req.GetManifest().GetBillingAddress().GetRegion(),
			},
			Settlement: &shippingv1.Shipment_Card{Card: &shippingv1.ShipmentCardSettlement{Network: req.GetManifest().GetCard().GetNetwork()}},
			Audit:      &commonv1.AuditInfo{CreatedAt: req.GetManifest().GetAudit().GetCreatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/PreviewShipment")
	}
	cloned := proto.Clone(req.GetManifest()).(*shippingv1.Manifest)
	watchDocumentsStream, err := s.documentClient.WatchDocuments(ctx, &documentsv1.WatchDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.WatchDocumentsRequest_Filter{
			Region: previewShipmentReply.GetShipment().GetBillingAddress().GetRegion(),
		},
		Subscriber: &commonv1.ContactInfo{Email: cloned.GetCounterparty().GetEmail()},
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
	return &shippingv1.PullManifestsResponse{
		Outcome: &shippingv1.PullManifestsResponse_Outcome{Status: previewShipmentReply.GetResult().GetStatus()},
		Manifest: &shippingv1.Manifest{
			ManifestId: req.GetManifestId(),
			DueAt:      req.GetManifest().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ReconcileManifests reconciles a manifest.
func (s *ManifestSyncServer) ReconcileManifests(ctx context.Context, req *shippingv1.ReconcileManifestsRequest) (*shippingv1.ReconcileManifestsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "reconcile manifests", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	recallInspectionRequest := &returnsv1.RecallInspectionRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetManifest().GetAudit().GetUpdatedAt(),
				Etag:      req.GetManifest().GetAudit().GetEtag(),
				CreatedBy: req.GetManifest().GetAudit().GetCreatedBy(),
			},
		},
	}
	recallInspectionReply, err := grpcx.NewClient(s.conn, returnsv1.NewInspectionApprovalServiceClient).RecallInspection(ctx, recallInspectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionApprovalService/RecallInspection")
	}
	calculateManifestRequest := &shippingv1.CalculateManifestRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{
				CreatedBy: recallInspectionReply.GetInspection().GetAudit().GetCreatedBy(),
			},
		},
	}
	calculateManifestReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*shippingv1.CalculateManifestResponse, error) {
		return s.manifestCalculationClient.CalculateManifest(ctx, calculateManifestRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/CalculateManifest")
	}
	return &shippingv1.ReconcileManifestsResponse{
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{
				CreatedAt: recallInspectionReply.GetInspection().GetAudit().GetCreatedAt(),
				CreatedBy: calculateManifestReply.GetManifest().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: calculateManifestReply.GetManifest().GetBillingAddress().GetRecipient(),
			},
			ManifestId: req.GetManifestId(),
			IssuedAt:   req.GetManifest().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResolveManifestConflict resolves a manifest.
func (s *ManifestSyncServer) ResolveManifestConflict(ctx context.Context, req *shippingv1.ResolveManifestConflictRequest) (*shippingv1.ResolveManifestConflictResponse, error) {
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "resolve manifest conflict", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetManifest()).(*shippingv1.Manifest)
	searchUsersRequest := &identityv1.SearchUsersRequest{}
	searchUsersRequest.Context = req.GetContext()
	searchUsersRequest.Filter = &identityv1.SearchUsersRequest_Filter{
		Region:       req.GetManifest().GetBillingAddress().GetRegion(),
		CreatedAfter: cloned.GetAudit().GetCreatedAt(),
		Labels:       req.GetManifest().GetMetadata(),
		OwnerId:      req.GetManifest().GetManifestId(),
	}
	searchUsersReply, err := s.userClient.SearchUsers(ctx, searchUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/SearchUsers")
	}
	recalculateConsentRecordsRequest := &customersv1.RecalculateConsentRecordsRequest{
		Context: req.GetContext(),
		Details: &customersv1.RecalculateConsentRecordsRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetManifest().GetBillingAddress().GetPostalCode(),
				Locality:   req.GetManifest().GetBillingAddress().GetLocality(),
			},
		},
	}
	recalculateConsentRecordsReply, err := grpcx.Call(ctx, s.consentRecordCalculationClient.RecalculateConsentRecords, recalculateConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords")
	}
	return &shippingv1.ResolveManifestConflictResponse{
		Result: &commonv1.OperationResult{
			Status:  searchUsersReply.GetResult().GetStatus(),
			Details: recalculateConsentRecordsReply.GetResult().GetDetails(),
		},
		Manifest: &shippingv1.Manifest{
			ManifestId:   req.GetManifestId(),
			Counterparty: req.GetManifest().GetCounterparty(),
		},
	}, nil
}

// GetManifestSyncState returns a manifest.
func (s *ManifestSyncServer) GetManifestSyncState(ctx context.Context, req *shippingv1.GetManifestSyncStateRequest) (*shippingv1.GetManifestSyncStateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "manifest", req.GetManifestId(), "get_manifest_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get manifest sync state", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.GetManifestSyncStateResponse{
		Manifest: &shippingv1.Manifest{ManifestId: req.GetManifestId()},
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListManifestConflicts lists manifests a page at a time.
func (s *ManifestSyncServer) ListManifestConflicts(ctx context.Context, req *shippingv1.ListManifestConflictsRequest) (*shippingv1.ListManifestConflictsResponse, error) {
	s.log.InfoContext(ctx, "list manifest conflicts", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.ListManifestConflictsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// manifestSyncServerPushManifestGetManifestRulesStep carries the request of one step of PushManifest and, once it ran,
// its reply.
type manifestSyncServerPushManifestGetManifestRulesStep struct {
	request *shippingv1.GetManifestRulesRequest
	reply   *shippingv1.GetManifestRulesResponse
}

func (s *ManifestSyncServer) runPushManifestGetManifestRules(ctx context.Context, step *manifestSyncServerPushManifestGetManifestRulesStep) error {
	reply, err := s.manifestValidationClient.GetManifestRules(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
