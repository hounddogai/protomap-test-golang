package customers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	chatv1 "github.com/hounddogai/protomap-test-golang/gen/acme/chat/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	mediav1 "github.com/hounddogai/protomap-test-golang/gen/acme/media/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	patientsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patients/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// CustomerSegmentSyncServer implements `acme.customers.v1.CustomerSegmentSyncService`.
// Keeps customer segments in step with the systems of record in other regions.
type CustomerSegmentSyncServer struct {
	customersv1.UnimplementedCustomerSegmentSyncServiceServer
	store                         *Store
	log                           *slog.Logger
	chatRoomClient                chatv1.ChatRoomServiceClient
	conn                          *grpc.ClientConn
	consentRecordSyncClient       customersv1.ConsentRecordSyncServiceClient
	clients                       *registry.Registry
	sellerScorecardApprovalClient sellersv1.SellerScorecardApprovalServiceClient
	mediaAssetClient              mediav1.MediaAssetServiceClient
	redemptionApprovalClient      promotionsv1.RedemptionApprovalServiceClient
	consentRecordValidationClient customersv1.ConsentRecordValidationServiceClient
	documentValidationClient      documentsv1.DocumentValidationServiceClient
	cartWorkflowClient            cartv1.CartWorkflowServiceClient
	patientClient                 patientsv1.PatientServiceClient
	meteredHouseholdValidation    meteredHouseholdValidation
	metricSeriesValidationClient  metricsv1.MetricSeriesValidationServiceClient
	householdQueryPort            householdQueryPort
	meteredCustomerQuery          meteredCustomerQuery
}

// NewCustomerSegmentSyncServer wires a CustomerSegmentSyncServer.
func NewCustomerSegmentSyncServer(deps platform.Deps) *CustomerSegmentSyncServer {
	return &CustomerSegmentSyncServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.customers.v1.CustomerSegmentSyncService"),
		chatRoomClient:                chatv1.NewChatRoomServiceClient(deps.Conn),
		conn:                          deps.Conn,
		consentRecordSyncClient:       customersv1.NewConsentRecordSyncServiceClient(deps.Conn),
		clients:                       deps.Clients,
		sellerScorecardApprovalClient: sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		mediaAssetClient:              mediav1.NewMediaAssetServiceClient(deps.Conn),
		redemptionApprovalClient:      promotionsv1.NewRedemptionApprovalServiceClient(deps.Conn),
		consentRecordValidationClient: customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		documentValidationClient:      documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		cartWorkflowClient:            cartv1.NewCartWorkflowServiceClient(deps.Conn),
		patientClient:                 patientsv1.NewPatientServiceClient(deps.Conn),
		meteredHouseholdValidation:    meteredHouseholdValidation{HouseholdValidationServiceClient: customersv1.NewHouseholdValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		metricSeriesValidationClient:  metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		householdQueryPort:            newHouseholdQueryPort(deps),
		meteredCustomerQuery:          meteredCustomerQuery{CustomerQueryServiceClient: customersv1.NewCustomerQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// SyncCustomerSegments syncs a customer segment.
func (s *CustomerSegmentSyncServer) SyncCustomerSegments(ctx context.Context, req *customersv1.SyncCustomerSegmentsRequest) (*customersv1.SyncCustomerSegmentsResponse, error) {
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "sync customer segments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetCustomerSegment()).(*customersv1.CustomerSegment)
	createChatRoomReply, err := s.chatRoomClient.CreateChatRoom(ctx, &chatv1.CreateChatRoomRequest{
		Context: req.GetContext(),
		ChatRoom: &chatv1.ChatRoom{
			Owner: &identityv1.UserRef{Email: cloned.GetOwner().GetEmail()},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetCustomerSegment().GetAudit().GetCreatedAt(),
				UpdatedAt: req.GetCustomerSegment().GetAudit().GetUpdatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.chat.v1.ChatRoomService/CreateChatRoom")
	}
	normalizeDocumentRequest := &documentsv1.NormalizeDocumentRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			Lines: []*documentsv1.DocumentLine{&documentsv1.DocumentLine{
				Tax: &documentsv1.DocumentLineTax{Amount: req.GetCustomerSegment().GetDiscount().GetAmount()},
			}},
			Counterparty: &documentsv1.DocumentFolderRef{
				DisplayName: req.GetCustomerSegment().GetOwner().GetDisplayName(),
			},
		},
	}
	normalizeDocumentReply, err := grpcx.NewClient(s.conn, documentsv1.NewDocumentValidationServiceClient).NormalizeDocument(ctx, normalizeDocumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/NormalizeDocument")
	}
	return &customersv1.SyncCustomerSegmentsResponse{
		Result: &commonv1.OperationResult{
			Details:       createChatRoomReply.GetResult().GetDetails(),
			CorrelationId: createChatRoomReply.GetResult().GetCorrelationId(),
			ReferenceIds:  normalizeDocumentReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		CustomerSegment: &customersv1.CustomerSegment{
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Effective:         req.GetCustomerSegment().GetEffective(),
		},
	}, nil
}

// PushCustomerSegment pushs a customer segment.
func (s *CustomerSegmentSyncServer) PushCustomerSegment(ctx context.Context, req *customersv1.PushCustomerSegmentRequest) (*customersv1.PushCustomerSegmentResponse, error) {
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "push customer segment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "checksum", req.GetDetails().GetChecksum())
	cloned := proto.Clone(req.GetCustomerSegment()).(*customersv1.CustomerSegment)
	snapshot, err := protojson.Marshal(req.GetCustomerSegment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveConsentRecordConflictRequest := &customersv1.ResolveConsentRecordConflictRequest{
		Context: req.GetContext(),
		Reason:  req.GetCustomerSegment().GetBlock().GetReason(),
		Details: &customersv1.ResolveConsentRecordConflictRequest_Details{
			PeerRegion: req.GetDetails().GetPeerRegion(),
			Checksum:   tenancy.Value(ctx, "checksum"),
			Payload:    snapshot,
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{DisplayName: cloned.GetOwner().GetDisplayName()},
		},
	}
	send := s.resolveConsentRecordConflictSender(resolveConsentRecordConflictRequest)
	resolveConsentRecordConflictReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/ResolveConsentRecordConflict")
	}
	mediaAsset, err := registry.Lookup[mediav1.MediaAssetServiceClient](s.clients, "acme.media.v1.MediaAssetService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	createMediaAssetRequest := &mediav1.CreateMediaAssetRequest{
		Context: req.GetContext(),
		MediaAsset: &mediav1.MediaAsset{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCustomerSegment().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{Email: req.GetCustomerSegment().GetOwner().GetEmail()},
		},
	}
	createMediaAssetReply, err := mediaAsset.CreateMediaAsset(ctx, createMediaAssetRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetService/CreateMediaAsset")
	}
	return &customersv1.PushCustomerSegmentResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  resolveConsentRecordConflictReply.GetResult().GetReferenceIds(),
			CorrelationId: resolveConsentRecordConflictReply.GetConsentRecord().GetCorrelationId(),
			Status:        createMediaAssetReply.GetResult().GetStatus(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				Etag: createMediaAssetReply.GetMediaAsset().GetAudit().GetEtag(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Version:           req.GetCustomerSegment().GetVersion(),
		},
	}, nil
}

// PullCustomerSegments pulls a customer segment.
func (s *CustomerSegmentSyncServer) PullCustomerSegments(ctx context.Context, req *customersv1.PullCustomerSegmentsRequest) (*customersv1.PullCustomerSegmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "pull customer segments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	customerSegmentCache := cache.New[string, *customersv1.CustomerSegment](1)
	customerSegmentCache.Put(req.GetContext().GetTraceId(), req.GetCustomerSegment())
	cachedCustomerSegment, _ := customerSegmentCache.Get(req.GetContext().GetTraceId())
	listPendingSellerScorecardsRequest := &sellersv1.ListPendingSellerScorecardsRequest{}
	listPendingSellerScorecardsRequest.Context = req.GetContext()
	listPendingSellerScorecardsRequest.Filter = &sellersv1.ListPendingSellerScorecardsRequest_Filter{
		CreatedAfter: req.GetCustomerSegment().GetAudit().GetUpdatedAt(),
		OwnerId:      cachedCustomerSegment.GetCustomerSegmentId(),
	}
	listPendingSellerScorecardsReply, err := s.sellerScorecardApprovalClient.ListPendingSellerScorecards(ctx, listPendingSellerScorecardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/ListPendingSellerScorecards")
	}
	cloned := proto.Clone(req.GetCustomerSegment()).(*customersv1.CustomerSegment)
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	searchMediaAssetsRequest := &mediav1.SearchMediaAssetsRequest{}
	searchMediaAssetsRequest.Context = req.GetContext()
	searchMediaAssetsRequest.Filter = &mediav1.SearchMediaAssetsRequest_Filter{
		CreatedAfter: req.GetCustomerSegment().GetAudit().GetCreatedAt(),
		OwnerId:      cloned.GetOwner().GetUserId(),
		Labels:       annotations,
	}
	searchMediaAssetsReply, err := s.mediaAssetClient.SearchMediaAssets(ctx, searchMediaAssetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetService/SearchMediaAssets")
	}
	cloned2 := proto.Clone(req.GetCustomerSegment()).(*customersv1.CustomerSegment)
	var notifyEmail string
	switch choice := req.GetCustomerSegment().GetAction().(type) {
	case *customersv1.CustomerSegment_Route:
		notifyEmail = choice.Route.GetTarget()
	case *customersv1.CustomerSegment_Block:
		notifyEmail = choice.Block.GetNotifyEmail()
	}
	approveRedemptionRequest := &promotionsv1.ApproveRedemptionRequest{}
	approveRedemptionRequest.Context = req.GetContext()
	approveRedemptionRequest.Redemption = &promotionsv1.Redemption{
		Subject: &promotionsv1.CouponRef{
			DisplayName: req.GetCustomerSegment().GetOwner().GetDisplayName(),
		},
	}
	approveRedemptionRequest.Details = &promotionsv1.ApproveRedemptionRequest_Details{
		Approver: &identityv1.UserRef{
			UserId: req.GetCustomerSegment().GetOwner().GetUserId(),
			Email:  cloned2.GetOwner().GetEmail(),
		},
		ApproverEmail: notifyEmail,
	}
	approveRedemptionReply, err := s.redemptionApprovalClient.ApproveRedemption(ctx, approveRedemptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionApprovalService/ApproveRedemption")
	}
	_ = searchMediaAssetsReply
	return &customersv1.PullCustomerSegmentsResponse{
		Result: &commonv1.OperationResult{
			Status:        listPendingSellerScorecardsReply.GetResult().GetStatus(),
			CorrelationId: approveRedemptionReply.GetResult().GetCorrelationId(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Version:           req.GetCustomerSegment().GetVersion(),
		},
	}, nil
}

// ReconcileCustomerSegments reconciles a customer segment.
func (s *CustomerSegmentSyncServer) ReconcileCustomerSegments(ctx context.Context, req *customersv1.ReconcileCustomerSegmentsRequest) (*customersv1.ReconcileCustomerSegmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "reconcile customer segments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetCustomerSegment().GetOwner().GetDisplayName())
	advanceCategoryWorkflowRequest := &catalogv1.AdvanceCategoryWorkflowRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: req.GetCustomerSegment().GetAudit().GetUpdatedBy(),
					},
					Manager: &identityv1.UserRef{
						Email:       req.GetCustomerSegment().GetOwner().GetEmail(),
						DisplayName: tenancy.Value(ctx, "display_name"),
					},
				}},
				Audit: &commonv1.AuditInfo{Etag: req.GetCustomerSegment().GetAudit().GetEtag()},
			}},
		},
	}
	advanceCategoryWorkflowReply := new(catalogv1.AdvanceCategoryWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", catalogCategoryWorkflowServiceName, "AdvanceCategoryWorkflow"), advanceCategoryWorkflowRequest, advanceCategoryWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryWorkflowService/AdvanceCategoryWorkflow")
	}
	packed, err := anypb.New(req.GetCustomerSegment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizeConsentRecordRequest := newNormalizeConsentRecordRequest(withNormalizeConsentRecordContext(req.GetContext()), withNormalizeConsentRecordConsentRecord(&customersv1.ConsentRecord{
		Subject: &customersv1.HouseholdRef{
			DisplayName: advanceCategoryWorkflowReply.GetCategory().GetManager().GetDisplayName(),
		},
		CorrelationId: advanceCategoryWorkflowReply.GetResult().GetCorrelationId(),
		Detail:        packed,
	}), withNormalizeConsentRecordDetails(&customersv1.NormalizeConsentRecordRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetCustomerSegment().GetOwner().GetEmail()},
	}))
	normalizeConsentRecordReply, err := s.consentRecordValidationClient.NormalizeConsentRecord(ctx, normalizeConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord")
	}
	return &customersv1.ReconcileCustomerSegmentsResponse{
		Result: &commonv1.OperationResult{
			Message: advanceCategoryWorkflowReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Outcome: &customersv1.ReconcileCustomerSegmentsResponse_Outcome{
			ReferenceId: normalizeConsentRecordReply.GetOutcome().GetReferenceId(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Version:           req.GetCustomerSegment().GetVersion(),
		},
	}, nil
}

// StreamCustomerSegmentChanges exchanges changes of customer segments with a peer.
func (s *CustomerSegmentSyncServer) StreamCustomerSegmentChanges(stream grpc.BidiStreamingServer[customersv1.StreamCustomerSegmentChangesRequest, customersv1.StreamCustomerSegmentChangesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *customersv1.StreamCustomerSegmentChangesRequest)
	failed := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			received, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					failed <- err
				}
				return
			}
			select {
			case incoming <- received:
			case <-ctx.Done():
				return
			}
		}
	}()
	for msg := range incoming {
		s.log.InfoContext(ctx, "stream customer segment changes", "tenant", msg.GetContext().GetTenantId())
		checkDocumentEligibilityRequest := &documentsv1.CheckDocumentEligibilityRequest{}
		checkDocumentEligibilityRequest.Context = msg.GetContext()
		checkDocumentEligibilityRequest.Details = &documentsv1.CheckDocumentEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: msg.GetChange().GetOwner().GetEmail()},
		}
		checkDocumentEligibilityRequest.Document = &documentsv1.Document{
			Audit: &commonv1.AuditInfo{UpdatedAt: msg.GetChange().GetAudit().GetUpdatedAt()},
			Lines: []*documentsv1.DocumentLine{&documentsv1.DocumentLine{
				Amount:      msg.GetChange().GetDiscount().GetAmount(),
				Description: msg.GetChange().GetDescription(),
			}},
		}
		if _, err := s.documentValidationClient.CheckDocumentEligibility(ctx, checkDocumentEligibilityRequest); err != nil {
			return grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/CheckDocumentEligibility")
		}
		retryCartWorkflowRequest := &cartv1.RetryCartWorkflowRequest{Context: msg.GetContext()}
		if _, err := cartv1.CartWorkflowServiceClient.RetryCartWorkflow(s.cartWorkflowClient, ctx, retryCartWorkflowRequest); err != nil {
			return grpcx.Wrap(err, "acme.cart.v1.CartWorkflowService/RetryCartWorkflow")
		}
		if _, err := s.patientClient.SearchPatients(ctx, &patientsv1.SearchPatientsRequest{Context: msg.GetContext()}); err != nil {
			return grpcx.Wrap(err, "acme.patients.v1.PatientService/SearchPatients")
		}
		if err := stream.Send(&customersv1.StreamCustomerSegmentChangesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
			return err
		}
	}
	select {
	case err := <-failed:
		return err
	default:
		return nil
	}
}

// ResolveCustomerSegmentConflict resolves a customer segment.
func (s *CustomerSegmentSyncServer) ResolveCustomerSegmentConflict(ctx context.Context, req *customersv1.ResolveCustomerSegmentConflictRequest) (*customersv1.ResolveCustomerSegmentConflictResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResolveCustomerSegmentConflict", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "resolve customer segment conflict", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	source := req.GetCustomerSegment().GetAudit().ProtoReflect()
	updatedBy := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("updated_by"))).String()
	retryPreferenceImportRequest := &customersv1.RetryPreferenceImportRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit:  &commonv1.AuditInfo{UpdatedBy: updatedBy},
			Action: &customersv1.Preference_Route{Route: &customersv1.PreferenceRouteAction{Weight: req.GetCustomerSegment().GetRoute().GetWeight()}},
		},
	}
	retryPreferenceImportReply := new(customersv1.RetryPreferenceImportResponse)
	if err := s.conn.Invoke(ctx, customersPreferenceImportServiceRetryPreferenceImportMethod, retryPreferenceImportRequest, retryPreferenceImportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceImportService/RetryPreferenceImport")
	}
	normalizeHouseholdRequest := &customersv1.NormalizeHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						Email: retryPreferenceImportReply.GetPreference().GetOwner().GetEmail(),
					},
					Audit: &commonv1.AuditInfo{
						UpdatedBy: req.GetCustomerSegment().GetAudit().GetUpdatedBy(),
						Etag:      retryPreferenceImportReply.GetPreference().GetAudit().GetEtag(),
						UpdatedAt: retryPreferenceImportReply.GetPreference().GetAudit().GetUpdatedAt(),
					},
				}},
			}},
		},
	}
	normalizeHouseholdReply, err := s.meteredHouseholdValidation.NormalizeHousehold(ctx, normalizeHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/NormalizeHousehold")
	}
	source2 := req.GetCustomerSegment().GetAudit().ProtoReflect()
	updatedBy2 := source2.Get(source2.Descriptor().Fields().ByName(protoreflect.Name("updated_by"))).String()
	cloned := proto.Clone(req.GetCustomerSegment()).(*customersv1.CustomerSegment)
	normalizeMetricSeriesRequest := &metricsv1.NormalizeMetricSeriesRequest{}
	normalizeMetricSeriesRequest.Context = req.GetContext()
	normalizeMetricSeriesRequest.MetricSeries = &metricsv1.MetricSeries{
		Audit: &commonv1.AuditInfo{
			UpdatedBy: updatedBy2,
			UpdatedAt: cloned.GetAudit().GetUpdatedAt(),
		},
		Locator: &metricsv1.MetricSeries_Address{Address: &commonv1.PostalAddress{
			PostalCode: normalizeHouseholdReply.GetHousehold().GetLocation().GetPostalCode(),
			Locality:   normalizeHouseholdReply.GetHousehold().GetLocation().GetLocality(),
		}},
	}
	normalizeMetricSeriesReply, err := s.metricSeriesValidationClient.NormalizeMetricSeries(ctx, normalizeMetricSeriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/NormalizeMetricSeries")
	}
	return &customersv1.ResolveCustomerSegmentConflictResponse{
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				Etag:      retryPreferenceImportReply.GetPreference().GetAudit().GetEtag(),
				CreatedAt: normalizeMetricSeriesReply.GetMetricSeries().GetAudit().GetCreatedAt(),
			},
			Version:           retryPreferenceImportReply.GetPreference().GetVersion(),
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Parameters:        req.GetCustomerSegment().GetParameters(),
		},
		Result: &commonv1.OperationResult{
			Message: normalizeHouseholdReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetCustomerSegmentSyncState returns a customer segment.
func (s *CustomerSegmentSyncServer) GetCustomerSegmentSyncState(ctx context.Context, req *customersv1.GetCustomerSegmentSyncStateRequest) (*customersv1.GetCustomerSegmentSyncStateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "get_customer_segment_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get customer segment sync state", "tenant", req.GetContext().GetTenantId())
	findHouseholdsRequest := &customersv1.FindHouseholdsRequest{
		Context: req.GetContext(),
		Filter:  &customersv1.FindHouseholdsRequest_Filter{CreatedAfter: req.GetContext().GetDeadline()},
	}
	findHouseholdsReply, err := s.householdQueryPort.FindHouseholds(ctx, findHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/FindHouseholds")
	}
	return &customersv1.GetCustomerSegmentSyncStateResponse{
		Result: &commonv1.OperationResult{
			Message: findHouseholdsReply.GetResult().GetMessage(),
			Details: findHouseholdsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		CustomerSegment: &customersv1.CustomerSegment{CustomerSegmentId: req.GetCustomerSegmentId()},
	}, nil
}

// ListCustomerSegmentConflicts lists customer segments a page at a time.
func (s *CustomerSegmentSyncServer) ListCustomerSegmentConflicts(ctx context.Context, req *customersv1.ListCustomerSegmentConflictsRequest) (*customersv1.ListCustomerSegmentConflictsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list customer segment conflicts", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizeCustomersRequest := &customersv1.SummarizeCustomersRequest{
		Context:     req.GetContext(),
		Details:     &customersv1.SummarizeCustomersRequest_Details{Filters: req.GetFilter().GetLabels()},
		Annotations: baggage,
	}
	summarizeCustomersReply, err := s.meteredCustomerQuery.SummarizeCustomers(ctx, summarizeCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SummarizeCustomers")
	}
	return &customersv1.ListCustomerSegmentConflictsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeCustomersReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// resolveConsentRecordConflictSender returns a function that sends one request to
// acme.customers.v1.ConsentRecordSyncService/ResolveConsentRecordConflict.
func (s *CustomerSegmentSyncServer) resolveConsentRecordConflictSender(request *customersv1.ResolveConsentRecordConflictRequest) func(context.Context) (*customersv1.ResolveConsentRecordConflictResponse, error) {
	return func(ctx context.Context) (*customersv1.ResolveConsentRecordConflictResponse, error) {
		return s.consentRecordSyncClient.ResolveConsentRecordConflict(ctx, request)
	}
}
