package customers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// ConsentRecordSyncServer implements `acme.customers.v1.ConsentRecordSyncService`.
// Keeps consent records in step with the systems of record in other regions.
type ConsentRecordSyncServer struct {
	customersv1.UnimplementedConsentRecordSyncServiceServer
	store                           *Store
	log                             *slog.Logger
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	customerQueryClient             customersv1.CustomerQueryServiceClient
	conn                            *grpc.ClientConn
	householdCalculationClient      customersv1.HouseholdCalculationServiceClient
	bus                             *eventbus.Bus
	sessionValidationClient         identityv1.SessionValidationServiceClient
	customerSegmentImportClient     customersv1.CustomerSegmentImportServiceClient
	paymentInstrumentSyncClient     paymentsv1.PaymentInstrumentSyncServiceClient
	customerClient                  customersv1.CustomerServiceClient
	synonymExportClient             searchv1.SynonymExportServiceClient
	userClient                      identityv1.UserServiceClient
	clients                         *registry.Registry
	consentRecordValidationClient   customersv1.ConsentRecordValidationServiceClient
	merchandisingRuleApprovalClient merchandisingv1.MerchandisingRuleApprovalServiceClient
	promotionReportingClient        promotionsv1.PromotionReportingServiceClient
}

// NewConsentRecordSyncServer wires a ConsentRecordSyncServer.
func NewConsentRecordSyncServer(deps platform.Deps) *ConsentRecordSyncServer {
	return &ConsentRecordSyncServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.customers.v1.ConsentRecordSyncService"),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		customerQueryClient:             customersv1.NewCustomerQueryServiceClient(deps.Conn),
		conn:                            deps.Conn,
		householdCalculationClient:      customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		bus:                             deps.Bus,
		sessionValidationClient:         identityv1.NewSessionValidationServiceClient(deps.Conn),
		customerSegmentImportClient:     customersv1.NewCustomerSegmentImportServiceClient(deps.Conn),
		paymentInstrumentSyncClient:     paymentsv1.NewPaymentInstrumentSyncServiceClient(deps.Conn),
		customerClient:                  customersv1.NewCustomerServiceClient(deps.Conn),
		synonymExportClient:             searchv1.NewSynonymExportServiceClient(deps.Conn),
		userClient:                      identityv1.NewUserServiceClient(deps.Conn),
		clients:                         deps.Clients,
		consentRecordValidationClient:   customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		merchandisingRuleApprovalClient: merchandisingv1.NewMerchandisingRuleApprovalServiceClient(deps.Conn),
		promotionReportingClient:        promotionsv1.NewPromotionReportingServiceClient(deps.Conn),
	}
}

// SyncConsentRecords syncs a consent record.
func (s *ConsentRecordSyncServer) SyncConsentRecords(ctx context.Context, req *customersv1.SyncConsentRecordsRequest) (*customersv1.SyncConsentRecordsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "sync consent records", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	previewCustomerSegmentReply, err := s.customerSegmentValidationClient.PreviewCustomerSegment(ctx, &customersv1.PreviewCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{Reason: req.GetReason()}},
		},
		Details: &customersv1.PreviewCustomerSegmentRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/PreviewCustomerSegment")
	}
	return &customersv1.SyncConsentRecordsResponse{
		Outcome: &customersv1.SyncConsentRecordsResponse_Outcome{
			Message: previewCustomerSegmentReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: previewCustomerSegmentReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			Source:          req.GetConsentRecord().GetSource(),
		},
	}, nil
}

// PushConsentRecord pushs a consent record.
func (s *ConsentRecordSyncServer) PushConsentRecord(ctx context.Context, req *customersv1.PushConsentRecordRequest) (*customersv1.PushConsentRecordResponse, error) {
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "push consent record", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*customersv1.SuggestCustomersResponse, len(req.GetConsentRecord().GetMeasurements()))
	for i, measurement := range req.GetConsentRecord().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.customerQueryClient.SuggestCustomers(groupCtx, &customersv1.SuggestCustomersRequest{
				Context: req.GetContext(),
				Filter: &customersv1.SuggestCustomersRequest_Filter{
					CreatedAfter: measurement.GetObservedAt(),
					OwnerId:      req.GetConsentRecord().GetConsentRecordId(),
					Labels:       req.GetConsentRecord().GetAttributes(),
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SuggestCustomers")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	searchUsersRequest := &identityv1.SearchUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.SearchUsersRequest_Filter{
			CreatedAfter: req.GetConsentRecord().GetOccurredAt(),
			OwnerId:      req.GetConsentRecord().GetCorrelationId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	searchUsersReply := new(identityv1.SearchUsersResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", identityUserServiceName, "SearchUsers"), searchUsersRequest, searchUsersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/SearchUsers")
	}
	quoteHouseholdReply, err := s.householdCalculationClient.QuoteHousehold(ctx, &customersv1.QuoteHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{
					DisplayName: req.GetConsentRecord().GetSubject().GetDisplayName(),
				},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/QuoteHousehold")
	}
	return &customersv1.PushConsentRecordResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: searchUsersReply.GetResult().GetCorrelationId(),
			ReferenceIds:  searchUsersReply.GetResult().GetReferenceIds(),
			Details:       quoteHouseholdReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			Sequence:        req.GetConsentRecord().GetSequence(),
		},
	}, nil
}

// PullConsentRecords pulls a consent record.
func (s *ConsentRecordSyncServer) PullConsentRecords(ctx context.Context, req *customersv1.PullConsentRecordsRequest) (*customersv1.PullConsentRecordsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PullConsentRecords", "error", err)
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "pull consent records", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	snapshot, err := protojson.Marshal(req.GetConsentRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recordRoleVersionRequest := &identityv1.RecordRoleVersionRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetConsentRecord().GetSubject().GetDisplayName(),
				UserId:      (&decoded).GetActor().GetUserId(),
			},
			Action: &identityv1.Role_Block{Block: &identityv1.RoleBlockAction{Reason: req.GetReason()}},
		},
		Annotations: annotations,
		Details:     &identityv1.RecordRoleVersionRequest_Details{Snapshot: snapshot},
		RoleId:      envelope.GetDisplayName(),
	}
	recordRoleVersionReply := new(identityv1.RecordRoleVersionResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.RoleHistoryService_RecordRoleVersion_FullMethodName, recordRoleVersionRequest, recordRoleVersionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/RecordRoleVersion")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "customers.pull_consent_records.quote_consent_record",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"consent_record.subject.household_id": req.GetConsentRecord().GetSubject().GetHouseholdId(),
			"consent_record.subject.display_name": recordRoleVersionReply.GetRole().GetOwner().GetDisplayName(),
			"consent_record.subject.path":         req.GetConsentRecord().GetSubject().GetPath(),
			"context.tenant_id":                   req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	validateSearchIndexRequest := &searchv1.ValidateSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Audit: &commonv1.AuditInfo{
				Etag:      recordRoleVersionReply.GetRole().GetAudit().GetEtag(),
				CreatedBy: recordRoleVersionReply.GetRole().GetAudit().GetCreatedBy(),
			},
			Components: map[string]*searchv1.SearchIndexComponent{"primary": &searchv1.SearchIndexComponent{
				Description: recordRoleVersionReply.GetRole().GetDescription(),
			}},
		},
	}
	validateSearchIndexReply := new(searchv1.ValidateSearchIndexResponse)
	if err := grpc.Invoke(ctx, "/acme.search.v1.SearchIndexValidationService/ValidateSearchIndex", validateSearchIndexRequest, validateSearchIndexReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/ValidateSearchIndex")
	}
	var envelope2 customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintSessionsRequest := &identityv1.LintSessionsRequest{
		Context: req.GetContext(),
		Details: &identityv1.LintSessionsRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: recordRoleVersionReply.GetRole().GetOwner().GetEmail(),
			},
		},
		Session: &identityv1.Session{
			Audit: &commonv1.AuditInfo{
				Etag:      validateSearchIndexReply.GetSearchIndex().GetAudit().GetEtag(),
				UpdatedBy: recordRoleVersionReply.GetRole().GetAudit().GetUpdatedBy(),
			},
		},
		SessionId: envelope2.GetDisplayName(),
	}
	send := s.lintSessionsSender(lintSessionsRequest)
	lintSessionsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/LintSessions")
	}
	return &customersv1.PullConsentRecordsResponse{
		Result: &commonv1.OperationResult{
			Message: recordRoleVersionReply.GetResult().GetMessage(),
			Details: validateSearchIndexReply.GetResult().GetDetails(),
			Status:  lintSessionsReply.GetResult().GetStatus(),
		},
		Outcome: &customersv1.PullConsentRecordsResponse_Outcome{
			EffectiveAt: recordRoleVersionReply.GetOutcome().GetEffectiveAt(),
			Status:      validateSearchIndexReply.GetResult().GetStatus(),
		},
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			Attributes:      req.GetConsentRecord().GetAttributes(),
		},
	}, nil
}

// ReconcileConsentRecords reconciles a consent record.
func (s *ConsentRecordSyncServer) ReconcileConsentRecords(ctx context.Context, req *customersv1.ReconcileConsentRecordsRequest) (*customersv1.ReconcileConsentRecordsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReconcileConsentRecords", "error", err)
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "reconcile consent records", "tenant", req.GetContext().GetTenantId())
	consentRecordCache := cache.New[string, *customersv1.ConsentRecord](1)
	consentRecordCache.Put(req.GetContext().GetTraceId(), req.GetConsentRecord())
	cachedConsentRecord, _ := consentRecordCache.Get(req.GetContext().GetTraceId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	calculatePreferenceRequest := &customersv1.CalculatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: cachedConsentRecord.GetSubject().GetDisplayName(),
				UserId:      tenancy.Value(ctx, "user_id"),
			},
			Action: &customersv1.Preference_Block{Block: &customersv1.PreferenceBlockAction{Reason: req.GetReason()}},
		},
		PreferenceId: envelope.GetDisplayName(),
	}
	calculatePreferenceReply := new(customersv1.CalculatePreferenceResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.PreferenceCalculationService_CalculatePreference_FullMethodName, calculatePreferenceRequest, calculatePreferenceReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/CalculatePreference")
	}
	retryCustomerSegmentImportRequest := &customersv1.RetryCustomerSegmentImportRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: calculatePreferenceReply.GetPreference().GetAudit().GetCreatedBy(),
			},
			Action: &customersv1.CustomerSegment_Discount{Discount: &customersv1.CustomerSegmentDiscountAction{
				Percent: calculatePreferenceReply.GetPreference().GetDiscount().GetPercent(),
			}},
		},
	}
	type retryCustomerSegmentImportResult struct {
		reply *customersv1.RetryCustomerSegmentImportResponse
		err   error
	}
	retryCustomerSegmentImportResults := make(chan retryCustomerSegmentImportResult, 1)
	go func() {
		reply, err := s.customerSegmentImportClient.RetryCustomerSegmentImport(ctx, retryCustomerSegmentImportRequest)
		retryCustomerSegmentImportResults <- retryCustomerSegmentImportResult{reply, err}
	}()
	var retryCustomerSegmentImportReply *customersv1.RetryCustomerSegmentImportResponse
	select {
	case result := <-retryCustomerSegmentImportResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.CustomerSegmentImportService/RetryCustomerSegmentImport")
		}
		retryCustomerSegmentImportReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "customers.reconcile_consent_records.list_customers",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.owner_id":   calculatePreferenceReply.GetPreference().GetOwner().GetUserId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	snapshot, err := protojson.Marshal(req.GetConsentRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	syncPaymentInstrumentsRequest := &paymentsv1.SyncPaymentInstrumentsRequest{}
	syncPaymentInstrumentsRequest.Context = req.GetContext()
	syncPaymentInstrumentsRequest.PaymentInstrument = &paymentsv1.PaymentInstrument{
		Holder: &customersv1.CustomerRef{
			Email:       calculatePreferenceReply.GetPreference().GetOwner().GetEmail(),
			DisplayName: calculatePreferenceReply.GetPreference().GetOwner().GetDisplayName(),
		},
	}
	syncPaymentInstrumentsRequest.Details = &paymentsv1.SyncPaymentInstrumentsRequest_Details{Payload: snapshot}
	syncPaymentInstrumentsReply, err := s.paymentInstrumentSyncClient.SyncPaymentInstruments(ctx, syncPaymentInstrumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/SyncPaymentInstruments")
	}
	return &customersv1.ReconcileConsentRecordsResponse{
		Outcome: &customersv1.ReconcileConsentRecordsResponse_Outcome{Message: calculatePreferenceReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Status:  retryCustomerSegmentImportReply.GetOutcome().GetStatus(),
			Message: syncPaymentInstrumentsReply.GetResult().GetMessage(),
		},
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			Subject:         req.GetConsentRecord().GetSubject(),
		},
	}, nil
}

// StreamConsentRecordChanges exchanges changes of consent records with a peer.
func (s *ConsentRecordSyncServer) StreamConsentRecordChanges(stream grpc.BidiStreamingServer[customersv1.StreamConsentRecordChangesRequest, customersv1.StreamConsentRecordChangesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *customersv1.StreamConsentRecordChangesRequest)
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
		s.log.InfoContext(ctx, "stream consent record changes", "tenant", msg.GetContext().GetTenantId())
		attributes := make(map[string]string, len(msg.GetChange().GetAttributes()))
		for key, value := range msg.GetChange().GetAttributes() {
			attributes[key] = value
		}
		batchCreateCustomersStream, err := s.customerClient.BatchCreateCustomers(ctx)
		if err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchCreateCustomers")
		}
		if err := batchCreateCustomersStream.Send(&customersv1.BatchCreateCustomersRequest{
			Context: msg.GetContext(),
			Chunk: &customersv1.BatchCreateCustomersRequest_Record{Record: &customersv1.Customer{
				RelationshipManager: &identityv1.UserRef{
					DisplayName: msg.GetChange().GetSubject().GetDisplayName(),
					Email:       msg.GetContext().GetActor().GetEmail(),
				},
				Attributes: attributes,
			}},
			Sequence: msg.GetChange().GetSequence(),
		}); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchCreateCustomers")
		}
		if _, err := batchCreateCustomersStream.CloseAndRecv(); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchCreateCustomers")
		}
		attributes2 := make(map[string]string, len(msg.GetChange().GetAttributes()))
		for key2, value2 := range msg.GetChange().GetAttributes() {
			attributes2[key2] = value2
		}
		baggage := make(map[string]string, len(msg.GetContext().GetBaggage()))
		for key3, value3 := range msg.GetContext().GetBaggage() {
			baggage[key3] = value3
		}
		scheduleSynonymExportRequest := &searchv1.ScheduleSynonymExportRequest{
			Context:     msg.GetContext(),
			Annotations: attributes2,
			Details:     &searchv1.ScheduleSynonymExportRequest_Details{Filters: baggage},
		}
		if _, err := searchv1.SynonymExportServiceClient.ScheduleSynonymExport(s.synonymExportClient, ctx, scheduleSynonymExportRequest); err != nil {
			return grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ScheduleSynonymExport")
		}
		for _, measurement := range msg.GetChange().GetMeasurements() {
			if _, err := s.userClient.ListUsers(ctx, &identityv1.ListUsersRequest{
				Context: msg.GetContext(),
				Filter: &identityv1.ListUsersRequest_Filter{
					CreatedAfter: measurement.GetObservedAt(),
					OwnerId:      msg.GetChange().GetConsentRecordId(),
				},
			}); err != nil {
				return grpcx.Wrap(err, "acme.identity.v1.UserService/ListUsers")
			}
		}
		if err := stream.Send(&customersv1.StreamConsentRecordChangesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
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

// ResolveConsentRecordConflict resolves a consent record.
func (s *ConsentRecordSyncServer) ResolveConsentRecordConflict(ctx context.Context, req *customersv1.ResolveConsentRecordConflictRequest) (*customersv1.ResolveConsentRecordConflictResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "resolve consent record conflict", "tenant", req.GetContext().GetTenantId())
	customerSegmentValidation, err := registry.Lookup[customersv1.CustomerSegmentValidationServiceClient](s.clients, "acme.customers.v1.CustomerSegmentValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	validateCustomerSegmentRequest := &customersv1.ValidateCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{Reason: req.GetReason()}},
		},
	}
	validateCustomerSegmentReply, err := customerSegmentValidation.ValidateCustomerSegment(ctx, validateCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*customersv1.PreviewConsentRecordResponse, len(req.GetConsentRecord().GetMeasurements()))
	for i, measurement := range req.GetConsentRecord().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.consentRecordValidationClient.PreviewConsentRecord(groupCtx, &customersv1.PreviewConsentRecordRequest{
				Context: req.GetContext(),
				ConsentRecord: &customersv1.ConsentRecord{
					Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{
						Value:      measurement.GetValue(),
						ObservedAt: measurement.GetObservedAt(),
						Unit:       measurement.GetUnit(),
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &customersv1.ResolveConsentRecordConflictResponse{
		Outcome: &customersv1.ResolveConsentRecordConflictResponse_Outcome{
			Status: validateCustomerSegmentReply.GetResult().GetStatus(),
		},
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			Kind:            req.GetConsentRecord().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetConsentRecordSyncState returns a consent record.
func (s *ConsentRecordSyncServer) GetConsentRecordSyncState(ctx context.Context, req *customersv1.GetConsentRecordSyncStateRequest) (*customersv1.GetConsentRecordSyncStateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "get_consent_record_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get consent record sync state", "tenant", req.GetContext().GetTenantId())
	recalculateHouseholdsRequest := &customersv1.RecalculateHouseholdsRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
				}},
			}},
		},
	}
	recalculateHouseholdsReply := new(customersv1.RecalculateHouseholdsResponse)
	if err := grpc.Invoke(ctx, "/acme.customers.v1.HouseholdCalculationService/RecalculateHouseholds", recalculateHouseholdsRequest, recalculateHouseholdsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/RecalculateHouseholds")
	}
	return &customersv1.GetConsentRecordSyncStateResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recalculateHouseholdsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  recalculateHouseholdsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{ConsentRecordId: req.GetConsentRecordId()},
	}, nil
}

// ListConsentRecordConflicts lists consent records a page at a time.
func (s *ConsentRecordSyncServer) ListConsentRecordConflicts(ctx context.Context, req *customersv1.ListConsentRecordConflictsRequest) (*customersv1.ListConsentRecordConflictsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListConsentRecordConflicts", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list consent record conflicts", "tenant", req.GetContext().GetTenantId())
	commentMerchandisingRuleApprovalRequest := &merchandisingv1.CommentMerchandisingRuleApprovalRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
	}
	commentMerchandisingRuleApprovalReply, err := grpcx.Call(ctx, s.merchandisingRuleApprovalClient.CommentMerchandisingRuleApproval, commentMerchandisingRuleApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleApprovalService/CommentMerchandisingRuleApproval")
	}
	streamPromotionMetricsStream, err := s.promotionReportingClient.StreamPromotionMetrics(ctx, &promotionsv1.StreamPromotionMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: commentMerchandisingRuleApprovalReply.GetMerchandisingRule().GetOwner().GetEmail(),
		},
		Filter: &promotionsv1.StreamPromotionMetricsRequest_Filter{
			Region:       req.GetFilter().GetRegion(),
			Query:        req.GetFilter().GetQuery(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/StreamPromotionMetrics")
	}
	for {
		received, err := streamPromotionMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/StreamPromotionMetrics")
		}
		_ = received
	}
	return &customersv1.ListConsentRecordConflictsResponse{
		Result: &commonv1.OperationResult{
			Details: commentMerchandisingRuleApprovalReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// lintSessionsSender returns a function that sends one request to
// acme.identity.v1.SessionValidationService/LintSessions.
func (s *ConsentRecordSyncServer) lintSessionsSender(request *identityv1.LintSessionsRequest) func(context.Context) (*identityv1.LintSessionsResponse, error) {
	return func(ctx context.Context) (*identityv1.LintSessionsResponse, error) {
		return s.sessionValidationClient.LintSessions(ctx, request)
	}
}
