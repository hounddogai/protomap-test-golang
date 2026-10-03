package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PreferenceSyncServer implements `acme.customers.v1.PreferenceSyncService`.
// Keeps preferences in step with the systems of record in other regions.
type PreferenceSyncServer struct {
	customersv1.UnimplementedPreferenceSyncServiceServer
	store                           *Store
	log                             *slog.Logger
	customerSegmentSyncClient       customersv1.CustomerSegmentSyncServiceClient
	documentClient                  documentsv1.DocumentServiceClient
	metricSeriesValidationClient    metricsv1.MetricSeriesValidationServiceClient
	meteredBrandLifecycle           meteredBrandLifecycle
	customerCalculationPort         customerCalculationPort
	ledgerAccountQueryClient        ledgerv1.LedgerAccountQueryServiceClient
	conn                            *grpc.ClientConn
	formCalculationOnce             sync.Once
	formCalculationCached           formsv1.FormCalculationServiceClient
	meteredConsentRecordReporting   meteredConsentRecordReporting
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	shareHouseholdReportFn          func(context.Context, *customersv1.ShareHouseholdReportRequest, ...grpc.CallOption) (*customersv1.ShareHouseholdReportResponse, error)
	preferenceValidationClient      customersv1.PreferenceValidationServiceClient
	venueQueryClient                venuesv1.VenueQueryServiceClient
}

// NewPreferenceSyncServer wires a PreferenceSyncServer.
func NewPreferenceSyncServer(deps platform.Deps) *PreferenceSyncServer {
	householdReporting := customersv1.NewHouseholdReportingServiceClient(deps.Conn)
	return &PreferenceSyncServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.customers.v1.PreferenceSyncService"),
		customerSegmentSyncClient:       customersv1.NewCustomerSegmentSyncServiceClient(deps.Conn),
		documentClient:                  documentsv1.NewDocumentServiceClient(deps.Conn),
		metricSeriesValidationClient:    metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		meteredBrandLifecycle:           meteredBrandLifecycle{BrandLifecycleServiceClient: catalogv1.NewBrandLifecycleServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		customerCalculationPort:         newCustomerCalculationPort(deps),
		ledgerAccountQueryClient:        ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		conn:                            deps.Conn,
		meteredConsentRecordReporting:   meteredConsentRecordReporting{ConsentRecordReportingServiceClient: customersv1.NewConsentRecordReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		shareHouseholdReportFn:          householdReporting.ShareHouseholdReport,
		preferenceValidationClient:      customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		venueQueryClient:                venuesv1.NewVenueQueryServiceClient(deps.Conn),
	}
}

// SyncPreferences syncs a preference.
func (s *PreferenceSyncServer) SyncPreferences(ctx context.Context, req *customersv1.SyncPreferencesRequest) (*customersv1.SyncPreferencesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "sync preferences", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetPreference())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullCustomerSegmentsRequest := &customersv1.PullCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Route{Route: &customersv1.CustomerSegmentRouteAction{Weight: req.GetPreference().GetRoute().GetWeight()}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetPreference().GetAudit().GetCreatedBy(),
				CreatedAt: req.GetPreference().GetAudit().GetCreatedAt(),
			},
		},
		Details: &customersv1.PullCustomerSegmentsRequest_Details{Payload: snapshot},
	}
	var pullCustomerSegmentsReply *customersv1.PullCustomerSegmentsResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.CustomerSegmentSyncService/PullCustomerSegments", func(ctx context.Context) error {
		answer, err := s.customerSegmentSyncClient.PullCustomerSegments(ctx, pullCustomerSegmentsRequest)
		pullCustomerSegmentsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/PullCustomerSegments")
	}
	listCustomerSegmentConflictsRequest := newListCustomerSegmentConflictsRequest(withListCustomerSegmentConflictsContext(req.GetContext()), withListCustomerSegmentConflictsFilter(&customersv1.ListCustomerSegmentConflictsRequest_Filter{
		CreatedAfter: pullCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetUpdatedAt(),
		OwnerId:      pullCustomerSegmentsReply.GetCustomerSegment().GetCustomerSegmentId(),
		Labels:       req.GetPreference().GetParameters(),
	}))
	listCustomerSegmentConflictsReply, err := s.customerSegmentSyncClient.ListCustomerSegmentConflicts(ctx, listCustomerSegmentConflictsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/ListCustomerSegmentConflicts")
	}
	return &customersv1.SyncPreferencesResponse{
		Preference: &customersv1.Preference{
			Parameters:   pullCustomerSegmentsReply.GetCustomerSegment().GetParameters(),
			PreferenceId: req.GetPreferenceId(),
			Owner:        req.GetPreference().GetOwner(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: listCustomerSegmentConflictsReply.GetResult().GetReferenceIds(),
			Status:       listCustomerSegmentConflictsReply.GetResult().GetStatus(),
		},
	}, nil
}

// PushPreference pushs a preference.
func (s *PreferenceSyncServer) PushPreference(ctx context.Context, req *customersv1.PushPreferenceRequest) (*customersv1.PushPreferenceResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PushPreference", "error", err)
		return nil, err
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "push preference", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	batchCreateDocumentsStream, err := s.documentClient.BatchCreateDocuments(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/BatchCreateDocuments")
	}
	if err := batchCreateDocumentsStream.Send(&documentsv1.BatchCreateDocumentsRequest{
		Context: req.GetContext(),
		Chunk: &documentsv1.BatchCreateDocumentsRequest_Record{Record: &documentsv1.Document{
			Lines: []*documentsv1.DocumentLine{&documentsv1.DocumentLine{
				Tax: &documentsv1.DocumentLineTax{Amount: req.GetPreference().GetDiscount().GetAmount()},
			}},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/BatchCreateDocuments")
	}
	if _, err := batchCreateDocumentsStream.CloseAndRecv(); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/BatchCreateDocuments")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetPreference().GetOwner().GetDisplayName())
	lintMetricSeriesesRequest := &metricsv1.LintMetricSeriesesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Owner: &identityv1.UserRef{DisplayName: tenancy.Value(ctx, "display_name")},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetPreference().GetAudit().GetCreatedBy()},
		},
	}
	lintMetricSeriesesReply, err := metricsv1.MetricSeriesValidationServiceClient.LintMetricSerieses(s.metricSeriesValidationClient, ctx, lintMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses")
	}
	return &customersv1.PushPreferenceResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: lintMetricSeriesesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Preference: &customersv1.Preference{
			PreferenceId: req.GetPreferenceId(),
			Parameters:   req.GetPreference().GetParameters(),
		},
	}, nil
}

// PullPreferences pulls a preference.
func (s *PreferenceSyncServer) PullPreferences(ctx context.Context, req *customersv1.PullPreferencesRequest) (*customersv1.PullPreferencesResponse, error) {
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "pull preferences", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	scheduleBrandExpiryRequest := &catalogv1.ScheduleBrandExpiryRequest{
		Context: req.GetContext(),
		Brand: &catalogv1.Brand{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPreference().GetAudit().GetCreatedAt(),
				UpdatedAt: req.GetPreference().GetAudit().GetUpdatedAt(),
				UpdatedBy: req.GetPreference().GetAudit().GetUpdatedBy(),
			},
		},
	}
	scheduleBrandExpiryReply, err := s.meteredBrandLifecycle.ScheduleBrandExpiry(ctx, scheduleBrandExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandLifecycleService/ScheduleBrandExpiry")
	}
	listCustomerSegmentImportsRequest := &customersv1.ListCustomerSegmentImportsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListCustomerSegmentImportsRequest_Filter{
			Region: scheduleBrandExpiryReply.GetBrand().GetPrimaryAddress().GetRegion(),
		},
	}
	listCustomerSegmentImportsReply, err := listCustomerSegmentImportsCall(ctx, listCustomerSegmentImportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/ListCustomerSegmentImports")
	}
	return &customersv1.PullPreferencesResponse{
		Outcome: &customersv1.PullPreferencesResponse_Outcome{
			ReferenceId: scheduleBrandExpiryReply.GetOutcome().GetReferenceId(),
			Message:     scheduleBrandExpiryReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: listCustomerSegmentImportsReply.GetResult().GetCorrelationId(),
			Details:       listCustomerSegmentImportsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Preference: &customersv1.Preference{
			PreferenceId: req.GetPreferenceId(),
			Name:         req.GetPreference().GetName(),
		},
	}, nil
}

// ReconcilePreferences reconciles a preference.
func (s *PreferenceSyncServer) ReconcilePreferences(ctx context.Context, req *customersv1.ReconcilePreferencesRequest) (*customersv1.ReconcilePreferencesResponse, error) {
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "reconcile preferences", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPreference())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.Preference
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	preferenceCache := cache.New[string, *customersv1.Preference](1)
	preferenceCache.Put(req.GetContext().GetTraceId(), req.GetPreference())
	cachedPreference, _ := preferenceCache.Get(req.GetContext().GetTraceId())
	simulateCustomerRequest := &customersv1.SimulateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPreference().GetAudit().GetCreatedAt(),
				Etag:      cachedPreference.GetAudit().GetEtag(),
			},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: (&decoded).GetOwner().GetDisplayName(),
				Email:       req.GetPreference().GetOwner().GetEmail(),
			},
		},
	}
	simulateCustomerReply, err := s.customerCalculationPort.SimulateCustomer(ctx, simulateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/SimulateCustomer")
	}
	resolveLedgerAccountsRequest := buildResolveLedgerAccountsRequestForReconcilePreferences(req)
	resolveLedgerAccountsReply, err := s.ledgerAccountQueryClient.ResolveLedgerAccounts(ctx, resolveLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/ResolveLedgerAccounts")
	}
	compareFormOptionsRequest := &formsv1.CompareFormOptionsRequest{
		Context: req.GetContext(),
		Details: &formsv1.CompareFormOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: simulateCustomerReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
			},
		},
	}
	compareFormOptionsReply, err := s.formCalculationLazy().CompareFormOptions(ctx, compareFormOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forms.v1.FormCalculationService/CompareFormOptions")
	}
	_ = resolveLedgerAccountsReply
	return &customersv1.ReconcilePreferencesResponse{
		Result: &commonv1.OperationResult{
			Details:       simulateCustomerReply.GetResult().GetDetails(),
			CorrelationId: compareFormOptionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &customersv1.ReconcilePreferencesResponse_Outcome{Message: compareFormOptionsReply.GetResult().GetMessage()},
		Preference: &customersv1.Preference{
			PreferenceId: req.GetPreferenceId(),
			Priority:     req.GetPreference().GetPriority(),
		},
	}, nil
}

// StreamPreferenceChanges exchanges changes of preferences with a peer.
func (s *PreferenceSyncServer) StreamPreferenceChanges(stream grpc.BidiStreamingServer[customersv1.StreamPreferenceChangesRequest, customersv1.StreamPreferenceChangesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *customersv1.StreamPreferenceChangesRequest)
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
		s.log.InfoContext(ctx, "stream preference changes", "tenant", msg.GetContext().GetTenantId())
		pushCustomerSegmentRequest := &customersv1.PushCustomerSegmentRequest{}
		pushCustomerSegmentRequest.Context = msg.GetContext()
		pushCustomerSegmentRequest.CustomerSegment = &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Discount{Discount: &customersv1.CustomerSegmentDiscountAction{Amount: msg.GetChange().GetDiscount().GetAmount()}},
		}
		if _, err := s.customerSegmentSyncClient.PushCustomerSegment(ctx, pushCustomerSegmentRequest); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/PushCustomerSegment")
		}
		parameters := make(map[string]string, len(msg.GetChange().GetParameters()))
		for key, value := range msg.GetChange().GetParameters() {
			parameters[key] = value
		}
		shareConsentRecordReportRequest := &customersv1.ShareConsentRecordReportRequest{
			Context: msg.GetContext(),
			Details: &customersv1.ShareConsentRecordReportRequest_Details{
				Recipient: &commonv1.ContactInfo{Email: msg.GetChange().GetBlock().GetNotifyEmail()},
			},
			Annotations: parameters,
		}
		if _, err := s.meteredConsentRecordReporting.ShareConsentRecordReport(ctx, shareConsentRecordReportRequest); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.ConsentRecordReportingService/ShareConsentRecordReport")
		}
		if err := stream.Send(&customersv1.StreamPreferenceChangesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
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

// ResolvePreferenceConflict resolves a preference.
func (s *PreferenceSyncServer) ResolvePreferenceConflict(ctx context.Context, req *customersv1.ResolvePreferenceConflictRequest) (*customersv1.ResolvePreferenceConflictResponse, error) {
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "resolve preference conflict", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	jobs := make(chan *customersv1.PreferenceCondition)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for condition := range jobs {
				if _, err := s.customerSegmentValidationClient.LintCustomerSegments(workCtx, &customersv1.LintCustomerSegmentsRequest{
					Context: req.GetContext(),
					CustomerSegment: &customersv1.CustomerSegment{
						Conditions: []*customersv1.CustomerSegmentCondition{&customersv1.CustomerSegmentCondition{
							Negate:   condition.GetNegate(),
							Operator: condition.GetOperator(),
						}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPreference().GetConditions() {
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	return &customersv1.ResolvePreferenceConflictResponse{
		Preference: &customersv1.Preference{
			PreferenceId: req.GetPreferenceId(),
			Effective:    req.GetPreference().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPreferenceSyncState returns a preference.
func (s *PreferenceSyncServer) GetPreferenceSyncState(ctx context.Context, req *customersv1.GetPreferenceSyncStateRequest) (*customersv1.GetPreferenceSyncStateResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "get_preference_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get preference sync state", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareHouseholdReportRequest := &customersv1.ShareHouseholdReportRequest{
		Context: req.GetContext(),
		Details: &customersv1.ShareHouseholdReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: cloned.GetActor().GetEmail()},
			Payload:   packed,
		},
		Annotations: req.GetContext().GetBaggage(),
	}
	shareHouseholdReportReply, err := s.shareHouseholdReportFn(ctx, shareHouseholdReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdReportingService/ShareHouseholdReport")
	}
	explainPreferenceDecisionRequest := &customersv1.ExplainPreferenceDecisionRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: shareHouseholdReportReply.GetHousehold().GetAudit().GetUpdatedAt(),
			},
		},
	}
	type explainPreferenceDecisionResult struct {
		reply *customersv1.ExplainPreferenceDecisionResponse
		err   error
	}
	explainPreferenceDecisionResults := make(chan explainPreferenceDecisionResult, 1)
	go func() {
		reply, err := s.preferenceValidationClient.ExplainPreferenceDecision(ctx, explainPreferenceDecisionRequest)
		explainPreferenceDecisionResults <- explainPreferenceDecisionResult{reply, err}
	}()
	var explainPreferenceDecisionReply *customersv1.ExplainPreferenceDecisionResponse
	select {
	case result := <-explainPreferenceDecisionResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.PreferenceValidationService/ExplainPreferenceDecision")
		}
		explainPreferenceDecisionReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &customersv1.GetPreferenceSyncStateResponse{
		Result: &commonv1.OperationResult{
			Status:  shareHouseholdReportReply.GetResult().GetStatus(),
			Message: explainPreferenceDecisionReply.GetOutcome().GetMessage(),
		},
		Preference: &customersv1.Preference{
			Version:      explainPreferenceDecisionReply.GetPreference().GetVersion(),
			PreferenceId: req.GetPreferenceId(),
		},
	}, nil
}

// ListPreferenceConflicts lists preferences a page at a time.
func (s *PreferenceSyncServer) ListPreferenceConflicts(ctx context.Context, req *customersv1.ListPreferenceConflictsRequest) (*customersv1.ListPreferenceConflictsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list preference conflicts", "tenant", req.GetContext().GetTenantId())
	findVenuesRequest := newFindVenuesRequest(withFindVenuesContext(req.GetContext()), withFindVenuesFilter(&venuesv1.FindVenuesRequest_Filter{OwnerId: req.GetFilter().GetOwnerId()}), withFindVenuesPage(&commonv1.PageRequest{PageSize: req.GetPage().GetPageSize()}))
	findVenuesReply, err := s.venueQueryClient.FindVenues(ctx, findVenuesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/FindVenues")
	}
	return &customersv1.ListPreferenceConflictsResponse{
		PageInfo: &commonv1.PageInfo{
			NextPageToken: findVenuesReply.GetPageInfo().GetNextPageToken(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// formCalculationLazy dials acme.forms.v1.FormCalculationService the first time a handler needs it.
func (s *PreferenceSyncServer) formCalculationLazy() formsv1.FormCalculationServiceClient {
	s.formCalculationOnce.Do(func() { s.formCalculationCached = formsv1.NewFormCalculationServiceClient(s.conn) })
	return s.formCalculationCached
}
