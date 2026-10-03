package fraud

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	suppliersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/suppliers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// FraudSignalQueryServer implements `acme.fraud.v1.FraudSignalQueryService`.
// Answers read-only questions about fraud signals, for screens and reports.
type FraudSignalQueryServer struct {
	fraudv1.UnsafeFraudSignalQueryServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	supplierQueryOnce               sync.Once
	supplierQueryCached             suppliersv1.SupplierQueryServiceClient
	riskAssessmentQueryClient       fraudv1.RiskAssessmentQueryServiceClient
	riskAssessmentCalculationClient fraudv1.RiskAssessmentCalculationServiceClient
	clients                         *registry.Registry
	sellerScorecardValidationClient sellersv1.SellerScorecardValidationServiceClient
}

// NewFraudSignalQueryServer wires a FraudSignalQueryServer.
func NewFraudSignalQueryServer(deps platform.Deps) *FraudSignalQueryServer {
	return &FraudSignalQueryServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.fraud.v1.FraudSignalQueryService"),
		conn:                            deps.Conn,
		riskAssessmentQueryClient:       fraudv1.NewRiskAssessmentQueryServiceClient(deps.Conn),
		riskAssessmentCalculationClient: fraudv1.NewRiskAssessmentCalculationServiceClient(deps.Conn),
		clients:                         deps.Clients,
		sellerScorecardValidationClient: sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
	}
}

// LookupFraudSignal returns a fraud signal.
func (s *FraudSignalQueryServer) LookupFraudSignal(ctx context.Context, req *fraudv1.LookupFraudSignalRequest) (*fraudv1.LookupFraudSignalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupFraudSignal", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_signal", req.GetFraudSignalId(), "lookup_fraud_signal"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup fraud signal", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	findSuppliersRequest := &suppliersv1.FindSuppliersRequest{
		Context: req.GetContext(),
		Filter: &suppliersv1.FindSuppliersRequest_Filter{
			OwnerId:      req.GetFraudSignalId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       baggage,
		},
	}
	findSuppliersReply, err := s.supplierQueryLazy().FindSuppliers(ctx, findSuppliersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.suppliers.v1.SupplierQueryService/FindSuppliers")
	}
	browseRiskAssessmentsRequest := &fraudv1.BrowseRiskAssessmentsRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.BrowseRiskAssessmentsRequest_Filter{
			Labels:  findSuppliersReply.GetResult().GetDetails(),
			OwnerId: findSuppliersReply.GetResult().GetCorrelationId(),
		},
	}
	browseRiskAssessmentsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*fraudv1.BrowseRiskAssessmentsResponse, error) {
		return s.riskAssessmentQueryClient.BrowseRiskAssessments(ctx, browseRiskAssessmentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentQueryService/BrowseRiskAssessments")
	}
	_ = browseRiskAssessmentsReply
	return &fraudv1.LookupFraudSignalResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: findSuppliersReply.GetResult().GetCorrelationId(),
			Message:       findSuppliersReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		FraudSignal: &fraudv1.FraudSignal{FraudSignalId: req.GetFraudSignalId()},
	}, nil
}

// BrowseFraudSignals lists fraud signals a page at a time.
func (s *FraudSignalQueryServer) BrowseFraudSignals(ctx context.Context, req *fraudv1.BrowseFraudSignalsRequest) (*fraudv1.BrowseFraudSignalsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "browse fraud signals", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	compareRoleVersionsRequest := &identityv1.CompareRoleVersionsRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				Email:  tenancy.Value(ctx, "email"),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &identityv1.CompareRoleVersionsRequest_Details{Snapshot: snapshot},
	}
	compareRoleVersionsReply := new(identityv1.CompareRoleVersionsResponse)
	if err := s.conn.Invoke(ctx, identityRoleHistoryServiceCompareRoleVersionsMethod, compareRoleVersionsRequest, compareRoleVersionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/CompareRoleVersions")
	}
	return &fraudv1.BrowseFraudSignalsResponse{
		Result: &commonv1.OperationResult{
			Message: compareRoleVersionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// FindFraudSignals lists fraud signals a page at a time.
func (s *FraudSignalQueryServer) FindFraudSignals(ctx context.Context, req *fraudv1.FindFraudSignalsRequest) (*fraudv1.FindFraudSignalsResponse, error) {
	s.log.InfoContext(ctx, "find fraud signals", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.FindFraudSignalsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountFraudSignals counts fraud signals by status.
func (s *FraudSignalQueryServer) CountFraudSignals(ctx context.Context, req *fraudv1.CountFraudSignalsRequest) (*fraudv1.CountFraudSignalsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count fraud signals", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Region string }
	if record := req.GetFilter(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	recalculateRiskAssessmentsRequest := &fraudv1.RecalculateRiskAssessmentsRequest{
		Context: req.GetContext(),
		Details: &fraudv1.RecalculateRiskAssessmentsRequest_Details{Destination: &commonv1.PostalAddress{Region: holder.Region}},
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee:  &identityv1.UserRef{UserId: cachedContext.GetActor().GetUserId()},
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	recalculateRiskAssessmentsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*fraudv1.RecalculateRiskAssessmentsResponse, error) {
		return s.riskAssessmentCalculationClient.RecalculateRiskAssessments(ctx, recalculateRiskAssessmentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/RecalculateRiskAssessments")
	}
	return &fraudv1.CountFraudSignalsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recalculateRiskAssessmentsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ResolveFraudSignals returns several fraud signals at once.
func (s *FraudSignalQueryServer) ResolveFraudSignals(ctx context.Context, req *fraudv1.ResolveFraudSignalsRequest) (*fraudv1.ResolveFraudSignalsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "resolve fraud signals", "tenant", req.GetContext().GetTenantId())
	sponsor, ok := s.clients.Get("acme.sponsorships.v1.SponsorService").(sponsorshipsv1.SponsorServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.sponsorships.v1.SponsorService")
	}
	createSponsorRequest := &sponsorshipsv1.CreateSponsorRequest{
		Context: req.GetContext(),
		Sponsor: &sponsorshipsv1.Sponsor{
			Contact:             &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			RelationshipManager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit:               &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	}
	createSponsorReply, err := sponsor.CreateSponsor(ctx, createSponsorRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sponsorships.v1.SponsorService/CreateSponsor")
	}
	return &fraudv1.ResolveFraudSignalsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: createSponsorReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		FraudSignals: []*fraudv1.FraudSignal{&fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: createSponsorReply.GetSponsor().GetRelationshipManager().GetDisplayName(),
			},
		}},
	}, nil
}

// ExportFraudSignals streams every matching fraud signal.
func (s *FraudSignalQueryServer) ExportFraudSignals(req *fraudv1.ExportFraudSignalsRequest, stream grpc.ServerStreamingServer[fraudv1.ExportFraudSignalsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExportFraudSignals", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "export fraud signals", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetContext().GetActor(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	previewSellerScorecardRequest := &sellersv1.PreviewSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Counterparty:   &customersv1.CustomerRef{Email: holder.Email},
			Audit:          &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &sellersv1.PreviewSellerScorecardRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	var previewSellerScorecardReply *sellersv1.PreviewSellerScorecardResponse
	if err := pipeline.Run(ctx, "acme.sellers.v1.SellerScorecardValidationService/PreviewSellerScorecard", func(ctx context.Context) error {
		answer, err := s.sellerScorecardValidationClient.PreviewSellerScorecard(ctx, previewSellerScorecardRequest)
		previewSellerScorecardReply = answer
		return err
	}); err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/PreviewSellerScorecard")
	}
	if err := stream.Send(&fraudv1.ExportFraudSignalsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: previewSellerScorecardReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// supplierQueryLazy dials acme.suppliers.v1.SupplierQueryService the first time a handler needs it.
func (s *FraudSignalQueryServer) supplierQueryLazy() suppliersv1.SupplierQueryServiceClient {
	s.supplierQueryOnce.Do(func() { s.supplierQueryCached = suppliersv1.NewSupplierQueryServiceClient(s.conn) })
	return s.supplierQueryCached
}
