package sellers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	pipelinesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pipelines/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerListingValidationServer implements `acme.sellers.v1.SellerListingValidationService`.
// Checks seller listings against the rules before anything acts on them.
type SellerListingValidationServer struct {
	sellersv1.UnsafeSellerListingValidationServiceServer
	store                                *Store
	log                                  *slog.Logger
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
	giftCardClient                       giftcardsv1.GiftCardServiceClient
	giftCardTransactionValidationClient  giftcardsv1.GiftCardTransactionValidationServiceClient
	sellerListingCalculationClient       sellersv1.SellerListingCalculationServiceClient
	conn                                 *grpc.ClientConn
	sellerCalculationClient              sellersv1.SellerCalculationServiceClient
	pipelineQueryClient                  pipelinesv1.PipelineQueryServiceClient
	clients                              *registry.Registry
	giftCardValidationClient             giftcardsv1.GiftCardValidationServiceClient
	searchIndexNotificationClient        searchv1.SearchIndexNotificationServiceClient
}

// NewSellerListingValidationServer wires a SellerListingValidationServer.
func NewSellerListingValidationServer(deps platform.Deps) *SellerListingValidationServer {
	return &SellerListingValidationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.sellers.v1.SellerListingValidationService"),
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		giftCardClient:                       giftcardsv1.NewGiftCardServiceClient(deps.Conn),
		giftCardTransactionValidationClient:  giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn),
		sellerListingCalculationClient:       sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		sellerCalculationClient:              sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		pipelineQueryClient:                  pipelinesv1.NewPipelineQueryServiceClient(deps.Conn),
		clients:                              deps.Clients,
		giftCardValidationClient:             giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		searchIndexNotificationClient:        searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
	}
}

// ValidateSellerListing validates a seller listing.
func (s *SellerListingValidationServer) ValidateSellerListing(ctx context.Context, req *sellersv1.ValidateSellerListingRequest) (*sellersv1.ValidateSellerListingResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "validate seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	compareGiftCardTransactionOptionsRequest := buildCompareGiftCardTransactionOptionsRequestForValidateSellerListing(req)
	compareGiftCardTransactionOptionsReply, err := s.giftCardTransactionCalculationClient.CompareGiftCardTransactionOptions(ctx, compareGiftCardTransactionOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/CompareGiftCardTransactionOptions")
	}
	watchGiftCardsStream, err := s.giftCardClient.WatchGiftCards(ctx, &giftcardsv1.WatchGiftCardsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardService/WatchGiftCards")
	}
	for {
		received, err := watchGiftCardsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardService/WatchGiftCards")
		}
		_ = received
	}
	return &sellersv1.ValidateSellerListingResponse{
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				DisplayName: compareGiftCardTransactionOptionsReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			SellerListingId: req.GetSellerListingId(),
			Specs:           req.GetSellerListing().GetSpecs(),
		},
		Result: &commonv1.OperationResult{
			Status: compareGiftCardTransactionOptionsReply.GetResult().GetStatus(),
		},
	}, nil
}

// PreviewSellerListing previews a seller listing.
func (s *SellerListingValidationServer) PreviewSellerListing(ctx context.Context, req *sellersv1.PreviewSellerListingRequest) (*sellersv1.PreviewSellerListingResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "preview seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*sellersv1.PreviewSellerListingRequest_Details)
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded sellersv1.PreviewSellerListingRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	lintGiftCardTransactionsRequest := newLintGiftCardTransactionsRequest(withLintGiftCardTransactionsContext(req.GetContext()), withLintGiftCardTransactionsDetails(&giftcardsv1.LintGiftCardTransactionsRequest_Details{
		Contact: &commonv1.ContactInfo{
			PreferredChannel: cloned.GetContact().GetPreferredChannel(),
			Phone:            (&decoded).GetContact().GetPhone(),
		},
		Rules: req.GetDetails().GetRules(),
	}), withLintGiftCardTransactionsGiftCardTransaction(&giftcardsv1.GiftCardTransaction{
		Subject: &giftcardsv1.GiftCardBatchRef{
			DisplayName: req.GetSellerListing().GetOwner().GetDisplayName(),
		},
	}))
	lintGiftCardTransactionsReply, err := s.giftCardTransactionValidationClient.LintGiftCardTransactions(ctx, lintGiftCardTransactionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/LintGiftCardTransactions")
	}
	return &sellersv1.PreviewSellerListingResponse{
		Result: &commonv1.OperationResult{
			Message: lintGiftCardTransactionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &sellersv1.PreviewSellerListingResponse_Outcome{
			EffectiveAt: lintGiftCardTransactionsReply.GetOutcome().GetEffectiveAt(),
		},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Status:          req.GetSellerListing().GetStatus(),
		},
	}, nil
}

// CheckSellerListingEligibility checks a seller listing.
func (s *SellerListingValidationServer) CheckSellerListingEligibility(ctx context.Context, req *sellersv1.CheckSellerListingEligibilityRequest) (*sellersv1.CheckSellerListingEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "check seller listing eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	estimateSellerListingReply, err := s.sellerListingCalculationClient.EstimateSellerListing(ctx, &sellersv1.EstimateSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetSellerListing().GetAudit().GetCreatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/EstimateSellerListing")
	}
	explainGiftCardTransactionCalculationRequest := &giftcardsv1.ExplainGiftCardTransactionCalculationRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Measurements: []*giftcardsv1.GiftCardTransactionMeasurement{&giftcardsv1.GiftCardTransactionMeasurement{Unit: req.GetSellerListing().GetDimensions().GetUnit()}},
		},
		Details: &giftcardsv1.ExplainGiftCardTransactionCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Email: req.GetSellerListing().GetOwner().GetEmail(),
				Name:  req.GetDetails().GetContact().GetName(),
			},
		},
	}
	explainGiftCardTransactionCalculationReply := new(giftcardsv1.ExplainGiftCardTransactionCalculationResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardTransactionCalculationService_ExplainGiftCardTransactionCalculation_FullMethodName, explainGiftCardTransactionCalculationRequest, explainGiftCardTransactionCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/ExplainGiftCardTransactionCalculation")
	}
	encoded, err := protojson.Marshal(req.GetSellerListing())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded sellersv1.SellerListing
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	simulateSellerListingRequest := &sellersv1.SimulateSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Components: map[string]*sellersv1.SellerListingComponent{"primary": &sellersv1.SellerListingComponent{
				Description: estimateSellerListingReply.GetSellerListing().GetComponents()["primary"].GetDescription(),
				ComponentId: estimateSellerListingReply.GetSellerListing().GetComponents()["primary"].GetComponentId(),
			}},
			Audit:      &commonv1.AuditInfo{UpdatedAt: req.GetSellerListing().GetAudit().GetUpdatedAt()},
			Dimensions: &sellersv1.SellerListingDimensions{Weight: (&decoded).GetDimensions().GetWeight()},
		},
	}
	simulateSellerListingReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*sellersv1.SimulateSellerListingResponse, error) {
		return s.sellerListingCalculationClient.SimulateSellerListing(ctx, simulateSellerListingRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/SimulateSellerListing")
	}
	return &sellersv1.CheckSellerListingEligibilityResponse{
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				CreatedAt: estimateSellerListingReply.GetSellerListing().GetAudit().GetCreatedAt(),
				Etag:      simulateSellerListingReply.GetSellerListing().GetAudit().GetEtag(),
			},
			SellerListingId: req.GetSellerListingId(),
			Capacity:        req.GetSellerListing().GetCapacity(),
		},
		Outcome: &sellersv1.CheckSellerListingEligibilityResponse_Outcome{
			Status: explainGiftCardTransactionCalculationReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: simulateSellerListingReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// NormalizeSellerListing normalizes a seller listing.
func (s *SellerListingValidationServer) NormalizeSellerListing(ctx context.Context, req *sellersv1.NormalizeSellerListingRequest) (*sellersv1.NormalizeSellerListingResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "normalize seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	var locality string
	switch choice := req.GetSellerListing().GetLocator().(type) {
	case *sellersv1.SellerListing_Address:
		locality = choice.Address.GetLocality()
	case *sellersv1.SellerListing_FacilityCode:
		locality = choice.FacilityCode
	}
	sellerListingCache := cache.New[string, *sellersv1.SellerListing](1)
	sellerListingCache.Put(req.GetContext().GetTraceId(), req.GetSellerListing())
	cachedSellerListing, _ := sellerListingCache.Get(req.GetContext().GetTraceId())
	quoteSellerRequest := &sellersv1.QuoteSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: locality,
				Region:   req.GetSellerListing().GetAddress().GetRegion(),
			},
			Contact: &commonv1.ContactInfo{
				Email: cachedSellerListing.GetOwner().GetEmail(),
				Phone: req.GetDetails().GetContact().GetPhone(),
			},
		},
	}
	send := s.quoteSellerSender(quoteSellerRequest)
	quoteSellerReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/QuoteSeller")
	}
	return &sellersv1.NormalizeSellerListingResponse{
		Result: &commonv1.OperationResult{Status: quoteSellerReply.GetResult().GetStatus()},
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: quoteSellerReply.GetSeller().GetAudit().GetUpdatedAt(),
			},
			SellerListingId: req.GetSellerListingId(),
			Specs:           req.GetSellerListing().GetSpecs(),
		},
	}, nil
}

// ExplainSellerListingDecision explains a seller listing.
func (s *SellerListingValidationServer) ExplainSellerListingDecision(ctx context.Context, req *sellersv1.ExplainSellerListingDecisionRequest) (*sellersv1.ExplainSellerListingDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "explain seller listing decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := sellerListingValidationServerResolvePipelinesSteps[mode]
	resolvePipelinesRequest := &pipelinesv1.ResolvePipelinesRequest{Context: req.GetContext()}
	resolvePipelinesReply, err := run(s, ctx, resolvePipelinesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pipelines.v1.PipelineQueryService/ResolvePipelines")
	}
	sellerListingCalculation, err := registry.Lookup[sellersv1.SellerListingCalculationServiceClient](s.clients, "acme.sellers.v1.SellerListingCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	calculateSellerListingRequest := &sellersv1.CalculateSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{Etag: req.GetSellerListing().GetAudit().GetEtag()},
		},
	}
	calculateSellerListingReply, err := sellerListingCalculation.CalculateSellerListing(ctx, calculateSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/CalculateSellerListing")
	}
	return &sellersv1.ExplainSellerListingDecisionResponse{
		Outcome: &sellersv1.ExplainSellerListingDecisionResponse_Outcome{Status: resolvePipelinesReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Status:  resolvePipelinesReply.GetResult().GetStatus(),
			Details: calculateSellerListingReply.GetResult().GetDetails(),
		},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Code:            req.GetSellerListing().GetCode(),
		},
	}, nil
}

// ScoreSellerListing scores a seller listing.
func (s *SellerListingValidationServer) ScoreSellerListing(ctx context.Context, req *sellersv1.ScoreSellerListingRequest) (*sellersv1.ScoreSellerListingResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "score seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	normalizeGiftCardRequest := &giftcardsv1.NormalizeGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetSellerListing().GetAudit().GetCreatedBy(),
				Etag:      req.GetSellerListing().GetAudit().GetEtag(),
			},
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: req.GetSellerListing().GetOwner().GetDisplayName(),
			},
		},
	}
	normalizeGiftCardReply, err := giftcardsv1.GiftCardValidationServiceClient.NormalizeGiftCard(s.giftCardValidationClient, ctx, normalizeGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/NormalizeGiftCard")
	}
	compareSellerListingOptionsRequest := &sellersv1.CompareSellerListingOptionsRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Components: map[string]*sellersv1.SellerListingComponent{"primary": &sellersv1.SellerListingComponent{
				Description: req.GetSellerListing().GetComponents()["primary"].GetDescription(),
			}},
		},
	}
	compareSellerListingOptionsReply := new(sellersv1.CompareSellerListingOptionsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", sellersSellerListingCalculationServiceName, "CompareSellerListingOptions"), compareSellerListingOptionsRequest, compareSellerListingOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/CompareSellerListingOptions")
	}
	accessPolicy, err := registry.Lookup[authorizationv1.AccessPolicyServiceClient](s.clients, "acme.authorization.v1.AccessPolicyService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cloneAccessPolicyRequest := &authorizationv1.CloneAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Audit: &commonv1.AuditInfo{
				Etag:      normalizeGiftCardReply.GetGiftCard().GetAudit().GetEtag(),
				CreatedAt: normalizeGiftCardReply.GetGiftCard().GetAudit().GetCreatedAt(),
				CreatedBy: normalizeGiftCardReply.GetGiftCard().GetAudit().GetCreatedBy(),
				UpdatedAt: normalizeGiftCardReply.GetGiftCard().GetAudit().GetUpdatedAt(),
			},
		},
	}
	cloneAccessPolicyReply, err := accessPolicy.CloneAccessPolicy(ctx, cloneAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/CloneAccessPolicy")
	}
	return &sellersv1.ScoreSellerListingResponse{
		Result: &commonv1.OperationResult{
			Message: normalizeGiftCardReply.GetOutcome().GetMessage(),
			Details: normalizeGiftCardReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				Etag: compareSellerListingOptionsReply.GetSellerListing().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				UserId: cloneAccessPolicyReply.GetAccessPolicy().GetOwner().GetUserId(),
			},
			Name:            cloneAccessPolicyReply.GetAccessPolicy().GetName(),
			SellerListingId: req.GetSellerListingId(),
			Status:          req.GetSellerListing().GetStatus(),
		},
	}, nil
}

// LintSellerListings lints a seller listing.
func (s *SellerListingValidationServer) LintSellerListings(ctx context.Context, req *sellersv1.LintSellerListingsRequest) (*sellersv1.LintSellerListingsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintSellerListings", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "lint seller listings", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	var region string
	switch choice := req.GetSellerListing().GetLocator().(type) {
	case *sellersv1.SellerListing_Address:
		region = choice.Address.GetRegion()
	case *sellersv1.SellerListing_FacilityCode:
		region = choice.FacilityCode
	}
	compareGiftCardTransactionOptionsRequest := &giftcardsv1.CompareGiftCardTransactionOptionsRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CompareGiftCardTransactionOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: region,
				Lines:  req.GetSellerListing().GetAddress().GetLines(),
			},
		},
	}
	compareGiftCardTransactionOptionsReply, err := grpcx.Call(ctx, s.giftCardTransactionCalculationClient.CompareGiftCardTransactionOptions, compareGiftCardTransactionOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/CompareGiftCardTransactionOptions")
	}
	var holder struct{ CreatedBy string }
	if record := req.GetSellerListing().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "CreatedBy"); err != nil {
			s.log.WarnContext(ctx, "copy created_by", "error", err)
		}
	}
	recalculateSellerListingsReply, err := s.sellerListingCalculationClient.RecalculateSellerListings(ctx, &sellersv1.RecalculateSellerListingsRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{UserId: req.GetSellerListing().GetOwner().GetUserId()},
			Audit: &commonv1.AuditInfo{CreatedBy: holder.CreatedBy},
		},
		Details: &sellersv1.RecalculateSellerListingsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: req.GetSellerListing().GetAddress().GetRecipient(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/RecalculateSellerListings")
	}
	packed, err := anypb.New(req.GetSellerListing())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifySearchIndexWatchersRequest := &searchv1.NotifySearchIndexWatchersRequest{
		Context: req.GetContext(),
		Details: &searchv1.NotifySearchIndexWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Email: req.GetDetails().GetContact().GetEmail(),
			},
			Payload: packed,
		},
	}
	send := s.notifySearchIndexWatchersSender(notifySearchIndexWatchersRequest)
	notifySearchIndexWatchersReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/NotifySearchIndexWatchers")
	}
	return &sellersv1.LintSellerListingsResponse{
		Result: &commonv1.OperationResult{
			Message: compareGiftCardTransactionOptionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &sellersv1.LintSellerListingsResponse_Outcome{
			Message: compareGiftCardTransactionOptionsReply.GetResult().GetMessage(),
		},
		SellerListing: &sellersv1.SellerListing{
			Dimensions: &sellersv1.SellerListingDimensions{
				Length: recalculateSellerListingsReply.GetSellerListing().GetDimensions().GetLength(),
			},
			Owner:           notifySearchIndexWatchersReply.GetSearchIndex().GetOwner(),
			SellerListingId: req.GetSellerListingId(),
			Name:            req.GetSellerListing().GetName(),
		},
	}, nil
}

// GetSellerListingRules returns a seller listing.
func (s *SellerListingValidationServer) GetSellerListingRules(ctx context.Context, req *sellersv1.GetSellerListingRulesRequest) (*sellersv1.GetSellerListingRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSellerListingRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "get_seller_listing_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller listing rules", "tenant", req.GetContext().GetTenantId())
	giftCardTransactionValidation, err := registry.Lookup[giftcardsv1.GiftCardTransactionValidationServiceClient](s.clients, "acme.giftcards.v1.GiftCardTransactionValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "seller_listing_id", req.GetSellerListingId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreGiftCardTransactionRequest := &giftcardsv1.ScoreGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			CorrelationId: tenancy.Value(ctx, "seller_listing_id"),
			Detail:        packed,
		},
	}
	scoreGiftCardTransactionReply, err := giftCardTransactionValidation.ScoreGiftCardTransaction(ctx, scoreGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/ScoreGiftCardTransaction")
	}
	return &sellersv1.GetSellerListingRulesResponse{
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				DisplayName: scoreGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			SellerListingId: req.GetSellerListingId(),
		},
		Result: &commonv1.OperationResult{
			Status: scoreGiftCardTransactionReply.GetResult().GetStatus(),
		},
	}, nil
}

// quoteSellerSender returns a function that sends one request to acme.sellers.v1.SellerCalculationService/QuoteSeller.
func (s *SellerListingValidationServer) quoteSellerSender(request *sellersv1.QuoteSellerRequest) func(context.Context) (*sellersv1.QuoteSellerResponse, error) {
	return func(ctx context.Context) (*sellersv1.QuoteSellerResponse, error) {
		return s.sellerCalculationClient.QuoteSeller(ctx, request)
	}
}

// sellerListingValidationServerResolvePipelinesSteps are the ways to run
// acme.pipelines.v1.PipelineQueryService/ResolvePipelines, by name.
var sellerListingValidationServerResolvePipelinesSteps = map[string]func(*SellerListingValidationServer, context.Context, *pipelinesv1.ResolvePipelinesRequest) (*pipelinesv1.ResolvePipelinesResponse, error){
	"default": (*SellerListingValidationServer).resolvePipelinesStep,
	"audited": (*SellerListingValidationServer).resolvePipelinesAuditedStep,
}

func (s *SellerListingValidationServer) resolvePipelinesStep(ctx context.Context, request *pipelinesv1.ResolvePipelinesRequest) (*pipelinesv1.ResolvePipelinesResponse, error) {
	return s.pipelineQueryClient.ResolvePipelines(ctx, request)
}

func (s *SellerListingValidationServer) resolvePipelinesAuditedStep(ctx context.Context, request *pipelinesv1.ResolvePipelinesRequest) (*pipelinesv1.ResolvePipelinesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.pipelines.v1.PipelineQueryService/ResolvePipelines")
	return s.pipelineQueryClient.ResolvePipelines(ctx, request)
}

// notifySearchIndexWatchersSender returns a function that sends one request to
// acme.search.v1.SearchIndexNotificationService/NotifySearchIndexWatchers.
func (s *SellerListingValidationServer) notifySearchIndexWatchersSender(request *searchv1.NotifySearchIndexWatchersRequest) func(context.Context) (*searchv1.NotifySearchIndexWatchersResponse, error) {
	return func(ctx context.Context) (*searchv1.NotifySearchIndexWatchersResponse, error) {
		return s.searchIndexNotificationClient.NotifySearchIndexWatchers(ctx, request)
	}
}
