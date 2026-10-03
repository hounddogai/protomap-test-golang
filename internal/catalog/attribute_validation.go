package catalog

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// AttributeValidationServer implements `acme.catalog.v1.AttributeValidationService`.
// Checks attributes against the rules before anything acts on them.
type AttributeValidationServer struct {
	catalogv1.UnimplementedAttributeValidationServiceServer
	store                           *Store
	log                             *slog.Logger
	meteredProductVariantValidation meteredProductVariantValidation
	attributeCalculationClient      catalogv1.AttributeCalculationServiceClient
	sessionHistoryClient            identityv1.SessionHistoryServiceClient
	conn                            *grpc.ClientConn
	productValidationOnce           sync.Once
	productValidationCached         catalogv1.ProductValidationServiceClient
	clients                         *registry.Registry
	productVariantCalculationPort   productVariantCalculationPort
	categoryCalculationClient       catalogv1.CategoryCalculationServiceClient
	venueClient                     venuesv1.VenueServiceClient
	searchIndexNotificationClient   searchv1.SearchIndexNotificationServiceClient
	meteredCategoryValidation       meteredCategoryValidation
}

// NewAttributeValidationServer wires an AttributeValidationServer.
func NewAttributeValidationServer(deps platform.Deps) *AttributeValidationServer {
	return &AttributeValidationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.catalog.v1.AttributeValidationService"),
		meteredProductVariantValidation: meteredProductVariantValidation{ProductVariantValidationServiceClient: catalogv1.NewProductVariantValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		attributeCalculationClient:      catalogv1.NewAttributeCalculationServiceClient(deps.Conn),
		sessionHistoryClient:            identityv1.NewSessionHistoryServiceClient(deps.Conn),
		conn:                            deps.Conn,
		clients:                         deps.Clients,
		productVariantCalculationPort:   newProductVariantCalculationPort(deps),
		categoryCalculationClient:       catalogv1.NewCategoryCalculationServiceClient(deps.Conn),
		venueClient:                     venuesv1.NewVenueServiceClient(deps.Conn),
		searchIndexNotificationClient:   searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		meteredCategoryValidation:       meteredCategoryValidation{CategoryValidationServiceClient: catalogv1.NewCategoryValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// ValidateAttribute validates an attribute.
func (s *AttributeValidationServer) ValidateAttribute(ctx context.Context, req *catalogv1.ValidateAttributeRequest) (*catalogv1.ValidateAttributeResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "validate attribute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	validateProductVariantRequest := &catalogv1.ValidateProductVariantRequest{
		Context: req.GetContext(),
		Details: &catalogv1.ValidateProductVariantRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	validateProductVariantReply, err := s.meteredProductVariantValidation.ValidateProductVariant(ctx, validateProductVariantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/ValidateProductVariant")
	}
	getAttributeRatesRequest := &catalogv1.GetAttributeRatesRequest{
		Context:     req.GetContext(),
		AttributeId: req.GetAttribute().GetAttributeId(),
	}
	type getAttributeRatesResult struct {
		reply *catalogv1.GetAttributeRatesResponse
		err   error
	}
	getAttributeRatesResults := make(chan getAttributeRatesResult, 1)
	go func() {
		reply, err := s.attributeCalculationClient.GetAttributeRates(ctx, getAttributeRatesRequest)
		getAttributeRatesResults <- getAttributeRatesResult{reply, err}
	}()
	var getAttributeRatesReply *catalogv1.GetAttributeRatesResponse
	select {
	case result := <-getAttributeRatesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.catalog.v1.AttributeCalculationService/GetAttributeRates")
		}
		getAttributeRatesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	listSessionVersionsReply, err := s.sessionHistoryClient.ListSessionVersions(ctx, &identityv1.ListSessionVersionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListSessionVersionsRequest_Filter{
			Region:       validateProductVariantReply.GetProductVariant().GetAddress().GetRegion(),
			CreatedAfter: getAttributeRatesReply.GetAttribute().GetAudit().GetUpdatedAt(),
			Labels:       validateProductVariantReply.GetProductVariant().GetSpecs(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/ListSessionVersions")
	}
	return &catalogv1.ValidateAttributeResponse{
		Result: &commonv1.OperationResult{
			Message: validateProductVariantReply.GetResult().GetMessage(),
			Status:  listSessionVersionsReply.GetResult().GetStatus(),
		},
		Attribute: &catalogv1.Attribute{
			Name:        validateProductVariantReply.GetProductVariant().GetName(),
			AttributeId: req.GetAttributeId(),
			Parameters:  req.GetAttribute().GetParameters(),
		},
	}, nil
}

// PreviewAttribute previews an attribute.
func (s *AttributeValidationServer) PreviewAttribute(ctx context.Context, req *catalogv1.PreviewAttributeRequest) (*catalogv1.PreviewAttributeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "preview attribute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	notifySessionWatchersRequest := &identityv1.NotifySessionWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifySessionWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetAttribute().GetOwner().GetEmail()},
		},
		Reason: req.GetAttribute().GetBlock().GetReason(),
	}
	notifySessionWatchersReply := new(identityv1.NotifySessionWatchersResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.SessionNotificationService_NotifySessionWatchers_FullMethodName, notifySessionWatchersRequest, notifySessionWatchersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/NotifySessionWatchers")
	}
	checkProductEligibilityRequest := &catalogv1.CheckProductEligibilityRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				DisplayName: notifySessionWatchersReply.GetSession().GetCustomer().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: notifySessionWatchersReply.GetSession().GetAudit().GetCreatedAt(),
				CreatedBy: notifySessionWatchersReply.GetSession().GetAudit().GetCreatedBy(),
			},
		},
		Details: &catalogv1.CheckProductEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	checkProductEligibilityReply, err := s.productValidationLazy().CheckProductEligibility(ctx, checkProductEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/CheckProductEligibility")
	}
	return &catalogv1.PreviewAttributeResponse{
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				CreatedBy: notifySessionWatchersReply.GetSession().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				UserId: checkProductEligibilityReply.GetProduct().GetOwner().GetUserId(),
			},
			AttributeId: req.GetAttributeId(),
			Description: req.GetAttribute().GetDescription(),
		},
		Outcome: &catalogv1.PreviewAttributeResponse_Outcome{
			EffectiveAt: checkProductEligibilityReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckAttributeEligibility checks an attribute.
func (s *AttributeValidationServer) CheckAttributeEligibility(ctx context.Context, req *catalogv1.CheckAttributeEligibilityRequest) (*catalogv1.CheckAttributeEligibilityResponse, error) {
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "check attribute eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	lintProductVariantsRequest := &catalogv1.LintProductVariantsRequest{
		Context: req.GetContext(),
		Details: &catalogv1.LintProductVariantsRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	lintProductVariantsReply := new(catalogv1.LintProductVariantsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", catalogProductVariantValidationServiceName, "LintProductVariants"), lintProductVariantsRequest, lintProductVariantsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/LintProductVariants")
	}
	giftCardQuery, err := registry.Lookup[giftcardsv1.GiftCardQueryServiceClient](s.clients, "acme.giftcards.v1.GiftCardQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resolveGiftCardsRequest := &giftcardsv1.ResolveGiftCardsRequest{Context: req.GetContext()}
	resolveGiftCardsReply, err := giftCardQuery.ResolveGiftCards(ctx, resolveGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/ResolveGiftCards")
	}
	return &catalogv1.CheckAttributeEligibilityResponse{
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: lintProductVariantsReply.GetProductVariant().GetAudit().GetUpdatedBy(),
			},
			Description: lintProductVariantsReply.GetProductVariant().GetComponents()["primary"].GetDescription(),
			AttributeId: req.GetAttributeId(),
			Priority:    req.GetAttribute().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: resolveGiftCardsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// NormalizeAttribute normalizes an attribute.
func (s *AttributeValidationServer) NormalizeAttribute(ctx context.Context, req *catalogv1.NormalizeAttributeRequest) (*catalogv1.NormalizeAttributeResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "normalize attribute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	estimateProductVariantRequest := &catalogv1.EstimateProductVariantRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetAttribute().GetAudit().GetCreatedBy()},
			Owner: &identityv1.UserRef{UserId: req.GetAttribute().GetOwner().GetUserId()},
		},
	}
	estimateProductVariantReply, err := s.productVariantCalculationPort.EstimateProductVariant(ctx, estimateProductVariantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/EstimateProductVariant")
	}
	return &catalogv1.NormalizeAttributeResponse{
		Attribute: &catalogv1.Attribute{
			Name:        estimateProductVariantReply.GetProductVariant().GetName(),
			AttributeId: req.GetAttributeId(),
			Effective:   req.GetAttribute().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainAttributeDecision explains an attribute.
func (s *AttributeValidationServer) ExplainAttributeDecision(ctx context.Context, req *catalogv1.ExplainAttributeDecisionRequest) (*catalogv1.ExplainAttributeDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainAttributeDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "explain attribute decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	recalculateCategoriesRequest := buildRecalculateCategoriesRequestForExplainAttributeDecision(req)
	recalculateCategoriesReply, err := s.categoryCalculationClient.RecalculateCategories(ctx, recalculateCategoriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/RecalculateCategories")
	}
	return &catalogv1.ExplainAttributeDecisionResponse{
		Result: &commonv1.OperationResult{
			Message: recalculateCategoriesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Attribute: &catalogv1.Attribute{
			AttributeId: req.GetAttributeId(),
			Status:      req.GetAttribute().GetStatus(),
		},
	}, nil
}

// ScoreAttribute scores an attribute.
func (s *AttributeValidationServer) ScoreAttribute(ctx context.Context, req *catalogv1.ScoreAttributeRequest) (*catalogv1.ScoreAttributeResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "score attribute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetAttribute())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.Attribute
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	createVenueRequest := &venuesv1.CreateVenueRequest{}
	createVenueRequest.Context = req.GetContext()
	createVenueRequest.Venue = &venuesv1.Venue{
		Audit: &commonv1.AuditInfo{UpdatedAt: (&decoded).GetAudit().GetUpdatedAt()},
		Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{
			PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
		}},
	}
	createVenueReply, err := s.venueClient.CreateVenue(ctx, createVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/CreateVenue")
	}
	packed, err := anypb.New(req.GetAttribute())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendSearchIndexTestNoticeReply, err := s.searchIndexNotificationClient.SendSearchIndexTestNotice(ctx, &searchv1.SendSearchIndexTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSearchIndexTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: createVenueReply.GetVenue().GetOwner().GetEmail()},
			Payload:   packed,
		},
		Annotations: req.GetAnnotations(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/SendSearchIndexTestNotice")
	}
	return &catalogv1.ScoreAttributeResponse{
		Attribute: &catalogv1.Attribute{
			Owner: createVenueReply.GetVenue().GetOwner(),
			Audit: &commonv1.AuditInfo{
				CreatedBy: sendSearchIndexTestNoticeReply.GetSearchIndex().GetAudit().GetCreatedBy(),
			},
			AttributeId: req.GetAttributeId(),
			Description: req.GetAttribute().GetDescription(),
		},
		Result: &commonv1.OperationResult{
			Details: sendSearchIndexTestNoticeReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// LintAttributes lints an attribute.
func (s *AttributeValidationServer) LintAttributes(ctx context.Context, req *catalogv1.LintAttributesRequest) (*catalogv1.LintAttributesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "lint attributes", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	previewSynonymRequest := &searchv1.PreviewSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit:  &commonv1.AuditInfo{CreatedBy: req.GetAttribute().GetAudit().GetCreatedBy()},
			Action: &searchv1.Synonym_Route{Route: &searchv1.SynonymRouteAction{Weight: req.GetAttribute().GetRoute().GetWeight()}},
			Owner:  &identityv1.UserRef{DisplayName: req.GetAttribute().GetOwner().GetDisplayName()},
		},
	}
	previewSynonymReply, err := previewSynonymCall(ctx, previewSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/PreviewSynonym")
	}
	checkCategoryEligibilityRequest := &catalogv1.CheckCategoryEligibilityRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: req.GetAttribute().GetAudit().GetUpdatedAt(),
						UpdatedBy: req.GetAttribute().GetAudit().GetUpdatedBy(),
					},
				}},
			}},
		},
	}
	checkCategoryEligibilityReply, err := s.meteredCategoryValidation.CheckCategoryEligibility(ctx, checkCategoryEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryValidationService/CheckCategoryEligibility")
	}
	return &catalogv1.LintAttributesResponse{
		Attribute: &catalogv1.Attribute{
			Parameters: previewSynonymReply.GetSynonym().GetParameters(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: checkCategoryEligibilityReply.GetCategory().GetAudit().GetUpdatedAt(),
			},
			AttributeId: req.GetAttributeId(),
			Owner:       req.GetAttribute().GetOwner(),
		},
		Result: &commonv1.OperationResult{
			Status:       previewSynonymReply.GetResult().GetStatus(),
			ReferenceIds: checkCategoryEligibilityReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// GetAttributeRules returns an attribute.
func (s *AttributeValidationServer) GetAttributeRules(ctx context.Context, req *catalogv1.GetAttributeRulesRequest) (*catalogv1.GetAttributeRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "get_attribute_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get attribute rules", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	recalculateCategoriesRequest := &catalogv1.RecalculateCategoriesRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Manager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager:  &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
					Audit:    &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
					Children: []*catalogv1.Category{&catalogv1.Category{ParentId: req.GetAttributeId()}},
				}},
			}},
		},
	}
	recalculateCategoriesReply, err := grpcx.Call(ctx, s.categoryCalculationClient.RecalculateCategories, recalculateCategoriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/RecalculateCategories")
	}
	return &catalogv1.GetAttributeRulesResponse{
		Attribute: &catalogv1.Attribute{
			Owner: &identityv1.UserRef{
				Email: recalculateCategoriesReply.GetCategory().GetManager().GetEmail(),
			},
			AttributeId: req.GetAttributeId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: recalculateCategoriesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// productValidationLazy dials acme.catalog.v1.ProductValidationService the first time a handler needs it.
func (s *AttributeValidationServer) productValidationLazy() catalogv1.ProductValidationServiceClient {
	s.productValidationOnce.Do(func() { s.productValidationCached = catalogv1.NewProductValidationServiceClient(s.conn) })
	return s.productValidationCached
}
