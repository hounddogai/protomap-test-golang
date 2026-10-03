package catalog

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	pipelinesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pipelines/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// AttributeCalculationServer implements `acme.catalog.v1.AttributeCalculationService`.
// Computes the figures of an attribute, such as totals, taxes, and estimates.
type AttributeCalculationServer struct {
	catalogv1.UnimplementedAttributeCalculationServiceServer
	store                        *Store
	log                          *slog.Logger
	accessPolicyValidationClient authorizationv1.AccessPolicyValidationServiceClient
	userClient                   identityv1.UserServiceClient
	userClientFallback           identityv1.UserServiceClient
	conn                         *grpc.ClientConn
	orderNoteCalculationClient   ordersv1.OrderNoteCalculationServiceClient
	notificationClient           notificationsv1.NotificationServiceClient
	pipelineQueryClient          pipelinesv1.PipelineQueryServiceClient
	meteredProductValidation     meteredProductValidation
	compareBrandOptionsFn        func(context.Context, *catalogv1.CompareBrandOptionsRequest, ...grpc.CallOption) (*catalogv1.CompareBrandOptionsResponse, error)
	placementQueryClient         merchandisingv1.PlacementQueryServiceClient
}

// NewAttributeCalculationServer wires an AttributeCalculationServer.
func NewAttributeCalculationServer(deps platform.Deps) *AttributeCalculationServer {
	brandCalculation := catalogv1.NewBrandCalculationServiceClient(deps.Conn)
	return &AttributeCalculationServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.catalog.v1.AttributeCalculationService"),
		accessPolicyValidationClient: authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		userClient:                   identityv1.NewUserServiceClient(deps.Conn),
		userClientFallback:           identityv1.NewUserServiceClient(deps.Conn),
		conn:                         deps.Conn,
		orderNoteCalculationClient:   ordersv1.NewOrderNoteCalculationServiceClient(deps.Conn),
		notificationClient:           notificationsv1.NewNotificationServiceClient(deps.Conn),
		pipelineQueryClient:          pipelinesv1.NewPipelineQueryServiceClient(deps.Conn),
		meteredProductValidation:     meteredProductValidation{ProductValidationServiceClient: catalogv1.NewProductValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		compareBrandOptionsFn:        brandCalculation.CompareBrandOptions,
		placementQueryClient:         merchandisingv1.NewPlacementQueryServiceClient(deps.Conn),
	}
}

// CalculateAttribute calculates an attribute.
func (s *AttributeCalculationServer) CalculateAttribute(ctx context.Context, req *catalogv1.CalculateAttributeRequest) (*catalogv1.CalculateAttributeResponse, error) {
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "calculate attribute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	for _, condition := range req.GetAttribute().GetConditions() {
		if _, err := s.accessPolicyValidationClient.ValidateAccessPolicy(ctx, &authorizationv1.ValidateAccessPolicyRequest{
			Context: req.GetContext(),
			AccessPolicy: &authorizationv1.AccessPolicy{
				Conditions: []*authorizationv1.AccessPolicyCondition{&authorizationv1.AccessPolicyCondition{Value: condition.GetValue()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/ValidateAccessPolicy")
		}
	}
	routes := map[string]func(context.Context, *identityv1.UpdateUserRequest, ...grpc.CallOption) (*identityv1.UpdateUserResponse, error){
		"primary":  s.userClient.UpdateUser,
		"fallback": s.userClientFallback.UpdateUser,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	updateUserRequest := &identityv1.UpdateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
		},
	}
	updateUserReply, err := route(ctx, updateUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/UpdateUser")
	}
	return &catalogv1.CalculateAttributeResponse{
		Attribute: &catalogv1.Attribute{
			Owner: &identityv1.UserRef{
				UserId: updateUserReply.GetUser().GetRelationshipManager().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: updateUserReply.GetUser().GetAudit().GetCreatedAt(),
			},
			AttributeId: req.GetAttributeId(),
			Status:      req.GetAttribute().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateAttribute estimates an attribute.
func (s *AttributeCalculationServer) EstimateAttribute(ctx context.Context, req *catalogv1.EstimateAttributeRequest) (*catalogv1.EstimateAttributeResponse, error) {
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "estimate attribute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	validatePriceOverrideRequest := &pricingv1.ValidatePriceOverrideRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Owner: &identityv1.UserRef{UserId: req.GetAttribute().GetOwner().GetUserId()},
		},
	}
	validatePriceOverrideReply := new(pricingv1.ValidatePriceOverrideResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.pricing.v1.PriceOverrideValidationService/ValidatePriceOverride", validatePriceOverrideRequest, validatePriceOverrideReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideValidationService/ValidatePriceOverride")
	}
	return &catalogv1.EstimateAttributeResponse{
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				CreatedBy: validatePriceOverrideReply.GetPriceOverride().GetAudit().GetCreatedBy(),
			},
			AttributeId: req.GetAttributeId(),
			Name:        req.GetAttribute().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteAttribute quotes an attribute.
func (s *AttributeCalculationServer) QuoteAttribute(ctx context.Context, req *catalogv1.QuoteAttributeRequest) (*catalogv1.QuoteAttributeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteAttribute", "error", err)
		return nil, err
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "quote attribute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetAttribute())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.Attribute
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	compareOrderNoteOptionsRequest := newCompareOrderNoteOptionsRequest(withCompareOrderNoteOptionsContext(req.GetContext()), withCompareOrderNoteOptionsDetails(&ordersv1.CompareOrderNoteOptionsRequest_Details{
		Destination: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
	}), withCompareOrderNoteOptionsOrderNote(&ordersv1.OrderNote{
		Subject: &ordersv1.OrderRef{DisplayName: (&decoded).GetOwner().GetDisplayName()},
	}))
	compareOrderNoteOptionsReply, err := s.orderNoteCalculationClient.CompareOrderNoteOptions(ctx, compareOrderNoteOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/CompareOrderNoteOptions")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := attributeCalculationServerCalculateOrderNoteSteps[mode]
	ctx = tenancy.With(ctx, "display_name", req.GetAttribute().GetOwner().GetDisplayName())
	packed, err := anypb.New(req.GetAttribute())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateOrderNoteRequest := &ordersv1.CalculateOrderNoteRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				Number:      compareOrderNoteOptionsReply.GetOrderNote().GetSubject().GetNumber(),
				DisplayName: tenancy.Value(ctx, "display_name"),
			},
			Detail: packed,
		},
		Details: &ordersv1.CalculateOrderNoteRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
				Locality:   req.GetDetails().GetDestination().GetLocality(),
			},
		},
	}
	calculateOrderNoteReply, err := run(s, ctx, calculateOrderNoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/CalculateOrderNote")
	}
	return &catalogv1.QuoteAttributeResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: compareOrderNoteOptionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Breakdown: &catalogv1.QuoteAttributeResponse_Breakdown{Total: calculateOrderNoteReply.GetBreakdown().GetTotal()},
		Attribute: &catalogv1.Attribute{
			AttributeId: req.GetAttributeId(),
			Name:        req.GetAttribute().GetName(),
		},
	}, nil
}

// CompareAttributeOptions compares an attribute.
func (s *AttributeCalculationServer) CompareAttributeOptions(ctx context.Context, req *catalogv1.CompareAttributeOptionsRequest) (*catalogv1.CompareAttributeOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "compare attribute options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	listNotificationsRequest := buildListNotificationsRequestForCompareAttributeOptions(req)
	listNotificationsReply, err := s.notificationClient.ListNotifications(ctx, listNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/ListNotifications")
	}
	return &catalogv1.CompareAttributeOptionsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listNotificationsReply.GetResult().GetCorrelationId(),
			Status:        listNotificationsReply.GetResult().GetStatus(),
		},
		Attribute: &catalogv1.Attribute{
			AttributeId: req.GetAttributeId(),
			Name:        req.GetAttribute().GetName(),
		},
	}, nil
}

// RecalculateAttributes recalculates an attribute.
func (s *AttributeCalculationServer) RecalculateAttributes(ctx context.Context, req *catalogv1.RecalculateAttributesRequest) (*catalogv1.RecalculateAttributesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculateAttributes", "error", err)
		return nil, err
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "recalculate attributes", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	explainBrandCalculationRequest := &catalogv1.ExplainBrandCalculationRequest{
		Context: req.GetContext(),
		Brand: &catalogv1.Brand{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetAttribute().GetOwner().GetUserId()},
		},
	}
	explainBrandCalculationReply := new(catalogv1.ExplainBrandCalculationResponse)
	if err := s.conn.Invoke(ctx, catalogBrandCalculationServiceExplainBrandCalculationMethod, explainBrandCalculationRequest, explainBrandCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandCalculationService/ExplainBrandCalculation")
	}
	cloned := proto.Clone(req.GetAttribute()).(*catalogv1.Attribute)
	explainProductDecisionRequest := &catalogv1.ExplainProductDecisionRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				Locality: req.GetDetails().GetDestination().GetLocality(),
				Lines:    req.GetDetails().GetDestination().GetLines(),
			}},
			Owner: &identityv1.UserRef{
				Email:       explainBrandCalculationReply.GetBrand().GetContact().GetEmail(),
				DisplayName: cloned.GetOwner().GetDisplayName(),
			},
		},
	}
	explainProductDecisionReply, err := grpcx.NewClient(s.conn, catalogv1.NewProductValidationServiceClient).ExplainProductDecision(ctx, explainProductDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/ExplainProductDecision")
	}
	suggestPipelinesRequest := &pipelinesv1.SuggestPipelinesRequest{}
	suggestPipelinesRequest.Context = req.GetContext()
	suggestPipelinesRequest.Filter = &pipelinesv1.SuggestPipelinesRequest_Filter{
		Region:       explainProductDecisionReply.GetProduct().GetAddress().GetRegion(),
		CreatedAfter: explainProductDecisionReply.GetProduct().GetAudit().GetUpdatedAt(),
		OwnerId:      explainBrandCalculationReply.GetBrand().GetRelationshipManager().GetUserId(),
		Labels:       explainProductDecisionReply.GetResult().GetDetails(),
	}
	suggestPipelinesReply, err := s.pipelineQueryClient.SuggestPipelines(ctx, suggestPipelinesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pipelines.v1.PipelineQueryService/SuggestPipelines")
	}
	scoreProductRequest := &catalogv1.ScoreProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainBrandCalculationReply.GetBrand().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{DisplayName: req.GetAttribute().GetOwner().GetDisplayName()},
		},
	}
	scoreProductReply, err := s.meteredProductValidation.ScoreProduct(ctx, scoreProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/ScoreProduct")
	}
	return &catalogv1.RecalculateAttributesResponse{
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainBrandCalculationReply.GetBrand().GetAudit().GetCreatedBy(),
				UpdatedBy: scoreProductReply.GetProduct().GetAudit().GetUpdatedBy(),
				UpdatedAt: scoreProductReply.GetProduct().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: explainProductDecisionReply.GetProduct().GetOwner().GetDisplayName(),
			},
			AttributeId: req.GetAttributeId(),
			Description: req.GetAttribute().GetDescription(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: explainBrandCalculationReply.GetResult().GetCorrelationId(),
			ReferenceIds:  suggestPipelinesReply.GetResult().GetReferenceIds(),
			Message:       suggestPipelinesReply.GetResult().GetMessage(),
			Status:        "ok",
		},
	}, nil
}

// ExplainAttributeCalculation explains an attribute.
func (s *AttributeCalculationServer) ExplainAttributeCalculation(ctx context.Context, req *catalogv1.ExplainAttributeCalculationRequest) (*catalogv1.ExplainAttributeCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "explain attribute calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	source := req.GetAttribute().GetOwner().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	compareBrandOptionsRequest := &catalogv1.CompareBrandOptionsRequest{
		Context: req.GetContext(),
		Brand: &catalogv1.Brand{
			Audit:               &commonv1.AuditInfo{UpdatedAt: req.GetAttribute().GetAudit().GetUpdatedAt()},
			RelationshipManager: &identityv1.UserRef{DisplayName: displayName},
			Contact:             &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}
	compareBrandOptionsReply, err := s.compareBrandOptionsFn(ctx, compareBrandOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandCalculationService/CompareBrandOptions")
	}
	summarizePlacementsRequest := buildSummarizePlacementsRequestForExplainAttributeCalculation(req)
	summarizePlacementsReply, err := s.placementQueryClient.SummarizePlacements(ctx, summarizePlacementsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/SummarizePlacements")
	}
	encoded, err := protojson.Marshal(req.GetAttribute())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.Attribute
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	calculateLoyaltyMemberRequest := &loyaltyv1.CalculateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{Etag: (&decoded).GetAudit().GetEtag()},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: compareBrandOptionsReply.GetBrand().GetContact().GetPreferredChannel(),
				Email:            req.GetAttribute().GetOwner().GetEmail(),
			},
			Holder: &loyaltyv1.RewardRef{
				DisplayName: compareBrandOptionsReply.GetBrand().GetRelationshipManager().GetDisplayName(),
			},
		},
	}
	calculateLoyaltyMemberReply, err := calculateLoyaltyMemberCall(ctx, calculateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CalculateLoyaltyMember")
	}
	packed, err := anypb.New(req.GetAttribute())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateOrderNoteRequest := &ordersv1.CalculateOrderNoteRequest{}
	calculateOrderNoteRequest.Context = req.GetContext()
	calculateOrderNoteRequest.Details = &ordersv1.CalculateOrderNoteRequest_Details{
		Destination: &commonv1.PostalAddress{
			Region:      compareBrandOptionsReply.GetBrand().GetPrimaryAddress().GetRegion(),
			CountryCode: compareBrandOptionsReply.GetBrand().GetPrimaryAddress().GetCountryCode(),
		},
	}
	calculateOrderNoteRequest.OrderNote = &ordersv1.OrderNote{
		Subject: &ordersv1.OrderRef{
			Number:      compareBrandOptionsReply.GetBrand().GetNationalId().GetNumber(),
			DisplayName: compareBrandOptionsReply.GetBrand().GetRelationshipManager().GetDisplayName(),
		},
		Detail: packed,
	}
	calculateOrderNoteReply, err := s.orderNoteCalculationClient.CalculateOrderNote(ctx, calculateOrderNoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/CalculateOrderNote")
	}
	_ = calculateOrderNoteReply
	return &catalogv1.ExplainAttributeCalculationResponse{
		Attribute: &catalogv1.Attribute{
			Owner: &identityv1.UserRef{
				Email: compareBrandOptionsReply.GetBrand().GetContact().GetEmail(),
			},
			AttributeId: req.GetAttributeId(),
			Effective:   req.GetAttribute().GetEffective(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: summarizePlacementsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  calculateLoyaltyMemberReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Outcome: &catalogv1.ExplainAttributeCalculationResponse_Outcome{Status: summarizePlacementsReply.GetResult().GetStatus()},
	}, nil
}

// GetAttributeRates returns an attribute.
func (s *AttributeCalculationServer) GetAttributeRates(ctx context.Context, req *catalogv1.GetAttributeRatesRequest) (*catalogv1.GetAttributeRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "get_attribute_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get attribute rates", "tenant", req.GetContext().GetTenantId())
	return &catalogv1.GetAttributeRatesResponse{
		Attribute: &catalogv1.Attribute{AttributeId: req.GetAttributeId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// attributeCalculationServerCalculateOrderNoteSteps are the ways to run
// acme.orders.v1.OrderNoteCalculationService/CalculateOrderNote, by name.
var attributeCalculationServerCalculateOrderNoteSteps = map[string]func(*AttributeCalculationServer, context.Context, *ordersv1.CalculateOrderNoteRequest) (*ordersv1.CalculateOrderNoteResponse, error){
	"default": (*AttributeCalculationServer).calculateOrderNoteStep,
	"audited": (*AttributeCalculationServer).calculateOrderNoteAuditedStep,
}

func (s *AttributeCalculationServer) calculateOrderNoteStep(ctx context.Context, request *ordersv1.CalculateOrderNoteRequest) (*ordersv1.CalculateOrderNoteResponse, error) {
	return s.orderNoteCalculationClient.CalculateOrderNote(ctx, request)
}

func (s *AttributeCalculationServer) calculateOrderNoteAuditedStep(ctx context.Context, request *ordersv1.CalculateOrderNoteRequest) (*ordersv1.CalculateOrderNoteResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.orders.v1.OrderNoteCalculationService/CalculateOrderNote")
	return s.orderNoteCalculationClient.CalculateOrderNote(ctx, request)
}
