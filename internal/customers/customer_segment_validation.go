package customers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// CustomerSegmentValidationServer implements `acme.customers.v1.CustomerSegmentValidationService`.
// Checks customer segments against the rules before anything acts on them.
type CustomerSegmentValidationServer struct {
	customersv1.UnimplementedCustomerSegmentValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	conn                             *grpc.ClientConn
	promotionValidationOnce          sync.Once
	promotionValidationCached        promotionsv1.PromotionValidationServiceClient
	userOnce                         sync.Once
	userCached                       identityv1.UserServiceClient
	preferenceCalculationClient      customersv1.PreferenceCalculationServiceClient
	customerSegmentCalculationClient customersv1.CustomerSegmentCalculationServiceClient
	householdCalculationClient       customersv1.HouseholdCalculationServiceClient
	bus                              *eventbus.Bus
	consentRecordValidationClient    customersv1.ConsentRecordValidationServiceClient
	customerValidationClient         customersv1.CustomerValidationServiceClient
}

// NewCustomerSegmentValidationServer wires a CustomerSegmentValidationServer.
func NewCustomerSegmentValidationServer(deps platform.Deps) *CustomerSegmentValidationServer {
	return &CustomerSegmentValidationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.customers.v1.CustomerSegmentValidationService"),
		conn:                             deps.Conn,
		preferenceCalculationClient:      customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		customerSegmentCalculationClient: customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		householdCalculationClient:       customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		bus:                              deps.Bus,
		consentRecordValidationClient:    customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		customerValidationClient:         customersv1.NewCustomerValidationServiceClient(deps.Conn),
	}
}

// ValidateCustomerSegment validates a customer segment.
func (s *CustomerSegmentValidationServer) ValidateCustomerSegment(ctx context.Context, req *customersv1.ValidateCustomerSegmentRequest) (*customersv1.ValidateCustomerSegmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidateCustomerSegment", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "validate customer segment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetCustomerSegment().GetOwner().GetEmail())
	cloned := proto.Clone(req.GetCustomerSegment()).(*customersv1.CustomerSegment)
	calculateSellerRequest := &sellersv1.CalculateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				Email:       tenancy.Value(ctx, "email"),
				DisplayName: cloned.GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetCustomerSegment().GetAudit().GetEtag()},
		},
	}
	calculateSellerReply := new(sellersv1.CalculateSellerResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", sellersSellerCalculationServiceName, "CalculateSeller"), calculateSellerRequest, calculateSellerReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/CalculateSeller")
	}
	validatePromotionRequest := &promotionsv1.ValidatePromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetCustomerSegment().GetOwner().GetDisplayName(),
			},
			Action: &promotionsv1.Promotion_Route{Route: &promotionsv1.PromotionRouteAction{Weight: req.GetCustomerSegment().GetRoute().GetWeight()}},
		},
		Details: &promotionsv1.ValidatePromotionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetCustomerSegment().GetOwner().GetEmail()},
		},
	}
	validatePromotionReply, err := s.promotionValidationLazy().ValidatePromotion(ctx, validatePromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/ValidatePromotion")
	}
	return &customersv1.ValidateCustomerSegmentResponse{
		Result: &commonv1.OperationResult{
			Message: calculateSellerReply.GetResult().GetMessage(),
			Status:  validatePromotionReply.GetResult().GetStatus(),
		},
		Outcome: &customersv1.ValidateCustomerSegmentResponse_Outcome{
			Message: calculateSellerReply.GetResult().GetMessage(),
			Status:  validatePromotionReply.GetResult().GetStatus(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Name:              req.GetCustomerSegment().GetName(),
		},
	}, nil
}

// PreviewCustomerSegment previews a customer segment.
func (s *CustomerSegmentValidationServer) PreviewCustomerSegment(ctx context.Context, req *customersv1.PreviewCustomerSegmentRequest) (*customersv1.PreviewCustomerSegmentResponse, error) {
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "preview customer segment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	createUserRequest := &identityv1.CreateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	createUserReply, err := s.userLazy().CreateUser(ctx, createUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/CreateUser")
	}
	return &customersv1.PreviewCustomerSegmentResponse{
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: createUserReply.GetUser().GetAudit().GetUpdatedBy(),
				UpdatedAt: createUserReply.GetUser().GetAudit().GetUpdatedAt(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Parameters:        req.GetCustomerSegment().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckCustomerSegmentEligibility checks a customer segment.
func (s *CustomerSegmentValidationServer) CheckCustomerSegmentEligibility(ctx context.Context, req *customersv1.CheckCustomerSegmentEligibilityRequest) (*customersv1.CheckCustomerSegmentEligibilityResponse, error) {
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "check customer segment eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetCustomerSegment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.CustomerSegment
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	recalculatePreferencesReply, err := s.preferenceCalculationClient.RecalculatePreferences(ctx, &customersv1.RecalculatePreferencesRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCustomerSegment().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{Email: (&decoded).GetOwner().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/RecalculatePreferences")
	}
	validateCategoryRequest := &catalogv1.ValidateCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						CreatedBy: recalculatePreferencesReply.GetPreference().GetAudit().GetCreatedBy(),
					},
					Manager: &identityv1.UserRef{
						Email: recalculatePreferencesReply.GetPreference().GetOwner().GetEmail(),
					},
				}},
			}},
		},
	}
	validateCategoryReply := new(catalogv1.ValidateCategoryResponse)
	if err := s.conn.Invoke(ctx, catalogCategoryValidationServiceValidateCategoryMethod, validateCategoryRequest, validateCategoryReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryValidationService/ValidateCategory")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*customersv1.CompareCustomerSegmentOptionsResponse, len(req.GetCustomerSegment().GetConditions()))
	for i, condition := range req.GetCustomerSegment().GetConditions() {
		group.Go(func() error {
			reply, err := s.customerSegmentCalculationClient.CompareCustomerSegmentOptions(groupCtx, &customersv1.CompareCustomerSegmentOptionsRequest{
				Context: req.GetContext(),
				CustomerSegment: &customersv1.CustomerSegment{
					Conditions: []*customersv1.CustomerSegmentCondition{&customersv1.CustomerSegmentCondition{
						Value:     condition.GetValue(),
						Negate:    condition.GetNegate(),
						Operator:  condition.GetOperator(),
						Attribute: condition.GetAttribute(),
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/CompareCustomerSegmentOptions")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &customersv1.CheckCustomerSegmentEligibilityResponse{
		CustomerSegment: &customersv1.CustomerSegment{
			Effective: recalculatePreferencesReply.GetPreference().GetEffective(),
			Owner: &identityv1.UserRef{
				UserId:      recalculatePreferencesReply.GetPreference().GetOwner().GetUserId(),
				DisplayName: validateCategoryReply.GetCategory().GetManager().GetDisplayName(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Version:           req.GetCustomerSegment().GetVersion(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainCustomerSegmentDecision explains a customer segment.
func (s *CustomerSegmentValidationServer) ExplainCustomerSegmentDecision(ctx context.Context, req *customersv1.ExplainCustomerSegmentDecisionRequest) (*customersv1.ExplainCustomerSegmentDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainCustomerSegmentDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "explain customer segment decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	compareHouseholdOptionsRequest := &customersv1.CompareHouseholdOptionsRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						Email:       req.GetCustomerSegment().GetOwner().GetEmail(),
						DisplayName: req.GetCustomerSegment().GetOwner().GetDisplayName(),
					},
					Audit: &commonv1.AuditInfo{
						UpdatedAt: req.GetCustomerSegment().GetAudit().GetUpdatedAt(),
					},
				}},
				Audit: &commonv1.AuditInfo{Etag: req.GetCustomerSegment().GetAudit().GetEtag()},
			}},
		},
	}
	compareHouseholdOptionsReply, err := customersv1.HouseholdCalculationServiceClient.CompareHouseholdOptions(s.householdCalculationClient, ctx, compareHouseholdOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "customers.explain_customer_segment_decision.recalculate_households",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"household.children.children.location.recipient": compareHouseholdOptionsReply.GetHousehold().GetLocation().GetRecipient(),
			"household.children.children.audit.updated_by":   req.GetCustomerSegment().GetAudit().GetUpdatedBy(),
			"household.children.children.manager.email":      req.GetDetails().GetContact().GetEmail(),
			"context.tenant_id":                              req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	repairSearchIndexRequest := &searchv1.RepairSearchIndexRequest{Context: req.GetContext()}
	repairSearchIndexReply, err := repairSearchIndexCall(ctx, repairSearchIndexRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexAdminService/RepairSearchIndex")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := customerSegmentValidationServerNormalizeConsentRecordSteps[mode]
	normalizeConsentRecordRequest := &customersv1.NormalizeConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{
				Unit: repairSearchIndexReply.GetSearchIndex().GetDimensions().GetUnit(),
			}},
			Attributes: compareHouseholdOptionsReply.GetHousehold().GetAttributes(),
		},
	}
	normalizeConsentRecordReply, err := run(s, ctx, normalizeConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord")
	}
	return &customersv1.ExplainCustomerSegmentDecisionResponse{
		Result: &commonv1.OperationResult{
			Details: compareHouseholdOptionsReply.GetResult().GetDetails(),
			Status:  normalizeConsentRecordReply.GetResult().GetStatus(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				DisplayName: compareHouseholdOptionsReply.GetHousehold().GetManager().GetDisplayName(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Effective:         req.GetCustomerSegment().GetEffective(),
		},
		Outcome: &customersv1.ExplainCustomerSegmentDecisionResponse_Outcome{
			ReferenceId: repairSearchIndexReply.GetOutcome().GetReferenceId(),
			EffectiveAt: normalizeConsentRecordReply.GetOutcome().GetEffectiveAt(),
		},
	}, nil
}

// LintCustomerSegments lints a customer segment.
func (s *CustomerSegmentValidationServer) LintCustomerSegments(ctx context.Context, req *customersv1.LintCustomerSegmentsRequest) (*customersv1.LintCustomerSegmentsResponse, error) {
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "lint customer segments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	previewCustomerRequest := &customersv1.PreviewCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetCustomerSegment().GetAudit().GetCreatedAt(),
			},
		},
	}
	var previewCustomerReply *customersv1.PreviewCustomerResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.CustomerValidationService/PreviewCustomer", func(ctx context.Context) error {
		answer, err := s.customerValidationClient.PreviewCustomer(ctx, previewCustomerRequest)
		previewCustomerReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/PreviewCustomer")
	}
	simulateHouseholdRequest := &customersv1.SimulateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						PostalCode: previewCustomerReply.GetCustomer().GetPrimaryAddress().GetPostalCode(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedBy: previewCustomerReply.GetCustomer().GetAudit().GetCreatedBy(),
						UpdatedBy: previewCustomerReply.GetCustomer().GetAudit().GetUpdatedBy(),
					},
				}},
				Location: &commonv1.PostalAddress{
					CountryCode: previewCustomerReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
				},
			}},
		},
	}
	simulateHouseholdReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.SimulateHouseholdResponse, error) {
		return s.householdCalculationClient.SimulateHousehold(ctx, simulateHouseholdRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/SimulateHousehold")
	}
	return &customersv1.LintCustomerSegmentsResponse{
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email:       previewCustomerReply.GetCustomer().GetContact().GetEmail(),
				DisplayName: previewCustomerReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
				UserId:      simulateHouseholdReply.GetHousehold().GetManager().GetUserId(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Parameters:        req.GetCustomerSegment().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetCustomerSegmentRules returns a customer segment.
func (s *CustomerSegmentValidationServer) GetCustomerSegmentRules(ctx context.Context, req *customersv1.GetCustomerSegmentRulesRequest) (*customersv1.GetCustomerSegmentRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetCustomerSegmentRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "get_customer_segment_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get customer segment rules", "tenant", req.GetContext().GetTenantId())
	return &customersv1.GetCustomerSegmentRulesResponse{
		CustomerSegment: &customersv1.CustomerSegment{CustomerSegmentId: req.GetCustomerSegmentId()},
		Result:          &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// promotionValidationLazy dials acme.promotions.v1.PromotionValidationService the first time a handler needs it.
func (s *CustomerSegmentValidationServer) promotionValidationLazy() promotionsv1.PromotionValidationServiceClient {
	s.promotionValidationOnce.Do(func() { s.promotionValidationCached = promotionsv1.NewPromotionValidationServiceClient(s.conn) })
	return s.promotionValidationCached
}

// userLazy dials acme.identity.v1.UserService the first time a handler needs it.
func (s *CustomerSegmentValidationServer) userLazy() identityv1.UserServiceClient {
	s.userOnce.Do(func() { s.userCached = identityv1.NewUserServiceClient(s.conn) })
	return s.userCached
}

// customerSegmentValidationServerNormalizeConsentRecordSteps are the ways to run
// acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord, by name.
var customerSegmentValidationServerNormalizeConsentRecordSteps = map[string]func(*CustomerSegmentValidationServer, context.Context, *customersv1.NormalizeConsentRecordRequest) (*customersv1.NormalizeConsentRecordResponse, error){
	"default": (*CustomerSegmentValidationServer).normalizeConsentRecordStep,
	"audited": (*CustomerSegmentValidationServer).normalizeConsentRecordAuditedStep,
}

func (s *CustomerSegmentValidationServer) normalizeConsentRecordStep(ctx context.Context, request *customersv1.NormalizeConsentRecordRequest) (*customersv1.NormalizeConsentRecordResponse, error) {
	return s.consentRecordValidationClient.NormalizeConsentRecord(ctx, request)
}

func (s *CustomerSegmentValidationServer) normalizeConsentRecordAuditedStep(ctx context.Context, request *customersv1.NormalizeConsentRecordRequest) (*customersv1.NormalizeConsentRecordResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord")
	return s.consentRecordValidationClient.NormalizeConsentRecord(ctx, request)
}
