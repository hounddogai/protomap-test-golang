package customers

import (
	"context"
	"fmt"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// HouseholdCalculationServer implements `acme.customers.v1.HouseholdCalculationService`.
// Computes the figures of a household, such as totals, taxes, and estimates.
type HouseholdCalculationServer struct {
	customersv1.UnimplementedHouseholdCalculationServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	customerCalculationClient       customersv1.CustomerCalculationServiceClient
	priceListClient                 pricingv1.PriceListServiceClient
	renewalScheduleValidationClient subscriptionsv1.RenewalScheduleValidationServiceClient
	roleHistoryClient               identityv1.RoleHistoryServiceClient
	meteredMetricSeriesValidation   meteredMetricSeriesValidation
	clients                         *registry.Registry
	notificationClient              notificationsv1.NotificationServiceClient
	bus                             *eventbus.Bus
	userQueryClient                 identityv1.UserQueryServiceClient
	meteredCustomerCalculation      meteredCustomerCalculation
}

// NewHouseholdCalculationServer wires a HouseholdCalculationServer.
func NewHouseholdCalculationServer(deps platform.Deps) *HouseholdCalculationServer {
	return &HouseholdCalculationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.customers.v1.HouseholdCalculationService"),
		conn:                            deps.Conn,
		customerCalculationClient:       customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		priceListClient:                 pricingv1.NewPriceListServiceClient(deps.Conn),
		renewalScheduleValidationClient: subscriptionsv1.NewRenewalScheduleValidationServiceClient(deps.Conn),
		roleHistoryClient:               identityv1.NewRoleHistoryServiceClient(deps.Conn),
		meteredMetricSeriesValidation:   meteredMetricSeriesValidation{MetricSeriesValidationServiceClient: metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                         deps.Clients,
		notificationClient:              notificationsv1.NewNotificationServiceClient(deps.Conn),
		bus:                             deps.Bus,
		userQueryClient:                 identityv1.NewUserQueryServiceClient(deps.Conn),
		meteredCustomerCalculation:      meteredCustomerCalculation{CustomerCalculationServiceClient: customersv1.NewCustomerCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// CalculateHousehold calculates a household.
func (s *HouseholdCalculationServer) CalculateHousehold(ctx context.Context, req *customersv1.CalculateHouseholdRequest) (*customersv1.CalculateHouseholdResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "calculate household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	names := collectHouseholdNames(req.GetHousehold(), nil)
	explainCustomerCalculationRequest := &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: req.GetHousehold().GetLocation().GetPostalCode(),
				Lines:      names,
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetHousehold().GetAudit().GetUpdatedAt(),
				CreatedAt: req.GetHousehold().GetAudit().GetCreatedAt(),
			},
		},
	}
	explainCustomerCalculationReply := new(customersv1.ExplainCustomerCalculationResponse)
	if err := s.conn.Invoke(ctx, customersCustomerCalculationServiceExplainCustomerCalculationMethod, explainCustomerCalculationRequest, explainCustomerCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	simulateCustomerRequest := buildSimulateCustomerRequestForCalculateHousehold(req, explainCustomerCalculationReply)
	simulateCustomerReply, err := s.customerCalculationClient.SimulateCustomer(ctx, simulateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/SimulateCustomer")
	}
	return &customersv1.CalculateHouseholdResponse{
		Household: &customersv1.Household{
			Manager: &identityv1.UserRef{
				Email: explainCustomerCalculationReply.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						Region: explainCustomerCalculationReply.GetCustomer().GetPrimaryAddress().GetRegion(),
					},
					Manager: &identityv1.UserRef{
						UserId: simulateCustomerReply.GetCustomer().GetRelationshipManager().GetUserId(),
					},
				}},
			}},
			HouseholdId: req.GetHouseholdId(),
			Depth:       req.GetHousehold().GetDepth(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateHousehold estimates a household.
func (s *HouseholdCalculationServer) EstimateHousehold(ctx context.Context, req *customersv1.EstimateHouseholdRequest) (*customersv1.EstimateHouseholdResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimateHousehold", "error", err)
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "estimate household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	getUserRequest := &identityv1.GetUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetHousehold().GetManager().GetUserId(),
	}
	getUserReply := new(identityv1.GetUserResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.UserService_GetUser_FullMethodName, getUserRequest, getUserReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/GetUser")
	}
	cloned := proto.Clone(req.GetHousehold()).(*customersv1.Household)
	listPriceListsReply, err := s.priceListClient.ListPriceLists(ctx, &pricingv1.ListPriceListsRequest{
		Context: req.GetContext(),
		Filter: &pricingv1.ListPriceListsRequest_Filter{
			Region:       cloned.GetLocation().GetRegion(),
			CreatedAfter: getUserReply.GetUser().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetHousehold().GetManager().GetUserId(),
			Labels:       getUserReply.GetUser().GetAttributes(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListService/ListPriceLists")
	}
	names := collectHouseholdNames(req.GetHousehold(), nil)
	calculateReplenishmentPlanRequest := &inventoryv1.CalculateReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Audit:          &commonv1.AuditInfo{CreatedAt: getUserReply.GetUser().GetAudit().GetCreatedAt()},
			BillingAddress: &commonv1.PostalAddress{Lines: names},
		},
	}
	calculateReplenishmentPlanReply := new(inventoryv1.CalculateReplenishmentPlanResponse)
	if err := grpcx.Invoke(ctx, s.conn, inventoryv1.ReplenishmentPlanCalculationService_CalculateReplenishmentPlan_FullMethodName, calculateReplenishmentPlanRequest, calculateReplenishmentPlanReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/CalculateReplenishmentPlan")
	}
	names2 := collectHouseholdNames(req.GetHousehold(), nil)
	scoreRenewalScheduleRequest := &subscriptionsv1.ScoreRenewalScheduleRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetHousehold().GetManager().GetEmail()},
			}},
			Audit:    &commonv1.AuditInfo{CreatedBy: getUserReply.GetUser().GetAudit().GetCreatedBy()},
			Location: &commonv1.PostalAddress{Lines: names2},
		},
	}
	scoreRenewalScheduleReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*subscriptionsv1.ScoreRenewalScheduleResponse, error) {
		return s.renewalScheduleValidationClient.ScoreRenewalSchedule(ctx, scoreRenewalScheduleRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/ScoreRenewalSchedule")
	}
	return &customersv1.EstimateHouseholdResponse{
		Household: &customersv1.Household{
			Manager: &identityv1.UserRef{
				DisplayName: getUserReply.GetUser().GetRelationshipManager().GetDisplayName(),
			},
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: calculateReplenishmentPlanReply.GetReplenishmentPlan().GetAudit().GetUpdatedAt(),
					},
				}},
				Audit: &commonv1.AuditInfo{
					Etag: calculateReplenishmentPlanReply.GetReplenishmentPlan().GetAudit().GetEtag(),
				},
			}},
			HouseholdId: req.GetHouseholdId(),
			Name:        req.GetHousehold().GetName(),
		},
		Result: &commonv1.OperationResult{
			Details:       listPriceListsReply.GetResult().GetDetails(),
			Status:        listPriceListsReply.GetResult().GetStatus(),
			CorrelationId: scoreRenewalScheduleReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// QuoteHousehold quotes a household.
func (s *HouseholdCalculationServer) QuoteHousehold(ctx context.Context, req *customersv1.QuoteHouseholdRequest) (*customersv1.QuoteHouseholdResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteHousehold", "error", err)
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "quote household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	var holder struct{ Recipient string }
	if record := req.GetDetails().GetDestination(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Recipient"); err != nil {
			s.log.WarnContext(ctx, "copy recipient", "error", err)
		}
	}
	names := collectHouseholdNames(req.GetHousehold(), nil)
	calculateCustomerReply, err := s.customerCalculationClient.CalculateCustomer(ctx, &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Region:    req.GetDetails().GetDestination().GetRegion(),
				Recipient: holder.Recipient,
				Lines:     names,
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetHousehold().GetAudit().GetCreatedBy(),
				CreatedAt: req.GetHousehold().GetAudit().GetCreatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	explainCustomerCalculationRequest := &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Region:   req.GetDetails().GetDestination().GetRegion(),
				Locality: req.GetDetails().GetDestination().GetLocality(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetHousehold().GetManager().GetEmail()},
		},
	}
	explainCustomerCalculationReply, err := grpcx.NewClient(s.conn, customersv1.NewCustomerCalculationServiceClient).ExplainCustomerCalculation(ctx, explainCustomerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	return &customersv1.QuoteHouseholdResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Children: []*customersv1.Household{&customersv1.Household{
						Attributes: calculateCustomerReply.GetCustomer().GetAttributes(),
					}},
					Location: &commonv1.PostalAddress{
						Region: calculateCustomerReply.GetCustomer().GetPrimaryAddress().GetRegion(),
					},
				}},
				Location: &commonv1.PostalAddress{
					Recipient: explainCustomerCalculationReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
				},
				Manager: &identityv1.UserRef{
					UserId: explainCustomerCalculationReply.GetCustomer().GetRelationshipManager().GetUserId(),
				},
			}},
			HouseholdId: req.GetHouseholdId(),
			Attributes:  req.GetHousehold().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareHouseholdOptions compares a household.
func (s *HouseholdCalculationServer) CompareHouseholdOptions(ctx context.Context, req *customersv1.CompareHouseholdOptionsRequest) (*customersv1.CompareHouseholdOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "compare household options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetHousehold()).(*customersv1.Household)
	step := &householdCalculationServerCompareHouseholdOptionsListRoleVersionsStep{request: &identityv1.ListRoleVersionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListRoleVersionsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: cloned.GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetHousehold().GetManager().GetUserId(),
		},
	}}
	if err := s.runCompareHouseholdOptionsListRoleVersions(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/ListRoleVersions")
	}
	listRoleVersionsReply := step.reply
	explainCustomerCalculationReply, err := s.customerCalculationClient.ExplainCustomerCalculation(ctx, &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainCustomerCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetHousehold().GetManager().GetEmail()},
		},
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetHousehold().GetAudit().GetCreatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	householdCache := cache.New[string, *customersv1.Household](1)
	householdCache.Put(req.GetContext().GetTraceId(), req.GetHousehold())
	cachedHousehold, _ := householdCache.Get(req.GetContext().GetTraceId())
	cloned2 := proto.Clone(req.GetHousehold()).(*customersv1.Household)
	lintMetricSeriesesRequest := &metricsv1.LintMetricSeriesesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Owner:   &identityv1.UserRef{Email: req.GetHousehold().GetManager().GetEmail()},
			Locator: &metricsv1.MetricSeries_Address{Address: &commonv1.PostalAddress{Recipient: cachedHousehold.GetLocation().GetRecipient()}},
			Audit:   &commonv1.AuditInfo{UpdatedBy: cloned2.GetAudit().GetUpdatedBy()},
		},
		Details: &metricsv1.LintMetricSeriesesRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: explainCustomerCalculationReply.GetCustomer().GetContact().GetEmail(),
			},
		},
	}
	lintMetricSeriesesReply, err := s.meteredMetricSeriesValidation.LintMetricSerieses(ctx, lintMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses")
	}
	ctx = tenancy.With(ctx, "email", req.GetHousehold().GetManager().GetEmail())
	names := collectHouseholdNames(req.GetHousehold(), nil)
	simulateCustomerRequest := &customersv1.SimulateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{
				Phone: explainCustomerCalculationReply.GetCustomer().GetContact().GetPhone(),
				Email: tenancy.Value(ctx, "email"),
			},
			PrimaryAddress: &commonv1.PostalAddress{Lines: names},
		},
		Details: &customersv1.SimulateCustomerRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: lintMetricSeriesesReply.GetMetricSeries().GetAddress().GetCountryCode(),
			},
		},
	}
	simulateCustomerReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.SimulateCustomerResponse, error) {
		return s.customerCalculationClient.SimulateCustomer(ctx, simulateCustomerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/SimulateCustomer")
	}
	return &customersv1.CompareHouseholdOptionsResponse{
		Result: &commonv1.OperationResult{
			Details:      listRoleVersionsReply.GetResult().GetDetails(),
			ReferenceIds: explainCustomerCalculationReply.GetResult().GetReferenceIds(),
			Status:       lintMetricSeriesesReply.GetOutcome().GetStatus(),
		},
		Household: &customersv1.Household{
			Audit: &commonv1.AuditInfo{
				Etag: lintMetricSeriesesReply.GetMetricSeries().GetAudit().GetEtag(),
			},
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						CountryCode: simulateCustomerReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
					},
				}},
			}},
			Attributes:  simulateCustomerReply.GetCustomer().GetAttributes(),
			HouseholdId: req.GetHouseholdId(),
		},
		Breakdown: &customersv1.CompareHouseholdOptionsResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// RecalculateHouseholds recalculates a household.
func (s *HouseholdCalculationServer) RecalculateHouseholds(ctx context.Context, req *customersv1.RecalculateHouseholdsRequest) (*customersv1.RecalculateHouseholdsResponse, error) {
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "recalculate households", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	names := collectHouseholdNames(req.GetHousehold(), nil)
	summarizePointsLedgersRequest := &loyaltyv1.SummarizePointsLedgersRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details: &loyaltyv1.SummarizePointsLedgersRequest_Details{
			Filters:         req.GetHousehold().GetAttributes(),
			RecipientEmails: names,
		},
	}
	summarizePointsLedgersReply, err := grpcx.NewClient(s.conn, loyaltyv1.NewPointsLedgerQueryServiceClient).SummarizePointsLedgers(ctx, summarizePointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/SummarizePointsLedgers")
	}
	searchIndexQuery, err := registry.Lookup[searchv1.SearchIndexQueryServiceClient](s.clients, "acme.search.v1.SearchIndexQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var holder struct{ Region string }
	if record := req.GetDetails().GetDestination(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	suggestSearchIndexesRequest := &searchv1.SuggestSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.SuggestSearchIndexesRequest_Filter{
			Region:       holder.Region,
			CreatedAfter: req.GetHousehold().GetAudit().GetUpdatedAt(),
			OwnerId:      summarizePointsLedgersReply.GetResult().GetCorrelationId(),
			Labels:       summarizePointsLedgersReply.GetSummary(),
		},
	}
	suggestSearchIndexesReply, err := searchIndexQuery.SuggestSearchIndexes(ctx, suggestSearchIndexesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/SuggestSearchIndexes")
	}
	calculateCustomerRequest := buildCalculateCustomerRequestForRecalculateHouseholds(req)
	calculateCustomerReply, err := s.customerCalculationClient.CalculateCustomer(ctx, calculateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	return &customersv1.RecalculateHouseholdsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizePointsLedgersReply.GetResult().GetCorrelationId(),
			Details:       suggestSearchIndexesReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						DisplayName: calculateCustomerReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
					},
				}},
			}},
			HouseholdId: req.GetHouseholdId(),
			Status:      req.GetHousehold().GetStatus(),
		},
	}, nil
}

// SimulateHousehold simulates a household.
func (s *HouseholdCalculationServer) SimulateHousehold(ctx context.Context, req *customersv1.SimulateHouseholdRequest) (*customersv1.SimulateHouseholdResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "simulate household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	calculateCustomerRequest := &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Details: &customersv1.CalculateCustomerRequest_Details{
			Destination: &commonv1.PostalAddress{Locality: req.GetHousehold().GetLocation().GetLocality()},
		},
	}
	calculateCustomerReply, err := grpcx.Call(ctx, s.customerCalculationClient.CalculateCustomer, calculateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	searchNotificationsRequest := buildSearchNotificationsRequestForSimulateHousehold(req, calculateCustomerReply)
	searchNotificationsReply, err := s.notificationClient.SearchNotifications(ctx, searchNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/SearchNotifications")
	}
	return &customersv1.SimulateHouseholdResponse{
		Household: &customersv1.Household{
			Location: &commonv1.PostalAddress{
				Recipient: calculateCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
				Locality:  calculateCustomerReply.GetCustomer().GetPrimaryAddress().GetLocality(),
			},
			HouseholdId: req.GetHouseholdId(),
			Manager:     req.GetHousehold().GetManager(),
		},
		Result: &commonv1.OperationResult{
			Details: searchNotificationsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ExplainHouseholdCalculation explains a household.
func (s *HouseholdCalculationServer) ExplainHouseholdCalculation(ctx context.Context, req *customersv1.ExplainHouseholdCalculationRequest) (*customersv1.ExplainHouseholdCalculationResponse, error) {
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "explain household calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	names := collectHouseholdNames(req.GetHousehold(), nil)
	calculateCustomerRequest := &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Details: &customersv1.CalculateCustomerRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: req.GetHousehold().GetLocation().GetRecipient()},
		},
		Customer: &customersv1.Customer{
			Contact:        &commonv1.ContactInfo{Email: req.GetHousehold().GetManager().GetEmail()},
			PrimaryAddress: &commonv1.PostalAddress{Lines: names},
		},
	}
	calculateCustomerReply := new(customersv1.CalculateCustomerResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersCustomerCalculationServiceName, "CalculateCustomer"), calculateCustomerRequest, calculateCustomerReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "customers.explain_household_calculation.simulate_customer",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"customer.audit.etag":               req.GetHousehold().GetAudit().GetEtag(),
			"customer.passport.issuing_country": calculateCustomerReply.GetCustomer().GetNationalId().GetIssuingCountry(),
			"context.tenant_id":                 req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	names2 := collectHouseholdNames(req.GetHousehold(), nil)
	resolveUsersReply, err := s.userQueryClient.ResolveUsers(ctx, &identityv1.ResolveUsersRequest{
		Context: req.GetContext(),
		UserIds: names2,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/ResolveUsers")
	}
	names3 := collectHouseholdNames(req.GetHousehold(), nil)
	explainCustomerCalculationRequest := &customersv1.ExplainCustomerCalculationRequest{
		Context:  req.GetContext(),
		Customer: &customersv1.Customer{PrimaryAddress: &commonv1.PostalAddress{Lines: names3}},
	}
	explainCustomerCalculationReply, err := s.meteredCustomerCalculation.ExplainCustomerCalculation(ctx, explainCustomerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	return &customersv1.ExplainHouseholdCalculationResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						Region: calculateCustomerReply.GetCustomer().GetPrimaryAddress().GetRegion(),
					},
				}},
				Audit: &commonv1.AuditInfo{
					UpdatedBy: explainCustomerCalculationReply.GetCustomer().GetAudit().GetUpdatedBy(),
				},
			}},
			HouseholdId: req.GetHouseholdId(),
			Location:    req.GetHousehold().GetLocation(),
		},
		Result: &commonv1.OperationResult{
			Details:      calculateCustomerReply.GetResult().GetDetails(),
			Message:      resolveUsersReply.GetResult().GetMessage(),
			ReferenceIds: resolveUsersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// householdCalculationServerCompareHouseholdOptionsListRoleVersionsStep carries the request of one step of
// CompareHouseholdOptions and, once it ran, its reply.
type householdCalculationServerCompareHouseholdOptionsListRoleVersionsStep struct {
	request *identityv1.ListRoleVersionsRequest
	reply   *identityv1.ListRoleVersionsResponse
}

func (s *HouseholdCalculationServer) runCompareHouseholdOptionsListRoleVersions(ctx context.Context, step *householdCalculationServerCompareHouseholdOptionsListRoleVersionsStep) error {
	reply, err := s.roleHistoryClient.ListRoleVersions(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
