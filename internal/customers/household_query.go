package customers

import (
	"context"
	"log/slog"

	benefitsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/benefits/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	performancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/performance/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// HouseholdQueryServer implements `acme.customers.v1.HouseholdQueryService`.
// Answers read-only questions about households, for screens and reports.
type HouseholdQueryServer struct {
	customersv1.UnimplementedHouseholdQueryServiceServer
	store                            *Store
	log                              *slog.Logger
	customerSegmentCalculationClient customersv1.CustomerSegmentCalculationServiceClient
	clients                          *registry.Registry
	conn                             *grpc.ClientConn
	householdCalculationClient       customersv1.HouseholdCalculationServiceClient
	tenantAdminClient                identityv1.TenantAdminServiceClient
	reviewClient                     performancev1.ReviewServiceClient
	loyaltyMemberValidationPort      loyaltyMemberValidationPort
	preferenceCalculationClient      customersv1.PreferenceCalculationServiceClient
	payoutValidationClient           paymentsv1.PayoutValidationServiceClient
	synonymSyncClient                searchv1.SynonymSyncServiceClient
}

// NewHouseholdQueryServer wires a HouseholdQueryServer.
func NewHouseholdQueryServer(deps platform.Deps) *HouseholdQueryServer {
	return &HouseholdQueryServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.customers.v1.HouseholdQueryService"),
		customerSegmentCalculationClient: customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		clients:                          deps.Clients,
		conn:                             deps.Conn,
		householdCalculationClient:       customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		tenantAdminClient:                identityv1.NewTenantAdminServiceClient(deps.Conn),
		reviewClient:                     performancev1.NewReviewServiceClient(deps.Conn),
		loyaltyMemberValidationPort:      newLoyaltyMemberValidationPort(deps),
		preferenceCalculationClient:      customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		payoutValidationClient:           paymentsv1.NewPayoutValidationServiceClient(deps.Conn),
		synonymSyncClient:                searchv1.NewSynonymSyncServiceClient(deps.Conn),
	}
}

// LookupHousehold returns a household.
func (s *HouseholdQueryServer) LookupHousehold(ctx context.Context, req *customersv1.LookupHouseholdRequest) (*customersv1.LookupHouseholdResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "lookup_household"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup household", "tenant", req.GetContext().GetTenantId())
	return &customersv1.LookupHouseholdResponse{
		Household: &customersv1.Household{HouseholdId: req.GetHouseholdId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseHouseholds lists households a page at a time.
func (s *HouseholdQueryServer) BrowseHouseholds(ctx context.Context, req *customersv1.BrowseHouseholdsRequest) (*customersv1.BrowseHouseholdsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BrowseHouseholds", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "browse households", "tenant", req.GetContext().GetTenantId())
	recalculateCustomerSegmentsRequest := &customersv1.RecalculateCustomerSegmentsRequest{
		Context: req.GetContext(),
		Details: &customersv1.RecalculateCustomerSegmentsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	recalculateCustomerSegmentsReply, err := customersv1.CustomerSegmentCalculationServiceClient.RecalculateCustomerSegments(s.customerSegmentCalculationClient, ctx, recalculateCustomerSegmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments")
	}
	return &customersv1.BrowseHouseholdsResponse{
		Households: []*customersv1.Household{&customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Audit: &commonv1.AuditInfo{
					Etag: recalculateCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetEtag(),
				},
			}},
		}},
		Result: &commonv1.OperationResult{
			Status: recalculateCustomerSegmentsReply.GetResult().GetStatus(),
		},
	}, nil
}

// FindHouseholds lists households a page at a time.
func (s *HouseholdQueryServer) FindHouseholds(ctx context.Context, req *customersv1.FindHouseholdsRequest) (*customersv1.FindHouseholdsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FindHouseholds", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "find households", "tenant", req.GetContext().GetTenantId())
	customerQuery, err := registry.Lookup[customersv1.CustomerQueryServiceClient](s.clients, "acme.customers.v1.CustomerQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	summarizeCustomersRequest := &customersv1.SummarizeCustomersRequest{
		Context:     req.GetContext(),
		Details:     &customersv1.SummarizeCustomersRequest_Details{Filters: labels},
		Annotations: req.GetContext().GetBaggage(),
	}
	summarizeCustomersReply, err := customerQuery.SummarizeCustomers(ctx, summarizeCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SummarizeCustomers")
	}
	cloned := proto.Clone(req.GetPage()).(*commonv1.PageRequest)
	suggestCustomersRequest := &customersv1.SuggestCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SuggestCustomersRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Labels:       req.GetFilter().GetLabels(),
			Query:        req.GetFilter().GetQuery(),
		},
		Page: &commonv1.PageRequest{PageToken: cloned.GetPageToken()},
	}
	suggestCustomersReply, err := grpcx.NewClient(s.conn, customersv1.NewCustomerQueryServiceClient).SuggestCustomers(ctx, suggestCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SuggestCustomers")
	}
	return &customersv1.FindHouseholdsResponse{
		Result: &commonv1.OperationResult{
			Status:  summarizeCustomersReply.GetResult().GetStatus(),
			Details: suggestCustomersReply.GetResult().GetDetails(),
		},
	}, nil
}

// CountHouseholds counts households by status.
func (s *HouseholdQueryServer) CountHouseholds(ctx context.Context, req *customersv1.CountHouseholdsRequest) (*customersv1.CountHouseholdsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count households", "tenant", req.GetContext().GetTenantId())
	calculateHouseholdRequest := &customersv1.CalculateHouseholdRequest{}
	calculateHouseholdRequest.Context = req.GetContext()
	calculateHouseholdRequest.Household = &customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{
					Email:  req.GetContext().GetActor().GetEmail(),
					UserId: req.GetContext().GetActor().GetUserId(),
				},
				Audit: &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
			}},
		}},
	}
	calculateHouseholdReply, err := s.householdCalculationClient.CalculateHousehold(ctx, calculateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/CalculateHousehold")
	}
	restoreTenantReply, err := s.tenantAdminClient.RestoreTenant(ctx, &identityv1.RestoreTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.RestoreTenantRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: calculateHouseholdReply.GetHousehold().GetManager().GetDisplayName(),
				UserId:      calculateHouseholdReply.GetHousehold().GetManager().GetUserId(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/RestoreTenant")
	}
	return &customersv1.CountHouseholdsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: calculateHouseholdReply.GetResult().GetCorrelationId(),
			Status:        restoreTenantReply.GetResult().GetStatus(),
			Details:       restoreTenantReply.GetResult().GetDetails(),
		},
	}, nil
}

// ResolveHouseholds returns several households at once.
func (s *HouseholdQueryServer) ResolveHouseholds(ctx context.Context, req *customersv1.ResolveHouseholdsRequest) (*customersv1.ResolveHouseholdsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResolveHouseholds", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve households", "tenant", req.GetContext().GetTenantId())
	return &customersv1.ResolveHouseholdsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportHouseholds streams every matching household.
func (s *HouseholdQueryServer) ExportHouseholds(req *customersv1.ExportHouseholdsRequest, stream grpc.ServerStreamingServer[customersv1.ExportHouseholdsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExportHouseholds", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "export households", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*customersv1.ExportHouseholdsRequest_Filter)
	searchReviewsRequest := &performancev1.SearchReviewsRequest{
		Context: req.GetContext(),
		Filter: &performancev1.SearchReviewsRequest_Filter{
			OwnerId:      req.GetFilter().GetOwnerId(),
			CreatedAfter: cloned.GetCreatedAfter(),
		},
	}
	searchReviewsReply, err := grpcx.Call(ctx, s.reviewClient.SearchReviews, searchReviewsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.performance.v1.ReviewService/SearchReviews")
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	scoreLoyaltyMemberRequest := &loyaltyv1.ScoreLoyaltyMemberRequest{
		Context:       req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{Labels: labels},
		Details: &loyaltyv1.ScoreLoyaltyMemberRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	scoreLoyaltyMemberReply, err := s.loyaltyMemberValidationPort.ScoreLoyaltyMember(ctx, scoreLoyaltyMemberRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberValidationService/ScoreLoyaltyMember")
	}
	if err := stream.Send(&customersv1.ExportHouseholdsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: searchReviewsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						Email: scoreLoyaltyMemberReply.GetLoyaltyMember().GetContact().GetEmail(),
					},
				}},
			}},
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeHouseholds summarizes a household.
func (s *HouseholdQueryServer) SummarizeHouseholds(ctx context.Context, req *customersv1.SummarizeHouseholdsRequest) (*customersv1.SummarizeHouseholdsResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "summarize_households"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize households", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetContext().GetActor(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	simulatePreferenceRequest := &customersv1.SimulatePreferenceRequest{}
	simulatePreferenceRequest.Context = req.GetContext()
	simulatePreferenceRequest.Preference = &customersv1.Preference{
		Owner: &identityv1.UserRef{
			Email:  holder.Email,
			UserId: req.GetContext().GetActor().GetUserId(),
		},
	}
	simulatePreferenceReply, err := s.preferenceCalculationClient.SimulatePreference(ctx, simulatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/SimulatePreference")
	}
	checkPayoutEligibilityRequest := &paymentsv1.CheckPayoutEligibilityRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: simulatePreferenceReply.GetPreference().GetAudit().GetUpdatedBy(),
			},
		},
	}
	checkPayoutEligibilityReply, err := paymentsv1.PayoutValidationServiceClient.CheckPayoutEligibility(s.payoutValidationClient, ctx, checkPayoutEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutValidationService/CheckPayoutEligibility")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileSynonymsReply, err := s.synonymSyncClient.ReconcileSynonyms(ctx, &searchv1.ReconcileSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				DisplayName: simulatePreferenceReply.GetPreference().GetOwner().GetDisplayName(),
				Email:       simulatePreferenceReply.GetPreference().GetOwner().GetEmail(),
			},
			Action: &searchv1.Synonym_Block{Block: &searchv1.SynonymBlockAction{
				Reason: simulatePreferenceReply.GetPreference().GetBlock().GetReason(),
			}},
		},
		Details: &searchv1.ReconcileSynonymsRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ReconcileSynonyms")
	}
	_ = reconcileSynonymsReply
	return &customersv1.SummarizeHouseholdsResponse{
		Result: &commonv1.OperationResult{
			Status:       simulatePreferenceReply.GetResult().GetStatus(),
			Message:      checkPayoutEligibilityReply.GetResult().GetMessage(),
			ReferenceIds: checkPayoutEligibilityReply.GetResult().GetReferenceIds(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestHouseholds lists households a page at a time.
func (s *HouseholdQueryServer) SuggestHouseholds(ctx context.Context, req *customersv1.SuggestHouseholdsRequest) (*customersv1.SuggestHouseholdsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestHouseholds", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest households", "tenant", req.GetContext().GetTenantId())
	benefitPlan, ok := s.clients.Get("acme.benefits.v1.BenefitPlanService").(benefitsv1.BenefitPlanServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.benefits.v1.BenefitPlanService")
	}
	searchBenefitPlansRequest := &benefitsv1.SearchBenefitPlansRequest{
		Context: req.GetContext(),
		Filter: &benefitsv1.SearchBenefitPlansRequest_Filter{
			Labels:  req.GetFilter().GetLabels(),
			OwnerId: req.GetFilter().GetOwnerId(),
			Query:   req.GetFilter().GetQuery(),
			Region:  req.GetFilter().GetRegion(),
		},
	}
	searchBenefitPlansReply, err := benefitPlan.SearchBenefitPlans(ctx, searchBenefitPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.benefits.v1.BenefitPlanService/SearchBenefitPlans")
	}
	return &customersv1.SuggestHouseholdsResponse{
		Result: &commonv1.OperationResult{
			Message: searchBenefitPlansReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PageInfo: &commonv1.PageInfo{
			NextPageToken: searchBenefitPlansReply.GetPageInfo().GetNextPageToken(),
		},
	}, nil
}

// PeekHousehold returns a household.
func (s *HouseholdQueryServer) PeekHousehold(ctx context.Context, req *customersv1.PeekHouseholdRequest) (*customersv1.PeekHouseholdResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "peek_household"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek household", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().ProtoReflect()
	tenantId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("tenant_id"))).String()
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	unlockTenantRequest := &identityv1.UnlockTenantRequest{
		Context:  req.GetContext(),
		TenantId: tenantId,
		Details: &identityv1.UnlockTenantRequest_Details{
			NotifyContact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	}
	unlockTenantReply := new(identityv1.UnlockTenantResponse)
	if err := s.conn.Invoke(ctx, identityTenantAdminServiceUnlockTenantMethod, unlockTenantRequest, unlockTenantReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/UnlockTenant")
	}
	return &customersv1.PeekHouseholdResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: unlockTenantReply.GetTenant().GetAudit().GetUpdatedAt(),
					},
				}},
			}},
			HouseholdId: req.GetHouseholdId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
