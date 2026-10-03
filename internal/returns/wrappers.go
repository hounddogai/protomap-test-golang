package returns

import (
	"context"
	"time"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredConsentRecordCalculation records how long each call to acme.customers.v1.ConsentRecordCalculationService
// takes.
type meteredConsentRecordCalculation struct {
	customersv1.ConsentRecordCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredConsentRecordCalculation) RecalculateConsentRecords(ctx context.Context, request *customersv1.RecalculateConsentRecordsRequest, options ...grpc.CallOption) (*customersv1.RecalculateConsentRecordsResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords", time.Now())
	return c.ConsentRecordCalculationServiceClient.RecalculateConsentRecords(ctx, request, options...)
}

// meteredHouseholdQuery records how long each call to acme.customers.v1.HouseholdQueryService takes.
type meteredHouseholdQuery struct {
	customersv1.HouseholdQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredHouseholdQuery) CountHouseholds(ctx context.Context, request *customersv1.CountHouseholdsRequest, options ...grpc.CallOption) (*customersv1.CountHouseholdsResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.HouseholdQueryService/CountHouseholds", time.Now())
	return c.HouseholdQueryServiceClient.CountHouseholds(ctx, request, options...)
}

// meteredLoyaltyMemberCalculation records how long each call to acme.loyalty.v1.LoyaltyMemberCalculationService takes.
type meteredLoyaltyMemberCalculation struct {
	loyaltyv1.LoyaltyMemberCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredLoyaltyMemberCalculation) CalculateLoyaltyMember(ctx context.Context, request *loyaltyv1.CalculateLoyaltyMemberRequest, options ...grpc.CallOption) (*loyaltyv1.CalculateLoyaltyMemberResponse, error) {
	defer c.recorder.Observe("acme.loyalty.v1.LoyaltyMemberCalculationService/CalculateLoyaltyMember", time.Now())
	return c.LoyaltyMemberCalculationServiceClient.CalculateLoyaltyMember(ctx, request, options...)
}

// meteredPreferenceValidation records how long each call to acme.customers.v1.PreferenceValidationService takes.
type meteredPreferenceValidation struct {
	customersv1.PreferenceValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPreferenceValidation) NormalizePreference(ctx context.Context, request *customersv1.NormalizePreferenceRequest, options ...grpc.CallOption) (*customersv1.NormalizePreferenceResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.PreferenceValidationService/NormalizePreference", time.Now())
	return c.PreferenceValidationServiceClient.NormalizePreference(ctx, request, options...)
}

// meteredReturnRequestValidation records how long each call to acme.returns.v1.ReturnRequestValidationService takes.
type meteredReturnRequestValidation struct {
	returnsv1.ReturnRequestValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredReturnRequestValidation) ScoreReturnRequest(ctx context.Context, request *returnsv1.ScoreReturnRequestRequest, options ...grpc.CallOption) (*returnsv1.ScoreReturnRequestResponse, error) {
	defer c.recorder.Observe("acme.returns.v1.ReturnRequestValidationService/ScoreReturnRequest", time.Now())
	return c.ReturnRequestValidationServiceClient.ScoreReturnRequest(ctx, request, options...)
}

// meteredReviewVoteApproval records how long each call to acme.reviews.v1.ReviewVoteApprovalService takes.
type meteredReviewVoteApproval struct {
	reviewsv1.ReviewVoteApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredReviewVoteApproval) RejectReviewVote(ctx context.Context, request *reviewsv1.RejectReviewVoteRequest, options ...grpc.CallOption) (*reviewsv1.RejectReviewVoteResponse, error) {
	defer c.recorder.Observe("acme.reviews.v1.ReviewVoteApprovalService/RejectReviewVote", time.Now())
	return c.ReviewVoteApprovalServiceClient.RejectReviewVote(ctx, request, options...)
}

// meteredSearchQueryQuery records how long each call to acme.search.v1.SearchQueryQueryService takes.
type meteredSearchQueryQuery struct {
	searchv1.SearchQueryQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredSearchQueryQuery) CountSearchQueries(ctx context.Context, request *searchv1.CountSearchQueriesRequest, options ...grpc.CallOption) (*searchv1.CountSearchQueriesResponse, error) {
	defer c.recorder.Observe("acme.search.v1.SearchQueryQueryService/CountSearchQueries", time.Now())
	return c.SearchQueryQueryServiceClient.CountSearchQueries(ctx, request, options...)
}

// meteredTenantAdmin records how long each call to acme.identity.v1.TenantAdminService takes.
type meteredTenantAdmin struct {
	identityv1.TenantAdminServiceClient
	recorder *metrics.Recorder
}

func (c meteredTenantAdmin) ReassignTenant(ctx context.Context, request *identityv1.ReassignTenantRequest, options ...grpc.CallOption) (*identityv1.ReassignTenantResponse, error) {
	defer c.recorder.Observe("acme.identity.v1.TenantAdminService/ReassignTenant", time.Now())
	return c.TenantAdminServiceClient.ReassignTenant(ctx, request, options...)
}
