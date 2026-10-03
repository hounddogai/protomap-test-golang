package loyalty

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
)

// listTenantExportsOption sets part of a request of acme.identity.v1.TenantExportService/ListTenantExports.
type listTenantExportsOption func(*identityv1.ListTenantExportsRequest)

// newListTenantExportsRequest builds a request of acme.identity.v1.TenantExportService/ListTenantExports from options.
func newListTenantExportsRequest(options ...listTenantExportsOption) *identityv1.ListTenantExportsRequest {
	request := &identityv1.ListTenantExportsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withListTenantExportsContext(value *commonv1.RequestContext) listTenantExportsOption {
	return func(request *identityv1.ListTenantExportsRequest) { request.Context = value }
}

func withListTenantExportsFilter(value *identityv1.ListTenantExportsRequest_Filter) listTenantExportsOption {
	return func(request *identityv1.ListTenantExportsRequest) { request.Filter = value }
}

// recalculateReviewVotesOption sets part of a request of
// acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes.
type recalculateReviewVotesOption func(*reviewsv1.RecalculateReviewVotesRequest)

// newRecalculateReviewVotesRequest builds a request of
// acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes from options.
func newRecalculateReviewVotesRequest(options ...recalculateReviewVotesOption) *reviewsv1.RecalculateReviewVotesRequest {
	request := &reviewsv1.RecalculateReviewVotesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withRecalculateReviewVotesContext(value *commonv1.RequestContext) recalculateReviewVotesOption {
	return func(request *reviewsv1.RecalculateReviewVotesRequest) { request.Context = value }
}

func withRecalculateReviewVotesReviewVote(value *reviewsv1.ReviewVote) recalculateReviewVotesOption {
	return func(request *reviewsv1.RecalculateReviewVotesRequest) { request.ReviewVote = value }
}

func withRecalculateReviewVotesDetails(value *reviewsv1.RecalculateReviewVotesRequest_Details) recalculateReviewVotesOption {
	return func(request *reviewsv1.RecalculateReviewVotesRequest) { request.Details = value }
}

// explainPointsLedgerCalculationOption sets part of a request of
// acme.loyalty.v1.PointsLedgerCalculationService/ExplainPointsLedgerCalculation.
type explainPointsLedgerCalculationOption func(*loyaltyv1.ExplainPointsLedgerCalculationRequest)

// newExplainPointsLedgerCalculationRequest builds a request of
// acme.loyalty.v1.PointsLedgerCalculationService/ExplainPointsLedgerCalculation from options.
func newExplainPointsLedgerCalculationRequest(options ...explainPointsLedgerCalculationOption) *loyaltyv1.ExplainPointsLedgerCalculationRequest {
	request := &loyaltyv1.ExplainPointsLedgerCalculationRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainPointsLedgerCalculationContext(value *commonv1.RequestContext) explainPointsLedgerCalculationOption {
	return func(request *loyaltyv1.ExplainPointsLedgerCalculationRequest) { request.Context = value }
}

func withExplainPointsLedgerCalculationPointsLedger(value *loyaltyv1.PointsLedger) explainPointsLedgerCalculationOption {
	return func(request *loyaltyv1.ExplainPointsLedgerCalculationRequest) { request.PointsLedger = value }
}

func withExplainPointsLedgerCalculationDetails(value *loyaltyv1.ExplainPointsLedgerCalculationRequest_Details) explainPointsLedgerCalculationOption {
	return func(request *loyaltyv1.ExplainPointsLedgerCalculationRequest) { request.Details = value }
}

// suggestLoyaltyMembersOption sets part of a request of
// acme.loyalty.v1.LoyaltyMemberQueryService/SuggestLoyaltyMembers.
type suggestLoyaltyMembersOption func(*loyaltyv1.SuggestLoyaltyMembersRequest)

// newSuggestLoyaltyMembersRequest builds a request of acme.loyalty.v1.LoyaltyMemberQueryService/SuggestLoyaltyMembers
// from options.
func newSuggestLoyaltyMembersRequest(options ...suggestLoyaltyMembersOption) *loyaltyv1.SuggestLoyaltyMembersRequest {
	request := &loyaltyv1.SuggestLoyaltyMembersRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSuggestLoyaltyMembersContext(value *commonv1.RequestContext) suggestLoyaltyMembersOption {
	return func(request *loyaltyv1.SuggestLoyaltyMembersRequest) { request.Context = value }
}

func withSuggestLoyaltyMembersFilter(value *loyaltyv1.SuggestLoyaltyMembersRequest_Filter) suggestLoyaltyMembersOption {
	return func(request *loyaltyv1.SuggestLoyaltyMembersRequest) { request.Filter = value }
}

// simulateLoyaltyMemberOption sets part of a request of
// acme.loyalty.v1.LoyaltyMemberCalculationService/SimulateLoyaltyMember.
type simulateLoyaltyMemberOption func(*loyaltyv1.SimulateLoyaltyMemberRequest)

// newSimulateLoyaltyMemberRequest builds a request of
// acme.loyalty.v1.LoyaltyMemberCalculationService/SimulateLoyaltyMember from options.
func newSimulateLoyaltyMemberRequest(options ...simulateLoyaltyMemberOption) *loyaltyv1.SimulateLoyaltyMemberRequest {
	request := &loyaltyv1.SimulateLoyaltyMemberRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSimulateLoyaltyMemberContext(value *commonv1.RequestContext) simulateLoyaltyMemberOption {
	return func(request *loyaltyv1.SimulateLoyaltyMemberRequest) { request.Context = value }
}

func withSimulateLoyaltyMemberLoyaltyMember(value *loyaltyv1.LoyaltyMember) simulateLoyaltyMemberOption {
	return func(request *loyaltyv1.SimulateLoyaltyMemberRequest) { request.LoyaltyMember = value }
}

// comparePointsLedgerOptionsOption sets part of a request of
// acme.loyalty.v1.PointsLedgerCalculationService/ComparePointsLedgerOptions.
type comparePointsLedgerOptionsOption func(*loyaltyv1.ComparePointsLedgerOptionsRequest)

// newComparePointsLedgerOptionsRequest builds a request of
// acme.loyalty.v1.PointsLedgerCalculationService/ComparePointsLedgerOptions from options.
func newComparePointsLedgerOptionsRequest(options ...comparePointsLedgerOptionsOption) *loyaltyv1.ComparePointsLedgerOptionsRequest {
	request := &loyaltyv1.ComparePointsLedgerOptionsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withComparePointsLedgerOptionsContext(value *commonv1.RequestContext) comparePointsLedgerOptionsOption {
	return func(request *loyaltyv1.ComparePointsLedgerOptionsRequest) { request.Context = value }
}

func withComparePointsLedgerOptionsPointsLedger(value *loyaltyv1.PointsLedger) comparePointsLedgerOptionsOption {
	return func(request *loyaltyv1.ComparePointsLedgerOptionsRequest) { request.PointsLedger = value }
}

// countUsersOption sets part of a request of acme.identity.v1.UserQueryService/CountUsers.
type countUsersOption func(*identityv1.CountUsersRequest)

// newCountUsersRequest builds a request of acme.identity.v1.UserQueryService/CountUsers from options.
func newCountUsersRequest(options ...countUsersOption) *identityv1.CountUsersRequest {
	request := &identityv1.CountUsersRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCountUsersContext(value *commonv1.RequestContext) countUsersOption {
	return func(request *identityv1.CountUsersRequest) { request.Context = value }
}

func withCountUsersFilter(value *identityv1.CountUsersRequest_Filter) countUsersOption {
	return func(request *identityv1.CountUsersRequest) { request.Filter = value }
}
