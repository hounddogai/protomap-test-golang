package merchandising

import (
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	surveysv1 "github.com/hounddogai/protomap-test-golang/gen/acme/surveys/v1"
)

// updateAccessPolicyOption sets part of a request of acme.authorization.v1.AccessPolicyService/UpdateAccessPolicy.
type updateAccessPolicyOption func(*authorizationv1.UpdateAccessPolicyRequest)

// newUpdateAccessPolicyRequest builds a request of acme.authorization.v1.AccessPolicyService/UpdateAccessPolicy from
// options.
func newUpdateAccessPolicyRequest(options ...updateAccessPolicyOption) *authorizationv1.UpdateAccessPolicyRequest {
	request := &authorizationv1.UpdateAccessPolicyRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withUpdateAccessPolicyContext(value *commonv1.RequestContext) updateAccessPolicyOption {
	return func(request *authorizationv1.UpdateAccessPolicyRequest) { request.Context = value }
}

func withUpdateAccessPolicyAccessPolicy(value *authorizationv1.AccessPolicy) updateAccessPolicyOption {
	return func(request *authorizationv1.UpdateAccessPolicyRequest) { request.AccessPolicy = value }
}

// browseSurveysOption sets part of a request of acme.surveys.v1.SurveyQueryService/BrowseSurveys.
type browseSurveysOption func(*surveysv1.BrowseSurveysRequest)

// newBrowseSurveysRequest builds a request of acme.surveys.v1.SurveyQueryService/BrowseSurveys from options.
func newBrowseSurveysRequest(options ...browseSurveysOption) *surveysv1.BrowseSurveysRequest {
	request := &surveysv1.BrowseSurveysRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withBrowseSurveysContext(value *commonv1.RequestContext) browseSurveysOption {
	return func(request *surveysv1.BrowseSurveysRequest) { request.Context = value }
}

func withBrowseSurveysFilter(value *surveysv1.BrowseSurveysRequest_Filter) browseSurveysOption {
	return func(request *surveysv1.BrowseSurveysRequest) { request.Filter = value }
}

// findCollectionsOption sets part of a request of acme.merchandising.v1.CollectionQueryService/FindCollections.
type findCollectionsOption func(*merchandisingv1.FindCollectionsRequest)

// newFindCollectionsRequest builds a request of acme.merchandising.v1.CollectionQueryService/FindCollections from
// options.
func newFindCollectionsRequest(options ...findCollectionsOption) *merchandisingv1.FindCollectionsRequest {
	request := &merchandisingv1.FindCollectionsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withFindCollectionsContext(value *commonv1.RequestContext) findCollectionsOption {
	return func(request *merchandisingv1.FindCollectionsRequest) { request.Context = value }
}

func withFindCollectionsFilter(value *merchandisingv1.FindCollectionsRequest_Filter) findCollectionsOption {
	return func(request *merchandisingv1.FindCollectionsRequest) { request.Filter = value }
}

// peekUserOption sets part of a request of acme.identity.v1.UserQueryService/PeekUser.
type peekUserOption func(*identityv1.PeekUserRequest)

// newPeekUserRequest builds a request of acme.identity.v1.UserQueryService/PeekUser from options.
func newPeekUserRequest(options ...peekUserOption) *identityv1.PeekUserRequest {
	request := &identityv1.PeekUserRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withPeekUserContext(value *commonv1.RequestContext) peekUserOption {
	return func(request *identityv1.PeekUserRequest) { request.Context = value }
}

func withPeekUserUserId(value string) peekUserOption {
	return func(request *identityv1.PeekUserRequest) { request.UserId = value }
}

// rejectGiftCardOption sets part of a request of acme.giftcards.v1.GiftCardApprovalService/RejectGiftCard.
type rejectGiftCardOption func(*giftcardsv1.RejectGiftCardRequest)

// newRejectGiftCardRequest builds a request of acme.giftcards.v1.GiftCardApprovalService/RejectGiftCard from options.
func newRejectGiftCardRequest(options ...rejectGiftCardOption) *giftcardsv1.RejectGiftCardRequest {
	request := &giftcardsv1.RejectGiftCardRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withRejectGiftCardContext(value *commonv1.RequestContext) rejectGiftCardOption {
	return func(request *giftcardsv1.RejectGiftCardRequest) { request.Context = value }
}

func withRejectGiftCardGiftCard(value *giftcardsv1.GiftCard) rejectGiftCardOption {
	return func(request *giftcardsv1.RejectGiftCardRequest) { request.GiftCard = value }
}

// suggestPlacementsOption sets part of a request of acme.merchandising.v1.PlacementQueryService/SuggestPlacements.
type suggestPlacementsOption func(*merchandisingv1.SuggestPlacementsRequest)

// newSuggestPlacementsRequest builds a request of acme.merchandising.v1.PlacementQueryService/SuggestPlacements from
// options.
func newSuggestPlacementsRequest(options ...suggestPlacementsOption) *merchandisingv1.SuggestPlacementsRequest {
	request := &merchandisingv1.SuggestPlacementsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSuggestPlacementsContext(value *commonv1.RequestContext) suggestPlacementsOption {
	return func(request *merchandisingv1.SuggestPlacementsRequest) { request.Context = value }
}

func withSuggestPlacementsFilter(value *merchandisingv1.SuggestPlacementsRequest_Filter) suggestPlacementsOption {
	return func(request *merchandisingv1.SuggestPlacementsRequest) { request.Filter = value }
}

// estimateGiftCardBatchOption sets part of a request of
// acme.giftcards.v1.GiftCardBatchCalculationService/EstimateGiftCardBatch.
type estimateGiftCardBatchOption func(*giftcardsv1.EstimateGiftCardBatchRequest)

// newEstimateGiftCardBatchRequest builds a request of
// acme.giftcards.v1.GiftCardBatchCalculationService/EstimateGiftCardBatch from options.
func newEstimateGiftCardBatchRequest(options ...estimateGiftCardBatchOption) *giftcardsv1.EstimateGiftCardBatchRequest {
	request := &giftcardsv1.EstimateGiftCardBatchRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withEstimateGiftCardBatchContext(value *commonv1.RequestContext) estimateGiftCardBatchOption {
	return func(request *giftcardsv1.EstimateGiftCardBatchRequest) { request.Context = value }
}

func withEstimateGiftCardBatchGiftCardBatch(value *giftcardsv1.GiftCardBatch) estimateGiftCardBatchOption {
	return func(request *giftcardsv1.EstimateGiftCardBatchRequest) { request.GiftCardBatch = value }
}
