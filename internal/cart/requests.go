package cart

import (
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	boxofficev1 "github.com/hounddogai/protomap-test-golang/gen/acme/boxoffice/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
)

// pushSynonymOption sets part of a request of acme.search.v1.SynonymSyncService/PushSynonym.
type pushSynonymOption func(*searchv1.PushSynonymRequest)

// newPushSynonymRequest builds a request of acme.search.v1.SynonymSyncService/PushSynonym from options.
func newPushSynonymRequest(options ...pushSynonymOption) *searchv1.PushSynonymRequest {
	request := &searchv1.PushSynonymRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withPushSynonymContext(value *commonv1.RequestContext) pushSynonymOption {
	return func(request *searchv1.PushSynonymRequest) { request.Context = value }
}

func withPushSynonymSynonym(value *searchv1.Synonym) pushSynonymOption {
	return func(request *searchv1.PushSynonymRequest) { request.Synonym = value }
}

// compareTranslationKeyOptionsOption sets part of a request of
// acme.localization.v1.TranslationKeyCalculationService/CompareTranslationKeyOptions.
type compareTranslationKeyOptionsOption func(*localizationv1.CompareTranslationKeyOptionsRequest)

// newCompareTranslationKeyOptionsRequest builds a request of
// acme.localization.v1.TranslationKeyCalculationService/CompareTranslationKeyOptions from options.
func newCompareTranslationKeyOptionsRequest(options ...compareTranslationKeyOptionsOption) *localizationv1.CompareTranslationKeyOptionsRequest {
	request := &localizationv1.CompareTranslationKeyOptionsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCompareTranslationKeyOptionsContext(value *commonv1.RequestContext) compareTranslationKeyOptionsOption {
	return func(request *localizationv1.CompareTranslationKeyOptionsRequest) { request.Context = value }
}

func withCompareTranslationKeyOptionsDetails(value *localizationv1.CompareTranslationKeyOptionsRequest_Details) compareTranslationKeyOptionsOption {
	return func(request *localizationv1.CompareTranslationKeyOptionsRequest) { request.Details = value }
}

func withCompareTranslationKeyOptionsTranslationKey(value *localizationv1.TranslationKey) compareTranslationKeyOptionsOption {
	return func(request *localizationv1.CompareTranslationKeyOptionsRequest) { request.TranslationKey = value }
}

// estimateCartOption sets part of a request of acme.cart.v1.CartCalculationService/EstimateCart.
type estimateCartOption func(*cartv1.EstimateCartRequest)

// newEstimateCartRequest builds a request of acme.cart.v1.CartCalculationService/EstimateCart from options.
func newEstimateCartRequest(options ...estimateCartOption) *cartv1.EstimateCartRequest {
	request := &cartv1.EstimateCartRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withEstimateCartContext(value *commonv1.RequestContext) estimateCartOption {
	return func(request *cartv1.EstimateCartRequest) { request.Context = value }
}

func withEstimateCartCart(value *cartv1.Cart) estimateCartOption {
	return func(request *cartv1.EstimateCartRequest) { request.Cart = value }
}

func withEstimateCartAnnotations(value map[string]string) estimateCartOption {
	return func(request *cartv1.EstimateCartRequest) { request.Annotations = value }
}

func withEstimateCartReason(value string) estimateCartOption {
	return func(request *cartv1.EstimateCartRequest) { request.Reason = value }
}

// explainAccessPolicyDecisionOption sets part of a request of
// acme.authorization.v1.AccessPolicyValidationService/ExplainAccessPolicyDecision.
type explainAccessPolicyDecisionOption func(*authorizationv1.ExplainAccessPolicyDecisionRequest)

// newExplainAccessPolicyDecisionRequest builds a request of
// acme.authorization.v1.AccessPolicyValidationService/ExplainAccessPolicyDecision from options.
func newExplainAccessPolicyDecisionRequest(options ...explainAccessPolicyDecisionOption) *authorizationv1.ExplainAccessPolicyDecisionRequest {
	request := &authorizationv1.ExplainAccessPolicyDecisionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainAccessPolicyDecisionContext(value *commonv1.RequestContext) explainAccessPolicyDecisionOption {
	return func(request *authorizationv1.ExplainAccessPolicyDecisionRequest) { request.Context = value }
}

func withExplainAccessPolicyDecisionAccessPolicy(value *authorizationv1.AccessPolicy) explainAccessPolicyDecisionOption {
	return func(request *authorizationv1.ExplainAccessPolicyDecisionRequest) { request.AccessPolicy = value }
}

// summarizeCartsOption sets part of a request of acme.cart.v1.CartQueryService/SummarizeCarts.
type summarizeCartsOption func(*cartv1.SummarizeCartsRequest)

// newSummarizeCartsRequest builds a request of acme.cart.v1.CartQueryService/SummarizeCarts from options.
func newSummarizeCartsRequest(options ...summarizeCartsOption) *cartv1.SummarizeCartsRequest {
	request := &cartv1.SummarizeCartsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSummarizeCartsContext(value *commonv1.RequestContext) summarizeCartsOption {
	return func(request *cartv1.SummarizeCartsRequest) { request.Context = value }
}

func withSummarizeCartsReason(value string) summarizeCartsOption {
	return func(request *cartv1.SummarizeCartsRequest) { request.Reason = value }
}

func withSummarizeCartsAnnotations(value map[string]string) summarizeCartsOption {
	return func(request *cartv1.SummarizeCartsRequest) { request.Annotations = value }
}

func withSummarizeCartsDetails(value *cartv1.SummarizeCartsRequest_Details) summarizeCartsOption {
	return func(request *cartv1.SummarizeCartsRequest) { request.Details = value }
}

// rejectStorageBinOption sets part of a request of acme.warehousing.v1.StorageBinApprovalService/RejectStorageBin.
type rejectStorageBinOption func(*warehousingv1.RejectStorageBinRequest)

// newRejectStorageBinRequest builds a request of acme.warehousing.v1.StorageBinApprovalService/RejectStorageBin from
// options.
func newRejectStorageBinRequest(options ...rejectStorageBinOption) *warehousingv1.RejectStorageBinRequest {
	request := &warehousingv1.RejectStorageBinRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withRejectStorageBinContext(value *commonv1.RequestContext) rejectStorageBinOption {
	return func(request *warehousingv1.RejectStorageBinRequest) { request.Context = value }
}

func withRejectStorageBinStorageBin(value *warehousingv1.StorageBin) rejectStorageBinOption {
	return func(request *warehousingv1.RejectStorageBinRequest) { request.StorageBin = value }
}

// normalizeCartOption sets part of a request of acme.cart.v1.CartValidationService/NormalizeCart.
type normalizeCartOption func(*cartv1.NormalizeCartRequest)

// newNormalizeCartRequest builds a request of acme.cart.v1.CartValidationService/NormalizeCart from options.
func newNormalizeCartRequest(options ...normalizeCartOption) *cartv1.NormalizeCartRequest {
	request := &cartv1.NormalizeCartRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withNormalizeCartContext(value *commonv1.RequestContext) normalizeCartOption {
	return func(request *cartv1.NormalizeCartRequest) { request.Context = value }
}

func withNormalizeCartCart(value *cartv1.Cart) normalizeCartOption {
	return func(request *cartv1.NormalizeCartRequest) { request.Cart = value }
}

// cloneCartOption sets part of a request of acme.cart.v1.CartService/CloneCart.
type cloneCartOption func(*cartv1.CloneCartRequest)

// newCloneCartRequest builds a request of acme.cart.v1.CartService/CloneCart from options.
func newCloneCartRequest(options ...cloneCartOption) *cartv1.CloneCartRequest {
	request := &cartv1.CloneCartRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCloneCartContext(value *commonv1.RequestContext) cloneCartOption {
	return func(request *cartv1.CloneCartRequest) { request.Context = value }
}

func withCloneCartCart(value *cartv1.Cart) cloneCartOption {
	return func(request *cartv1.CloneCartRequest) { request.Cart = value }
}

// estimateBoxOfficeSaleOption sets part of a request of
// acme.boxoffice.v1.BoxOfficeSaleCalculationService/EstimateBoxOfficeSale.
type estimateBoxOfficeSaleOption func(*boxofficev1.EstimateBoxOfficeSaleRequest)

// newEstimateBoxOfficeSaleRequest builds a request of
// acme.boxoffice.v1.BoxOfficeSaleCalculationService/EstimateBoxOfficeSale from options.
func newEstimateBoxOfficeSaleRequest(options ...estimateBoxOfficeSaleOption) *boxofficev1.EstimateBoxOfficeSaleRequest {
	request := &boxofficev1.EstimateBoxOfficeSaleRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withEstimateBoxOfficeSaleContext(value *commonv1.RequestContext) estimateBoxOfficeSaleOption {
	return func(request *boxofficev1.EstimateBoxOfficeSaleRequest) { request.Context = value }
}

func withEstimateBoxOfficeSaleBoxOfficeSale(value *boxofficev1.BoxOfficeSale) estimateBoxOfficeSaleOption {
	return func(request *boxofficev1.EstimateBoxOfficeSaleRequest) { request.BoxOfficeSale = value }
}

// scoreSellerScorecardOption sets part of a request of
// acme.sellers.v1.SellerScorecardValidationService/ScoreSellerScorecard.
type scoreSellerScorecardOption func(*sellersv1.ScoreSellerScorecardRequest)

// newScoreSellerScorecardRequest builds a request of
// acme.sellers.v1.SellerScorecardValidationService/ScoreSellerScorecard from options.
func newScoreSellerScorecardRequest(options ...scoreSellerScorecardOption) *sellersv1.ScoreSellerScorecardRequest {
	request := &sellersv1.ScoreSellerScorecardRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withScoreSellerScorecardContext(value *commonv1.RequestContext) scoreSellerScorecardOption {
	return func(request *sellersv1.ScoreSellerScorecardRequest) { request.Context = value }
}

func withScoreSellerScorecardDetails(value *sellersv1.ScoreSellerScorecardRequest_Details) scoreSellerScorecardOption {
	return func(request *sellersv1.ScoreSellerScorecardRequest) { request.Details = value }
}
