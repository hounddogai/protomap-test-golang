package reviews

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
)

// validateReviewVoteOption sets part of a request of acme.reviews.v1.ReviewVoteValidationService/ValidateReviewVote.
type validateReviewVoteOption func(*reviewsv1.ValidateReviewVoteRequest)

// newValidateReviewVoteRequest builds a request of acme.reviews.v1.ReviewVoteValidationService/ValidateReviewVote from
// options.
func newValidateReviewVoteRequest(options ...validateReviewVoteOption) *reviewsv1.ValidateReviewVoteRequest {
	request := &reviewsv1.ValidateReviewVoteRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withValidateReviewVoteContext(value *commonv1.RequestContext) validateReviewVoteOption {
	return func(request *reviewsv1.ValidateReviewVoteRequest) { request.Context = value }
}

func withValidateReviewVoteReviewVote(value *reviewsv1.ReviewVote) validateReviewVoteOption {
	return func(request *reviewsv1.ValidateReviewVoteRequest) { request.ReviewVote = value }
}

// calculateSellerScorecardOption sets part of a request of
// acme.sellers.v1.SellerScorecardCalculationService/CalculateSellerScorecard.
type calculateSellerScorecardOption func(*sellersv1.CalculateSellerScorecardRequest)

// newCalculateSellerScorecardRequest builds a request of
// acme.sellers.v1.SellerScorecardCalculationService/CalculateSellerScorecard from options.
func newCalculateSellerScorecardRequest(options ...calculateSellerScorecardOption) *sellersv1.CalculateSellerScorecardRequest {
	request := &sellersv1.CalculateSellerScorecardRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCalculateSellerScorecardContext(value *commonv1.RequestContext) calculateSellerScorecardOption {
	return func(request *sellersv1.CalculateSellerScorecardRequest) { request.Context = value }
}

func withCalculateSellerScorecardSellerScorecard(value *sellersv1.SellerScorecard) calculateSellerScorecardOption {
	return func(request *sellersv1.CalculateSellerScorecardRequest) { request.SellerScorecard = value }
}

func withCalculateSellerScorecardSellerScorecardId(value string) calculateSellerScorecardOption {
	return func(request *sellersv1.CalculateSellerScorecardRequest) { request.SellerScorecardId = value }
}
