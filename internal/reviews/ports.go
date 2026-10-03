package reviews

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// accessPolicyQueryPort is what the reviews domain needs of acme.authorization.v1.AccessPolicyQueryService.
type accessPolicyQueryPort interface {
	FindAccessPolicies(ctx context.Context, request *authorizationv1.FindAccessPoliciesRequest) (*authorizationv1.FindAccessPoliciesResponse, error)
}

// grpcAccessPolicyQueryPort reaches acme.authorization.v1.AccessPolicyQueryService over gRPC.
type grpcAccessPolicyQueryPort struct {
	client authorizationv1.AccessPolicyQueryServiceClient
}

// sandboxAccessPolicyQueryPort answers in the sandbox environment, where acme.authorization.v1.AccessPolicyQueryService
// is not deployed.
type sandboxAccessPolicyQueryPort struct{ log *slog.Logger }

func (p grpcAccessPolicyQueryPort) FindAccessPolicies(ctx context.Context, request *authorizationv1.FindAccessPoliciesRequest) (*authorizationv1.FindAccessPoliciesResponse, error) {
	return p.client.FindAccessPolicies(ctx, request)
}

func (p sandboxAccessPolicyQueryPort) FindAccessPolicies(ctx context.Context, request *authorizationv1.FindAccessPoliciesRequest) (*authorizationv1.FindAccessPoliciesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers FindAccessPolicies")
	return &authorizationv1.FindAccessPoliciesResponse{}, nil
}

func newAccessPolicyQueryPort(deps platform.Deps) accessPolicyQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxAccessPolicyQueryPort{log: deps.Logger}
	}
	return grpcAccessPolicyQueryPort{client: authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn)}
}

// customerCalculationPort is what the reviews domain needs of acme.customers.v1.CustomerCalculationService.
type customerCalculationPort interface {
	QuoteCustomer(ctx context.Context, request *customersv1.QuoteCustomerRequest) (*customersv1.QuoteCustomerResponse, error)
}

// grpcCustomerCalculationPort reaches acme.customers.v1.CustomerCalculationService over gRPC.
type grpcCustomerCalculationPort struct {
	client customersv1.CustomerCalculationServiceClient
}

// sandboxCustomerCalculationPort answers in the sandbox environment, where acme.customers.v1.CustomerCalculationService
// is not deployed.
type sandboxCustomerCalculationPort struct{ log *slog.Logger }

func (p grpcCustomerCalculationPort) QuoteCustomer(ctx context.Context, request *customersv1.QuoteCustomerRequest) (*customersv1.QuoteCustomerResponse, error) {
	return p.client.QuoteCustomer(ctx, request)
}

func (p sandboxCustomerCalculationPort) QuoteCustomer(ctx context.Context, request *customersv1.QuoteCustomerRequest) (*customersv1.QuoteCustomerResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers QuoteCustomer")
	return &customersv1.QuoteCustomerResponse{}, nil
}

func newCustomerCalculationPort(deps platform.Deps) customerCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCustomerCalculationPort{log: deps.Logger}
	}
	return grpcCustomerCalculationPort{client: customersv1.NewCustomerCalculationServiceClient(deps.Conn)}
}

// merchandisingRuleApprovalPort is what the reviews domain needs of
// acme.merchandising.v1.MerchandisingRuleApprovalService.
type merchandisingRuleApprovalPort interface {
	CommentMerchandisingRuleApproval(ctx context.Context, request *merchandisingv1.CommentMerchandisingRuleApprovalRequest) (*merchandisingv1.CommentMerchandisingRuleApprovalResponse, error)
}

// grpcMerchandisingRuleApprovalPort reaches acme.merchandising.v1.MerchandisingRuleApprovalService over gRPC.
type grpcMerchandisingRuleApprovalPort struct {
	client merchandisingv1.MerchandisingRuleApprovalServiceClient
}

// sandboxMerchandisingRuleApprovalPort answers in the sandbox environment, where
// acme.merchandising.v1.MerchandisingRuleApprovalService is not deployed.
type sandboxMerchandisingRuleApprovalPort struct{ log *slog.Logger }

func (p grpcMerchandisingRuleApprovalPort) CommentMerchandisingRuleApproval(ctx context.Context, request *merchandisingv1.CommentMerchandisingRuleApprovalRequest) (*merchandisingv1.CommentMerchandisingRuleApprovalResponse, error) {
	return p.client.CommentMerchandisingRuleApproval(ctx, request)
}

func (p sandboxMerchandisingRuleApprovalPort) CommentMerchandisingRuleApproval(ctx context.Context, request *merchandisingv1.CommentMerchandisingRuleApprovalRequest) (*merchandisingv1.CommentMerchandisingRuleApprovalResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CommentMerchandisingRuleApproval")
	return &merchandisingv1.CommentMerchandisingRuleApprovalResponse{}, nil
}

func newMerchandisingRuleApprovalPort(deps platform.Deps) merchandisingRuleApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxMerchandisingRuleApprovalPort{log: deps.Logger}
	}
	return grpcMerchandisingRuleApprovalPort{client: merchandisingv1.NewMerchandisingRuleApprovalServiceClient(deps.Conn)}
}

// preferenceCalculationPort is what the reviews domain needs of acme.customers.v1.PreferenceCalculationService.
type preferenceCalculationPort interface {
	CalculatePreference(ctx context.Context, request *customersv1.CalculatePreferenceRequest) (*customersv1.CalculatePreferenceResponse, error)
}

// grpcPreferenceCalculationPort reaches acme.customers.v1.PreferenceCalculationService over gRPC.
type grpcPreferenceCalculationPort struct {
	client customersv1.PreferenceCalculationServiceClient
}

// sandboxPreferenceCalculationPort answers in the sandbox environment, where
// acme.customers.v1.PreferenceCalculationService is not deployed.
type sandboxPreferenceCalculationPort struct{ log *slog.Logger }

func (p grpcPreferenceCalculationPort) CalculatePreference(ctx context.Context, request *customersv1.CalculatePreferenceRequest) (*customersv1.CalculatePreferenceResponse, error) {
	return p.client.CalculatePreference(ctx, request)
}

func (p sandboxPreferenceCalculationPort) CalculatePreference(ctx context.Context, request *customersv1.CalculatePreferenceRequest) (*customersv1.CalculatePreferenceResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculatePreference")
	return &customersv1.CalculatePreferenceResponse{}, nil
}

func newPreferenceCalculationPort(deps platform.Deps) preferenceCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPreferenceCalculationPort{log: deps.Logger}
	}
	return grpcPreferenceCalculationPort{client: customersv1.NewPreferenceCalculationServiceClient(deps.Conn)}
}

// productReviewPort is what the reviews domain needs of acme.reviews.v1.ProductReviewService.
type productReviewPort interface {
	ListProductReviews(ctx context.Context, request *reviewsv1.ListProductReviewsRequest) (*reviewsv1.ListProductReviewsResponse, error)
}

// grpcProductReviewPort reaches acme.reviews.v1.ProductReviewService over gRPC.
type grpcProductReviewPort struct {
	client reviewsv1.ProductReviewServiceClient
}

// sandboxProductReviewPort answers in the sandbox environment, where acme.reviews.v1.ProductReviewService is not
// deployed.
type sandboxProductReviewPort struct{ log *slog.Logger }

func (p grpcProductReviewPort) ListProductReviews(ctx context.Context, request *reviewsv1.ListProductReviewsRequest) (*reviewsv1.ListProductReviewsResponse, error) {
	return p.client.ListProductReviews(ctx, request)
}

func (p sandboxProductReviewPort) ListProductReviews(ctx context.Context, request *reviewsv1.ListProductReviewsRequest) (*reviewsv1.ListProductReviewsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ListProductReviews")
	return &reviewsv1.ListProductReviewsResponse{}, nil
}

func newProductReviewPort(deps platform.Deps) productReviewPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxProductReviewPort{log: deps.Logger}
	}
	return grpcProductReviewPort{client: reviewsv1.NewProductReviewServiceClient(deps.Conn)}
}

// reviewModerationCalculationPort is what the reviews domain needs of
// acme.reviews.v1.ReviewModerationCalculationService.
type reviewModerationCalculationPort interface {
	EstimateReviewModeration(ctx context.Context, request *reviewsv1.EstimateReviewModerationRequest) (*reviewsv1.EstimateReviewModerationResponse, error)
}

// grpcReviewModerationCalculationPort reaches acme.reviews.v1.ReviewModerationCalculationService over gRPC.
type grpcReviewModerationCalculationPort struct {
	client reviewsv1.ReviewModerationCalculationServiceClient
}

// sandboxReviewModerationCalculationPort answers in the sandbox environment, where
// acme.reviews.v1.ReviewModerationCalculationService is not deployed.
type sandboxReviewModerationCalculationPort struct{ log *slog.Logger }

func (p grpcReviewModerationCalculationPort) EstimateReviewModeration(ctx context.Context, request *reviewsv1.EstimateReviewModerationRequest) (*reviewsv1.EstimateReviewModerationResponse, error) {
	return p.client.EstimateReviewModeration(ctx, request)
}

func (p sandboxReviewModerationCalculationPort) EstimateReviewModeration(ctx context.Context, request *reviewsv1.EstimateReviewModerationRequest) (*reviewsv1.EstimateReviewModerationResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateReviewModeration")
	return &reviewsv1.EstimateReviewModerationResponse{}, nil
}

func newReviewModerationCalculationPort(deps platform.Deps) reviewModerationCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxReviewModerationCalculationPort{log: deps.Logger}
	}
	return grpcReviewModerationCalculationPort{client: reviewsv1.NewReviewModerationCalculationServiceClient(deps.Conn)}
}

// reviewVoteCalculationPort is what the reviews domain needs of acme.reviews.v1.ReviewVoteCalculationService.
type reviewVoteCalculationPort interface {
	CalculateReviewVote(ctx context.Context, request *reviewsv1.CalculateReviewVoteRequest) (*reviewsv1.CalculateReviewVoteResponse, error)
}

// grpcReviewVoteCalculationPort reaches acme.reviews.v1.ReviewVoteCalculationService over gRPC.
type grpcReviewVoteCalculationPort struct {
	client reviewsv1.ReviewVoteCalculationServiceClient
}

// sandboxReviewVoteCalculationPort answers in the sandbox environment, where
// acme.reviews.v1.ReviewVoteCalculationService is not deployed.
type sandboxReviewVoteCalculationPort struct{ log *slog.Logger }

func (p grpcReviewVoteCalculationPort) CalculateReviewVote(ctx context.Context, request *reviewsv1.CalculateReviewVoteRequest) (*reviewsv1.CalculateReviewVoteResponse, error) {
	return p.client.CalculateReviewVote(ctx, request)
}

func (p sandboxReviewVoteCalculationPort) CalculateReviewVote(ctx context.Context, request *reviewsv1.CalculateReviewVoteRequest) (*reviewsv1.CalculateReviewVoteResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculateReviewVote")
	return &reviewsv1.CalculateReviewVoteResponse{}, nil
}

func newReviewVoteCalculationPort(deps platform.Deps) reviewVoteCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxReviewVoteCalculationPort{log: deps.Logger}
	}
	return grpcReviewVoteCalculationPort{client: reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn)}
}

// sellerApprovalPort is what the reviews domain needs of acme.sellers.v1.SellerApprovalService.
type sellerApprovalPort interface {
	RecallSeller(ctx context.Context, request *sellersv1.RecallSellerRequest) (*sellersv1.RecallSellerResponse, error)
}

// grpcSellerApprovalPort reaches acme.sellers.v1.SellerApprovalService over gRPC.
type grpcSellerApprovalPort struct {
	client sellersv1.SellerApprovalServiceClient
}

// sandboxSellerApprovalPort answers in the sandbox environment, where acme.sellers.v1.SellerApprovalService is not
// deployed.
type sandboxSellerApprovalPort struct{ log *slog.Logger }

func (p grpcSellerApprovalPort) RecallSeller(ctx context.Context, request *sellersv1.RecallSellerRequest) (*sellersv1.RecallSellerResponse, error) {
	return p.client.RecallSeller(ctx, request)
}

func (p sandboxSellerApprovalPort) RecallSeller(ctx context.Context, request *sellersv1.RecallSellerRequest) (*sellersv1.RecallSellerResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RecallSeller")
	return &sellersv1.RecallSellerResponse{}, nil
}

func newSellerApprovalPort(deps platform.Deps) sellerApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSellerApprovalPort{log: deps.Logger}
	}
	return grpcSellerApprovalPort{client: sellersv1.NewSellerApprovalServiceClient(deps.Conn)}
}
