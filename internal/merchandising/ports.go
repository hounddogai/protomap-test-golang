package merchandising

import (
	"context"
	"log/slog"

	corporateeventsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/corporateevents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	learningv1 "github.com/hounddogai/protomap-test-golang/gen/acme/learning/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// corporateEventQueryPort is what the merchandising domain needs of acme.corporateevents.v1.CorporateEventQueryService.
type corporateEventQueryPort interface {
	FindCorporateEvents(ctx context.Context, request *corporateeventsv1.FindCorporateEventsRequest) (*corporateeventsv1.FindCorporateEventsResponse, error)
}

// grpcCorporateEventQueryPort reaches acme.corporateevents.v1.CorporateEventQueryService over gRPC.
type grpcCorporateEventQueryPort struct {
	client corporateeventsv1.CorporateEventQueryServiceClient
}

// sandboxCorporateEventQueryPort answers in the sandbox environment, where
// acme.corporateevents.v1.CorporateEventQueryService is not deployed.
type sandboxCorporateEventQueryPort struct{ log *slog.Logger }

func (p grpcCorporateEventQueryPort) FindCorporateEvents(ctx context.Context, request *corporateeventsv1.FindCorporateEventsRequest) (*corporateeventsv1.FindCorporateEventsResponse, error) {
	return p.client.FindCorporateEvents(ctx, request)
}

func (p sandboxCorporateEventQueryPort) FindCorporateEvents(ctx context.Context, request *corporateeventsv1.FindCorporateEventsRequest) (*corporateeventsv1.FindCorporateEventsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers FindCorporateEvents")
	return &corporateeventsv1.FindCorporateEventsResponse{}, nil
}

func newCorporateEventQueryPort(deps platform.Deps) corporateEventQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCorporateEventQueryPort{log: deps.Logger}
	}
	return grpcCorporateEventQueryPort{client: corporateeventsv1.NewCorporateEventQueryServiceClient(deps.Conn)}
}

// courseQueryPort is what the merchandising domain needs of acme.learning.v1.CourseQueryService.
type courseQueryPort interface {
	FindCourses(ctx context.Context, request *learningv1.FindCoursesRequest) (*learningv1.FindCoursesResponse, error)
}

// grpcCourseQueryPort reaches acme.learning.v1.CourseQueryService over gRPC.
type grpcCourseQueryPort struct {
	client learningv1.CourseQueryServiceClient
}

// sandboxCourseQueryPort answers in the sandbox environment, where acme.learning.v1.CourseQueryService is not deployed.
type sandboxCourseQueryPort struct{ log *slog.Logger }

func (p grpcCourseQueryPort) FindCourses(ctx context.Context, request *learningv1.FindCoursesRequest) (*learningv1.FindCoursesResponse, error) {
	return p.client.FindCourses(ctx, request)
}

func (p sandboxCourseQueryPort) FindCourses(ctx context.Context, request *learningv1.FindCoursesRequest) (*learningv1.FindCoursesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers FindCourses")
	return &learningv1.FindCoursesResponse{}, nil
}

func newCourseQueryPort(deps platform.Deps) courseQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCourseQueryPort{log: deps.Logger}
	}
	return grpcCourseQueryPort{client: learningv1.NewCourseQueryServiceClient(deps.Conn)}
}

// giftCardBatchCalculationPort is what the merchandising domain needs of
// acme.giftcards.v1.GiftCardBatchCalculationService.
type giftCardBatchCalculationPort interface {
	EstimateGiftCardBatch(ctx context.Context, request *giftcardsv1.EstimateGiftCardBatchRequest) (*giftcardsv1.EstimateGiftCardBatchResponse, error)
}

// grpcGiftCardBatchCalculationPort reaches acme.giftcards.v1.GiftCardBatchCalculationService over gRPC.
type grpcGiftCardBatchCalculationPort struct {
	client giftcardsv1.GiftCardBatchCalculationServiceClient
}

// sandboxGiftCardBatchCalculationPort answers in the sandbox environment, where
// acme.giftcards.v1.GiftCardBatchCalculationService is not deployed.
type sandboxGiftCardBatchCalculationPort struct{ log *slog.Logger }

func (p grpcGiftCardBatchCalculationPort) EstimateGiftCardBatch(ctx context.Context, request *giftcardsv1.EstimateGiftCardBatchRequest) (*giftcardsv1.EstimateGiftCardBatchResponse, error) {
	return p.client.EstimateGiftCardBatch(ctx, request)
}

func (p sandboxGiftCardBatchCalculationPort) EstimateGiftCardBatch(ctx context.Context, request *giftcardsv1.EstimateGiftCardBatchRequest) (*giftcardsv1.EstimateGiftCardBatchResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateGiftCardBatch")
	return &giftcardsv1.EstimateGiftCardBatchResponse{}, nil
}

func newGiftCardBatchCalculationPort(deps platform.Deps) giftCardBatchCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardBatchCalculationPort{log: deps.Logger}
	}
	return grpcGiftCardBatchCalculationPort{client: giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn)}
}

// placementValidationPort is what the merchandising domain needs of acme.merchandising.v1.PlacementValidationService.
type placementValidationPort interface {
	NormalizePlacement(ctx context.Context, request *merchandisingv1.NormalizePlacementRequest) (*merchandisingv1.NormalizePlacementResponse, error)
}

// grpcPlacementValidationPort reaches acme.merchandising.v1.PlacementValidationService over gRPC.
type grpcPlacementValidationPort struct {
	client merchandisingv1.PlacementValidationServiceClient
}

// sandboxPlacementValidationPort answers in the sandbox environment, where
// acme.merchandising.v1.PlacementValidationService is not deployed.
type sandboxPlacementValidationPort struct{ log *slog.Logger }

func (p grpcPlacementValidationPort) NormalizePlacement(ctx context.Context, request *merchandisingv1.NormalizePlacementRequest) (*merchandisingv1.NormalizePlacementResponse, error) {
	return p.client.NormalizePlacement(ctx, request)
}

func (p sandboxPlacementValidationPort) NormalizePlacement(ctx context.Context, request *merchandisingv1.NormalizePlacementRequest) (*merchandisingv1.NormalizePlacementResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers NormalizePlacement")
	return &merchandisingv1.NormalizePlacementResponse{}, nil
}

func newPlacementValidationPort(deps platform.Deps) placementValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPlacementValidationPort{log: deps.Logger}
	}
	return grpcPlacementValidationPort{client: merchandisingv1.NewPlacementValidationServiceClient(deps.Conn)}
}

// sellerQueryPort is what the merchandising domain needs of acme.sellers.v1.SellerQueryService.
type sellerQueryPort interface {
	SummarizeSellers(ctx context.Context, request *sellersv1.SummarizeSellersRequest) (*sellersv1.SummarizeSellersResponse, error)
}

// grpcSellerQueryPort reaches acme.sellers.v1.SellerQueryService over gRPC.
type grpcSellerQueryPort struct {
	client sellersv1.SellerQueryServiceClient
}

// sandboxSellerQueryPort answers in the sandbox environment, where acme.sellers.v1.SellerQueryService is not deployed.
type sandboxSellerQueryPort struct{ log *slog.Logger }

func (p grpcSellerQueryPort) SummarizeSellers(ctx context.Context, request *sellersv1.SummarizeSellersRequest) (*sellersv1.SummarizeSellersResponse, error) {
	return p.client.SummarizeSellers(ctx, request)
}

func (p sandboxSellerQueryPort) SummarizeSellers(ctx context.Context, request *sellersv1.SummarizeSellersRequest) (*sellersv1.SummarizeSellersResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SummarizeSellers")
	return &sellersv1.SummarizeSellersResponse{}, nil
}

func newSellerQueryPort(deps platform.Deps) sellerQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSellerQueryPort{log: deps.Logger}
	}
	return grpcSellerQueryPort{client: sellersv1.NewSellerQueryServiceClient(deps.Conn)}
}

// sellerScorecardLifecyclePort is what the merchandising domain needs of
// acme.sellers.v1.SellerScorecardLifecycleService.
type sellerScorecardLifecyclePort interface {
	ScheduleSellerScorecardExpiry(ctx context.Context, request *sellersv1.ScheduleSellerScorecardExpiryRequest) (*sellersv1.ScheduleSellerScorecardExpiryResponse, error)
}

// grpcSellerScorecardLifecyclePort reaches acme.sellers.v1.SellerScorecardLifecycleService over gRPC.
type grpcSellerScorecardLifecyclePort struct {
	client sellersv1.SellerScorecardLifecycleServiceClient
}

// sandboxSellerScorecardLifecyclePort answers in the sandbox environment, where
// acme.sellers.v1.SellerScorecardLifecycleService is not deployed.
type sandboxSellerScorecardLifecyclePort struct{ log *slog.Logger }

func (p grpcSellerScorecardLifecyclePort) ScheduleSellerScorecardExpiry(ctx context.Context, request *sellersv1.ScheduleSellerScorecardExpiryRequest) (*sellersv1.ScheduleSellerScorecardExpiryResponse, error) {
	return p.client.ScheduleSellerScorecardExpiry(ctx, request)
}

func (p sandboxSellerScorecardLifecyclePort) ScheduleSellerScorecardExpiry(ctx context.Context, request *sellersv1.ScheduleSellerScorecardExpiryRequest) (*sellersv1.ScheduleSellerScorecardExpiryResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ScheduleSellerScorecardExpiry")
	return &sellersv1.ScheduleSellerScorecardExpiryResponse{}, nil
}

func newSellerScorecardLifecyclePort(deps platform.Deps) sellerScorecardLifecyclePort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSellerScorecardLifecyclePort{log: deps.Logger}
	}
	return grpcSellerScorecardLifecyclePort{client: sellersv1.NewSellerScorecardLifecycleServiceClient(deps.Conn)}
}

// userHistoryPort is what the merchandising domain needs of acme.identity.v1.UserHistoryService.
type userHistoryPort interface {
	GetUserVersion(ctx context.Context, request *identityv1.GetUserVersionRequest) (*identityv1.GetUserVersionResponse, error)
}

// grpcUserHistoryPort reaches acme.identity.v1.UserHistoryService over gRPC.
type grpcUserHistoryPort struct {
	client identityv1.UserHistoryServiceClient
}

// sandboxUserHistoryPort answers in the sandbox environment, where acme.identity.v1.UserHistoryService is not deployed.
type sandboxUserHistoryPort struct{ log *slog.Logger }

func (p grpcUserHistoryPort) GetUserVersion(ctx context.Context, request *identityv1.GetUserVersionRequest) (*identityv1.GetUserVersionResponse, error) {
	return p.client.GetUserVersion(ctx, request)
}

func (p sandboxUserHistoryPort) GetUserVersion(ctx context.Context, request *identityv1.GetUserVersionRequest) (*identityv1.GetUserVersionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GetUserVersion")
	return &identityv1.GetUserVersionResponse{}, nil
}

func newUserHistoryPort(deps platform.Deps) userHistoryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxUserHistoryPort{log: deps.Logger}
	}
	return grpcUserHistoryPort{client: identityv1.NewUserHistoryServiceClient(deps.Conn)}
}
