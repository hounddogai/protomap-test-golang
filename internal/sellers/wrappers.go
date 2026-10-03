package sellers

import (
	"context"
	"time"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	modelregistryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/modelregistry/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredCustomerSegmentValidation records how long each call to acme.customers.v1.CustomerSegmentValidationService
// takes.
type meteredCustomerSegmentValidation struct {
	customersv1.CustomerSegmentValidationServiceClient
	recorder *metrics.Recorder
}

// meteredGiftCardBatchApproval records how long each call to acme.giftcards.v1.GiftCardBatchApprovalService takes.
type meteredGiftCardBatchApproval struct {
	giftcardsv1.GiftCardBatchApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredGiftCardBatchApproval) DelegateGiftCardBatchApproval(ctx context.Context, request *giftcardsv1.DelegateGiftCardBatchApprovalRequest, options ...grpc.CallOption) (*giftcardsv1.DelegateGiftCardBatchApprovalResponse, error) {
	defer c.recorder.Observe("acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval", time.Now())
	return c.GiftCardBatchApprovalServiceClient.DelegateGiftCardBatchApproval(ctx, request, options...)
}

// meteredGiftCardTransactionWorkflow records how long each call to acme.giftcards.v1.GiftCardTransactionWorkflowService
// takes.
type meteredGiftCardTransactionWorkflow struct {
	giftcardsv1.GiftCardTransactionWorkflowServiceClient
	recorder *metrics.Recorder
}

func (c meteredGiftCardTransactionWorkflow) ResumeGiftCardTransactionWorkflow(ctx context.Context, request *giftcardsv1.ResumeGiftCardTransactionWorkflowRequest, options ...grpc.CallOption) (*giftcardsv1.ResumeGiftCardTransactionWorkflowResponse, error) {
	defer c.recorder.Observe("acme.giftcards.v1.GiftCardTransactionWorkflowService/ResumeGiftCardTransactionWorkflow", time.Now())
	return c.GiftCardTransactionWorkflowServiceClient.ResumeGiftCardTransactionWorkflow(ctx, request, options...)
}

// meteredHouseholdCalculation records how long each call to acme.customers.v1.HouseholdCalculationService takes.
type meteredHouseholdCalculation struct {
	customersv1.HouseholdCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredHouseholdCalculation) CompareHouseholdOptions(ctx context.Context, request *customersv1.CompareHouseholdOptionsRequest, options ...grpc.CallOption) (*customersv1.CompareHouseholdOptionsResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions", time.Now())
	return c.HouseholdCalculationServiceClient.CompareHouseholdOptions(ctx, request, options...)
}

func (c meteredHouseholdCalculation) ExplainHouseholdCalculation(ctx context.Context, request *customersv1.ExplainHouseholdCalculationRequest, options ...grpc.CallOption) (*customersv1.ExplainHouseholdCalculationResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.HouseholdCalculationService/ExplainHouseholdCalculation", time.Now())
	return c.HouseholdCalculationServiceClient.ExplainHouseholdCalculation(ctx, request, options...)
}

// meteredHouseholdQuery records how long each call to acme.customers.v1.HouseholdQueryService takes.
type meteredHouseholdQuery struct {
	customersv1.HouseholdQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredHouseholdQuery) BrowseHouseholds(ctx context.Context, request *customersv1.BrowseHouseholdsRequest, options ...grpc.CallOption) (*customersv1.BrowseHouseholdsResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.HouseholdQueryService/BrowseHouseholds", time.Now())
	return c.HouseholdQueryServiceClient.BrowseHouseholds(ctx, request, options...)
}

// meteredRegisteredModel records how long each call to acme.modelregistry.v1.RegisteredModelService takes.
type meteredRegisteredModel struct {
	modelregistryv1.RegisteredModelServiceClient
	recorder *metrics.Recorder
}

func (c meteredRegisteredModel) UpdateRegisteredModel(ctx context.Context, request *modelregistryv1.UpdateRegisteredModelRequest, options ...grpc.CallOption) (*modelregistryv1.UpdateRegisteredModelResponse, error) {
	defer c.recorder.Observe("acme.modelregistry.v1.RegisteredModelService/UpdateRegisteredModel", time.Now())
	return c.RegisteredModelServiceClient.UpdateRegisteredModel(ctx, request, options...)
}

// meteredSellerListingCalculation records how long each call to acme.sellers.v1.SellerListingCalculationService takes.
type meteredSellerListingCalculation struct {
	sellersv1.SellerListingCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredSellerListingCalculation) CalculateSellerListing(ctx context.Context, request *sellersv1.CalculateSellerListingRequest, options ...grpc.CallOption) (*sellersv1.CalculateSellerListingResponse, error) {
	defer c.recorder.Observe("acme.sellers.v1.SellerListingCalculationService/CalculateSellerListing", time.Now())
	return c.SellerListingCalculationServiceClient.CalculateSellerListing(ctx, request, options...)
}

// meteredSellerListingQuery records how long each call to acme.sellers.v1.SellerListingQueryService takes.
type meteredSellerListingQuery struct {
	sellersv1.SellerListingQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredSellerListingQuery) SummarizeSellerListings(ctx context.Context, request *sellersv1.SummarizeSellerListingsRequest, options ...grpc.CallOption) (*sellersv1.SummarizeSellerListingsResponse, error) {
	defer c.recorder.Observe("acme.sellers.v1.SellerListingQueryService/SummarizeSellerListings", time.Now())
	return c.SellerListingQueryServiceClient.SummarizeSellerListings(ctx, request, options...)
}

// meteredSellerPayoutScheduleCalculation records how long each call to
// acme.sellers.v1.SellerPayoutScheduleCalculationService takes.
type meteredSellerPayoutScheduleCalculation struct {
	sellersv1.SellerPayoutScheduleCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredSellerPayoutScheduleCalculation) RecalculateSellerPayoutSchedules(ctx context.Context, request *sellersv1.RecalculateSellerPayoutSchedulesRequest, options ...grpc.CallOption) (*sellersv1.RecalculateSellerPayoutSchedulesResponse, error) {
	defer c.recorder.Observe("acme.sellers.v1.SellerPayoutScheduleCalculationService/RecalculateSellerPayoutSchedules", time.Now())
	return c.SellerPayoutScheduleCalculationServiceClient.RecalculateSellerPayoutSchedules(ctx, request, options...)
}

// meteredSellerScorecardApproval records how long each call to acme.sellers.v1.SellerScorecardApprovalService takes.
type meteredSellerScorecardApproval struct {
	sellersv1.SellerScorecardApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredSellerScorecardApproval) RecallSellerScorecard(ctx context.Context, request *sellersv1.RecallSellerScorecardRequest, options ...grpc.CallOption) (*sellersv1.RecallSellerScorecardResponse, error) {
	defer c.recorder.Observe("acme.sellers.v1.SellerScorecardApprovalService/RecallSellerScorecard", time.Now())
	return c.SellerScorecardApprovalServiceClient.RecallSellerScorecard(ctx, request, options...)
}

// meteredSellerScorecardCalculation records how long each call to acme.sellers.v1.SellerScorecardCalculationService
// takes.
type meteredSellerScorecardCalculation struct {
	sellersv1.SellerScorecardCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredSellerScorecardCalculation) QuoteSellerScorecard(ctx context.Context, request *sellersv1.QuoteSellerScorecardRequest, options ...grpc.CallOption) (*sellersv1.QuoteSellerScorecardResponse, error) {
	defer c.recorder.Observe("acme.sellers.v1.SellerScorecardCalculationService/QuoteSellerScorecard", time.Now())
	return c.SellerScorecardCalculationServiceClient.QuoteSellerScorecard(ctx, request, options...)
}

// meteredSoftwareLicense records how long each call to acme.licensing.v1.SoftwareLicenseService takes.
type meteredSoftwareLicense struct {
	licensingv1.SoftwareLicenseServiceClient
	recorder *metrics.Recorder
}

func (c meteredSoftwareLicense) CreateSoftwareLicense(ctx context.Context, request *licensingv1.CreateSoftwareLicenseRequest, options ...grpc.CallOption) (*licensingv1.CreateSoftwareLicenseResponse, error) {
	defer c.recorder.Observe("acme.licensing.v1.SoftwareLicenseService/CreateSoftwareLicense", time.Now())
	return c.SoftwareLicenseServiceClient.CreateSoftwareLicense(ctx, request, options...)
}

// meteredSynonymSync records how long each call to acme.search.v1.SynonymSyncService takes.
type meteredSynonymSync struct {
	searchv1.SynonymSyncServiceClient
	recorder *metrics.Recorder
}

func (c meteredSynonymSync) SyncSynonyms(ctx context.Context, request *searchv1.SyncSynonymsRequest, options ...grpc.CallOption) (*searchv1.SyncSynonymsResponse, error) {
	defer c.recorder.Observe("acme.search.v1.SynonymSyncService/SyncSynonyms", time.Now())
	return c.SynonymSyncServiceClient.SyncSynonyms(ctx, request, options...)
}

// meteredTenantExport records how long each call to acme.identity.v1.TenantExportService takes.
type meteredTenantExport struct {
	identityv1.TenantExportServiceClient
	recorder *metrics.Recorder
}

func (c meteredTenantExport) GetTenantExport(ctx context.Context, request *identityv1.GetTenantExportRequest, options ...grpc.CallOption) (*identityv1.GetTenantExportResponse, error) {
	defer c.recorder.Observe("acme.identity.v1.TenantExportService/GetTenantExport", time.Now())
	return c.TenantExportServiceClient.GetTenantExport(ctx, request, options...)
}
