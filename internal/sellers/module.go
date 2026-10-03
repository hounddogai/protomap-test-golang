package sellers

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	collectionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/collections/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	secretsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/secrets/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the sellers domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.authorization.v1.AccessPolicyService", authorizationv1.NewAccessPolicyServiceClient(deps.Conn))
	deps.Clients.Put("acme.collections.v1.CollectionCaseService", collectionsv1.NewCollectionCaseServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.PreferenceCalculationService", customersv1.NewPreferenceCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardBatchValidationService", giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardTransactionCalculationService", giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardTransactionValidationService", giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardValidationService", giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.SessionValidationService", identityv1.NewSessionValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.TenantExportService", identityv1.NewTenantExportServiceClient(deps.Conn))
	deps.Clients.Put("acme.returns.v1.ReturnRequestCalculationService", returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.secrets.v1.SecretService", secretsv1.NewSecretServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerCalculationService", sellersv1.NewSellerCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerListingCalculationService", sellersv1.NewSellerListingCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerPayoutScheduleCalculationService", sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerPayoutScheduleValidationService", sellersv1.NewSellerPayoutScheduleValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerScorecardCalculationService", sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerScorecardValidationService", sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerValidationService", sellersv1.NewSellerValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.subscriptions.v1.SubscriptionService", subscriptionsv1.NewSubscriptionServiceClient(deps.Conn))
	sellerServer := NewSellerServer(deps)
	server.RegisterService(&sellersv1.SellerService_ServiceDesc, sellerServer)
	sellersv1.RegisterSellerListingQueryServiceServer(server, NewSellerListingQueryServer(deps))
	sellersv1.RegisterSellerPayoutScheduleWorkflowServiceServer(server, NewSellerPayoutScheduleWorkflowServer(deps))
	sellersv1.RegisterSellerScorecardApprovalServiceServer(server, NewSellerScorecardApprovalServer(deps))
	sellerQueryServer := NewSellerQueryServer(deps)
	server.RegisterService(&sellersv1.SellerQueryService_ServiceDesc, sellerQueryServer)
	sellersv1.RegisterSellerListingWorkflowServiceServer(server, NewSellerListingWorkflowServer(deps))
	sellersv1.RegisterSellerPayoutScheduleApprovalServiceServer(server, NewSellerPayoutScheduleApprovalServer(deps))
	sellersv1.RegisterSellerScorecardValidationServiceServer(server, NewSellerScorecardValidationServer(deps))
	sellersv1.RegisterSellerWorkflowServiceServer(server, NewSellerWorkflowServer(deps))
	sellersv1.RegisterSellerListingApprovalServiceServer(server, NewSellerListingApprovalServer(deps))
	sellersv1.RegisterSellerPayoutScheduleValidationServiceServer(server, NewSellerPayoutScheduleValidationServer(deps))
	sellersv1.RegisterSellerScorecardCalculationServiceServer(server, NewSellerScorecardCalculationServer(deps))
	sellersv1.RegisterSellerApprovalServiceServer(server, NewSellerApprovalServer(deps))
	sellerListingValidationServer := NewSellerListingValidationServer(deps)
	server.RegisterService(&sellersv1.SellerListingValidationService_ServiceDesc, sellerListingValidationServer)
	sellersv1.RegisterSellerPayoutScheduleCalculationServiceServer(server, NewSellerPayoutScheduleCalculationServer(deps))
	sellerScorecardLifecycleServer := NewSellerScorecardLifecycleServer(deps)
	server.RegisterService(&sellersv1.SellerScorecardLifecycleService_ServiceDesc, sellerScorecardLifecycleServer)
	sellersv1.RegisterSellerValidationServiceServer(server, NewSellerValidationServer(deps))
	sellersv1.RegisterSellerListingCalculationServiceServer(server, NewSellerListingCalculationServer(deps))
	sellersv1.RegisterSellerPayoutScheduleLifecycleServiceServer(server, NewSellerPayoutScheduleLifecycleServer(deps))
	sellerScorecardReportingServer := NewSellerScorecardReportingServer(deps)
	server.RegisterService(&sellersv1.SellerScorecardReportingService_ServiceDesc, sellerScorecardReportingServer)
	sellersv1.RegisterSellerCalculationServiceServer(server, NewSellerCalculationServer(deps))
	sellersv1.RegisterSellerListingLifecycleServiceServer(server, NewSellerListingLifecycleServer(deps))
	sellersv1.RegisterSellerPayoutScheduleReportingServiceServer(server, NewSellerPayoutScheduleReportingServer(deps))
	sellerScorecardSyncServer := NewSellerScorecardSyncServer(deps)
	server.RegisterService(&sellersv1.SellerScorecardSyncService_ServiceDesc, sellerScorecardSyncServer)
	sellersv1.RegisterSellerLifecycleServiceServer(server, NewSellerLifecycleServer(deps))
	sellerListingReportingServer := NewSellerListingReportingServer(deps)
	server.RegisterService(&sellersv1.SellerListingReportingService_ServiceDesc, sellerListingReportingServer)
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	summarizeSellerListingsCall          func(context.Context, *sellersv1.SummarizeSellerListingsRequest, ...grpc.CallOption) (*sellersv1.SummarizeSellerListingsResponse, error)
	browseSellerListingsCall             func(context.Context, *sellersv1.BrowseSellerListingsRequest) (*sellersv1.BrowseSellerListingsResponse, error)
	pullSynonymsCall                     func(context.Context, *searchv1.PullSynonymsRequest) (*searchv1.PullSynonymsResponse, error)
	recalculateCustomerSegmentsCall      func(context.Context, *customersv1.RecalculateCustomerSegmentsRequest) (*customersv1.RecalculateCustomerSegmentsResponse, error)
	checkSellerListingEligibilityCall    func(context.Context, *sellersv1.CheckSellerListingEligibilityRequest) (*sellersv1.CheckSellerListingEligibilityResponse, error)
	advanceGiftCardBatchWorkflowCall     func(context.Context, *giftcardsv1.AdvanceGiftCardBatchWorkflowRequest, ...grpc.CallOption) (*giftcardsv1.AdvanceGiftCardBatchWorkflowResponse, error)
	resolveSoftwareLicensesCall          func(context.Context, *licensingv1.ResolveSoftwareLicensesRequest) (*licensingv1.ResolveSoftwareLicensesResponse, error)
	explainCustomerSegmentDecisionCall   func(context.Context, *customersv1.ExplainCustomerSegmentDecisionRequest) (*customersv1.ExplainCustomerSegmentDecisionResponse, error)
	summarizeCustomersCall               func(context.Context, *customersv1.SummarizeCustomersRequest, ...grpc.CallOption) (*customersv1.SummarizeCustomersResponse, error)
	resolveGiftCardBatchesCall           func(context.Context, *giftcardsv1.ResolveGiftCardBatchesRequest) (*giftcardsv1.ResolveGiftCardBatchesResponse, error)
	compareSellerOptionsCall             func(context.Context, *sellersv1.CompareSellerOptionsRequest) (*sellersv1.CompareSellerOptionsResponse, error)
	resolveSellerScorecardConflictCall   func(context.Context, *sellersv1.ResolveSellerScorecardConflictRequest) (*sellersv1.ResolveSellerScorecardConflictResponse, error)
	generateSellerListingReportCall      func(context.Context, *sellersv1.GenerateSellerListingReportRequest, ...grpc.CallOption) (*sellersv1.GenerateSellerListingReportResponse, error)
	validateConsentRecordCall            func(context.Context, *customersv1.ValidateConsentRecordRequest, ...grpc.CallOption) (*customersv1.ValidateConsentRecordResponse, error)
	listGiftCardTransactionWorkflowsCall func(context.Context, *giftcardsv1.ListGiftCardTransactionWorkflowsRequest, ...grpc.CallOption) (*giftcardsv1.ListGiftCardTransactionWorkflowsResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	sellerListingQueryForSummarizeSellerListings := sellersv1.NewSellerListingQueryServiceClient(deps.Conn)
	summarizeSellerListingsCall = sellerListingQueryForSummarizeSellerListings.SummarizeSellerListings
	sellerListingQueryForBrowseSellerListings := sellersv1.NewSellerListingQueryServiceClient(deps.Conn)
	browseSellerListingsCall = func(ctx context.Context, request *sellersv1.BrowseSellerListingsRequest) (*sellersv1.BrowseSellerListingsResponse, error) {
		return sellerListingQueryForBrowseSellerListings.BrowseSellerListings(ctx, request)
	}
	synonymSyncForPullSynonyms := searchv1.NewSynonymSyncServiceClient(deps.Conn)
	pullSynonymsCall = func(ctx context.Context, request *searchv1.PullSynonymsRequest) (*searchv1.PullSynonymsResponse, error) {
		return synonymSyncForPullSynonyms.PullSynonyms(ctx, request)
	}
	customerSegmentCalculationForRecalculateCustomerSegments := customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn)
	recalculateCustomerSegmentsCall = func(ctx context.Context, request *customersv1.RecalculateCustomerSegmentsRequest) (*customersv1.RecalculateCustomerSegmentsResponse, error) {
		return customerSegmentCalculationForRecalculateCustomerSegments.RecalculateCustomerSegments(ctx, request)
	}
	sellerListingValidationForCheckSellerListingEligibility := sellersv1.NewSellerListingValidationServiceClient(deps.Conn)
	checkSellerListingEligibilityCall = func(ctx context.Context, request *sellersv1.CheckSellerListingEligibilityRequest) (*sellersv1.CheckSellerListingEligibilityResponse, error) {
		return sellerListingValidationForCheckSellerListingEligibility.CheckSellerListingEligibility(ctx, request)
	}
	giftCardBatchWorkflowForAdvanceGiftCardBatchWorkflow := giftcardsv1.NewGiftCardBatchWorkflowServiceClient(deps.Conn)
	advanceGiftCardBatchWorkflowCall = giftCardBatchWorkflowForAdvanceGiftCardBatchWorkflow.AdvanceGiftCardBatchWorkflow
	softwareLicenseQueryForResolveSoftwareLicenses := licensingv1.NewSoftwareLicenseQueryServiceClient(deps.Conn)
	resolveSoftwareLicensesCall = func(ctx context.Context, request *licensingv1.ResolveSoftwareLicensesRequest) (*licensingv1.ResolveSoftwareLicensesResponse, error) {
		return softwareLicenseQueryForResolveSoftwareLicenses.ResolveSoftwareLicenses(ctx, request)
	}
	customerSegmentValidationForExplainCustomerSegmentDecision := customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn)
	explainCustomerSegmentDecisionCall = func(ctx context.Context, request *customersv1.ExplainCustomerSegmentDecisionRequest) (*customersv1.ExplainCustomerSegmentDecisionResponse, error) {
		return customerSegmentValidationForExplainCustomerSegmentDecision.ExplainCustomerSegmentDecision(ctx, request)
	}
	customerQueryForSummarizeCustomers := customersv1.NewCustomerQueryServiceClient(deps.Conn)
	summarizeCustomersCall = customerQueryForSummarizeCustomers.SummarizeCustomers
	giftCardBatchQueryForResolveGiftCardBatches := giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn)
	resolveGiftCardBatchesCall = func(ctx context.Context, request *giftcardsv1.ResolveGiftCardBatchesRequest) (*giftcardsv1.ResolveGiftCardBatchesResponse, error) {
		return giftCardBatchQueryForResolveGiftCardBatches.ResolveGiftCardBatches(ctx, request)
	}
	sellerCalculationForCompareSellerOptions := sellersv1.NewSellerCalculationServiceClient(deps.Conn)
	compareSellerOptionsCall = func(ctx context.Context, request *sellersv1.CompareSellerOptionsRequest) (*sellersv1.CompareSellerOptionsResponse, error) {
		return sellerCalculationForCompareSellerOptions.CompareSellerOptions(ctx, request)
	}
	sellerScorecardSyncForResolveSellerScorecardConflict := sellersv1.NewSellerScorecardSyncServiceClient(deps.Conn)
	resolveSellerScorecardConflictCall = func(ctx context.Context, request *sellersv1.ResolveSellerScorecardConflictRequest) (*sellersv1.ResolveSellerScorecardConflictResponse, error) {
		return sellerScorecardSyncForResolveSellerScorecardConflict.ResolveSellerScorecardConflict(ctx, request)
	}
	sellerListingReportingForGenerateSellerListingReport := sellersv1.NewSellerListingReportingServiceClient(deps.Conn)
	generateSellerListingReportCall = sellerListingReportingForGenerateSellerListingReport.GenerateSellerListingReport
	consentRecordValidationForValidateConsentRecord := customersv1.NewConsentRecordValidationServiceClient(deps.Conn)
	validateConsentRecordCall = consentRecordValidationForValidateConsentRecord.ValidateConsentRecord
	giftCardTransactionWorkflowForListGiftCardTransactionWorkflows := giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn)
	listGiftCardTransactionWorkflowsCall = giftCardTransactionWorkflowForListGiftCardTransactionWorkflows.ListGiftCardTransactionWorkflows
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                            *slog.Logger
	sellerScorecardSyncClient      sellersv1.SellerScorecardSyncServiceClient
	sellerListingCalculationClient sellersv1.SellerListingCalculationServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                            deps.Logger,
		sellerScorecardSyncClient:      sellersv1.NewSellerScorecardSyncServiceClient(deps.Conn),
		sellerListingCalculationClient: sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
	}
	go w.consume(ctx, "sellers.resume_seller_payout_schedule_workflow.reconcile_seller_scorecards", deps.Bus.Subscribe("sellers.resume_seller_payout_schedule_workflow.reconcile_seller_scorecards"), w.handleSellersResumeSellerPayoutScheduleWorkflowReconcileSellerScorecards)
	go w.consume(ctx, "sellers.comment_seller_payout_schedule_approval.compare_seller_listing_options", deps.Bus.Subscribe("sellers.comment_seller_payout_schedule_approval.compare_seller_listing_options"), w.handleSellersCommentSellerPayoutScheduleApprovalCompareSellerListingOptions)
}

func (w *workers) consume(ctx context.Context, topic string, events <-chan eventbus.Event, handle func(context.Context, eventbus.Event) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			if err := handle(ctx, event); err != nil {
				w.log.ErrorContext(ctx, "event failed", "topic", topic, "error", err)
			}
		}
	}
}

// handleSellersResumeSellerPayoutScheduleWorkflowReconcileSellerScorecards makes the call the
// sellers.resume_seller_payout_schedule_workflow.reconcile_seller_scorecards event asks for.
func (w *workers) handleSellersResumeSellerPayoutScheduleWorkflowReconcileSellerScorecards(ctx context.Context, event eventbus.Event) error {
	_, err := w.sellerScorecardSyncClient.ReconcileSellerScorecards(ctx, &sellersv1.ReconcileSellerScorecardsRequest{
		SellerScorecard: &sellersv1.SellerScorecard{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: event.Attributes["seller_scorecard.audit.updated_by"],
			},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleSellersCommentSellerPayoutScheduleApprovalCompareSellerListingOptions makes the call the
// sellers.comment_seller_payout_schedule_approval.compare_seller_listing_options event asks for.
func (w *workers) handleSellersCommentSellerPayoutScheduleApprovalCompareSellerListingOptions(ctx context.Context, event eventbus.Event) error {
	_, err := w.sellerListingCalculationClient.CompareSellerListingOptions(ctx, &sellersv1.CompareSellerListingOptionsRequest{
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				Locality: event.Attributes["seller_listing.address.locality"],
			}},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}
