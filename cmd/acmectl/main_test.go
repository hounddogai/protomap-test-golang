package main

import (
	"testing"

	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestInvokeSendsTheRequestOfTheMethod(t *testing.T) {
	method, err := describe("/acme.sellers.v1.SellerScorecardReportingService/DeleteSellerScorecardReport")
	if err != nil {
		t.Fatal(err)
	}
	in := dynamicpb.NewMessage(method.Input())
	if err := protojson.Unmarshal([]byte(`{"context":{"tenantId":"tenant-a"},"sellerScorecardId":"record-7"}`), in); err != nil {
		t.Fatal(err)
	}
	wire, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var request sellersv1.DeleteSellerScorecardReportRequest
	if err := proto.Unmarshal(wire, &request); err != nil {
		t.Fatal(err)
	}
	if request.GetContext().GetTenantId() != "tenant-a" || request.GetSellerScorecardId() != "record-7" {
		t.Fatalf("the method received %v", &request)
	}
}

func TestInvokeFindsTheMethodsOfEveryPackage(t *testing.T) {
	for _, path := range []string{
		"/acme.accounting.v1.ChartOfAccountsQueryService/BrowseChartOfAccountses",
		"/acme.admissions.v1.AdmissionCalculationService/CalculateAdmission",
		"/acme.analytics.v1.AnalyticsEventQueryService/BrowseAnalyticsEvents",
		"/acme.anomalies.v1.AnomalyQueryService/BrowseAnomalies",
		"/acme.appointments.v1.AppointmentQueryService/BrowseAppointments",
		"/acme.assets.v1.FixedAssetCalculationService/CalculateFixedAsset",
		"/acme.audiences.v1.AudienceQueryService/BrowseAudiences",
		"/acme.audit.v1.AuditEventQueryService/BrowseAuditEvents",
		"/acme.authorization.v1.AccessPolicyQueryService/BrowseAccessPolicies",
		"/acme.banking.v1.BankAccountQueryService/BrowseBankAccounts",
		"/acme.benefits.v1.BenefitPlanQueryService/BrowseBenefitPlans",
		"/acme.billing.v1.BillingAccountQueryService/BrowseBillingAccounts",
		"/acme.boxoffice.v1.BoxOfficeSaleCalculationService/CalculateBoxOfficeSale",
		"/acme.broadcasts.v1.BroadcastQueryService/BrowseBroadcasts",
		"/acme.budgeting.v1.BudgetCalculationService/CalculateBudget",
		"/acme.campaigns.v1.MarketingCampaignService/BatchGetMarketingCampaigns",
		"/acme.careplans.v1.CarePlanCalculationService/CalculateCarePlan",
		"/acme.cart.v1.CartApprovalService/ApproveCart",
		"/acme.catalog.v1.AttributeCalculationService/CalculateAttribute",
		"/acme.certificates.v1.CertificateQueryService/BrowseCertificates",
		"/acme.chat.v1.ChatRoomQueryService/BrowseChatRooms",
		"/acme.checkout.v1.CheckoutSessionApprovalService/ApproveCheckoutSession",
		"/acme.collections.v1.CollectionCaseQueryService/BrowseCollectionCases",
		"/acme.community.v1.ForumQueryService/BrowseForums",
		"/acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase",
		"/acme.concessions.v1.ConcessionStandCalculationService/CalculateConcessionStand",
		"/acme.configuration.v1.ConfigSetQueryService/BrowseConfigSets",
		"/acme.content.v1.ArticleQueryService/BrowseArticles",
		"/acme.contracts.v1.ContractQueryService/BrowseContracts",
		"/acme.corporateevents.v1.CorporateEventQueryService/BrowseCorporateEvents",
		"/acme.customers.v1.ConsentRecordApprovalService/ApproveConsentRecord",
		"/acme.datahub.v1.WorkspaceQueryService/BrowseWorkspaces",
		"/acme.datasets.v1.DatasetQueryService/BrowseDatasets",
		"/acme.deliveries.v1.DeliveryQueryService/BrowseDeliveries",
		"/acme.devices.v1.ManagedDeviceCalculationService/CalculateManagedDevice",
		"/acme.dispatch.v1.DispatchOrderQueryService/BrowseDispatchOrders",
		"/acme.documents.v1.DocumentQueryService/BrowseDocuments",
		"/acme.driverapp.v1.DriverHomeQueryService/BrowseDriverHomes",
		"/acme.drivers.v1.DriverCalculationService/CalculateDriver",
		"/acme.edgegateway.v1.RouteBindingQueryService/BrowseRouteBindings",
		"/acme.employeeportal.v1.PortalHomeQueryService/BrowsePortalHomes",
		"/acme.employees.v1.EmployeeQueryService/BrowseEmployees",
		"/acme.expenses.v1.ExpenseReportCalculationService/CalculateExpenseReport",
		"/acme.experiments.v1.ExperimentQueryService/BrowseExperiments",
		"/acme.fanportal.v1.FanSessionQueryService/BrowseFanSessions",
		"/acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags",
		"/acme.featurestore.v1.FeatureViewCalculationService/CalculateFeatureView",
		"/acme.financeportal.v1.FinanceWorkspaceQueryService/BrowseFinanceWorkspaces",
		"/acme.fleet.v1.VehicleService/BatchGetVehicles",
		"/acme.forecasting.v1.DemandForecastQueryService/BrowseDemandForecasts",
		"/acme.forms.v1.FormCalculationService/CalculateForm",
		"/acme.fraud.v1.FraudRuleApprovalService/ApproveFraudRule",
		"/acme.fuel.v1.FuelCardCalculationService/CalculateFuelCard",
		"/acme.fulfillment.v1.FulfillmentExceptionApprovalService/ApproveFulfillmentException",
		"/acme.giftcards.v1.GiftCardApprovalService/ApproveGiftCard",
		"/acme.identity.v1.ApiCredentialAdminService/ArchiveApiCredential",
		"/acme.insurance.v1.PolicyCalculationService/CalculatePolicy",
		"/acme.inventory.v1.ReplenishmentPlanApprovalService/ApproveReplenishmentPlan",
		"/acme.invoicing.v1.InvoiceBatchCalculationService/CalculateInvoiceBatch",
		"/acme.itsm.v1.IncidentCalculationService/CalculateIncident",
		"/acme.keymanagement.v1.CryptoKeyQueryService/BrowseCryptoKeys",
		"/acme.labeling.v1.LabelingTaskCalculationService/CalculateLabelingTask",
		"/acme.learning.v1.CourseQueryService/BrowseCourses",
		"/acme.ledger.v1.LedgerAccountQueryService/BrowseLedgerAccounts",
		"/acme.lending.v1.LoanApplicationCalculationService/CalculateLoanApplication",
		"/acme.licensing.v1.SoftwareLicenseCalculationService/CalculateSoftwareLicense",
		"/acme.lineage.v1.LineageNodeCalculationService/CalculateLineageNode",
		"/acme.localization.v1.TranslationKeyCalculationService/CalculateTranslationKey",
		"/acme.loyalty.v1.LoyaltyMemberApprovalService/ApproveLoyaltyMember",
		"/acme.maintenance.v1.WorkOrderCalculationService/CalculateWorkOrder",
		"/acme.media.v1.MediaAssetCalculationService/CalculateMediaAsset",
		"/acme.merchandising.v1.CollectionApprovalService/ApproveCollection",
		"/acme.messaging.v1.ConversationCalculationService/CalculateConversation",
		"/acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses",
		"/acme.modelregistry.v1.RegisteredModelCalculationService/CalculateRegisteredModel",
		"/acme.notifications.v1.NotificationQueryService/BrowseNotifications",
		"/acme.onboarding.v1.OnboardingPlanCalculationService/CalculateOnboardingPlan",
		"/acme.orders.v1.BackorderApprovalService/ApproveBackorder",
		"/acme.partnerapi.v1.PartnerIntegrationGatewayService/ComposePartnerIntegrationPage",
		"/acme.patientapp.v1.PatientHomeQueryService/BrowsePatientHomes",
		"/acme.patients.v1.PatientCalculationService/CalculatePatient",
		"/acme.payments.v1.DisputeCalculationService/CalculateDispute",
		"/acme.payroll.v1.PayRunCalculationService/CalculatePayRun",
		"/acme.performance.v1.ReviewQueryService/BrowseReviews",
		"/acme.performers.v1.PerformerQueryService/BrowsePerformers",
		"/acme.personalization.v1.ProfileCalculationService/CalculateProfile",
		"/acme.pipelines.v1.PipelineCalculationService/CalculatePipeline",
		"/acme.prescriptions.v1.PrescriptionQueryService/BrowsePrescriptions",
		"/acme.pricing.v1.CurrencyRateApprovalService/ApproveCurrencyRate",
		"/acme.procurement.v1.PurchaseRequisitionCalculationService/CalculatePurchaseRequisition",
		"/acme.promotions.v1.CampaignApprovalService/ApproveCampaign",
		"/acme.ratelimiting.v1.RateLimitQueryService/BrowseRateLimits",
		"/acme.recommendations.v1.RecommendationSetQueryService/BrowseRecommendationSets",
		"/acme.recruiting.v1.JobOpeningCalculationService/CalculateJobOpening",
		"/acme.returns.v1.InspectionApprovalService/ApproveInspection",
		"/acme.reviews.v1.ProductReviewApprovalService/ApproveProductReview",
		"/acme.risk.v1.RiskRegisterQueryService/BrowseRiskRegisters",
		"/acme.routing.v1.RouteCalculationService/CalculateRoute",
		"/acme.scheduling.v1.ShiftQueryService/BrowseShifts",
		"/acme.search.v1.SearchIndexAdminService/ArchiveSearchIndex",
		"/acme.seating.v1.SeatMapCalculationService/CalculateSeatMap",
		"/acme.secrets.v1.SecretQueryService/BrowseSecrets",
		"/acme.sellers.v1.SellerApprovalService/ApproveSeller",
		"/acme.shipping.v1.CarrierRateApprovalService/ApproveCarrierRate",
		"/acme.shows.v1.ShowCalculationService/CalculateShow",
		"/acme.sponsorships.v1.SponsorCalculationService/CalculateSponsor",
		"/acme.storefront.v1.LandingPageIntegrationService/AcknowledgeLandingPageEvent",
		"/acme.subscriptions.v1.RenewalScheduleApprovalService/ApproveRenewalSchedule",
		"/acme.suppliers.v1.SupplierQueryService/BrowseSuppliers",
		"/acme.surveys.v1.SurveyQueryService/BrowseSurveys",
		"/acme.telehealth.v1.VideoVisitCalculationService/CalculateVideoVisit",
		"/acme.telematics.v1.TelemetryReadingCalculationService/CalculateTelemetryReading",
		"/acme.ticketing.v1.TicketQueryService/BrowseTickets",
		"/acme.timekeeping.v1.TimesheetQueryService/BrowseTimesheets",
		"/acme.tours.v1.TourQueryService/BrowseTours",
		"/acme.travel.v1.TripQueryService/BrowseTrips",
		"/acme.treasury.v1.CashPositionCalculationService/CalculateCashPosition",
		"/acme.venues.v1.VenueQueryService/BrowseVenues",
		"/acme.warehousing.v1.PickListApprovalService/ApprovePickList",
		"/acme.webbff.v1.PageCompositionQueryService/BrowsePageCompositions",
		"/acme.webhooks.v1.WebhookEndpointQueryService/BrowseWebhookEndpoints",
		"/acme.workplaces.v1.BuildingCalculationService/CalculateBuilding",
		"/acme.yards.v1.YardCalculationService/CalculateYard",
	} {
		if _, err := describe(path); err != nil {
			t.Error(err)
		}
	}
}
