package shipping

import (
	"context"
	"log/slog"

	admissionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/admissions/v1"
	assetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/assets/v1"
	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	boxofficev1 "github.com/hounddogai/protomap-test-golang/gen/acme/boxoffice/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	fuelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fuel/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	lendingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/lending/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	recruitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recruiting/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the shipping domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.admissions.v1.AdmissionQueryService", admissionsv1.NewAdmissionQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.ConsentRecordCalculationService", customersv1.NewConsentRecordCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerCalculationService", customersv1.NewCustomerCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.HouseholdValidationService", customersv1.NewHouseholdValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.PreferenceCalculationService", customersv1.NewPreferenceCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.forms.v1.FormCalculationService", formsv1.NewFormCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardBatchValidationService", giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardTransactionValidationService", giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.ledger.v1.LedgerAccountQueryService", ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.recruiting.v1.JobOpeningQueryService", recruitingv1.NewJobOpeningQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerScorecardReportingService", sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn))
	deps.Clients.Put("acme.shipping.v1.ManifestCalculationService", shippingv1.NewManifestCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.shipping.v1.ParcelCalculationService", shippingv1.NewParcelCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.shipping.v1.ShipmentCalculationService", shippingv1.NewShipmentCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.shipping.v1.ShipmentValidationService", shippingv1.NewShipmentValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.shipping.v1.TrackingEventCalculationService", shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn))
	shippingv1.RegisterShipmentServiceServer(server, NewShipmentServer(deps))
	shippingv1.RegisterParcelQueryServiceServer(server, NewParcelQueryServer(deps))
	shippingv1.RegisterCarrierRateWorkflowServiceServer(server, NewCarrierRateWorkflowServer(deps))
	shippingv1.RegisterTrackingEventApprovalServiceServer(server, NewTrackingEventApprovalServer(deps))
	shippingv1.RegisterManifestValidationServiceServer(server, NewManifestValidationServer(deps))
	shippingv1.RegisterShipmentQueryServiceServer(server, NewShipmentQueryServer(deps))
	parcelWorkflowServer := NewParcelWorkflowServer(deps)
	server.RegisterService(&shippingv1.ParcelWorkflowService_ServiceDesc, parcelWorkflowServer)
	shippingv1.RegisterCarrierRateApprovalServiceServer(server, NewCarrierRateApprovalServer(deps))
	shippingv1.RegisterTrackingEventValidationServiceServer(server, NewTrackingEventValidationServer(deps))
	shippingv1.RegisterManifestCalculationServiceServer(server, NewManifestCalculationServer(deps))
	shippingv1.RegisterShipmentWorkflowServiceServer(server, NewShipmentWorkflowServer(deps))
	shippingv1.RegisterParcelApprovalServiceServer(server, NewParcelApprovalServer(deps))
	shippingv1.RegisterCarrierRateValidationServiceServer(server, NewCarrierRateValidationServer(deps))
	shippingv1.RegisterTrackingEventCalculationServiceServer(server, NewTrackingEventCalculationServer(deps))
	shippingv1.RegisterManifestLifecycleServiceServer(server, NewManifestLifecycleServer(deps))
	shippingv1.RegisterShipmentApprovalServiceServer(server, NewShipmentApprovalServer(deps))
	shippingv1.RegisterParcelValidationServiceServer(server, NewParcelValidationServer(deps))
	shippingv1.RegisterCarrierRateCalculationServiceServer(server, NewCarrierRateCalculationServer(deps))
	shippingv1.RegisterTrackingEventLifecycleServiceServer(server, NewTrackingEventLifecycleServer(deps))
	shippingv1.RegisterManifestReportingServiceServer(server, NewManifestReportingServer(deps))
	shipmentValidationServer := NewShipmentValidationServer(deps)
	server.RegisterService(&shippingv1.ShipmentValidationService_ServiceDesc, shipmentValidationServer)
	parcelCalculationServer := NewParcelCalculationServer(deps)
	server.RegisterService(&shippingv1.ParcelCalculationService_ServiceDesc, parcelCalculationServer)
	carrierRateLifecycleServer := NewCarrierRateLifecycleServer(deps)
	server.RegisterService(&shippingv1.CarrierRateLifecycleService_ServiceDesc, carrierRateLifecycleServer)
	trackingEventReportingServer := NewTrackingEventReportingServer(deps)
	server.RegisterService(&shippingv1.TrackingEventReportingService_ServiceDesc, trackingEventReportingServer)
	shippingv1.RegisterManifestSyncServiceServer(server, NewManifestSyncServer(deps))
	shippingv1.RegisterShipmentCalculationServiceServer(server, NewShipmentCalculationServer(deps))
	shippingv1.RegisterParcelLifecycleServiceServer(server, NewParcelLifecycleServer(deps))
	carrierRateReportingServer := NewCarrierRateReportingServer(deps)
	server.RegisterService(&shippingv1.CarrierRateReportingService_ServiceDesc, carrierRateReportingServer)
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	previewShipmentCall              func(context.Context, *shippingv1.PreviewShipmentRequest, ...grpc.CallOption) (*shippingv1.PreviewShipmentResponse, error)
	explainRewardDecisionCall        func(context.Context, *loyaltyv1.ExplainRewardDecisionRequest, ...grpc.CallOption) (*loyaltyv1.ExplainRewardDecisionResponse, error)
	scheduleSellerListingReportCall  func(context.Context, *sellersv1.ScheduleSellerListingReportRequest) (*sellersv1.ScheduleSellerListingReportResponse, error)
	reconcileManifestsCall           func(context.Context, *shippingv1.ReconcileManifestsRequest) (*shippingv1.ReconcileManifestsResponse, error)
	recalculateCustomersCall         func(context.Context, *customersv1.RecalculateCustomersRequest, ...grpc.CallOption) (*customersv1.RecalculateCustomersResponse, error)
	estimateFuelCardCall             func(context.Context, *fuelv1.EstimateFuelCardRequest, ...grpc.CallOption) (*fuelv1.EstimateFuelCardResponse, error)
	browseFixedAssetsCall            func(context.Context, *assetsv1.BrowseFixedAssetsRequest) (*assetsv1.BrowseFixedAssetsResponse, error)
	scoreCustomerCall                func(context.Context, *customersv1.ScoreCustomerRequest) (*customersv1.ScoreCustomerResponse, error)
	explainCustomerDecisionCall      func(context.Context, *customersv1.ExplainCustomerDecisionRequest, ...grpc.CallOption) (*customersv1.ExplainCustomerDecisionResponse, error)
	renewParcelCall                  func(context.Context, *shippingv1.RenewParcelRequest) (*shippingv1.RenewParcelResponse, error)
	searchBoxOfficeSalesCall         func(context.Context, *boxofficev1.SearchBoxOfficeSalesRequest, ...grpc.CallOption) (*boxofficev1.SearchBoxOfficeSalesResponse, error)
	explainConsentRecordDecisionCall func(context.Context, *customersv1.ExplainConsentRecordDecisionRequest) (*customersv1.ExplainConsentRecordDecisionResponse, error)
	createLoanApplicationCall        func(context.Context, *lendingv1.CreateLoanApplicationRequest) (*lendingv1.CreateLoanApplicationResponse, error)
	getManifestRulesCall             func(context.Context, *shippingv1.GetManifestRulesRequest) (*shippingv1.GetManifestRulesResponse, error)
	listDocumentsCall                func(context.Context, *documentsv1.ListDocumentsRequest, ...grpc.CallOption) (*documentsv1.ListDocumentsResponse, error)
	checkParcelEligibilityCall       func(context.Context, *shippingv1.CheckParcelEligibilityRequest) (*shippingv1.CheckParcelEligibilityResponse, error)
	simulateParcelCall               func(context.Context, *shippingv1.SimulateParcelRequest) (*shippingv1.SimulateParcelResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	shipmentValidationForPreviewShipment := shippingv1.NewShipmentValidationServiceClient(deps.Conn)
	previewShipmentCall = shipmentValidationForPreviewShipment.PreviewShipment
	rewardValidationForExplainRewardDecision := loyaltyv1.NewRewardValidationServiceClient(deps.Conn)
	explainRewardDecisionCall = rewardValidationForExplainRewardDecision.ExplainRewardDecision
	sellerListingReportingForScheduleSellerListingReport := sellersv1.NewSellerListingReportingServiceClient(deps.Conn)
	scheduleSellerListingReportCall = func(ctx context.Context, request *sellersv1.ScheduleSellerListingReportRequest) (*sellersv1.ScheduleSellerListingReportResponse, error) {
		return sellerListingReportingForScheduleSellerListingReport.ScheduleSellerListingReport(ctx, request)
	}
	manifestSyncForReconcileManifests := shippingv1.NewManifestSyncServiceClient(deps.Conn)
	reconcileManifestsCall = func(ctx context.Context, request *shippingv1.ReconcileManifestsRequest) (*shippingv1.ReconcileManifestsResponse, error) {
		return manifestSyncForReconcileManifests.ReconcileManifests(ctx, request)
	}
	customerCalculationForRecalculateCustomers := customersv1.NewCustomerCalculationServiceClient(deps.Conn)
	recalculateCustomersCall = customerCalculationForRecalculateCustomers.RecalculateCustomers
	fuelCardCalculationForEstimateFuelCard := fuelv1.NewFuelCardCalculationServiceClient(deps.Conn)
	estimateFuelCardCall = fuelCardCalculationForEstimateFuelCard.EstimateFuelCard
	fixedAssetQueryForBrowseFixedAssets := assetsv1.NewFixedAssetQueryServiceClient(deps.Conn)
	browseFixedAssetsCall = func(ctx context.Context, request *assetsv1.BrowseFixedAssetsRequest) (*assetsv1.BrowseFixedAssetsResponse, error) {
		return fixedAssetQueryForBrowseFixedAssets.BrowseFixedAssets(ctx, request)
	}
	customerValidationForScoreCustomer := customersv1.NewCustomerValidationServiceClient(deps.Conn)
	scoreCustomerCall = func(ctx context.Context, request *customersv1.ScoreCustomerRequest) (*customersv1.ScoreCustomerResponse, error) {
		return customerValidationForScoreCustomer.ScoreCustomer(ctx, request)
	}
	customerValidationForExplainCustomerDecision := customersv1.NewCustomerValidationServiceClient(deps.Conn)
	explainCustomerDecisionCall = customerValidationForExplainCustomerDecision.ExplainCustomerDecision
	parcelLifecycleForRenewParcel := shippingv1.NewParcelLifecycleServiceClient(deps.Conn)
	renewParcelCall = func(ctx context.Context, request *shippingv1.RenewParcelRequest) (*shippingv1.RenewParcelResponse, error) {
		return parcelLifecycleForRenewParcel.RenewParcel(ctx, request)
	}
	boxOfficeSaleForSearchBoxOfficeSales := boxofficev1.NewBoxOfficeSaleServiceClient(deps.Conn)
	searchBoxOfficeSalesCall = boxOfficeSaleForSearchBoxOfficeSales.SearchBoxOfficeSales
	consentRecordValidationForExplainConsentRecordDecision := customersv1.NewConsentRecordValidationServiceClient(deps.Conn)
	explainConsentRecordDecisionCall = func(ctx context.Context, request *customersv1.ExplainConsentRecordDecisionRequest) (*customersv1.ExplainConsentRecordDecisionResponse, error) {
		return consentRecordValidationForExplainConsentRecordDecision.ExplainConsentRecordDecision(ctx, request)
	}
	loanApplicationForCreateLoanApplication := lendingv1.NewLoanApplicationServiceClient(deps.Conn)
	createLoanApplicationCall = func(ctx context.Context, request *lendingv1.CreateLoanApplicationRequest) (*lendingv1.CreateLoanApplicationResponse, error) {
		return loanApplicationForCreateLoanApplication.CreateLoanApplication(ctx, request)
	}
	manifestValidationForGetManifestRules := shippingv1.NewManifestValidationServiceClient(deps.Conn)
	getManifestRulesCall = func(ctx context.Context, request *shippingv1.GetManifestRulesRequest) (*shippingv1.GetManifestRulesResponse, error) {
		return manifestValidationForGetManifestRules.GetManifestRules(ctx, request)
	}
	documentForListDocuments := documentsv1.NewDocumentServiceClient(deps.Conn)
	listDocumentsCall = documentForListDocuments.ListDocuments
	parcelValidationForCheckParcelEligibility := shippingv1.NewParcelValidationServiceClient(deps.Conn)
	checkParcelEligibilityCall = func(ctx context.Context, request *shippingv1.CheckParcelEligibilityRequest) (*shippingv1.CheckParcelEligibilityResponse, error) {
		return parcelValidationForCheckParcelEligibility.CheckParcelEligibility(ctx, request)
	}
	parcelCalculationForSimulateParcel := shippingv1.NewParcelCalculationServiceClient(deps.Conn)
	simulateParcelCall = func(ctx context.Context, request *shippingv1.SimulateParcelRequest) (*shippingv1.SimulateParcelResponse, error) {
		return parcelCalculationForSimulateParcel.SimulateParcel(ctx, request)
	}
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                                   *slog.Logger
	householdCalculationClient            customersv1.HouseholdCalculationServiceClient
	sellerValidationClient                sellersv1.SellerValidationServiceClient
	collectionWorkflowClient              merchandisingv1.CollectionWorkflowServiceClient
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	collectionClient                      merchandisingv1.CollectionServiceClient
	auditEventValidationClient            auditv1.AuditEventValidationServiceClient
	loyaltyMemberApprovalClient           loyaltyv1.LoyaltyMemberApprovalServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                                   deps.Logger,
		householdCalculationClient:            customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		sellerValidationClient:                sellersv1.NewSellerValidationServiceClient(deps.Conn),
		collectionWorkflowClient:              merchandisingv1.NewCollectionWorkflowServiceClient(deps.Conn),
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		collectionClient:                      merchandisingv1.NewCollectionServiceClient(deps.Conn),
		auditEventValidationClient:            auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		loyaltyMemberApprovalClient:           loyaltyv1.NewLoyaltyMemberApprovalServiceClient(deps.Conn),
	}
	go w.consume(ctx, "shipping.delegate_parcel_approval.recalculate_households", deps.Bus.Subscribe("shipping.delegate_parcel_approval.recalculate_households"), w.handleShippingDelegateParcelApprovalRecalculateHouseholds)
	go w.consume(ctx, "shipping.get_tracking_event_rates.validate_seller", deps.Bus.Subscribe("shipping.get_tracking_event_rates.validate_seller"), w.handleShippingGetTrackingEventRatesValidateSeller)
	go w.consume(ctx, "shipping.terminate_tracking_event.retry_collection_workflow", deps.Bus.Subscribe("shipping.terminate_tracking_event.retry_collection_workflow"), w.handleShippingTerminateTrackingEventRetryCollectionWorkflow)
	go w.consume(ctx, "shipping.score_shipment.recalculate_seller_payout_schedules", deps.Bus.Subscribe("shipping.score_shipment.recalculate_seller_payout_schedules"), w.handleShippingScoreShipmentRecalculateSellerPayoutSchedules)
	go w.consume(ctx, "shipping.quote_parcel.update_collection", deps.Bus.Subscribe("shipping.quote_parcel.update_collection"), w.handleShippingQuoteParcelUpdateCollection)
	go w.consume(ctx, "shipping.suspend_carrier_rate.preview_audit_event", deps.Bus.Subscribe("shipping.suspend_carrier_rate.preview_audit_event"), w.handleShippingSuspendCarrierRatePreviewAuditEvent)
	go w.consume(ctx, "shipping.schedule_tracking_event_report.escalate_loyalty_member", deps.Bus.Subscribe("shipping.schedule_tracking_event_report.escalate_loyalty_member"), w.handleShippingScheduleTrackingEventReportEscalateLoyaltyMember)
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

// handleShippingDelegateParcelApprovalRecalculateHouseholds makes the call the
// shipping.delegate_parcel_approval.recalculate_households event asks for.
func (w *workers) handleShippingDelegateParcelApprovalRecalculateHouseholds(ctx context.Context, event eventbus.Event) error {
	_, err := w.householdCalculationClient.RecalculateHouseholds(ctx, &customersv1.RecalculateHouseholdsRequest{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						DisplayName: event.Attributes["household.children.children.manager.display_name"],
					},
					Audit: &commonv1.AuditInfo{
						CreatedBy: event.Attributes["household.children.children.audit.created_by"],
					},
				}},
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

// handleShippingGetTrackingEventRatesValidateSeller makes the call the
// shipping.get_tracking_event_rates.validate_seller event asks for.
func (w *workers) handleShippingGetTrackingEventRatesValidateSeller(ctx context.Context, event eventbus.Event) error {
	_, err := w.sellerValidationClient.ValidateSeller(ctx, &sellersv1.ValidateSellerRequest{
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				UserId: event.Attributes["seller.relationship_manager.user_id"],
			},
		},
		Details: &sellersv1.ValidateSellerRequest_Details{
			Contact: &commonv1.ContactInfo{Email: event.Attributes["details.contact.email"]},
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

// handleShippingTerminateTrackingEventRetryCollectionWorkflow makes the call the
// shipping.terminate_tracking_event.retry_collection_workflow event asks for.
func (w *workers) handleShippingTerminateTrackingEventRetryCollectionWorkflow(ctx context.Context, event eventbus.Event) error {
	_, err := w.collectionWorkflowClient.RetryCollectionWorkflow(ctx, &merchandisingv1.RetryCollectionWorkflowRequest{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Manager: &identityv1.UserRef{
						DisplayName: event.Attributes["collection.children.children.manager.display_name"],
					},
				}},
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

// handleShippingScoreShipmentRecalculateSellerPayoutSchedules makes the call the
// shipping.score_shipment.recalculate_seller_payout_schedules event asks for.
func (w *workers) handleShippingScoreShipmentRecalculateSellerPayoutSchedules(ctx context.Context, event eventbus.Event) error {
	_, err := w.sellerPayoutScheduleCalculationClient.RecalculateSellerPayoutSchedules(ctx, &sellersv1.RecalculateSellerPayoutSchedulesRequest{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: event.Attributes["seller_payout_schedule.audit.created_by"],
				UpdatedBy: event.Attributes["seller_payout_schedule.audit.updated_by"],
			},
		},
		Details: &sellersv1.RecalculateSellerPayoutSchedulesRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: event.Attributes["details.destination.recipient"],
				Region:    event.Attributes["details.destination.region"],
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

// handleShippingQuoteParcelUpdateCollection makes the call the shipping.quote_parcel.update_collection event asks for.
func (w *workers) handleShippingQuoteParcelUpdateCollection(ctx context.Context, event eventbus.Event) error {
	_, err := w.collectionClient.UpdateCollection(ctx, &merchandisingv1.UpdateCollectionRequest{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Location: &commonv1.PostalAddress{
						Recipient: event.Attributes["collection.children.children.location.recipient"],
						Region:    event.Attributes["collection.children.children.location.region"],
					},
					Manager: &identityv1.UserRef{
						Email: event.Attributes["collection.children.children.manager.email"],
					},
				}},
				Location: &commonv1.PostalAddress{
					Locality: event.Attributes["collection.children.location.locality"],
				},
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

// handleShippingSuspendCarrierRatePreviewAuditEvent makes the call the
// shipping.suspend_carrier_rate.preview_audit_event event asks for.
func (w *workers) handleShippingSuspendCarrierRatePreviewAuditEvent(ctx context.Context, event eventbus.Event) error {
	_, err := w.auditEventValidationClient.PreviewAuditEvent(ctx, &auditv1.PreviewAuditEventRequest{
		Details: &auditv1.PreviewAuditEventRequest_Details{
			Contact: &commonv1.ContactInfo{Email: event.Attributes["details.contact.email"]},
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

// handleShippingScheduleTrackingEventReportEscalateLoyaltyMember makes the call the
// shipping.schedule_tracking_event_report.escalate_loyalty_member event asks for.
func (w *workers) handleShippingScheduleTrackingEventReportEscalateLoyaltyMember(ctx context.Context, event eventbus.Event) error {
	_, err := w.loyaltyMemberApprovalClient.EscalateLoyaltyMember(ctx, &loyaltyv1.EscalateLoyaltyMemberRequest{
		Details: &loyaltyv1.EscalateLoyaltyMemberRequest_Details{
			Approver: &identityv1.UserRef{Email: event.Attributes["details.approver.email"]},
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
