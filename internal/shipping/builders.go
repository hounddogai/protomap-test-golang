package shipping

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
)

// buildSyncSynonymsRequestForCloneShipment maps what CloneShipment has to the request of
// acme.search.v1.SynonymSyncService/SyncSynonyms.
func buildSyncSynonymsRequestForCloneShipment(req *shippingv1.CloneShipmentRequest) *searchv1.SyncSynonymsRequest {
	return &searchv1.SyncSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{Etag: req.GetShipment().GetAudit().GetEtag()},
		},
	}
}

// buildScheduleTrackingEventExpiryRequestForStartCarrierRateWorkflow maps what StartCarrierRateWorkflow has to the
// request of acme.shipping.v1.TrackingEventLifecycleService/ScheduleTrackingEventExpiry.
func buildScheduleTrackingEventExpiryRequestForStartCarrierRateWorkflow(req *shippingv1.StartCarrierRateWorkflowRequest) *shippingv1.ScheduleTrackingEventExpiryRequest {
	return &shippingv1.ScheduleTrackingEventExpiryRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ScheduleTrackingEventExpiryRequest_Details{Parameters: req.GetCarrierRate().GetParameters()},
	}
}

// buildSuspendCarrierRateRequestForResumeCarrierRateWorkflow maps what ResumeCarrierRateWorkflow has to the request of
// acme.shipping.v1.CarrierRateLifecycleService/SuspendCarrierRate.
func buildSuspendCarrierRateRequestForResumeCarrierRateWorkflow(req *shippingv1.ResumeCarrierRateWorkflowRequest) *shippingv1.SuspendCarrierRateRequest {
	return &shippingv1.SuspendCarrierRateRequest{
		Context:       req.GetContext(),
		CarrierRateId: req.GetCarrierRate().GetCarrierRateId(),
		Details: &shippingv1.SuspendCarrierRateRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				Email: req.GetCarrierRate().GetOwner().GetEmail(),
				Name:  req.GetDetails().GetNotify().GetName(),
			},
		},
	}
}

// buildCheckReturnRequestEligibilityRequestForPreviewManifest maps what PreviewManifest has to the request of
// acme.returns.v1.ReturnRequestValidationService/CheckReturnRequestEligibility.
func buildCheckReturnRequestEligibilityRequestForPreviewManifest(req *shippingv1.PreviewManifestRequest) *returnsv1.CheckReturnRequestEligibilityRequest {
	return &returnsv1.CheckReturnRequestEligibilityRequest{
		Context: req.GetContext(),
		Details: &returnsv1.CheckReturnRequestEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
}

// buildCalculatePointsLedgerRequestForPreviewManifest maps what PreviewManifest has to the request of
// acme.loyalty.v1.PointsLedgerCalculationService/CalculatePointsLedger.
func buildCalculatePointsLedgerRequestForPreviewManifest(req *shippingv1.PreviewManifestRequest) *loyaltyv1.CalculatePointsLedgerRequest {
	return &loyaltyv1.CalculatePointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Holder: &loyaltyv1.RewardRef{
				DisplayName: req.GetManifest().GetCounterparty().GetDisplayName(),
			},
		},
	}
}

// buildEstimateTrackingEventRequestForBrowseShipments maps what BrowseShipments has to the request of
// acme.shipping.v1.TrackingEventCalculationService/EstimateTrackingEvent.
func buildEstimateTrackingEventRequestForBrowseShipments(req *shippingv1.BrowseShipmentsRequest) *shippingv1.EstimateTrackingEventRequest {
	return &shippingv1.EstimateTrackingEventRequest{
		Context: req.GetContext(),
		Details: &shippingv1.EstimateTrackingEventRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		TrackingEvent: &shippingv1.TrackingEvent{OccurredAt: req.GetFilter().GetCreatedAfter()},
	}
}

// buildValidateCustomerSegmentRequestForAdvanceParcelWorkflow maps what AdvanceParcelWorkflow has to the request of
// acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment.
func buildValidateCustomerSegmentRequestForAdvanceParcelWorkflow(req *shippingv1.AdvanceParcelWorkflowRequest) *customersv1.ValidateCustomerSegmentRequest {
	return &customersv1.ValidateCustomerSegmentRequest{
		Context: req.GetContext(),
		Details: &customersv1.ValidateCustomerSegmentRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
}

// buildEstimateManifestRequestForSubmitCarrierRate maps what SubmitCarrierRate has to the request of
// acme.shipping.v1.ManifestCalculationService/EstimateManifest.
func buildEstimateManifestRequestForSubmitCarrierRate(req *shippingv1.SubmitCarrierRateRequest) *shippingv1.EstimateManifestRequest {
	return &shippingv1.EstimateManifestRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Lines: []*shippingv1.ManifestLine{&shippingv1.ManifestLine{
				Tax: &shippingv1.ManifestLineTax{Amount: req.GetCarrierRate().GetDiscount().GetAmount()},
			}},
			Audit:        &commonv1.AuditInfo{CreatedAt: req.GetCarrierRate().GetAudit().GetCreatedAt()},
			Counterparty: &customersv1.CustomerRef{Email: req.GetCarrierRate().GetOwner().GetEmail()},
		},
	}
}

// buildEscalateMerchandisingRuleRequestForApproveCarrierRate maps what ApproveCarrierRate has to the request of
// acme.merchandising.v1.MerchandisingRuleApprovalService/EscalateMerchandisingRule.
func buildEscalateMerchandisingRuleRequestForApproveCarrierRate(req *shippingv1.ApproveCarrierRateRequest) *merchandisingv1.EscalateMerchandisingRuleRequest {
	return &merchandisingv1.EscalateMerchandisingRuleRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{UserId: req.GetCarrierRate().GetOwner().GetUserId()},
		},
	}
}

// buildSubmitRenewalScheduleRequestForCancelShipmentWorkflow maps what CancelShipmentWorkflow has to the request of
// acme.subscriptions.v1.RenewalScheduleApprovalService/SubmitRenewalSchedule.
func buildSubmitRenewalScheduleRequestForCancelShipmentWorkflow(req *shippingv1.CancelShipmentWorkflowRequest, delegateRenewalScheduleApprovalReply *subscriptionsv1.DelegateRenewalScheduleApprovalResponse) *subscriptionsv1.SubmitRenewalScheduleRequest {
	return &subscriptionsv1.SubmitRenewalScheduleRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: delegateRenewalScheduleApprovalReply.GetRenewalSchedule().GetAudit().GetUpdatedBy(),
			},
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
			}},
			Organizer: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
}

// buildRecallShipmentRequestForSignalShipmentWorkflow maps what SignalShipmentWorkflow has to the request of
// acme.shipping.v1.ShipmentApprovalService/RecallShipment.
func buildRecallShipmentRequestForSignalShipmentWorkflow(req *shippingv1.SignalShipmentWorkflowRequest) *shippingv1.RecallShipmentRequest {
	return &shippingv1.RecallShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetShipment().GetBillingAddress().GetCountryCode(),
				Lines:       req.GetShipment().GetBillingAddress().GetLines(),
				Locality:    req.GetShipment().GetBillingAddress().GetLocality(),
			},
			Settlement: &shippingv1.Shipment_Bank{Bank: &shippingv1.ShipmentBankSettlement{Bic: req.GetShipment().GetBank().GetBic()}},
		},
	}
}

// buildRecalculateTrackingEventsRequestForCommentParcelApproval maps what CommentParcelApproval has to the request of
// acme.shipping.v1.TrackingEventCalculationService/RecalculateTrackingEvents.
func buildRecalculateTrackingEventsRequestForCommentParcelApproval(req *shippingv1.CommentParcelApprovalRequest) *shippingv1.RecalculateTrackingEventsRequest {
	return &shippingv1.RecalculateTrackingEventsRequest{
		Context: req.GetContext(),
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	}
}

// buildLookupParcelRequestForEscalateShipment maps what EscalateShipment has to the request of
// acme.shipping.v1.ParcelQueryService/LookupParcel.
func buildLookupParcelRequestForEscalateShipment(req *shippingv1.EscalateShipmentRequest) *shippingv1.LookupParcelRequest {
	return &shippingv1.LookupParcelRequest{
		Context:  req.GetContext(),
		ParcelId: req.GetShipment().GetCounterparty().GetParcelId(),
	}
}

// buildEstimateParcelRequestForPreviewParcel maps what PreviewParcel has to the request of
// acme.shipping.v1.ParcelCalculationService/EstimateParcel.
func buildEstimateParcelRequestForPreviewParcel(req *shippingv1.PreviewParcelRequest) *shippingv1.EstimateParcelRequest {
	return &shippingv1.EstimateParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{
				Region: req.GetParcel().GetAddress().GetRegion(),
				Lines:  req.GetParcel().GetAddress().GetLines(),
			}},
			Dimensions: &shippingv1.ParcelDimensions{Height: req.GetParcel().GetDimensions().GetHeight()},
			Components: map[string]*shippingv1.ParcelComponent{"primary": &shippingv1.ParcelComponent{
				Quantity: req.GetParcel().GetComponents()["primary"].GetQuantity(),
			}},
		},
	}
}

// buildCompareParcelOptionsRequestForCheckParcelEligibility maps what CheckParcelEligibility has to the request of
// acme.shipping.v1.ParcelCalculationService/CompareParcelOptions.
func buildCompareParcelOptionsRequestForCheckParcelEligibility(req *shippingv1.CheckParcelEligibilityRequest, explainReturnRequestDecisionReply *returnsv1.ExplainReturnRequestDecisionResponse, calculateParcelReply *shippingv1.CalculateParcelResponse) *shippingv1.CompareParcelOptionsRequest {
	return &shippingv1.CompareParcelOptionsRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{
				Lines: calculateParcelReply.GetParcel().GetAddress().GetLines(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: calculateParcelReply.GetParcel().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &shippingv1.CompareParcelOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality:    explainReturnRequestDecisionReply.GetReturnRequest().GetBillingAddress().GetLocality(),
				CountryCode: calculateParcelReply.GetParcel().GetAddress().GetCountryCode(),
			},
		},
	}
}

// buildExplainSynonymDecisionRequestForEstimateCarrierRate maps what EstimateCarrierRate has to the request of
// acme.search.v1.SynonymValidationService/ExplainSynonymDecision.
func buildExplainSynonymDecisionRequestForEstimateCarrierRate(req *shippingv1.EstimateCarrierRateRequest) *searchv1.ExplainSynonymDecisionRequest {
	return &searchv1.ExplainSynonymDecisionRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetCarrierRate().GetAudit().GetCreatedBy()},
		},
	}
}

// buildApproveSellerPayoutScheduleRequestForSuspendTrackingEvent maps what SuspendTrackingEvent has to the request of
// acme.sellers.v1.SellerPayoutScheduleApprovalService/ApproveSellerPayoutSchedule.
func buildApproveSellerPayoutScheduleRequestForSuspendTrackingEvent(req *shippingv1.SuspendTrackingEventRequest, explainTrackingEventDecisionReply *shippingv1.ExplainTrackingEventDecisionResponse) *sellersv1.ApproveSellerPayoutScheduleRequest {
	return &sellersv1.ApproveSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			}},
			Organizer: &identityv1.UserRef{
				DisplayName: explainTrackingEventDecisionReply.GetTrackingEvent().GetSubject().GetDisplayName(),
			},
		},
	}
}

// buildListManifestConflictsRequestForDeleteManifestReport maps what DeleteManifestReport has to the request of
// acme.shipping.v1.ManifestSyncService/ListManifestConflicts.
func buildListManifestConflictsRequestForDeleteManifestReport(req *shippingv1.DeleteManifestReportRequest) *shippingv1.ListManifestConflictsRequest {
	return &shippingv1.ListManifestConflictsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ListManifestConflictsRequest_Filter{
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      req.GetManifestId(),
			Labels:       req.GetContext().GetBaggage(),
		},
	}
}

// buildEstimateShipmentRequestForEstimateParcel maps what EstimateParcel has to the request of
// acme.shipping.v1.ShipmentCalculationService/EstimateShipment.
func buildEstimateShipmentRequestForEstimateParcel(req *shippingv1.EstimateParcelRequest) *shippingv1.EstimateShipmentRequest {
	return &shippingv1.EstimateShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetParcel().GetAddress().GetPostalCode(),
				Locality:   req.GetDetails().GetDestination().GetLocality(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetParcel().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetParcel().GetAudit().GetCreatedBy(),
			},
		},
	}
}

// buildExplainShipmentCalculationRequestForQuoteParcel maps what QuoteParcel has to the request of
// acme.shipping.v1.ShipmentCalculationService/ExplainShipmentCalculation.
func buildExplainShipmentCalculationRequestForQuoteParcel(req *shippingv1.QuoteParcelRequest) *shippingv1.ExplainShipmentCalculationRequest {
	return &shippingv1.ExplainShipmentCalculationRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{Lines: req.GetDetails().GetDestination().GetLines()},
		},
	}
}

// buildCompareShipmentOptionsRequestForCompareParcelOptions maps what CompareParcelOptions has to the request of
// acme.shipping.v1.ShipmentCalculationService/CompareShipmentOptions.
func buildCompareShipmentOptionsRequestForCompareParcelOptions(req *shippingv1.CompareParcelOptionsRequest) *shippingv1.CompareShipmentOptionsRequest {
	return &shippingv1.CompareShipmentOptionsRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetParcel().GetAudit().GetUpdatedBy(),
				CreatedBy: req.GetParcel().GetAudit().GetCreatedBy(),
			},
			Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{
				Description: req.GetParcel().GetComponents()["primary"].GetDescription(),
			}},
		},
		Details: &shippingv1.CompareShipmentOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetParcel().GetAddress().GetRegion()},
		},
	}
}

// buildScheduleSellerPayoutScheduleExpiryRequestForScheduleCarrierRateExpiry maps what ScheduleCarrierRateExpiry has to
// the request of acme.sellers.v1.SellerPayoutScheduleLifecycleService/ScheduleSellerPayoutScheduleExpiry.
func buildScheduleSellerPayoutScheduleExpiryRequestForScheduleCarrierRateExpiry(req *shippingv1.ScheduleCarrierRateExpiryRequest) *sellersv1.ScheduleSellerPayoutScheduleExpiryRequest {
	return &sellersv1.ScheduleSellerPayoutScheduleExpiryRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Name:  req.GetDetails().GetNotify().GetName(),
					Email: req.GetDetails().GetNotify().GetEmail(),
				},
			}},
		},
	}
}

// buildExplainCustomerCalculationRequestForShareTrackingEventReport maps what ShareTrackingEventReport has to the
// request of acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation.
func buildExplainCustomerCalculationRequestForShareTrackingEventReport(req *shippingv1.ShareTrackingEventReportRequest) *customersv1.ExplainCustomerCalculationRequest {
	return &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetRecipient().GetPhone(),
				Email: req.GetDetails().GetRecipient().GetEmail(),
			},
			Name: req.GetDetails().GetRecipient().GetName(),
		},
		Details: &customersv1.ExplainCustomerCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
		},
	}
}

// buildBrowseDocumentsRequestForCalculateShipment maps what CalculateShipment has to the request of
// acme.documents.v1.DocumentQueryService/BrowseDocuments.
func buildBrowseDocumentsRequestForCalculateShipment(req *shippingv1.CalculateShipmentRequest) *documentsv1.BrowseDocumentsRequest {
	return &documentsv1.BrowseDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.BrowseDocumentsRequest_Filter{
			Region:       req.GetShipment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetShipment().GetAudit().GetUpdatedAt(),
		},
	}
}

// buildCompareParcelOptionsRequestForTerminateParcel maps what TerminateParcel has to the request of
// acme.shipping.v1.ParcelCalculationService/CompareParcelOptions.
func buildCompareParcelOptionsRequestForTerminateParcel(req *shippingv1.TerminateParcelRequest, comparePreferenceOptionsReply *customersv1.ComparePreferenceOptionsResponse) *shippingv1.CompareParcelOptionsRequest {
	return &shippingv1.CompareParcelOptionsRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				DisplayName: comparePreferenceOptionsReply.GetPreference().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: comparePreferenceOptionsReply.GetPreference().GetAudit().GetCreatedAt(),
				UpdatedBy: comparePreferenceOptionsReply.GetPreference().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildSubscribeSearchQueryUpdatesRequestForGenerateCarrierRateReport maps what GenerateCarrierRateReport has to the
// request of acme.search.v1.SearchQueryNotificationService/SubscribeSearchQueryUpdates.
func buildSubscribeSearchQueryUpdatesRequestForGenerateCarrierRateReport(req *shippingv1.GenerateCarrierRateReportRequest) *searchv1.SubscribeSearchQueryUpdatesRequest {
	return &searchv1.SubscribeSearchQueryUpdatesRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &searchv1.SubscribeSearchQueryUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: req.GetDetails().GetFilters(),
		},
		Annotations: req.GetAnnotations(),
	}
}

// buildResolveHouseholdsRequestForShareCarrierRateReport maps what ShareCarrierRateReport has to the request of
// acme.customers.v1.HouseholdQueryService/ResolveHouseholds.
func buildResolveHouseholdsRequestForShareCarrierRateReport(req *shippingv1.ShareCarrierRateReportRequest) *customersv1.ResolveHouseholdsRequest {
	return &customersv1.ResolveHouseholdsRequest{Context: req.GetContext()}
}
