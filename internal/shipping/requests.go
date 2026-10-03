package shipping

import (
	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
)

// summarizeParcelsOption sets part of a request of acme.shipping.v1.ParcelQueryService/SummarizeParcels.
type summarizeParcelsOption func(*shippingv1.SummarizeParcelsRequest)

// newSummarizeParcelsRequest builds a request of acme.shipping.v1.ParcelQueryService/SummarizeParcels from options.
func newSummarizeParcelsRequest(options ...summarizeParcelsOption) *shippingv1.SummarizeParcelsRequest {
	request := &shippingv1.SummarizeParcelsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSummarizeParcelsContext(value *commonv1.RequestContext) summarizeParcelsOption {
	return func(request *shippingv1.SummarizeParcelsRequest) { request.Context = value }
}

func withSummarizeParcelsDetails(value *shippingv1.SummarizeParcelsRequest_Details) summarizeParcelsOption {
	return func(request *shippingv1.SummarizeParcelsRequest) { request.Details = value }
}

// delegateRestockDecisionApprovalOption sets part of a request of
// acme.returns.v1.RestockDecisionApprovalService/DelegateRestockDecisionApproval.
type delegateRestockDecisionApprovalOption func(*returnsv1.DelegateRestockDecisionApprovalRequest)

// newDelegateRestockDecisionApprovalRequest builds a request of
// acme.returns.v1.RestockDecisionApprovalService/DelegateRestockDecisionApproval from options.
func newDelegateRestockDecisionApprovalRequest(options ...delegateRestockDecisionApprovalOption) *returnsv1.DelegateRestockDecisionApprovalRequest {
	request := &returnsv1.DelegateRestockDecisionApprovalRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withDelegateRestockDecisionApprovalContext(value *commonv1.RequestContext) delegateRestockDecisionApprovalOption {
	return func(request *returnsv1.DelegateRestockDecisionApprovalRequest) { request.Context = value }
}

func withDelegateRestockDecisionApprovalRestockDecision(value *returnsv1.RestockDecision) delegateRestockDecisionApprovalOption {
	return func(request *returnsv1.DelegateRestockDecisionApprovalRequest) { request.RestockDecision = value }
}

func withDelegateRestockDecisionApprovalDetails(value *returnsv1.DelegateRestockDecisionApprovalRequest_Details) delegateRestockDecisionApprovalOption {
	return func(request *returnsv1.DelegateRestockDecisionApprovalRequest) { request.Details = value }
}

// compareCarrierRateOptionsOption sets part of a request of
// acme.shipping.v1.CarrierRateCalculationService/CompareCarrierRateOptions.
type compareCarrierRateOptionsOption func(*shippingv1.CompareCarrierRateOptionsRequest)

// newCompareCarrierRateOptionsRequest builds a request of
// acme.shipping.v1.CarrierRateCalculationService/CompareCarrierRateOptions from options.
func newCompareCarrierRateOptionsRequest(options ...compareCarrierRateOptionsOption) *shippingv1.CompareCarrierRateOptionsRequest {
	request := &shippingv1.CompareCarrierRateOptionsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCompareCarrierRateOptionsContext(value *commonv1.RequestContext) compareCarrierRateOptionsOption {
	return func(request *shippingv1.CompareCarrierRateOptionsRequest) { request.Context = value }
}

func withCompareCarrierRateOptionsCarrierRate(value *shippingv1.CarrierRate) compareCarrierRateOptionsOption {
	return func(request *shippingv1.CompareCarrierRateOptionsRequest) { request.CarrierRate = value }
}

// previewParcelOption sets part of a request of acme.shipping.v1.ParcelValidationService/PreviewParcel.
type previewParcelOption func(*shippingv1.PreviewParcelRequest)

// newPreviewParcelRequest builds a request of acme.shipping.v1.ParcelValidationService/PreviewParcel from options.
func newPreviewParcelRequest(options ...previewParcelOption) *shippingv1.PreviewParcelRequest {
	request := &shippingv1.PreviewParcelRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withPreviewParcelContext(value *commonv1.RequestContext) previewParcelOption {
	return func(request *shippingv1.PreviewParcelRequest) { request.Context = value }
}

func withPreviewParcelParcel(value *shippingv1.Parcel) previewParcelOption {
	return func(request *shippingv1.PreviewParcelRequest) { request.Parcel = value }
}

// generateTrackingEventReportOption sets part of a request of
// acme.shipping.v1.TrackingEventReportingService/GenerateTrackingEventReport.
type generateTrackingEventReportOption func(*shippingv1.GenerateTrackingEventReportRequest)

// newGenerateTrackingEventReportRequest builds a request of
// acme.shipping.v1.TrackingEventReportingService/GenerateTrackingEventReport from options.
func newGenerateTrackingEventReportRequest(options ...generateTrackingEventReportOption) *shippingv1.GenerateTrackingEventReportRequest {
	request := &shippingv1.GenerateTrackingEventReportRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withGenerateTrackingEventReportContext(value *commonv1.RequestContext) generateTrackingEventReportOption {
	return func(request *shippingv1.GenerateTrackingEventReportRequest) { request.Context = value }
}

func withGenerateTrackingEventReportAnnotations(value map[string]string) generateTrackingEventReportOption {
	return func(request *shippingv1.GenerateTrackingEventReportRequest) { request.Annotations = value }
}

func withGenerateTrackingEventReportReason(value string) generateTrackingEventReportOption {
	return func(request *shippingv1.GenerateTrackingEventReportRequest) { request.Reason = value }
}

// submitParcelOption sets part of a request of acme.shipping.v1.ParcelApprovalService/SubmitParcel.
type submitParcelOption func(*shippingv1.SubmitParcelRequest)

// newSubmitParcelRequest builds a request of acme.shipping.v1.ParcelApprovalService/SubmitParcel from options.
func newSubmitParcelRequest(options ...submitParcelOption) *shippingv1.SubmitParcelRequest {
	request := &shippingv1.SubmitParcelRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSubmitParcelContext(value *commonv1.RequestContext) submitParcelOption {
	return func(request *shippingv1.SubmitParcelRequest) { request.Context = value }
}

func withSubmitParcelParcel(value *shippingv1.Parcel) submitParcelOption {
	return func(request *shippingv1.SubmitParcelRequest) { request.Parcel = value }
}

func withSubmitParcelDetails(value *shippingv1.SubmitParcelRequest_Details) submitParcelOption {
	return func(request *shippingv1.SubmitParcelRequest) { request.Details = value }
}

// explainConsentRecordDecisionOption sets part of a request of
// acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision.
type explainConsentRecordDecisionOption func(*customersv1.ExplainConsentRecordDecisionRequest)

// newExplainConsentRecordDecisionRequest builds a request of
// acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision from options.
func newExplainConsentRecordDecisionRequest(options ...explainConsentRecordDecisionOption) *customersv1.ExplainConsentRecordDecisionRequest {
	request := &customersv1.ExplainConsentRecordDecisionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainConsentRecordDecisionContext(value *commonv1.RequestContext) explainConsentRecordDecisionOption {
	return func(request *customersv1.ExplainConsentRecordDecisionRequest) { request.Context = value }
}

func withExplainConsentRecordDecisionConsentRecord(value *customersv1.ConsentRecord) explainConsentRecordDecisionOption {
	return func(request *customersv1.ExplainConsentRecordDecisionRequest) { request.ConsentRecord = value }
}

func withExplainConsentRecordDecisionDetails(value *customersv1.ExplainConsentRecordDecisionRequest_Details) explainConsentRecordDecisionOption {
	return func(request *customersv1.ExplainConsentRecordDecisionRequest) { request.Details = value }
}

// explainParcelCalculationOption sets part of a request of
// acme.shipping.v1.ParcelCalculationService/ExplainParcelCalculation.
type explainParcelCalculationOption func(*shippingv1.ExplainParcelCalculationRequest)

// newExplainParcelCalculationRequest builds a request of
// acme.shipping.v1.ParcelCalculationService/ExplainParcelCalculation from options.
func newExplainParcelCalculationRequest(options ...explainParcelCalculationOption) *shippingv1.ExplainParcelCalculationRequest {
	request := &shippingv1.ExplainParcelCalculationRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainParcelCalculationContext(value *commonv1.RequestContext) explainParcelCalculationOption {
	return func(request *shippingv1.ExplainParcelCalculationRequest) { request.Context = value }
}

func withExplainParcelCalculationDetails(value *shippingv1.ExplainParcelCalculationRequest_Details) explainParcelCalculationOption {
	return func(request *shippingv1.ExplainParcelCalculationRequest) { request.Details = value }
}

// countCarrierRateTotalsOption sets part of a request of
// acme.shipping.v1.CarrierRateReportingService/CountCarrierRateTotals.
type countCarrierRateTotalsOption func(*shippingv1.CountCarrierRateTotalsRequest)

// newCountCarrierRateTotalsRequest builds a request of
// acme.shipping.v1.CarrierRateReportingService/CountCarrierRateTotals from options.
func newCountCarrierRateTotalsRequest(options ...countCarrierRateTotalsOption) *shippingv1.CountCarrierRateTotalsRequest {
	request := &shippingv1.CountCarrierRateTotalsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCountCarrierRateTotalsContext(value *commonv1.RequestContext) countCarrierRateTotalsOption {
	return func(request *shippingv1.CountCarrierRateTotalsRequest) { request.Context = value }
}

func withCountCarrierRateTotalsFilter(value *shippingv1.CountCarrierRateTotalsRequest_Filter) countCarrierRateTotalsOption {
	return func(request *shippingv1.CountCarrierRateTotalsRequest) { request.Filter = value }
}

// comparePreferenceOptionsOption sets part of a request of
// acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions.
type comparePreferenceOptionsOption func(*customersv1.ComparePreferenceOptionsRequest)

// newComparePreferenceOptionsRequest builds a request of
// acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions from options.
func newComparePreferenceOptionsRequest(options ...comparePreferenceOptionsOption) *customersv1.ComparePreferenceOptionsRequest {
	request := &customersv1.ComparePreferenceOptionsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withComparePreferenceOptionsContext(value *commonv1.RequestContext) comparePreferenceOptionsOption {
	return func(request *customersv1.ComparePreferenceOptionsRequest) { request.Context = value }
}

func withComparePreferenceOptionsPreference(value *customersv1.Preference) comparePreferenceOptionsOption {
	return func(request *customersv1.ComparePreferenceOptionsRequest) { request.Preference = value }
}

func withComparePreferenceOptionsDetails(value *customersv1.ComparePreferenceOptionsRequest_Details) comparePreferenceOptionsOption {
	return func(request *customersv1.ComparePreferenceOptionsRequest) { request.Details = value }
}

// suggestAuditEventsOption sets part of a request of acme.audit.v1.AuditEventQueryService/SuggestAuditEvents.
type suggestAuditEventsOption func(*auditv1.SuggestAuditEventsRequest)

// newSuggestAuditEventsRequest builds a request of acme.audit.v1.AuditEventQueryService/SuggestAuditEvents from
// options.
func newSuggestAuditEventsRequest(options ...suggestAuditEventsOption) *auditv1.SuggestAuditEventsRequest {
	request := &auditv1.SuggestAuditEventsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSuggestAuditEventsContext(value *commonv1.RequestContext) suggestAuditEventsOption {
	return func(request *auditv1.SuggestAuditEventsRequest) { request.Context = value }
}

func withSuggestAuditEventsFilter(value *auditv1.SuggestAuditEventsRequest_Filter) suggestAuditEventsOption {
	return func(request *auditv1.SuggestAuditEventsRequest) { request.Filter = value }
}

// quoteConsentRecordOption sets part of a request of
// acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord.
type quoteConsentRecordOption func(*customersv1.QuoteConsentRecordRequest)

// newQuoteConsentRecordRequest builds a request of acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord
// from options.
func newQuoteConsentRecordRequest(options ...quoteConsentRecordOption) *customersv1.QuoteConsentRecordRequest {
	request := &customersv1.QuoteConsentRecordRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withQuoteConsentRecordContext(value *commonv1.RequestContext) quoteConsentRecordOption {
	return func(request *customersv1.QuoteConsentRecordRequest) { request.Context = value }
}

func withQuoteConsentRecordDetails(value *customersv1.QuoteConsentRecordRequest_Details) quoteConsentRecordOption {
	return func(request *customersv1.QuoteConsentRecordRequest) { request.Details = value }
}

func withQuoteConsentRecordConsentRecord(value *customersv1.ConsentRecord) quoteConsentRecordOption {
	return func(request *customersv1.QuoteConsentRecordRequest) { request.ConsentRecord = value }
}

// annotateSynonymVersionOption sets part of a request of acme.search.v1.SynonymHistoryService/AnnotateSynonymVersion.
type annotateSynonymVersionOption func(*searchv1.AnnotateSynonymVersionRequest)

// newAnnotateSynonymVersionRequest builds a request of acme.search.v1.SynonymHistoryService/AnnotateSynonymVersion from
// options.
func newAnnotateSynonymVersionRequest(options ...annotateSynonymVersionOption) *searchv1.AnnotateSynonymVersionRequest {
	request := &searchv1.AnnotateSynonymVersionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withAnnotateSynonymVersionContext(value *commonv1.RequestContext) annotateSynonymVersionOption {
	return func(request *searchv1.AnnotateSynonymVersionRequest) { request.Context = value }
}

func withAnnotateSynonymVersionSynonym(value *searchv1.Synonym) annotateSynonymVersionOption {
	return func(request *searchv1.AnnotateSynonymVersionRequest) { request.Synonym = value }
}

func withAnnotateSynonymVersionDetails(value *searchv1.AnnotateSynonymVersionRequest_Details) annotateSynonymVersionOption {
	return func(request *searchv1.AnnotateSynonymVersionRequest) { request.Details = value }
}

// delegateCollectionApprovalOption sets part of a request of
// acme.merchandising.v1.CollectionApprovalService/DelegateCollectionApproval.
type delegateCollectionApprovalOption func(*merchandisingv1.DelegateCollectionApprovalRequest)

// newDelegateCollectionApprovalRequest builds a request of
// acme.merchandising.v1.CollectionApprovalService/DelegateCollectionApproval from options.
func newDelegateCollectionApprovalRequest(options ...delegateCollectionApprovalOption) *merchandisingv1.DelegateCollectionApprovalRequest {
	request := &merchandisingv1.DelegateCollectionApprovalRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withDelegateCollectionApprovalContext(value *commonv1.RequestContext) delegateCollectionApprovalOption {
	return func(request *merchandisingv1.DelegateCollectionApprovalRequest) { request.Context = value }
}

func withDelegateCollectionApprovalCollection(value *merchandisingv1.Collection) delegateCollectionApprovalOption {
	return func(request *merchandisingv1.DelegateCollectionApprovalRequest) { request.Collection = value }
}

// calculateShipmentOption sets part of a request of acme.shipping.v1.ShipmentCalculationService/CalculateShipment.
type calculateShipmentOption func(*shippingv1.CalculateShipmentRequest)

// newCalculateShipmentRequest builds a request of acme.shipping.v1.ShipmentCalculationService/CalculateShipment from
// options.
func newCalculateShipmentRequest(options ...calculateShipmentOption) *shippingv1.CalculateShipmentRequest {
	request := &shippingv1.CalculateShipmentRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCalculateShipmentContext(value *commonv1.RequestContext) calculateShipmentOption {
	return func(request *shippingv1.CalculateShipmentRequest) { request.Context = value }
}

func withCalculateShipmentDetails(value *shippingv1.CalculateShipmentRequest_Details) calculateShipmentOption {
	return func(request *shippingv1.CalculateShipmentRequest) { request.Details = value }
}

// rejectRewardOption sets part of a request of acme.loyalty.v1.RewardApprovalService/RejectReward.
type rejectRewardOption func(*loyaltyv1.RejectRewardRequest)

// newRejectRewardRequest builds a request of acme.loyalty.v1.RewardApprovalService/RejectReward from options.
func newRejectRewardRequest(options ...rejectRewardOption) *loyaltyv1.RejectRewardRequest {
	request := &loyaltyv1.RejectRewardRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withRejectRewardContext(value *commonv1.RequestContext) rejectRewardOption {
	return func(request *loyaltyv1.RejectRewardRequest) { request.Context = value }
}

func withRejectRewardDetails(value *loyaltyv1.RejectRewardRequest_Details) rejectRewardOption {
	return func(request *loyaltyv1.RejectRewardRequest) { request.Details = value }
}

// suggestSellersOption sets part of a request of acme.sellers.v1.SellerQueryService/SuggestSellers.
type suggestSellersOption func(*sellersv1.SuggestSellersRequest)

// newSuggestSellersRequest builds a request of acme.sellers.v1.SellerQueryService/SuggestSellers from options.
func newSuggestSellersRequest(options ...suggestSellersOption) *sellersv1.SuggestSellersRequest {
	request := &sellersv1.SuggestSellersRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSuggestSellersContext(value *commonv1.RequestContext) suggestSellersOption {
	return func(request *sellersv1.SuggestSellersRequest) { request.Context = value }
}

func withSuggestSellersFilter(value *sellersv1.SuggestSellersRequest_Filter) suggestSellersOption {
	return func(request *sellersv1.SuggestSellersRequest) { request.Filter = value }
}

// submitSellerPayoutScheduleOption sets part of a request of
// acme.sellers.v1.SellerPayoutScheduleApprovalService/SubmitSellerPayoutSchedule.
type submitSellerPayoutScheduleOption func(*sellersv1.SubmitSellerPayoutScheduleRequest)

// newSubmitSellerPayoutScheduleRequest builds a request of
// acme.sellers.v1.SellerPayoutScheduleApprovalService/SubmitSellerPayoutSchedule from options.
func newSubmitSellerPayoutScheduleRequest(options ...submitSellerPayoutScheduleOption) *sellersv1.SubmitSellerPayoutScheduleRequest {
	request := &sellersv1.SubmitSellerPayoutScheduleRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSubmitSellerPayoutScheduleContext(value *commonv1.RequestContext) submitSellerPayoutScheduleOption {
	return func(request *sellersv1.SubmitSellerPayoutScheduleRequest) { request.Context = value }
}

func withSubmitSellerPayoutScheduleSellerPayoutSchedule(value *sellersv1.SellerPayoutSchedule) submitSellerPayoutScheduleOption {
	return func(request *sellersv1.SubmitSellerPayoutScheduleRequest) { request.SellerPayoutSchedule = value }
}

// compareTrackingEventOptionsOption sets part of a request of
// acme.shipping.v1.TrackingEventCalculationService/CompareTrackingEventOptions.
type compareTrackingEventOptionsOption func(*shippingv1.CompareTrackingEventOptionsRequest)

// newCompareTrackingEventOptionsRequest builds a request of
// acme.shipping.v1.TrackingEventCalculationService/CompareTrackingEventOptions from options.
func newCompareTrackingEventOptionsRequest(options ...compareTrackingEventOptionsOption) *shippingv1.CompareTrackingEventOptionsRequest {
	request := &shippingv1.CompareTrackingEventOptionsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCompareTrackingEventOptionsContext(value *commonv1.RequestContext) compareTrackingEventOptionsOption {
	return func(request *shippingv1.CompareTrackingEventOptionsRequest) { request.Context = value }
}

func withCompareTrackingEventOptionsDetails(value *shippingv1.CompareTrackingEventOptionsRequest_Details) compareTrackingEventOptionsOption {
	return func(request *shippingv1.CompareTrackingEventOptionsRequest) { request.Details = value }
}

func withCompareTrackingEventOptionsTrackingEvent(value *shippingv1.TrackingEvent) compareTrackingEventOptionsOption {
	return func(request *shippingv1.CompareTrackingEventOptionsRequest) { request.TrackingEvent = value }
}
