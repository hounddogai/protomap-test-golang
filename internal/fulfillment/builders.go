package fulfillment

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
)

// buildResolvePackingSlipsRequestForUpdateFulfillment maps what UpdateFulfillment has to the request of
// acme.fulfillment.v1.PackingSlipQueryService/ResolvePackingSlips.
func buildResolvePackingSlipsRequestForUpdateFulfillment(req *fulfillmentv1.UpdateFulfillmentRequest) *fulfillmentv1.ResolvePackingSlipsRequest {
	return &fulfillmentv1.ResolvePackingSlipsRequest{Context: req.GetContext()}
}

// buildBrowseFulfillmentsRequestForCancelFulfillmentExceptionWorkflow maps what CancelFulfillmentExceptionWorkflow has
// to the request of acme.fulfillment.v1.FulfillmentQueryService/BrowseFulfillments.
func buildBrowseFulfillmentsRequestForCancelFulfillmentExceptionWorkflow(req *fulfillmentv1.CancelFulfillmentExceptionWorkflowRequest, previewFeatureFlagReply *featureflagsv1.PreviewFeatureFlagResponse) *fulfillmentv1.BrowseFulfillmentsRequest {
	return &fulfillmentv1.BrowseFulfillmentsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.BrowseFulfillmentsRequest_Filter{
			CreatedAfter: previewFeatureFlagReply.GetFeatureFlag().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetFulfillmentException().GetCustomer().GetPackingSlipId(),
			Labels:       previewFeatureFlagReply.GetResult().GetDetails(),
		},
	}
}

// buildCommentSubscriptionApprovalRequestForRetryFulfillmentExceptionWorkflow maps what
// RetryFulfillmentExceptionWorkflow has to the request of
// acme.subscriptions.v1.SubscriptionApprovalService/CommentSubscriptionApproval.
func buildCommentSubscriptionApprovalRequestForRetryFulfillmentExceptionWorkflow(req *fulfillmentv1.RetryFulfillmentExceptionWorkflowRequest, delegateCarrierRateApprovalReply *shippingv1.DelegateCarrierRateApprovalResponse) *subscriptionsv1.CommentSubscriptionApprovalRequest {
	return &subscriptionsv1.CommentSubscriptionApprovalRequest{
		Context: req.GetContext(),
		Subscription: &subscriptionsv1.Subscription{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetFulfillmentException().GetRequester().GetPreferredChannel(),
			},
			Limits: []*subscriptionsv1.SubscriptionLimit{&subscriptionsv1.SubscriptionLimit{
				Amount: delegateCarrierRateApprovalReply.GetCarrierRate().GetDiscount().GetAmount(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetFulfillmentException().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildSubmitUsageRecordRequestForListPackingSlipWorkflows maps what ListPackingSlipWorkflows has to the request of
// acme.subscriptions.v1.UsageRecordApprovalService/SubmitUsageRecord.
func buildSubmitUsageRecordRequestForListPackingSlipWorkflows(req *fulfillmentv1.ListPackingSlipWorkflowsRequest) *subscriptionsv1.SubmitUsageRecordRequest {
	return &subscriptionsv1.SubmitUsageRecordRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.SubmitUsageRecordRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	}
}

// buildExplainHouseholdDecisionRequestForStartFulfillmentWorkflow maps what StartFulfillmentWorkflow has to the request
// of acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision.
func buildExplainHouseholdDecisionRequestForStartFulfillmentWorkflow(req *fulfillmentv1.StartFulfillmentWorkflowRequest) *customersv1.ExplainHouseholdDecisionRequest {
	return &customersv1.ExplainHouseholdDecisionRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
				}},
				Location: &commonv1.PostalAddress{
					Locality: req.GetFulfillment().GetBillingAddress().GetLocality(),
				},
			}},
		},
	}
}

// buildSearchFulfillmentsRequestForPauseFulfillmentWorkflow maps what PauseFulfillmentWorkflow has to the request of
// acme.fulfillment.v1.FulfillmentService/SearchFulfillments.
func buildSearchFulfillmentsRequestForPauseFulfillmentWorkflow(req *fulfillmentv1.PauseFulfillmentWorkflowRequest, createFulfillmentReply *fulfillmentv1.CreateFulfillmentResponse) *fulfillmentv1.SearchFulfillmentsRequest {
	return &fulfillmentv1.SearchFulfillmentsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.SearchFulfillmentsRequest_Filter{
			Region:       req.GetFulfillment().GetBillingAddress().GetRegion(),
			CreatedAfter: createFulfillmentReply.GetFulfillment().GetAudit().GetCreatedAt(),
			OwnerId:      createFulfillmentReply.GetFulfillment().GetCounterparty().GetCustomerId(),
		},
	}
}

// buildSubmitPackingSlipRequestForSignalFulfillmentWorkflow maps what SignalFulfillmentWorkflow has to the request of
// acme.fulfillment.v1.PackingSlipApprovalService/SubmitPackingSlip.
func buildSubmitPackingSlipRequestForSignalFulfillmentWorkflow(req *fulfillmentv1.SignalFulfillmentWorkflowRequest, estimateConsentRecordReply *customersv1.EstimateConsentRecordResponse) *fulfillmentv1.SubmitPackingSlipRequest {
	return &fulfillmentv1.SubmitPackingSlipRequest{
		Context: req.GetContext(),
		Details: &fulfillmentv1.SubmitPackingSlipRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: estimateConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
		},
	}
}

// buildNormalizeFulfillmentExceptionRequestForSubmitPackingSlip maps what SubmitPackingSlip has to the request of
// acme.fulfillment.v1.FulfillmentExceptionValidationService/NormalizeFulfillmentException.
func buildNormalizeFulfillmentExceptionRequestForSubmitPackingSlip(req *fulfillmentv1.SubmitPackingSlipRequest) *fulfillmentv1.NormalizeFulfillmentExceptionRequest {
	return &fulfillmentv1.NormalizeFulfillmentExceptionRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPackingSlip().GetAudit().GetCreatedAt(),
				CreatedBy: req.GetPackingSlip().GetAudit().GetCreatedBy(),
				UpdatedAt: req.GetPackingSlip().GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
}

// buildSignalSubscriptionPlanWorkflowRequestForDelegatePackingSlipApproval maps what DelegatePackingSlipApproval has to
// the request of acme.subscriptions.v1.SubscriptionPlanWorkflowService/SignalSubscriptionPlanWorkflow.
func buildSignalSubscriptionPlanWorkflowRequestForDelegatePackingSlipApproval(req *fulfillmentv1.DelegatePackingSlipApprovalRequest) *subscriptionsv1.SignalSubscriptionPlanWorkflowRequest {
	return &subscriptionsv1.SignalSubscriptionPlanWorkflowRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
}

// buildPreviewParcelRequestForValidateFulfillmentException maps what ValidateFulfillmentException has to the request of
// acme.shipping.v1.ParcelValidationService/PreviewParcel.
func buildPreviewParcelRequestForValidateFulfillmentException(req *fulfillmentv1.ValidateFulfillmentExceptionRequest) *shippingv1.PreviewParcelRequest {
	return &shippingv1.PreviewParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetFulfillmentException().GetCustomer().GetDisplayName(),
				Email:       req.GetFulfillmentException().GetRequester().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetFulfillmentException().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildScheduleSearchIndexExportRequestForNormalizeFulfillmentException maps what NormalizeFulfillmentException has to
// the request of acme.search.v1.SearchIndexExportService/ScheduleSearchIndexExport.
func buildScheduleSearchIndexExportRequestForNormalizeFulfillmentException(req *fulfillmentv1.NormalizeFulfillmentExceptionRequest) *searchv1.ScheduleSearchIndexExportRequest {
	return &searchv1.ScheduleSearchIndexExportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
	}
}

// buildValidateVenueRequestForScoreFulfillmentException maps what ScoreFulfillmentException has to the request of
// acme.venues.v1.VenueValidationService/ValidateVenue.
func buildValidateVenueRequestForScoreFulfillmentException(req *fulfillmentv1.ScoreFulfillmentExceptionRequest) *venuesv1.ValidateVenueRequest {
	return &venuesv1.ValidateVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Audit: &commonv1.AuditInfo{Etag: req.GetFulfillmentException().GetAudit().GetEtag()},
		},
	}
}
