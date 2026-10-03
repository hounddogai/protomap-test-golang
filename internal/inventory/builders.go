package inventory

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
)

// buildValidateStockMovementRequestForDeleteStockItem maps what DeleteStockItem has to the request of
// acme.inventory.v1.StockMovementValidationService/ValidateStockMovement.
func buildValidateStockMovementRequestForDeleteStockItem(req *inventoryv1.DeleteStockItemRequest, scoreReplenishmentPlanReply *inventoryv1.ScoreReplenishmentPlanResponse) *inventoryv1.ValidateStockMovementRequest {
	return &inventoryv1.ValidateStockMovementRequest{
		Context: req.GetContext(),
		StockMovement: &inventoryv1.StockMovement{
			CorrelationId: scoreReplenishmentPlanReply.GetResult().GetCorrelationId(),
			Subject: &customersv1.CustomerRef{
				DisplayName: scoreReplenishmentPlanReply.GetReplenishmentPlan().GetCounterparty().GetDisplayName(),
			},
		},
		Reason: req.GetReason(),
	}
}

// buildQuoteReplenishmentPlanRequestForExportStockReservations maps what ExportStockReservations has to the request of
// acme.inventory.v1.ReplenishmentPlanCalculationService/QuoteReplenishmentPlan.
func buildQuoteReplenishmentPlanRequestForExportStockReservations(req *inventoryv1.ExportStockReservationsRequest, previewReplenishmentPlanReply *inventoryv1.PreviewReplenishmentPlanResponse) *inventoryv1.QuoteReplenishmentPlanRequest {
	return &inventoryv1.QuoteReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Counterparty: &inventoryv1.StockReservationRef{
				StockReservationId: previewReplenishmentPlanReply.GetReplenishmentPlan().GetCounterparty().GetStockReservationId(),
			},
		},
	}
}

// buildEscalateStockReservationRequestForRejectReplenishmentPlan maps what RejectReplenishmentPlan has to the request
// of acme.inventory.v1.StockReservationApprovalService/EscalateStockReservation.
func buildEscalateStockReservationRequestForRejectReplenishmentPlan(req *inventoryv1.RejectReplenishmentPlanRequest) *inventoryv1.EscalateStockReservationRequest {
	return &inventoryv1.EscalateStockReservationRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.EscalateStockReservationRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
		StockReservation: &inventoryv1.StockReservation{
			Organizer: &identityv1.UserRef{
				DisplayName: req.GetReplenishmentPlan().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetReplenishmentPlan().GetAudit().GetUpdatedBy(),
			},
			Location: &commonv1.PostalAddress{
				Region: req.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
			},
		},
	}
}

// buildSimulateParcelRequestForExportStockItems maps what ExportStockItems has to the request of
// acme.shipping.v1.ParcelCalculationService/SimulateParcel.
func buildSimulateParcelRequestForExportStockItems(req *inventoryv1.ExportStockItemsRequest, calculateReplenishmentPlanReply *inventoryv1.CalculateReplenishmentPlanResponse) *shippingv1.SimulateParcelRequest {
	return &shippingv1.SimulateParcelRequest{
		Context: req.GetContext(),
		Details: &shippingv1.SimulateParcelRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: calculateReplenishmentPlanReply.GetReplenishmentPlan().GetBillingAddress().GetRecipient(),
			},
		},
	}
}

// buildResolveSubscriptionPlansRequestForSummarizeStockItems maps what SummarizeStockItems has to the request of
// acme.subscriptions.v1.SubscriptionPlanQueryService/ResolveSubscriptionPlans.
func buildResolveSubscriptionPlansRequestForSummarizeStockItems(req *inventoryv1.SummarizeStockItemsRequest) *subscriptionsv1.ResolveSubscriptionPlansRequest {
	return &subscriptionsv1.ResolveSubscriptionPlansRequest{Context: req.GetContext()}
}

// buildCalculateReplenishmentPlanRequestForSignalStockReservationWorkflow maps what SignalStockReservationWorkflow has
// to the request of acme.inventory.v1.ReplenishmentPlanCalculationService/CalculateReplenishmentPlan.
func buildCalculateReplenishmentPlanRequestForSignalStockReservationWorkflow(req *inventoryv1.SignalStockReservationWorkflowRequest) *inventoryv1.CalculateReplenishmentPlanRequest {
	return &inventoryv1.CalculateReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetStockReservation().GetAudit().GetUpdatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient:   req.GetStockReservation().GetLocation().GetRecipient(),
				Region:      req.GetStockReservation().GetLocation().GetRegion(),
				CountryCode: req.GetStockReservation().GetLocation().GetCountryCode(),
			},
		},
	}
}

// buildPreviewPutawayTaskRequestForCheckReplenishmentPlanEligibility maps what CheckReplenishmentPlanEligibility has to
// the request of acme.warehousing.v1.PutawayTaskValidationService/PreviewPutawayTask.
func buildPreviewPutawayTaskRequestForCheckReplenishmentPlanEligibility(req *inventoryv1.CheckReplenishmentPlanEligibilityRequest) *warehousingv1.PreviewPutawayTaskRequest {
	return &warehousingv1.PreviewPutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetReplenishmentPlan().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetReplenishmentPlan().GetAudit().GetCreatedBy(),
			},
			Requester: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
		Details: &warehousingv1.PreviewPutawayTaskRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}
}

// buildExplainUsageRecordCalculationRequestForValidateStockMovement maps what ValidateStockMovement has to the request
// of acme.subscriptions.v1.UsageRecordCalculationService/ExplainUsageRecordCalculation.
func buildExplainUsageRecordCalculationRequestForValidateStockMovement(req *inventoryv1.ValidateStockMovementRequest, simulateReplenishmentPlanReply *inventoryv1.SimulateReplenishmentPlanResponse) *subscriptionsv1.ExplainUsageRecordCalculationRequest {
	return &subscriptionsv1.ExplainUsageRecordCalculationRequest{
		Context: req.GetContext(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{
				DisplayName: simulateReplenishmentPlanReply.GetReplenishmentPlan().GetCounterparty().GetDisplayName(),
			},
		},
	}
}

// buildResumePackingSlipWorkflowRequestForApproveStockItem maps what ApproveStockItem has to the request of
// acme.fulfillment.v1.PackingSlipWorkflowService/ResumePackingSlipWorkflow.
func buildResumePackingSlipWorkflowRequestForApproveStockItem(req *inventoryv1.ApproveStockItemRequest) *fulfillmentv1.ResumePackingSlipWorkflowRequest {
	return &fulfillmentv1.ResumePackingSlipWorkflowRequest{
		Context: req.GetContext(),
		PackingSlip: &fulfillmentv1.PackingSlip{
			BillingAddress: &commonv1.PostalAddress{
				Locality: req.GetStockItem().GetAddress().GetLocality(),
				Region:   req.GetStockItem().GetAddress().GetRegion(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetStockItem().GetAudit().GetUpdatedAt()},
		},
	}
}

// buildSubmitPutawayTaskRequestForListPendingStockItems maps what ListPendingStockItems has to the request of
// acme.warehousing.v1.PutawayTaskApprovalService/SubmitPutawayTask.
func buildSubmitPutawayTaskRequestForListPendingStockItems(req *inventoryv1.ListPendingStockItemsRequest) *warehousingv1.SubmitPutawayTaskRequest {
	return &warehousingv1.SubmitPutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// buildNormalizeStockMovementRequestForCommentStockItemApproval maps what CommentStockItemApproval has to the request
// of acme.inventory.v1.StockMovementValidationService/NormalizeStockMovement.
func buildNormalizeStockMovementRequestForCommentStockItemApproval(req *inventoryv1.CommentStockItemApprovalRequest) *inventoryv1.NormalizeStockMovementRequest {
	return &inventoryv1.NormalizeStockMovementRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.NormalizeStockMovementRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetStockItem().GetOwner().GetEmail()},
		},
	}
}
