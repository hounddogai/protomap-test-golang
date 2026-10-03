package returns

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
)

// buildCheckRestockDecisionEligibilityRequestForExportReturnLabels maps what ExportReturnLabels has to the request of
// acme.returns.v1.RestockDecisionValidationService/CheckRestockDecisionEligibility.
func buildCheckRestockDecisionEligibilityRequestForExportReturnLabels(req *returnsv1.ExportReturnLabelsRequest) *returnsv1.CheckRestockDecisionEligibilityRequest {
	return &returnsv1.CheckRestockDecisionEligibilityRequest{
		Context: req.GetContext(),
		Details: &returnsv1.CheckRestockDecisionEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
}

// buildSignalReturnRequestWorkflowRequestForRetryInspectionWorkflow maps what RetryInspectionWorkflow has to the
// request of acme.returns.v1.ReturnRequestWorkflowService/SignalReturnRequestWorkflow.
func buildSignalReturnRequestWorkflowRequestForRetryInspectionWorkflow(req *returnsv1.RetryInspectionWorkflowRequest, scheduleInspectionExpiryReply *returnsv1.ScheduleInspectionExpiryResponse) *returnsv1.SignalReturnRequestWorkflowRequest {
	return &returnsv1.SignalReturnRequestWorkflowRequest{
		Context: req.GetContext(),
		Details: &returnsv1.SignalReturnRequestWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotify().GetPhone(),
				Email: scheduleInspectionExpiryReply.GetInspection().GetCustomer().GetEmail(),
			},
		},
	}
}

// buildSimulateCustomerSegmentRequestForCommentRestockDecisionApproval maps what CommentRestockDecisionApproval has to
// the request of acme.customers.v1.CustomerSegmentCalculationService/SimulateCustomerSegment.
func buildSimulateCustomerSegmentRequestForCommentRestockDecisionApproval(req *returnsv1.CommentRestockDecisionApprovalRequest) *customersv1.SimulateCustomerSegmentRequest {
	return &customersv1.SimulateCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetRestockDecision().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	}
}

// buildSummarizeMetricSeriesesRequestForAdvanceReturnLabelWorkflow maps what AdvanceReturnLabelWorkflow has to the
// request of acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses.
func buildSummarizeMetricSeriesesRequestForAdvanceReturnLabelWorkflow(req *returnsv1.AdvanceReturnLabelWorkflowRequest) *metricsv1.SummarizeMetricSeriesesRequest {
	return &metricsv1.SummarizeMetricSeriesesRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details:     &metricsv1.SummarizeMetricSeriesesRequest_Details{Filters: req.GetDetails().GetParameters()},
	}
}

// buildCompareInspectionOptionsRequestForCheckRestockDecisionEligibility maps what CheckRestockDecisionEligibility has
// to the request of acme.returns.v1.InspectionCalculationService/CompareInspectionOptions.
func buildCompareInspectionOptionsRequestForCheckRestockDecisionEligibility(req *returnsv1.CheckRestockDecisionEligibilityRequest) *returnsv1.CompareInspectionOptionsRequest {
	return &returnsv1.CompareInspectionOptionsRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Requester: &commonv1.ContactInfo{
				Email: req.GetRestockDecision().GetOwner().GetEmail(),
				Phone: req.GetDetails().GetContact().GetPhone(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetRestockDecision().GetAudit().GetCreatedBy(),
			},
		},
	}
}

// buildRecallReturnLabelRequestForStartReturnRequestWorkflow maps what StartReturnRequestWorkflow has to the request of
// acme.returns.v1.ReturnLabelApprovalService/RecallReturnLabel.
func buildRecallReturnLabelRequestForStartReturnRequestWorkflow(req *returnsv1.StartReturnRequestWorkflowRequest) *returnsv1.RecallReturnLabelRequest {
	return &returnsv1.RecallReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Settlement: &returnsv1.ReturnLabel_Bank{Bank: &returnsv1.ReturnLabelBankSettlement{Bic: req.GetReturnRequest().GetBank().GetBic()}},
			BillingAddress: &commonv1.PostalAddress{
				Lines:     req.GetReturnRequest().GetBillingAddress().GetLines(),
				Recipient: req.GetReturnRequest().GetBillingAddress().GetRecipient(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetReturnRequest().GetAudit().GetCreatedBy()},
		},
	}
}

// buildExplainReturnRequestDecisionRequestForPreviewReturnLabel maps what PreviewReturnLabel has to the request of
// acme.returns.v1.ReturnRequestValidationService/ExplainReturnRequestDecision.
func buildExplainReturnRequestDecisionRequestForPreviewReturnLabel(req *returnsv1.PreviewReturnLabelRequest) *returnsv1.ExplainReturnRequestDecisionRequest {
	return &returnsv1.ExplainReturnRequestDecisionRequest{
		Context: req.GetContext(),
		Details: &returnsv1.ExplainReturnRequestDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
}

// buildSimulateReturnLabelRequestForNormalizeReturnLabel maps what NormalizeReturnLabel has to the request of
// acme.returns.v1.ReturnLabelCalculationService/SimulateReturnLabel.
func buildSimulateReturnLabelRequestForNormalizeReturnLabel(req *returnsv1.NormalizeReturnLabelRequest) *returnsv1.SimulateReturnLabelRequest {
	return &returnsv1.SimulateReturnLabelRequest{Context: req.GetContext()}
}

// buildGetReturnLabelRatesRequestForLintReturnLabels maps what LintReturnLabels has to the request of
// acme.returns.v1.ReturnLabelCalculationService/GetReturnLabelRates.
func buildGetReturnLabelRatesRequestForLintReturnLabels(req *returnsv1.LintReturnLabelsRequest) *returnsv1.GetReturnLabelRatesRequest {
	return &returnsv1.GetReturnLabelRatesRequest{
		Context:       req.GetContext(),
		ReturnLabelId: req.GetReturnLabelId(),
	}
}

// buildQuoteReturnLabelRequestForCompareInspectionOptions maps what CompareInspectionOptions has to the request of
// acme.returns.v1.ReturnLabelCalculationService/QuoteReturnLabel.
func buildQuoteReturnLabelRequestForCompareInspectionOptions(req *returnsv1.CompareInspectionOptionsRequest) *returnsv1.QuoteReturnLabelRequest {
	return &returnsv1.QuoteReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit:          &commonv1.AuditInfo{Etag: req.GetInspection().GetAudit().GetEtag()},
			BillingAddress: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
		},
	}
}

// buildEstimateRestockDecisionRequestForActivateRestockDecision maps what ActivateRestockDecision has to the request of
// acme.returns.v1.RestockDecisionCalculationService/EstimateRestockDecision.
func buildEstimateRestockDecisionRequestForActivateRestockDecision(req *returnsv1.ActivateRestockDecisionRequest, calculateInspectionReply *returnsv1.CalculateInspectionResponse) *returnsv1.EstimateRestockDecisionRequest {
	return &returnsv1.EstimateRestockDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateInspectionReply.GetInspection().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildListSynonymExportsRequestForCalculateReturnRequest maps what CalculateReturnRequest has to the request of
// acme.search.v1.SynonymExportService/ListSynonymExports.
func buildListSynonymExportsRequestForCalculateReturnRequest(req *returnsv1.CalculateReturnRequestRequest) *searchv1.ListSynonymExportsRequest {
	return &searchv1.ListSynonymExportsRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.ListSynonymExportsRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	}
}

// buildBrowseLedgerAccountsRequestForActivateReturnLabel maps what ActivateReturnLabel has to the request of
// acme.ledger.v1.LedgerAccountQueryService/BrowseLedgerAccounts.
func buildBrowseLedgerAccountsRequestForActivateReturnLabel(req *returnsv1.ActivateReturnLabelRequest) *ledgerv1.BrowseLedgerAccountsRequest {
	return &ledgerv1.BrowseLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.BrowseLedgerAccountsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			Labels:       req.GetAnnotations(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		},
	}
}

// buildExplainInspectionCalculationRequestForTerminateReturnLabel maps what TerminateReturnLabel has to the request of
// acme.returns.v1.InspectionCalculationService/ExplainInspectionCalculation.
func buildExplainInspectionCalculationRequestForTerminateReturnLabel(req *returnsv1.TerminateReturnLabelRequest) *returnsv1.ExplainInspectionCalculationRequest {
	return &returnsv1.ExplainInspectionCalculationRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Requester: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetNotifyContact().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Details: &returnsv1.ExplainInspectionCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
}
