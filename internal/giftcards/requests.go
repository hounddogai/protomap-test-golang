package giftcards

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	payrollv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
)

// calculatePayRunOption sets part of a request of acme.payroll.v1.PayRunCalculationService/CalculatePayRun.
type calculatePayRunOption func(*payrollv1.CalculatePayRunRequest)

// newCalculatePayRunRequest builds a request of acme.payroll.v1.PayRunCalculationService/CalculatePayRun from options.
func newCalculatePayRunRequest(options ...calculatePayRunOption) *payrollv1.CalculatePayRunRequest {
	request := &payrollv1.CalculatePayRunRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCalculatePayRunContext(value *commonv1.RequestContext) calculatePayRunOption {
	return func(request *payrollv1.CalculatePayRunRequest) { request.Context = value }
}

func withCalculatePayRunDetails(value *payrollv1.CalculatePayRunRequest_Details) calculatePayRunOption {
	return func(request *payrollv1.CalculatePayRunRequest) { request.Details = value }
}

func withCalculatePayRunPayRun(value *payrollv1.PayRun) calculatePayRunOption {
	return func(request *payrollv1.CalculatePayRunRequest) { request.PayRun = value }
}

// suggestDocumentsOption sets part of a request of acme.documents.v1.DocumentQueryService/SuggestDocuments.
type suggestDocumentsOption func(*documentsv1.SuggestDocumentsRequest)

// newSuggestDocumentsRequest builds a request of acme.documents.v1.DocumentQueryService/SuggestDocuments from options.
func newSuggestDocumentsRequest(options ...suggestDocumentsOption) *documentsv1.SuggestDocumentsRequest {
	request := &documentsv1.SuggestDocumentsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSuggestDocumentsContext(value *commonv1.RequestContext) suggestDocumentsOption {
	return func(request *documentsv1.SuggestDocumentsRequest) { request.Context = value }
}

func withSuggestDocumentsFilter(value *documentsv1.SuggestDocumentsRequest_Filter) suggestDocumentsOption {
	return func(request *documentsv1.SuggestDocumentsRequest) { request.Filter = value }
}

// normalizeGiftCardTransactionOption sets part of a request of
// acme.giftcards.v1.GiftCardTransactionValidationService/NormalizeGiftCardTransaction.
type normalizeGiftCardTransactionOption func(*giftcardsv1.NormalizeGiftCardTransactionRequest)

// newNormalizeGiftCardTransactionRequest builds a request of
// acme.giftcards.v1.GiftCardTransactionValidationService/NormalizeGiftCardTransaction from options.
func newNormalizeGiftCardTransactionRequest(options ...normalizeGiftCardTransactionOption) *giftcardsv1.NormalizeGiftCardTransactionRequest {
	request := &giftcardsv1.NormalizeGiftCardTransactionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withNormalizeGiftCardTransactionContext(value *commonv1.RequestContext) normalizeGiftCardTransactionOption {
	return func(request *giftcardsv1.NormalizeGiftCardTransactionRequest) { request.Context = value }
}

func withNormalizeGiftCardTransactionDetails(value *giftcardsv1.NormalizeGiftCardTransactionRequest_Details) normalizeGiftCardTransactionOption {
	return func(request *giftcardsv1.NormalizeGiftCardTransactionRequest) { request.Details = value }
}

func withNormalizeGiftCardTransactionGiftCardTransaction(value *giftcardsv1.GiftCardTransaction) normalizeGiftCardTransactionOption {
	return func(request *giftcardsv1.NormalizeGiftCardTransactionRequest) { request.GiftCardTransaction = value }
}

// advanceGiftCardWorkflowOption sets part of a request of
// acme.giftcards.v1.GiftCardWorkflowService/AdvanceGiftCardWorkflow.
type advanceGiftCardWorkflowOption func(*giftcardsv1.AdvanceGiftCardWorkflowRequest)

// newAdvanceGiftCardWorkflowRequest builds a request of
// acme.giftcards.v1.GiftCardWorkflowService/AdvanceGiftCardWorkflow from options.
func newAdvanceGiftCardWorkflowRequest(options ...advanceGiftCardWorkflowOption) *giftcardsv1.AdvanceGiftCardWorkflowRequest {
	request := &giftcardsv1.AdvanceGiftCardWorkflowRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withAdvanceGiftCardWorkflowContext(value *commonv1.RequestContext) advanceGiftCardWorkflowOption {
	return func(request *giftcardsv1.AdvanceGiftCardWorkflowRequest) { request.Context = value }
}

func withAdvanceGiftCardWorkflowGiftCard(value *giftcardsv1.GiftCard) advanceGiftCardWorkflowOption {
	return func(request *giftcardsv1.AdvanceGiftCardWorkflowRequest) { request.GiftCard = value }
}

// summarizeGiftCardsOption sets part of a request of acme.giftcards.v1.GiftCardQueryService/SummarizeGiftCards.
type summarizeGiftCardsOption func(*giftcardsv1.SummarizeGiftCardsRequest)

// newSummarizeGiftCardsRequest builds a request of acme.giftcards.v1.GiftCardQueryService/SummarizeGiftCards from
// options.
func newSummarizeGiftCardsRequest(options ...summarizeGiftCardsOption) *giftcardsv1.SummarizeGiftCardsRequest {
	request := &giftcardsv1.SummarizeGiftCardsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSummarizeGiftCardsContext(value *commonv1.RequestContext) summarizeGiftCardsOption {
	return func(request *giftcardsv1.SummarizeGiftCardsRequest) { request.Context = value }
}

func withSummarizeGiftCardsGiftCardId(value string) summarizeGiftCardsOption {
	return func(request *giftcardsv1.SummarizeGiftCardsRequest) { request.GiftCardId = value }
}

// explainGiftCardCalculationOption sets part of a request of
// acme.giftcards.v1.GiftCardCalculationService/ExplainGiftCardCalculation.
type explainGiftCardCalculationOption func(*giftcardsv1.ExplainGiftCardCalculationRequest)

// newExplainGiftCardCalculationRequest builds a request of
// acme.giftcards.v1.GiftCardCalculationService/ExplainGiftCardCalculation from options.
func newExplainGiftCardCalculationRequest(options ...explainGiftCardCalculationOption) *giftcardsv1.ExplainGiftCardCalculationRequest {
	request := &giftcardsv1.ExplainGiftCardCalculationRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainGiftCardCalculationContext(value *commonv1.RequestContext) explainGiftCardCalculationOption {
	return func(request *giftcardsv1.ExplainGiftCardCalculationRequest) { request.Context = value }
}

func withExplainGiftCardCalculationGiftCard(value *giftcardsv1.GiftCard) explainGiftCardCalculationOption {
	return func(request *giftcardsv1.ExplainGiftCardCalculationRequest) { request.GiftCard = value }
}

func withExplainGiftCardCalculationDetails(value *giftcardsv1.ExplainGiftCardCalculationRequest_Details) explainGiftCardCalculationOption {
	return func(request *giftcardsv1.ExplainGiftCardCalculationRequest) { request.Details = value }
}

func withExplainGiftCardCalculationGiftCardId(value string) explainGiftCardCalculationOption {
	return func(request *giftcardsv1.ExplainGiftCardCalculationRequest) { request.GiftCardId = value }
}

// resolveDocumentsOption sets part of a request of acme.documents.v1.DocumentQueryService/ResolveDocuments.
type resolveDocumentsOption func(*documentsv1.ResolveDocumentsRequest)

// newResolveDocumentsRequest builds a request of acme.documents.v1.DocumentQueryService/ResolveDocuments from options.
func newResolveDocumentsRequest(options ...resolveDocumentsOption) *documentsv1.ResolveDocumentsRequest {
	request := &documentsv1.ResolveDocumentsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withResolveDocumentsContext(value *commonv1.RequestContext) resolveDocumentsOption {
	return func(request *documentsv1.ResolveDocumentsRequest) { request.Context = value }
}

// quoteGiftCardBatchOption sets part of a request of
// acme.giftcards.v1.GiftCardBatchCalculationService/QuoteGiftCardBatch.
type quoteGiftCardBatchOption func(*giftcardsv1.QuoteGiftCardBatchRequest)

// newQuoteGiftCardBatchRequest builds a request of acme.giftcards.v1.GiftCardBatchCalculationService/QuoteGiftCardBatch
// from options.
func newQuoteGiftCardBatchRequest(options ...quoteGiftCardBatchOption) *giftcardsv1.QuoteGiftCardBatchRequest {
	request := &giftcardsv1.QuoteGiftCardBatchRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withQuoteGiftCardBatchContext(value *commonv1.RequestContext) quoteGiftCardBatchOption {
	return func(request *giftcardsv1.QuoteGiftCardBatchRequest) { request.Context = value }
}

func withQuoteGiftCardBatchDetails(value *giftcardsv1.QuoteGiftCardBatchRequest_Details) quoteGiftCardBatchOption {
	return func(request *giftcardsv1.QuoteGiftCardBatchRequest) { request.Details = value }
}

func withQuoteGiftCardBatchGiftCardBatch(value *giftcardsv1.GiftCardBatch) quoteGiftCardBatchOption {
	return func(request *giftcardsv1.QuoteGiftCardBatchRequest) { request.GiftCardBatch = value }
}

// recalculateGiftCardsOption sets part of a request of
// acme.giftcards.v1.GiftCardCalculationService/RecalculateGiftCards.
type recalculateGiftCardsOption func(*giftcardsv1.RecalculateGiftCardsRequest)

// newRecalculateGiftCardsRequest builds a request of acme.giftcards.v1.GiftCardCalculationService/RecalculateGiftCards
// from options.
func newRecalculateGiftCardsRequest(options ...recalculateGiftCardsOption) *giftcardsv1.RecalculateGiftCardsRequest {
	request := &giftcardsv1.RecalculateGiftCardsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withRecalculateGiftCardsContext(value *commonv1.RequestContext) recalculateGiftCardsOption {
	return func(request *giftcardsv1.RecalculateGiftCardsRequest) { request.Context = value }
}

func withRecalculateGiftCardsGiftCard(value *giftcardsv1.GiftCard) recalculateGiftCardsOption {
	return func(request *giftcardsv1.RecalculateGiftCardsRequest) { request.GiftCard = value }
}

// validateGiftCardBatchOption sets part of a request of
// acme.giftcards.v1.GiftCardBatchValidationService/ValidateGiftCardBatch.
type validateGiftCardBatchOption func(*giftcardsv1.ValidateGiftCardBatchRequest)

// newValidateGiftCardBatchRequest builds a request of
// acme.giftcards.v1.GiftCardBatchValidationService/ValidateGiftCardBatch from options.
func newValidateGiftCardBatchRequest(options ...validateGiftCardBatchOption) *giftcardsv1.ValidateGiftCardBatchRequest {
	request := &giftcardsv1.ValidateGiftCardBatchRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withValidateGiftCardBatchContext(value *commonv1.RequestContext) validateGiftCardBatchOption {
	return func(request *giftcardsv1.ValidateGiftCardBatchRequest) { request.Context = value }
}

func withValidateGiftCardBatchDetails(value *giftcardsv1.ValidateGiftCardBatchRequest_Details) validateGiftCardBatchOption {
	return func(request *giftcardsv1.ValidateGiftCardBatchRequest) { request.Details = value }
}

// explainSoftwareLicenseCalculationOption sets part of a request of
// acme.licensing.v1.SoftwareLicenseCalculationService/ExplainSoftwareLicenseCalculation.
type explainSoftwareLicenseCalculationOption func(*licensingv1.ExplainSoftwareLicenseCalculationRequest)

// newExplainSoftwareLicenseCalculationRequest builds a request of
// acme.licensing.v1.SoftwareLicenseCalculationService/ExplainSoftwareLicenseCalculation from options.
func newExplainSoftwareLicenseCalculationRequest(options ...explainSoftwareLicenseCalculationOption) *licensingv1.ExplainSoftwareLicenseCalculationRequest {
	request := &licensingv1.ExplainSoftwareLicenseCalculationRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainSoftwareLicenseCalculationContext(value *commonv1.RequestContext) explainSoftwareLicenseCalculationOption {
	return func(request *licensingv1.ExplainSoftwareLicenseCalculationRequest) { request.Context = value }
}

func withExplainSoftwareLicenseCalculationSoftwareLicense(value *licensingv1.SoftwareLicense) explainSoftwareLicenseCalculationOption {
	return func(request *licensingv1.ExplainSoftwareLicenseCalculationRequest) { request.SoftwareLicense = value }
}

func withExplainSoftwareLicenseCalculationDetails(value *licensingv1.ExplainSoftwareLicenseCalculationRequest_Details) explainSoftwareLicenseCalculationOption {
	return func(request *licensingv1.ExplainSoftwareLicenseCalculationRequest) { request.Details = value }
}

// countLedgerAccountsOption sets part of a request of acme.ledger.v1.LedgerAccountQueryService/CountLedgerAccounts.
type countLedgerAccountsOption func(*ledgerv1.CountLedgerAccountsRequest)

// newCountLedgerAccountsRequest builds a request of acme.ledger.v1.LedgerAccountQueryService/CountLedgerAccounts from
// options.
func newCountLedgerAccountsRequest(options ...countLedgerAccountsOption) *ledgerv1.CountLedgerAccountsRequest {
	request := &ledgerv1.CountLedgerAccountsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCountLedgerAccountsContext(value *commonv1.RequestContext) countLedgerAccountsOption {
	return func(request *ledgerv1.CountLedgerAccountsRequest) { request.Context = value }
}

func withCountLedgerAccountsFilter(value *ledgerv1.CountLedgerAccountsRequest_Filter) countLedgerAccountsOption {
	return func(request *ledgerv1.CountLedgerAccountsRequest) { request.Filter = value }
}
