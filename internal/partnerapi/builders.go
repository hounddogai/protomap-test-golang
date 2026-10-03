package partnerapi

import (
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	driverappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/driverapp/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
)

// buildAdvancePartnerOrderWorkflowRequestForStartPartnerIntegrationCheckout maps what StartPartnerIntegrationCheckout
// has to the request of acme.partnerapi.v1.PartnerOrderWorkflowService/AdvancePartnerOrderWorkflow.
func buildAdvancePartnerOrderWorkflowRequestForStartPartnerIntegrationCheckout(req *partnerapiv1.StartPartnerIntegrationCheckoutRequest, signalPartnerOrderWorkflowReply *partnerapiv1.SignalPartnerOrderWorkflowResponse) *partnerapiv1.AdvancePartnerOrderWorkflowRequest {
	return &partnerapiv1.AdvancePartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.AdvancePartnerOrderWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetPartnerIntegration().GetContact().GetPreferredChannel(),
			},
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			BillingAddress: &commonv1.PostalAddress{
				Locality: signalPartnerOrderWorkflowReply.GetPartnerOrder().GetBillingAddress().GetLocality(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: signalPartnerOrderWorkflowReply.GetPartnerOrder().GetAudit().GetCreatedAt(),
			},
		},
	}
}

// buildConnectPartnerWebhookSourceRequestForSearchPartnerOrders maps what SearchPartnerOrders has to the request of
// acme.partnerapi.v1.PartnerWebhookIntegrationService/ConnectPartnerWebhookSource.
func buildConnectPartnerWebhookSourceRequestForSearchPartnerOrders(req *partnerapiv1.SearchPartnerOrdersRequest, createAccessPolicyReply *authorizationv1.CreateAccessPolicyResponse) *partnerapiv1.ConnectPartnerWebhookSourceRequest {
	return &partnerapiv1.ConnectPartnerWebhookSourceRequest{
		Context: req.GetContext(),
		Reason:  createAccessPolicyReply.GetAccessPolicy().GetBlock().GetReason(),
		Details: &partnerapiv1.ConnectPartnerWebhookSourceRequest_Details{MessageId: createAccessPolicyReply.GetResult().GetMessage()},
	}
}

// buildListPartnerOrderSubscriptionsRequestForUpdatePartnerIntegration maps what UpdatePartnerIntegration has to the
// request of acme.partnerapi.v1.PartnerOrderNotificationService/ListPartnerOrderSubscriptions.
func buildListPartnerOrderSubscriptionsRequestForUpdatePartnerIntegration(req *partnerapiv1.UpdatePartnerIntegrationRequest) *partnerapiv1.ListPartnerOrderSubscriptionsRequest {
	return &partnerapiv1.ListPartnerOrderSubscriptionsRequest{
		Context: req.GetContext(),
		Filter:  &partnerapiv1.ListPartnerOrderSubscriptionsRequest_Filter{Labels: req.GetPartnerIntegration().GetLabels()},
	}
}

// buildCancelSellerPayoutScheduleWorkflowRequestForCountPartnerOrders maps what CountPartnerOrders has to the request
// of acme.sellers.v1.SellerPayoutScheduleWorkflowService/CancelSellerPayoutScheduleWorkflow.
func buildCancelSellerPayoutScheduleWorkflowRequestForCountPartnerOrders(req *partnerapiv1.CountPartnerOrdersRequest) *sellersv1.CancelSellerPayoutScheduleWorkflowRequest {
	return &sellersv1.CancelSellerPayoutScheduleWorkflowRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
}

// buildDelegateMerchandisingRuleApprovalRequestForSendPartnerWebhookTestNotice maps what SendPartnerWebhookTestNotice
// has to the request of acme.merchandising.v1.MerchandisingRuleApprovalService/DelegateMerchandisingRuleApproval.
func buildDelegateMerchandisingRuleApprovalRequestForSendPartnerWebhookTestNotice(req *partnerapiv1.SendPartnerWebhookTestNoticeRequest) *merchandisingv1.DelegateMerchandisingRuleApprovalRequest {
	return &merchandisingv1.DelegateMerchandisingRuleApprovalRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Action: &merchandisingv1.MerchandisingRule_Block{Block: &merchandisingv1.MerchandisingRuleBlockAction{NotifyEmail: req.GetDetails().GetRecipient().GetEmail()}},
			Owner:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
}

// buildListPendingPartnerOrdersRequestForFindPartnerIntegrations maps what FindPartnerIntegrations has to the request
// of acme.partnerapi.v1.PartnerOrderApprovalService/ListPendingPartnerOrders.
func buildListPendingPartnerOrdersRequestForFindPartnerIntegrations(req *partnerapiv1.FindPartnerIntegrationsRequest) *partnerapiv1.ListPendingPartnerOrdersRequest {
	return &partnerapiv1.ListPendingPartnerOrdersRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
		Filter:  &partnerapiv1.ListPendingPartnerOrdersRequest_Filter{CreatedAfter: req.GetFilter().GetCreatedAfter()},
	}
}

// buildAdvancePartnerWebhookWorkflowRequestForConnectPartnerOrderSource maps what ConnectPartnerOrderSource has to the
// request of acme.partnerapi.v1.PartnerWebhookWorkflowService/AdvancePartnerWebhookWorkflow.
func buildAdvancePartnerWebhookWorkflowRequestForConnectPartnerOrderSource(req *partnerapiv1.ConnectPartnerOrderSourceRequest, previewPartnerWebhookReply *partnerapiv1.PreviewPartnerWebhookResponse) *partnerapiv1.AdvancePartnerWebhookWorkflowRequest {
	return &partnerapiv1.AdvancePartnerWebhookWorkflowRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				PartnerIntegrationId: previewPartnerWebhookReply.GetPartnerWebhook().GetSubject().GetPartnerIntegrationId(),
			},
		},
	}
}

// buildSignalPartnerOrderWorkflowRequestForTestPartnerOrderConnection maps what TestPartnerOrderConnection has to the
// request of acme.partnerapi.v1.PartnerOrderWorkflowService/SignalPartnerOrderWorkflow.
func buildSignalPartnerOrderWorkflowRequestForTestPartnerOrderConnection(req *partnerapiv1.TestPartnerOrderConnectionRequest) *partnerapiv1.SignalPartnerOrderWorkflowRequest {
	return &partnerapiv1.SignalPartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &partnerapiv1.SignalPartnerOrderWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{PartnerOrderId: req.GetPartnerOrderId()},
		Annotations:  req.GetAnnotations(),
	}
}

// buildResolveDriverHomesRequestForRetryPartnerWebhookWorkflow maps what RetryPartnerWebhookWorkflow has to the request
// of acme.driverapp.v1.DriverHomeQueryService/ResolveDriverHomes.
func buildResolveDriverHomesRequestForRetryPartnerWebhookWorkflow(req *partnerapiv1.RetryPartnerWebhookWorkflowRequest) *driverappv1.ResolveDriverHomesRequest {
	return &driverappv1.ResolveDriverHomesRequest{Context: req.GetContext()}
}

// buildListRoleSubscriptionsRequestForListPartnerWebhookWorkflows maps what ListPartnerWebhookWorkflows has to the
// request of acme.identity.v1.RoleNotificationService/ListRoleSubscriptions.
func buildListRoleSubscriptionsRequestForListPartnerWebhookWorkflows(req *partnerapiv1.ListPartnerWebhookWorkflowsRequest) *identityv1.ListRoleSubscriptionsRequest {
	return &identityv1.ListRoleSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListRoleSubscriptionsRequest_Filter{
			Labels: req.GetFilter().GetLabels(),
			Query:  req.GetFilter().GetQuery(),
			Region: req.GetFilter().GetRegion(),
		},
		OrderBy: req.GetOrderBy(),
	}
}

// buildStartSellerListingWorkflowRequestForAcknowledgePartnerIntegrationEvent maps what
// AcknowledgePartnerIntegrationEvent has to the request of
// acme.sellers.v1.SellerListingWorkflowService/StartSellerListingWorkflow.
func buildStartSellerListingWorkflowRequestForAcknowledgePartnerIntegrationEvent(req *partnerapiv1.AcknowledgePartnerIntegrationEventRequest, pauseCouponWorkflowReply *promotionsv1.PauseCouponWorkflowResponse) *sellersv1.StartSellerListingWorkflowRequest {
	return &sellersv1.StartSellerListingWorkflowRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				Email: pauseCouponWorkflowReply.GetCoupon().GetContact().GetEmail(),
			},
		},
		Details: &sellersv1.StartSellerListingWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: pauseCouponWorkflowReply.GetCoupon().GetContact().GetPreferredChannel(),
				Phone:            pauseCouponWorkflowReply.GetCoupon().GetContact().GetPhone(),
			},
		},
	}
}

// buildListStockItemWorkflowsRequestForAdvancePartnerOrderWorkflow maps what AdvancePartnerOrderWorkflow has to the
// request of acme.inventory.v1.StockItemWorkflowService/ListStockItemWorkflows.
func buildListStockItemWorkflowsRequestForAdvancePartnerOrderWorkflow(req *partnerapiv1.AdvancePartnerOrderWorkflowRequest, startStockItemWorkflowReply *inventoryv1.StartStockItemWorkflowResponse) *inventoryv1.ListStockItemWorkflowsRequest {
	return &inventoryv1.ListStockItemWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &inventoryv1.ListStockItemWorkflowsRequest_Filter{
			Region:       startStockItemWorkflowReply.GetStockItem().GetAddress().GetRegion(),
			CreatedAfter: req.GetPartnerOrder().GetAudit().GetUpdatedAt(),
			Labels:       req.GetDetails().GetParameters(),
		},
	}
}

// buildCountPartnerIntegrationTotalsRequestForSharePartnerWebhookReport maps what SharePartnerWebhookReport has to the
// request of acme.partnerapi.v1.PartnerIntegrationReportingService/CountPartnerIntegrationTotals.
func buildCountPartnerIntegrationTotalsRequestForSharePartnerWebhookReport(req *partnerapiv1.SharePartnerWebhookReportRequest, sharePartnerOrderReportReply *partnerapiv1.SharePartnerOrderReportResponse) *partnerapiv1.CountPartnerIntegrationTotalsRequest {
	return &partnerapiv1.CountPartnerIntegrationTotalsRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.CountPartnerIntegrationTotalsRequest_Filter{
			Region: sharePartnerOrderReportReply.GetPartnerOrder().GetBillingAddress().GetRegion(),
		},
	}
}

// buildSharePartnerIntegrationReportRequestForPausePartnerIntegrationWorkflow maps what PausePartnerIntegrationWorkflow
// has to the request of acme.partnerapi.v1.PartnerIntegrationReportingService/SharePartnerIntegrationReport.
func buildSharePartnerIntegrationReportRequestForPausePartnerIntegrationWorkflow(req *partnerapiv1.PausePartnerIntegrationWorkflowRequest) *partnerapiv1.SharePartnerIntegrationReportRequest {
	return &partnerapiv1.SharePartnerIntegrationReportRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.SharePartnerIntegrationReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Name: req.GetPartnerIntegration().GetContact().GetName()},
		},
	}
}

// buildRecallSellerPayoutScheduleRequestForSchedulePartnerOrderReport maps what SchedulePartnerOrderReport has to the
// request of acme.sellers.v1.SellerPayoutScheduleApprovalService/RecallSellerPayoutSchedule.
func buildRecallSellerPayoutScheduleRequestForSchedulePartnerOrderReport(req *partnerapiv1.SchedulePartnerOrderReportRequest) *sellersv1.RecallSellerPayoutScheduleRequest {
	return &sellersv1.RecallSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		Details: &sellersv1.RecallSellerPayoutScheduleRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
}

// buildNormalizePartnerWebhookRequestForStreamPartnerOrderMetrics maps what StreamPartnerOrderMetrics has to the
// request of acme.partnerapi.v1.PartnerWebhookValidationService/NormalizePartnerWebhook.
func buildNormalizePartnerWebhookRequestForStreamPartnerOrderMetrics(req *partnerapiv1.StreamPartnerOrderMetricsRequest, submitPartnerWebhookReply *partnerapiv1.SubmitPartnerWebhookResponse) *partnerapiv1.NormalizePartnerWebhookRequest {
	return &partnerapiv1.NormalizePartnerWebhookRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName:  submitPartnerWebhookReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
				CurrencyCode: submitPartnerWebhookReply.GetPartnerWebhook().GetSubject().GetCurrencyCode(),
			},
			PartnerWebhookId: submitPartnerWebhookReply.GetPartnerWebhook().GetPartnerWebhookId(),
		},
		Details: &partnerapiv1.NormalizePartnerWebhookRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetSubscriber().GetName()},
		},
	}
}

// buildPreviewVenueRequestForDeletePartnerOrderReport maps what DeletePartnerOrderReport has to the request of
// acme.venues.v1.VenueValidationService/PreviewVenue.
func buildPreviewVenueRequestForDeletePartnerOrderReport(req *partnerapiv1.DeletePartnerOrderReportRequest, startSellerListingWorkflowReply *sellersv1.StartSellerListingWorkflowResponse) *venuesv1.PreviewVenueRequest {
	return &venuesv1.PreviewVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{
				CountryCode: startSellerListingWorkflowReply.GetSellerListing().GetAddress().GetCountryCode(),
			}},
			Owner: &identityv1.UserRef{
				Email: startSellerListingWorkflowReply.GetSellerListing().GetOwner().GetEmail(),
			},
			Components: map[string]*venuesv1.VenueComponent{"primary": &venuesv1.VenueComponent{
				Description: startSellerListingWorkflowReply.GetSellerListing().GetComponents()["primary"].GetDescription(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag: startSellerListingWorkflowReply.GetSellerListing().GetAudit().GetEtag(),
			},
		},
	}
}

// buildCommentPartnerWebhookApprovalRequestForDeletePartnerOrderReport maps what DeletePartnerOrderReport has to the
// request of acme.partnerapi.v1.PartnerWebhookApprovalService/CommentPartnerWebhookApproval.
func buildCommentPartnerWebhookApprovalRequestForDeletePartnerOrderReport(req *partnerapiv1.DeletePartnerOrderReportRequest, approvePartnerWebhookReply *partnerapiv1.ApprovePartnerWebhookResponse, previewVenueReply *venuesv1.PreviewVenueResponse) *partnerapiv1.CommentPartnerWebhookApprovalRequest {
	return &partnerapiv1.CommentPartnerWebhookApprovalRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName:  previewVenueReply.GetVenue().GetOwner().GetDisplayName(),
				CurrencyCode: approvePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetCurrencyCode(),
			},
			Measurements: []*partnerapiv1.PartnerWebhookMeasurement{&partnerapiv1.PartnerWebhookMeasurement{
				Unit: previewVenueReply.GetVenue().GetDimensions().GetUnit(),
			}},
		},
		Details: &partnerapiv1.CommentPartnerWebhookApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: approvePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
		},
	}
}

// buildNormalizeLedgerAccountRequestForListPendingPartnerWebhooks maps what ListPendingPartnerWebhooks has to the
// request of acme.ledger.v1.LedgerAccountValidationService/NormalizeLedgerAccount.
func buildNormalizeLedgerAccountRequestForListPendingPartnerWebhooks(req *partnerapiv1.ListPendingPartnerWebhooksRequest) *ledgerv1.NormalizeLedgerAccountRequest {
	return &ledgerv1.NormalizeLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Labels:  req.GetFilter().GetLabels(),
		},
	}
}

// buildListPendingPartnerOrdersRequestForCountPartnerIntegrationTotals maps what CountPartnerIntegrationTotals has to
// the request of acme.partnerapi.v1.PartnerOrderApprovalService/ListPendingPartnerOrders.
func buildListPendingPartnerOrdersRequestForCountPartnerIntegrationTotals(req *partnerapiv1.CountPartnerIntegrationTotalsRequest) *partnerapiv1.ListPendingPartnerOrdersRequest {
	return &partnerapiv1.ListPendingPartnerOrdersRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPendingPartnerOrdersRequest_Filter{
			Query:  req.GetFilter().GetQuery(),
			Labels: req.GetFilter().GetLabels(),
		},
		Page: &commonv1.PageRequest{
			PageSize:  req.GetPage().GetPageSize(),
			PageToken: req.GetPage().GetPageToken(),
		},
	}
}
