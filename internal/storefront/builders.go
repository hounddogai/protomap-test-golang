package storefront

import (
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	procurementv1 "github.com/hounddogai/protomap-test-golang/gen/acme/procurement/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
)

// buildDeleteStorefrontSessionRequestForSubmitStorefrontSessionForm maps what SubmitStorefrontSessionForm has to the
// request of acme.storefront.v1.StorefrontSessionService/DeleteStorefrontSession.
func buildDeleteStorefrontSessionRequestForSubmitStorefrontSessionForm(req *storefrontv1.SubmitStorefrontSessionFormRequest) *storefrontv1.DeleteStorefrontSessionRequest {
	return &storefrontv1.DeleteStorefrontSessionRequest{
		Context:             req.GetContext(),
		StorefrontSessionId: req.GetStorefrontSession().GetStorefrontSessionId(),
		Etag:                req.GetStorefrontSession().GetAudit().GetEtag(),
		Reason:              req.GetReason(),
	}
}

// buildDisconnectLandingPageSourceRequestForTrackStorefrontSession maps what TrackStorefrontSession has to the request
// of acme.storefront.v1.LandingPageIntegrationService/DisconnectLandingPageSource.
func buildDisconnectLandingPageSourceRequestForTrackStorefrontSession(req *storefrontv1.TrackStorefrontSessionRequest) *storefrontv1.DisconnectLandingPageSourceRequest {
	return &storefrontv1.DisconnectLandingPageSourceRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details:     &storefrontv1.DisconnectLandingPageSourceRequest_Details{ReceivedAt: req.GetStorefrontSession().GetSla().GetDueAt()},
	}
}

// buildSummarizeLandingPagesRequestForCreateShopperView maps what CreateShopperView has to the request of
// acme.storefront.v1.LandingPageQueryService/SummarizeLandingPages.
func buildSummarizeLandingPagesRequestForCreateShopperView(req *storefrontv1.CreateShopperViewRequest, listLandingPageSubscriptionsReply *storefrontv1.ListLandingPageSubscriptionsResponse) *storefrontv1.SummarizeLandingPagesRequest {
	return &storefrontv1.SummarizeLandingPagesRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.SummarizeLandingPagesRequest_Details{
			Filters: listLandingPageSubscriptionsReply.GetResult().GetDetails(),
		},
	}
}

// buildPausePartnerOrderWorkflowRequestForDeleteStorefrontSession maps what DeleteStorefrontSession has to the request
// of acme.partnerapi.v1.PartnerOrderWorkflowService/PausePartnerOrderWorkflow.
func buildPausePartnerOrderWorkflowRequestForDeleteStorefrontSession(req *storefrontv1.DeleteStorefrontSessionRequest, updatePurchaseRequisitionReply *procurementv1.UpdatePurchaseRequisitionResponse) *partnerapiv1.PausePartnerOrderWorkflowRequest {
	return &partnerapiv1.PausePartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: updatePurchaseRequisitionReply.GetPurchaseRequisition().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildAdvancePartnerWebhookWorkflowRequestForNotifyLandingPageWatchers maps what NotifyLandingPageWatchers has to the
// request of acme.partnerapi.v1.PartnerWebhookWorkflowService/AdvancePartnerWebhookWorkflow.
func buildAdvancePartnerWebhookWorkflowRequestForNotifyLandingPageWatchers(req *storefrontv1.NotifyLandingPageWatchersRequest) *partnerapiv1.AdvancePartnerWebhookWorkflowRequest {
	return &partnerapiv1.AdvancePartnerWebhookWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.AdvancePartnerWebhookWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetRecipient().GetName(),
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
		},
	}
}

// buildResumeShopperViewWorkflowRequestForStreamLandingPageNotices maps what StreamLandingPageNotices has to the
// request of acme.storefront.v1.ShopperViewWorkflowService/ResumeShopperViewWorkflow.
func buildResumeShopperViewWorkflowRequestForStreamLandingPageNotices(req *storefrontv1.StreamLandingPageNoticesRequest, pushPreferenceReply *customersv1.PushPreferenceResponse, pushSynonymReply *searchv1.PushSynonymResponse) *storefrontv1.ResumeShopperViewWorkflowRequest {
	return &storefrontv1.ResumeShopperViewWorkflowRequest{
		Context: req.GetContext(),
		ShopperView: &storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				UserId:      pushSynonymReply.GetSynonym().GetOwner().GetUserId(),
				DisplayName: pushPreferenceReply.GetPreference().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: pushPreferenceReply.GetPreference().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildSubscribeStorefrontSessionUpdatesRequestForResolveStorefrontSessions maps what ResolveStorefrontSessions has to
// the request of acme.storefront.v1.StorefrontSessionNotificationService/SubscribeStorefrontSessionUpdates.
func buildSubscribeStorefrontSessionUpdatesRequestForResolveStorefrontSessions(req *storefrontv1.ResolveStorefrontSessionsRequest) *storefrontv1.SubscribeStorefrontSessionUpdatesRequest {
	return &storefrontv1.SubscribeStorefrontSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.SubscribeStorefrontSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Annotations: req.GetContext().GetBaggage(),
	}
}

// buildUnsubscribeShopperViewUpdatesRequestForResolveStorefrontSessions maps what ResolveStorefrontSessions has to the
// request of acme.storefront.v1.ShopperViewNotificationService/UnsubscribeShopperViewUpdates.
func buildUnsubscribeShopperViewUpdatesRequestForResolveStorefrontSessions(req *storefrontv1.ResolveStorefrontSessionsRequest, subscribeStorefrontSessionUpdatesReply *storefrontv1.SubscribeStorefrontSessionUpdatesResponse) *storefrontv1.UnsubscribeShopperViewUpdatesRequest {
	return &storefrontv1.UnsubscribeShopperViewUpdatesRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.UnsubscribeShopperViewUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: subscribeStorefrontSessionUpdatesReply.GetStorefrontSession().GetRequester().GetPreferredChannel(),
			},
			Channel: subscribeStorefrontSessionUpdatesReply.GetStorefrontSession().GetChannel().GetChannel(),
		},
	}
}

// buildPauseLandingPageWorkflowRequestForSendStorefrontSessionTestNotice maps what SendStorefrontSessionTestNotice has
// to the request of acme.storefront.v1.LandingPageWorkflowService/PauseLandingPageWorkflow.
func buildPauseLandingPageWorkflowRequestForSendStorefrontSessionTestNotice(req *storefrontv1.SendStorefrontSessionTestNoticeRequest, cancelStorefrontSessionWorkflowReply *storefrontv1.CancelStorefrontSessionWorkflowResponse) *storefrontv1.PauseLandingPageWorkflowRequest {
	return &storefrontv1.PauseLandingPageWorkflowRequest{
		Context: req.GetContext(),
		LandingPage: &storefrontv1.LandingPage{
			Audit: &commonv1.AuditInfo{
				CreatedAt: cancelStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetCreatedAt(),
			},
		},
	}
}

// buildStartConsentRecordWorkflowRequestForReplayShopperViewEvents maps what ReplayShopperViewEvents has to the request
// of acme.customers.v1.ConsentRecordWorkflowService/StartConsentRecordWorkflow.
func buildStartConsentRecordWorkflowRequestForReplayShopperViewEvents(req *storefrontv1.ReplayShopperViewEventsRequest, disconnectStorefrontSessionSourceReply *storefrontv1.DisconnectStorefrontSessionSourceResponse) *customersv1.StartConsentRecordWorkflowRequest {
	return &customersv1.StartConsentRecordWorkflowRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: disconnectStorefrontSessionSourceReply.GetStorefrontSession().GetCustomer().GetDisplayName(),
			},
		},
		Details: &customersv1.StartConsentRecordWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:  disconnectStorefrontSessionSourceReply.GetStorefrontSession().GetRequester().GetName(),
				Email: disconnectStorefrontSessionSourceReply.GetStorefrontSession().GetAssignee().GetEmail(),
			},
		},
	}
}

// buildBatchGetCustomersRequestForPauseLandingPageWorkflow maps what PauseLandingPageWorkflow has to the request of
// acme.customers.v1.CustomerService/BatchGetCustomers.
func buildBatchGetCustomersRequestForPauseLandingPageWorkflow(req *storefrontv1.PauseLandingPageWorkflowRequest) *customersv1.BatchGetCustomersRequest {
	return &customersv1.BatchGetCustomersRequest{Context: req.GetContext()}
}

// buildStartStorefrontSessionWorkflowRequestForConnectStorefrontSessionSource maps what ConnectStorefrontSessionSource
// has to the request of acme.storefront.v1.StorefrontSessionWorkflowService/StartStorefrontSessionWorkflow.
func buildStartStorefrontSessionWorkflowRequestForConnectStorefrontSessionSource(req *storefrontv1.ConnectStorefrontSessionSourceRequest, lintAccessPoliciesReply *authorizationv1.LintAccessPoliciesResponse) *storefrontv1.StartStorefrontSessionWorkflowRequest {
	return &storefrontv1.StartStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: lintAccessPoliciesReply.GetAccessPolicy().GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{
				Email: lintAccessPoliciesReply.GetAccessPolicy().GetOwner().GetEmail(),
			},
		},
	}
}

// buildResumeStorefrontSessionWorkflowRequestForAcknowledgeStorefrontSessionEvent maps what
// AcknowledgeStorefrontSessionEvent has to the request of
// acme.storefront.v1.StorefrontSessionWorkflowService/ResumeStorefrontSessionWorkflow.
func buildResumeStorefrontSessionWorkflowRequestForAcknowledgeStorefrontSessionEvent(req *storefrontv1.AcknowledgeStorefrontSessionEventRequest, approveCustomerReply *customersv1.ApproveCustomerResponse, pauseStorefrontSessionWorkflowReply *storefrontv1.PauseStorefrontSessionWorkflowResponse) *storefrontv1.ResumeStorefrontSessionWorkflowRequest {
	return &storefrontv1.ResumeStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Assignee: &identityv1.UserRef{
				Email: approveCustomerReply.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: pauseStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetEtag(),
			},
		},
		Details: &storefrontv1.ResumeStorefrontSessionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:             pauseStorefrontSessionWorkflowReply.GetStorefrontSession().GetRequester().GetName(),
				PreferredChannel: pauseStorefrontSessionWorkflowReply.GetStorefrontSession().GetRequester().GetPreferredChannel(),
			},
		},
	}
}

// buildAdvanceStorefrontSessionWorkflowRequestForListStorefrontSessionSources maps what ListStorefrontSessionSources
// has to the request of acme.storefront.v1.StorefrontSessionWorkflowService/AdvanceStorefrontSessionWorkflow.
func buildAdvanceStorefrontSessionWorkflowRequestForListStorefrontSessionSources(req *storefrontv1.ListStorefrontSessionSourcesRequest) *storefrontv1.AdvanceStorefrontSessionWorkflowRequest {
	return &storefrontv1.AdvanceStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.AdvanceStorefrontSessionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Assignee: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			History:  []*storefrontv1.StorefrontSessionStatusChange{&storefrontv1.StorefrontSessionStatusChange{ChangedAt: req.GetFilter().GetCreatedAfter()}},
		},
	}
}

// buildResumeStorefrontSessionWorkflowRequestForCancelShopperViewWorkflow maps what CancelShopperViewWorkflow has to
// the request of acme.storefront.v1.StorefrontSessionWorkflowService/ResumeStorefrontSessionWorkflow.
func buildResumeStorefrontSessionWorkflowRequestForCancelShopperViewWorkflow(req *storefrontv1.CancelShopperViewWorkflowRequest, retryStorefrontSessionWorkflowReply *storefrontv1.RetryStorefrontSessionWorkflowResponse) *storefrontv1.ResumeStorefrontSessionWorkflowRequest {
	return &storefrontv1.ResumeStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Sla: &storefrontv1.StorefrontSessionSla{
				Breached: retryStorefrontSessionWorkflowReply.GetStorefrontSession().GetSla().GetBreached(),
			},
			Comments: []*storefrontv1.StorefrontSessionComment{&storefrontv1.StorefrontSessionComment{
				CreatedAt: retryStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetCreatedAt(),
			}},
		},
	}
}

// buildAdvancePartnerIntegrationWorkflowRequestForInspectShopperViewWorkflow maps what InspectShopperViewWorkflow has
// to the request of acme.partnerapi.v1.PartnerIntegrationWorkflowService/AdvancePartnerIntegrationWorkflow.
func buildAdvancePartnerIntegrationWorkflowRequestForInspectShopperViewWorkflow(req *storefrontv1.InspectShopperViewWorkflowRequest) *partnerapiv1.AdvancePartnerIntegrationWorkflowRequest {
	return &partnerapiv1.AdvancePartnerIntegrationWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.AdvancePartnerIntegrationWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
			Funding: &partnerapiv1.PartnerIntegration_ExternalAccountId{ExternalAccountId: req.GetShopperViewId()},
		},
	}
}

// buildArchiveTenantRequestForListShopperViewWorkflows maps what ListShopperViewWorkflows has to the request of
// acme.identity.v1.TenantAdminService/ArchiveTenant.
func buildArchiveTenantRequestForListShopperViewWorkflows(req *storefrontv1.ListShopperViewWorkflowsRequest) *identityv1.ArchiveTenantRequest {
	return &identityv1.ArchiveTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.ArchiveTenantRequest_Details{
			NotifyContact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			NewOwner:      &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			EffectiveAt:   req.GetFilter().GetCreatedAfter(),
		},
	}
}

// buildListSearchIndexSubscriptionsRequestForShareLandingPageReport maps what ShareLandingPageReport has to the request
// of acme.search.v1.SearchIndexNotificationService/ListSearchIndexSubscriptions.
func buildListSearchIndexSubscriptionsRequestForShareLandingPageReport(req *storefrontv1.ShareLandingPageReportRequest) *searchv1.ListSearchIndexSubscriptionsRequest {
	return &searchv1.ListSearchIndexSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexSubscriptionsRequest_Filter{
			Labels:  req.GetAnnotations(),
			OwnerId: req.GetLandingPageId(),
		},
	}
}

// buildResumePartnerOrderWorkflowRequestForRetryStorefrontSessionWorkflow maps what RetryStorefrontSessionWorkflow has
// to the request of acme.partnerapi.v1.PartnerOrderWorkflowService/ResumePartnerOrderWorkflow.
func buildResumePartnerOrderWorkflowRequestForRetryStorefrontSessionWorkflow(req *storefrontv1.RetryStorefrontSessionWorkflowRequest) *partnerapiv1.ResumePartnerOrderWorkflowRequest {
	return &partnerapiv1.ResumePartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				DisplayName: req.GetStorefrontSession().GetAssignee().GetDisplayName(),
				Email:       req.GetDetails().GetNotify().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetStorefrontSession().GetAudit().GetCreatedAt(),
			},
		},
	}
}
