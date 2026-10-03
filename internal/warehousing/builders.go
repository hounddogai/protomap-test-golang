package warehousing

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
)

// buildSuggestWarehousesRequestForGetWarehouse maps what GetWarehouse has to the request of
// acme.warehousing.v1.WarehouseQueryService/SuggestWarehouses.
func buildSuggestWarehousesRequestForGetWarehouse(req *warehousingv1.GetWarehouseRequest) *warehousingv1.SuggestWarehousesRequest {
	return &warehousingv1.SuggestWarehousesRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.SuggestWarehousesRequest_Filter{
			OwnerId:      req.GetWarehouseId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	}
}

// buildAdvanceGiftCardBatchWorkflowRequestForResumePickListWorkflow maps what ResumePickListWorkflow has to the request
// of acme.giftcards.v1.GiftCardBatchWorkflowService/AdvanceGiftCardBatchWorkflow.
func buildAdvanceGiftCardBatchWorkflowRequestForResumePickListWorkflow(req *warehousingv1.ResumePickListWorkflowRequest) *giftcardsv1.AdvanceGiftCardBatchWorkflowRequest {
	return &giftcardsv1.AdvanceGiftCardBatchWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.AdvanceGiftCardBatchWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
}

// collectStorageBinNames lists the names of a storage bin and of everything under it.
func collectStorageBinNames(node *warehousingv1.StorageBin, names []string) []string {
	if node == nil {
		return names
	}
	names = append(names, node.GetName())
	for _, child := range node.GetChildren() {
		names = collectStorageBinNames(child, names)
	}
	return names
}

// buildPreviewNotificationRequestForApprovePickList maps what ApprovePickList has to the request of
// acme.notifications.v1.NotificationValidationService/PreviewNotification.
func buildPreviewNotificationRequestForApprovePickList(req *warehousingv1.ApprovePickListRequest) *notificationsv1.PreviewNotificationRequest {
	return &notificationsv1.PreviewNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetPickList().GetAudit().GetUpdatedBy(),
				Etag:      req.GetPickList().GetAudit().GetEtag(),
			},
			Comments: []*notificationsv1.NotificationComment{&notificationsv1.NotificationComment{CreatedAt: req.GetPickList().GetAudit().GetCreatedAt()}},
			Assignee: &identityv1.UserRef{Email: req.GetPickList().GetCounterparty().GetEmail()},
		},
	}
}

// buildNormalizePickListRequestForScorePutawayTask maps what ScorePutawayTask has to the request of
// acme.warehousing.v1.PickListValidationService/NormalizePickList.
func buildNormalizePickListRequestForScorePutawayTask(req *warehousingv1.ScorePutawayTaskRequest) *warehousingv1.NormalizePickListRequest {
	return &warehousingv1.NormalizePickListRequest{Context: req.GetContext()}
}

// buildSuggestStorageBinsRequestForResumeWarehouseWorkflow maps what ResumeWarehouseWorkflow has to the request of
// acme.warehousing.v1.StorageBinQueryService/SuggestStorageBins.
func buildSuggestStorageBinsRequestForResumeWarehouseWorkflow(req *warehousingv1.ResumeWarehouseWorkflowRequest) *warehousingv1.SuggestStorageBinsRequest {
	return &warehousingv1.SuggestStorageBinsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.SuggestStorageBinsRequest_Filter{
			Region:       req.GetWarehouse().GetAddress().GetRegion(),
			CreatedAfter: req.GetWarehouse().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetWarehouse().GetComponents()["primary"].GetComponentId(),
		},
	}
}

// buildCloneVenueRequestForCheckPickListEligibility maps what CheckPickListEligibility has to the request of
// acme.venues.v1.VenueService/CloneVenue.
func buildCloneVenueRequestForCheckPickListEligibility(req *warehousingv1.CheckPickListEligibilityRequest) *venuesv1.CloneVenueRequest {
	return &venuesv1.CloneVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{Email: req.GetPickList().GetCounterparty().GetEmail()},
			Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{
				CountryCode: req.GetPickList().GetBillingAddress().GetCountryCode(),
			}},
			Audit: &commonv1.AuditInfo{Etag: req.GetPickList().GetAudit().GetEtag()},
		},
	}
}

// buildFindNotificationsRequestForNormalizePickList maps what NormalizePickList has to the request of
// acme.notifications.v1.NotificationQueryService/FindNotifications.
func buildFindNotificationsRequestForNormalizePickList(req *warehousingv1.NormalizePickListRequest) *notificationsv1.FindNotificationsRequest {
	return &notificationsv1.FindNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.FindNotificationsRequest_Filter{
			Region:       req.GetPickList().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPickList().GetAudit().GetCreatedAt(),
		},
	}
}
