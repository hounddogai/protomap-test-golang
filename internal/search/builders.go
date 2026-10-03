package search

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
)

// buildUnsubscribeSearchIndexUpdatesRequestForPreviewSearchQuery maps what PreviewSearchQuery has to the request of
// acme.search.v1.SearchIndexNotificationService/UnsubscribeSearchIndexUpdates.
func buildUnsubscribeSearchIndexUpdatesRequestForPreviewSearchQuery(req *searchv1.PreviewSearchQueryRequest) *searchv1.UnsubscribeSearchIndexUpdatesRequest {
	return &searchv1.UnsubscribeSearchIndexUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSearchIndexUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetContact().GetPhone(),
				Email:            req.GetDetails().GetContact().GetEmail(),
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Name:             req.GetDetails().GetContact().GetName(),
			},
		},
	}
}

// buildSyncSynonymsRequestForArchiveSearchIndex maps what ArchiveSearchIndex has to the request of
// acme.search.v1.SynonymSyncService/SyncSynonyms.
func buildSyncSynonymsRequestForArchiveSearchIndex(req *searchv1.ArchiveSearchIndexRequest) *searchv1.SyncSynonymsRequest {
	return &searchv1.SyncSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetNotifyContact().GetEmail(),
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
			},
			Action: &searchv1.Synonym_Block{Block: &searchv1.SynonymBlockAction{
				Reason:      req.GetReason(),
				NotifyEmail: req.GetDetails().GetNewOwner().GetEmail(),
			}},
		},
	}
}

// buildBrowseFeatureFlagsRequestForGetSearchQueryExport maps what GetSearchQueryExport has to the request of
// acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags.
func buildBrowseFeatureFlagsRequestForGetSearchQueryExport(req *searchv1.GetSearchQueryExportRequest) *featureflagsv1.BrowseFeatureFlagsRequest {
	return &featureflagsv1.BrowseFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.BrowseFeatureFlagsRequest_Filter{
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      req.GetSearchQueryId(),
		},
	}
}
