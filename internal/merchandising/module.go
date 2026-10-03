package merchandising

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	payrollv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the merchandising domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.identity.v1.SessionQueryService", identityv1.NewSessionQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.merchandising.v1.CollectionApprovalService", merchandisingv1.NewCollectionApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.merchandising.v1.PlacementValidationService", merchandisingv1.NewPlacementValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.payroll.v1.PayRunQueryService", payrollv1.NewPayRunQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SearchQueryExportService", searchv1.NewSearchQueryExportServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerApprovalService", sellersv1.NewSellerApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerListingReportingService", sellersv1.NewSellerListingReportingServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerWorkflowService", sellersv1.NewSellerWorkflowServiceClient(deps.Conn))
	merchandisingv1.RegisterCollectionServiceServer(server, NewCollectionServer(deps))
	merchandisingv1.RegisterPlacementQueryServiceServer(server, NewPlacementQueryServer(deps))
	merchandisingRuleWorkflowServer := NewMerchandisingRuleWorkflowServer(deps)
	server.RegisterService(&merchandisingv1.MerchandisingRuleWorkflowService_ServiceDesc, merchandisingRuleWorkflowServer)
	merchandisingv1.RegisterCollectionQueryServiceServer(server, NewCollectionQueryServer(deps))
	placementWorkflowServer := NewPlacementWorkflowServer(deps)
	server.RegisterService(&merchandisingv1.PlacementWorkflowService_ServiceDesc, placementWorkflowServer)
	merchandisingv1.RegisterMerchandisingRuleApprovalServiceServer(server, NewMerchandisingRuleApprovalServer(deps))
	merchandisingv1.RegisterCollectionWorkflowServiceServer(server, NewCollectionWorkflowServer(deps))
	merchandisingv1.RegisterPlacementApprovalServiceServer(server, NewPlacementApprovalServer(deps))
	merchandisingv1.RegisterMerchandisingRuleValidationServiceServer(server, NewMerchandisingRuleValidationServer(deps))
	merchandisingv1.RegisterCollectionApprovalServiceServer(server, NewCollectionApprovalServer(deps))
	merchandisingv1.RegisterPlacementValidationServiceServer(server, NewPlacementValidationServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	explainMerchandisingRuleDecisionCall func(context.Context, *merchandisingv1.ExplainMerchandisingRuleDecisionRequest, ...grpc.CallOption) (*merchandisingv1.ExplainMerchandisingRuleDecisionResponse, error)
	recallGiftCardCall                   func(context.Context, *giftcardsv1.RecallGiftCardRequest) (*giftcardsv1.RecallGiftCardResponse, error)
	createCollectionCall                 func(context.Context, *merchandisingv1.CreateCollectionRequest) (*merchandisingv1.CreateCollectionResponse, error)
	checkAccessPolicyEligibilityCall     func(context.Context, *authorizationv1.CheckAccessPolicyEligibilityRequest) (*authorizationv1.CheckAccessPolicyEligibilityResponse, error)
	summarizeSellersCall                 func(context.Context, *sellersv1.SummarizeSellersRequest, ...grpc.CallOption) (*sellersv1.SummarizeSellersResponse, error)
	previewGiftCardBatchCall             func(context.Context, *giftcardsv1.PreviewGiftCardBatchRequest, ...grpc.CallOption) (*giftcardsv1.PreviewGiftCardBatchResponse, error)
	cancelGiftCardWorkflowCall           func(context.Context, *giftcardsv1.CancelGiftCardWorkflowRequest, ...grpc.CallOption) (*giftcardsv1.CancelGiftCardWorkflowResponse, error)
	suggestPlacementsCall                func(context.Context, *merchandisingv1.SuggestPlacementsRequest) (*merchandisingv1.SuggestPlacementsResponse, error)
	advanceGiftCardWorkflowCall          func(context.Context, *giftcardsv1.AdvanceGiftCardWorkflowRequest) (*giftcardsv1.AdvanceGiftCardWorkflowResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	merchandisingRuleValidationForExplainMerchandisingRuleDecision := merchandisingv1.NewMerchandisingRuleValidationServiceClient(deps.Conn)
	explainMerchandisingRuleDecisionCall = merchandisingRuleValidationForExplainMerchandisingRuleDecision.ExplainMerchandisingRuleDecision
	giftCardApprovalForRecallGiftCard := giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn)
	recallGiftCardCall = func(ctx context.Context, request *giftcardsv1.RecallGiftCardRequest) (*giftcardsv1.RecallGiftCardResponse, error) {
		return giftCardApprovalForRecallGiftCard.RecallGiftCard(ctx, request)
	}
	collectionForCreateCollection := merchandisingv1.NewCollectionServiceClient(deps.Conn)
	createCollectionCall = func(ctx context.Context, request *merchandisingv1.CreateCollectionRequest) (*merchandisingv1.CreateCollectionResponse, error) {
		return collectionForCreateCollection.CreateCollection(ctx, request)
	}
	accessPolicyValidationForCheckAccessPolicyEligibility := authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn)
	checkAccessPolicyEligibilityCall = func(ctx context.Context, request *authorizationv1.CheckAccessPolicyEligibilityRequest) (*authorizationv1.CheckAccessPolicyEligibilityResponse, error) {
		return accessPolicyValidationForCheckAccessPolicyEligibility.CheckAccessPolicyEligibility(ctx, request)
	}
	sellerQueryForSummarizeSellers := sellersv1.NewSellerQueryServiceClient(deps.Conn)
	summarizeSellersCall = sellerQueryForSummarizeSellers.SummarizeSellers
	giftCardBatchValidationForPreviewGiftCardBatch := giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn)
	previewGiftCardBatchCall = giftCardBatchValidationForPreviewGiftCardBatch.PreviewGiftCardBatch
	giftCardWorkflowForCancelGiftCardWorkflow := giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn)
	cancelGiftCardWorkflowCall = giftCardWorkflowForCancelGiftCardWorkflow.CancelGiftCardWorkflow
	placementQueryForSuggestPlacements := merchandisingv1.NewPlacementQueryServiceClient(deps.Conn)
	suggestPlacementsCall = func(ctx context.Context, request *merchandisingv1.SuggestPlacementsRequest) (*merchandisingv1.SuggestPlacementsResponse, error) {
		return placementQueryForSuggestPlacements.SuggestPlacements(ctx, request)
	}
	giftCardWorkflowForAdvanceGiftCardWorkflow := giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn)
	advanceGiftCardWorkflowCall = func(ctx context.Context, request *giftcardsv1.AdvanceGiftCardWorkflowRequest) (*giftcardsv1.AdvanceGiftCardWorkflowResponse, error) {
		return giftCardWorkflowForAdvanceGiftCardWorkflow.AdvanceGiftCardWorkflow(ctx, request)
	}
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                       *slog.Logger
	placementValidationClient merchandisingv1.PlacementValidationServiceClient
	collectionClient          merchandisingv1.CollectionServiceClient
	searchIndexQueryClient    searchv1.SearchIndexQueryServiceClient
	placementQueryClient      merchandisingv1.PlacementQueryServiceClient
	synonymNotificationClient searchv1.SynonymNotificationServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                       deps.Logger,
		placementValidationClient: merchandisingv1.NewPlacementValidationServiceClient(deps.Conn),
		collectionClient:          merchandisingv1.NewCollectionServiceClient(deps.Conn),
		searchIndexQueryClient:    searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
		placementQueryClient:      merchandisingv1.NewPlacementQueryServiceClient(deps.Conn),
		synonymNotificationClient: searchv1.NewSynonymNotificationServiceClient(deps.Conn),
	}
	go w.consume(ctx, "merchandising.delete_collection.check_placement_eligibility", deps.Bus.Subscribe("merchandising.delete_collection.check_placement_eligibility"), w.handleMerchandisingDeleteCollectionCheckPlacementEligibility)
	go w.consume(ctx, "merchandising.cancel_merchandising_rule_workflow.clone_collection", deps.Bus.Subscribe("merchandising.cancel_merchandising_rule_workflow.clone_collection"), w.handleMerchandisingCancelMerchandisingRuleWorkflowCloneCollection)
	go w.consume(ctx, "merchandising.submit_merchandising_rule.find_search_indexes", deps.Bus.Subscribe("merchandising.submit_merchandising_rule.find_search_indexes"), w.handleMerchandisingSubmitMerchandisingRuleFindSearchIndexes)
	go w.consume(ctx, "merchandising.comment_merchandising_rule_approval.create_collection", deps.Bus.Subscribe("merchandising.comment_merchandising_rule_approval.create_collection"), w.handleMerchandisingCommentMerchandisingRuleApprovalCreateCollection)
	go w.consume(ctx, "merchandising.retry_collection_workflow.count_placements", deps.Bus.Subscribe("merchandising.retry_collection_workflow.count_placements"), w.handleMerchandisingRetryCollectionWorkflowCountPlacements)
	go w.consume(ctx, "merchandising.approve_placement.peek_placement", deps.Bus.Subscribe("merchandising.approve_placement.peek_placement"), w.handleMerchandisingApprovePlacementPeekPlacement)
	go w.consume(ctx, "merchandising.normalize_merchandising_rule.explain_placement_decision", deps.Bus.Subscribe("merchandising.normalize_merchandising_rule.explain_placement_decision"), w.handleMerchandisingNormalizeMerchandisingRuleExplainPlacementDecision)
	go w.consume(ctx, "merchandising.check_placement_eligibility.subscribe_synonym_updates", deps.Bus.Subscribe("merchandising.check_placement_eligibility.subscribe_synonym_updates"), w.handleMerchandisingCheckPlacementEligibilitySubscribeSynonymUpdates)
}

func (w *workers) consume(ctx context.Context, topic string, events <-chan eventbus.Event, handle func(context.Context, eventbus.Event) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			if err := handle(ctx, event); err != nil {
				w.log.ErrorContext(ctx, "event failed", "topic", topic, "error", err)
			}
		}
	}
}

// handleMerchandisingDeleteCollectionCheckPlacementEligibility makes the call the
// merchandising.delete_collection.check_placement_eligibility event asks for.
func (w *workers) handleMerchandisingDeleteCollectionCheckPlacementEligibility(ctx context.Context, event eventbus.Event) error {
	_, err := w.placementValidationClient.CheckPlacementEligibility(ctx, &merchandisingv1.CheckPlacementEligibilityRequest{
		Reason: event.Attributes["reason"],
		Placement: &merchandisingv1.Placement{
			Audit: &commonv1.AuditInfo{Etag: event.Attributes["placement.audit.etag"]},
		},
		Details: &merchandisingv1.CheckPlacementEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: event.Attributes["details.contact.email"]},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleMerchandisingCancelMerchandisingRuleWorkflowCloneCollection makes the call the
// merchandising.cancel_merchandising_rule_workflow.clone_collection event asks for.
func (w *workers) handleMerchandisingCancelMerchandisingRuleWorkflowCloneCollection(ctx context.Context, event eventbus.Event) error {
	_, err := w.collectionClient.CloneCollection(ctx, &merchandisingv1.CloneCollectionRequest{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Location: &commonv1.PostalAddress{
						Locality:   event.Attributes["collection.children.children.location.locality"],
						PostalCode: event.Attributes["collection.children.children.location.postal_code"],
					},
				}},
			}},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleMerchandisingSubmitMerchandisingRuleFindSearchIndexes makes the call the
// merchandising.submit_merchandising_rule.find_search_indexes event asks for.
func (w *workers) handleMerchandisingSubmitMerchandisingRuleFindSearchIndexes(ctx context.Context, event eventbus.Event) error {
	_, err := w.searchIndexQueryClient.FindSearchIndexes(ctx, &searchv1.FindSearchIndexesRequest{
		Filter: &searchv1.FindSearchIndexesRequest_Filter{
			Region:  event.Attributes["filter.region"],
			OwnerId: event.Attributes["filter.owner_id"],
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleMerchandisingCommentMerchandisingRuleApprovalCreateCollection makes the call the
// merchandising.comment_merchandising_rule_approval.create_collection event asks for.
func (w *workers) handleMerchandisingCommentMerchandisingRuleApprovalCreateCollection(ctx context.Context, event eventbus.Event) error {
	_, err := w.collectionClient.CreateCollection(ctx, &merchandisingv1.CreateCollectionRequest{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Manager: &identityv1.UserRef{
						Email: event.Attributes["collection.children.children.manager.email"],
					},
				}},
			}},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleMerchandisingRetryCollectionWorkflowCountPlacements makes the call the
// merchandising.retry_collection_workflow.count_placements event asks for.
func (w *workers) handleMerchandisingRetryCollectionWorkflowCountPlacements(ctx context.Context, event eventbus.Event) error {
	_, err := w.placementQueryClient.CountPlacements(ctx, &merchandisingv1.CountPlacementsRequest{
		Filter: &merchandisingv1.CountPlacementsRequest_Filter{Region: event.Attributes["filter.region"]},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleMerchandisingApprovePlacementPeekPlacement makes the call the merchandising.approve_placement.peek_placement
// event asks for.
func (w *workers) handleMerchandisingApprovePlacementPeekPlacement(ctx context.Context, event eventbus.Event) error {
	_, err := w.placementQueryClient.PeekPlacement(ctx, &merchandisingv1.PeekPlacementRequest{
		PlacementId: event.Attributes["placement_id"],
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleMerchandisingNormalizeMerchandisingRuleExplainPlacementDecision makes the call the
// merchandising.normalize_merchandising_rule.explain_placement_decision event asks for.
func (w *workers) handleMerchandisingNormalizeMerchandisingRuleExplainPlacementDecision(ctx context.Context, event eventbus.Event) error {
	_, err := w.placementValidationClient.ExplainPlacementDecision(ctx, &merchandisingv1.ExplainPlacementDecisionRequest{
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					Email: event.Attributes["placement.participants.contact.email"],
				},
			}},
			Organizer: &identityv1.UserRef{
				DisplayName: event.Attributes["placement.organizer.display_name"],
			},
			Audit: &commonv1.AuditInfo{CreatedBy: event.Attributes["placement.audit.created_by"]},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleMerchandisingCheckPlacementEligibilitySubscribeSynonymUpdates makes the call the
// merchandising.check_placement_eligibility.subscribe_synonym_updates event asks for.
func (w *workers) handleMerchandisingCheckPlacementEligibilitySubscribeSynonymUpdates(ctx context.Context, event eventbus.Event) error {
	_, err := w.synonymNotificationClient.SubscribeSynonymUpdates(ctx, &searchv1.SubscribeSynonymUpdatesRequest{
		Details: &searchv1.SubscribeSynonymUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: event.Attributes["details.recipient.preferred_channel"],
			},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}
