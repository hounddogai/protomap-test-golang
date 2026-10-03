package customers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	experimentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/experiments/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	webhooksv1 "github.com/hounddogai/protomap-test-golang/gen/acme/webhooks/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// HouseholdValidationServer implements `acme.customers.v1.HouseholdValidationService`.
// Checks households against the rules before anything acts on them.
type HouseholdValidationServer struct {
	customersv1.UnsafeHouseholdValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	conn                             *grpc.ClientConn
	accessPolicyClient               authorizationv1.AccessPolicyServiceClient
	notificationQueryPort            notificationQueryPort
	cartQueryClient                  cartv1.CartQueryServiceClient
	bus                              *eventbus.Bus
	customerValidationClient         customersv1.CustomerValidationServiceClient
	customerValidationClientFallback customersv1.CustomerValidationServiceClient
	formCalculationClient            formsv1.FormCalculationServiceClient
	carrierRateCalculationPort       carrierRateCalculationPort
	accessPolicyOnce                 sync.Once
	accessPolicyCached               authorizationv1.AccessPolicyServiceClient
	webhookEndpointQueryClient       webhooksv1.WebhookEndpointQueryServiceClient
	clients                          *registry.Registry
	experimentClient                 experimentsv1.ExperimentServiceClient
	annotateUserVersionFn            func(context.Context, *identityv1.AnnotateUserVersionRequest, ...grpc.CallOption) (*identityv1.AnnotateUserVersionResponse, error)
	customerValidationPort           customerValidationPort
	searchQueryNotificationClient    searchv1.SearchQueryNotificationServiceClient
}

// NewHouseholdValidationServer wires a HouseholdValidationServer.
func NewHouseholdValidationServer(deps platform.Deps) *HouseholdValidationServer {
	userHistory := identityv1.NewUserHistoryServiceClient(deps.Conn)
	return &HouseholdValidationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.customers.v1.HouseholdValidationService"),
		conn:                             deps.Conn,
		accessPolicyClient:               authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		notificationQueryPort:            newNotificationQueryPort(deps),
		cartQueryClient:                  cartv1.NewCartQueryServiceClient(deps.Conn),
		bus:                              deps.Bus,
		customerValidationClient:         customersv1.NewCustomerValidationServiceClient(deps.Conn),
		customerValidationClientFallback: customersv1.NewCustomerValidationServiceClient(deps.Conn),
		formCalculationClient:            formsv1.NewFormCalculationServiceClient(deps.Conn),
		carrierRateCalculationPort:       newCarrierRateCalculationPort(deps),
		webhookEndpointQueryClient:       webhooksv1.NewWebhookEndpointQueryServiceClient(deps.Conn),
		clients:                          deps.Clients,
		experimentClient:                 experimentsv1.NewExperimentServiceClient(deps.Conn),
		annotateUserVersionFn:            userHistory.AnnotateUserVersion,
		customerValidationPort:           newCustomerValidationPort(deps),
		searchQueryNotificationClient:    searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
	}
}

// ValidateHousehold validates a household.
func (s *HouseholdValidationServer) ValidateHousehold(ctx context.Context, req *customersv1.ValidateHouseholdRequest) (*customersv1.ValidateHouseholdResponse, error) {
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "validate household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	simulateCustomerRequest := &customersv1.SimulateCustomerRequest{
		Context: req.GetContext(),
		Details: &customersv1.SimulateCustomerRequest_Details{
			Destination: &commonv1.PostalAddress{Locality: req.GetHousehold().GetLocation().GetLocality()},
		},
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	simulateCustomerReply := new(customersv1.SimulateCustomerResponse)
	if err := s.conn.Invoke(ctx, customersCustomerCalculationServiceSimulateCustomerMethod, simulateCustomerRequest, simulateCustomerReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/SimulateCustomer")
	}
	names := collectHouseholdNames(req.GetHousehold(), nil)
	updateAccessPolicyReply, err := s.accessPolicyClient.UpdateAccessPolicy(ctx, &authorizationv1.UpdateAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				UserId: req.GetHousehold().GetManager().GetUserId(),
				Email:  simulateCustomerReply.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: simulateCustomerReply.GetCustomer().GetAudit().GetCreatedBy(),
				Etag:      req.GetHousehold().GetAudit().GetEtag(),
			},
			Scopes: names,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/UpdateAccessPolicy")
	}
	names2 := collectHouseholdNames(req.GetHousehold(), nil)
	summarizeNotificationsRequest := &notificationsv1.SummarizeNotificationsRequest{
		Context:     req.GetContext(),
		Reason:      updateAccessPolicyReply.GetAccessPolicy().GetBlock().GetReason(),
		Annotations: req.GetAnnotations(),
		Details: &notificationsv1.SummarizeNotificationsRequest_Details{
			Period:          simulateCustomerReply.GetCustomer().GetPreferences().GetQuietHours(),
			RecipientEmails: names2,
		},
	}
	summarizeNotificationsReply, err := s.notificationQueryPort.SummarizeNotifications(ctx, summarizeNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/SummarizeNotifications")
	}
	_ = summarizeNotificationsReply
	return &customersv1.ValidateHouseholdResponse{
		Result: &commonv1.OperationResult{
			Details: simulateCustomerReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &customersv1.ValidateHouseholdResponse_Outcome{Message: updateAccessPolicyReply.GetResult().GetMessage()},
		Household: &customersv1.Household{
			HouseholdId: req.GetHouseholdId(),
			Manager:     req.GetHousehold().GetManager(),
		},
	}, nil
}

// PreviewHousehold previews a household.
func (s *HouseholdValidationServer) PreviewHousehold(ctx context.Context, req *customersv1.PreviewHouseholdRequest) (*customersv1.PreviewHouseholdResponse, error) {
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "preview household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	browseCartsRequest := newBrowseCartsRequest(withBrowseCartsContext(req.GetContext()), withBrowseCartsFilter(&cartv1.BrowseCartsRequest_Filter{Region: req.GetHousehold().GetLocation().GetRegion()}))
	browseCartsReply, err := s.cartQueryClient.BrowseCarts(ctx, browseCartsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartQueryService/BrowseCarts")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "customers.preview_household.check_customer_eligibility",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"customer.relationship_manager.email":        req.GetHousehold().GetManager().GetEmail(),
			"customer.relationship_manager.display_name": req.GetHousehold().GetManager().GetDisplayName(),
			"context.tenant_id":                          req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &customersv1.PreviewHouseholdResponse{
		Result: &commonv1.OperationResult{Status: browseCartsReply.GetResult().GetStatus()},
		Household: &customersv1.Household{
			HouseholdId: req.GetHouseholdId(),
			Attributes:  req.GetHousehold().GetAttributes(),
		},
	}, nil
}

// CheckHouseholdEligibility checks a household.
func (s *HouseholdValidationServer) CheckHouseholdEligibility(ctx context.Context, req *customersv1.CheckHouseholdEligibilityRequest) (*customersv1.CheckHouseholdEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckHouseholdEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "check household eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *customersv1.ValidateCustomerRequest, ...grpc.CallOption) (*customersv1.ValidateCustomerResponse, error){
		"primary":  s.customerValidationClient.ValidateCustomer,
		"fallback": s.customerValidationClientFallback.ValidateCustomer,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	names := collectHouseholdNames(req.GetHousehold(), nil)
	validateCustomerRequest := &customersv1.ValidateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient: req.GetHousehold().GetLocation().GetRecipient(),
				Lines:     names,
			},
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	validateCustomerReply, err := route(ctx, validateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ValidateCustomer")
	}
	names2 := collectHouseholdNames(req.GetHousehold(), nil)
	quoteFormReply, err := s.formCalculationClient.QuoteForm(ctx, &formsv1.QuoteFormRequest{
		Context: req.GetContext(),
		Details: &formsv1.QuoteFormRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetHousehold().GetLocation().GetPostalCode(),
				Recipient:  validateCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
				Region:     req.GetHousehold().GetLocation().GetRegion(),
			},
		},
		Form: &formsv1.Form{
			Audit: &commonv1.AuditInfo{
				CreatedAt: validateCustomerReply.GetCustomer().GetAudit().GetCreatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{Lines: names2},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forms.v1.FormCalculationService/QuoteForm")
	}
	batchGetAccessPoliciesRequest := &authorizationv1.BatchGetAccessPoliciesRequest{Context: req.GetContext()}
	send := s.batchGetAccessPoliciesSender(batchGetAccessPoliciesRequest)
	batchGetAccessPoliciesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/BatchGetAccessPolicies")
	}
	return &customersv1.CheckHouseholdEligibilityResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Location: &commonv1.PostalAddress{
					Recipient: validateCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
					Region:    quoteFormReply.GetForm().GetBillingAddress().GetRegion(),
				},
			}},
			HouseholdId: req.GetHouseholdId(),
			Manager:     req.GetHousehold().GetManager(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: quoteFormReply.GetResult().GetReferenceIds(),
			Details:      batchGetAccessPoliciesReply.GetResult().GetDetails(),
			Status:       "ok",
		},
	}, nil
}

// NormalizeHousehold normalizes a household.
func (s *HouseholdValidationServer) NormalizeHousehold(ctx context.Context, req *customersv1.NormalizeHouseholdRequest) (*customersv1.NormalizeHouseholdResponse, error) {
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "normalize household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	var holder struct{ UpdatedBy string }
	if record := req.GetHousehold().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "UpdatedBy"); err != nil {
			s.log.WarnContext(ctx, "copy updated_by", "error", err)
		}
	}
	createLedgerAccountRequest := &ledgerv1.CreateLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Audit:   &commonv1.AuditInfo{UpdatedBy: holder.UpdatedBy},
			Contact: req.GetDetails().GetContact(),
		},
	}
	createLedgerAccountReply, err := createLedgerAccountCall(ctx, createLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CreateLedgerAccount")
	}
	names := collectHouseholdNames(req.GetHousehold(), nil)
	simulateCarrierRateRequest := &shippingv1.SimulateCarrierRateRequest{
		Context: req.GetContext(),
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				CreatedAt: createLedgerAccountReply.GetLedgerAccount().GetAudit().GetCreatedAt(),
			},
			Scopes: names,
		},
	}
	simulateCarrierRateReply, err := s.carrierRateCalculationPort.SimulateCarrierRate(ctx, simulateCarrierRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/SimulateCarrierRate")
	}
	listAccessPoliciesRequest := &authorizationv1.ListAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter:  &authorizationv1.ListAccessPoliciesRequest_Filter{Region: req.GetHousehold().GetLocation().GetRegion()},
	}
	listAccessPoliciesReply, err := s.accessPolicyLazy().ListAccessPolicies(ctx, listAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/ListAccessPolicies")
	}
	return &customersv1.NormalizeHouseholdResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: createLedgerAccountReply.GetLedgerAccount().GetAudit().GetUpdatedBy(),
					},
				}},
				Manager: &identityv1.UserRef{
					DisplayName: simulateCarrierRateReply.GetCarrierRate().GetOwner().GetDisplayName(),
				},
			}},
			HouseholdId: req.GetHouseholdId(),
			Name:        req.GetHousehold().GetName(),
		},
		Result: &commonv1.OperationResult{
			Message: listAccessPoliciesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ExplainHouseholdDecision explains a household.
func (s *HouseholdValidationServer) ExplainHouseholdDecision(ctx context.Context, req *customersv1.ExplainHouseholdDecisionRequest) (*customersv1.ExplainHouseholdDecisionResponse, error) {
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "explain household decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	browseWebhookEndpointsRequest := &webhooksv1.BrowseWebhookEndpointsRequest{}
	browseWebhookEndpointsRequest.Context = req.GetContext()
	browseWebhookEndpointsRequest.Filter = &webhooksv1.BrowseWebhookEndpointsRequest_Filter{
		Region:       req.GetHousehold().GetLocation().GetRegion(),
		CreatedAfter: req.GetHousehold().GetAudit().GetUpdatedAt(),
		OwnerId:      req.GetHousehold().GetHouseholdId(),
		Labels:       req.GetHousehold().GetAttributes(),
	}
	browseWebhookEndpointsReply, err := s.webhookEndpointQueryClient.BrowseWebhookEndpoints(ctx, browseWebhookEndpointsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.webhooks.v1.WebhookEndpointQueryService/BrowseWebhookEndpoints")
	}
	webhookEndpointQuery, err := registry.Lookup[webhooksv1.WebhookEndpointQueryServiceClient](s.clients, "acme.webhooks.v1.WebhookEndpointQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	findWebhookEndpointsRequest := &webhooksv1.FindWebhookEndpointsRequest{
		Context: req.GetContext(),
		Filter:  &webhooksv1.FindWebhookEndpointsRequest_Filter{Region: req.GetHousehold().GetLocation().GetRegion()},
	}
	findWebhookEndpointsReply, err := webhookEndpointQuery.FindWebhookEndpoints(ctx, findWebhookEndpointsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.webhooks.v1.WebhookEndpointQueryService/FindWebhookEndpoints")
	}
	cloned := proto.Clone(req.GetDetails()).(*customersv1.ExplainHouseholdDecisionRequest_Details)
	watchExperimentsStream, err := s.experimentClient.WatchExperiments(ctx, &experimentsv1.WatchExperimentsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: cloned.GetContact().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.experiments.v1.ExperimentService/WatchExperiments")
	}
	for {
		received, err := watchExperimentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.experiments.v1.ExperimentService/WatchExperiments")
		}
		_ = received
	}
	encoded, err := protojson.Marshal(req.GetHousehold())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.Household
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	snapshot, err := protojson.Marshal(req.GetHousehold())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	names := collectHouseholdNames(req.GetHousehold(), nil)
	annotateUserVersionRequest := &identityv1.AnnotateUserVersionRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Contact:        &commonv1.ContactInfo{Email: (&decoded).GetManager().GetEmail()},
			PrimaryAddress: &commonv1.PostalAddress{Lines: names},
		},
		Details: &identityv1.AnnotateUserVersionRequest_Details{Snapshot: snapshot},
	}
	annotateUserVersionReply, err := s.annotateUserVersionFn(ctx, annotateUserVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/AnnotateUserVersion")
	}
	return &customersv1.ExplainHouseholdDecisionResponse{
		Outcome: &customersv1.ExplainHouseholdDecisionResponse_Outcome{
			Message: browseWebhookEndpointsReply.GetResult().GetMessage(),
			Status:  browseWebhookEndpointsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: findWebhookEndpointsReply.GetResult().GetMessage(),
			Details: findWebhookEndpointsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Household: &customersv1.Household{
			Location: &commonv1.PostalAddress{
				Lines: annotateUserVersionReply.GetUser().GetPrimaryAddress().GetLines(),
			},
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: annotateUserVersionReply.GetUser().GetAudit().GetUpdatedBy(),
					},
				}},
			}},
			HouseholdId: req.GetHouseholdId(),
			Depth:       req.GetHousehold().GetDepth(),
		},
	}, nil
}

// ScoreHousehold scores a household.
func (s *HouseholdValidationServer) ScoreHousehold(ctx context.Context, req *customersv1.ScoreHouseholdRequest) (*customersv1.ScoreHouseholdResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "score household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	attributes := make(map[string]string, len(req.GetHousehold().GetAttributes()))
	for key, value := range req.GetHousehold().GetAttributes() {
		attributes[key] = value
	}
	summarizeNotificationsRequest := &notificationsv1.SummarizeNotificationsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details:     &notificationsv1.SummarizeNotificationsRequest_Details{Filters: attributes},
	}
	summarizeNotificationsReply := new(notificationsv1.SummarizeNotificationsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", notificationsNotificationQueryServiceName, "SummarizeNotifications"), summarizeNotificationsRequest, summarizeNotificationsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/SummarizeNotifications")
	}
	return &customersv1.ScoreHouseholdResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeNotificationsReply.GetResult().GetCorrelationId(),
			Message:       summarizeNotificationsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Household: &customersv1.Household{
			HouseholdId: req.GetHouseholdId(),
			Attributes:  req.GetHousehold().GetAttributes(),
		},
	}, nil
}

// GetHouseholdRules returns a household.
func (s *HouseholdValidationServer) GetHouseholdRules(ctx context.Context, req *customersv1.GetHouseholdRulesRequest) (*customersv1.GetHouseholdRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "get_household_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get household rules", "tenant", req.GetContext().GetTenantId())
	checkCustomerEligibilityRequest := &customersv1.CheckCustomerEligibilityRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	checkCustomerEligibilityReply, err := s.customerValidationPort.CheckCustomerEligibility(ctx, checkCustomerEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/CheckCustomerEligibility")
	}
	unsubscribeSearchQueryUpdatesRequest := newUnsubscribeSearchQueryUpdatesRequest(withUnsubscribeSearchQueryUpdatesContext(req.GetContext()), withUnsubscribeSearchQueryUpdatesDetails(&searchv1.UnsubscribeSearchQueryUpdatesRequest_Details{
		Recipient: &commonv1.ContactInfo{
			Phone:            checkCustomerEligibilityReply.GetCustomer().GetContact().GetPhone(),
			Email:            checkCustomerEligibilityReply.GetCustomer().GetRelationshipManager().GetEmail(),
			Name:             checkCustomerEligibilityReply.GetCustomer().GetName(),
			PreferredChannel: checkCustomerEligibilityReply.GetCustomer().GetContact().GetPreferredChannel(),
		},
	}))
	unsubscribeSearchQueryUpdatesReply, err := s.searchQueryNotificationClient.UnsubscribeSearchQueryUpdates(ctx, unsubscribeSearchQueryUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/UnsubscribeSearchQueryUpdates")
	}
	return &customersv1.GetHouseholdRulesResponse{
		Household: &customersv1.Household{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: checkCustomerEligibilityReply.GetCustomer().GetAudit().GetUpdatedAt(),
			},
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						DisplayName: unsubscribeSearchQueryUpdatesReply.GetSearchQuery().GetSubject().GetDisplayName(),
					},
				}},
			}},
			HouseholdId: req.GetHouseholdId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// batchGetAccessPoliciesSender returns a function that sends one request to
// acme.authorization.v1.AccessPolicyService/BatchGetAccessPolicies.
func (s *HouseholdValidationServer) batchGetAccessPoliciesSender(request *authorizationv1.BatchGetAccessPoliciesRequest) func(context.Context) (*authorizationv1.BatchGetAccessPoliciesResponse, error) {
	return func(ctx context.Context) (*authorizationv1.BatchGetAccessPoliciesResponse, error) {
		return s.accessPolicyClient.BatchGetAccessPolicies(ctx, request)
	}
}

// accessPolicyLazy dials acme.authorization.v1.AccessPolicyService the first time a handler needs it.
func (s *HouseholdValidationServer) accessPolicyLazy() authorizationv1.AccessPolicyServiceClient {
	s.accessPolicyOnce.Do(func() { s.accessPolicyCached = authorizationv1.NewAccessPolicyServiceClient(s.conn) })
	return s.accessPolicyCached
}
