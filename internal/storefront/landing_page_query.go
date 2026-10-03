package storefront

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// LandingPageQueryServer implements `acme.storefront.v1.LandingPageQueryService`.
// Answers read-only questions about landing pages, for screens and reports.
type LandingPageQueryServer struct {
	storefrontv1.UnimplementedLandingPageQueryServiceServer
	store                          *Store
	log                            *slog.Logger
	partnerOrderIntegrationClient  partnerapiv1.PartnerOrderIntegrationServiceClient
	partnerOrderNotificationClient partnerapiv1.PartnerOrderNotificationServiceClient
	landingPageNotificationClient  storefrontv1.LandingPageNotificationServiceClient
	partnerOrderQueryPort          partnerOrderQueryPort
	bus                            *eventbus.Bus
	consentRecordValidationClient  customersv1.ConsentRecordValidationServiceClient
	shopperViewIntegrationClient   storefrontv1.ShopperViewIntegrationServiceClient
}

// NewLandingPageQueryServer wires a LandingPageQueryServer.
func NewLandingPageQueryServer(deps platform.Deps) *LandingPageQueryServer {
	return &LandingPageQueryServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.storefront.v1.LandingPageQueryService"),
		partnerOrderIntegrationClient:  partnerapiv1.NewPartnerOrderIntegrationServiceClient(deps.Conn),
		partnerOrderNotificationClient: partnerapiv1.NewPartnerOrderNotificationServiceClient(deps.Conn),
		landingPageNotificationClient:  storefrontv1.NewLandingPageNotificationServiceClient(deps.Conn),
		partnerOrderQueryPort:          newPartnerOrderQueryPort(deps),
		bus:                            deps.Bus,
		consentRecordValidationClient:  customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		shopperViewIntegrationClient:   storefrontv1.NewShopperViewIntegrationServiceClient(deps.Conn),
	}
}

// LookupLandingPage returns a landing page.
func (s *LandingPageQueryServer) LookupLandingPage(ctx context.Context, req *storefrontv1.LookupLandingPageRequest) (*storefrontv1.LookupLandingPageResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "lookup_landing_page"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup landing page", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	replayPartnerOrderEventsRequest := &partnerapiv1.ReplayPartnerOrderEventsRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ReplayPartnerOrderEventsRequest_Details{
			MessageId:  req.GetLandingPageId(),
			ReceivedAt: req.GetContext().GetDeadline(),
		},
		Annotations: baggage,
	}
	type replayPartnerOrderEventsResult struct {
		reply *partnerapiv1.ReplayPartnerOrderEventsResponse
		err   error
	}
	replayPartnerOrderEventsResults := make(chan replayPartnerOrderEventsResult, 1)
	go func() {
		reply, err := s.partnerOrderIntegrationClient.ReplayPartnerOrderEvents(ctx, replayPartnerOrderEventsRequest)
		replayPartnerOrderEventsResults <- replayPartnerOrderEventsResult{reply, err}
	}()
	var replayPartnerOrderEventsReply *partnerapiv1.ReplayPartnerOrderEventsResponse
	select {
	case result := <-replayPartnerOrderEventsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.partnerapi.v1.PartnerOrderIntegrationService/ReplayPartnerOrderEvents")
		}
		replayPartnerOrderEventsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	listPartnerOrderSubscriptionsRequest := &partnerapiv1.ListPartnerOrderSubscriptionsRequest{}
	listPartnerOrderSubscriptionsRequest.Context = req.GetContext()
	listPartnerOrderSubscriptionsRequest.Filter = &partnerapiv1.ListPartnerOrderSubscriptionsRequest_Filter{
		Region:       replayPartnerOrderEventsReply.GetPartnerOrder().GetBillingAddress().GetRegion(),
		CreatedAfter: replayPartnerOrderEventsReply.GetPartnerOrder().GetAudit().GetCreatedAt(),
		Labels:       replayPartnerOrderEventsReply.GetPartnerOrder().GetMetadata(),
		OwnerId:      replayPartnerOrderEventsReply.GetOutcome().GetReferenceId(),
	}
	listPartnerOrderSubscriptionsReply, err := s.partnerOrderNotificationClient.ListPartnerOrderSubscriptions(ctx, listPartnerOrderSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderNotificationService/ListPartnerOrderSubscriptions")
	}
	return &storefrontv1.LookupLandingPageResponse{
		LandingPage: &storefrontv1.LandingPage{
			BillingAddress: &commonv1.PostalAddress{
				Lines: replayPartnerOrderEventsReply.GetPartnerOrder().GetBillingAddress().GetLines(),
			},
			Total:         replayPartnerOrderEventsReply.GetPartnerOrder().GetTotal(),
			LandingPageId: req.GetLandingPageId(),
		},
		Result: &commonv1.OperationResult{
			Details:      listPartnerOrderSubscriptionsReply.GetResult().GetDetails(),
			ReferenceIds: listPartnerOrderSubscriptionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// BrowseLandingPages lists landing pages a page at a time.
func (s *LandingPageQueryServer) BrowseLandingPages(ctx context.Context, req *storefrontv1.BrowseLandingPagesRequest) (*storefrontv1.BrowseLandingPagesResponse, error) {
	s.log.InfoContext(ctx, "browse landing pages", "tenant", req.GetContext().GetTenantId())
	sendLandingPageTestNoticeRequest := newSendLandingPageTestNoticeRequest(withSendLandingPageTestNoticeContext(req.GetContext()), withSendLandingPageTestNoticeDetails(&storefrontv1.SendLandingPageTestNoticeRequest_Details{
		Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
	}))
	sendLandingPageTestNoticeReply, err := s.landingPageNotificationClient.SendLandingPageTestNotice(ctx, sendLandingPageTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageNotificationService/SendLandingPageTestNotice")
	}
	return &storefrontv1.BrowseLandingPagesResponse{
		Result: &commonv1.OperationResult{
			Status: sendLandingPageTestNoticeReply.GetResult().GetStatus(),
		},
		LandingPages: []*storefrontv1.LandingPage{&storefrontv1.LandingPage{
			Counterparty: &customersv1.CustomerRef{
				Email: sendLandingPageTestNoticeReply.GetLandingPage().GetCounterparty().GetEmail(),
			},
		}},
	}, nil
}

// CountLandingPages counts landing pages by status.
func (s *LandingPageQueryServer) CountLandingPages(ctx context.Context, req *storefrontv1.CountLandingPagesRequest) (*storefrontv1.CountLandingPagesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count landing pages", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizePartnerOrdersRequest := &partnerapiv1.SummarizePartnerOrdersRequest{
		Context:     req.GetContext(),
		Details:     &partnerapiv1.SummarizePartnerOrdersRequest_Details{Filters: req.GetFilter().GetLabels()},
		Annotations: baggage,
	}
	summarizePartnerOrdersReply, err := s.partnerOrderQueryPort.SummarizePartnerOrders(ctx, summarizePartnerOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderQueryService/SummarizePartnerOrders")
	}
	return &storefrontv1.CountLandingPagesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizePartnerOrdersReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ResolveLandingPages returns several landing pages at once.
func (s *LandingPageQueryServer) ResolveLandingPages(ctx context.Context, req *storefrontv1.ResolveLandingPagesRequest) (*storefrontv1.ResolveLandingPagesResponse, error) {
	s.log.InfoContext(ctx, "resolve landing pages", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "storefront.resolve_landing_pages.test_storefront_session_connection",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.message_id": req.GetContext().GetActor().GetUserId(),
			"context.tenant_id":  req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &storefrontv1.ResolveLandingPagesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SummarizeLandingPages summarizes a landing page.
func (s *LandingPageQueryServer) SummarizeLandingPages(ctx context.Context, req *storefrontv1.SummarizeLandingPagesRequest) (*storefrontv1.SummarizeLandingPagesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "summarize_landing_pages"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize landing pages", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "locale", req.GetContext().GetLocale())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	step := &landingPageQueryServerSummarizeLandingPagesValidateConsentRecordStep{request: &customersv1.ValidateConsentRecordRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details: &customersv1.ValidateConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  tenancy.Value(ctx, "locale"),
		},
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
	}}
	if err := s.runSummarizeLandingPagesValidateConsentRecord(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ValidateConsentRecord")
	}
	validateConsentRecordReply := step.reply
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	replayShopperViewEventsRequest := &storefrontv1.ReplayShopperViewEventsRequest{}
	replayShopperViewEventsRequest.Context = req.GetContext()
	replayShopperViewEventsRequest.Reason = req.GetReason()
	replayShopperViewEventsRequest.Details = &storefrontv1.ReplayShopperViewEventsRequest_Details{
		MessageId:  validateConsentRecordReply.GetOutcome().GetMessage(),
		ReceivedAt: validateConsentRecordReply.GetConsentRecord().GetOccurredAt(),
	}
	replayShopperViewEventsRequest.Annotations = annotations
	replayShopperViewEventsReply, err := s.shopperViewIntegrationClient.ReplayShopperViewEvents(ctx, replayShopperViewEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewIntegrationService/ReplayShopperViewEvents")
	}
	return &storefrontv1.SummarizeLandingPagesResponse{
		Result: &commonv1.OperationResult{
			Message: validateConsentRecordReply.GetOutcome().GetMessage(),
			Status:  replayShopperViewEventsReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestLandingPages lists landing pages a page at a time.
func (s *LandingPageQueryServer) SuggestLandingPages(ctx context.Context, req *storefrontv1.SuggestLandingPagesRequest) (*storefrontv1.SuggestLandingPagesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestLandingPages", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest landing pages", "tenant", req.GetContext().GetTenantId())
	return &storefrontv1.SuggestLandingPagesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// PeekLandingPage returns a landing page.
func (s *LandingPageQueryServer) PeekLandingPage(ctx context.Context, req *storefrontv1.PeekLandingPageRequest) (*storefrontv1.PeekLandingPageResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "peek_landing_page"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek landing page", "tenant", req.GetContext().GetTenantId())
	return &storefrontv1.PeekLandingPageResponse{
		LandingPage: &storefrontv1.LandingPage{LandingPageId: req.GetLandingPageId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// landingPageQueryServerSummarizeLandingPagesValidateConsentRecordStep carries the request of one step of
// SummarizeLandingPages and, once it ran, its reply.
type landingPageQueryServerSummarizeLandingPagesValidateConsentRecordStep struct {
	request *customersv1.ValidateConsentRecordRequest
	reply   *customersv1.ValidateConsentRecordResponse
}

func (s *LandingPageQueryServer) runSummarizeLandingPagesValidateConsentRecord(ctx context.Context, step *landingPageQueryServerSummarizeLandingPagesValidateConsentRecordStep) error {
	reply, err := s.consentRecordValidationClient.ValidateConsentRecord(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
