package partnerapi

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	driverappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/driverapp/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerWebhookQueryServer implements `acme.partnerapi.v1.PartnerWebhookQueryService`.
// Answers read-only questions about partner webhooks, for screens and reports.
type PartnerWebhookQueryServer struct {
	partnerapiv1.UnimplementedPartnerWebhookQueryServiceServer
	store                                *Store
	log                                  *slog.Logger
	synonymNotificationClient            searchv1.SynonymNotificationServiceClient
	driverHomeClient                     driverappv1.DriverHomeServiceClient
	bus                                  *eventbus.Bus
	sellerScorecardApprovalClient        sellersv1.SellerScorecardApprovalServiceClient
	submitSellerFn                       func(context.Context, *sellersv1.SubmitSellerRequest, ...grpc.CallOption) (*sellersv1.SubmitSellerResponse, error)
	partnerIntegrationIntegrationClient  partnerapiv1.PartnerIntegrationIntegrationServiceClient
	documentClient                       documentsv1.DocumentServiceClient
	partnerOrderWorkflowClient           partnerapiv1.PartnerOrderWorkflowServiceClient
	partnerIntegrationNotificationClient partnerapiv1.PartnerIntegrationNotificationServiceClient
}

// NewPartnerWebhookQueryServer wires a PartnerWebhookQueryServer.
func NewPartnerWebhookQueryServer(deps platform.Deps) *PartnerWebhookQueryServer {
	sellerApproval := sellersv1.NewSellerApprovalServiceClient(deps.Conn)
	return &PartnerWebhookQueryServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.partnerapi.v1.PartnerWebhookQueryService"),
		synonymNotificationClient:            searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		driverHomeClient:                     driverappv1.NewDriverHomeServiceClient(deps.Conn),
		bus:                                  deps.Bus,
		sellerScorecardApprovalClient:        sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		submitSellerFn:                       sellerApproval.SubmitSeller,
		partnerIntegrationIntegrationClient:  partnerapiv1.NewPartnerIntegrationIntegrationServiceClient(deps.Conn),
		documentClient:                       documentsv1.NewDocumentServiceClient(deps.Conn),
		partnerOrderWorkflowClient:           partnerapiv1.NewPartnerOrderWorkflowServiceClient(deps.Conn),
		partnerIntegrationNotificationClient: partnerapiv1.NewPartnerIntegrationNotificationServiceClient(deps.Conn),
	}
}

// LookupPartnerWebhook returns a partner webhook.
func (s *PartnerWebhookQueryServer) LookupPartnerWebhook(ctx context.Context, req *partnerapiv1.LookupPartnerWebhookRequest) (*partnerapiv1.LookupPartnerWebhookResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "lookup_partner_webhook"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup partner webhook", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeSynonymUpdatesReply, err := s.synonymNotificationClient.SubscribeSynonymUpdates(ctx, &searchv1.SubscribeSynonymUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.SubscribeSynonymUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: cloned.GetActor().GetEmail()},
			Variables: req.GetContext().GetBaggage(),
			Payload:   packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/SubscribeSynonymUpdates")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := partnerWebhookQueryServerUpdateDriverHomeSteps[mode]
	updateDriverHomeRequest := &driverappv1.UpdateDriverHomeRequest{
		Context: req.GetContext(),
		DriverHome: &driverappv1.DriverHome{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: subscribeSynonymUpdatesReply.GetSynonym().GetAudit().GetUpdatedBy(),
			},
			Origin: &driverappv1.DriverHome_Customer{Customer: &customersv1.CustomerRef{
				Email: subscribeSynonymUpdatesReply.GetSynonym().GetOwner().GetEmail(),
			}},
		},
	}
	updateDriverHomeReply, err := run(s, ctx, updateDriverHomeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.driverapp.v1.DriverHomeService/UpdateDriverHome")
	}
	return &partnerapiv1.LookupPartnerWebhookResponse{
		Result: &commonv1.OperationResult{
			Status:       subscribeSynonymUpdatesReply.GetOutcome().GetStatus(),
			Details:      subscribeSynonymUpdatesReply.GetResult().GetDetails(),
			ReferenceIds: updateDriverHomeReply.GetResult().GetReferenceIds(),
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{PartnerWebhookId: req.GetPartnerWebhookId()},
	}, nil
}

// BrowsePartnerWebhooks lists partner webhooks a page at a time.
func (s *PartnerWebhookQueryServer) BrowsePartnerWebhooks(ctx context.Context, req *partnerapiv1.BrowsePartnerWebhooksRequest) (*partnerapiv1.BrowsePartnerWebhooksResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BrowsePartnerWebhooks", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "browse partner webhooks", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "partnerapi.browse_partner_webhooks.list_household_reports",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.region":     req.GetFilter().GetRegion(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &partnerapiv1.BrowsePartnerWebhooksResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ResolvePartnerWebhooks returns several partner webhooks at once.
func (s *PartnerWebhookQueryServer) ResolvePartnerWebhooks(ctx context.Context, req *partnerapiv1.ResolvePartnerWebhooksRequest) (*partnerapiv1.ResolvePartnerWebhooksResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "resolve partner webhooks", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	commentSellerScorecardApprovalRequest := &sellersv1.CommentSellerScorecardApprovalRequest{}
	commentSellerScorecardApprovalRequest.Context = req.GetContext()
	commentSellerScorecardApprovalRequest.Details = &sellersv1.CommentSellerScorecardApprovalRequest_Details{
		Approver: &identityv1.UserRef{
			Email:  cloned.GetActor().GetEmail(),
			UserId: tenancy.Value(ctx, "user_id"),
		},
	}
	commentSellerScorecardApprovalRequest.SellerScorecard = &sellersv1.SellerScorecard{
		DueAt: req.GetContext().GetDeadline(),
		Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{Attributes: baggage}},
	}
	commentSellerScorecardApprovalReply, err := s.sellerScorecardApprovalClient.CommentSellerScorecardApproval(ctx, commentSellerScorecardApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/CommentSellerScorecardApproval")
	}
	return &partnerapiv1.ResolvePartnerWebhooksResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: commentSellerScorecardApprovalReply.GetResult().GetReferenceIds(),
			Status:       commentSellerScorecardApprovalReply.GetResult().GetStatus(),
		},
	}, nil
}

// ExportPartnerWebhooks streams every matching partner webhook.
func (s *PartnerWebhookQueryServer) ExportPartnerWebhooks(req *partnerapiv1.ExportPartnerWebhooksRequest, stream grpc.ServerStreamingServer[partnerapiv1.ExportPartnerWebhooksResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExportPartnerWebhooks", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "export partner webhooks", "tenant", req.GetContext().GetTenantId())
	submitSellerRequest := &sellersv1.SubmitSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			PrimaryAddress:      &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			RelationshipManager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	submitSellerReply, err := s.submitSellerFn(ctx, submitSellerRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/SubmitSeller")
	}
	countHouseholdTotalsRequest := &customersv1.CountHouseholdTotalsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.CountHouseholdTotalsRequest_Filter{
			Region: req.GetFilter().GetRegion(),
			Query:  req.GetFilter().GetQuery(),
			Labels: req.GetFilter().GetLabels(),
		},
	}
	countHouseholdTotalsReply, err := countHouseholdTotalsCall(ctx, countHouseholdTotalsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.HouseholdReportingService/CountHouseholdTotals")
	}
	disconnectPartnerIntegrationSourceRequest := &partnerapiv1.DisconnectPartnerIntegrationSourceRequest{}
	disconnectPartnerIntegrationSourceRequest.Context = req.GetContext()
	disconnectPartnerIntegrationSourceRequest.Details = &partnerapiv1.DisconnectPartnerIntegrationSourceRequest_Details{MessageId: submitSellerReply.GetOutcome().GetMessage()}
	disconnectPartnerIntegrationSourceReply, err := s.partnerIntegrationIntegrationClient.DisconnectPartnerIntegrationSource(ctx, disconnectPartnerIntegrationSourceRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationIntegrationService/DisconnectPartnerIntegrationSource")
	}
	if err := stream.Send(&partnerapiv1.ExportPartnerWebhooksResponse{
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Attributes:    submitSellerReply.GetSeller().GetAttributes(),
			CorrelationId: countHouseholdTotalsReply.GetResult().GetCorrelationId(),
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: disconnectPartnerIntegrationSourceReply.GetPartnerIntegration().GetHolder().GetDisplayName(),
			},
		},
		Result: &commonv1.OperationResult{
			CorrelationId: countHouseholdTotalsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// SuggestPartnerWebhooks lists partner webhooks a page at a time.
func (s *PartnerWebhookQueryServer) SuggestPartnerWebhooks(ctx context.Context, req *partnerapiv1.SuggestPartnerWebhooksRequest) (*partnerapiv1.SuggestPartnerWebhooksResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "suggest partner webhooks", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.SuggestPartnerWebhooksRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	watchDocumentsStream, err := s.documentClient.WatchDocuments(ctx, &documentsv1.WatchDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.WatchDocumentsRequest_Filter{
			OwnerId:      req.GetFilter().GetOwnerId(),
			CreatedAfter: (&decoded).GetCreatedAfter(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
	}
	for {
		received, err := watchDocumentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
		}
		_ = received
	}
	listPartnerOrderWorkflowsReply, err := s.partnerOrderWorkflowClient.ListPartnerOrderWorkflows(ctx, &partnerapiv1.ListPartnerOrderWorkflowsRequest{
		Context: req.GetContext(),
		Filter:  &partnerapiv1.ListPartnerOrderWorkflowsRequest_Filter{CreatedAfter: req.GetFilter().GetCreatedAfter()},
		Page: &commonv1.PageRequest{
			PageToken: req.GetPage().GetPageToken(),
			PageSize:  req.GetPage().GetPageSize(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/ListPartnerOrderWorkflows")
	}
	return &partnerapiv1.SuggestPartnerWebhooksResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listPartnerOrderWorkflowsReply.GetResult().GetReferenceIds(),
			Message:      listPartnerOrderWorkflowsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
	}, nil
}

// PeekPartnerWebhook returns a partner webhook.
func (s *PartnerWebhookQueryServer) PeekPartnerWebhook(ctx context.Context, req *partnerapiv1.PeekPartnerWebhookRequest) (*partnerapiv1.PeekPartnerWebhookResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PeekPartnerWebhook", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "peek_partner_webhook"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek partner webhook", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	subscribePartnerIntegrationUpdatesRequest := &partnerapiv1.SubscribePartnerIntegrationUpdatesRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.SubscribePartnerIntegrationUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: baggage,
		},
	}
	var subscribePartnerIntegrationUpdatesReply *partnerapiv1.SubscribePartnerIntegrationUpdatesResponse
	if err := pipeline.Run(ctx, "acme.partnerapi.v1.PartnerIntegrationNotificationService/SubscribePartnerIntegrationUpdates", func(ctx context.Context) error {
		answer, err := s.partnerIntegrationNotificationClient.SubscribePartnerIntegrationUpdates(ctx, subscribePartnerIntegrationUpdatesRequest)
		subscribePartnerIntegrationUpdatesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationNotificationService/SubscribePartnerIntegrationUpdates")
	}
	return &partnerapiv1.PeekPartnerWebhookResponse{
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: subscribePartnerIntegrationUpdatesReply.GetPartnerIntegration().GetHolder().GetDisplayName(),
			},
			PartnerWebhookId: req.GetPartnerWebhookId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// partnerWebhookQueryServerUpdateDriverHomeSteps are the ways to run
// acme.driverapp.v1.DriverHomeService/UpdateDriverHome, by name.
var partnerWebhookQueryServerUpdateDriverHomeSteps = map[string]func(*PartnerWebhookQueryServer, context.Context, *driverappv1.UpdateDriverHomeRequest) (*driverappv1.UpdateDriverHomeResponse, error){
	"default": (*PartnerWebhookQueryServer).updateDriverHomeStep,
	"audited": (*PartnerWebhookQueryServer).updateDriverHomeAuditedStep,
}

func (s *PartnerWebhookQueryServer) updateDriverHomeStep(ctx context.Context, request *driverappv1.UpdateDriverHomeRequest) (*driverappv1.UpdateDriverHomeResponse, error) {
	return s.driverHomeClient.UpdateDriverHome(ctx, request)
}

func (s *PartnerWebhookQueryServer) updateDriverHomeAuditedStep(ctx context.Context, request *driverappv1.UpdateDriverHomeRequest) (*driverappv1.UpdateDriverHomeResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.driverapp.v1.DriverHomeService/UpdateDriverHome")
	return s.driverHomeClient.UpdateDriverHome(ctx, request)
}
