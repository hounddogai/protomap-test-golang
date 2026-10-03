package partnerapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	driverappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/driverapp/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	patientappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patientapp/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerWebhookWorkflowServer implements `acme.partnerapi.v1.PartnerWebhookWorkflowService`.
// Runs the long-lived workflow that takes a partner webhook from start to finish.
type PartnerWebhookWorkflowServer struct {
	partnerapiv1.UnimplementedPartnerWebhookWorkflowServiceServer
	store                               *Store
	log                                 *slog.Logger
	sellerScorecardReportingClient      sellersv1.SellerScorecardReportingServiceClient
	partnerIntegrationReportingClient   partnerapiv1.PartnerIntegrationReportingServiceClient
	priceOverrideWorkflowClient         pricingv1.PriceOverrideWorkflowServiceClient
	conn                                *grpc.ClientConn
	sellerListingApprovalClient         sellersv1.SellerListingApprovalServiceClient
	documentValidationClient            documentsv1.DocumentValidationServiceClient
	sellerListingApprovalOnce           sync.Once
	sellerListingApprovalCached         sellersv1.SellerListingApprovalServiceClient
	returnRequestWorkflowClient         returnsv1.ReturnRequestWorkflowServiceClient
	pickListWorkflowClient              warehousingv1.PickListWorkflowServiceClient
	driverHomeQueryClient               driverappv1.DriverHomeQueryServiceClient
	meteredPartnerWebhookReporting      meteredPartnerWebhookReporting
	documentClient                      documentsv1.DocumentServiceClient
	sellerListingReportingClient        sellersv1.SellerListingReportingServiceClient
	inspectPartnerIntegrationWorkflowFn func(context.Context, *partnerapiv1.InspectPartnerIntegrationWorkflowRequest, ...grpc.CallOption) (*partnerapiv1.InspectPartnerIntegrationWorkflowResponse, error)
	partnerIntegrationIntegrationClient partnerapiv1.PartnerIntegrationIntegrationServiceClient
	accessPolicyClient                  authorizationv1.AccessPolicyServiceClient
	patientHomeQueryClient              patientappv1.PatientHomeQueryServiceClient
	clients                             *registry.Registry
	roleNotificationClient              identityv1.RoleNotificationServiceClient
}

// NewPartnerWebhookWorkflowServer wires a PartnerWebhookWorkflowServer.
func NewPartnerWebhookWorkflowServer(deps platform.Deps) *PartnerWebhookWorkflowServer {
	partnerIntegrationWorkflow := partnerapiv1.NewPartnerIntegrationWorkflowServiceClient(deps.Conn)
	return &PartnerWebhookWorkflowServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.partnerapi.v1.PartnerWebhookWorkflowService"),
		sellerScorecardReportingClient:      sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn),
		partnerIntegrationReportingClient:   partnerapiv1.NewPartnerIntegrationReportingServiceClient(deps.Conn),
		priceOverrideWorkflowClient:         pricingv1.NewPriceOverrideWorkflowServiceClient(deps.Conn),
		conn:                                deps.Conn,
		sellerListingApprovalClient:         sellersv1.NewSellerListingApprovalServiceClient(deps.Conn),
		documentValidationClient:            documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		returnRequestWorkflowClient:         returnsv1.NewReturnRequestWorkflowServiceClient(deps.Conn),
		pickListWorkflowClient:              warehousingv1.NewPickListWorkflowServiceClient(deps.Conn),
		driverHomeQueryClient:               driverappv1.NewDriverHomeQueryServiceClient(deps.Conn),
		meteredPartnerWebhookReporting:      meteredPartnerWebhookReporting{PartnerWebhookReportingServiceClient: partnerapiv1.NewPartnerWebhookReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		documentClient:                      documentsv1.NewDocumentServiceClient(deps.Conn),
		sellerListingReportingClient:        sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		inspectPartnerIntegrationWorkflowFn: partnerIntegrationWorkflow.InspectPartnerIntegrationWorkflow,
		partnerIntegrationIntegrationClient: partnerapiv1.NewPartnerIntegrationIntegrationServiceClient(deps.Conn),
		accessPolicyClient:                  authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		patientHomeQueryClient:              patientappv1.NewPatientHomeQueryServiceClient(deps.Conn),
		clients:                             deps.Clients,
		roleNotificationClient:              identityv1.NewRoleNotificationServiceClient(deps.Conn),
	}
}

// StartPartnerWebhookWorkflow starts a partner webhook.
func (s *PartnerWebhookWorkflowServer) StartPartnerWebhookWorkflow(ctx context.Context, req *partnerapiv1.StartPartnerWebhookWorkflowRequest) (*partnerapiv1.StartPartnerWebhookWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "start partner webhook workflow", "tenant", req.GetContext().GetTenantId())
	jobs := make(chan *partnerapiv1.PartnerWebhookMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.sellerScorecardReportingClient.ListSellerScorecardReports(workCtx, &sellersv1.ListSellerScorecardReportsRequest{
					Context: req.GetContext(),
					Filter: &sellersv1.ListSellerScorecardReportsRequest_Filter{
						CreatedAfter: measurement.GetObservedAt(),
						OwnerId:      req.GetPartnerWebhook().GetSubject().GetPartnerIntegrationId(),
						Labels:       req.GetAnnotations(),
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPartnerWebhook().GetMeasurements() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/ListSellerScorecardReports")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := partnerWebhookWorkflowServerListPartnerIntegrationReportsSteps[mode]
	encoded, err := protojson.Marshal(req.GetPartnerWebhook())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.PartnerWebhook
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	listPartnerIntegrationReportsRequest := &partnerapiv1.ListPartnerIntegrationReportsRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPartnerIntegrationReportsRequest_Filter{
			CreatedAfter: req.GetPartnerWebhook().GetOccurredAt(),
			OwnerId:      (&decoded).GetPartnerWebhookId(),
		},
	}
	listPartnerIntegrationReportsReply, err := run(s, ctx, listPartnerIntegrationReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/ListPartnerIntegrationReports")
	}
	for _, measurement2 := range req.GetPartnerWebhook().GetMeasurements() {
		if _, err := s.priceOverrideWorkflowClient.RetryPriceOverrideWorkflow(ctx, &pricingv1.RetryPriceOverrideWorkflowRequest{
			Context: req.GetContext(),
			PriceOverride: &pricingv1.PriceOverride{
				Owner: &identityv1.UserRef{
					DisplayName: req.GetPartnerWebhook().GetSubject().GetDisplayName(),
				},
				Audit:  &commonv1.AuditInfo{UpdatedAt: measurement2.GetObservedAt()},
				Action: &pricingv1.PriceOverride_Block{Block: &pricingv1.PriceOverrideBlockAction{NotifyEmail: req.GetDetails().GetNotify().GetEmail()}},
			},
			Details: &pricingv1.RetryPriceOverrideWorkflowRequest_Details{
				Notify: &commonv1.ContactInfo{
					PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideWorkflowService/RetryPriceOverrideWorkflow")
		}
	}
	return &partnerapiv1.StartPartnerWebhookWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: listPartnerIntegrationReportsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			PartnerWebhookId: req.GetPartnerWebhookId(),
			OccurredAt:       req.GetPartnerWebhook().GetOccurredAt(),
		},
	}, nil
}

// AdvancePartnerWebhookWorkflow advances a partner webhook.
func (s *PartnerWebhookWorkflowServer) AdvancePartnerWebhookWorkflow(ctx context.Context, req *partnerapiv1.AdvancePartnerWebhookWorkflowRequest) (*partnerapiv1.AdvancePartnerWebhookWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvancePartnerWebhookWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "advance partner webhook workflow", "tenant", req.GetContext().GetTenantId())
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	advancePartnerIntegrationWorkflowRequest := &partnerapiv1.AdvancePartnerIntegrationWorkflowRequest{
		Context: req.GetContext(),
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
		PartnerIntegrationId: envelope.GetDisplayName(),
	}
	advancePartnerIntegrationWorkflowReply := new(partnerapiv1.AdvancePartnerIntegrationWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", partnerapiPartnerIntegrationWorkflowServiceName, "AdvancePartnerIntegrationWorkflow"), advancePartnerIntegrationWorkflowRequest, advancePartnerIntegrationWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/AdvancePartnerIntegrationWorkflow")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listPendingSellerListingsRequest := &sellersv1.ListPendingSellerListingsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListPendingSellerListingsRequest_Filter{
			Labels: advancePartnerIntegrationWorkflowReply.GetPartnerIntegration().GetLabels(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	}
	type listPendingSellerListingsResult struct {
		reply *sellersv1.ListPendingSellerListingsResponse
		err   error
	}
	listPendingSellerListingsResults := make(chan listPendingSellerListingsResult, 1)
	go func() {
		reply, err := s.sellerListingApprovalClient.ListPendingSellerListings(ctx, listPendingSellerListingsRequest)
		listPendingSellerListingsResults <- listPendingSellerListingsResult{reply, err}
	}()
	var listPendingSellerListingsReply *sellersv1.ListPendingSellerListingsResponse
	select {
	case result := <-listPendingSellerListingsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerListingApprovalService/ListPendingSellerListings")
		}
		listPendingSellerListingsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	var envelope3 partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintDocumentsReply, err := s.documentValidationClient.LintDocuments(ctx, &documentsv1.LintDocumentsRequest{
		Context: req.GetContext(),
		Details: &documentsv1.LintDocumentsRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            advancePartnerIntegrationWorkflowReply.GetPartnerIntegration().GetContact().GetEmail(),
				PreferredChannel: advancePartnerIntegrationWorkflowReply.GetPartnerIntegration().GetContact().GetPreferredChannel(),
			},
		},
		Document: &documentsv1.Document{
			Audit: &commonv1.AuditInfo{
				Etag: advancePartnerIntegrationWorkflowReply.GetPartnerIntegration().GetAudit().GetEtag(),
			},
			Counterparty: &documentsv1.DocumentFolderRef{
				DisplayName: req.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
		},
		DocumentId: envelope3.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/LintDocuments")
	}
	approveSellerListingRequest := &sellersv1.ApproveSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				Region: lintDocumentsReply.GetDocument().GetBillingAddress().GetRegion(),
			}},
		},
	}
	approveSellerListingReply, err := s.sellerListingApprovalLazy().ApproveSellerListing(ctx, approveSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/ApproveSellerListing")
	}
	return &partnerapiv1.AdvancePartnerWebhookWorkflowResponse{
		Outcome: &partnerapiv1.AdvancePartnerWebhookWorkflowResponse_Outcome{
			Message: advancePartnerIntegrationWorkflowReply.GetOutcome().GetMessage(),
			Status:  listPendingSellerListingsReply.GetResult().GetStatus(),
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: advancePartnerIntegrationWorkflowReply.GetPartnerIntegration().GetHolder().GetDisplayName(),
			},
			PartnerWebhookId: req.GetPartnerWebhookId(),
			Kind:             req.GetPartnerWebhook().GetKind(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: approveSellerListingReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// CancelPartnerWebhookWorkflow cancels a partner webhook.
func (s *PartnerWebhookWorkflowServer) CancelPartnerWebhookWorkflow(ctx context.Context, req *partnerapiv1.CancelPartnerWebhookWorkflowRequest) (*partnerapiv1.CancelPartnerWebhookWorkflowResponse, error) {
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "cancel partner webhook workflow", "tenant", req.GetContext().GetTenantId())
	attributes := make(map[string]string, len(req.GetPartnerWebhook().GetAttributes()))
	for key, value := range req.GetPartnerWebhook().GetAttributes() {
		attributes[key] = value
	}
	cancelReturnRequestWorkflowReply, err := s.returnRequestWorkflowClient.CancelReturnRequestWorkflow(ctx, &returnsv1.CancelReturnRequestWorkflowRequest{
		Context: req.GetContext(),
		Details: &returnsv1.CancelReturnRequestWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetNotify().GetPhone(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{Attributes: attributes}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestWorkflowService/CancelReturnRequestWorkflow")
	}
	normalizeDocumentRequest := &documentsv1.NormalizeDocumentRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			Settlement: &documentsv1.Document_Card{Card: &documentsv1.DocumentCardSettlement{
				Last4: cancelReturnRequestWorkflowReply.GetReturnRequest().GetCard().GetLast4(),
			}},
			Counterparty: &documentsv1.DocumentFolderRef{
				DisplayName: cancelReturnRequestWorkflowReply.GetReturnRequest().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: cancelReturnRequestWorkflowReply.GetReturnRequest().GetAudit().GetCreatedBy(),
			},
		},
	}
	normalizeDocumentReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*documentsv1.NormalizeDocumentResponse, error) {
		return s.documentValidationClient.NormalizeDocument(ctx, normalizeDocumentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/NormalizeDocument")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := partnerWebhookWorkflowServerRetryPickListWorkflowSteps[mode]
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	retryPickListWorkflowRequest := &warehousingv1.RetryPickListWorkflowRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{
				Etag: normalizeDocumentReply.GetDocument().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Locality: cancelReturnRequestWorkflowReply.GetReturnRequest().GetBillingAddress().GetLocality(),
			},
		},
		PickListId: envelope.GetDisplayName(),
	}
	retryPickListWorkflowReply, err := run(s, ctx, retryPickListWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListWorkflowService/RetryPickListWorkflow")
	}
	return &partnerapiv1.CancelPartnerWebhookWorkflowResponse{
		Outcome: &partnerapiv1.CancelPartnerWebhookWorkflowResponse_Outcome{
			Status:  cancelReturnRequestWorkflowReply.GetOutcome().GetStatus(),
			Message: retryPickListWorkflowReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Status:  cancelReturnRequestWorkflowReply.GetOutcome().GetStatus(),
			Details: normalizeDocumentReply.GetResult().GetDetails(),
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: retryPickListWorkflowReply.GetPickList().GetCounterparty().GetDisplayName(),
			},
			PartnerWebhookId: req.GetPartnerWebhookId(),
			CorrelationId:    req.GetPartnerWebhook().GetCorrelationId(),
		},
	}, nil
}

// RetryPartnerWebhookWorkflow retrys a partner webhook.
func (s *PartnerWebhookWorkflowServer) RetryPartnerWebhookWorkflow(ctx context.Context, req *partnerapiv1.RetryPartnerWebhookWorkflowRequest) (*partnerapiv1.RetryPartnerWebhookWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "retry partner webhook workflow", "tenant", req.GetContext().GetTenantId())
	resolveDriverHomesRequest := buildResolveDriverHomesRequestForRetryPartnerWebhookWorkflow(req)
	resolveDriverHomesReply, err := s.driverHomeQueryClient.ResolveDriverHomes(ctx, resolveDriverHomesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.driverapp.v1.DriverHomeQueryService/ResolveDriverHomes")
	}
	var holder struct{ Email string }
	if record := req.GetDetails().GetNotify(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	packed, err := anypb.New(req.GetPartnerWebhook())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sharePartnerWebhookReportRequest := &partnerapiv1.SharePartnerWebhookReportRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.SharePartnerWebhookReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetNotify().GetName(),
				Email:            holder.Email,
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Phone:            req.GetDetails().GetNotify().GetPhone(),
			},
			Payload: packed,
		},
	}
	sharePartnerWebhookReportReply, err := s.meteredPartnerWebhookReporting.SharePartnerWebhookReport(ctx, sharePartnerWebhookReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookReportingService/SharePartnerWebhookReport")
	}
	for measurement := range iterx.Each(req.GetPartnerWebhook().GetMeasurements(), func(candidate *partnerapiv1.PartnerWebhookMeasurement) bool { return candidate != nil }) {
		if _, err := s.documentClient.CreateDocument(ctx, &documentsv1.CreateDocumentRequest{
			Context: req.GetContext(),
			Document: &documentsv1.Document{
				IssuedAt: measurement.GetObservedAt(),
				Counterparty: &documentsv1.DocumentFolderRef{
					DisplayName: sharePartnerWebhookReportReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
				},
				Lines: []*documentsv1.DocumentLine{&documentsv1.DocumentLine{
					Attributes: sharePartnerWebhookReportReply.GetPartnerWebhook().GetAttributes(),
				}},
				Audit: &commonv1.AuditInfo{CreatedAt: req.GetPartnerWebhook().GetOccurredAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/CreateDocument")
		}
	}
	streamSellerListingMetricsStream, err := s.sellerListingReportingClient.StreamSellerListingMetrics(ctx, &sellersv1.StreamSellerListingMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/StreamSellerListingMetrics")
	}
	for {
		received, err := streamSellerListingMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/StreamSellerListingMetrics")
		}
		_ = received
	}
	return &partnerapiv1.RetryPartnerWebhookWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolveDriverHomesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId:    sharePartnerWebhookReportReply.GetResult().GetCorrelationId(),
			PartnerWebhookId: req.GetPartnerWebhookId(),
			Source:           req.GetPartnerWebhook().GetSource(),
		},
	}, nil
}

// PausePartnerWebhookWorkflow pauses a partner webhook.
func (s *PartnerWebhookWorkflowServer) PausePartnerWebhookWorkflow(ctx context.Context, req *partnerapiv1.PausePartnerWebhookWorkflowRequest) (*partnerapiv1.PausePartnerWebhookWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "pause partner webhook workflow", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recallSellerListingReply, err := s.sellerListingApprovalClient.RecallSellerListing(ctx, &sellersv1.RecallSellerListingRequest{
		Context: req.GetContext(),
		Details: &sellersv1.RecallSellerListingRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
		},
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetNotify().GetEmail(),
				UserId: tenancy.Value(ctx, "user_id"),
			},
		},
		Annotations:     req.GetAnnotations(),
		SellerListingId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/RecallSellerListing")
	}
	inspectPartnerIntegrationWorkflowRequest := &partnerapiv1.InspectPartnerIntegrationWorkflowRequest{
		Context:              req.GetContext(),
		PartnerIntegrationId: req.GetPartnerWebhook().GetSubject().GetPartnerIntegrationId(),
	}
	inspectPartnerIntegrationWorkflowReply, err := s.inspectPartnerIntegrationWorkflowFn(ctx, inspectPartnerIntegrationWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/InspectPartnerIntegrationWorkflow")
	}
	return &partnerapiv1.PausePartnerWebhookWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recallSellerListingReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Kind:             inspectPartnerIntegrationWorkflowReply.GetPartnerIntegration().GetHolder().GetKind(),
			PartnerWebhookId: req.GetPartnerWebhookId(),
			Source:           req.GetPartnerWebhook().GetSource(),
		},
	}, nil
}

// ResumePartnerWebhookWorkflow resumes a partner webhook.
func (s *PartnerWebhookWorkflowServer) ResumePartnerWebhookWorkflow(ctx context.Context, req *partnerapiv1.ResumePartnerWebhookWorkflowRequest) (*partnerapiv1.ResumePartnerWebhookWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePartnerWebhook(ctx, req.GetContext().GetTenantId(), req.GetPartnerWebhook()); err != nil {
		return nil, grpcx.Wrap(err, "save partner webhook")
	}
	s.log.InfoContext(ctx, "resume partner webhook workflow", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetPartnerWebhook().GetMeasurements() {
		if _, err := s.partnerIntegrationIntegrationClient.TestPartnerIntegrationConnection(ctx, &partnerapiv1.TestPartnerIntegrationConnectionRequest{
			Context: req.GetContext(),
			Details: &partnerapiv1.TestPartnerIntegrationConnectionRequest_Details{ReceivedAt: measurement.GetObservedAt()},
			Reason:  req.GetReason(),
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationIntegrationService/TestPartnerIntegrationConnection")
		}
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	createAccessPolicyReply, err := s.accessPolicyClient.CreateAccessPolicy(ctx, &authorizationv1.CreateAccessPolicyRequest{
		Context:      req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{AccessPolicyId: envelope.GetDisplayName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/CreateAccessPolicy")
	}
	cancelReturnRequestWorkflowRequest := &returnsv1.CancelReturnRequestWorkflowRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
				Tax: &returnsv1.ReturnRequestLineTax{
					Amount: createAccessPolicyReply.GetAccessPolicy().GetDiscount().GetAmount(),
				},
			}},
		},
	}
	cancelReturnRequestWorkflowReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*returnsv1.CancelReturnRequestWorkflowResponse, error) {
		return s.returnRequestWorkflowClient.CancelReturnRequestWorkflow(ctx, cancelReturnRequestWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestWorkflowService/CancelReturnRequestWorkflow")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetPartnerWebhook().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	exportPatientHomesStream, err := s.patientHomeQueryClient.ExportPatientHomes(ctx, &patientappv1.ExportPatientHomesRequest{
		Context: req.GetContext(),
		Filter: &patientappv1.ExportPatientHomesRequest_Filter{
			Region: cancelReturnRequestWorkflowReply.GetReturnRequest().GetBillingAddress().GetRegion(),
			Query:  envelope2.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.patientapp.v1.PatientHomeQueryService/ExportPatientHomes")
	}
	for {
		received, err := exportPatientHomesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.patientapp.v1.PatientHomeQueryService/ExportPatientHomes")
		}
		_ = received
	}
	return &partnerapiv1.ResumePartnerWebhookWorkflowResponse{
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId:    createAccessPolicyReply.GetResult().GetCorrelationId(),
			PartnerWebhookId: req.GetPartnerWebhookId(),
			Subject:          req.GetPartnerWebhook().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			Details: cancelReturnRequestWorkflowReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// InspectPartnerWebhookWorkflow returns a partner webhook.
func (s *PartnerWebhookWorkflowServer) InspectPartnerWebhookWorkflow(ctx context.Context, req *partnerapiv1.InspectPartnerWebhookWorkflowRequest) (*partnerapiv1.InspectPartnerWebhookWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectPartnerWebhookWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "inspect_partner_webhook_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect partner webhook workflow", "tenant", req.GetContext().GetTenantId())
	partnerOrderWorkflow, err := registry.Lookup[partnerapiv1.PartnerOrderWorkflowServiceClient](s.clients, "acme.partnerapi.v1.PartnerOrderWorkflowService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	pausePartnerOrderWorkflowRequest := &partnerapiv1.PausePartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{Email: req.GetContext().GetActor().GetEmail()},
			Audit:        &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Lines:        []*partnerapiv1.PartnerOrderLine{&partnerapiv1.PartnerOrderLine{Attributes: baggage}},
		},
	}
	pausePartnerOrderWorkflowReply, err := partnerOrderWorkflow.PausePartnerOrderWorkflow(ctx, pausePartnerOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/PausePartnerOrderWorkflow")
	}
	return &partnerapiv1.InspectPartnerWebhookWorkflowResponse{
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: pausePartnerOrderWorkflowReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
			PartnerWebhookId: req.GetPartnerWebhookId(),
		},
		Result: &commonv1.OperationResult{
			Details: pausePartnerOrderWorkflowReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ListPartnerWebhookWorkflows lists partner webhooks a page at a time.
func (s *PartnerWebhookWorkflowServer) ListPartnerWebhookWorkflows(ctx context.Context, req *partnerapiv1.ListPartnerWebhookWorkflowsRequest) (*partnerapiv1.ListPartnerWebhookWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list partner webhook workflows", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	generateSellerScorecardReportRequest := &sellersv1.GenerateSellerScorecardReportRequest{
		Context: req.GetContext(),
		Details: &sellersv1.GenerateSellerScorecardReportRequest_Details{Filters: labels},
	}
	generateSellerScorecardReportReply := new(sellersv1.GenerateSellerScorecardReportResponse)
	if err := grpcx.Invoke(ctx, s.conn, sellersv1.SellerScorecardReportingService_GenerateSellerScorecardReport_FullMethodName, generateSellerScorecardReportRequest, generateSellerScorecardReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/GenerateSellerScorecardReport")
	}
	listRoleSubscriptionsRequest := buildListRoleSubscriptionsRequestForListPartnerWebhookWorkflows(req)
	listRoleSubscriptionsReply, err := s.roleNotificationClient.ListRoleSubscriptions(ctx, listRoleSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/ListRoleSubscriptions")
	}
	return &partnerapiv1.ListPartnerWebhookWorkflowsResponse{
		PartnerWebhooks: []*partnerapiv1.PartnerWebhook{&partnerapiv1.PartnerWebhook{
			CorrelationId: generateSellerScorecardReportReply.GetResult().GetCorrelationId(),
		}},
		Result: &commonv1.OperationResult{
			CorrelationId: generateSellerScorecardReportReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PageInfo: &commonv1.PageInfo{
			TotalSize: listRoleSubscriptionsReply.GetPageInfo().GetTotalSize(),
		},
	}, nil
}

// partnerWebhookWorkflowServerListPartnerIntegrationReportsSteps are the ways to run
// acme.partnerapi.v1.PartnerIntegrationReportingService/ListPartnerIntegrationReports, by name.
var partnerWebhookWorkflowServerListPartnerIntegrationReportsSteps = map[string]func(*PartnerWebhookWorkflowServer, context.Context, *partnerapiv1.ListPartnerIntegrationReportsRequest) (*partnerapiv1.ListPartnerIntegrationReportsResponse, error){
	"default": (*PartnerWebhookWorkflowServer).listPartnerIntegrationReportsStep,
	"audited": (*PartnerWebhookWorkflowServer).listPartnerIntegrationReportsAuditedStep,
}

func (s *PartnerWebhookWorkflowServer) listPartnerIntegrationReportsStep(ctx context.Context, request *partnerapiv1.ListPartnerIntegrationReportsRequest) (*partnerapiv1.ListPartnerIntegrationReportsResponse, error) {
	return s.partnerIntegrationReportingClient.ListPartnerIntegrationReports(ctx, request)
}

func (s *PartnerWebhookWorkflowServer) listPartnerIntegrationReportsAuditedStep(ctx context.Context, request *partnerapiv1.ListPartnerIntegrationReportsRequest) (*partnerapiv1.ListPartnerIntegrationReportsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.partnerapi.v1.PartnerIntegrationReportingService/ListPartnerIntegrationReports")
	return s.partnerIntegrationReportingClient.ListPartnerIntegrationReports(ctx, request)
}

// sellerListingApprovalLazy dials acme.sellers.v1.SellerListingApprovalService the first time a handler needs it.
func (s *PartnerWebhookWorkflowServer) sellerListingApprovalLazy() sellersv1.SellerListingApprovalServiceClient {
	s.sellerListingApprovalOnce.Do(func() { s.sellerListingApprovalCached = sellersv1.NewSellerListingApprovalServiceClient(s.conn) })
	return s.sellerListingApprovalCached
}

// partnerWebhookWorkflowServerRetryPickListWorkflowSteps are the ways to run
// acme.warehousing.v1.PickListWorkflowService/RetryPickListWorkflow, by name.
var partnerWebhookWorkflowServerRetryPickListWorkflowSteps = map[string]func(*PartnerWebhookWorkflowServer, context.Context, *warehousingv1.RetryPickListWorkflowRequest) (*warehousingv1.RetryPickListWorkflowResponse, error){
	"default": (*PartnerWebhookWorkflowServer).retryPickListWorkflowStep,
	"audited": (*PartnerWebhookWorkflowServer).retryPickListWorkflowAuditedStep,
}

func (s *PartnerWebhookWorkflowServer) retryPickListWorkflowStep(ctx context.Context, request *warehousingv1.RetryPickListWorkflowRequest) (*warehousingv1.RetryPickListWorkflowResponse, error) {
	return s.pickListWorkflowClient.RetryPickListWorkflow(ctx, request)
}

func (s *PartnerWebhookWorkflowServer) retryPickListWorkflowAuditedStep(ctx context.Context, request *warehousingv1.RetryPickListWorkflowRequest) (*warehousingv1.RetryPickListWorkflowResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.warehousing.v1.PickListWorkflowService/RetryPickListWorkflow")
	return s.pickListWorkflowClient.RetryPickListWorkflow(ctx, request)
}
