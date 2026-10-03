package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	driversv1 "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ReturnRequestServer implements `acme.returns.v1.ReturnRequestService`.
// Manages return requests: creates, reads, changes, and lists them.
type ReturnRequestServer struct {
	returnsv1.UnimplementedReturnRequestServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	accessPolicyOnce                sync.Once
	accessPolicyCached              authorizationv1.AccessPolicyServiceClient
	returnRequestCalculationClient  returnsv1.ReturnRequestCalculationServiceClient
	tierValidationClient            loyaltyv1.TierValidationServiceClient
	documentValidationClient        documentsv1.DocumentValidationServiceClient
	returnLabelCalculationPort      returnLabelCalculationPort
	inspectionCalculationPort       inspectionCalculationPort
	returnLabelQueryClient          returnsv1.ReturnLabelQueryServiceClient
	returnLabelCalculationClient    returnsv1.ReturnLabelCalculationServiceClient
	inspectionValidationOnce        sync.Once
	inspectionValidationCached      returnsv1.InspectionValidationServiceClient
	documentClient                  documentsv1.DocumentServiceClient
	sellerScorecardValidationClient sellersv1.SellerScorecardValidationServiceClient
	loyaltyMemberValidationClient   loyaltyv1.LoyaltyMemberValidationServiceClient
	driverClient                    driversv1.DriverServiceClient
}

// NewReturnRequestServer wires a ReturnRequestServer.
func NewReturnRequestServer(deps platform.Deps) *ReturnRequestServer {
	return &ReturnRequestServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.returns.v1.ReturnRequestService"),
		conn:                            deps.Conn,
		returnRequestCalculationClient:  returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn),
		tierValidationClient:            loyaltyv1.NewTierValidationServiceClient(deps.Conn),
		documentValidationClient:        documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		returnLabelCalculationPort:      newReturnLabelCalculationPort(deps),
		inspectionCalculationPort:       newInspectionCalculationPort(deps),
		returnLabelQueryClient:          returnsv1.NewReturnLabelQueryServiceClient(deps.Conn),
		returnLabelCalculationClient:    returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn),
		documentClient:                  documentsv1.NewDocumentServiceClient(deps.Conn),
		sellerScorecardValidationClient: sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		loyaltyMemberValidationClient:   loyaltyv1.NewLoyaltyMemberValidationServiceClient(deps.Conn),
		driverClient:                    driversv1.NewDriverServiceClient(deps.Conn),
	}
}

// CreateReturnRequest creates a return request.
func (s *ReturnRequestServer) CreateReturnRequest(ctx context.Context, req *returnsv1.CreateReturnRequestRequest) (*returnsv1.CreateReturnRequestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "create return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	listAccessPoliciesRequest := &authorizationv1.ListAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.ListAccessPoliciesRequest_Filter{
			Region:       req.GetReturnRequest().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetReturnRequest().GetDueAt(),
		},
	}
	listAccessPoliciesReply, err := s.accessPolicyLazy().ListAccessPolicies(ctx, listAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/ListAccessPolicies")
	}
	getReturnRequestRatesRequest := &returnsv1.GetReturnRequestRatesRequest{
		Context:         req.GetContext(),
		ReturnRequestId: req.GetReturnRequest().GetReturnRequestId(),
	}
	type getReturnRequestRatesResult struct {
		reply *returnsv1.GetReturnRequestRatesResponse
		err   error
	}
	getReturnRequestRatesResults := make(chan getReturnRequestRatesResult, 1)
	go func() {
		reply, err := s.returnRequestCalculationClient.GetReturnRequestRates(ctx, getReturnRequestRatesRequest)
		getReturnRequestRatesResults <- getReturnRequestRatesResult{reply, err}
	}()
	var getReturnRequestRatesReply *returnsv1.GetReturnRequestRatesResponse
	select {
	case result := <-getReturnRequestRatesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.returns.v1.ReturnRequestCalculationService/GetReturnRequestRates")
		}
		getReturnRequestRatesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &returnsv1.CreateReturnRequestResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listAccessPoliciesReply.GetResult().GetCorrelationId(),
			Status:        listAccessPoliciesReply.GetResult().GetStatus(),
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Metadata:     getReturnRequestRatesReply.GetReturnRequest().GetMetadata(),
			Counterparty: req.GetReturnRequest().GetCounterparty(),
			Number:       req.GetReturnRequest().GetNumber(),
		},
	}, nil
}

// GetReturnRequest returns a return request.
func (s *ReturnRequestServer) GetReturnRequest(ctx context.Context, req *returnsv1.GetReturnRequestRequest) (*returnsv1.GetReturnRequestResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_request", req.GetReturnRequestId(), "get_return_request"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get return request", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := returnRequestServerPreviewTierSteps[mode]
	var cachedNumber string
	if entry, ok := recentReturnRequests.Load(recordKey{req.GetContext().GetTenantId(), req.GetReturnRequestId()}); ok {
		if previous, ok := entry.(*returnsv1.ReturnRequest); ok {
			cachedNumber = previous.GetNumber()
		}
	}
	previewTierRequest := &loyaltyv1.PreviewTierRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		Reason: cachedNumber,
	}
	previewTierReply, err := run(s, ctx, previewTierRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/PreviewTier")
	}
	return &returnsv1.GetReturnRequestResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: previewTierReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ReturnRequest: &returnsv1.ReturnRequest{ReturnRequestId: req.GetReturnRequestId()},
	}, nil
}

// UpdateReturnRequest changes a return request.
func (s *ReturnRequestServer) UpdateReturnRequest(ctx context.Context, req *returnsv1.UpdateReturnRequestRequest) (*returnsv1.UpdateReturnRequestResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "update return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	simulateSellerPayoutScheduleRequest := &sellersv1.SimulateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetReturnRequest().GetCounterparty().GetEmail()},
			}},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetReturnRequest().GetAudit().GetCreatedBy()},
		},
	}
	simulateSellerPayoutScheduleReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerPayoutScheduleCalculationServiceClient).SimulateSellerPayoutSchedule(ctx, simulateSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/SimulateSellerPayoutSchedule")
	}
	recentReturnRequests.Store(recordKey{req.GetContext().GetTenantId(), req.GetReturnRequest().GetReturnRequestId()}, req.GetReturnRequest())
	return &returnsv1.UpdateReturnRequestResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			Metadata:     simulateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetMetadata(),
			DueAt:        req.GetReturnRequest().GetDueAt(),
			Counterparty: req.GetReturnRequest().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DeleteReturnRequest deletes a return request.
func (s *ReturnRequestServer) DeleteReturnRequest(ctx context.Context, req *returnsv1.DeleteReturnRequestRequest) (*returnsv1.DeleteReturnRequestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_request", req.GetReturnRequestId(), "delete_return_request"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete return request", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	previewDocumentReply, err := s.documentValidationClient.PreviewDocument(ctx, &documentsv1.PreviewDocumentRequest{
		Context:  req.GetContext(),
		Document: &documentsv1.Document{Audit: &commonv1.AuditInfo{Etag: req.GetEtag()}},
		Reason:   req.GetReason(),
		Details: &documentsv1.PreviewDocumentRequest_Details{
			Contact: &commonv1.ContactInfo{Email: (&decoded).GetActor().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/PreviewDocument")
	}
	simulateReturnLabelRequest := &returnsv1.SimulateReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode:  previewDocumentReply.GetDocument().GetBillingAddress().GetPostalCode(),
				Lines:       previewDocumentReply.GetDocument().GetBillingAddress().GetLines(),
				CountryCode: previewDocumentReply.GetDocument().GetBillingAddress().GetCountryCode(),
			},
			Settlement: &returnsv1.ReturnLabel_Bank{Bank: &returnsv1.ReturnLabelBankSettlement{
				AccountHolder: previewDocumentReply.GetDocument().GetBank().GetAccountHolder(),
			}},
		},
	}
	simulateReturnLabelReply, err := s.returnLabelCalculationPort.SimulateReturnLabel(ctx, simulateReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/SimulateReturnLabel")
	}
	return &returnsv1.DeleteReturnRequestResponse{
		Result: &commonv1.OperationResult{
			Message:       previewDocumentReply.GetOutcome().GetMessage(),
			CorrelationId: simulateReturnLabelReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ListReturnRequests lists return requests a page at a time.
func (s *ReturnRequestServer) ListReturnRequests(ctx context.Context, req *returnsv1.ListReturnRequestsRequest) (*returnsv1.ListReturnRequestsResponse, error) {
	s.log.InfoContext(ctx, "list return requests", "tenant", req.GetContext().GetTenantId())
	compareInspectionOptionsRequest := &returnsv1.CompareInspectionOptionsRequest{
		Context: req.GetContext(),
		Details: &returnsv1.CompareInspectionOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	compareInspectionOptionsReply, err := s.inspectionCalculationPort.CompareInspectionOptions(ctx, compareInspectionOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/CompareInspectionOptions")
	}
	return &returnsv1.ListReturnRequestsResponse{
		ReturnRequests: []*returnsv1.ReturnRequest{&returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				CustomerId: compareInspectionOptionsReply.GetInspection().GetCustomer().GetCustomerId(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: compareInspectionOptionsReply.GetInspection().GetAudit().GetEtag(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BatchGetReturnRequests returns several return requests at once.
func (s *ReturnRequestServer) BatchGetReturnRequests(ctx context.Context, req *returnsv1.BatchGetReturnRequestsRequest) (*returnsv1.BatchGetReturnRequestsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "batch get return requests", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizeReturnLabelsReply, err := s.returnLabelQueryClient.SummarizeReturnLabels(ctx, &returnsv1.SummarizeReturnLabelsRequest{
		Context:     req.GetContext(),
		Annotations: baggage,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelQueryService/SummarizeReturnLabels")
	}
	return &returnsv1.BatchGetReturnRequestsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeReturnLabelsReply.GetResult().GetReferenceIds(),
			Status:       summarizeReturnLabelsReply.GetResult().GetStatus(),
		},
	}, nil
}

// SearchReturnRequests lists return requests a page at a time.
func (s *ReturnRequestServer) SearchReturnRequests(ctx context.Context, req *returnsv1.SearchReturnRequestsRequest) (*returnsv1.SearchReturnRequestsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SearchReturnRequests", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "search return requests", "tenant", req.GetContext().GetTenantId())
	estimateReturnLabelRequest := newEstimateReturnLabelRequest(withEstimateReturnLabelContext(req.GetContext()), withEstimateReturnLabelReturnLabel(&returnsv1.ReturnLabel{
		BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
	}))
	estimateReturnLabelReply, err := s.returnLabelCalculationClient.EstimateReturnLabel(ctx, estimateReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/EstimateReturnLabel")
	}
	return &returnsv1.SearchReturnRequestsResponse{
		ReturnRequests: []*returnsv1.ReturnRequest{&returnsv1.ReturnRequest{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: estimateReturnLabelReply.GetReturnLabel().GetBillingAddress().GetRecipient(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// WatchReturnRequests streams the changes of return requests as they happen.
func (s *ReturnRequestServer) WatchReturnRequests(req *returnsv1.WatchReturnRequestsRequest, stream grpc.ServerStreamingServer[returnsv1.WatchReturnRequestsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "watch return requests", "tenant", req.GetContext().GetTenantId())
	estimateReturnRequestRequest := &returnsv1.EstimateReturnRequestRequest{
		Context: req.GetContext(),
		Details: &returnsv1.EstimateReturnRequestRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{Email: req.GetSubscriber().GetEmail()},
		},
	}
	estimateReturnRequestReply, err := estimateReturnRequestCall(ctx, estimateReturnRequestRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/EstimateReturnRequest")
	}
	validateInspectionRequest := &returnsv1.ValidateInspectionRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Origin: &returnsv1.Inspection_Customer{Customer: &customersv1.CustomerRef{
				DisplayName: estimateReturnRequestReply.GetReturnRequest().GetCounterparty().GetDisplayName(),
				CustomerId:  estimateReturnRequestReply.GetReturnRequest().GetCounterparty().GetCustomerId(),
			}},
		},
	}
	validateInspectionReply, err := s.inspectionValidationLazy().ValidateInspection(ctx, validateInspectionRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.returns.v1.InspectionValidationService/ValidateInspection")
	}
	if err := stream.Send(&returnsv1.WatchReturnRequestsResponse{
		Result: &commonv1.OperationResult{Status: estimateReturnRequestReply.GetResult().GetStatus()},
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				Email:       validateInspectionReply.GetInspection().GetRequester().GetEmail(),
				DisplayName: validateInspectionReply.GetInspection().GetCustomer().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreateReturnRequests takes a stream of return requests and answers once at the end.
func (s *ReturnRequestServer) BatchCreateReturnRequests(stream grpc.ClientStreamingServer[returnsv1.BatchCreateReturnRequestsRequest, returnsv1.BatchCreateReturnRequestsResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "batch create return requests", "tenant", msg.GetContext().GetTenantId())
		watchDocumentsStream, err := s.documentClient.WatchDocuments(ctx, &documentsv1.WatchDocumentsRequest{
			Context: msg.GetContext(),
			Subscriber: &commonv1.ContactInfo{
				Email: msg.GetRecord().GetCounterparty().GetEmail(),
				Name:  msg.GetHeader().GetSubmittedBy().GetName(),
			},
			Filter: &documentsv1.WatchDocumentsRequest_Filter{Region: msg.GetRecord().GetBillingAddress().GetRegion()},
		})
		if err != nil {
			return grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
		}
		for {
			received, err := watchDocumentsStream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
			}
			_ = received
		}
		accepted++
	}
	return stream.SendAndClose(&returnsv1.BatchCreateReturnRequestsResponse{Accepted: accepted})
}

// CloneReturnRequest creates a return request.
func (s *ReturnRequestServer) CloneReturnRequest(ctx context.Context, req *returnsv1.CloneReturnRequestRequest) (*returnsv1.CloneReturnRequestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "clone return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetReturnRequest().GetBillingAddress().GetRegion())
	suggestReturnLabelsRequest := &returnsv1.SuggestReturnLabelsRequest{}
	suggestReturnLabelsRequest.Context = req.GetContext()
	suggestReturnLabelsRequest.Filter = &returnsv1.SuggestReturnLabelsRequest_Filter{
		Region:       tenancy.Value(ctx, "region"),
		CreatedAfter: req.GetReturnRequest().GetAudit().GetCreatedAt(),
		Labels:       req.GetReturnRequest().GetMetadata(),
		OwnerId:      req.GetReturnRequest().GetReturnRequestId(),
	}
	suggestReturnLabelsReply, err := s.returnLabelQueryClient.SuggestReturnLabels(ctx, suggestReturnLabelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelQueryService/SuggestReturnLabels")
	}
	cloned := proto.Clone(req.GetReturnRequest()).(*returnsv1.ReturnRequest)
	validateSellerScorecardReply, err := s.sellerScorecardValidationClient.ValidateSellerScorecard(ctx, &sellersv1.ValidateSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetReturnRequest().GetAudit().GetUpdatedAt(),
				Etag:      cloned.GetAudit().GetEtag(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/ValidateSellerScorecard")
	}
	var holderName string
	switch choice := req.GetReturnRequest().GetSettlement().(type) {
	case *returnsv1.ReturnRequest_Card:
		holderName = choice.Card.GetHolderName()
	case *returnsv1.ReturnRequest_Bank:
		holderName = choice.Bank.GetIban()
	case *returnsv1.ReturnRequest_VoucherCode:
		holderName = choice.VoucherCode
	}
	scoreLoyaltyMemberRequest := &loyaltyv1.ScoreLoyaltyMemberRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.ScoreLoyaltyMemberRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: req.GetReturnRequest().GetBillingAddress().GetLocality(),
			},
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Funding: &loyaltyv1.LoyaltyMember_Card{Card: &loyaltyv1.LoyaltyMemberCardFunding{HolderName: holderName}},
		},
	}
	scoreLoyaltyMemberReply, err := loyaltyv1.LoyaltyMemberValidationServiceClient.ScoreLoyaltyMember(s.loyaltyMemberValidationClient, ctx, scoreLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberValidationService/ScoreLoyaltyMember")
	}
	for _, line := range req.GetReturnRequest().GetLines() {
		if _, err := s.driverClient.ListDrivers(ctx, &driversv1.ListDriversRequest{
			Context: req.GetContext(),
			Filter: &driversv1.ListDriversRequest_Filter{
				Region:       validateSellerScorecardReply.GetSellerScorecard().GetBillingAddress().GetRegion(),
				Labels:       scoreLoyaltyMemberReply.GetLoyaltyMember().GetLabels(),
				OwnerId:      line.GetLineId(),
				CreatedAfter: scoreLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.drivers.v1.DriverService/ListDrivers")
		}
	}
	recentReturnRequests.Store(recordKey{req.GetContext().GetTenantId(), req.GetReturnRequest().GetReturnRequestId()}, req.GetReturnRequest())
	return &returnsv1.CloneReturnRequestResponse{
		Result: &commonv1.OperationResult{
			Status:  suggestReturnLabelsReply.GetResult().GetStatus(),
			Details: suggestReturnLabelsReply.GetResult().GetDetails(),
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Audit: &commonv1.AuditInfo{
				Etag:      validateSellerScorecardReply.GetSellerScorecard().GetAudit().GetEtag(),
				CreatedBy: scoreLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetCreatedBy(),
				UpdatedAt: scoreLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
			},
			BillingAddress: validateSellerScorecardReply.GetSellerScorecard().GetBillingAddress(),
			IssuedAt:       req.GetReturnRequest().GetIssuedAt(),
			Notes:          req.GetReturnRequest().GetNotes(),
		},
	}, nil
}

// accessPolicyLazy dials acme.authorization.v1.AccessPolicyService the first time a handler needs it.
func (s *ReturnRequestServer) accessPolicyLazy() authorizationv1.AccessPolicyServiceClient {
	s.accessPolicyOnce.Do(func() { s.accessPolicyCached = authorizationv1.NewAccessPolicyServiceClient(s.conn) })
	return s.accessPolicyCached
}

// returnRequestServerPreviewTierSteps are the ways to run acme.loyalty.v1.TierValidationService/PreviewTier, by name.
var returnRequestServerPreviewTierSteps = map[string]func(*ReturnRequestServer, context.Context, *loyaltyv1.PreviewTierRequest) (*loyaltyv1.PreviewTierResponse, error){
	"default": (*ReturnRequestServer).previewTierStep,
	"audited": (*ReturnRequestServer).previewTierAuditedStep,
}

func (s *ReturnRequestServer) previewTierStep(ctx context.Context, request *loyaltyv1.PreviewTierRequest) (*loyaltyv1.PreviewTierResponse, error) {
	return s.tierValidationClient.PreviewTier(ctx, request)
}

func (s *ReturnRequestServer) previewTierAuditedStep(ctx context.Context, request *loyaltyv1.PreviewTierRequest) (*loyaltyv1.PreviewTierResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.loyalty.v1.TierValidationService/PreviewTier")
	return s.tierValidationClient.PreviewTier(ctx, request)
}

// inspectionValidationLazy dials acme.returns.v1.InspectionValidationService the first time a handler needs it.
func (s *ReturnRequestServer) inspectionValidationLazy() returnsv1.InspectionValidationServiceClient {
	s.inspectionValidationOnce.Do(func() { s.inspectionValidationCached = returnsv1.NewInspectionValidationServiceClient(s.conn) })
	return s.inspectionValidationCached
}
