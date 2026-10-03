package sellers

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerListingReportingServer implements `acme.sellers.v1.SellerListingReportingService`.
// Builds and schedules reports about seller listings.
type SellerListingReportingServer struct {
	sellersv1.UnimplementedSellerListingReportingServiceServer
	store                              *Store
	log                                *slog.Logger
	sellerQueryPort                    sellerQueryPort
	conn                               *grpc.ClientConn
	householdValidationClient          customersv1.HouseholdValidationServiceClient
	startGiftCardTransactionWorkflowFn func(context.Context, *giftcardsv1.StartGiftCardTransactionWorkflowRequest, ...grpc.CallOption) (*giftcardsv1.StartGiftCardTransactionWorkflowResponse, error)
	customerQueryOnce                  sync.Once
	customerQueryCached                customersv1.CustomerQueryServiceClient
	clients                            *registry.Registry
	meteredTenantExport                meteredTenantExport
	giftCardWorkflowClient             giftcardsv1.GiftCardWorkflowServiceClient
	householdQueryClient               customersv1.HouseholdQueryServiceClient
	preferenceCalculationClient        customersv1.PreferenceCalculationServiceClient
	submitGiftCardTransactionFn        func(context.Context, *giftcardsv1.SubmitGiftCardTransactionRequest, ...grpc.CallOption) (*giftcardsv1.SubmitGiftCardTransactionResponse, error)
	sellerCalculationClient            sellersv1.SellerCalculationServiceClient
}

// NewSellerListingReportingServer wires a SellerListingReportingServer.
func NewSellerListingReportingServer(deps platform.Deps) *SellerListingReportingServer {
	giftCardTransactionWorkflow := giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn)
	giftCardTransactionApproval := giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn)
	return &SellerListingReportingServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.sellers.v1.SellerListingReportingService"),
		sellerQueryPort:                    newSellerQueryPort(deps),
		conn:                               deps.Conn,
		householdValidationClient:          customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		startGiftCardTransactionWorkflowFn: giftCardTransactionWorkflow.StartGiftCardTransactionWorkflow,
		clients:                            deps.Clients,
		meteredTenantExport:                meteredTenantExport{TenantExportServiceClient: identityv1.NewTenantExportServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		giftCardWorkflowClient:             giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
		householdQueryClient:               customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		preferenceCalculationClient:        customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		submitGiftCardTransactionFn:        giftCardTransactionApproval.SubmitGiftCardTransaction,
		sellerCalculationClient:            sellersv1.NewSellerCalculationServiceClient(deps.Conn),
	}
}

// GenerateSellerListingReport generates a seller listing.
func (s *SellerListingReportingServer) GenerateSellerListingReport(ctx context.Context, req *sellersv1.GenerateSellerListingReportRequest) (*sellersv1.GenerateSellerListingReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "generate_seller_listing_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate seller listing report", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	suggestSellersRequest := &sellersv1.SuggestSellersRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.SuggestSellersRequest_Filter{
			Labels:  annotations,
			OwnerId: req.GetSellerListingId(),
		},
	}
	suggestSellersReply, err := s.sellerQueryPort.SuggestSellers(ctx, suggestSellersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/SuggestSellers")
	}
	return &sellersv1.GenerateSellerListingReportResponse{
		Result: &commonv1.OperationResult{
			Message: suggestSellersReply.GetResult().GetMessage(),
			Details: suggestSellersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchSellerListingReport returns a seller listing.
func (s *SellerListingReportingServer) FetchSellerListingReport(ctx context.Context, req *sellersv1.FetchSellerListingReportRequest) (*sellersv1.FetchSellerListingReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FetchSellerListingReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "fetch_seller_listing_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch seller listing report", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.FetchSellerListingReportResponse{
		SellerListing: &sellersv1.SellerListing{SellerListingId: req.GetSellerListingId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleSellerListingReport schedules a seller listing.
func (s *SellerListingReportingServer) ScheduleSellerListingReport(ctx context.Context, req *sellersv1.ScheduleSellerListingReportRequest) (*sellersv1.ScheduleSellerListingReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "schedule_seller_listing_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule seller listing report", "tenant", req.GetContext().GetTenantId())
	batchGetSellersRequest := &sellersv1.BatchGetSellersRequest{Context: req.GetContext()}
	batchGetSellersReply := new(sellersv1.BatchGetSellersResponse)
	if err := grpc.Invoke(ctx, "/acme.sellers.v1.SellerService/BatchGetSellers", batchGetSellersRequest, batchGetSellersReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/BatchGetSellers")
	}
	validateHouseholdRequest := &customersv1.ValidateHouseholdRequest{}
	validateHouseholdRequest.Context = req.GetContext()
	validateHouseholdRequest.Household = &customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		}},
	}
	validateHouseholdReply, err := s.householdValidationClient.ValidateHousehold(ctx, validateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ValidateHousehold")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	startGiftCardTransactionWorkflowRequest := &giftcardsv1.StartGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.StartGiftCardTransactionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: validateHouseholdReply.GetHousehold().GetManager().GetEmail(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: validateHouseholdReply.GetHousehold().GetManager().GetDisplayName(),
			},
			Attributes:    validateHouseholdReply.GetHousehold().GetAttributes(),
			CorrelationId: batchGetSellersReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
	}
	startGiftCardTransactionWorkflowReply, err := s.startGiftCardTransactionWorkflowFn(ctx, startGiftCardTransactionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/StartGiftCardTransactionWorkflow")
	}
	browseCustomersRequest := &customersv1.BrowseCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.BrowseCustomersRequest_Filter{
			Region:       validateHouseholdReply.GetHousehold().GetLocation().GetRegion(),
			CreatedAfter: validateHouseholdReply.GetOutcome().GetEffectiveAt(),
			OwnerId:      startGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetSubject().GetGiftCardBatchId(),
		},
	}
	browseCustomersReply, err := s.customerQueryLazy().BrowseCustomers(ctx, browseCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/BrowseCustomers")
	}
	return &sellersv1.ScheduleSellerListingReportResponse{
		Result: &commonv1.OperationResult{
			Details:       batchGetSellersReply.GetResult().GetDetails(),
			Status:        validateHouseholdReply.GetOutcome().GetStatus(),
			Message:       startGiftCardTransactionWorkflowReply.GetResult().GetMessage(),
			CorrelationId: startGiftCardTransactionWorkflowReply.GetResult().GetCorrelationId(),
			ReferenceIds:  browseCustomersReply.GetResult().GetReferenceIds(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamSellerListingMetrics streams the changes of seller listings as they happen.
func (s *SellerListingReportingServer) StreamSellerListingMetrics(req *sellersv1.StreamSellerListingMetricsRequest, stream grpc.ServerStreamingServer[sellersv1.StreamSellerListingMetricsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream seller listing metrics", "tenant", req.GetContext().GetTenantId())
	returnRequestCalculation, ok := s.clients.Get("acme.returns.v1.ReturnRequestCalculationService").(returnsv1.ReturnRequestCalculationServiceClient)
	if !ok {
		return status.Errorf(codes.Internal, "no client of %s", "acme.returns.v1.ReturnRequestCalculationService")
	}
	quoteReturnRequestRequest := &returnsv1.QuoteReturnRequestRequest{
		Context: req.GetContext(),
		Details: &returnsv1.QuoteReturnRequestRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{Email: req.GetSubscriber().GetEmail()},
		},
	}
	quoteReturnRequestReply, err := returnRequestCalculation.QuoteReturnRequest(ctx, quoteReturnRequestRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/QuoteReturnRequest")
	}
	getTenantExportRequest := &identityv1.GetTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	}
	getTenantExportReply, err := s.meteredTenantExport.GetTenantExport(ctx, getTenantExportRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.TenantExportService/GetTenantExport")
	}
	if err := stream.Send(&sellersv1.StreamSellerListingMetricsResponse{
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				Email: quoteReturnRequestReply.GetReturnRequest().GetCounterparty().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: quoteReturnRequestReply.GetReturnRequest().GetAudit().GetCreatedAt(),
			},
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: getTenantExportReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListSellerListingReports lists seller listings a page at a time.
func (s *SellerListingReportingServer) ListSellerListingReports(ctx context.Context, req *sellersv1.ListSellerListingReportsRequest) (*sellersv1.ListSellerListingReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list seller listing reports", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.ListSellerListingReportsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ShareSellerListingReport shares a seller listing.
func (s *SellerListingReportingServer) ShareSellerListingReport(ctx context.Context, req *sellersv1.ShareSellerListingReportRequest) (*sellersv1.ShareSellerListingReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "share_seller_listing_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share seller listing report", "tenant", req.GetContext().GetTenantId())
	var envelope sellersv1.SellerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	retryGiftCardWorkflowReply, err := s.giftCardWorkflowClient.RetryGiftCardWorkflow(ctx, &giftcardsv1.RetryGiftCardWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.RetryGiftCardWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetDetails().GetRecipient().GetName()},
		},
		GiftCardId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/RetryGiftCardWorkflow")
	}
	var envelope2 sellersv1.SellerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateConsentRecordRequest := &customersv1.ValidateConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.ValidateConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: retryGiftCardWorkflowReply.GetGiftCard().GetContact().GetPhone(),
				Name:  retryGiftCardWorkflowReply.GetGiftCard().GetContact().GetName(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: retryGiftCardWorkflowReply.GetGiftCard().GetHolder().GetDisplayName(),
			},
		},
		ConsentRecordId: envelope2.GetDisplayName(),
	}
	validateConsentRecordReply, err := validateConsentRecordCall(ctx, validateConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ValidateConsentRecord")
	}
	var envelope3 sellersv1.SellerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	summarizeHouseholdsReply, err := s.householdQueryClient.SummarizeHouseholds(ctx, &customersv1.SummarizeHouseholdsRequest{
		Context:     req.GetContext(),
		HouseholdId: validateConsentRecordReply.GetConsentRecord().GetSubject().GetHouseholdId(),
		Details:     &customersv1.SummarizeHouseholdsRequest_Details{Format: envelope3.GetDisplayName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/SummarizeHouseholds")
	}
	listGiftCardTransactionWorkflowsRequest := &giftcardsv1.ListGiftCardTransactionWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.ListGiftCardTransactionWorkflowsRequest_Filter{
			Labels:       retryGiftCardWorkflowReply.GetGiftCard().GetLabels(),
			CreatedAfter: retryGiftCardWorkflowReply.GetGiftCard().GetAudit().GetUpdatedAt(),
			OwnerId:      validateConsentRecordReply.GetConsentRecord().GetSubject().GetHouseholdId(),
		},
	}
	listGiftCardTransactionWorkflowsReply, err := listGiftCardTransactionWorkflowsCall(ctx, listGiftCardTransactionWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/ListGiftCardTransactionWorkflows")
	}
	return &sellersv1.ShareSellerListingReportResponse{
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: retryGiftCardWorkflowReply.GetGiftCard().GetAudit().GetUpdatedAt(),
			},
			SellerListingId: req.GetSellerListingId(),
			Owner:           &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{
			CorrelationId: validateConsentRecordReply.GetResult().GetCorrelationId(),
			ReferenceIds:  summarizeHouseholdsReply.GetResult().GetReferenceIds(),
			Details:       listGiftCardTransactionWorkflowsReply.GetResult().GetDetails(),
			Message:       listGiftCardTransactionWorkflowsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Outcome: &sellersv1.ShareSellerListingReportResponse_Outcome{
			NextSteps: validateConsentRecordReply.GetOutcome().GetNextSteps(),
		},
	}, nil
}

// CountSellerListingTotals counts seller listings by status.
func (s *SellerListingReportingServer) CountSellerListingTotals(ctx context.Context, req *sellersv1.CountSellerListingTotalsRequest) (*sellersv1.CountSellerListingTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count seller listing totals", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Region string }
	if record := req.GetFilter(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	calculatePreferenceRequest := &customersv1.CalculatePreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.CalculatePreferenceRequest_Details{Destination: &commonv1.PostalAddress{Region: holder.Region}},
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
	}
	type calculatePreferenceResult struct {
		reply *customersv1.CalculatePreferenceResponse
		err   error
	}
	calculatePreferenceResults := make(chan calculatePreferenceResult, 1)
	go func() {
		reply, err := s.preferenceCalculationClient.CalculatePreference(ctx, calculatePreferenceRequest)
		calculatePreferenceResults <- calculatePreferenceResult{reply, err}
	}()
	var calculatePreferenceReply *customersv1.CalculatePreferenceResponse
	select {
	case result := <-calculatePreferenceResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.PreferenceCalculationService/CalculatePreference")
		}
		calculatePreferenceReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	submitGiftCardTransactionRequest := &giftcardsv1.SubmitGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: calculatePreferenceReply.GetPreference().GetOwner().GetDisplayName(),
			},
			CorrelationId: calculatePreferenceReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
		Details: &giftcardsv1.SubmitGiftCardTransactionRequest_Details{
			Approver: &identityv1.UserRef{
				Email: calculatePreferenceReply.GetPreference().GetOwner().GetEmail(),
			},
		},
	}
	submitGiftCardTransactionReply, err := s.submitGiftCardTransactionFn(ctx, submitGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/SubmitGiftCardTransaction")
	}
	return &sellersv1.CountSellerListingTotalsResponse{
		Result: &commonv1.OperationResult{
			Status:       calculatePreferenceReply.GetResult().GetStatus(),
			ReferenceIds: submitGiftCardTransactionReply.GetResult().GetReferenceIds(),
			Message:      submitGiftCardTransactionReply.GetResult().GetMessage(),
		},
	}, nil
}

// DeleteSellerListingReport deletes a seller listing.
func (s *SellerListingReportingServer) DeleteSellerListingReport(ctx context.Context, req *sellersv1.DeleteSellerListingReportRequest) (*sellersv1.DeleteSellerListingReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DeleteSellerListingReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "delete_seller_listing_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete seller listing report", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "seller_listing_id", req.GetSellerListingId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	advanceGiftCardTransactionWorkflowRequest := &giftcardsv1.AdvanceGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.AdvanceGiftCardTransactionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Reason: req.GetReason(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			CorrelationId: tenancy.Value(ctx, "seller_listing_id"),
			Measurements:  []*giftcardsv1.GiftCardTransactionMeasurement{&giftcardsv1.GiftCardTransactionMeasurement{ObservedAt: cloned.GetDeadline()}},
		},
	}
	advanceGiftCardTransactionWorkflowReply := new(giftcardsv1.AdvanceGiftCardTransactionWorkflowResponse)
	if err := s.conn.Invoke(ctx, giftcardsGiftCardTransactionWorkflowServiceAdvanceGiftCardTransactionWorkflowMethod, advanceGiftCardTransactionWorkflowRequest, advanceGiftCardTransactionWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/AdvanceGiftCardTransactionWorkflow")
	}
	quoteSellerReply, err := s.sellerCalculationClient.QuoteSeller(ctx, &sellersv1.QuoteSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			IdentityDocument: &sellersv1.Seller_NationalId{NationalId: &sellersv1.SellerNationalId{
				Number: advanceGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetSubject().GetNumber(),
			}},
			Audit: &commonv1.AuditInfo{Etag: req.GetEtag()},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: advanceGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			Attributes: advanceGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetAttributes(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/QuoteSeller")
	}
	return &sellersv1.DeleteSellerListingReportResponse{
		Result: &commonv1.OperationResult{
			Status:       advanceGiftCardTransactionWorkflowReply.GetResult().GetStatus(),
			ReferenceIds: advanceGiftCardTransactionWorkflowReply.GetResult().GetReferenceIds(),
			Message:      quoteSellerReply.GetResult().GetMessage(),
		},
	}, nil
}

// customerQueryLazy dials acme.customers.v1.CustomerQueryService the first time a handler needs it.
func (s *SellerListingReportingServer) customerQueryLazy() customersv1.CustomerQueryServiceClient {
	s.customerQueryOnce.Do(func() { s.customerQueryCached = customersv1.NewCustomerQueryServiceClient(s.conn) })
	return s.customerQueryCached
}
