package customers

import (
	"context"
	"fmt"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	expensesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/expenses/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	travelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/travel/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// CustomerSegmentCalculationServer implements `acme.customers.v1.CustomerSegmentCalculationService`.
// Computes the figures of a customer segment, such as totals, taxes, and estimates.
type CustomerSegmentCalculationServer struct {
	customersv1.UnimplementedCustomerSegmentCalculationServiceServer
	store                         *Store
	log                           *slog.Logger
	customerCalculationClient     customersv1.CustomerCalculationServiceClient
	sellerClient                  sellersv1.SellerServiceClient
	tripQueryClient               travelv1.TripQueryServiceClient
	conn                          *grpc.ClientConn
	estimateConsentRecordFn       func(context.Context, *customersv1.EstimateConsentRecordRequest, ...grpc.CallOption) (*customersv1.EstimateConsentRecordResponse, error)
	giftCardBatchValidationClient giftcardsv1.GiftCardBatchValidationServiceClient
	expenseReportQueryClient      expensesv1.ExpenseReportQueryServiceClient
	productReviewClient           reviewsv1.ProductReviewServiceClient
	customerValidationClient      customersv1.CustomerValidationServiceClient
	tenantNotificationClient      identityv1.TenantNotificationServiceClient
	clients                       *registry.Registry
	storageBinQueryClient         warehousingv1.StorageBinQueryServiceClient
	householdValidationClient     customersv1.HouseholdValidationServiceClient
}

// NewCustomerSegmentCalculationServer wires a CustomerSegmentCalculationServer.
func NewCustomerSegmentCalculationServer(deps platform.Deps) CustomerSegmentCalculationServer {
	consentRecordCalculation := customersv1.NewConsentRecordCalculationServiceClient(deps.Conn)
	return CustomerSegmentCalculationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.customers.v1.CustomerSegmentCalculationService"),
		customerCalculationClient:     customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		sellerClient:                  sellersv1.NewSellerServiceClient(deps.Conn),
		tripQueryClient:               travelv1.NewTripQueryServiceClient(deps.Conn),
		conn:                          deps.Conn,
		estimateConsentRecordFn:       consentRecordCalculation.EstimateConsentRecord,
		giftCardBatchValidationClient: giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn),
		expenseReportQueryClient:      expensesv1.NewExpenseReportQueryServiceClient(deps.Conn),
		productReviewClient:           reviewsv1.NewProductReviewServiceClient(deps.Conn),
		customerValidationClient:      customersv1.NewCustomerValidationServiceClient(deps.Conn),
		tenantNotificationClient:      identityv1.NewTenantNotificationServiceClient(deps.Conn),
		clients:                       deps.Clients,
		storageBinQueryClient:         warehousingv1.NewStorageBinQueryServiceClient(deps.Conn),
		householdValidationClient:     customersv1.NewHouseholdValidationServiceClient(deps.Conn),
	}
}

// CalculateCustomerSegment calculates a customer segment.
func (s CustomerSegmentCalculationServer) CalculateCustomerSegment(ctx context.Context, req *customersv1.CalculateCustomerSegmentRequest) (*customersv1.CalculateCustomerSegmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "calculate customer segment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	customerSegmentCache := cache.New[string, *customersv1.CustomerSegment](1)
	customerSegmentCache.Put(req.GetContext().GetTraceId(), req.GetCustomerSegment())
	cachedCustomerSegment, _ := customerSegmentCache.Get(req.GetContext().GetTraceId())
	detailsCache := cache.New[string, *customersv1.CalculateCustomerSegmentRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	calculateCustomerRequest := &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{Email: cachedCustomerSegment.GetOwner().GetEmail()},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetCustomerSegment().GetAudit().GetCreatedAt(),
				Etag:      req.GetCustomerSegment().GetAudit().GetEtag(),
			},
		},
		Details: &customersv1.CalculateCustomerRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: cachedDetails.GetDestination().GetCountryCode(),
			},
		},
	}
	send := s.calculateCustomerSender(calculateCustomerRequest)
	calculateCustomerReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	cloneSellerRequest := &sellersv1.CloneSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{
				Name: calculateCustomerReply.GetCustomer().GetContact().GetName(),
			},
		},
	}
	type cloneSellerResult struct {
		reply *sellersv1.CloneSellerResponse
		err   error
	}
	cloneSellerResults := make(chan cloneSellerResult, 1)
	go func() {
		reply, err := s.sellerClient.CloneSeller(ctx, cloneSellerRequest)
		cloneSellerResults <- cloneSellerResult{reply, err}
	}()
	var cloneSellerReply *sellersv1.CloneSellerResponse
	select {
	case result := <-cloneSellerResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerService/CloneSeller")
		}
		cloneSellerReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	resolveTripsReply, err := s.tripQueryClient.ResolveTrips(ctx, &travelv1.ResolveTripsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.travel.v1.TripQueryService/ResolveTrips")
	}
	_ = resolveTripsReply
	return &customersv1.CalculateCustomerSegmentResponse{
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: calculateCustomerReply.GetCustomer().GetAudit().GetCreatedBy(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
		},
		Result: &commonv1.OperationResult{
			Message: cloneSellerReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Breakdown: &customersv1.CalculateCustomerSegmentResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// EstimateCustomerSegment estimates a customer segment.
func (s CustomerSegmentCalculationServer) EstimateCustomerSegment(ctx context.Context, req *customersv1.EstimateCustomerSegmentRequest) (*customersv1.EstimateCustomerSegmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "estimate customer segment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	validateCustomerRequest := &customersv1.ValidateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetCustomerSegment().GetOwner().GetDisplayName(),
			},
			PrimaryAddress: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
		},
	}
	validateCustomerReply := new(customersv1.ValidateCustomerResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.CustomerValidationService_ValidateCustomer_FullMethodName, validateCustomerRequest, validateCustomerReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ValidateCustomer")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetCustomerSegment().GetOwner().GetDisplayName())
	ctx = tenancy.With(ctx, "country_code", req.GetDetails().GetDestination().GetCountryCode())
	estimateConsentRecordRequest := &customersv1.EstimateConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
		Details: &customersv1.EstimateConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{CountryCode: tenancy.Value(ctx, "country_code")},
		},
	}
	estimateConsentRecordReply, err := s.estimateConsentRecordFn(ctx, estimateConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/EstimateConsentRecord")
	}
	explainCustomerCalculationRequest := buildExplainCustomerCalculationRequestForEstimateCustomerSegment(req, validateCustomerReply)
	explainCustomerCalculationReply, err := s.customerCalculationClient.ExplainCustomerCalculation(ctx, explainCustomerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	snapshot, err := protojson.Marshal(req.GetCustomerSegment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullSynonymsRequest := &searchv1.PullSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				DisplayName: explainCustomerCalculationReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
		},
		Details: &searchv1.PullSynonymsRequest_Details{Payload: snapshot},
	}
	pullSynonymsReply := new(searchv1.PullSynonymsResponse)
	if err := grpcx.Invoke(ctx, s.conn, searchv1.SynonymSyncService_PullSynonyms_FullMethodName, pullSynonymsRequest, pullSynonymsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PullSynonyms")
	}
	_ = estimateConsentRecordReply
	return &customersv1.EstimateCustomerSegmentResponse{
		Result: &commonv1.OperationResult{
			Status:  validateCustomerReply.GetResult().GetStatus(),
			Details: explainCustomerCalculationReply.GetResult().GetDetails(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				Etag:      validateCustomerReply.GetCustomer().GetAudit().GetEtag(),
				CreatedBy: pullSynonymsReply.GetSynonym().GetAudit().GetCreatedBy(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
		},
		Breakdown: &customersv1.EstimateCustomerSegmentResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// QuoteCustomerSegment quotes a customer segment.
func (s CustomerSegmentCalculationServer) QuoteCustomerSegment(ctx context.Context, req *customersv1.QuoteCustomerSegmentRequest) (*customersv1.QuoteCustomerSegmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "quote customer segment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*customersv1.QuoteCustomerSegmentRequest_Details)
	estimateCustomerRequest := &customersv1.EstimateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetCustomerSegment().GetAudit().GetCreatedBy(),
			},
		},
		Details: &customersv1.EstimateCustomerRequest_Details{
			Destination: &commonv1.PostalAddress{Region: cloned.GetDestination().GetRegion()},
		},
	}
	estimateCustomerReply := new(customersv1.EstimateCustomerResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersCustomerCalculationServiceName, "EstimateCustomer"), estimateCustomerRequest, estimateCustomerReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/EstimateCustomer")
	}
	recalculateCustomersRequest := &customersv1.RecalculateCustomersRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Preferences: &customersv1.CustomerPreferences{
				Channels: estimateCustomerReply.GetCustomer().GetPreferences().GetChannels(),
			},
		},
	}
	recalculateCustomersReply, err := customersv1.CustomerCalculationServiceClient.RecalculateCustomers(s.customerCalculationClient, ctx, recalculateCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	lintGiftCardBatchesReply, err := s.giftCardBatchValidationClient.LintGiftCardBatches(ctx, &giftcardsv1.LintGiftCardBatchesRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: estimateCustomerReply.GetCustomer().GetAudit().GetUpdatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/LintGiftCardBatches")
	}
	return &customersv1.QuoteCustomerSegmentResponse{
		Result: &commonv1.OperationResult{
			Message:      estimateCustomerReply.GetResult().GetMessage(),
			ReferenceIds: recalculateCustomersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: lintGiftCardBatchesReply.GetGiftCardBatch().GetAudit().GetUpdatedBy(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Owner:             req.GetCustomerSegment().GetOwner(),
		},
	}, nil
}

// CompareCustomerSegmentOptions compares a customer segment.
func (s CustomerSegmentCalculationServer) CompareCustomerSegmentOptions(ctx context.Context, req *customersv1.CompareCustomerSegmentOptionsRequest) (*customersv1.CompareCustomerSegmentOptionsResponse, error) {
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "compare customer segment options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	countExpenseReportsRequest := buildCountExpenseReportsRequestForCompareCustomerSegmentOptions(req)
	countExpenseReportsReply, err := s.expenseReportQueryClient.CountExpenseReports(ctx, countExpenseReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.expenses.v1.ExpenseReportQueryService/CountExpenseReports")
	}
	cloneProductReviewRequest := &reviewsv1.CloneProductReviewRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			Audit:          &commonv1.AuditInfo{Etag: req.GetCustomerSegment().GetAudit().GetEtag()},
			BillingAddress: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
		},
	}
	type cloneProductReviewResult struct {
		reply *reviewsv1.CloneProductReviewResponse
		err   error
	}
	cloneProductReviewResults := make(chan cloneProductReviewResult, 1)
	go func() {
		reply, err := s.productReviewClient.CloneProductReview(ctx, cloneProductReviewRequest)
		cloneProductReviewResults <- cloneProductReviewResult{reply, err}
	}()
	var cloneProductReviewReply *reviewsv1.CloneProductReviewResponse
	select {
	case result := <-cloneProductReviewResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.reviews.v1.ProductReviewService/CloneProductReview")
		}
		cloneProductReviewReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	checkCustomerEligibilityRequest := buildCheckCustomerEligibilityRequestForCompareCustomerSegmentOptions(req, cloneProductReviewReply)
	checkCustomerEligibilityReply, err := s.customerValidationClient.CheckCustomerEligibility(ctx, checkCustomerEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/CheckCustomerEligibility")
	}
	var reason string
	switch choice := req.GetCustomerSegment().GetAction().(type) {
	case *customersv1.CustomerSegment_Route:
		reason = choice.Route.GetTarget()
	case *customersv1.CustomerSegment_Block:
		reason = choice.Block.GetReason()
	}
	packed, err := anypb.New(req.GetCustomerSegment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	step := &customerSegmentCalculationServerCompareCustomerSegmentOptionsSendTenantTestNoticeStep{request: &identityv1.SendTenantTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendTenantTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: checkCustomerEligibilityReply.GetCustomer().GetRelationshipManager().GetEmail(),
				Phone: checkCustomerEligibilityReply.GetCustomer().GetContact().GetPhone(),
				Name:  checkCustomerEligibilityReply.GetCustomer().GetContact().GetName(),
			},
			Payload: packed,
		},
		Reason: reason,
	}}
	if err := s.runCompareCustomerSegmentOptionsSendTenantTestNotice(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/SendTenantTestNotice")
	}
	sendTenantTestNoticeReply := step.reply
	return &customersv1.CompareCustomerSegmentOptionsResponse{
		Result: &commonv1.OperationResult{
			Details:       countExpenseReportsReply.GetResult().GetDetails(),
			CorrelationId: countExpenseReportsReply.GetResult().GetCorrelationId(),
			Message:       checkCustomerEligibilityReply.GetOutcome().GetMessage(),
			Status:        sendTenantTestNoticeReply.GetResult().GetStatus(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				Etag:      cloneProductReviewReply.GetProductReview().GetAudit().GetEtag(),
				UpdatedAt: sendTenantTestNoticeReply.GetTenant().GetAudit().GetUpdatedAt(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Version:           req.GetCustomerSegment().GetVersion(),
		},
	}, nil
}

// RecalculateCustomerSegments recalculates a customer segment.
func (s CustomerSegmentCalculationServer) RecalculateCustomerSegments(ctx context.Context, req *customersv1.RecalculateCustomerSegmentsRequest) (*customersv1.RecalculateCustomerSegmentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "recalculate customer segments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	householdCalculation, ok := s.clients.Get("acme.customers.v1.HouseholdCalculationService").(customersv1.HouseholdCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.HouseholdCalculationService")
	}
	ctx = tenancy.With(ctx, "name", req.GetCustomerSegment().GetName())
	simulateHouseholdRequest := &customersv1.SimulateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager:  &identityv1.UserRef{UserId: req.GetCustomerSegment().GetOwner().GetUserId()},
					Children: []*customersv1.Household{&customersv1.Household{Name: tenancy.Value(ctx, "name")}},
				}},
				Manager: &identityv1.UserRef{Email: req.GetCustomerSegment().GetOwner().GetEmail()},
				Location: &commonv1.PostalAddress{
					CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
				},
			}},
		},
	}
	simulateHouseholdReply, err := householdCalculation.SimulateHousehold(ctx, simulateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/SimulateHousehold")
	}
	return &customersv1.RecalculateCustomerSegmentsResponse{
		CustomerSegment: &customersv1.CustomerSegment{
			Name:              simulateHouseholdReply.GetHousehold().GetName(),
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Effective:         req.GetCustomerSegment().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: simulateHouseholdReply.GetResult().GetStatus()},
	}, nil
}

// SimulateCustomerSegment simulates a customer segment.
func (s CustomerSegmentCalculationServer) SimulateCustomerSegment(ctx context.Context, req *customersv1.SimulateCustomerSegmentRequest) (*customersv1.SimulateCustomerSegmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SimulateCustomerSegment", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "simulate customer segment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	getTenantPreferencesRequest := &identityv1.GetTenantPreferencesRequest{}
	getTenantPreferencesRequest.Context = req.GetContext()
	getTenantPreferencesRequest.TenantId = req.GetContext().GetTenantId()
	getTenantPreferencesReply, err := s.tenantNotificationClient.GetTenantPreferences(ctx, getTenantPreferencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/GetTenantPreferences")
	}
	return &customersv1.SimulateCustomerSegmentResponse{
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: getTenantPreferencesReply.GetTenant().GetAudit().GetUpdatedBy(),
				Etag:      getTenantPreferencesReply.GetTenant().GetAudit().GetEtag(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Name:              req.GetCustomerSegment().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainCustomerSegmentCalculation explains a customer segment.
func (s CustomerSegmentCalculationServer) ExplainCustomerSegmentCalculation(ctx context.Context, req *customersv1.ExplainCustomerSegmentCalculationRequest) (*customersv1.ExplainCustomerSegmentCalculationResponse, error) {
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "explain customer segment calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	browseStorageBinsRequest := &warehousingv1.BrowseStorageBinsRequest{}
	browseStorageBinsRequest.Context = req.GetContext()
	browseStorageBinsRequest.Filter = &warehousingv1.BrowseStorageBinsRequest_Filter{
		OwnerId:      req.GetCustomerSegment().GetOwner().GetUserId(),
		CreatedAfter: req.GetCustomerSegment().GetAudit().GetCreatedAt(),
		Labels:       req.GetCustomerSegment().GetParameters(),
	}
	browseStorageBinsReply, err := s.storageBinQueryClient.BrowseStorageBins(ctx, browseStorageBinsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/BrowseStorageBins")
	}
	return &customersv1.ExplainCustomerSegmentCalculationResponse{
		Outcome: &customersv1.ExplainCustomerSegmentCalculationResponse_Outcome{Message: browseStorageBinsReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			CorrelationId: browseStorageBinsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		CustomerSegment: &customersv1.CustomerSegment{
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Owner:             req.GetCustomerSegment().GetOwner(),
		},
	}, nil
}

// GetCustomerSegmentRates returns a customer segment.
func (s CustomerSegmentCalculationServer) GetCustomerSegmentRates(ctx context.Context, req *customersv1.GetCustomerSegmentRatesRequest) (*customersv1.GetCustomerSegmentRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "get_customer_segment_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get customer segment rates", "tenant", req.GetContext().GetTenantId())
	validateHouseholdReply, err := s.householdValidationClient.ValidateHousehold(ctx, &customersv1.ValidateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager:  &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
					ParentId: req.GetCustomerSegmentId(),
				}},
				Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			}},
		},
		Details: &customersv1.ValidateHouseholdRequest_Details{Locale: req.GetContext().GetLocale()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ValidateHousehold")
	}
	normalizeHouseholdReply, err := s.householdValidationClient.NormalizeHousehold(ctx, &customersv1.NormalizeHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						DisplayName: validateHouseholdReply.GetHousehold().GetManager().GetDisplayName(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedBy: validateHouseholdReply.GetHousehold().GetAudit().GetCreatedBy(),
					},
					Location: &commonv1.PostalAddress{
						Locality: validateHouseholdReply.GetHousehold().GetLocation().GetLocality(),
					},
					Children: []*customersv1.Household{&customersv1.Household{Manager: validateHouseholdReply.GetHousehold().GetManager()}},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/NormalizeHousehold")
	}
	return &customersv1.GetCustomerSegmentRatesResponse{
		Result: &commonv1.OperationResult{
			Status:        validateHouseholdReply.GetOutcome().GetStatus(),
			CorrelationId: normalizeHouseholdReply.GetResult().GetCorrelationId(),
		},
		CustomerSegment: &customersv1.CustomerSegment{CustomerSegmentId: req.GetCustomerSegmentId()},
	}, nil
}

// calculateCustomerSender returns a function that sends one request to
// acme.customers.v1.CustomerCalculationService/CalculateCustomer.
func (s CustomerSegmentCalculationServer) calculateCustomerSender(request *customersv1.CalculateCustomerRequest) func(context.Context) (*customersv1.CalculateCustomerResponse, error) {
	return func(ctx context.Context) (*customersv1.CalculateCustomerResponse, error) {
		return s.customerCalculationClient.CalculateCustomer(ctx, request)
	}
}

// customerSegmentCalculationServerCompareCustomerSegmentOptionsSendTenantTestNoticeStep carries the request of one step
// of CompareCustomerSegmentOptions and, once it ran, its reply.
type customerSegmentCalculationServerCompareCustomerSegmentOptionsSendTenantTestNoticeStep struct {
	request *identityv1.SendTenantTestNoticeRequest
	reply   *identityv1.SendTenantTestNoticeResponse
}

func (s CustomerSegmentCalculationServer) runCompareCustomerSegmentOptionsSendTenantTestNotice(ctx context.Context, step *customerSegmentCalculationServerCompareCustomerSegmentOptionsSendTenantTestNoticeStep) error {
	reply, err := s.tenantNotificationClient.SendTenantTestNotice(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
