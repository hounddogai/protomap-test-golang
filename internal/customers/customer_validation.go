package customers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
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
)

// CustomerValidationServer implements `acme.customers.v1.CustomerValidationService`.
// Checks customers against the rules before anything acts on them.
type CustomerValidationServer struct {
	customersv1.UnimplementedCustomerValidationServiceServer
	store                              *Store
	log                                *slog.Logger
	customerCalculationClient          customersv1.CustomerCalculationServiceClient
	customerCalculationClientFallback  customersv1.CustomerCalculationServiceClient
	conn                               *grpc.ClientConn
	customerCalculationOnce            sync.Once
	customerCalculationCached          customersv1.CustomerCalculationServiceClient
	storageBinQueryClient              warehousingv1.StorageBinQueryServiceClient
	clients                            *registry.Registry
	carePlanClient                     careplansv1.CarePlanServiceClient
	householdCalculationClient         customersv1.HouseholdCalculationServiceClient
	fulfillmentQueryClient             fulfillmentv1.FulfillmentQueryServiceClient
	householdCalculationClientFallback customersv1.HouseholdCalculationServiceClient
}

// NewCustomerValidationServer wires a CustomerValidationServer.
func NewCustomerValidationServer(deps platform.Deps) *CustomerValidationServer {
	return &CustomerValidationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.customers.v1.CustomerValidationService"),
		customerCalculationClient:          customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		customerCalculationClientFallback:  customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		conn:                               deps.Conn,
		storageBinQueryClient:              warehousingv1.NewStorageBinQueryServiceClient(deps.Conn),
		clients:                            deps.Clients,
		carePlanClient:                     careplansv1.NewCarePlanServiceClient(deps.Conn),
		householdCalculationClient:         customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		fulfillmentQueryClient:             fulfillmentv1.NewFulfillmentQueryServiceClient(deps.Conn),
		householdCalculationClientFallback: customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
	}
}

// ValidateCustomer validates a customer.
func (s *CustomerValidationServer) ValidateCustomer(ctx context.Context, req *customersv1.ValidateCustomerRequest) (*customersv1.ValidateCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "validate customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *customersv1.GetCustomerRatesRequest, ...grpc.CallOption) (*customersv1.GetCustomerRatesResponse, error){
		"primary":  s.customerCalculationClient.GetCustomerRates,
		"fallback": s.customerCalculationClientFallback.GetCustomerRates,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	getCustomerRatesRequest := &customersv1.GetCustomerRatesRequest{
		Context:    req.GetContext(),
		CustomerId: req.GetCustomerId(),
	}
	getCustomerRatesReply, err := route(ctx, getCustomerRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/GetCustomerRates")
	}
	var number string
	switch choice := req.GetCustomer().GetIdentityDocument().(type) {
	case *customersv1.Customer_Passport:
		number = choice.Passport.GetNumber()
	case *customersv1.Customer_NationalId:
		number = choice.NationalId.GetNumber()
	case *customersv1.Customer_TaxNumber:
		number = choice.TaxNumber
	}
	quoteCustomerRequest := &customersv1.QuoteCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress:   &commonv1.PostalAddress{Lines: req.GetCustomer().GetPrimaryAddress().GetLines()},
			IdentityDocument: &customersv1.Customer_NationalId{NationalId: &customersv1.CustomerNationalId{Number: number}},
			Preferences: &customersv1.CustomerPreferences{
				MarketingOptIn: getCustomerRatesReply.GetCustomer().GetPreferences().GetMarketingOptIn(),
				Channels:       req.GetCustomer().GetPreferences().GetChannels(),
			},
		},
	}
	quoteCustomerReply, err := grpcx.Call(ctx, s.customerCalculationClient.QuoteCustomer, quoteCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/QuoteCustomer")
	}
	estimateCustomerRequest := &customersv1.EstimateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient: req.GetCustomer().GetPrimaryAddress().GetRecipient(),
			},
		},
	}
	estimateCustomerReply, err := s.customerCalculationLazy().EstimateCustomer(ctx, estimateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/EstimateCustomer")
	}
	return &customersv1.ValidateCustomerResponse{
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{
				Name: getCustomerRatesReply.GetCustomer().GetContact().GetName(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				CountryCode: quoteCustomerReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
			},
			Preferences: &customersv1.CustomerPreferences{
				Channels: estimateCustomerReply.GetCustomer().GetPreferences().GetChannels(),
			},
			CustomerId: req.GetCustomerId(),
			Attributes: req.GetCustomer().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewCustomer previews a customer.
func (s *CustomerValidationServer) PreviewCustomer(ctx context.Context, req *customersv1.PreviewCustomerRequest) (*customersv1.PreviewCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "preview customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetCustomer())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.Customer
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	ctx = tenancy.With(ctx, "issuing_country", req.GetCustomer().GetNationalId().GetIssuingCountry())
	explainCustomerCalculationRequest := &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainCustomerCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: (&decoded).GetContact().GetEmail(),
				Phone: req.GetCustomer().GetContact().GetPhone(),
			},
		},
		Customer: &customersv1.Customer{
			IdentityDocument: &customersv1.Customer_NationalId{NationalId: &customersv1.CustomerNationalId{IssuingCountry: tenancy.Value(ctx, "issuing_country")}},
		},
	}
	explainCustomerCalculationReply, err := s.customerCalculationLazy().ExplainCustomerCalculation(ctx, explainCustomerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	suggestStorageBinsRequest := &warehousingv1.SuggestStorageBinsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.SuggestStorageBinsRequest_Filter{
			Region: explainCustomerCalculationReply.GetCustomer().GetPrimaryAddress().GetRegion(),
		},
	}
	suggestStorageBinsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*warehousingv1.SuggestStorageBinsResponse, error) {
		return s.storageBinQueryClient.SuggestStorageBins(ctx, suggestStorageBinsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/SuggestStorageBins")
	}
	return &customersv1.PreviewCustomerResponse{
		Outcome: &customersv1.PreviewCustomerResponse_Outcome{
			Status: explainCustomerCalculationReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: suggestStorageBinsReply.GetResult().GetCorrelationId(),
			Message:       suggestStorageBinsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Customer: &customersv1.Customer{
			CustomerId: req.GetCustomerId(),
			Contact:    req.GetCustomer().GetContact(),
		},
	}, nil
}

// CheckCustomerEligibility checks a customer.
func (s *CustomerValidationServer) CheckCustomerEligibility(ctx context.Context, req *customersv1.CheckCustomerEligibilityRequest) (*customersv1.CheckCustomerEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckCustomerEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "check customer eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetCustomer())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.Customer
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	customerCache := cache.New[string, *customersv1.Customer](1)
	customerCache.Put(req.GetContext().GetTraceId(), req.GetCustomer())
	cachedCustomer, _ := customerCache.Get(req.GetContext().GetTraceId())
	recalculateHouseholdsRequest := &customersv1.RecalculateHouseholdsRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						Recipient:   (&decoded).GetPrimaryAddress().GetRecipient(),
						CountryCode: req.GetCustomer().GetPrimaryAddress().GetCountryCode(),
					},
					Audit: &commonv1.AuditInfo{CreatedBy: cachedCustomer.GetAudit().GetCreatedBy()},
				}},
			}},
		},
	}
	recalculateHouseholdsReply := new(customersv1.RecalculateHouseholdsResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.HouseholdCalculationService_RecalculateHouseholds_FullMethodName, recalculateHouseholdsRequest, recalculateHouseholdsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/RecalculateHouseholds")
	}
	return &customersv1.CheckCustomerEligibilityResponse{
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				UserId: recalculateHouseholdsReply.GetHousehold().GetManager().GetUserId(),
			},
			CustomerId:     req.GetCustomerId(),
			PrimaryAddress: req.GetCustomer().GetPrimaryAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainCustomerDecision explains a customer.
func (s *CustomerValidationServer) ExplainCustomerDecision(ctx context.Context, req *customersv1.ExplainCustomerDecisionRequest) (*customersv1.ExplainCustomerDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainCustomerDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "explain customer decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	customerCalculation, ok := s.clients.Get("acme.customers.v1.CustomerCalculationService").(customersv1.CustomerCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.CustomerCalculationService")
	}
	var issuingCountry string
	switch choice := req.GetCustomer().GetIdentityDocument().(type) {
	case *customersv1.Customer_Passport:
		issuingCountry = choice.Passport.GetIssuingCountry()
	case *customersv1.Customer_NationalId:
		issuingCountry = choice.NationalId.GetNumber()
	case *customersv1.Customer_TaxNumber:
		issuingCountry = choice.TaxNumber
	}
	cloned := proto.Clone(req.GetCustomer()).(*customersv1.Customer)
	explainCustomerCalculationRequest := &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			IdentityDocument: &customersv1.Customer_Passport{Passport: &customersv1.CustomerPassport{IssuingCountry: issuingCountry}},
			Audit:            &commonv1.AuditInfo{UpdatedBy: cloned.GetAudit().GetUpdatedBy()},
		},
		Details: &customersv1.ExplainCustomerCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
	}
	explainCustomerCalculationReply, err := customerCalculation.ExplainCustomerCalculation(ctx, explainCustomerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	createCarePlanReply, err := s.carePlanClient.CreateCarePlan(ctx, &careplansv1.CreateCarePlanRequest{
		Context: req.GetContext(),
		CarePlan: &careplansv1.CarePlan{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainCustomerCalculationReply.GetCustomer().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: explainCustomerCalculationReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
				Recipient:   req.GetCustomer().GetPrimaryAddress().GetRecipient(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.careplans.v1.CarePlanService/CreateCarePlan")
	}
	return &customersv1.ExplainCustomerDecisionResponse{
		Customer: &customersv1.Customer{
			Status:  explainCustomerCalculationReply.GetCustomer().GetStatus(),
			Contact: explainCustomerCalculationReply.GetCustomer().GetContact(),
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: createCarePlanReply.GetCarePlan().GetBillingAddress().GetPostalCode(),
			},
			CustomerId:          req.GetCustomerId(),
			RelationshipManager: req.GetCustomer().GetRelationshipManager(),
		},
		Result: &commonv1.OperationResult{Status: createCarePlanReply.GetResult().GetStatus()},
	}, nil
}

// ScoreCustomer scores a customer.
func (s *CustomerValidationServer) ScoreCustomer(ctx context.Context, req *customersv1.ScoreCustomerRequest) (*customersv1.ScoreCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreCustomer", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "score customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	calculateCustomerRequest := &customersv1.CalculateCustomerRequest{}
	calculateCustomerRequest.Context = req.GetContext()
	calculateCustomerRequest.Details = &customersv1.CalculateCustomerRequest_Details{
		Destination: &commonv1.PostalAddress{Region: req.GetCustomer().GetPrimaryAddress().GetRegion()},
	}
	calculateCustomerReply, err := s.customerCalculationClient.CalculateCustomer(ctx, calculateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	recalculateHouseholdsRequest := &customersv1.RecalculateHouseholdsRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						Recipient: calculateCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
						Region:    req.GetDetails().GetDestination().GetRegion(),
					},
					Audit: &commonv1.AuditInfo{
						UpdatedAt: calculateCustomerReply.GetCustomer().GetAudit().GetUpdatedAt(),
					},
				}},
			}},
		},
	}
	recalculateHouseholdsReply, err := grpcx.Call(ctx, s.householdCalculationClient.RecalculateHouseholds, recalculateHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/RecalculateHouseholds")
	}
	return &customersv1.ScoreCustomerResponse{
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateCustomerReply.GetCustomer().GetAudit().GetUpdatedBy(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Region: recalculateHouseholdsReply.GetHousehold().GetLocation().GetRegion(),
			},
			CustomerId: req.GetCustomerId(),
			Contact:    &commonv1.ContactInfo{Name: req.GetCustomer().GetName()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintCustomers lints a customer.
func (s *CustomerValidationServer) LintCustomers(ctx context.Context, req *customersv1.LintCustomersRequest) (*customersv1.LintCustomersResponse, error) {
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "lint customers", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetCustomer())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.Customer
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cloned := proto.Clone(req.GetCustomer()).(*customersv1.Customer)
	explainOrderAmendmentDecisionRequest := &ordersv1.ExplainOrderAmendmentDecisionRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			BillingAddress: &commonv1.PostalAddress{
				Region: req.GetCustomer().GetPrimaryAddress().GetRegion(),
				Lines:  req.GetCustomer().GetPrimaryAddress().GetLines(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: (&decoded).GetAudit().GetCreatedAt(),
				UpdatedAt: cloned.GetAudit().GetUpdatedAt(),
			},
		},
	}
	explainOrderAmendmentDecisionReply := new(ordersv1.ExplainOrderAmendmentDecisionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", ordersOrderAmendmentValidationServiceName, "ExplainOrderAmendmentDecision"), explainOrderAmendmentDecisionRequest, explainOrderAmendmentDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/ExplainOrderAmendmentDecision")
	}
	customerCache := cache.New[string, *customersv1.Customer](1)
	customerCache.Put(req.GetContext().GetTraceId(), req.GetCustomer())
	cachedCustomer, _ := customerCache.Get(req.GetContext().GetTraceId())
	estimateCustomerReply, err := s.customerCalculationClient.EstimateCustomer(ctx, &customersv1.EstimateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact:        &commonv1.ContactInfo{Email: cachedCustomer.GetRelationshipManager().GetEmail()},
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetCustomer().GetPrimaryAddress().GetRegion()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/EstimateCustomer")
	}
	resolveFulfillmentsRequest := &fulfillmentv1.ResolveFulfillmentsRequest{Context: req.GetContext()}
	resolveFulfillmentsReply, err := fulfillmentv1.FulfillmentQueryServiceClient.ResolveFulfillments(s.fulfillmentQueryClient, ctx, resolveFulfillmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/ResolveFulfillments")
	}
	validateProductReviewRequest := &reviewsv1.ValidateProductReviewRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: req.GetCustomer().GetPrimaryAddress().GetRecipient(),
			},
		},
	}
	validateProductReviewReply := new(reviewsv1.ValidateProductReviewResponse)
	if err := grpcx.Invoke(ctx, s.conn, reviewsv1.ProductReviewValidationService_ValidateProductReview_FullMethodName, validateProductReviewRequest, validateProductReviewReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewValidationService/ValidateProductReview")
	}
	return &customersv1.LintCustomersResponse{
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Region:     explainOrderAmendmentDecisionReply.GetOrderAmendment().GetBillingAddress().GetRegion(),
				PostalCode: validateProductReviewReply.GetProductReview().GetBillingAddress().GetPostalCode(),
			},
			Contact: estimateCustomerReply.GetCustomer().GetContact(),
			RelationshipManager: &identityv1.UserRef{
				Email: estimateCustomerReply.GetCustomer().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: validateProductReviewReply.GetProductReview().GetAudit().GetUpdatedBy(),
			},
			CustomerId: req.GetCustomerId(),
			Name:       req.GetCustomer().GetName(),
		},
		Outcome: &customersv1.LintCustomersResponse_Outcome{
			Status:  explainOrderAmendmentDecisionReply.GetOutcome().GetStatus(),
			Message: resolveFulfillmentsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: resolveFulfillmentsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// GetCustomerRules returns a customer.
func (s *CustomerValidationServer) GetCustomerRules(ctx context.Context, req *customersv1.GetCustomerRulesRequest) (*customersv1.GetCustomerRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer", req.GetCustomerId(), "get_customer_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get customer rules", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *customersv1.ExplainHouseholdCalculationRequest, ...grpc.CallOption) (*customersv1.ExplainHouseholdCalculationResponse, error){
		"primary":  s.householdCalculationClient.ExplainHouseholdCalculation,
		"fallback": s.householdCalculationClientFallback.ExplainHouseholdCalculation,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	explainHouseholdCalculationRequest := &customersv1.ExplainHouseholdCalculationRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						Email:  tenancy.Value(ctx, "email"),
						UserId: req.GetContext().GetActor().GetUserId(),
					},
					ParentId: req.GetCustomerId(),
				}},
			}},
		},
	}
	explainHouseholdCalculationReply, err := route(ctx, explainHouseholdCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/ExplainHouseholdCalculation")
	}
	return &customersv1.GetCustomerRulesResponse{
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				Etag: explainHouseholdCalculationReply.GetHousehold().GetAudit().GetEtag(),
			},
			CustomerId: req.GetCustomerId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// customerCalculationLazy dials acme.customers.v1.CustomerCalculationService the first time a handler needs it.
func (s *CustomerValidationServer) customerCalculationLazy() customersv1.CustomerCalculationServiceClient {
	s.customerCalculationOnce.Do(func() { s.customerCalculationCached = customersv1.NewCustomerCalculationServiceClient(s.conn) })
	return s.customerCalculationCached
}
