package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	forecastingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forecasting/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	keymanagementv1 "github.com/hounddogai/protomap-test-golang/gen/acme/keymanagement/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CustomerServer implements `acme.customers.v1.CustomerService`.
// Manages customers: creates, reads, changes, and lists them.
type CustomerServer struct {
	customersv1.UnimplementedCustomerServiceServer
	store                             *Store
	log                               *slog.Logger
	demandForecastClient              forecastingv1.DemandForecastServiceClient
	subscriptionPlanValidationClient  subscriptionsv1.SubscriptionPlanValidationServiceClient
	customerSegmentCalculationClient  customersv1.CustomerSegmentCalculationServiceClient
	householdQueryClient              customersv1.HouseholdQueryServiceClient
	synonymValidationClient           searchv1.SynonymValidationServiceClient
	conn                              *grpc.ClientConn
	cryptoKeyQueryOnce                sync.Once
	cryptoKeyQueryCached              keymanagementv1.CryptoKeyQueryServiceClient
	householdCalculationClient        customersv1.HouseholdCalculationServiceClient
	manifestValidationClient          shippingv1.ManifestValidationServiceClient
	meteredCustomerSegmentCalculation meteredCustomerSegmentCalculation
	customerQueryClient               customersv1.CustomerQueryServiceClient
	preferenceCalculationOnce         sync.Once
	preferenceCalculationCached       customersv1.PreferenceCalculationServiceClient
	preferenceValidationClient        customersv1.PreferenceValidationServiceClient
	preferenceCalculationClient       customersv1.PreferenceCalculationServiceClient
	consentRecordCalculationClient    customersv1.ConsentRecordCalculationServiceClient
}

// NewCustomerServer wires a CustomerServer.
func NewCustomerServer(deps platform.Deps) *CustomerServer {
	return &CustomerServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.customers.v1.CustomerService"),
		demandForecastClient:              forecastingv1.NewDemandForecastServiceClient(deps.Conn),
		subscriptionPlanValidationClient:  subscriptionsv1.NewSubscriptionPlanValidationServiceClient(deps.Conn),
		customerSegmentCalculationClient:  customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		householdQueryClient:              customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		synonymValidationClient:           searchv1.NewSynonymValidationServiceClient(deps.Conn),
		conn:                              deps.Conn,
		householdCalculationClient:        customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		manifestValidationClient:          shippingv1.NewManifestValidationServiceClient(deps.Conn),
		meteredCustomerSegmentCalculation: meteredCustomerSegmentCalculation{CustomerSegmentCalculationServiceClient: customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		customerQueryClient:               customersv1.NewCustomerQueryServiceClient(deps.Conn),
		preferenceValidationClient:        customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		preferenceCalculationClient:       customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		consentRecordCalculationClient:    customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
	}
}

// CreateCustomer creates a customer.
func (s *CustomerServer) CreateCustomer(ctx context.Context, req *customersv1.CreateCustomerRequest) (*customersv1.CreateCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CreateCustomer", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "create customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	createDemandForecastRequest := newCreateDemandForecastRequest(withCreateDemandForecastContext(req.GetContext()), withCreateDemandForecastDemandForecast(&forecastingv1.DemandForecast{
		BillingAddress: &commonv1.PostalAddress{
			CountryCode: req.GetCustomer().GetPrimaryAddress().GetCountryCode(),
			Recipient:   req.GetCustomer().GetPrimaryAddress().GetRecipient(),
		},
		Audit: &commonv1.AuditInfo{Etag: req.GetCustomer().GetAudit().GetEtag()},
		Counterparty: &forecastingv1.ScenarioRef{
			DisplayName: req.GetCustomer().GetRelationshipManager().GetDisplayName(),
		},
	}))
	createDemandForecastReply, err := s.demandForecastClient.CreateDemandForecast(ctx, createDemandForecastRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forecasting.v1.DemandForecastService/CreateDemandForecast")
	}
	return &customersv1.CreateCustomerResponse{
		Result: &commonv1.OperationResult{Status: createDemandForecastReply.GetResult().GetStatus()},
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Region: createDemandForecastReply.GetDemandForecast().GetBillingAddress().GetRegion(),
			},
			Attributes: req.GetCustomer().GetAttributes(),
			CustomerId: req.GetCustomer().GetCustomerId(),
		},
	}, nil
}

// GetCustomer returns a customer.
func (s *CustomerServer) GetCustomer(ctx context.Context, req *customersv1.GetCustomerRequest) (*customersv1.GetCustomerResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer", req.GetCustomerId(), "get_customer"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get customer", "tenant", req.GetContext().GetTenantId())
	return &customersv1.GetCustomerResponse{
		Customer: &customersv1.Customer{CustomerId: req.GetCustomerId()},
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UpdateCustomer changes a customer.
func (s *CustomerServer) UpdateCustomer(ctx context.Context, req *customersv1.UpdateCustomerRequest) (*customersv1.UpdateCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected UpdateCustomer", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "update customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	scoreSubscriptionPlanRequest := &subscriptionsv1.ScoreSubscriptionPlanRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.ScoreSubscriptionPlanRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: req.GetCustomer().GetPrimaryAddress().GetLocality(),
				Region:   req.GetCustomer().GetPrimaryAddress().GetRegion(),
			},
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetCustomer().GetAudit().GetUpdatedBy()},
		},
	}
	scoreSubscriptionPlanReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*subscriptionsv1.ScoreSubscriptionPlanResponse, error) {
		return s.subscriptionPlanValidationClient.ScoreSubscriptionPlan(ctx, scoreSubscriptionPlanRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/ScoreSubscriptionPlan")
	}
	recalculateCustomerSegmentsRequest := &customersv1.RecalculateCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{
				NotifyEmail: scoreSubscriptionPlanReply.GetSubscriptionPlan().GetBlock().GetNotifyEmail(),
			}},
		},
	}
	var recalculateCustomerSegmentsReply *customersv1.RecalculateCustomerSegmentsResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments", func(ctx context.Context) error {
		answer, err := s.customerSegmentCalculationClient.RecalculateCustomerSegments(ctx, recalculateCustomerSegmentsRequest)
		recalculateCustomerSegmentsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments")
	}
	calculateCustomerSegmentReply, err := s.customerSegmentCalculationClient.CalculateCustomerSegment(ctx, &customersv1.CalculateCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				UserId: recalculateCustomerSegmentsReply.GetCustomerSegment().GetOwner().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scoreSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &customersv1.CalculateCustomerSegmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: req.GetCustomer().GetPrimaryAddress().GetRecipient(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/CalculateCustomerSegment")
	}
	exportHouseholdsStream, err := s.householdQueryClient.ExportHouseholds(ctx, &customersv1.ExportHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ExportHouseholdsRequest_Filter{
			Region:       req.GetCustomer().GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetCustomer().GetPassport().GetExpiresAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ExportHouseholds")
	}
	for {
		received, err := exportHouseholdsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ExportHouseholds")
		}
		_ = received
	}
	return &customersv1.UpdateCustomerResponse{
		Result: &commonv1.OperationResult{
			Status:  scoreSubscriptionPlanReply.GetResult().GetStatus(),
			Details: scoreSubscriptionPlanReply.GetResult().GetDetails(),
		},
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{
				Email: recalculateCustomerSegmentsReply.GetCustomerSegment().GetOwner().GetEmail(),
				Name:  req.GetCustomer().GetName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: calculateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetEtag(),
			},
			CustomerId: req.GetCustomer().GetCustomerId(),
		},
	}, nil
}

// ListCustomers lists customers a page at a time.
func (s *CustomerServer) ListCustomers(ctx context.Context, req *customersv1.ListCustomersRequest) (*customersv1.ListCustomersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list customers", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := customerServerCheckSynonymEligibilitySteps[mode]
	checkSynonymEligibilityRequest := &searchv1.CheckSynonymEligibilityRequest{
		Context: req.GetContext(),
		Details: &searchv1.CheckSynonymEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	checkSynonymEligibilityReply, err := run(s, ctx, checkSynonymEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/CheckSynonymEligibility")
	}
	return &customersv1.ListCustomersResponse{
		Customers: []*customersv1.Customer{&customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: checkSynonymEligibilityReply.GetSynonym().GetAudit().GetUpdatedAt(),
			},
			RelationshipManager: &identityv1.UserRef{
				Email: checkSynonymEligibilityReply.GetSynonym().GetOwner().GetEmail(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BatchGetCustomers returns several customers at once.
func (s *CustomerServer) BatchGetCustomers(ctx context.Context, req *customersv1.BatchGetCustomersRequest) (*customersv1.BatchGetCustomersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BatchGetCustomers", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "batch get customers", "tenant", req.GetContext().GetTenantId())
	return &customersv1.BatchGetCustomersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SearchCustomers lists customers a page at a time.
func (s *CustomerServer) SearchCustomers(ctx context.Context, req *customersv1.SearchCustomersRequest) (*customersv1.SearchCustomersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "search customers", "tenant", req.GetContext().GetTenantId())
	resolveCryptoKeysRequest := &keymanagementv1.ResolveCryptoKeysRequest{Context: req.GetContext()}
	resolveCryptoKeysReply, err := s.cryptoKeyQueryLazy().ResolveCryptoKeys(ctx, resolveCryptoKeysRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.keymanagement.v1.CryptoKeyQueryService/ResolveCryptoKeys")
	}
	step := &customerServerSearchCustomersSimulateHouseholdStep{request: &customersv1.SimulateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
					Manager:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
				}},
			}},
		},
	}}
	if err := s.runSearchCustomersSimulateHousehold(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/SimulateHousehold")
	}
	simulateHouseholdReply := step.reply
	return &customersv1.SearchCustomersResponse{
		PageInfo: &commonv1.PageInfo{
			TotalSize: resolveCryptoKeysReply.GetPageInfo().GetTotalSize(),
		},
		Customers: []*customersv1.Customer{&customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				Etag: simulateHouseholdReply.GetHousehold().GetAudit().GetEtag(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: simulateHouseholdReply.GetHousehold().GetLocation().GetPostalCode(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// WatchCustomers streams the changes of customers as they happen.
func (s *CustomerServer) WatchCustomers(req *customersv1.WatchCustomersRequest, stream grpc.ServerStreamingServer[customersv1.WatchCustomersResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "watch customers", "tenant", req.GetContext().GetTenantId())
	scoreManifestReply, err := s.manifestValidationClient.ScoreManifest(ctx, &shippingv1.ScoreManifestRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Counterparty: &customersv1.CustomerRef{Email: req.GetSubscriber().GetEmail()},
			Audit:        &commonv1.AuditInfo{CreatedAt: req.GetSince()},
		},
		Details: &shippingv1.ScoreManifestRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.shipping.v1.ManifestValidationService/ScoreManifest")
	}
	explainCustomerSegmentCalculationRequest := &customersv1.ExplainCustomerSegmentCalculationRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				Etag:      scoreManifestReply.GetManifest().GetAudit().GetEtag(),
				UpdatedBy: scoreManifestReply.GetManifest().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &customersv1.ExplainCustomerSegmentCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetSubscriber().GetEmail(),
				Name:  req.GetSubscriber().GetName(),
			},
		},
	}
	explainCustomerSegmentCalculationReply, err := s.meteredCustomerSegmentCalculation.ExplainCustomerSegmentCalculation(ctx, explainCustomerSegmentCalculationRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/ExplainCustomerSegmentCalculation")
	}
	summarizeCustomersRequest := &customersv1.SummarizeCustomersRequest{
		Context:    req.GetContext(),
		Reason:     explainCustomerSegmentCalculationReply.GetCustomerSegment().GetBlock().GetReason(),
		CustomerId: scoreManifestReply.GetManifest().GetCounterparty().GetCustomerId(),
	}
	summarizeCustomersReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*customersv1.SummarizeCustomersResponse, error) {
		return s.customerQueryClient.SummarizeCustomers(ctx, summarizeCustomersRequest)
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SummarizeCustomers")
	}
	comparePreferenceOptionsRequest := &customersv1.ComparePreferenceOptionsRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scoreManifestReply.GetManifest().GetAudit().GetUpdatedAt(),
			},
		},
	}
	comparePreferenceOptionsReply, err := s.preferenceCalculationLazy().ComparePreferenceOptions(ctx, comparePreferenceOptionsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions")
	}
	if err := stream.Send(&customersv1.WatchCustomersResponse{
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: scoreManifestReply.GetManifest().GetBillingAddress().GetLocality(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scoreManifestReply.GetManifest().GetAudit().GetUpdatedBy(),
			},
			Contact: &commonv1.ContactInfo{
				Phone: req.GetSubscriber().GetPhone(),
				Name:  req.GetSubscriber().GetName(),
			},
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  explainCustomerSegmentCalculationReply.GetResult().GetReferenceIds(),
			Details:       summarizeCustomersReply.GetResult().GetDetails(),
			CorrelationId: summarizeCustomersReply.GetResult().GetCorrelationId(),
			Status:        comparePreferenceOptionsReply.GetResult().GetStatus(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreateCustomers takes a stream of customers and answers once at the end.
func (s *CustomerServer) BatchCreateCustomers(stream grpc.ClientStreamingServer[customersv1.BatchCreateCustomersRequest, customersv1.BatchCreateCustomersResponse]) error {
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
		s.log.InfoContext(ctx, "batch create customers", "tenant", msg.GetContext().GetTenantId())
		previewPreferenceRequest := &customersv1.PreviewPreferenceRequest{
			Context: msg.GetContext(),
			Preference: &customersv1.Preference{
				Audit: &commonv1.AuditInfo{
					CreatedBy: msg.GetRecord().GetAudit().GetCreatedBy(),
					UpdatedBy: msg.GetRecord().GetAudit().GetUpdatedBy(),
				},
			},
		}
		if _, err := grpcx.Call(ctx, s.preferenceValidationClient.PreviewPreference, previewPreferenceRequest); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
		}
		if _, err := s.preferenceCalculationClient.ComparePreferenceOptions(ctx, &customersv1.ComparePreferenceOptionsRequest{Context: msg.GetContext()}); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions")
		}
		accepted++
	}
	return stream.SendAndClose(&customersv1.BatchCreateCustomersResponse{Accepted: accepted})
}

// CloneCustomer creates a customer.
func (s *CustomerServer) CloneCustomer(ctx context.Context, req *customersv1.CloneCustomerRequest) (*customersv1.CloneCustomerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomer(ctx, req.GetContext().GetTenantId(), req.GetCustomer()); err != nil {
		return nil, grpcx.Wrap(err, "save customer")
	}
	s.log.InfoContext(ctx, "clone customer", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomer().GetAudit().GetEtag())
	quoteConsentRecordReply, err := s.consentRecordCalculationClient.QuoteConsentRecord(ctx, &customersv1.QuoteConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.QuoteConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetCustomer().GetPrimaryAddress().GetPostalCode(),
				Recipient:  req.GetCustomer().GetPrimaryAddress().GetRecipient(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord")
	}
	return &customersv1.CloneCustomerResponse{
		Customer: &customersv1.Customer{
			Attributes: quoteConsentRecordReply.GetConsentRecord().GetAttributes(),
			Name:       req.GetCustomer().GetName(),
			Status:     req.GetCustomer().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: quoteConsentRecordReply.GetResult().GetStatus()},
	}, nil
}

// customerServerCheckSynonymEligibilitySteps are the ways to run
// acme.search.v1.SynonymValidationService/CheckSynonymEligibility, by name.
var customerServerCheckSynonymEligibilitySteps = map[string]func(*CustomerServer, context.Context, *searchv1.CheckSynonymEligibilityRequest) (*searchv1.CheckSynonymEligibilityResponse, error){
	"default": (*CustomerServer).checkSynonymEligibilityStep,
	"audited": (*CustomerServer).checkSynonymEligibilityAuditedStep,
}

func (s *CustomerServer) checkSynonymEligibilityStep(ctx context.Context, request *searchv1.CheckSynonymEligibilityRequest) (*searchv1.CheckSynonymEligibilityResponse, error) {
	return s.synonymValidationClient.CheckSynonymEligibility(ctx, request)
}

func (s *CustomerServer) checkSynonymEligibilityAuditedStep(ctx context.Context, request *searchv1.CheckSynonymEligibilityRequest) (*searchv1.CheckSynonymEligibilityResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.search.v1.SynonymValidationService/CheckSynonymEligibility")
	return s.synonymValidationClient.CheckSynonymEligibility(ctx, request)
}

// cryptoKeyQueryLazy dials acme.keymanagement.v1.CryptoKeyQueryService the first time a handler needs it.
func (s *CustomerServer) cryptoKeyQueryLazy() keymanagementv1.CryptoKeyQueryServiceClient {
	s.cryptoKeyQueryOnce.Do(func() { s.cryptoKeyQueryCached = keymanagementv1.NewCryptoKeyQueryServiceClient(s.conn) })
	return s.cryptoKeyQueryCached
}

// customerServerSearchCustomersSimulateHouseholdStep carries the request of one step of SearchCustomers and, once it
// ran, its reply.
type customerServerSearchCustomersSimulateHouseholdStep struct {
	request *customersv1.SimulateHouseholdRequest
	reply   *customersv1.SimulateHouseholdResponse
}

func (s *CustomerServer) runSearchCustomersSimulateHousehold(ctx context.Context, step *customerServerSearchCustomersSimulateHouseholdStep) error {
	reply, err := s.householdCalculationClient.SimulateHousehold(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// preferenceCalculationLazy dials acme.customers.v1.PreferenceCalculationService the first time a handler needs it.
func (s *CustomerServer) preferenceCalculationLazy() customersv1.PreferenceCalculationServiceClient {
	s.preferenceCalculationOnce.Do(func() { s.preferenceCalculationCached = customersv1.NewPreferenceCalculationServiceClient(s.conn) })
	return s.preferenceCalculationCached
}
