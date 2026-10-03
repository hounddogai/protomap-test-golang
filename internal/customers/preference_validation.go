package customers

import (
	"context"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	lendingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/lending/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	personalizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/personalization/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PreferenceValidationServer implements `acme.customers.v1.PreferenceValidationService`.
// Checks preferences against the rules before anything acts on them.
type PreferenceValidationServer struct {
	*preferenceValidationHandlers
}

// preferenceValidationHandlers holds the handlers of PreferenceValidationServer, with what they use.
type preferenceValidationHandlers struct {
	customersv1.UnimplementedPreferenceValidationServiceServer
	store                           *Store
	log                             *slog.Logger
	customerValidationClient        customersv1.CustomerValidationServiceClient
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	conn                            *grpc.ClientConn
	householdValidationClient       customersv1.HouseholdValidationServiceClient
	customerCalculationClient       customersv1.CustomerCalculationServiceClient
	customerQueryClient             customersv1.CustomerQueryServiceClient
	userValidationPort              userValidationPort
	loanApplicationQueryClient      lendingv1.LoanApplicationQueryServiceClient
	consentRecordCalculationClient  customersv1.ConsentRecordCalculationServiceClient
	householdCalculationClient      customersv1.HouseholdCalculationServiceClient
	preferenceCalculationClient     customersv1.PreferenceCalculationServiceClient
}

// NewPreferenceValidationServer wires a PreferenceValidationServer.
func NewPreferenceValidationServer(deps platform.Deps) *PreferenceValidationServer {
	return &PreferenceValidationServer{preferenceValidationHandlers: &preferenceValidationHandlers{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.customers.v1.PreferenceValidationService"),
		customerValidationClient:        customersv1.NewCustomerValidationServiceClient(deps.Conn),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		householdValidationClient:       customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		customerCalculationClient:       customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		customerQueryClient:             customersv1.NewCustomerQueryServiceClient(deps.Conn),
		userValidationPort:              newUserValidationPort(deps),
		loanApplicationQueryClient:      lendingv1.NewLoanApplicationQueryServiceClient(deps.Conn),
		consentRecordCalculationClient:  customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		householdCalculationClient:      customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		preferenceCalculationClient:     customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
	}}
}

// ValidatePreference validates a preference.
func (s *preferenceValidationHandlers) ValidatePreference(ctx context.Context, req *customersv1.ValidatePreferenceRequest) (*customersv1.ValidatePreferenceResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "validate preference", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*customersv1.ValidatePreferenceRequest_Details)
	validateCustomerRequest := newValidateCustomerRequest(withValidateCustomerContext(req.GetContext()), withValidateCustomerCustomer(&customersv1.Customer{
		Contact: &commonv1.ContactInfo{
			Phone: req.GetDetails().GetContact().GetPhone(),
			Email: cloned.GetContact().GetEmail(),
			Name:  req.GetDetails().GetContact().GetName(),
		},
	}))
	validateCustomerReply, err := s.customerValidationClient.ValidateCustomer(ctx, validateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ValidateCustomer")
	}
	lintCustomerSegmentsRequest := &customersv1.LintCustomerSegmentsRequest{
		Context: req.GetContext(),
		Details: &customersv1.LintCustomerSegmentsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{Reason: req.GetPreference().GetBlock().GetReason()}},
		},
	}
	lintCustomerSegmentsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*customersv1.LintCustomerSegmentsResponse, error) {
		return s.customerSegmentValidationClient.LintCustomerSegments(ctx, lintCustomerSegmentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	normalizePaymentRequest := &paymentsv1.NormalizePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Lines: []*paymentsv1.PaymentLine{&paymentsv1.PaymentLine{
				Amount: lintCustomerSegmentsReply.GetCustomerSegment().GetDiscount().GetAmount(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag: validateCustomerReply.GetCustomer().GetAudit().GetEtag(),
			},
		},
	}
	normalizePaymentReply := new(paymentsv1.NormalizePaymentResponse)
	if err := grpcx.Invoke(ctx, s.conn, paymentsv1.PaymentValidationService_NormalizePayment_FullMethodName, normalizePaymentRequest, normalizePaymentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/NormalizePayment")
	}
	checkHouseholdEligibilityRequest := buildCheckHouseholdEligibilityRequestForValidatePreference(req, validateCustomerReply, lintCustomerSegmentsReply, normalizePaymentReply)
	checkHouseholdEligibilityReply, err := s.householdValidationClient.CheckHouseholdEligibility(ctx, checkHouseholdEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/CheckHouseholdEligibility")
	}
	_ = checkHouseholdEligibilityReply
	return &customersv1.ValidatePreferenceResponse{
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:  validateCustomerReply.GetCustomer().GetContact().GetEmail(),
				UserId: validateCustomerReply.GetCustomer().GetRelationshipManager().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
				Etag:      normalizePaymentReply.GetPayment().GetAudit().GetEtag(),
			},
			PreferenceId: req.GetPreferenceId(),
			Priority:     req.GetPreference().GetPriority(),
		},
		Result:  &commonv1.OperationResult{Status: lintCustomerSegmentsReply.GetResult().GetStatus()},
		Outcome: &customersv1.ValidatePreferenceResponse_Outcome{Status: normalizePaymentReply.GetOutcome().GetStatus()},
	}, nil
}

// PreviewPreference previews a preference.
func (s *preferenceValidationHandlers) PreviewPreference(ctx context.Context, req *customersv1.PreviewPreferenceRequest) (*customersv1.PreviewPreferenceResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewPreference", "error", err)
		return nil, err
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "preview preference", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	step := &preferenceValidationHandlersPreviewPreferenceQuoteCustomerStep{request: &customersv1.QuoteCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetPreference().GetOwner().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}}
	if err := s.runPreviewPreferenceQuoteCustomer(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/QuoteCustomer")
	}
	quoteCustomerReply := step.reply
	return &customersv1.PreviewPreferenceResponse{
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				UserId: quoteCustomerReply.GetCustomer().GetRelationshipManager().GetUserId(),
			},
			PreferenceId: req.GetPreferenceId(),
			Effective:    req.GetPreference().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckPreferenceEligibility checks a preference.
func (s *preferenceValidationHandlers) CheckPreferenceEligibility(ctx context.Context, req *customersv1.CheckPreferenceEligibilityRequest) (*customersv1.CheckPreferenceEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckPreferenceEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "check preference eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	source := req.GetPreference().GetOwner().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	ctx = tenancy.With(ctx, "email", req.GetPreference().GetOwner().GetEmail())
	estimateCustomerRequest := &customersv1.EstimateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: displayName,
				Email:       tenancy.Value(ctx, "email"),
			},
		},
	}
	estimateCustomerReply := new(customersv1.EstimateCustomerResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.CustomerCalculationService_EstimateCustomer_FullMethodName, estimateCustomerRequest, estimateCustomerReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/EstimateCustomer")
	}
	ctx = tenancy.With(ctx, "reason", req.GetPreference().GetBlock().GetReason())
	summarizeCustomersRequest := &customersv1.SummarizeCustomersRequest{
		Context:    req.GetContext(),
		Reason:     tenancy.Value(ctx, "reason"),
		CustomerId: estimateCustomerReply.GetCustomer().GetCustomerId(),
	}
	summarizeCustomersReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*customersv1.SummarizeCustomersResponse, error) {
		return s.customerQueryClient.SummarizeCustomers(ctx, summarizeCustomersRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SummarizeCustomers")
	}
	return &customersv1.CheckPreferenceEligibilityResponse{
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				Etag: estimateCustomerReply.GetCustomer().GetAudit().GetEtag(),
			},
			PreferenceId: req.GetPreferenceId(),
			Name:         req.GetPreference().GetName(),
		},
		Outcome: &customersv1.CheckPreferenceEligibilityResponse_Outcome{Message: summarizeCustomersReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Message: summarizeCustomersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// NormalizePreference normalizes a preference.
func (s *preferenceValidationHandlers) NormalizePreference(ctx context.Context, req *customersv1.NormalizePreferenceRequest) (*customersv1.NormalizePreferenceResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "normalize preference", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	validateCustomerSegmentRequest := buildValidateCustomerSegmentRequestForNormalizePreference(req)
	validateCustomerSegmentReply, err := s.customerSegmentValidationClient.ValidateCustomerSegment(ctx, validateCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment")
	}
	cloned := proto.Clone(req.GetDetails()).(*customersv1.NormalizePreferenceRequest_Details)
	checkUserEligibilityRequest := &identityv1.CheckUserEligibilityRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Email: cloned.GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: validateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
			},
		},
	}
	checkUserEligibilityReply, err := s.userValidationPort.CheckUserEligibility(ctx, checkUserEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/CheckUserEligibility")
	}
	browseLoanApplicationsRequest := buildBrowseLoanApplicationsRequestForNormalizePreference(req, validateCustomerSegmentReply, checkUserEligibilityReply)
	browseLoanApplicationsReply, err := s.loanApplicationQueryClient.BrowseLoanApplications(ctx, browseLoanApplicationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.lending.v1.LoanApplicationQueryService/BrowseLoanApplications")
	}
	scoreHouseholdRequest := &customersv1.ScoreHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{CreatedAt: req.GetPreference().GetAudit().GetCreatedAt()},
				}},
			}},
		},
	}
	scoreHouseholdReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*customersv1.ScoreHouseholdResponse, error) {
		return s.householdValidationClient.ScoreHousehold(ctx, scoreHouseholdRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ScoreHousehold")
	}
	return &customersv1.NormalizePreferenceResponse{
		Outcome: &customersv1.NormalizePreferenceResponse_Outcome{
			ReferenceId: validateCustomerSegmentReply.GetOutcome().GetReferenceId(),
			Message:     browseLoanApplicationsReply.GetResult().GetMessage(),
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scoreHouseholdReply.GetHousehold().GetAudit().GetUpdatedBy(),
			},
			PreferenceId: req.GetPreferenceId(),
			Parameters:   req.GetPreference().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: scoreHouseholdReply.GetResult().GetStatus()},
	}, nil
}

// ExplainPreferenceDecision explains a preference.
func (s *preferenceValidationHandlers) ExplainPreferenceDecision(ctx context.Context, req *customersv1.ExplainPreferenceDecisionRequest) (*customersv1.ExplainPreferenceDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePreference(ctx, req.GetContext().GetTenantId(), req.GetPreference()); err != nil {
		return nil, grpcx.Wrap(err, "save preference")
	}
	s.log.InfoContext(ctx, "explain preference decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPreference().GetAudit().GetEtag())
	source := req.GetPreference().GetOwner().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	simulateProfileRequest := &personalizationv1.SimulateProfileRequest{
		Context: req.GetContext(),
		Profile: &personalizationv1.Profile{RelationshipManager: &identityv1.UserRef{Email: email}},
	}
	simulateProfileReply := new(personalizationv1.SimulateProfileResponse)
	if err := s.conn.Invoke(ctx, personalizationProfileCalculationServiceSimulateProfileMethod, simulateProfileRequest, simulateProfileReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.personalization.v1.ProfileCalculationService/SimulateProfile")
	}
	packed, err := anypb.New(req.GetPreference())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareConsentRecordOptionsReply, err := s.consentRecordCalculationClient.CompareConsentRecordOptions(ctx, &customersv1.CompareConsentRecordOptionsRequest{
		Context: req.GetContext(),
		Details: &customersv1.CompareConsentRecordOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality:    simulateProfileReply.GetProfile().GetPrimaryAddress().GetLocality(),
				CountryCode: simulateProfileReply.GetProfile().GetPrimaryAddress().GetCountryCode(),
				Region:      simulateProfileReply.GetProfile().GetPrimaryAddress().GetRegion(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CompareConsentRecordOptions")
	}
	previewBrandRequest := &catalogv1.PreviewBrandRequest{
		Context: req.GetContext(),
		Details: &catalogv1.PreviewBrandRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: simulateProfileReply.GetProfile().GetRelationshipManager().GetEmail(),
			},
		},
		Brand: &catalogv1.Brand{
			IdentityDocument: &catalogv1.Brand_NationalId{NationalId: &catalogv1.BrandNationalId{
				Number: simulateProfileReply.GetProfile().GetNationalId().GetNumber(),
			}},
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: simulateProfileReply.GetProfile().GetPrimaryAddress().GetPostalCode(),
			},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	previewBrandReply, err := previewBrandCall(ctx, previewBrandRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandValidationService/PreviewBrand")
	}
	recalculateCustomersReply, err := s.customerCalculationClient.RecalculateCustomers(ctx, &customersv1.RecalculateCustomersRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			IdentityDocument: &customersv1.Customer_NationalId{NationalId: &customersv1.CustomerNationalId{
				Number: previewBrandReply.GetBrand().GetNationalId().GetNumber(),
			}},
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
		Details: &customersv1.RecalculateCustomersRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines: simulateProfileReply.GetProfile().GetPrimaryAddress().GetLines(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	return &customersv1.ExplainPreferenceDecisionResponse{
		Result: &commonv1.OperationResult{
			Details: simulateProfileReply.GetResult().GetDetails(),
			Status:  compareConsentRecordOptionsReply.GetResult().GetStatus(),
		},
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: compareConsentRecordOptionsReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: previewBrandReply.GetBrand().GetAudit().GetUpdatedBy(),
				CreatedAt: recalculateCustomersReply.GetCustomer().GetAudit().GetCreatedAt(),
			},
			PreferenceId: req.GetPreferenceId(),
			Effective:    req.GetPreference().GetEffective(),
		},
	}, nil
}

// GetPreferenceRules returns a preference.
func (s *preferenceValidationHandlers) GetPreferenceRules(ctx context.Context, req *customersv1.GetPreferenceRulesRequest) (*customersv1.GetPreferenceRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "get_preference_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get preference rules", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	recalculateHouseholdsReply, err := s.householdCalculationClient.RecalculateHouseholds(ctx, &customersv1.RecalculateHouseholdsRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
				}},
			}},
			Manager: &identityv1.UserRef{Email: (&decoded).GetActor().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/RecalculateHouseholds")
	}
	recalculatePreferencesRequest := &customersv1.RecalculatePreferencesRequest{
		Context: req.GetContext(),
		Details: &customersv1.RecalculatePreferencesRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode:  recalculateHouseholdsReply.GetHousehold().GetLocation().GetPostalCode(),
				Recipient:   recalculateHouseholdsReply.GetHousehold().GetLocation().GetRecipient(),
				CountryCode: recalculateHouseholdsReply.GetHousehold().GetLocation().GetCountryCode(),
			},
		},
	}
	type recalculatePreferencesResult struct {
		reply *customersv1.RecalculatePreferencesResponse
		err   error
	}
	recalculatePreferencesResults := make(chan recalculatePreferencesResult, 1)
	go func() {
		reply, err := s.preferenceCalculationClient.RecalculatePreferences(ctx, recalculatePreferencesRequest)
		recalculatePreferencesResults <- recalculatePreferencesResult{reply, err}
	}()
	var recalculatePreferencesReply *customersv1.RecalculatePreferencesResponse
	select {
	case result := <-recalculatePreferencesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.PreferenceCalculationService/RecalculatePreferences")
		}
		recalculatePreferencesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &customersv1.GetPreferenceRulesResponse{
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: recalculateHouseholdsReply.GetHousehold().GetManager().GetDisplayName(),
			},
			Version:      recalculatePreferencesReply.GetPreference().GetVersion(),
			PreferenceId: req.GetPreferenceId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// preferenceValidationHandlersPreviewPreferenceQuoteCustomerStep carries the request of one step of PreviewPreference
// and, once it ran, its reply.
type preferenceValidationHandlersPreviewPreferenceQuoteCustomerStep struct {
	request *customersv1.QuoteCustomerRequest
	reply   *customersv1.QuoteCustomerResponse
}

func (s *preferenceValidationHandlers) runPreviewPreferenceQuoteCustomer(ctx context.Context, step *preferenceValidationHandlersPreviewPreferenceQuoteCustomerStep) error {
	reply, err := s.customerCalculationClient.QuoteCustomer(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
