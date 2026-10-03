package fraud

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// FraudSignalCalculationServer implements `acme.fraud.v1.FraudSignalCalculationService`.
// Computes the figures of a fraud signal, such as totals, taxes, and estimates.
type FraudSignalCalculationServer struct {
	fraudv1.UnimplementedFraudSignalCalculationServiceServer
	store                           *Store
	log                             *slog.Logger
	subscriptionPlanQueryClient     subscriptionsv1.SubscriptionPlanQueryServiceClient
	conn                            *grpc.ClientConn
	inspectionCalculationClient     returnsv1.InspectionCalculationServiceClient
	pointsLedgerValidationClient    loyaltyv1.PointsLedgerValidationServiceClient
	riskAssessmentCalculationClient fraudv1.RiskAssessmentCalculationServiceClient
	userClient                      identityv1.UserServiceClient
	riskAssessmentCalculationOnce   sync.Once
	riskAssessmentCalculationCached fraudv1.RiskAssessmentCalculationServiceClient
}

// NewFraudSignalCalculationServer wires a FraudSignalCalculationServer.
func NewFraudSignalCalculationServer(deps platform.Deps) *FraudSignalCalculationServer {
	return &FraudSignalCalculationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.fraud.v1.FraudSignalCalculationService"),
		subscriptionPlanQueryClient:     subscriptionsv1.NewSubscriptionPlanQueryServiceClient(deps.Conn),
		conn:                            deps.Conn,
		inspectionCalculationClient:     returnsv1.NewInspectionCalculationServiceClient(deps.Conn),
		pointsLedgerValidationClient:    loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn),
		riskAssessmentCalculationClient: fraudv1.NewRiskAssessmentCalculationServiceClient(deps.Conn),
		userClient:                      identityv1.NewUserServiceClient(deps.Conn),
	}
}

// CalculateFraudSignal calculates a fraud signal.
func (s *FraudSignalCalculationServer) CalculateFraudSignal(ctx context.Context, req *fraudv1.CalculateFraudSignalRequest) (*fraudv1.CalculateFraudSignalResponse, error) {
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "calculate fraud signal", "tenant", req.GetContext().GetTenantId())
	countSubscriptionPlansRequest := &subscriptionsv1.CountSubscriptionPlansRequest{
		Context: req.GetContext(),
		Filter: &subscriptionsv1.CountSubscriptionPlansRequest_Filter{
			Region:  req.GetDetails().GetDestination().GetRegion(),
			OwnerId: req.GetFraudSignal().GetSubject().GetFraudRuleId(),
		},
	}
	type countSubscriptionPlansResult struct {
		reply *subscriptionsv1.CountSubscriptionPlansResponse
		err   error
	}
	countSubscriptionPlansResults := make(chan countSubscriptionPlansResult, 1)
	go func() {
		reply, err := s.subscriptionPlanQueryClient.CountSubscriptionPlans(ctx, countSubscriptionPlansRequest)
		countSubscriptionPlansResults <- countSubscriptionPlansResult{reply, err}
	}()
	var countSubscriptionPlansReply *subscriptionsv1.CountSubscriptionPlansResponse
	select {
	case result := <-countSubscriptionPlansResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.subscriptions.v1.SubscriptionPlanQueryService/CountSubscriptionPlans")
		}
		countSubscriptionPlansReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &fraudv1.CalculateFraudSignalResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: countSubscriptionPlansReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		FraudSignal: &fraudv1.FraudSignal{
			CorrelationId: countSubscriptionPlansReply.GetResult().GetCorrelationId(),
			FraudSignalId: req.GetFraudSignalId(),
		},
		Breakdown: &fraudv1.CalculateFraudSignalResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// EstimateFraudSignal estimates a fraud signal.
func (s *FraudSignalCalculationServer) EstimateFraudSignal(ctx context.Context, req *fraudv1.EstimateFraudSignalRequest) (*fraudv1.EstimateFraudSignalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "estimate fraud signal", "tenant", req.GetContext().GetTenantId())
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recalculateRiskAssessmentsRequest := &fraudv1.RecalculateRiskAssessmentsRequest{
		Context: req.GetContext(),
		Details: &fraudv1.RecalculateRiskAssessmentsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{
				DisplayName: req.GetFraudSignal().GetSubject().GetDisplayName(),
			}},
		},
		RiskAssessmentId: envelope.GetDisplayName(),
	}
	recalculateRiskAssessmentsReply := new(fraudv1.RecalculateRiskAssessmentsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", fraudRiskAssessmentCalculationServiceName, "RecalculateRiskAssessments"), recalculateRiskAssessmentsRequest, recalculateRiskAssessmentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/RecalculateRiskAssessments")
	}
	var envelope2 fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	compareInspectionOptionsRequest := &returnsv1.CompareInspectionOptionsRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				CreatedBy: recalculateRiskAssessmentsReply.GetRiskAssessment().GetAudit().GetCreatedBy(),
				UpdatedBy: recalculateRiskAssessmentsReply.GetRiskAssessment().GetAudit().GetUpdatedBy(),
			},
			Origin: &returnsv1.Inspection_Customer{Customer: &customersv1.CustomerRef{
				CustomerId:  recalculateRiskAssessmentsReply.GetRiskAssessment().GetCustomer().GetCustomerId(),
				DisplayName: recalculateRiskAssessmentsReply.GetRiskAssessment().GetCustomer().GetDisplayName(),
			}},
		},
		InspectionId: envelope2.GetDisplayName(),
	}
	compareInspectionOptionsReply, err := returnsv1.InspectionCalculationServiceClient.CompareInspectionOptions(s.inspectionCalculationClient, ctx, compareInspectionOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/CompareInspectionOptions")
	}
	return &fraudv1.EstimateFraudSignalResponse{
		FraudSignal: &fraudv1.FraudSignal{
			CorrelationId: recalculateRiskAssessmentsReply.GetResult().GetCorrelationId(),
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: compareInspectionOptionsReply.GetInspection().GetCustomer().GetDisplayName(),
			},
			FraudSignalId: req.GetFraudSignalId(),
			Sequence:      req.GetFraudSignal().GetSequence(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteFraudSignal quotes a fraud signal.
func (s *FraudSignalCalculationServer) QuoteFraudSignal(ctx context.Context, req *fraudv1.QuoteFraudSignalRequest) (*fraudv1.QuoteFraudSignalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "quote fraud signal", "tenant", req.GetContext().GetTenantId())
	for measurement := range iterx.Each(req.GetFraudSignal().GetMeasurements(), func(candidate *fraudv1.FraudSignalMeasurement) bool { return candidate != nil }) {
		if _, err := s.pointsLedgerValidationClient.CheckPointsLedgerEligibility(ctx, &loyaltyv1.CheckPointsLedgerEligibilityRequest{
			Context: req.GetContext(),
			PointsLedger: &loyaltyv1.PointsLedger{
				Audit: &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
				Holder: &loyaltyv1.RewardRef{
					DisplayName: req.GetFraudSignal().GetSubject().GetDisplayName(),
					Version:     req.GetFraudSignal().GetSubject().GetVersion(),
				},
				CurrencyCode: req.GetDetails().GetCurrencyCode(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/CheckPointsLedgerEligibility")
		}
	}
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recalculateRiskAssessmentsReply, err := s.riskAssessmentCalculationClient.RecalculateRiskAssessments(ctx, &fraudv1.RecalculateRiskAssessmentsRequest{
		Context: req.GetContext(),
		Details: &fraudv1.RecalculateRiskAssessmentsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
		RiskAssessmentId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/RecalculateRiskAssessments")
	}
	return &fraudv1.QuoteFraudSignalResponse{
		FraudSignal: &fraudv1.FraudSignal{
			CorrelationId: recalculateRiskAssessmentsReply.GetResult().GetCorrelationId(),
			FraudSignalId: req.GetFraudSignalId(),
			Subject:       req.GetFraudSignal().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareFraudSignalOptions compares a fraud signal.
func (s *FraudSignalCalculationServer) CompareFraudSignalOptions(ctx context.Context, req *fraudv1.CompareFraudSignalOptionsRequest) (*fraudv1.CompareFraudSignalOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "compare fraud signal options", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetFraudSignal().GetMeasurements() {
		if _, err := s.riskAssessmentCalculationClient.EstimateRiskAssessment(ctx, &fraudv1.EstimateRiskAssessmentRequest{
			Context: req.GetContext(),
			Details: &fraudv1.EstimateRiskAssessmentRequest_Details{
				Destination: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
			},
			RiskAssessment: &fraudv1.RiskAssessment{
				Audit: &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/EstimateRiskAssessment")
		}
	}
	for measurement2 := range iterx.Each(req.GetFraudSignal().GetMeasurements(), func(candidate *fraudv1.FraudSignalMeasurement) bool { return candidate != nil }) {
		if _, err := s.userClient.CloneUser(ctx, &identityv1.CloneUserRequest{
			Context: req.GetContext(),
			User: &identityv1.User{
				IdentityDocument: &identityv1.User_Passport{Passport: &identityv1.UserPassport{ExpiresAt: measurement2.GetObservedAt()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/CloneUser")
		}
	}
	return &fraudv1.CompareFraudSignalOptionsResponse{
		FraudSignal: &fraudv1.FraudSignal{
			FraudSignalId: req.GetFraudSignalId(),
			Sequence:      req.GetFraudSignal().GetSequence(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculateFraudSignals recalculates a fraud signal.
func (s *FraudSignalCalculationServer) RecalculateFraudSignals(ctx context.Context, req *fraudv1.RecalculateFraudSignalsRequest) (*fraudv1.RecalculateFraudSignalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculateFraudSignals", "error", err)
		return nil, err
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "recalculate fraud signals", "tenant", req.GetContext().GetTenantId())
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainRiskAssessmentCalculationRequest := &fraudv1.ExplainRiskAssessmentCalculationRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetFraudSignal().GetSubject().GetDisplayName(),
			},
		},
		RiskAssessmentId: envelope.GetDisplayName(),
	}
	explainRiskAssessmentCalculationReply, err := s.riskAssessmentCalculationLazy().ExplainRiskAssessmentCalculation(ctx, explainRiskAssessmentCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/ExplainRiskAssessmentCalculation")
	}
	var envelope2 fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	compareRiskAssessmentOptionsRequest := &fraudv1.CompareRiskAssessmentOptionsRequest{
		Context: req.GetContext(),
		Details: &fraudv1.CompareRiskAssessmentOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainRiskAssessmentCalculationReply.GetRiskAssessment().GetAudit().GetUpdatedBy(),
			},
		},
		RiskAssessmentId: envelope2.GetDisplayName(),
	}
	compareRiskAssessmentOptionsReply, err := grpcx.Call(ctx, s.riskAssessmentCalculationClient.CompareRiskAssessmentOptions, compareRiskAssessmentOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/CompareRiskAssessmentOptions")
	}
	return &fraudv1.RecalculateFraudSignalsResponse{
		Result: &commonv1.OperationResult{
			Message: explainRiskAssessmentCalculationReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		FraudSignal: &fraudv1.FraudSignal{
			CorrelationId: explainRiskAssessmentCalculationReply.GetResult().GetCorrelationId(),
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: compareRiskAssessmentOptionsReply.GetRiskAssessment().GetCustomer().GetDisplayName(),
			},
			FraudSignalId: req.GetFraudSignalId(),
			Attributes:    req.GetFraudSignal().GetAttributes(),
		},
	}, nil
}

// SimulateFraudSignal simulates a fraud signal.
func (s *FraudSignalCalculationServer) SimulateFraudSignal(ctx context.Context, req *fraudv1.SimulateFraudSignalRequest) (*fraudv1.SimulateFraudSignalResponse, error) {
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "simulate fraud signal", "tenant", req.GetContext().GetTenantId())
	compareRiskAssessmentOptionsReply, err := s.riskAssessmentCalculationClient.CompareRiskAssessmentOptions(ctx, &fraudv1.CompareRiskAssessmentOptionsRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{
				DisplayName: req.GetFraudSignal().GetSubject().GetDisplayName(),
			}},
		},
		Details: &fraudv1.CompareRiskAssessmentOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode:  req.GetDetails().GetDestination().GetPostalCode(),
				Recipient:   req.GetDetails().GetDestination().GetRecipient(),
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/CompareRiskAssessmentOptions")
	}
	cloned := proto.Clone(req.GetFraudSignal()).(*fraudv1.FraudSignal)
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recalculateRiskAssessmentsRequest := &fraudv1.RecalculateRiskAssessmentsRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{
				DisplayName: cloned.GetSubject().GetDisplayName(),
				Email:       compareRiskAssessmentOptionsReply.GetRiskAssessment().GetCustomer().GetEmail(),
			}},
			Assignee: &identityv1.UserRef{
				Email: compareRiskAssessmentOptionsReply.GetRiskAssessment().GetRequester().GetEmail(),
			},
			Sla: &fraudv1.RiskAssessmentSla{
				Breached: compareRiskAssessmentOptionsReply.GetRiskAssessment().GetSla().GetBreached(),
			},
		},
		RiskAssessmentId: envelope.GetDisplayName(),
	}
	recalculateRiskAssessmentsReply, err := grpcx.Call(ctx, s.riskAssessmentCalculationClient.RecalculateRiskAssessments, recalculateRiskAssessmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/RecalculateRiskAssessments")
	}
	return &fraudv1.SimulateFraudSignalResponse{
		Breakdown: &fraudv1.SimulateFraudSignalResponse_Breakdown{
			Total: compareRiskAssessmentOptionsReply.GetBreakdown().GetTotal(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: compareRiskAssessmentOptionsReply.GetResult().GetReferenceIds(),
			Status:       recalculateRiskAssessmentsReply.GetResult().GetStatus(),
			Message:      recalculateRiskAssessmentsReply.GetResult().GetMessage(),
		},
		FraudSignal: &fraudv1.FraudSignal{
			FraudSignalId: req.GetFraudSignalId(),
			Sequence:      req.GetFraudSignal().GetSequence(),
		},
	}, nil
}

// ExplainFraudSignalCalculation explains a fraud signal.
func (s *FraudSignalCalculationServer) ExplainFraudSignalCalculation(ctx context.Context, req *fraudv1.ExplainFraudSignalCalculationRequest) (*fraudv1.ExplainFraudSignalCalculationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "explain fraud signal calculation", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetContact().GetPreferredChannel())
	simulateRiskAssessmentReply, err := s.riskAssessmentCalculationClient.SimulateRiskAssessment(ctx, &fraudv1.SimulateRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Requester: &commonv1.ContactInfo{PreferredChannel: tenancy.Value(ctx, "preferred_channel")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/SimulateRiskAssessment")
	}
	return &fraudv1.ExplainFraudSignalCalculationResponse{
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: simulateRiskAssessmentReply.GetRiskAssessment().GetCustomer().GetDisplayName(),
			},
			FraudSignalId: req.GetFraudSignalId(),
			Source:        req.GetFraudSignal().GetSource(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetFraudSignalRates returns a fraud signal.
func (s *FraudSignalCalculationServer) GetFraudSignalRates(ctx context.Context, req *fraudv1.GetFraudSignalRatesRequest) (*fraudv1.GetFraudSignalRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetFraudSignalRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_signal", req.GetFraudSignalId(), "get_fraud_signal_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get fraud signal rates", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.GetFraudSignalRatesResponse{
		FraudSignal: &fraudv1.FraudSignal{FraudSignalId: req.GetFraudSignalId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// riskAssessmentCalculationLazy dials acme.fraud.v1.RiskAssessmentCalculationService the first time a handler needs it.
func (s *FraudSignalCalculationServer) riskAssessmentCalculationLazy() fraudv1.RiskAssessmentCalculationServiceClient {
	s.riskAssessmentCalculationOnce.Do(func() { s.riskAssessmentCalculationCached = fraudv1.NewRiskAssessmentCalculationServiceClient(s.conn) })
	return s.riskAssessmentCalculationCached
}
