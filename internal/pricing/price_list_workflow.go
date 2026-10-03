package pricing

import (
	"context"
	"fmt"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PriceListWorkflowServer implements `acme.pricing.v1.PriceListWorkflowService`.
// Runs the long-lived workflow that takes a price list from start to finish.
type PriceListWorkflowServer struct {
	pricingv1.UnsafePriceListWorkflowServiceServer
	store                             *Store
	log                               *slog.Logger
	householdValidationClient         customersv1.HouseholdValidationServiceClient
	householdValidationClientFallback customersv1.HouseholdValidationServiceClient
	preferenceCalculationClient       customersv1.PreferenceCalculationServiceClient
	conn                              *grpc.ClientConn
	manifestSyncClient                shippingv1.ManifestSyncServiceClient
	apiCredentialNotificationClient   identityv1.ApiCredentialNotificationServiceClient
	meteredPriceQuoteCalculation      meteredPriceQuoteCalculation
	consentRecordCalculationClient    customersv1.ConsentRecordCalculationServiceClient
	refundWorkflowClient              paymentsv1.RefundWorkflowServiceClient
	commentPriceQuoteApprovalFn       func(context.Context, *pricingv1.CommentPriceQuoteApprovalRequest, ...grpc.CallOption) (*pricingv1.CommentPriceQuoteApprovalResponse, error)
}

// NewPriceListWorkflowServer wires a PriceListWorkflowServer.
func NewPriceListWorkflowServer(deps platform.Deps) *PriceListWorkflowServer {
	priceQuoteApproval := pricingv1.NewPriceQuoteApprovalServiceClient(deps.Conn)
	return &PriceListWorkflowServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.pricing.v1.PriceListWorkflowService"),
		householdValidationClient:         customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		householdValidationClientFallback: customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		preferenceCalculationClient:       customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		conn:                              deps.Conn,
		manifestSyncClient:                shippingv1.NewManifestSyncServiceClient(deps.Conn),
		apiCredentialNotificationClient:   identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		meteredPriceQuoteCalculation:      meteredPriceQuoteCalculation{PriceQuoteCalculationServiceClient: pricingv1.NewPriceQuoteCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		consentRecordCalculationClient:    customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		refundWorkflowClient:              paymentsv1.NewRefundWorkflowServiceClient(deps.Conn),
		commentPriceQuoteApprovalFn:       priceQuoteApproval.CommentPriceQuoteApproval,
	}
}

// StartPriceListWorkflow starts a price list.
func (s *PriceListWorkflowServer) StartPriceListWorkflow(ctx context.Context, req *pricingv1.StartPriceListWorkflowRequest) (*pricingv1.StartPriceListWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "start price list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *customersv1.CheckHouseholdEligibilityRequest, ...grpc.CallOption) (*customersv1.CheckHouseholdEligibilityResponse, error){
		"primary":  s.householdValidationClient.CheckHouseholdEligibility,
		"fallback": s.householdValidationClientFallback.CheckHouseholdEligibility,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	ctx = tenancy.With(ctx, "user_id", req.GetPriceList().GetOwner().GetUserId())
	var holder struct{ UpdatedBy string }
	if record := req.GetPriceList().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "UpdatedBy"); err != nil {
			s.log.WarnContext(ctx, "copy updated_by", "error", err)
		}
	}
	checkHouseholdEligibilityRequest := &customersv1.CheckHouseholdEligibilityRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit:   &commonv1.AuditInfo{CreatedBy: req.GetPriceList().GetAudit().GetCreatedBy()},
					Manager: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
				}},
				Audit: &commonv1.AuditInfo{
					UpdatedBy: holder.UpdatedBy,
					Etag:      req.GetPriceList().GetAudit().GetEtag(),
				},
			}},
		},
	}
	checkHouseholdEligibilityReply, err := route(ctx, checkHouseholdEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/CheckHouseholdEligibility")
	}
	simulatePreferenceReply, err := s.preferenceCalculationClient.SimulatePreference(ctx, &customersv1.SimulatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				UserId: checkHouseholdEligibilityReply.GetHousehold().GetManager().GetUserId(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/SimulatePreference")
	}
	validateHouseholdRequest := newValidateHouseholdRequest(withValidateHouseholdContext(req.GetContext()), withValidateHouseholdHousehold(&customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPriceList().GetAudit().GetUpdatedBy()},
				Manager: &identityv1.UserRef{
					Email: simulatePreferenceReply.GetPreference().GetOwner().GetEmail(),
				},
			}},
		}},
	}))
	validateHouseholdReply, err := s.householdValidationClient.ValidateHousehold(ctx, validateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ValidateHousehold")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetPriceList().GetOwner().GetDisplayName())
	recalculatePriceQuotesRequest := &pricingv1.RecalculatePriceQuotesRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{
				Tax: &pricingv1.PriceQuoteLineTax{
					Amount: simulatePreferenceReply.GetPreference().GetDiscount().GetAmount(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkHouseholdEligibilityReply.GetHousehold().GetAudit().GetCreatedBy(),
				Etag:      validateHouseholdReply.GetHousehold().GetAudit().GetEtag(),
			},
			Counterparty: &pricingv1.PriceListRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
	}
	recalculatePriceQuotesReply := new(pricingv1.RecalculatePriceQuotesResponse)
	if err := s.conn.Invoke(ctx, pricingPriceQuoteCalculationServiceRecalculatePriceQuotesMethod, recalculatePriceQuotesRequest, recalculatePriceQuotesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/RecalculatePriceQuotes")
	}
	return &pricingv1.StartPriceListWorkflowResponse{
		PriceList: &pricingv1.PriceList{
			Name:        checkHouseholdEligibilityReply.GetHousehold().GetName(),
			Description: simulatePreferenceReply.GetPreference().GetDescription(),
			Version:     recalculatePriceQuotesReply.GetPriceQuote().GetCounterparty().GetVersion(),
			Audit: &commonv1.AuditInfo{
				CreatedBy: recalculatePriceQuotesReply.GetPriceQuote().GetAudit().GetCreatedBy(),
			},
			PriceListId: req.GetPriceListId(),
			Parameters:  req.GetDetails().GetParameters(),
		},
		Outcome: &pricingv1.StartPriceListWorkflowResponse_Outcome{
			Message: checkHouseholdEligibilityReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Details: simulatePreferenceReply.GetResult().GetDetails(),
			Message: validateHouseholdReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// AdvancePriceListWorkflow advances a price list.
func (s *PriceListWorkflowServer) AdvancePriceListWorkflow(ctx context.Context, req *pricingv1.AdvancePriceListWorkflowRequest) (*pricingv1.AdvancePriceListWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "advance price list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	pullManifestsRequest := buildPullManifestsRequestForAdvancePriceListWorkflow(req)
	pullManifestsReply, err := s.manifestSyncClient.PullManifests(ctx, pullManifestsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestSyncService/PullManifests")
	}
	packed, err := anypb.New(req.GetPriceList())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareConsentRecordOptionsRequest := &customersv1.CompareConsentRecordOptionsRequest{
		Context: req.GetContext(),
		Details: &customersv1.CompareConsentRecordOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:     pullManifestsReply.GetManifest().GetBillingAddress().GetLines(),
				Recipient: pullManifestsReply.GetManifest().GetBillingAddress().GetRecipient(),
				Locality:  pullManifestsReply.GetManifest().GetBillingAddress().GetLocality(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{DisplayName: req.GetPriceList().GetOwner().GetDisplayName()},
			Detail:  packed,
		},
	}
	compareConsentRecordOptionsReply := new(customersv1.CompareConsentRecordOptionsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersConsentRecordCalculationServiceName, "CompareConsentRecordOptions"), compareConsentRecordOptionsRequest, compareConsentRecordOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CompareConsentRecordOptions")
	}
	return &pricingv1.AdvancePriceListWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  pullManifestsReply.GetResult().GetReferenceIds(),
			Message:       compareConsentRecordOptionsReply.GetResult().GetMessage(),
			CorrelationId: compareConsentRecordOptionsReply.GetConsentRecord().GetCorrelationId(),
			Status:        "ok",
		},
		PriceList: &pricingv1.PriceList{
			PriceListId: req.GetPriceListId(),
			Version:     req.GetPriceList().GetVersion(),
		},
	}, nil
}

// CancelPriceListWorkflow cancels a price list.
func (s *PriceListWorkflowServer) CancelPriceListWorkflow(ctx context.Context, req *pricingv1.CancelPriceListWorkflowRequest) (*pricingv1.CancelPriceListWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "cancel price list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded pricingv1.CancelPriceListWorkflowRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	ctx = tenancy.With(ctx, "email", req.GetPriceList().GetOwner().GetEmail())
	cloned := proto.Clone(req.GetDetails()).(*pricingv1.CancelPriceListWorkflowRequest_Details)
	subscribeApiCredentialUpdatesRequest := &identityv1.SubscribeApiCredentialUpdatesRequest{}
	subscribeApiCredentialUpdatesRequest.Context = req.GetContext()
	subscribeApiCredentialUpdatesRequest.Details = &identityv1.SubscribeApiCredentialUpdatesRequest_Details{
		Recipient: &commonv1.ContactInfo{
			Phone:            (&decoded).GetNotify().GetPhone(),
			Email:            tenancy.Value(ctx, "email"),
			Name:             req.GetDetails().GetNotify().GetName(),
			PreferredChannel: cloned.GetNotify().GetPreferredChannel(),
		},
	}
	subscribeApiCredentialUpdatesReply, err := s.apiCredentialNotificationClient.SubscribeApiCredentialUpdates(ctx, subscribeApiCredentialUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SubscribeApiCredentialUpdates")
	}
	estimatePriceQuoteRequest := &pricingv1.EstimatePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{
				Tax: &pricingv1.PriceQuoteLineTax{Amount: req.GetPriceList().GetDiscount().GetAmount()},
			}},
		},
	}
	estimatePriceQuoteReply, err := s.meteredPriceQuoteCalculation.EstimatePriceQuote(ctx, estimatePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/EstimatePriceQuote")
	}
	return &pricingv1.CancelPriceListWorkflowResponse{
		Outcome: &pricingv1.CancelPriceListWorkflowResponse_Outcome{
			ReferenceId: subscribeApiCredentialUpdatesReply.GetOutcome().GetReferenceId(),
		},
		PriceList: &pricingv1.PriceList{
			Owner: &identityv1.UserRef{
				DisplayName: subscribeApiCredentialUpdatesReply.GetApiCredential().GetHolder().GetDisplayName(),
			},
			PriceListId: req.GetPriceListId(),
			Effective:   req.GetPriceList().GetEffective(),
		},
		Result: &commonv1.OperationResult{
			Details: estimatePriceQuoteReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// RetryPriceListWorkflow retrys a price list.
func (s *PriceListWorkflowServer) RetryPriceListWorkflow(ctx context.Context, req *pricingv1.RetryPriceListWorkflowRequest) (*pricingv1.RetryPriceListWorkflowResponse, error) {
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "retry price list workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	priceListCache := cache.New[string, *pricingv1.PriceList](1)
	priceListCache.Put(req.GetContext().GetTraceId(), req.GetPriceList())
	cachedPriceList, _ := priceListCache.Get(req.GetContext().GetTraceId())
	var reason string
	switch choice := req.GetPriceList().GetAction().(type) {
	case *pricingv1.PriceList_Route:
		reason = choice.Route.GetTarget()
	case *pricingv1.PriceList_Block:
		reason = choice.Block.GetReason()
	}
	packed, err := anypb.New(req.GetPriceList())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateConsentRecordsRequest := &customersv1.RecalculateConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{DisplayName: cachedPriceList.GetOwner().GetDisplayName()},
			Detail:  packed,
		},
		Reason:      reason,
		Annotations: req.GetAnnotations(),
	}
	recalculateConsentRecordsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*customersv1.RecalculateConsentRecordsResponse, error) {
		return s.consentRecordCalculationClient.RecalculateConsentRecords(ctx, recalculateConsentRecordsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords")
	}
	cancelRefundWorkflowRequest := buildCancelRefundWorkflowRequestForRetryPriceListWorkflow(req)
	cancelRefundWorkflowReply, err := s.refundWorkflowClient.CancelRefundWorkflow(ctx, cancelRefundWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundWorkflowService/CancelRefundWorkflow")
	}
	commentPriceQuoteApprovalRequest := &pricingv1.CommentPriceQuoteApprovalRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{
				Locality: cancelRefundWorkflowReply.GetRefund().GetBillingAddress().GetLocality(),
			},
			Settlement: &pricingv1.PriceQuote_Bank{Bank: &pricingv1.PriceQuoteBankSettlement{
				Iban: cancelRefundWorkflowReply.GetRefund().GetBank().GetIban(),
			}},
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{Amount: req.GetPriceList().GetDiscount().GetAmount()}},
			Audit: &commonv1.AuditInfo{Etag: req.GetPriceList().GetAudit().GetEtag()},
		},
	}
	commentPriceQuoteApprovalReply, err := s.commentPriceQuoteApprovalFn(ctx, commentPriceQuoteApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteApprovalService/CommentPriceQuoteApproval")
	}
	resumeRiskAssessmentWorkflowRequest := &fraudv1.ResumeRiskAssessmentWorkflowRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				Email: cancelRefundWorkflowReply.GetRefund().GetCounterparty().GetEmail(),
			},
		},
	}
	resumeRiskAssessmentWorkflowReply := new(fraudv1.ResumeRiskAssessmentWorkflowResponse)
	if err := grpc.Invoke(ctx, "/acme.fraud.v1.RiskAssessmentWorkflowService/ResumeRiskAssessmentWorkflow", resumeRiskAssessmentWorkflowRequest, resumeRiskAssessmentWorkflowReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentWorkflowService/ResumeRiskAssessmentWorkflow")
	}
	return &pricingv1.RetryPriceListWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: recalculateConsentRecordsReply.GetResult().GetMessage(),
			Status:  recalculateConsentRecordsReply.GetResult().GetStatus(),
		},
		Outcome: &pricingv1.RetryPriceListWorkflowResponse_Outcome{
			EffectiveAt: cancelRefundWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		PriceList: &pricingv1.PriceList{
			Owner: &identityv1.UserRef{
				DisplayName: commentPriceQuoteApprovalReply.GetPriceQuote().GetCounterparty().GetDisplayName(),
				Email:       resumeRiskAssessmentWorkflowReply.GetRiskAssessment().GetRequester().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: commentPriceQuoteApprovalReply.GetPriceQuote().GetAudit().GetCreatedAt(),
			},
			PriceListId: req.GetPriceListId(),
			Parameters:  req.GetDetails().GetParameters(),
		},
	}, nil
}

// InspectPriceListWorkflow returns a price list.
func (s *PriceListWorkflowServer) InspectPriceListWorkflow(ctx context.Context, req *pricingv1.InspectPriceListWorkflowRequest) (*pricingv1.InspectPriceListWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectPriceListWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_list", req.GetPriceListId(), "inspect_price_list_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect price list workflow", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	startFulfillmentExceptionWorkflowRequest := &fulfillmentv1.StartFulfillmentExceptionWorkflowRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Assignee: &identityv1.UserRef{UserId: (&decoded).GetActor().GetUserId()},
		},
	}
	startFulfillmentExceptionWorkflowReply, err := startFulfillmentExceptionWorkflowCall(ctx, startFulfillmentExceptionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionWorkflowService/StartFulfillmentExceptionWorkflow")
	}
	return &pricingv1.InspectPriceListWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: startFulfillmentExceptionWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PriceList: &pricingv1.PriceList{PriceListId: req.GetPriceListId()},
	}, nil
}

// ListPriceListWorkflows lists price lists a page at a time.
func (s *PriceListWorkflowServer) ListPriceListWorkflows(ctx context.Context, req *pricingv1.ListPriceListWorkflowsRequest) (*pricingv1.ListPriceListWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list price list workflows", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainCurrencyRateDecisionRequest := &pricingv1.ExplainCurrencyRateDecisionRequest{
		Context: req.GetContext(),
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject:       &customersv1.CustomerRef{Email: cachedContext.GetActor().GetEmail()},
			OccurredAt:    req.GetFilter().GetCreatedAfter(),
			CorrelationId: req.GetFilter().GetOwnerId(),
			Detail:        packed,
		},
		Details: &pricingv1.ExplainCurrencyRateDecisionRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	explainCurrencyRateDecisionReply := new(pricingv1.ExplainCurrencyRateDecisionResponse)
	if err := s.conn.Invoke(ctx, pricingCurrencyRateValidationServiceExplainCurrencyRateDecisionMethod, explainCurrencyRateDecisionRequest, explainCurrencyRateDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateValidationService/ExplainCurrencyRateDecision")
	}
	return &pricingv1.ListPriceListWorkflowsResponse{
		Result: &commonv1.OperationResult{
			Status: explainCurrencyRateDecisionReply.GetResult().GetStatus(),
		},
		PriceLists: []*pricingv1.PriceList{&pricingv1.PriceList{
			Owner: &identityv1.UserRef{
				Email: explainCurrencyRateDecisionReply.GetCurrencyRate().GetSubject().GetEmail(),
			},
		}},
	}, nil
}
