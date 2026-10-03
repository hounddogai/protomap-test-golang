package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	corporateeventsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/corporateevents/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	maintenancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/maintenance/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// CustomerSegmentImportServer implements `acme.customers.v1.CustomerSegmentImportService`.
// Takes customer segments in bulk from partners and legacy systems.
type CustomerSegmentImportServer struct {
	*customerSegmentImportHandlers
}

// customerSegmentImportHandlers holds the handlers of CustomerSegmentImportServer, with what they use.
type customerSegmentImportHandlers struct {
	customersv1.UnimplementedCustomerSegmentImportServiceServer
	store                              *Store
	log                                *slog.Logger
	preferenceCalculationClient        customersv1.PreferenceCalculationServiceClient
	fraudRuleApprovalClient            fraudv1.FraudRuleApprovalServiceClient
	corporateEventQueryClient          corporateeventsv1.CorporateEventQueryServiceClient
	householdCalculationClient         customersv1.HouseholdCalculationServiceClient
	conn                               *grpc.ClientConn
	userHistoryClient                  identityv1.UserHistoryServiceClient
	sellerPayoutScheduleWorkflowClient sellersv1.SellerPayoutScheduleWorkflowServiceClient
	renewalScheduleWorkflowClient      subscriptionsv1.RenewalScheduleWorkflowServiceClient
	stockReservationApprovalPort       stockReservationApprovalPort
	featureFlagValidationClient        featureflagsv1.FeatureFlagValidationServiceClient
	searchIndexClient                  searchv1.SearchIndexServiceClient
	consentRecordCalculationPort       consentRecordCalculationPort
	priceOverrideApprovalClient        pricingv1.PriceOverrideApprovalServiceClient
	workOrderClient                    maintenancev1.WorkOrderServiceClient
	customerClient                     customersv1.CustomerServiceClient
}

// NewCustomerSegmentImportServer wires a CustomerSegmentImportServer.
func NewCustomerSegmentImportServer(deps platform.Deps) *CustomerSegmentImportServer {
	return &CustomerSegmentImportServer{customerSegmentImportHandlers: &customerSegmentImportHandlers{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.customers.v1.CustomerSegmentImportService"),
		preferenceCalculationClient:        customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		fraudRuleApprovalClient:            fraudv1.NewFraudRuleApprovalServiceClient(deps.Conn),
		corporateEventQueryClient:          corporateeventsv1.NewCorporateEventQueryServiceClient(deps.Conn),
		householdCalculationClient:         customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		conn:                               deps.Conn,
		userHistoryClient:                  identityv1.NewUserHistoryServiceClient(deps.Conn),
		sellerPayoutScheduleWorkflowClient: sellersv1.NewSellerPayoutScheduleWorkflowServiceClient(deps.Conn),
		renewalScheduleWorkflowClient:      subscriptionsv1.NewRenewalScheduleWorkflowServiceClient(deps.Conn),
		stockReservationApprovalPort:       newStockReservationApprovalPort(deps),
		featureFlagValidationClient:        featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		searchIndexClient:                  searchv1.NewSearchIndexServiceClient(deps.Conn),
		consentRecordCalculationPort:       newConsentRecordCalculationPort(deps),
		priceOverrideApprovalClient:        pricingv1.NewPriceOverrideApprovalServiceClient(deps.Conn),
		workOrderClient:                    maintenancev1.NewWorkOrderServiceClient(deps.Conn),
		customerClient:                     customersv1.NewCustomerServiceClient(deps.Conn),
	}}
}

// ImportCustomerSegments takes a stream of customer segments and answers once at the end.
func (s *customerSegmentImportHandlers) ImportCustomerSegments(stream grpc.ClientStreamingServer[customersv1.ImportCustomerSegmentsRequest, customersv1.ImportCustomerSegmentsResponse]) error {
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
		s.log.InfoContext(ctx, "import customer segments", "tenant", msg.GetContext().GetTenantId())
		calculatePreferenceRequest := &customersv1.CalculatePreferenceRequest{}
		calculatePreferenceRequest.Context = msg.GetContext()
		calculatePreferenceRequest.Preference = &customersv1.Preference{
			Owner:  &identityv1.UserRef{Email: msg.GetRecord().GetOwner().GetEmail()},
			Action: &customersv1.Preference_Block{Block: &customersv1.PreferenceBlockAction{Reason: msg.GetRecord().GetBlock().GetReason()}},
		}
		if _, err := s.preferenceCalculationClient.CalculatePreference(ctx, calculatePreferenceRequest); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/CalculatePreference")
		}
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(8)
		replies := make([]*fraudv1.EscalateFraudRuleResponse, len(msg.GetRecord().GetConditions()))
		for i, condition := range msg.GetRecord().GetConditions() {
			group.Go(func() error {
				reply, err := s.fraudRuleApprovalClient.EscalateFraudRule(groupCtx, &fraudv1.EscalateFraudRuleRequest{
					Context: msg.GetContext(),
					FraudRule: &fraudv1.FraudRule{
						Conditions: []*fraudv1.FraudRuleCondition{&fraudv1.FraudRuleCondition{
							Attribute: condition.GetAttribute(),
							Value:     condition.GetValue(),
							Negate:    condition.GetNegate(),
							Operator:  condition.GetOperator(),
						}},
					},
				})
				if err != nil {
					return err
				}
				replies[i] = reply
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			return grpcx.Wrap(err, "acme.fraud.v1.FraudRuleApprovalService/EscalateFraudRule")
		}
		s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
		accepted++
	}
	return stream.SendAndClose(&customersv1.ImportCustomerSegmentsResponse{Accepted: accepted})
}

// GetCustomerSegmentImportStatus returns a customer segment.
func (s *customerSegmentImportHandlers) GetCustomerSegmentImportStatus(ctx context.Context, req *customersv1.GetCustomerSegmentImportStatusRequest) (*customersv1.GetCustomerSegmentImportStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetCustomerSegmentImportStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "get_customer_segment_import_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get customer segment import status", "tenant", req.GetContext().GetTenantId())
	suggestCorporateEventsReply, err := s.corporateEventQueryClient.SuggestCorporateEvents(ctx, &corporateeventsv1.SuggestCorporateEventsRequest{
		Context: req.GetContext(),
		Filter: &corporateeventsv1.SuggestCorporateEventsRequest_Filter{
			OwnerId:      req.GetCustomerSegmentId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.corporateevents.v1.CorporateEventQueryService/SuggestCorporateEvents")
	}
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	simulateHouseholdRequest := &customersv1.SimulateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						Email:  req.GetContext().GetActor().GetEmail(),
						UserId: (&decoded).GetActor().GetUserId(),
					},
					Children: []*customersv1.Household{&customersv1.Household{
						Attributes: suggestCorporateEventsReply.GetResult().GetDetails(),
						ParentId:   req.GetCustomerSegmentId(),
					}},
				}},
			}},
		},
	}
	send := s.simulateHouseholdSender(simulateHouseholdRequest)
	simulateHouseholdReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/SimulateHousehold")
	}
	return &customersv1.GetCustomerSegmentImportStatusResponse{
		Result: &commonv1.OperationResult{
			Details:       suggestCorporateEventsReply.GetResult().GetDetails(),
			CorrelationId: simulateHouseholdReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		CustomerSegment: &customersv1.CustomerSegment{CustomerSegmentId: req.GetCustomerSegmentId()},
	}, nil
}

// CancelCustomerSegmentImport cancels a customer segment.
func (s *customerSegmentImportHandlers) CancelCustomerSegmentImport(ctx context.Context, req *customersv1.CancelCustomerSegmentImportRequest) (*customersv1.CancelCustomerSegmentImportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "cancel_customer_segment_import"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "cancel customer segment import", "tenant", req.GetContext().GetTenantId())
	normalizeHouseholdRequest := &customersv1.NormalizeHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
				}},
				Manager: &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			}},
		},
		Details: &customersv1.NormalizeHouseholdRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	normalizeHouseholdReply := new(customersv1.NormalizeHouseholdResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.customers.v1.HouseholdValidationService/NormalizeHousehold", normalizeHouseholdRequest, normalizeHouseholdReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/NormalizeHousehold")
	}
	recordUserVersionRequest := &identityv1.RecordUserVersionRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: normalizeHouseholdReply.GetHousehold().GetLocation().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      normalizeHouseholdReply.GetHousehold().GetAudit().GetEtag(),
				UpdatedAt: normalizeHouseholdReply.GetHousehold().GetAudit().GetUpdatedAt(),
			},
		},
	}
	type recordUserVersionResult struct {
		reply *identityv1.RecordUserVersionResponse
		err   error
	}
	recordUserVersionResults := make(chan recordUserVersionResult, 1)
	go func() {
		reply, err := s.userHistoryClient.RecordUserVersion(ctx, recordUserVersionRequest)
		recordUserVersionResults <- recordUserVersionResult{reply, err}
	}()
	var recordUserVersionReply *identityv1.RecordUserVersionResponse
	select {
	case result := <-recordUserVersionResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.UserHistoryService/RecordUserVersion")
		}
		recordUserVersionReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	listSellerPayoutScheduleWorkflowsReply, err := s.sellerPayoutScheduleWorkflowClient.ListSellerPayoutScheduleWorkflows(ctx, &sellersv1.ListSellerPayoutScheduleWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListSellerPayoutScheduleWorkflowsRequest_Filter{
			Region:       recordUserVersionReply.GetUser().GetPrimaryAddress().GetRegion(),
			CreatedAfter: normalizeHouseholdReply.GetHousehold().GetAudit().GetCreatedAt(),
			OwnerId:      normalizeHouseholdReply.GetHousehold().GetHouseholdId(),
			Labels:       recordUserVersionReply.GetUser().GetAttributes(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleWorkflowService/ListSellerPayoutScheduleWorkflows")
	}
	retryRenewalScheduleWorkflowRequest := buildRetryRenewalScheduleWorkflowRequestForCancelCustomerSegmentImport(req, recordUserVersionReply)
	retryRenewalScheduleWorkflowReply, err := s.renewalScheduleWorkflowClient.RetryRenewalScheduleWorkflow(ctx, retryRenewalScheduleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleWorkflowService/RetryRenewalScheduleWorkflow")
	}
	return &customersv1.CancelCustomerSegmentImportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: normalizeHouseholdReply.GetResult().GetCorrelationId(),
			ReferenceIds:  normalizeHouseholdReply.GetResult().GetReferenceIds(),
			Message:       listSellerPayoutScheduleWorkflowsReply.GetResult().GetMessage(),
			Details:       retryRenewalScheduleWorkflowReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Audit:             &commonv1.AuditInfo{Etag: recordUserVersionReply.GetUser().GetAudit().GetEtag()},
			CustomerSegmentId: req.GetCustomerSegmentId(),
		},
		Outcome: &customersv1.CancelCustomerSegmentImportResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ValidateCustomerSegmentImport validates a customer segment.
func (s *customerSegmentImportHandlers) ValidateCustomerSegmentImport(ctx context.Context, req *customersv1.ValidateCustomerSegmentImportRequest) (*customersv1.ValidateCustomerSegmentImportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "validate customer segment import", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetCustomerSegment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewConsentRecordRequest := &customersv1.PreviewConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.PreviewConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetContact().GetEmail(),
				Phone:            req.GetDetails().GetContact().GetPhone(),
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Name:             req.GetDetails().GetContact().GetName(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
	}
	previewConsentRecordReply := new(customersv1.PreviewConsentRecordResponse)
	if err := s.conn.Invoke(ctx, customersConsentRecordValidationServicePreviewConsentRecordMethod, previewConsentRecordRequest, previewConsentRecordReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord")
	}
	escalateStockReservationRequest := &inventoryv1.EscalateStockReservationRequest{
		Context: req.GetContext(),
		StockReservation: &inventoryv1.StockReservation{
			Organizer: &identityv1.UserRef{
				Email:  req.GetDetails().GetContact().GetEmail(),
				UserId: req.GetCustomerSegment().GetOwner().GetUserId(),
			},
			Participants: []*inventoryv1.StockReservationParticipant{&inventoryv1.StockReservationParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetCustomerSegment().GetOwner().GetEmail()},
			}},
			Audit: &commonv1.AuditInfo{Etag: req.GetCustomerSegment().GetAudit().GetEtag()},
		},
	}
	escalateStockReservationReply, err := s.stockReservationApprovalPort.EscalateStockReservation(ctx, escalateStockReservationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationApprovalService/EscalateStockReservation")
	}
	checkCustomerEligibilityRequest := &customersv1.CheckCustomerEligibilityRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{Email: req.GetCustomerSegment().GetOwner().GetEmail()},
			PrimaryAddress: &commonv1.PostalAddress{
				Region: escalateStockReservationReply.GetStockReservation().GetLocation().GetRegion(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: escalateStockReservationReply.GetStockReservation().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &customersv1.CheckCustomerEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	checkCustomerEligibilityReply, err := checkCustomerEligibilityCall(ctx, checkCustomerEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/CheckCustomerEligibility")
	}
	explainFeatureFlagDecisionRequest := &featureflagsv1.ExplainFeatureFlagDecisionRequest{}
	explainFeatureFlagDecisionRequest.Context = req.GetContext()
	explainFeatureFlagDecisionRequest.FeatureFlag = &featureflagsv1.FeatureFlag{
		Audit: &commonv1.AuditInfo{
			UpdatedBy: checkCustomerEligibilityReply.GetCustomer().GetAudit().GetUpdatedBy(),
		},
	}
	explainFeatureFlagDecisionReply, err := s.featureFlagValidationClient.ExplainFeatureFlagDecision(ctx, explainFeatureFlagDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision")
	}
	return &customersv1.ValidateCustomerSegmentImportResponse{
		Result: &commonv1.OperationResult{
			Status:        previewConsentRecordReply.GetOutcome().GetStatus(),
			Message:       previewConsentRecordReply.GetResult().GetMessage(),
			CorrelationId: explainFeatureFlagDecisionReply.GetResult().GetCorrelationId(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				Etag: escalateStockReservationReply.GetStockReservation().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: escalateStockReservationReply.GetStockReservation().GetOrganizer().GetDisplayName(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Parameters:        req.GetCustomerSegment().GetParameters(),
		},
		Outcome: &customersv1.ValidateCustomerSegmentImportResponse_Outcome{
			ReferenceId: checkCustomerEligibilityReply.GetOutcome().GetReferenceId(),
		},
	}, nil
}

// ListCustomerSegmentImports lists customer segments a page at a time.
func (s *customerSegmentImportHandlers) ListCustomerSegmentImports(ctx context.Context, req *customersv1.ListCustomerSegmentImportsRequest) (*customersv1.ListCustomerSegmentImportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list customer segment imports", "tenant", req.GetContext().GetTenantId())
	searchSearchIndexesRequest := &searchv1.SearchSearchIndexesRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
		Filter:  &searchv1.SearchSearchIndexesRequest_Filter{CreatedAfter: req.GetFilter().GetCreatedAfter()},
	}
	send := s.searchSearchIndexesSender(searchSearchIndexesRequest)
	searchSearchIndexesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/SearchSearchIndexes")
	}
	return &customersv1.ListCustomerSegmentImportsResponse{
		Result: &commonv1.OperationResult{
			Details: searchSearchIndexesReply.GetResult().GetDetails(),
			Message: searchSearchIndexesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// RetryCustomerSegmentImport retrys a customer segment.
func (s *customerSegmentImportHandlers) RetryCustomerSegmentImport(ctx context.Context, req *customersv1.RetryCustomerSegmentImportRequest) (*customersv1.RetryCustomerSegmentImportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryCustomerSegmentImport", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "retry customer segment import", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetCustomerSegment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateConsentRecordRequest := &customersv1.CalculateConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetCustomerSegment().GetOwner().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	calculateConsentRecordReply, err := s.consentRecordCalculationPort.CalculateConsentRecord(ctx, calculateConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CalculateConsentRecord")
	}
	ctx = tenancy.With(ctx, "user_id", req.GetCustomerSegment().GetOwner().GetUserId())
	rejectPriceOverrideReply, err := s.priceOverrideApprovalClient.RejectPriceOverride(ctx, &pricingv1.RejectPriceOverrideRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCustomerSegment().GetAudit().GetUpdatedAt(),
			},
			Action: &pricingv1.PriceOverride_Discount{Discount: &pricingv1.PriceOverrideDiscountAction{
				Percent: req.GetCustomerSegment().GetDiscount().GetPercent(),
			}},
			Owner: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideApprovalService/RejectPriceOverride")
	}
	return &customersv1.RetryCustomerSegmentImportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: calculateConsentRecordReply.GetConsentRecord().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &customersv1.RetryCustomerSegmentImportResponse_Outcome{Status: rejectPriceOverrideReply.GetResult().GetStatus()},
		CustomerSegment: &customersv1.CustomerSegment{
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Description:       req.GetCustomerSegment().GetDescription(),
		},
	}, nil
}

// MapCustomerSegmentImportColumns maps a customer segment.
func (s *customerSegmentImportHandlers) MapCustomerSegmentImportColumns(ctx context.Context, req *customersv1.MapCustomerSegmentImportColumnsRequest) (*customersv1.MapCustomerSegmentImportColumnsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "map customer segment import columns", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	batchGetWorkOrdersRequest := &maintenancev1.BatchGetWorkOrdersRequest{}
	batchGetWorkOrdersRequest.Context = req.GetContext()
	batchGetWorkOrdersReply, err := s.workOrderClient.BatchGetWorkOrders(ctx, batchGetWorkOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.maintenance.v1.WorkOrderService/BatchGetWorkOrders")
	}
	cloned := proto.Clone(req.GetCustomerSegment()).(*customersv1.CustomerSegment)
	source := req.GetCustomerSegment().GetAudit().ProtoReflect()
	updatedBy := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("updated_by"))).String()
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.MapCustomerSegmentImportColumnsRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	updateCustomerRequest := &customersv1.UpdateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: cloned.GetOwner().GetDisplayName(),
				Email:       req.GetCustomerSegment().GetOwner().GetEmail(),
			},
			Audit:   &commonv1.AuditInfo{UpdatedBy: updatedBy},
			Contact: &commonv1.ContactInfo{Email: (&decoded).GetContact().GetEmail()},
		},
	}
	updateCustomerReply, err := customersv1.CustomerServiceClient.UpdateCustomer(s.customerClient, ctx, updateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/UpdateCustomer")
	}
	return &customersv1.MapCustomerSegmentImportColumnsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetWorkOrdersReply.GetResult().GetCorrelationId(),
			ReferenceIds:  updateCustomerReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		CustomerSegment: &customersv1.CustomerSegment{
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Description:       req.GetCustomerSegment().GetDescription(),
		},
	}, nil
}

// simulateHouseholdSender returns a function that sends one request to
// acme.customers.v1.HouseholdCalculationService/SimulateHousehold.
func (s *customerSegmentImportHandlers) simulateHouseholdSender(request *customersv1.SimulateHouseholdRequest) func(context.Context) (*customersv1.SimulateHouseholdResponse, error) {
	return func(ctx context.Context) (*customersv1.SimulateHouseholdResponse, error) {
		return s.householdCalculationClient.SimulateHousehold(ctx, request)
	}
}

// searchSearchIndexesSender returns a function that sends one request to
// acme.search.v1.SearchIndexService/SearchSearchIndexes.
func (s *customerSegmentImportHandlers) searchSearchIndexesSender(request *searchv1.SearchSearchIndexesRequest) func(context.Context) (*searchv1.SearchSearchIndexesResponse, error) {
	return func(ctx context.Context) (*searchv1.SearchSearchIndexesResponse, error) {
		return s.searchIndexClient.SearchSearchIndexes(ctx, request)
	}
}
