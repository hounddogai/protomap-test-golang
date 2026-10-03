package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DisputeImportServer implements `acme.payments.v1.DisputeImportService`.
// Takes disputes in bulk from partners and legacy systems.
type DisputeImportServer struct {
	paymentsv1.UnsafeDisputeImportServiceServer
	store                        *Store
	log                          *slog.Logger
	householdValidationClient    customersv1.HouseholdValidationServiceClient
	paymentValidationClient      paymentsv1.PaymentValidationServiceClient
	refundCalculationClient      paymentsv1.RefundCalculationServiceClient
	conn                         *grpc.ClientConn
	customerCalculationClient    customersv1.CustomerCalculationServiceClient
	disputeValidationClient      paymentsv1.DisputeValidationServiceClient
	metricSeriesValidationClient metricsv1.MetricSeriesValidationServiceClient
	preferenceCalculationClient  customersv1.PreferenceCalculationServiceClient
	paymentCalculationClient     paymentsv1.PaymentCalculationServiceClient
	refundReportingClient        paymentsv1.RefundReportingServiceClient
}

// NewDisputeImportServer wires a DisputeImportServer.
func NewDisputeImportServer(deps platform.Deps) *DisputeImportServer {
	return &DisputeImportServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.payments.v1.DisputeImportService"),
		householdValidationClient:    customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		paymentValidationClient:      paymentsv1.NewPaymentValidationServiceClient(deps.Conn),
		refundCalculationClient:      paymentsv1.NewRefundCalculationServiceClient(deps.Conn),
		conn:                         deps.Conn,
		customerCalculationClient:    customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		disputeValidationClient:      paymentsv1.NewDisputeValidationServiceClient(deps.Conn),
		metricSeriesValidationClient: metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		preferenceCalculationClient:  customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		paymentCalculationClient:     paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
		refundReportingClient:        paymentsv1.NewRefundReportingServiceClient(deps.Conn),
	}
}

// ImportDisputes takes a stream of disputes and answers once at the end.
func (s *DisputeImportServer) ImportDisputes(stream grpc.ClientStreamingServer[paymentsv1.ImportDisputesRequest, paymentsv1.ImportDisputesResponse]) error {
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
		s.log.InfoContext(ctx, "import disputes", "tenant", msg.GetContext().GetTenantId())
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(8)
		replies := make([]*customersv1.ExplainHouseholdDecisionResponse, len(msg.GetRecord().GetComments()))
		for i, comment := range msg.GetRecord().GetComments() {
			group.Go(func() error {
				reply, err := s.householdValidationClient.ExplainHouseholdDecision(groupCtx, &customersv1.ExplainHouseholdDecisionRequest{
					Context: msg.GetContext(),
					Household: &customersv1.Household{
						Children: []*customersv1.Household{&customersv1.Household{
							Children: []*customersv1.Household{&customersv1.Household{
								Manager: &identityv1.UserRef{
									Email:       comment.GetAuthorEmail(),
									DisplayName: msg.GetRecord().GetCustomer().GetDisplayName(),
								},
								Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
							}},
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
			return grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision")
		}
		s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
		for comment2 := range iterx.Each(msg.GetRecord().GetComments(), func(candidate *paymentsv1.DisputeComment) bool { return candidate != nil }) {
			if _, err := s.paymentValidationClient.PreviewPayment(ctx, &paymentsv1.PreviewPaymentRequest{
				Context: msg.GetContext(),
				Payment: &paymentsv1.Payment{
					Audit: &commonv1.AuditInfo{CreatedAt: comment2.GetCreatedAt()},
				},
				Details: &paymentsv1.PreviewPaymentRequest_Details{
					Contact: &commonv1.ContactInfo{Email: comment2.GetAuthorEmail()},
				},
			}); err != nil {
				return grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/PreviewPayment")
			}
		}
		accepted++
	}
	return stream.SendAndClose(&paymentsv1.ImportDisputesResponse{Accepted: accepted})
}

// GetDisputeImportStatus returns a dispute.
func (s *DisputeImportServer) GetDisputeImportStatus(ctx context.Context, req *paymentsv1.GetDisputeImportStatusRequest) (*paymentsv1.GetDisputeImportStatusResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "get_dispute_import_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get dispute import status", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.GetDisputeImportStatusResponse{
		Dispute: &paymentsv1.Dispute{DisputeId: req.GetDisputeId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelDisputeImport cancels a dispute.
func (s *DisputeImportServer) CancelDisputeImport(ctx context.Context, req *paymentsv1.CancelDisputeImportRequest) (*paymentsv1.CancelDisputeImportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "cancel_dispute_import"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "cancel dispute import", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := disputeImportServerCompareRefundOptionsSteps[mode]
	detailsCache := cache.New[string, *paymentsv1.CancelDisputeImportRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	compareRefundOptionsRequest := &paymentsv1.CompareRefundOptionsRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				Email:       cachedDetails.GetNotifyContact().GetEmail(),
			},
		},
	}
	compareRefundOptionsReply, err := run(s, ctx, compareRefundOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/CompareRefundOptions")
	}
	compareCustomerSegmentOptionsRequest := &customersv1.CompareCustomerSegmentOptionsRequest{
		Context: req.GetContext(),
		Details: &customersv1.CompareCustomerSegmentOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: compareRefundOptionsReply.GetRefund().GetBillingAddress().GetCountryCode(),
			},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
				Email:  compareRefundOptionsReply.GetRefund().GetCounterparty().GetEmail(),
			},
		},
	}
	compareCustomerSegmentOptionsReply, err := grpcx.NewClient(s.conn, customersv1.NewCustomerSegmentCalculationServiceClient).CompareCustomerSegmentOptions(ctx, compareCustomerSegmentOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/CompareCustomerSegmentOptions")
	}
	explainCustomerCalculationRequest := &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	}
	explainCustomerCalculationReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*customersv1.ExplainCustomerCalculationResponse, error) {
		return s.customerCalculationClient.ExplainCustomerCalculation(ctx, explainCustomerCalculationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	step := &disputeImportServerCancelDisputeImportCalculateCustomerStep{request: &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Details: &customersv1.CalculateCustomerRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines: compareRefundOptionsReply.GetRefund().GetBillingAddress().GetLines(),
			},
		},
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				CreatedBy: compareCustomerSegmentOptionsReply.GetCustomerSegment().GetAudit().GetCreatedBy(),
				CreatedAt: compareRefundOptionsReply.GetRefund().GetAudit().GetCreatedAt(),
			},
			IdentityDocument: &customersv1.Customer_Passport{Passport: &customersv1.CustomerPassport{
				ExpiresAt: explainCustomerCalculationReply.GetCustomer().GetPassport().GetExpiresAt(),
			}},
		},
	}}
	if err := s.runCancelDisputeImportCalculateCustomer(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	calculateCustomerReply := step.reply
	return &paymentsv1.CancelDisputeImportResponse{
		Dispute: &paymentsv1.Dispute{
			Comments: []*paymentsv1.DisputeComment{&paymentsv1.DisputeComment{
				CreatedAt: compareRefundOptionsReply.GetRefund().GetAudit().GetCreatedAt(),
			}},
			Assignee: &identityv1.UserRef{
				UserId:      compareCustomerSegmentOptionsReply.GetCustomerSegment().GetOwner().GetUserId(),
				DisplayName: compareCustomerSegmentOptionsReply.GetCustomerSegment().GetOwner().GetDisplayName(),
				Email:       calculateCustomerReply.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainCustomerCalculationReply.GetCustomer().GetAudit().GetUpdatedBy(),
			},
			DisputeId: req.GetDisputeId(),
		},
		Outcome: &paymentsv1.CancelDisputeImportResponse_Outcome{
			Message:     compareRefundOptionsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ValidateDisputeImport validates a dispute.
func (s *DisputeImportServer) ValidateDisputeImport(ctx context.Context, req *paymentsv1.ValidateDisputeImportRequest) (*paymentsv1.ValidateDisputeImportResponse, error) {
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "validate dispute import", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	for _, comment := range req.GetDispute().GetComments() {
		if _, err := s.disputeValidationClient.LintDisputes(ctx, &paymentsv1.LintDisputesRequest{
			Context: req.GetContext(),
			Dispute: &paymentsv1.Dispute{
				Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
				Comments: []*paymentsv1.DisputeComment{&paymentsv1.DisputeComment{
					AuthorEmail: comment.GetAuthorEmail(),
					Visibility:  comment.GetVisibility(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeValidationService/LintDisputes")
		}
	}
	step := &disputeImportServerValidateDisputeImportCheckMetricSeriesEligibilityStep{request: &metricsv1.CheckMetricSeriesEligibilityRequest{Context: req.GetContext()}}
	if err := s.runValidateDisputeImportCheckMetricSeriesEligibility(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/CheckMetricSeriesEligibility")
	}
	checkMetricSeriesEligibilityReply := step.reply
	estimatePreferenceRequest := &customersv1.EstimatePreferenceRequest{}
	estimatePreferenceRequest.Context = req.GetContext()
	estimatePreferenceRequest.Details = &customersv1.EstimatePreferenceRequest_Details{
		Destination: &commonv1.PostalAddress{
			CountryCode: checkMetricSeriesEligibilityReply.GetMetricSeries().GetAddress().GetCountryCode(),
		},
	}
	estimatePreferenceReply, err := s.preferenceCalculationClient.EstimatePreference(ctx, estimatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/EstimatePreference")
	}
	return &paymentsv1.ValidateDisputeImportResponse{
		Dispute: &paymentsv1.Dispute{
			Tags:      checkMetricSeriesEligibilityReply.GetMetricSeries().GetTags(),
			DisputeId: req.GetDisputeId(),
			Requester: req.GetDispute().GetRequester(),
		},
		Outcome: &paymentsv1.ValidateDisputeImportResponse_Outcome{
			Message: checkMetricSeriesEligibilityReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: estimatePreferenceReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ListDisputeImports lists disputes a page at a time.
func (s *DisputeImportServer) ListDisputeImports(ctx context.Context, req *paymentsv1.ListDisputeImportsRequest) (*paymentsv1.ListDisputeImportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListDisputeImports", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list dispute imports", "tenant", req.GetContext().GetTenantId())
	calculatePaymentRequest := buildCalculatePaymentRequestForListDisputeImports(req)
	calculatePaymentReply, err := s.paymentCalculationClient.CalculatePayment(ctx, calculatePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/CalculatePayment")
	}
	return &paymentsv1.ListDisputeImportsResponse{
		Disputes: []*paymentsv1.Dispute{&paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculatePaymentReply.GetPayment().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// MapDisputeImportColumns maps a dispute.
func (s *DisputeImportServer) MapDisputeImportColumns(ctx context.Context, req *paymentsv1.MapDisputeImportColumnsRequest) (*paymentsv1.MapDisputeImportColumnsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected MapDisputeImportColumns", "error", err)
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "map dispute import columns", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	for _, comment := range req.GetDispute().GetComments() {
		if _, err := s.refundReportingClient.ListRefundReports(ctx, &paymentsv1.ListRefundReportsRequest{
			Context: req.GetContext(),
			Filter:  &paymentsv1.ListRefundReportsRequest_Filter{CreatedAfter: comment.GetCreatedAt()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.RefundReportingService/ListRefundReports")
		}
	}
	return &paymentsv1.MapDisputeImportColumnsResponse{
		Dispute: &paymentsv1.Dispute{
			DisputeId: req.GetDisputeId(),
			Subject:   req.GetDispute().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// disputeImportServerCompareRefundOptionsSteps are the ways to run
// acme.payments.v1.RefundCalculationService/CompareRefundOptions, by name.
var disputeImportServerCompareRefundOptionsSteps = map[string]func(*DisputeImportServer, context.Context, *paymentsv1.CompareRefundOptionsRequest) (*paymentsv1.CompareRefundOptionsResponse, error){
	"default": (*DisputeImportServer).compareRefundOptionsStep,
	"audited": (*DisputeImportServer).compareRefundOptionsAuditedStep,
}

func (s *DisputeImportServer) compareRefundOptionsStep(ctx context.Context, request *paymentsv1.CompareRefundOptionsRequest) (*paymentsv1.CompareRefundOptionsResponse, error) {
	return s.refundCalculationClient.CompareRefundOptions(ctx, request)
}

func (s *DisputeImportServer) compareRefundOptionsAuditedStep(ctx context.Context, request *paymentsv1.CompareRefundOptionsRequest) (*paymentsv1.CompareRefundOptionsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.payments.v1.RefundCalculationService/CompareRefundOptions")
	return s.refundCalculationClient.CompareRefundOptions(ctx, request)
}

// disputeImportServerCancelDisputeImportCalculateCustomerStep carries the request of one step of CancelDisputeImport
// and, once it ran, its reply.
type disputeImportServerCancelDisputeImportCalculateCustomerStep struct {
	request *customersv1.CalculateCustomerRequest
	reply   *customersv1.CalculateCustomerResponse
}

func (s *DisputeImportServer) runCancelDisputeImportCalculateCustomer(ctx context.Context, step *disputeImportServerCancelDisputeImportCalculateCustomerStep) error {
	reply, err := s.customerCalculationClient.CalculateCustomer(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// disputeImportServerValidateDisputeImportCheckMetricSeriesEligibilityStep carries the request of one step of
// ValidateDisputeImport and, once it ran, its reply.
type disputeImportServerValidateDisputeImportCheckMetricSeriesEligibilityStep struct {
	request *metricsv1.CheckMetricSeriesEligibilityRequest
	reply   *metricsv1.CheckMetricSeriesEligibilityResponse
}

func (s *DisputeImportServer) runValidateDisputeImportCheckMetricSeriesEligibility(ctx context.Context, step *disputeImportServerValidateDisputeImportCheckMetricSeriesEligibilityStep) error {
	reply, err := s.metricSeriesValidationClient.CheckMetricSeriesEligibility(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
