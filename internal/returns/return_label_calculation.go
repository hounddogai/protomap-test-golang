package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	telematicsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ReturnLabelCalculationServer implements `acme.returns.v1.ReturnLabelCalculationService`.
// Computes the figures of a return label, such as totals, taxes, and estimates.
type ReturnLabelCalculationServer struct {
	returnsv1.UnimplementedReturnLabelCalculationServiceServer
	store                              *Store
	log                                *slog.Logger
	reviewModerationValidationClient   reviewsv1.ReviewModerationValidationServiceClient
	venueQueryClient                   venuesv1.VenueQueryServiceClient
	apiCredentialHistoryClient         identityv1.ApiCredentialHistoryServiceClient
	apiCredentialHistoryClientFallback identityv1.ApiCredentialHistoryServiceClient
	returnRequestCalculationClient     returnsv1.ReturnRequestCalculationServiceClient
	documentQueryClient                documentsv1.DocumentQueryServiceClient
	synonymAdminClient                 searchv1.SynonymAdminServiceClient
	placementQueryClient               merchandisingv1.PlacementQueryServiceClient
	telemetryReadingQueryClient        telematicsv1.TelemetryReadingQueryServiceClient
	bus                                *eventbus.Bus
}

// NewReturnLabelCalculationServer wires a ReturnLabelCalculationServer.
func NewReturnLabelCalculationServer(deps platform.Deps) *ReturnLabelCalculationServer {
	return &ReturnLabelCalculationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.returns.v1.ReturnLabelCalculationService"),
		reviewModerationValidationClient:   reviewsv1.NewReviewModerationValidationServiceClient(deps.Conn),
		venueQueryClient:                   venuesv1.NewVenueQueryServiceClient(deps.Conn),
		apiCredentialHistoryClient:         identityv1.NewApiCredentialHistoryServiceClient(deps.Conn),
		apiCredentialHistoryClientFallback: identityv1.NewApiCredentialHistoryServiceClient(deps.Conn),
		returnRequestCalculationClient:     returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn),
		documentQueryClient:                documentsv1.NewDocumentQueryServiceClient(deps.Conn),
		synonymAdminClient:                 searchv1.NewSynonymAdminServiceClient(deps.Conn),
		placementQueryClient:               merchandisingv1.NewPlacementQueryServiceClient(deps.Conn),
		telemetryReadingQueryClient:        telematicsv1.NewTelemetryReadingQueryServiceClient(deps.Conn),
		bus:                                deps.Bus,
	}
}

// CalculateReturnLabel calculates a return label.
func (s *ReturnLabelCalculationServer) CalculateReturnLabel(ctx context.Context, req *returnsv1.CalculateReturnLabelRequest) (*returnsv1.CalculateReturnLabelResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "calculate return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := returnLabelCalculationServerExplainReviewModerationDecisionSteps[mode]
	explainReviewModerationDecisionRequest := &reviewsv1.ExplainReviewModerationDecisionRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetReturnLabel().GetAudit().GetUpdatedBy()},
		},
	}
	explainReviewModerationDecisionReply, err := run(s, ctx, explainReviewModerationDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationValidationService/ExplainReviewModerationDecision")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*venuesv1.CountVenuesResponse, len(req.GetReturnLabel().GetLines()))
	for i, line := range req.GetReturnLabel().GetLines() {
		group.Go(func() error {
			reply, err := s.venueQueryClient.CountVenues(groupCtx, &venuesv1.CountVenuesRequest{
				Context: req.GetContext(),
				Filter: &venuesv1.CountVenuesRequest_Filter{
					Region: req.GetReturnLabel().GetBillingAddress().GetRegion(),
					Labels: line.GetAttributes(),
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
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/CountVenues")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	routes := map[string]func(context.Context, *identityv1.AnnotateApiCredentialVersionRequest, ...grpc.CallOption) (*identityv1.AnnotateApiCredentialVersionResponse, error){
		"primary":  s.apiCredentialHistoryClient.AnnotateApiCredentialVersion,
		"fallback": s.apiCredentialHistoryClientFallback.AnnotateApiCredentialVersion,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloned := proto.Clone(req.GetReturnLabel()).(*returnsv1.ReturnLabel)
	snapshot, err := protojson.Marshal(req.GetReturnLabel())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateApiCredentialVersionRequest := &identityv1.AnnotateApiCredentialVersionRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: explainReviewModerationDecisionReply.GetReviewModeration().GetRequester().GetPreferredChannel(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: cloned.GetAudit().GetCreatedAt()},
		},
		Details: &identityv1.AnnotateApiCredentialVersionRequest_Details{Snapshot: snapshot},
	}
	annotateApiCredentialVersionReply, err := route(ctx, annotateApiCredentialVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/AnnotateApiCredentialVersion")
	}
	return &returnsv1.CalculateReturnLabelResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainReviewModerationDecisionReply.GetReviewModeration().GetAudit().GetUpdatedAt(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
			Number:        req.GetReturnLabel().GetNumber(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: annotateApiCredentialVersionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// EstimateReturnLabel estimates a return label.
func (s *ReturnLabelCalculationServer) EstimateReturnLabel(ctx context.Context, req *returnsv1.EstimateReturnLabelRequest) (*returnsv1.EstimateReturnLabelResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "estimate return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*returnsv1.CompareReturnRequestOptionsResponse, len(req.GetReturnLabel().GetLines()))
	for i, line := range req.GetReturnLabel().GetLines() {
		group.Go(func() error {
			reply, err := s.returnRequestCalculationClient.CompareReturnRequestOptions(groupCtx, &returnsv1.CompareReturnRequestOptionsRequest{
				Context: req.GetContext(),
				ReturnRequest: &returnsv1.ReturnRequest{
					Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
						Tax: &returnsv1.ReturnRequestLineTax{
							Amount: line.GetAmount(),
							Rate:   line.GetTax().GetRate(),
						},
						Description: line.GetDescription(),
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
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/CompareReturnRequestOptions")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &returnsv1.EstimateReturnLabelResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			ReturnLabelId: req.GetReturnLabelId(),
			Total:         req.GetReturnLabel().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteReturnLabel quotes a return label.
func (s *ReturnLabelCalculationServer) QuoteReturnLabel(ctx context.Context, req *returnsv1.QuoteReturnLabelRequest) (*returnsv1.QuoteReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "quote return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	findDocumentsReply, err := s.documentQueryClient.FindDocuments(ctx, &documentsv1.FindDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.FindDocumentsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetReturnLabel().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetReturnLabel().GetCounterparty().GetInspectionId(),
			Labels:       annotations,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/FindDocuments")
	}
	estimateReturnRequestReply, err := s.returnRequestCalculationClient.EstimateReturnRequest(ctx, &returnsv1.EstimateReturnRequestRequest{
		Context: req.GetContext(),
		Details: &returnsv1.EstimateReturnRequestRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetReturnLabel().GetBillingAddress().GetCountryCode(),
				Lines:       req.GetReturnLabel().GetBillingAddress().GetLines(),
			},
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetReturnLabel().GetAudit().GetCreatedBy()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/EstimateReturnRequest")
	}
	for _, attachment := range req.GetReturnLabel().GetAttachments() {
		if _, err := s.returnRequestCalculationClient.QuoteReturnRequest(ctx, &returnsv1.QuoteReturnRequestRequest{
			Context: req.GetContext(),
			ReturnRequest: &returnsv1.ReturnRequest{
				Attachments: []*returnsv1.ReturnRequestAttachment{&returnsv1.ReturnRequestAttachment{
					Checksum:    attachment.GetChecksum(),
					SizeBytes:   attachment.GetSizeBytes(),
					ContentType: attachment.GetContentType(),
					FileName:    attachment.GetFileName(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/QuoteReturnRequest")
		}
	}
	for _, attachment2 := range req.GetReturnLabel().GetAttachments() {
		if _, err := s.returnRequestCalculationClient.CalculateReturnRequest(ctx, &returnsv1.CalculateReturnRequestRequest{
			Context: req.GetContext(),
			ReturnRequest: &returnsv1.ReturnRequest{
				Attachments: []*returnsv1.ReturnRequestAttachment{&returnsv1.ReturnRequestAttachment{
					SizeBytes: attachment2.GetSizeBytes(),
					FileName:  attachment2.GetFileName(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/CalculateReturnRequest")
		}
	}
	return &returnsv1.QuoteReturnLabelResponse{
		Result: &commonv1.OperationResult{
			Details:       findDocumentsReply.GetResult().GetDetails(),
			ReferenceIds:  findDocumentsReply.GetResult().GetReferenceIds(),
			CorrelationId: estimateReturnRequestReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				Locality: estimateReturnRequestReply.GetReturnRequest().GetBillingAddress().GetLocality(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
			Number:        req.GetReturnLabel().GetNumber(),
		},
	}, nil
}

// CompareReturnLabelOptions compares a return label.
func (s *ReturnLabelCalculationServer) CompareReturnLabelOptions(ctx context.Context, req *returnsv1.CompareReturnLabelOptionsRequest) (*returnsv1.CompareReturnLabelOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "compare return label options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	for attachment := range iterx.Each(req.GetReturnLabel().GetAttachments(), func(candidate *returnsv1.ReturnLabelAttachment) bool { return candidate != nil }) {
		if _, err := s.returnRequestCalculationClient.SimulateReturnRequest(ctx, &returnsv1.SimulateReturnRequestRequest{
			Context: req.GetContext(),
			ReturnRequest: &returnsv1.ReturnRequest{
				Attachments: []*returnsv1.ReturnRequestAttachment{&returnsv1.ReturnRequestAttachment{
					Checksum:  attachment.GetChecksum(),
					FileName:  attachment.GetFileName(),
					Uri:       attachment.GetUri(),
					SizeBytes: attachment.GetSizeBytes(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/SimulateReturnRequest")
		}
	}
	for _, line := range req.GetReturnLabel().GetLines() {
		if _, err := s.returnRequestCalculationClient.RecalculateReturnRequests(ctx, &returnsv1.RecalculateReturnRequestsRequest{
			Context: req.GetContext(),
			ReturnRequest: &returnsv1.ReturnRequest{
				Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
					Tax: &returnsv1.ReturnRequestLineTax{Rate: line.GetTax().GetRate()},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/RecalculateReturnRequests")
		}
	}
	jobs := make(chan *returnsv1.ReturnLabelLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line2 := range jobs {
				if _, err := s.returnRequestCalculationClient.CompareReturnRequestOptions(workCtx, &returnsv1.CompareReturnRequestOptionsRequest{
					Context: req.GetContext(),
					ReturnRequest: &returnsv1.ReturnRequest{
						Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
							Tax: &returnsv1.ReturnRequestLineTax{
								Jurisdiction: line2.GetTax().GetJurisdiction(),
								Exemptions:   line2.GetTax().GetExemptions(),
							},
							Amount: line2.GetTax().GetAmount(),
						}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetReturnLabel().GetLines() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/CompareReturnRequestOptions")
	}
	explainReturnRequestCalculationRequest := &returnsv1.ExplainReturnRequestCalculationRequest{Context: req.GetContext()}
	type explainReturnRequestCalculationResult struct {
		reply *returnsv1.ExplainReturnRequestCalculationResponse
		err   error
	}
	explainReturnRequestCalculationResults := make(chan explainReturnRequestCalculationResult, 1)
	go func() {
		reply, err := s.returnRequestCalculationClient.ExplainReturnRequestCalculation(ctx, explainReturnRequestCalculationRequest)
		explainReturnRequestCalculationResults <- explainReturnRequestCalculationResult{reply, err}
	}()
	var explainReturnRequestCalculationReply *returnsv1.ExplainReturnRequestCalculationResponse
	select {
	case result := <-explainReturnRequestCalculationResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.returns.v1.ReturnRequestCalculationService/ExplainReturnRequestCalculation")
		}
		explainReturnRequestCalculationReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &returnsv1.CompareReturnLabelOptionsResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{
				Etag:      explainReturnRequestCalculationReply.GetReturnRequest().GetAudit().GetEtag(),
				UpdatedAt: explainReturnRequestCalculationReply.GetReturnRequest().GetAudit().GetUpdatedAt(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
			DueAt:         req.GetReturnLabel().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculateReturnLabels recalculates a return label.
func (s *ReturnLabelCalculationServer) RecalculateReturnLabels(ctx context.Context, req *returnsv1.RecalculateReturnLabelsRequest) (*returnsv1.RecalculateReturnLabelsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculateReturnLabels", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "recalculate return labels", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "country_code", req.GetDetails().GetDestination().GetCountryCode())
	recalculateReturnRequestsRequest := newRecalculateReturnRequestsRequest(withRecalculateReturnRequestsContext(req.GetContext()), withRecalculateReturnRequestsReturnRequest(&returnsv1.ReturnRequest{
		BillingAddress: &commonv1.PostalAddress{CountryCode: tenancy.Value(ctx, "country_code")},
	}), withRecalculateReturnRequestsDetails(&returnsv1.RecalculateReturnRequestsRequest_Details{
		Destination: &commonv1.PostalAddress{
			Locality: req.GetReturnLabel().GetBillingAddress().GetLocality(),
		},
	}))
	recalculateReturnRequestsReply, err := s.returnRequestCalculationClient.RecalculateReturnRequests(ctx, recalculateReturnRequestsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/RecalculateReturnRequests")
	}
	return &returnsv1.RecalculateReturnLabelsResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{
				DisplayName: recalculateReturnRequestsReply.GetReturnRequest().GetCounterparty().GetDisplayName(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
			Number:        req.GetReturnLabel().GetNumber(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SimulateReturnLabel simulates a return label.
func (s *ReturnLabelCalculationServer) SimulateReturnLabel(ctx context.Context, req *returnsv1.SimulateReturnLabelRequest) (*returnsv1.SimulateReturnLabelResponse, error) {
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "simulate return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	returnLabelCache := cache.New[string, *returnsv1.ReturnLabel](1)
	returnLabelCache.Put(req.GetContext().GetTraceId(), req.GetReturnLabel())
	cachedReturnLabel, _ := returnLabelCache.Get(req.GetContext().GetTraceId())
	snapshot, err := protojson.Marshal(req.GetReturnLabel())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	repairSynonymRequest := &searchv1.RepairSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				CreatedAt: cachedReturnLabel.GetAudit().GetCreatedAt(),
				CreatedBy: req.GetReturnLabel().GetAudit().GetCreatedBy(),
			},
		},
		Details: &searchv1.RepairSynonymRequest_Details{Payload: snapshot},
	}
	repairSynonymReply, err := grpcx.Call(ctx, s.synonymAdminClient.RepairSynonym, repairSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymAdminService/RepairSynonym")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := returnLabelCalculationServerCompareReturnRequestOptionsSteps[mode]
	compareReturnRequestOptionsRequest := &returnsv1.CompareReturnRequestOptionsRequest{
		Context: req.GetContext(),
		Details: &returnsv1.CompareReturnRequestOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetReturnLabel().GetBillingAddress().GetCountryCode(),
				PostalCode:  req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
	}
	compareReturnRequestOptionsReply, err := run(s, ctx, compareReturnRequestOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/CompareReturnRequestOptions")
	}
	exportPlacementsStream, err := s.placementQueryClient.ExportPlacements(ctx, &merchandisingv1.ExportPlacementsRequest{
		Context: req.GetContext(),
		Filter: &merchandisingv1.ExportPlacementsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetReturnLabel().GetAudit().GetUpdatedAt(),
			OwnerId:      compareReturnRequestOptionsReply.GetReturnRequest().GetCounterparty().GetCustomerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/ExportPlacements")
	}
	for {
		received, err := exportPlacementsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/ExportPlacements")
		}
		_ = received
	}
	return &returnsv1.SimulateReturnLabelResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit:         &commonv1.AuditInfo{Etag: repairSynonymReply.GetSynonym().GetAudit().GetEtag()},
			ReturnLabelId: req.GetReturnLabelId(),
			IssuedAt:      req.GetReturnLabel().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{
			Status: compareReturnRequestOptionsReply.GetResult().GetStatus(),
		},
	}, nil
}

// ExplainReturnLabelCalculation explains a return label.
func (s *ReturnLabelCalculationServer) ExplainReturnLabelCalculation(ctx context.Context, req *returnsv1.ExplainReturnLabelCalculationRequest) (*returnsv1.ExplainReturnLabelCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "explain return label calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	var bic string
	switch choice := req.GetReturnLabel().GetSettlement().(type) {
	case *returnsv1.ReturnLabel_Card:
		bic = choice.Card.GetHolderName()
	case *returnsv1.ReturnLabel_Bank:
		bic = choice.Bank.GetBic()
	case *returnsv1.ReturnLabel_VoucherCode:
		bic = choice.VoucherCode
	}
	explainReturnRequestCalculationRequest := &returnsv1.ExplainReturnRequestCalculationRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Settlement: &returnsv1.ReturnRequest_Bank{Bank: &returnsv1.ReturnRequestBankSettlement{Bic: bic}},
		},
	}
	explainReturnRequestCalculationReply, err := explainReturnRequestCalculationCall(ctx, explainReturnRequestCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/ExplainReturnRequestCalculation")
	}
	return &returnsv1.ExplainReturnLabelCalculationResponse{
		Outcome: &returnsv1.ExplainReturnLabelCalculationResponse_Outcome{
			ReferenceId: explainReturnRequestCalculationReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			Details: explainReturnRequestCalculationReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			ReturnLabelId: req.GetReturnLabelId(),
			Metadata:      req.GetReturnLabel().GetMetadata(),
		},
	}, nil
}

// GetReturnLabelRates returns a return label.
func (s *ReturnLabelCalculationServer) GetReturnLabelRates(ctx context.Context, req *returnsv1.GetReturnLabelRatesRequest) (*returnsv1.GetReturnLabelRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "get_return_label_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get return label rates", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	step := &returnLabelCalculationServerGetReturnLabelRatesSuggestTelemetryReadingsStep{request: &telematicsv1.SuggestTelemetryReadingsRequest{
		Context: req.GetContext(),
		Filter: &telematicsv1.SuggestTelemetryReadingsRequest_Filter{
			OwnerId:      req.GetReturnLabelId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       baggage,
		},
	}}
	if err := s.runGetReturnLabelRatesSuggestTelemetryReadings(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.telematics.v1.TelemetryReadingQueryService/SuggestTelemetryReadings")
	}
	suggestTelemetryReadingsReply := step.reply
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "returns.get_return_label_rates.quote_return_request",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"return_request.counterparty.email": req.GetContext().GetActor().GetEmail(),
			"context.tenant_id":                 req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &returnsv1.GetReturnLabelRatesResponse{
		Result: &commonv1.OperationResult{
			Status:        suggestTelemetryReadingsReply.GetResult().GetStatus(),
			CorrelationId: suggestTelemetryReadingsReply.GetResult().GetCorrelationId(),
		},
		ReturnLabel: &returnsv1.ReturnLabel{ReturnLabelId: req.GetReturnLabelId()},
	}, nil
}

// returnLabelCalculationServerExplainReviewModerationDecisionSteps are the ways to run
// acme.reviews.v1.ReviewModerationValidationService/ExplainReviewModerationDecision, by name.
var returnLabelCalculationServerExplainReviewModerationDecisionSteps = map[string]func(*ReturnLabelCalculationServer, context.Context, *reviewsv1.ExplainReviewModerationDecisionRequest) (*reviewsv1.ExplainReviewModerationDecisionResponse, error){
	"default": (*ReturnLabelCalculationServer).explainReviewModerationDecisionStep,
	"audited": (*ReturnLabelCalculationServer).explainReviewModerationDecisionAuditedStep,
}

func (s *ReturnLabelCalculationServer) explainReviewModerationDecisionStep(ctx context.Context, request *reviewsv1.ExplainReviewModerationDecisionRequest) (*reviewsv1.ExplainReviewModerationDecisionResponse, error) {
	return s.reviewModerationValidationClient.ExplainReviewModerationDecision(ctx, request)
}

func (s *ReturnLabelCalculationServer) explainReviewModerationDecisionAuditedStep(ctx context.Context, request *reviewsv1.ExplainReviewModerationDecisionRequest) (*reviewsv1.ExplainReviewModerationDecisionResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.reviews.v1.ReviewModerationValidationService/ExplainReviewModerationDecision")
	return s.reviewModerationValidationClient.ExplainReviewModerationDecision(ctx, request)
}

// returnLabelCalculationServerCompareReturnRequestOptionsSteps are the ways to run
// acme.returns.v1.ReturnRequestCalculationService/CompareReturnRequestOptions, by name.
var returnLabelCalculationServerCompareReturnRequestOptionsSteps = map[string]func(*ReturnLabelCalculationServer, context.Context, *returnsv1.CompareReturnRequestOptionsRequest) (*returnsv1.CompareReturnRequestOptionsResponse, error){
	"default": (*ReturnLabelCalculationServer).compareReturnRequestOptionsStep,
	"audited": (*ReturnLabelCalculationServer).compareReturnRequestOptionsAuditedStep,
}

func (s *ReturnLabelCalculationServer) compareReturnRequestOptionsStep(ctx context.Context, request *returnsv1.CompareReturnRequestOptionsRequest) (*returnsv1.CompareReturnRequestOptionsResponse, error) {
	return s.returnRequestCalculationClient.CompareReturnRequestOptions(ctx, request)
}

func (s *ReturnLabelCalculationServer) compareReturnRequestOptionsAuditedStep(ctx context.Context, request *returnsv1.CompareReturnRequestOptionsRequest) (*returnsv1.CompareReturnRequestOptionsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.returns.v1.ReturnRequestCalculationService/CompareReturnRequestOptions")
	return s.returnRequestCalculationClient.CompareReturnRequestOptions(ctx, request)
}

// returnLabelCalculationServerGetReturnLabelRatesSuggestTelemetryReadingsStep carries the request of one step of
// GetReturnLabelRates and, once it ran, its reply.
type returnLabelCalculationServerGetReturnLabelRatesSuggestTelemetryReadingsStep struct {
	request *telematicsv1.SuggestTelemetryReadingsRequest
	reply   *telematicsv1.SuggestTelemetryReadingsResponse
}

func (s *ReturnLabelCalculationServer) runGetReturnLabelRatesSuggestTelemetryReadings(ctx context.Context, step *returnLabelCalculationServerGetReturnLabelRatesSuggestTelemetryReadingsStep) error {
	reply, err := s.telemetryReadingQueryClient.SuggestTelemetryReadings(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
