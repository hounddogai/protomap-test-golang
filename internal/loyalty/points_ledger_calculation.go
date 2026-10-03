package loyalty

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PointsLedgerCalculationServer implements `acme.loyalty.v1.PointsLedgerCalculationService`.
// Computes the figures of a points ledger, such as totals, taxes, and estimates.
type PointsLedgerCalculationServer struct {
	loyaltyv1.UnimplementedPointsLedgerCalculationServiceServer
	store                          *Store
	log                            *slog.Logger
	loyaltyMemberCalculationClient loyaltyv1.LoyaltyMemberCalculationServiceClient
	conn                           *grpc.ClientConn
	sessionHistoryClient           identityv1.SessionHistoryServiceClient
	searchQueryValidationClient    searchv1.SearchQueryValidationServiceClient
}

// NewPointsLedgerCalculationServer wires a PointsLedgerCalculationServer.
func NewPointsLedgerCalculationServer(deps platform.Deps) *PointsLedgerCalculationServer {
	return &PointsLedgerCalculationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.loyalty.v1.PointsLedgerCalculationService"),
		loyaltyMemberCalculationClient: loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		conn:                           deps.Conn,
		sessionHistoryClient:           identityv1.NewSessionHistoryServiceClient(deps.Conn),
		searchQueryValidationClient:    searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
	}
}

// CalculatePointsLedger calculates a points ledger.
func (s *PointsLedgerCalculationServer) CalculatePointsLedger(ctx context.Context, req *loyaltyv1.CalculatePointsLedgerRequest) (*loyaltyv1.CalculatePointsLedgerResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "calculate points ledger", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	estimateSellerRequest := &sellersv1.EstimateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{Email: req.GetPointsLedger().GetContact().GetEmail()},
			PrimaryAddress:      &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
			Audit:               &commonv1.AuditInfo{UpdatedBy: req.GetPointsLedger().GetAudit().GetUpdatedBy()},
		},
	}
	estimateSellerReply, err := estimateSellerCall(ctx, estimateSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/EstimateSeller")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*loyaltyv1.ExplainLoyaltyMemberCalculationResponse, len(req.GetPointsLedger().GetLimits()))
	for i, limit := range req.GetPointsLedger().GetLimits() {
		group.Go(func() error {
			reply, err := s.loyaltyMemberCalculationClient.ExplainLoyaltyMemberCalculation(groupCtx, &loyaltyv1.ExplainLoyaltyMemberCalculationRequest{
				Context: req.GetContext(),
				LoyaltyMember: &loyaltyv1.LoyaltyMember{
					Limits: []*loyaltyv1.LoyaltyMemberLimit{&loyaltyv1.LoyaltyMemberLimit{
						Name:   limit.GetName(),
						Amount: limit.GetAmount(),
						Period: limit.GetPeriod(),
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
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/ExplainLoyaltyMemberCalculation")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &loyaltyv1.CalculatePointsLedgerResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			CurrencyCode:   estimateSellerReply.GetBreakdown().GetCurrencyCode(),
			PointsLedgerId: req.GetPointsLedgerId(),
		},
		Breakdown: &loyaltyv1.CalculatePointsLedgerResponse_Breakdown{CurrencyCode: req.GetPointsLedger().GetCurrencyCode()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimatePointsLedger estimates a points ledger.
func (s *PointsLedgerCalculationServer) EstimatePointsLedger(ctx context.Context, req *loyaltyv1.EstimatePointsLedgerRequest) (*loyaltyv1.EstimatePointsLedgerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimatePointsLedger", "error", err)
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "estimate points ledger", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*loyaltyv1.EstimatePointsLedgerRequest_Details)
	browseFormsRequest := &formsv1.BrowseFormsRequest{
		Context: req.GetContext(),
		Filter: &formsv1.BrowseFormsRequest_Filter{
			Region:       cloned.GetDestination().GetRegion(),
			Labels:       req.GetPointsLedger().GetLabels(),
			CreatedAfter: req.GetPointsLedger().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetPointsLedger().GetHolder().GetRewardId(),
		},
	}
	browseFormsReply, err := grpcx.NewClient(s.conn, formsv1.NewFormQueryServiceClient).BrowseForms(ctx, browseFormsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forms.v1.FormQueryService/BrowseForms")
	}
	simulateLoyaltyMemberRequest := &loyaltyv1.SimulateLoyaltyMemberRequest{}
	simulateLoyaltyMemberRequest.Context = req.GetContext()
	simulateLoyaltyMemberRequest.LoyaltyMember = &loyaltyv1.LoyaltyMember{
		Funding: &loyaltyv1.LoyaltyMember_Card{Card: &loyaltyv1.LoyaltyMemberCardFunding{Last4: req.GetPointsLedger().GetCard().GetLast4()}},
	}
	simulateLoyaltyMemberReply, err := s.loyaltyMemberCalculationClient.SimulateLoyaltyMember(ctx, simulateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/SimulateLoyaltyMember")
	}
	return &loyaltyv1.EstimatePointsLedgerResponse{
		Result: &commonv1.OperationResult{
			Details:       browseFormsReply.GetResult().GetDetails(),
			CorrelationId: browseFormsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: simulateLoyaltyMemberReply.GetLoyaltyMember().GetContact(),
			Audit: &commonv1.AuditInfo{
				CreatedBy: simulateLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetCreatedBy(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			CurrencyCode:   req.GetDetails().GetCurrencyCode(),
		},
	}, nil
}

// QuotePointsLedger quotes a points ledger.
func (s *PointsLedgerCalculationServer) QuotePointsLedger(ctx context.Context, req *loyaltyv1.QuotePointsLedgerRequest) (*loyaltyv1.QuotePointsLedgerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuotePointsLedger", "error", err)
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "quote points ledger", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	jobs := make(chan *loyaltyv1.PointsLedgerLimit)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for limit := range jobs {
				if _, err := s.loyaltyMemberCalculationClient.CalculateLoyaltyMember(workCtx, &loyaltyv1.CalculateLoyaltyMemberRequest{
					Context: req.GetContext(),
					LoyaltyMember: &loyaltyv1.LoyaltyMember{
						Limits: []*loyaltyv1.LoyaltyMemberLimit{&loyaltyv1.LoyaltyMemberLimit{
							Name:   limit.GetName(),
							Period: limit.GetPeriod(),
							Amount: limit.GetAmount(),
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
	for _, queued := range req.GetPointsLedger().GetLimits() {
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
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CalculateLoyaltyMember")
	}
	return &loyaltyv1.QuotePointsLedgerResponse{
		PointsLedger: &loyaltyv1.PointsLedger{PointsLedgerId: req.GetPointsLedgerId()},
		Breakdown:    &loyaltyv1.QuotePointsLedgerResponse_Breakdown{CurrencyCode: req.GetPointsLedger().GetCurrencyCode()},
		Result:       &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ComparePointsLedgerOptions compares a points ledger.
func (s *PointsLedgerCalculationServer) ComparePointsLedgerOptions(ctx context.Context, req *loyaltyv1.ComparePointsLedgerOptionsRequest) (*loyaltyv1.ComparePointsLedgerOptionsResponse, error) {
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "compare points ledger options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	quoteLoyaltyMemberRequest := &loyaltyv1.QuoteLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{Etag: req.GetPointsLedger().GetAudit().GetEtag()},
		},
	}
	quoteLoyaltyMemberReply, err := grpcx.Call(ctx, s.loyaltyMemberCalculationClient.QuoteLoyaltyMember, quoteLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/QuoteLoyaltyMember")
	}
	return &loyaltyv1.ComparePointsLedgerOptionsResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			Available:      quoteLoyaltyMemberReply.GetLoyaltyMember().GetAvailable(),
			CurrencyCode:   quoteLoyaltyMemberReply.GetBreakdown().GetCurrencyCode(),
			PointsLedgerId: req.GetPointsLedgerId(),
			Balance:        req.GetPointsLedger().GetBalance(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculatePointsLedgers recalculates a points ledger.
func (s *PointsLedgerCalculationServer) RecalculatePointsLedgers(ctx context.Context, req *loyaltyv1.RecalculatePointsLedgersRequest) (*loyaltyv1.RecalculatePointsLedgersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "recalculate points ledgers", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "token", req.GetPointsLedger().GetCard().GetToken())
	calculateLoyaltyMemberRequest := &loyaltyv1.CalculateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Email: req.GetPointsLedger().GetContact().GetEmail()},
			Holder:  &loyaltyv1.RewardRef{Version: req.GetPointsLedger().GetHolder().GetVersion()},
			Funding: &loyaltyv1.LoyaltyMember_Card{Card: &loyaltyv1.LoyaltyMemberCardFunding{Token: tenancy.Value(ctx, "token")}},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetPointsLedger().GetAudit().GetCreatedAt()},
		},
	}
	calculateLoyaltyMemberReply, err := loyaltyv1.LoyaltyMemberCalculationServiceClient.CalculateLoyaltyMember(s.loyaltyMemberCalculationClient, ctx, calculateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CalculateLoyaltyMember")
	}
	return &loyaltyv1.RecalculatePointsLedgersResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			CurrencyCode:   calculateLoyaltyMemberReply.GetLoyaltyMember().GetCurrencyCode(),
			PointsLedgerId: req.GetPointsLedgerId(),
		},
		Breakdown: &loyaltyv1.RecalculatePointsLedgersResponse_Breakdown{CurrencyCode: req.GetPointsLedger().GetCurrencyCode()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SimulatePointsLedger simulates a points ledger.
func (s *PointsLedgerCalculationServer) SimulatePointsLedger(ctx context.Context, req *loyaltyv1.SimulatePointsLedgerRequest) (*loyaltyv1.SimulatePointsLedgerResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "simulate points ledger", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	compareLoyaltyMemberOptionsRequest := &loyaltyv1.CompareLoyaltyMemberOptionsRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Holder: &loyaltyv1.RewardRef{
				Version:  req.GetPointsLedger().GetHolder().GetVersion(),
				RewardId: req.GetPointsLedger().GetHolder().GetRewardId(),
			},
		},
	}
	compareLoyaltyMemberOptionsReply := new(loyaltyv1.CompareLoyaltyMemberOptionsResponse)
	if err := s.conn.Invoke(ctx, loyaltyLoyaltyMemberCalculationServiceCompareLoyaltyMemberOptionsMethod, compareLoyaltyMemberOptionsRequest, compareLoyaltyMemberOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CompareLoyaltyMemberOptions")
	}
	calculateSellerPayoutScheduleRequest := &sellersv1.CalculateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: req.GetPointsLedger().GetContact().GetPreferredChannel(),
				},
			}},
			Organizer: &identityv1.UserRef{
				DisplayName: compareLoyaltyMemberOptionsReply.GetLoyaltyMember().GetHolder().GetDisplayName(),
			},
			Location: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
		},
	}
	calculateSellerPayoutScheduleReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerPayoutScheduleCalculationServiceClient).CalculateSellerPayoutSchedule(ctx, calculateSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/CalculateSellerPayoutSchedule")
	}
	return &loyaltyv1.SimulatePointsLedgerResponse{
		Result: &commonv1.OperationResult{
			Message: compareLoyaltyMemberOptionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			Available:    compareLoyaltyMemberOptionsReply.GetLoyaltyMember().GetAvailable(),
			CurrencyCode: calculateSellerPayoutScheduleReply.GetBreakdown().GetCurrencyCode(),
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetUpdatedBy(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			Contact:        req.GetPointsLedger().GetContact(),
		},
	}, nil
}

// ExplainPointsLedgerCalculation explains a points ledger.
func (s *PointsLedgerCalculationServer) ExplainPointsLedgerCalculation(ctx context.Context, req *loyaltyv1.ExplainPointsLedgerCalculationRequest) (*loyaltyv1.ExplainPointsLedgerCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "explain points ledger calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPointsLedger()).(*loyaltyv1.PointsLedger)
	encoded, err := protojson.Marshal(req.GetPointsLedger())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded loyaltyv1.PointsLedger
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	snapshot, err := protojson.Marshal(req.GetPointsLedger())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertSessionReply, err := s.sessionHistoryClient.RevertSession(ctx, &identityv1.RevertSessionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Requester: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Name:  cloned.GetContact().GetName(),
				Email: (&decoded).GetContact().GetEmail(),
			},
			Assignee: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
		},
		Details: &identityv1.RevertSessionRequest_Details{Snapshot: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/RevertSession")
	}
	ctx = tenancy.With(ctx, "phone", req.GetPointsLedger().GetContact().GetPhone())
	packed, err := anypb.New(req.GetPointsLedger())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintSearchQueriesRequest := &searchv1.LintSearchQueriesRequest{
		Context: req.GetContext(),
		Details: &searchv1.LintSearchQueriesRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: tenancy.Value(ctx, "phone"),
				Email: revertSessionReply.GetSession().GetAssignee().GetEmail(),
			},
		},
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: revertSessionReply.GetSession().GetAssignee().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	lintSearchQueriesReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*searchv1.LintSearchQueriesResponse, error) {
		return s.searchQueryValidationClient.LintSearchQueries(ctx, lintSearchQueriesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/LintSearchQueries")
	}
	return &loyaltyv1.ExplainPointsLedgerCalculationResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{
				Email: revertSessionReply.GetSession().GetAssignee().GetEmail(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			Status:         req.GetPointsLedger().GetStatus(),
		},
		Outcome: &loyaltyv1.ExplainPointsLedgerCalculationResponse_Outcome{
			ReferenceId: lintSearchQueriesReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPointsLedgerRates returns a points ledger.
func (s *PointsLedgerCalculationServer) GetPointsLedgerRates(ctx context.Context, req *loyaltyv1.GetPointsLedgerRatesRequest) (*loyaltyv1.GetPointsLedgerRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "points_ledger", req.GetPointsLedgerId(), "get_points_ledger_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get points ledger rates", "tenant", req.GetContext().GetTenantId())
	pruneUserVersionsRequest := &identityv1.PruneUserVersionsRequest{
		Context: req.GetContext(),
		UserId:  req.GetContext().GetActor().GetUserId(),
	}
	pruneUserVersionsReply, err := grpcx.NewClient(s.conn, identityv1.NewUserHistoryServiceClient).PruneUserVersions(ctx, pruneUserVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/PruneUserVersions")
	}
	return &loyaltyv1.GetPointsLedgerRatesResponse{
		Result: &commonv1.OperationResult{
			Message: pruneUserVersionsReply.GetResult().GetMessage(),
			Status:  pruneUserVersionsReply.GetResult().GetStatus(),
		},
		PointsLedger: &loyaltyv1.PointsLedger{PointsLedgerId: req.GetPointsLedgerId()},
	}, nil
}
