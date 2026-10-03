package cart

import (
	"context"
	"errors"
	"io"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	recruitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recruiting/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// CartServer implements `acme.cart.v1.CartService`.
// Manages carts: creates, reads, changes, and lists them.
type CartServer struct {
	*cartHandlers
}

// cartHandlers holds the handlers of CartServer, with what they use.
type cartHandlers struct {
	cartv1.UnimplementedCartServiceServer
	store                                *Store
	log                                  *slog.Logger
	getSavedListRatesFn                  func(context.Context, *cartv1.GetSavedListRatesRequest, ...grpc.CallOption) (*cartv1.GetSavedListRatesResponse, error)
	payoutCalculationClient              paymentsv1.PayoutCalculationServiceClient
	savedListValidationClient            cartv1.SavedListValidationServiceClient
	cartRuleCalculationClient            cartv1.CartRuleCalculationServiceClient
	jobOpeningCalculationClient          recruitingv1.JobOpeningCalculationServiceClient
	conn                                 *grpc.ClientConn
	roleHistoryClient                    identityv1.RoleHistoryServiceClient
	savedListQueryClient                 cartv1.SavedListQueryServiceClient
	synonymSyncClient                    searchv1.SynonymSyncServiceClient
	sellerValidationClient               sellersv1.SellerValidationServiceClient
	meteredCartRuleCalculation           meteredCartRuleCalculation
	sellerPayoutScheduleValidationClient sellersv1.SellerPayoutScheduleValidationServiceClient
	cartCalculationClient                cartv1.CartCalculationServiceClient
	savedListCalculationClient           cartv1.SavedListCalculationServiceClient
}

// NewCartServer wires a CartServer.
func NewCartServer(deps platform.Deps) *CartServer {
	savedListCalculation := cartv1.NewSavedListCalculationServiceClient(deps.Conn)
	return &CartServer{cartHandlers: &cartHandlers{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.cart.v1.CartService"),
		getSavedListRatesFn:                  savedListCalculation.GetSavedListRates,
		payoutCalculationClient:              paymentsv1.NewPayoutCalculationServiceClient(deps.Conn),
		savedListValidationClient:            cartv1.NewSavedListValidationServiceClient(deps.Conn),
		cartRuleCalculationClient:            cartv1.NewCartRuleCalculationServiceClient(deps.Conn),
		jobOpeningCalculationClient:          recruitingv1.NewJobOpeningCalculationServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		roleHistoryClient:                    identityv1.NewRoleHistoryServiceClient(deps.Conn),
		savedListQueryClient:                 cartv1.NewSavedListQueryServiceClient(deps.Conn),
		synonymSyncClient:                    searchv1.NewSynonymSyncServiceClient(deps.Conn),
		sellerValidationClient:               sellersv1.NewSellerValidationServiceClient(deps.Conn),
		meteredCartRuleCalculation:           meteredCartRuleCalculation{CartRuleCalculationServiceClient: cartv1.NewCartRuleCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerPayoutScheduleValidationClient: sellersv1.NewSellerPayoutScheduleValidationServiceClient(deps.Conn),
		cartCalculationClient:                cartv1.NewCartCalculationServiceClient(deps.Conn),
		savedListCalculationClient:           cartv1.NewSavedListCalculationServiceClient(deps.Conn),
	}}
}

// CreateCart creates a cart.
func (s *cartHandlers) CreateCart(ctx context.Context, req *cartv1.CreateCartRequest) (*cartv1.CreateCartResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "create cart", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	getSavedListRatesRequest := &cartv1.GetSavedListRatesRequest{
		Context:     req.GetContext(),
		SavedListId: req.GetCart().GetCounterparty().GetSavedListId(),
	}
	getSavedListRatesReply, err := s.getSavedListRatesFn(ctx, getSavedListRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListCalculationService/GetSavedListRates")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*paymentsv1.CalculatePayoutResponse, len(req.GetCart().GetLines()))
	for i, line := range req.GetCart().GetLines() {
		group.Go(func() error {
			reply, err := s.payoutCalculationClient.CalculatePayout(groupCtx, &paymentsv1.CalculatePayoutRequest{
				Context: req.GetContext(),
				Payout: &paymentsv1.Payout{
					Lines: []*paymentsv1.PayoutLine{&paymentsv1.PayoutLine{
						Tax: &paymentsv1.PayoutLineTax{
							Exemptions:   line.GetTax().GetExemptions(),
							Amount:       line.GetTax().GetAmount(),
							Jurisdiction: line.GetTax().GetJurisdiction(),
						},
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
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/CalculatePayout")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	for _, line2 := range req.GetCart().GetLines() {
		if _, err := s.savedListValidationClient.ExplainSavedListDecision(ctx, &cartv1.ExplainSavedListDecisionRequest{
			Context: req.GetContext(),
			SavedList: &cartv1.SavedList{
				Lines: []*cartv1.SavedListLine{&cartv1.SavedListLine{
					Tax:    &cartv1.SavedListLineTax{Rate: line2.GetTax().GetRate()},
					Amount: line2.GetTax().GetAmount(),
					Sku:    line2.GetSku(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListValidationService/ExplainSavedListDecision")
		}
	}
	return &cartv1.CreateCartResponse{
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				Locality: getSavedListRatesReply.GetSavedList().GetBillingAddress().GetLocality(),
			},
			DueAt:    getSavedListRatesReply.GetSavedList().GetDueAt(),
			Metadata: req.GetCart().GetMetadata(),
			CartId:   req.GetCart().GetCartId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetCart returns a cart.
func (s *cartHandlers) GetCart(ctx context.Context, req *cartv1.GetCartRequest) (*cartv1.GetCartResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart", req.GetCartId(), "get_cart"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get cart", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	validateSavedListRequest := &cartv1.ValidateSavedListRequest{
		Context: req.GetContext(),
		Details: &cartv1.ValidateSavedListRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		SavedList: &cartv1.SavedList{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
		Annotations: baggage,
	}
	var validateSavedListReply *cartv1.ValidateSavedListResponse
	if err := pipeline.Run(ctx, "acme.cart.v1.SavedListValidationService/ValidateSavedList", func(ctx context.Context) error {
		answer, err := s.savedListValidationClient.ValidateSavedList(ctx, validateSavedListRequest)
		validateSavedListReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListValidationService/ValidateSavedList")
	}
	estimateCartRuleReply, err := s.cartRuleCalculationClient.EstimateCartRule(ctx, &cartv1.EstimateCartRuleRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				Etag: validateSavedListReply.GetSavedList().GetAudit().GetEtag(),
			},
		},
		Details: &cartv1.EstimateCartRuleRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: validateSavedListReply.GetSavedList().GetBillingAddress().GetRegion(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/EstimateCartRule")
	}
	return &cartv1.GetCartResponse{
		Cart: &cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{
				SavedListId: validateSavedListReply.GetSavedList().GetSavedListId(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: estimateCartRuleReply.GetCartRule().GetAudit().GetEtag(),
			},
			CartId: req.GetCartId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DeleteCart deletes a cart.
func (s *cartHandlers) DeleteCart(ctx context.Context, req *cartv1.DeleteCartRequest) (*cartv1.DeleteCartResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart", req.GetCartId(), "delete_cart"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete cart", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "etag", req.GetEtag())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	quoteJobOpeningRequest := &recruitingv1.QuoteJobOpeningRequest{}
	quoteJobOpeningRequest.Context = req.GetContext()
	quoteJobOpeningRequest.JobOpening = &recruitingv1.JobOpening{
		Audit: &commonv1.AuditInfo{
			Etag:      tenancy.Value(ctx, "etag"),
			CreatedAt: req.GetContext().GetDeadline(),
		},
		Lines: []*recruitingv1.JobOpeningLine{&recruitingv1.JobOpeningLine{Attributes: baggage}},
	}
	quoteJobOpeningRequest.Reason = req.GetReason()
	quoteJobOpeningReply, err := s.jobOpeningCalculationClient.QuoteJobOpening(ctx, quoteJobOpeningRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.recruiting.v1.JobOpeningCalculationService/QuoteJobOpening")
	}
	lintCartsRequest := &cartv1.LintCartsRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				Recipient:  quoteJobOpeningReply.GetJobOpening().GetBillingAddress().GetRecipient(),
				PostalCode: quoteJobOpeningReply.GetJobOpening().GetBillingAddress().GetPostalCode(),
			},
			Settlement: &cartv1.Cart_Card{Card: &cartv1.CartCardSettlement{
				Token: quoteJobOpeningReply.GetJobOpening().GetCard().GetToken(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: quoteJobOpeningReply.GetJobOpening().GetAudit().GetCreatedBy(),
			},
		},
	}
	lintCartsReply := new(cartv1.LintCartsResponse)
	if err := s.conn.Invoke(ctx, cartCartValidationServiceLintCartsMethod, lintCartsRequest, lintCartsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/LintCarts")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := cartHandlersRevertRoleSteps[mode]
	revertRoleRequest := &identityv1.RevertRoleRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				DisplayName: quoteJobOpeningReply.GetJobOpening().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: lintCartsReply.GetCart().GetAudit().GetUpdatedBy(),
				CreatedAt: quoteJobOpeningReply.GetJobOpening().GetAudit().GetCreatedAt(),
			},
		},
	}
	revertRoleReply, err := run(s, ctx, revertRoleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/RevertRole")
	}
	return &cartv1.DeleteCartResponse{
		Result: &commonv1.OperationResult{
			Status:       quoteJobOpeningReply.GetResult().GetStatus(),
			Message:      lintCartsReply.GetResult().GetMessage(),
			Details:      lintCartsReply.GetResult().GetDetails(),
			ReferenceIds: revertRoleReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// ListCarts lists carts a page at a time.
func (s *cartHandlers) ListCarts(ctx context.Context, req *cartv1.ListCartsRequest) (*cartv1.ListCartsResponse, error) {
	s.log.InfoContext(ctx, "list carts", "tenant", req.GetContext().GetTenantId())
	previewSellerRequest := &sellersv1.PreviewSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			PrimaryAddress:      &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			RelationshipManager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	previewSellerReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerValidationServiceClient).PreviewSeller(ctx, previewSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/PreviewSeller")
	}
	summarizeSavedListsReply, err := s.savedListQueryClient.SummarizeSavedLists(ctx, &cartv1.SummarizeSavedListsRequest{
		Context: req.GetContext(),
		Details: &cartv1.SummarizeSavedListsRequest_Details{
			Period:  previewSellerReply.GetSeller().GetPreferences().GetQuietHours(),
			Filters: req.GetFilter().GetLabels(),
		},
		Annotations: previewSellerReply.GetResult().GetDetails(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListQueryService/SummarizeSavedLists")
	}
	return &cartv1.ListCartsResponse{
		Carts: []*cartv1.Cart{&cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: previewSellerReply.GetSeller().GetPrimaryAddress().GetCountryCode(),
			},
		}},
		Result: &commonv1.OperationResult{
			Status:        previewSellerReply.GetResult().GetStatus(),
			CorrelationId: summarizeSavedListsReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// BatchGetCarts returns several carts at once.
func (s *cartHandlers) BatchGetCarts(ctx context.Context, req *cartv1.BatchGetCartsRequest) (*cartv1.BatchGetCartsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "batch get carts", "tenant", req.GetContext().GetTenantId())
	scoreSavedListReply, err := s.savedListValidationClient.ScoreSavedList(ctx, &cartv1.ScoreSavedListRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Lines: []*cartv1.SavedListLine{&cartv1.SavedListLine{Attributes: req.GetContext().GetBaggage()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListValidationService/ScoreSavedList")
	}
	return &cartv1.BatchGetCartsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scoreSavedListReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Carts: []*cartv1.Cart{&cartv1.Cart{Total: scoreSavedListReply.GetBreakdown().GetTotal()}},
	}, nil
}

// SearchCarts lists carts a page at a time.
func (s *cartHandlers) SearchCarts(ctx context.Context, req *cartv1.SearchCartsRequest) (*cartv1.SearchCartsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SearchCarts", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "search carts", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveSynonymConflictReply, err := s.synonymSyncClient.ResolveSynonymConflict(ctx, &searchv1.ResolveSynonymConflictRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &searchv1.ResolveSynonymConflictRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ResolveSynonymConflict")
	}
	normalizeSellerRequest := &sellersv1.NormalizeSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: resolveSynonymConflictReply.GetSynonym().GetAudit().GetUpdatedAt(),
				Etag:      resolveSynonymConflictReply.GetSynonym().GetAudit().GetEtag(),
			},
			Contact: &commonv1.ContactInfo{
				Email: resolveSynonymConflictReply.GetSynonym().GetOwner().GetEmail(),
			},
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	normalizeSellerReply, err := sellersv1.SellerValidationServiceClient.NormalizeSeller(s.sellerValidationClient, ctx, normalizeSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/NormalizeSeller")
	}
	return &cartv1.SearchCartsResponse{
		Result: &commonv1.OperationResult{
			Details: resolveSynonymConflictReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Carts: []*cartv1.Cart{&cartv1.Cart{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizeSellerReply.GetSeller().GetAudit().GetUpdatedAt(),
			},
		}},
	}, nil
}

// WatchCarts streams the changes of carts as they happen.
func (s *cartHandlers) WatchCarts(req *cartv1.WatchCartsRequest, stream grpc.ServerStreamingServer[cartv1.WatchCartsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "watch carts", "tenant", req.GetContext().GetTenantId())
	quoteCartRuleRequest := buildQuoteCartRuleRequestForWatchCarts(req)
	quoteCartRuleReply, err := s.cartRuleCalculationClient.QuoteCartRule(ctx, quoteCartRuleRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/QuoteCartRule")
	}
	pushSynonymRequest := newPushSynonymRequest(withPushSynonymContext(req.GetContext()), withPushSynonymSynonym(&searchv1.Synonym{
		Audit: &commonv1.AuditInfo{
			CreatedAt: quoteCartRuleReply.GetCartRule().GetAudit().GetCreatedAt(),
			UpdatedBy: quoteCartRuleReply.GetCartRule().GetAudit().GetUpdatedBy(),
		},
		Owner: &identityv1.UserRef{
			DisplayName: quoteCartRuleReply.GetCartRule().GetOwner().GetDisplayName(),
		},
		Action: &searchv1.Synonym_Block{Block: &searchv1.SynonymBlockAction{
			Reason: quoteCartRuleReply.GetCartRule().GetBlock().GetReason(),
		}},
	}))
	pushSynonymReply, err := s.synonymSyncClient.PushSynonym(ctx, pushSynonymRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PushSynonym")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := cartHandlersFindSavedListsSteps[mode]
	findSavedListsRequest := &cartv1.FindSavedListsRequest{
		Context: req.GetContext(),
		Filter:  &cartv1.FindSavedListsRequest_Filter{OwnerId: req.GetFilter().GetOwnerId()},
	}
	findSavedListsReply, err := run(s, ctx, findSavedListsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.cart.v1.SavedListQueryService/FindSavedLists")
	}
	if err := stream.Send(&cartv1.WatchCartsResponse{
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{
				CreatedBy: quoteCartRuleReply.GetCartRule().GetAudit().GetCreatedBy(),
			},
			Lines: []*cartv1.CartLine{&cartv1.CartLine{
				Description: quoteCartRuleReply.GetCartRule().GetDescription(),
			}},
			Counterparty: &cartv1.SavedListRef{
				DisplayName: pushSynonymReply.GetSynonym().GetOwner().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{
			CorrelationId: findSavedListsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  findSavedListsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreateCarts takes a stream of carts and answers once at the end.
func (s *cartHandlers) BatchCreateCarts(stream grpc.ClientStreamingServer[cartv1.BatchCreateCartsRequest, cartv1.BatchCreateCartsResponse]) error {
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
		s.log.InfoContext(ctx, "batch create carts", "tenant", msg.GetContext().GetTenantId())
		compareCartRuleOptionsRequest := &cartv1.CompareCartRuleOptionsRequest{
			Context: msg.GetContext(),
			CartRule: &cartv1.CartRule{
				Audit: &commonv1.AuditInfo{
					CreatedBy: msg.GetRecord().GetAudit().GetCreatedBy(),
					UpdatedAt: msg.GetRecord().GetAudit().GetUpdatedAt(),
				},
			},
			Details: &cartv1.CompareCartRuleOptionsRequest_Details{
				Destination: &commonv1.PostalAddress{
					Locality:    msg.GetRecord().GetBillingAddress().GetLocality(),
					CountryCode: msg.GetRecord().GetBillingAddress().GetCountryCode(),
				},
			},
		}
		if _, err := s.meteredCartRuleCalculation.CompareCartRuleOptions(ctx, compareCartRuleOptionsRequest); err != nil {
			return grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/CompareCartRuleOptions")
		}
		summarizeSavedListsRequest := &cartv1.SummarizeSavedListsRequest{}
		summarizeSavedListsRequest.Context = msg.GetContext()
		summarizeSavedListsRequest.SavedListId = msg.GetRecord().GetCounterparty().GetSavedListId()
		if _, err := s.savedListQueryClient.SummarizeSavedLists(ctx, summarizeSavedListsRequest); err != nil {
			return grpcx.Wrap(err, "acme.cart.v1.SavedListQueryService/SummarizeSavedLists")
		}
		if _, err := s.sellerPayoutScheduleValidationClient.ExplainSellerPayoutScheduleDecision(ctx, &sellersv1.ExplainSellerPayoutScheduleDecisionRequest{
			Context: msg.GetContext(),
			SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
				Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
					Contact: &commonv1.ContactInfo{
						PreferredChannel: msg.GetHeader().GetSubmittedBy().GetPreferredChannel(),
					},
				}},
			},
			Details: &sellersv1.ExplainSellerPayoutScheduleDecisionRequest_Details{
				Contact: &commonv1.ContactInfo{Email: msg.GetHeader().GetSubmittedBy().GetEmail()},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/ExplainSellerPayoutScheduleDecision")
		}
		accepted++
	}
	return stream.SendAndClose(&cartv1.BatchCreateCartsResponse{Accepted: accepted})
}

// CloneCart creates a cart.
func (s *cartHandlers) CloneCart(ctx context.Context, req *cartv1.CloneCartRequest) (*cartv1.CloneCartResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "clone cart", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "created_by", req.GetCart().GetAudit().GetCreatedBy())
	estimateCartRequest := &cartv1.EstimateCartRequest{
		Context: req.GetContext(),
		Details: &cartv1.EstimateCartRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:    req.GetCart().GetBillingAddress().GetLines(),
				Locality: req.GetCart().GetBillingAddress().GetLocality(),
			},
		},
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{CreatedBy: tenancy.Value(ctx, "created_by")},
		},
	}
	estimateCartReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*cartv1.EstimateCartResponse, error) {
		return s.cartCalculationClient.EstimateCart(ctx, estimateCartRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/EstimateCart")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*cartv1.CalculateSavedListResponse, len(req.GetCart().GetAttachments()))
	for i, attachment := range req.GetCart().GetAttachments() {
		group.Go(func() error {
			reply, err := s.savedListCalculationClient.CalculateSavedList(groupCtx, &cartv1.CalculateSavedListRequest{
				Context: req.GetContext(),
				SavedList: &cartv1.SavedList{
					Attachments: []*cartv1.SavedListAttachment{&cartv1.SavedListAttachment{
						Checksum:    attachment.GetChecksum(),
						ContentType: attachment.GetContentType(),
						FileName:    attachment.GetFileName(),
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
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListCalculationService/CalculateSavedList")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	findSavedListsRequest := &cartv1.FindSavedListsRequest{
		Context: req.GetContext(),
		Filter: &cartv1.FindSavedListsRequest_Filter{
			Region:       estimateCartReply.GetCart().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetCart().GetAudit().GetUpdatedAt(),
		},
	}
	type findSavedListsResult struct {
		reply *cartv1.FindSavedListsResponse
		err   error
	}
	findSavedListsResults := make(chan findSavedListsResult, 1)
	go func() {
		reply, err := s.savedListQueryClient.FindSavedLists(ctx, findSavedListsRequest)
		findSavedListsResults <- findSavedListsResult{reply, err}
	}()
	var findSavedListsReply *cartv1.FindSavedListsResponse
	select {
	case result := <-findSavedListsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.cart.v1.SavedListQueryService/FindSavedLists")
		}
		findSavedListsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	estimateJobOpeningRequest := &recruitingv1.EstimateJobOpeningRequest{
		Context: req.GetContext(),
		JobOpening: &recruitingv1.JobOpening{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: estimateCartReply.GetCart().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	type estimateJobOpeningResult struct {
		reply *recruitingv1.EstimateJobOpeningResponse
		err   error
	}
	estimateJobOpeningResults := make(chan estimateJobOpeningResult, 1)
	go func() {
		reply, err := s.jobOpeningCalculationClient.EstimateJobOpening(ctx, estimateJobOpeningRequest)
		estimateJobOpeningResults <- estimateJobOpeningResult{reply, err}
	}()
	var estimateJobOpeningReply *recruitingv1.EstimateJobOpeningResponse
	select {
	case result2 := <-estimateJobOpeningResults:
		if result2.err != nil {
			return nil, grpcx.Wrap(result2.err, "acme.recruiting.v1.JobOpeningCalculationService/EstimateJobOpening")
		}
		estimateJobOpeningReply = result2.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &cartv1.CloneCartResponse{
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: estimateCartReply.GetCart().GetBillingAddress().GetCountryCode(),
			},
			Counterparty: &cartv1.SavedListRef{
				DisplayName: estimateCartReply.GetCart().GetCounterparty().GetDisplayName(),
			},
			IssuedAt: req.GetCart().GetIssuedAt(),
			Metadata: req.GetCart().GetMetadata(),
		},
		Result: &commonv1.OperationResult{
			Status:       findSavedListsReply.GetResult().GetStatus(),
			Details:      findSavedListsReply.GetResult().GetDetails(),
			ReferenceIds: estimateJobOpeningReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// cartHandlersRevertRoleSteps are the ways to run acme.identity.v1.RoleHistoryService/RevertRole, by name.
var cartHandlersRevertRoleSteps = map[string]func(*cartHandlers, context.Context, *identityv1.RevertRoleRequest) (*identityv1.RevertRoleResponse, error){
	"default": (*cartHandlers).revertRoleStep,
	"audited": (*cartHandlers).revertRoleAuditedStep,
}

func (s *cartHandlers) revertRoleStep(ctx context.Context, request *identityv1.RevertRoleRequest) (*identityv1.RevertRoleResponse, error) {
	return s.roleHistoryClient.RevertRole(ctx, request)
}

func (s *cartHandlers) revertRoleAuditedStep(ctx context.Context, request *identityv1.RevertRoleRequest) (*identityv1.RevertRoleResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.RoleHistoryService/RevertRole")
	return s.roleHistoryClient.RevertRole(ctx, request)
}

// cartHandlersFindSavedListsSteps are the ways to run acme.cart.v1.SavedListQueryService/FindSavedLists, by name.
var cartHandlersFindSavedListsSteps = map[string]func(*cartHandlers, context.Context, *cartv1.FindSavedListsRequest) (*cartv1.FindSavedListsResponse, error){
	"default": (*cartHandlers).findSavedListsStep,
	"audited": (*cartHandlers).findSavedListsAuditedStep,
}

func (s *cartHandlers) findSavedListsStep(ctx context.Context, request *cartv1.FindSavedListsRequest) (*cartv1.FindSavedListsResponse, error) {
	return s.savedListQueryClient.FindSavedLists(ctx, request)
}

func (s *cartHandlers) findSavedListsAuditedStep(ctx context.Context, request *cartv1.FindSavedListsRequest) (*cartv1.FindSavedListsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.cart.v1.SavedListQueryService/FindSavedLists")
	return s.savedListQueryClient.FindSavedLists(ctx, request)
}
