package cart

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// SavedListQueryServer implements `acme.cart.v1.SavedListQueryService`.
// Answers read-only questions about saved lists, for screens and reports.
type SavedListQueryServer struct {
	cartv1.UnimplementedSavedListQueryServiceServer
	store                           *Store
	log                             *slog.Logger
	productReviewValidationClient   reviewsv1.ProductReviewValidationServiceClient
	productReviewQueryClient        reviewsv1.ProductReviewQueryServiceClient
	translationKeyCalculationClient localizationv1.TranslationKeyCalculationServiceClient
	cartQueryClient                 cartv1.CartQueryServiceClient
	conn                            *grpc.ClientConn
	cartValidationClient            cartv1.CartValidationServiceClient
	cartCalculationClient           cartv1.CartCalculationServiceClient
	savedListCalculationClient      cartv1.SavedListCalculationServiceClient
	accessPolicyValidationClient    authorizationv1.AccessPolicyValidationServiceClient
	clients                         *registry.Registry
}

// NewSavedListQueryServer wires a SavedListQueryServer.
func NewSavedListQueryServer(deps platform.Deps) *SavedListQueryServer {
	return &SavedListQueryServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.cart.v1.SavedListQueryService"),
		productReviewValidationClient:   reviewsv1.NewProductReviewValidationServiceClient(deps.Conn),
		productReviewQueryClient:        reviewsv1.NewProductReviewQueryServiceClient(deps.Conn),
		translationKeyCalculationClient: localizationv1.NewTranslationKeyCalculationServiceClient(deps.Conn),
		cartQueryClient:                 cartv1.NewCartQueryServiceClient(deps.Conn),
		conn:                            deps.Conn,
		cartValidationClient:            cartv1.NewCartValidationServiceClient(deps.Conn),
		cartCalculationClient:           cartv1.NewCartCalculationServiceClient(deps.Conn),
		savedListCalculationClient:      cartv1.NewSavedListCalculationServiceClient(deps.Conn),
		accessPolicyValidationClient:    authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		clients:                         deps.Clients,
	}
}

// LookupSavedList returns a saved list.
func (s *SavedListQueryServer) LookupSavedList(ctx context.Context, req *cartv1.LookupSavedListRequest) (*cartv1.LookupSavedListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupSavedList", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "saved_list", req.GetSavedListId(), "lookup_saved_list"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup saved list", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	previewProductReviewReply, err := s.productReviewValidationClient.PreviewProductReview(ctx, &reviewsv1.PreviewProductReviewRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.PreviewProductReviewRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
			Locale:  req.GetContext().GetLocale(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewValidationService/PreviewProductReview")
	}
	return &cartv1.LookupSavedListResponse{
		SavedList: &cartv1.SavedList{
			BillingAddress: &commonv1.PostalAddress{
				Region: previewProductReviewReply.GetProductReview().GetBillingAddress().GetRegion(),
			},
			SavedListId: req.GetSavedListId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseSavedLists lists saved lists a page at a time.
func (s *SavedListQueryServer) BrowseSavedLists(ctx context.Context, req *cartv1.BrowseSavedListsRequest) (*cartv1.BrowseSavedListsResponse, error) {
	s.log.InfoContext(ctx, "browse saved lists", "tenant", req.GetContext().GetTenantId())
	summarizeProductReviewsReply, err := s.productReviewQueryClient.SummarizeProductReviews(ctx, &reviewsv1.SummarizeProductReviewsRequest{
		Context:     req.GetContext(),
		Details:     &reviewsv1.SummarizeProductReviewsRequest_Details{Filters: req.GetFilter().GetLabels()},
		Annotations: req.GetContext().GetBaggage(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewQueryService/SummarizeProductReviews")
	}
	return &cartv1.BrowseSavedListsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeProductReviewsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// FindSavedLists lists saved lists a page at a time.
func (s *SavedListQueryServer) FindSavedLists(ctx context.Context, req *cartv1.FindSavedListsRequest) (*cartv1.FindSavedListsResponse, error) {
	s.log.InfoContext(ctx, "find saved lists", "tenant", req.GetContext().GetTenantId())
	source := req.GetFilter().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	compareTranslationKeyOptionsRequest := newCompareTranslationKeyOptionsRequest(withCompareTranslationKeyOptionsContext(req.GetContext()), withCompareTranslationKeyOptionsDetails(&localizationv1.CompareTranslationKeyOptionsRequest_Details{Destination: &commonv1.PostalAddress{Region: region}}), withCompareTranslationKeyOptionsTranslationKey(&localizationv1.TranslationKey{
		Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
	}))
	compareTranslationKeyOptionsReply, err := s.translationKeyCalculationClient.CompareTranslationKeyOptions(ctx, compareTranslationKeyOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.localization.v1.TranslationKeyCalculationService/CompareTranslationKeyOptions")
	}
	summarizeCartsRequest := &cartv1.SummarizeCartsRequest{}
	summarizeCartsRequest.Context = req.GetContext()
	summarizeCartsRequest.Details = &cartv1.SummarizeCartsRequest_Details{
		Filters: compareTranslationKeyOptionsReply.GetResult().GetDetails(),
	}
	summarizeCartsRequest.Annotations = req.GetFilter().GetLabels()
	summarizeCartsReply, err := s.cartQueryClient.SummarizeCarts(ctx, summarizeCartsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartQueryService/SummarizeCarts")
	}
	return &cartv1.FindSavedListsResponse{
		SavedLists: []*cartv1.SavedList{&cartv1.SavedList{
			Total: compareTranslationKeyOptionsReply.GetBreakdown().GetTotal(),
		}},
		Result: &commonv1.OperationResult{Status: summarizeCartsReply.GetResult().GetStatus()},
	}, nil
}

// CountSavedLists counts saved lists by status.
func (s *SavedListQueryServer) CountSavedLists(ctx context.Context, req *cartv1.CountSavedListsRequest) (*cartv1.CountSavedListsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count saved lists", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	unsubscribeSearchQueryUpdatesRequest := &searchv1.UnsubscribeSearchQueryUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSearchQueryUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: labels,
			Payload:   packed,
		},
		Annotations: req.GetContext().GetBaggage(),
	}
	unsubscribeSearchQueryUpdatesReply := new(searchv1.UnsubscribeSearchQueryUpdatesResponse)
	if err := grpcx.Invoke(ctx, s.conn, searchv1.SearchQueryNotificationService_UnsubscribeSearchQueryUpdates_FullMethodName, unsubscribeSearchQueryUpdatesRequest, unsubscribeSearchQueryUpdatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/UnsubscribeSearchQueryUpdates")
	}
	return &cartv1.CountSavedListsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: unsubscribeSearchQueryUpdatesReply.GetResult().GetCorrelationId(),
			Message:       unsubscribeSearchQueryUpdatesReply.GetResult().GetMessage(),
			Status:        "ok",
		},
	}, nil
}

// ResolveSavedLists returns several saved lists at once.
func (s *SavedListQueryServer) ResolveSavedLists(ctx context.Context, req *cartv1.ResolveSavedListsRequest) (*cartv1.ResolveSavedListsResponse, error) {
	s.log.InfoContext(ctx, "resolve saved lists", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := savedListQueryServerCheckCartEligibilitySteps[mode]
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	checkCartEligibilityRequest := &cartv1.CheckCartEligibilityRequest{
		Context: req.GetContext(),
		Details: &cartv1.CheckCartEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		Cart: &cartv1.Cart{
			Audit:    &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Metadata: baggage,
		},
	}
	checkCartEligibilityReply, err := run(s, ctx, checkCartEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/CheckCartEligibility")
	}
	return &cartv1.ResolveSavedListsResponse{
		SavedLists: []*cartv1.SavedList{&cartv1.SavedList{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: checkCartEligibilityReply.GetCart().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SummarizeSavedLists summarizes a saved list.
func (s *SavedListQueryServer) SummarizeSavedLists(ctx context.Context, req *cartv1.SummarizeSavedListsRequest) (*cartv1.SummarizeSavedListsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SummarizeSavedLists", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "saved_list", req.GetSavedListId(), "summarize_saved_lists"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize saved lists", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key2, value2 := range req.GetDetails().GetFilters() {
		filters[key2] = value2
	}
	estimateCartRequest := newEstimateCartRequest(withEstimateCartContext(req.GetContext()), withEstimateCartCart(&cartv1.Cart{
		Counterparty: &cartv1.SavedListRef{SavedListId: req.GetSavedListId()},
		Lines:        []*cartv1.CartLine{&cartv1.CartLine{Attributes: filters}},
	}), withEstimateCartAnnotations(annotations), withEstimateCartReason(req.GetReason()))
	estimateCartReply, err := s.cartCalculationClient.EstimateCart(ctx, estimateCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/EstimateCart")
	}
	cloned := proto.Clone(req.GetDetails()).(*cartv1.SummarizeSavedListsRequest_Details)
	exportCartsStream, err := s.cartQueryClient.ExportCarts(ctx, &cartv1.ExportCartsRequest{
		Context: req.GetContext(),
		Filter: &cartv1.ExportCartsRequest_Filter{
			Region:       estimateCartReply.GetCart().GetBillingAddress().GetRegion(),
			CreatedAfter: estimateCartReply.GetCart().GetAudit().GetUpdatedAt(),
			OwnerId:      estimateCartReply.GetResult().GetCorrelationId(),
		},
		Format: cloned.GetFormat(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartQueryService/ExportCarts")
	}
	for {
		received, err := exportCartsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.CartQueryService/ExportCarts")
		}
		_ = received
	}
	getSavedListRatesReply, err := s.savedListCalculationClient.GetSavedListRates(ctx, &cartv1.GetSavedListRatesRequest{
		Context:     req.GetContext(),
		SavedListId: req.GetSavedListId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListCalculationService/GetSavedListRates")
	}
	explainAccessPolicyDecisionRequest := newExplainAccessPolicyDecisionRequest(withExplainAccessPolicyDecisionContext(req.GetContext()), withExplainAccessPolicyDecisionAccessPolicy(&authorizationv1.AccessPolicy{
		Audit: &commonv1.AuditInfo{
			Etag: getSavedListRatesReply.GetSavedList().GetAudit().GetEtag(),
		},
	}))
	explainAccessPolicyDecisionReply, err := s.accessPolicyValidationClient.ExplainAccessPolicyDecision(ctx, explainAccessPolicyDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/ExplainAccessPolicyDecision")
	}
	return &cartv1.SummarizeSavedListsResponse{
		Result: &commonv1.OperationResult{
			Message:      estimateCartReply.GetResult().GetMessage(),
			Details:      estimateCartReply.GetResult().GetDetails(),
			ReferenceIds: getSavedListRatesReply.GetResult().GetReferenceIds(),
			Status:       explainAccessPolicyDecisionReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestSavedLists lists saved lists a page at a time.
func (s *SavedListQueryServer) SuggestSavedLists(ctx context.Context, req *cartv1.SuggestSavedListsRequest) (*cartv1.SuggestSavedListsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestSavedLists", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest saved lists", "tenant", req.GetContext().GetTenantId())
	manifestCalculation, ok := s.clients.Get("acme.shipping.v1.ManifestCalculationService").(shippingv1.ManifestCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.shipping.v1.ManifestCalculationService")
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	simulateManifestRequest := &shippingv1.SimulateManifestRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			Audit:          &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
			Metadata:       labels,
		},
	}
	simulateManifestReply, err := manifestCalculation.SimulateManifest(ctx, simulateManifestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/SimulateManifest")
	}
	return &cartv1.SuggestSavedListsResponse{
		SavedLists: []*cartv1.SavedList{&cartv1.SavedList{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulateManifestReply.GetManifest().GetAudit().GetUpdatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PeekSavedList returns a saved list.
func (s *SavedListQueryServer) PeekSavedList(ctx context.Context, req *cartv1.PeekSavedListRequest) (*cartv1.PeekSavedListResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "saved_list", req.GetSavedListId(), "peek_saved_list"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek saved list", "tenant", req.GetContext().GetTenantId())
	return &cartv1.PeekSavedListResponse{
		SavedList: &cartv1.SavedList{SavedListId: req.GetSavedListId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// savedListQueryServerCheckCartEligibilitySteps are the ways to run
// acme.cart.v1.CartValidationService/CheckCartEligibility, by name.
var savedListQueryServerCheckCartEligibilitySteps = map[string]func(*SavedListQueryServer, context.Context, *cartv1.CheckCartEligibilityRequest) (*cartv1.CheckCartEligibilityResponse, error){
	"default": (*SavedListQueryServer).checkCartEligibilityStep,
	"audited": (*SavedListQueryServer).checkCartEligibilityAuditedStep,
}

func (s *SavedListQueryServer) checkCartEligibilityStep(ctx context.Context, request *cartv1.CheckCartEligibilityRequest) (*cartv1.CheckCartEligibilityResponse, error) {
	return s.cartValidationClient.CheckCartEligibility(ctx, request)
}

func (s *SavedListQueryServer) checkCartEligibilityAuditedStep(ctx context.Context, request *cartv1.CheckCartEligibilityRequest) (*cartv1.CheckCartEligibilityResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.cart.v1.CartValidationService/CheckCartEligibility")
	return s.cartValidationClient.CheckCartEligibility(ctx, request)
}
