package sellers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	communityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/community/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	insurancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/insurance/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// SellerServer implements `acme.sellers.v1.SellerService`.
// Manages sellers: creates, reads, changes, and lists them.
type SellerServer struct {
	sellersv1.UnsafeSellerServiceServer
	store                                  *Store
	log                                    *slog.Logger
	featureFlagValidationClient            featureflagsv1.FeatureFlagValidationServiceClient
	sellerListingQueryClient               sellersv1.SellerListingQueryServiceClient
	documentClient                         documentsv1.DocumentServiceClient
	userClient                             identityv1.UserServiceClient
	giftCardQueryPort                      giftCardQueryPort
	sellerScorecardValidationPort          sellerScorecardValidationPort
	forumQueryClient                       communityv1.ForumQueryServiceClient
	conn                                   *grpc.ClientConn
	sellerValidationClient                 sellersv1.SellerValidationServiceClient
	sellerCalculationClient                sellersv1.SellerCalculationServiceClient
	policyClient                           insurancev1.PolicyServiceClient
	giftCardCalculationClient              giftcardsv1.GiftCardCalculationServiceClient
	meteredSellerPayoutScheduleCalculation meteredSellerPayoutScheduleCalculation
	sellerPayoutScheduleCalculationClient  sellersv1.SellerPayoutScheduleCalculationServiceClient
	giftCardQueryClient                    giftcardsv1.GiftCardQueryServiceClient
}

// NewSellerServer wires a SellerServer.
func NewSellerServer(deps platform.Deps) *SellerServer {
	return &SellerServer{
		store:                                  NewStore(deps.DB),
		log:                                    deps.Logger.With("service", "acme.sellers.v1.SellerService"),
		featureFlagValidationClient:            featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		sellerListingQueryClient:               sellersv1.NewSellerListingQueryServiceClient(deps.Conn),
		documentClient:                         documentsv1.NewDocumentServiceClient(deps.Conn),
		userClient:                             identityv1.NewUserServiceClient(deps.Conn),
		giftCardQueryPort:                      newGiftCardQueryPort(deps),
		sellerScorecardValidationPort:          newSellerScorecardValidationPort(deps),
		forumQueryClient:                       communityv1.NewForumQueryServiceClient(deps.Conn),
		conn:                                   deps.Conn,
		sellerValidationClient:                 sellersv1.NewSellerValidationServiceClient(deps.Conn),
		sellerCalculationClient:                sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		policyClient:                           insurancev1.NewPolicyServiceClient(deps.Conn),
		giftCardCalculationClient:              giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
		meteredSellerPayoutScheduleCalculation: meteredSellerPayoutScheduleCalculation{SellerPayoutScheduleCalculationServiceClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerPayoutScheduleCalculationClient:  sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		giftCardQueryClient:                    giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn),
	}
}

// CreateSeller creates a seller.
func (s *SellerServer) CreateSeller(ctx context.Context, req *sellersv1.CreateSellerRequest) (*sellersv1.CreateSellerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CreateSeller", "error", err)
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "create seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	scoreFeatureFlagRequest := &featureflagsv1.ScoreFeatureFlagRequest{}
	scoreFeatureFlagRequest.Context = req.GetContext()
	scoreFeatureFlagRequest.Details = &featureflagsv1.ScoreFeatureFlagRequest_Details{
		Destination: &commonv1.PostalAddress{
			CountryCode: req.GetSeller().GetPrimaryAddress().GetCountryCode(),
		},
	}
	scoreFeatureFlagRequest.FeatureFlag = &featureflagsv1.FeatureFlag{
		Audit: &commonv1.AuditInfo{CreatedAt: req.GetSeller().GetAudit().GetCreatedAt()},
	}
	scoreFeatureFlagReply, err := s.featureFlagValidationClient.ScoreFeatureFlag(ctx, scoreFeatureFlagRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ScoreFeatureFlag")
	}
	return &sellersv1.CreateSellerResponse{
		Result: &commonv1.OperationResult{
			Details: scoreFeatureFlagReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Seller: &sellersv1.Seller{
			Contact:    &commonv1.ContactInfo{Name: req.GetSeller().GetName()},
			Attributes: req.GetSeller().GetAttributes(),
		},
	}, nil
}

// GetSeller returns a seller.
func (s *SellerServer) GetSeller(ctx context.Context, req *sellersv1.GetSellerRequest) (*sellersv1.GetSellerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "get_seller"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller", "tenant", req.GetContext().GetTenantId())
	browseSellerListingsRequest := &sellersv1.BrowseSellerListingsRequest{
		Context: req.GetContext(),
		Filter:  &sellersv1.BrowseSellerListingsRequest_Filter{OwnerId: req.GetSellerId()},
	}
	browseSellerListingsReply, err := grpcx.Call(ctx, s.sellerListingQueryClient.BrowseSellerListings, browseSellerListingsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/BrowseSellerListings")
	}
	listDocumentsRequest := &documentsv1.ListDocumentsRequest{}
	listDocumentsRequest.Context = req.GetContext()
	listDocumentsRequest.Filter = &documentsv1.ListDocumentsRequest_Filter{
		Labels:       browseSellerListingsReply.GetResult().GetDetails(),
		OwnerId:      browseSellerListingsReply.GetResult().GetCorrelationId(),
		CreatedAfter: req.GetContext().GetDeadline(),
	}
	listDocumentsReply, err := s.documentClient.ListDocuments(ctx, listDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/ListDocuments")
	}
	return &sellersv1.GetSellerResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: browseSellerListingsReply.GetResult().GetCorrelationId(),
			Message:       browseSellerListingsReply.GetResult().GetMessage(),
			ReferenceIds:  listDocumentsReply.GetResult().GetReferenceIds(),
			Details:       listDocumentsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Seller: &sellersv1.Seller{SellerId: req.GetSellerId()},
	}, nil
}

// UpdateSeller changes a seller.
func (s *SellerServer) UpdateSeller(ctx context.Context, req *sellersv1.UpdateSellerRequest) (*sellersv1.UpdateSellerResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "update seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	validateFeatureFlagRequest := &featureflagsv1.ValidateFeatureFlagRequest{
		Context: req.GetContext(),
		Details: &featureflagsv1.ValidateFeatureFlagRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetSeller().GetContact().GetEmail()},
		},
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetSeller().GetAudit().GetUpdatedBy(),
				CreatedBy: req.GetSeller().GetAudit().GetCreatedBy(),
			},
		},
	}
	validateFeatureFlagReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*featureflagsv1.ValidateFeatureFlagResponse, error) {
		return s.featureFlagValidationClient.ValidateFeatureFlag(ctx, validateFeatureFlagRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ValidateFeatureFlag")
	}
	step := &sellerServerUpdateSellerCreateUserStep{request: &identityv1.CreateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			IdentityDocument: &identityv1.User_Passport{Passport: &identityv1.UserPassport{ExpiresAt: req.GetSeller().GetPassport().GetExpiresAt()}},
			PrimaryAddress:   &commonv1.PostalAddress{Lines: req.GetSeller().GetPrimaryAddress().GetLines()},
			Audit: &commonv1.AuditInfo{
				CreatedAt: validateFeatureFlagReply.GetFeatureFlag().GetAudit().GetCreatedAt(),
			},
		},
	}}
	if err := s.runUpdateSellerCreateUser(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/CreateUser")
	}
	createUserReply := step.reply
	findGiftCardsRequest := &giftcardsv1.FindGiftCardsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.FindGiftCardsRequest_Filter{
			Region:       createUserReply.GetUser().GetPrimaryAddress().GetRegion(),
			CreatedAfter: createUserReply.GetUser().GetAudit().GetUpdatedAt(),
			OwnerId:      validateFeatureFlagReply.GetFeatureFlag().GetOwner().GetUserId(),
			Labels:       validateFeatureFlagReply.GetResult().GetDetails(),
		},
	}
	findGiftCardsReply, err := s.giftCardQueryPort.FindGiftCards(ctx, findGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/FindGiftCards")
	}
	getUserRequest := buildGetUserRequestForUpdateSeller(req)
	getUserReply, err := s.userClient.GetUser(ctx, getUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/GetUser")
	}
	return &sellersv1.UpdateSellerResponse{
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateFeatureFlagReply.GetFeatureFlag().GetAudit().GetCreatedBy(),
				Etag:      validateFeatureFlagReply.GetFeatureFlag().GetAudit().GetEtag(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient: createUserReply.GetUser().GetPrimaryAddress().GetRecipient(),
			},
			RelationshipManager: createUserReply.GetUser().GetRelationshipManager(),
			Contact:             &commonv1.ContactInfo{Name: getUserReply.GetUser().GetContact().GetName()},
			SellerId:            req.GetSeller().GetSellerId(),
			Attributes:          req.GetSeller().GetAttributes(),
		},
		Result: &commonv1.OperationResult{
			Details: findGiftCardsReply.GetResult().GetDetails(),
			Status:  findGiftCardsReply.GetResult().GetStatus(),
		},
	}, nil
}

// DeleteSeller deletes a seller.
func (s *SellerServer) DeleteSeller(ctx context.Context, req *sellersv1.DeleteSellerRequest) (*sellersv1.DeleteSellerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DeleteSeller", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "delete_seller"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete seller", "tenant", req.GetContext().GetTenantId())
	previewSellerScorecardRequest := &sellersv1.PreviewSellerScorecardRequest{
		Context:         req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{Audit: &commonv1.AuditInfo{Etag: req.GetEtag()}},
	}
	previewSellerScorecardReply, err := s.sellerScorecardValidationPort.PreviewSellerScorecard(ctx, previewSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/PreviewSellerScorecard")
	}
	findForumsReply, err := s.forumQueryClient.FindForums(ctx, &communityv1.FindForumsRequest{
		Context: req.GetContext(),
		Filter: &communityv1.FindForumsRequest_Filter{
			Region:       previewSellerScorecardReply.GetSellerScorecard().GetBillingAddress().GetRegion(),
			CreatedAfter: previewSellerScorecardReply.GetSellerScorecard().GetDueAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.community.v1.ForumQueryService/FindForums")
	}
	getSellerRatesRequest := &sellersv1.GetSellerRatesRequest{
		Context:  req.GetContext(),
		SellerId: req.GetSellerId(),
	}
	getSellerRatesReply := new(sellersv1.GetSellerRatesResponse)
	if err := grpcx.Invoke(ctx, s.conn, sellersv1.SellerCalculationService_GetSellerRates_FullMethodName, getSellerRatesRequest, getSellerRatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/GetSellerRates")
	}
	validateSellerRequest := buildValidateSellerRequestForDeleteSeller(req, previewSellerScorecardReply)
	validateSellerReply, err := s.sellerValidationClient.ValidateSeller(ctx, validateSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/ValidateSeller")
	}
	_ = validateSellerReply
	return &sellersv1.DeleteSellerResponse{
		Result: &commonv1.OperationResult{
			Status:       previewSellerScorecardReply.GetResult().GetStatus(),
			Details:      previewSellerScorecardReply.GetResult().GetDetails(),
			ReferenceIds: findForumsReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// ListSellers lists sellers a page at a time.
func (s *SellerServer) ListSellers(ctx context.Context, req *sellersv1.ListSellersRequest) (*sellersv1.ListSellersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListSellers", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list sellers", "tenant", req.GetContext().GetTenantId())
	explainSellerCalculationRequest := buildExplainSellerCalculationRequestForListSellers(req)
	explainSellerCalculationReply, err := s.sellerCalculationClient.ExplainSellerCalculation(ctx, explainSellerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/ExplainSellerCalculation")
	}
	return &sellersv1.ListSellersResponse{
		Sellers: []*sellersv1.Seller{&sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainSellerCalculationReply.GetSeller().GetAudit().GetUpdatedAt(),
			},
			RelationshipManager: &identityv1.UserRef{
				UserId: explainSellerCalculationReply.GetSeller().GetRelationshipManager().GetUserId(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BatchGetSellers returns several sellers at once.
func (s *SellerServer) BatchGetSellers(ctx context.Context, req *sellersv1.BatchGetSellersRequest) (*sellersv1.BatchGetSellersResponse, error) {
	s.log.InfoContext(ctx, "batch get sellers", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	lintFeatureFlagsRequest := &featureflagsv1.LintFeatureFlagsRequest{}
	lintFeatureFlagsRequest.Context = req.GetContext()
	lintFeatureFlagsRequest.FeatureFlag = &featureflagsv1.FeatureFlag{
		Owner: &identityv1.UserRef{
			Email:  tenancy.Value(ctx, "email"),
			UserId: req.GetContext().GetActor().GetUserId(),
		},
		Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
	}
	lintFeatureFlagsRequest.Details = &featureflagsv1.LintFeatureFlagsRequest_Details{Locale: req.GetContext().GetLocale()}
	lintFeatureFlagsReply, err := s.featureFlagValidationClient.LintFeatureFlags(ctx, lintFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/LintFeatureFlags")
	}
	createPolicyReply, err := s.policyClient.CreatePolicy(ctx, &insurancev1.CreatePolicyRequest{
		Context: req.GetContext(),
		Policy: &insurancev1.Policy{
			Limits: []*insurancev1.PolicyLimit{&insurancev1.PolicyLimit{
				Amount: lintFeatureFlagsReply.GetFeatureFlag().GetDiscount().GetAmount(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintFeatureFlagsReply.GetFeatureFlag().GetAudit().GetCreatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.insurance.v1.PolicyService/CreatePolicy")
	}
	return &sellersv1.BatchGetSellersResponse{
		Result: &commonv1.OperationResult{
			Message: lintFeatureFlagsReply.GetOutcome().GetMessage(),
			Status:  lintFeatureFlagsReply.GetOutcome().GetStatus(),
		},
		Sellers: []*sellersv1.Seller{&sellersv1.Seller{
			Contact: createPolicyReply.GetPolicy().GetContact(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: createPolicyReply.GetPolicy().GetAudit().GetUpdatedAt(),
			},
		}},
	}, nil
}

// SearchSellers lists sellers a page at a time.
func (s *SellerServer) SearchSellers(ctx context.Context, req *sellersv1.SearchSellersRequest) (*sellersv1.SearchSellersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "search sellers", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	explainGiftCardCalculationReply, err := s.giftCardCalculationClient.ExplainGiftCardCalculation(ctx, &giftcardsv1.ExplainGiftCardCalculationRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Labels:  req.GetFilter().GetLabels(),
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
		Details: &giftcardsv1.ExplainGiftCardCalculationRequest_Details{Locale: req.GetContext().GetLocale()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/ExplainGiftCardCalculation")
	}
	return &sellersv1.SearchSellersResponse{
		Result: &commonv1.OperationResult{
			Status: explainGiftCardCalculationReply.GetResult().GetStatus(),
		},
	}, nil
}

// WatchSellers streams the changes of sellers as they happen.
func (s *SellerServer) WatchSellers(req *sellersv1.WatchSellersRequest, stream grpc.ServerStreamingServer[sellersv1.WatchSellersResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected WatchSellers", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "watch sellers", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetSubscriber())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.ContactInfo
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	lintFeatureFlagsReply, err := s.featureFlagValidationClient.LintFeatureFlags(ctx, &featureflagsv1.LintFeatureFlagsRequest{
		Context:     req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{Owner: &identityv1.UserRef{Email: (&decoded).GetEmail()}},
		Details: &featureflagsv1.LintFeatureFlagsRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetSubscriber().GetName(),
				Phone: req.GetSubscriber().GetPhone(),
			},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/LintFeatureFlags")
	}
	recalculateSellerPayoutSchedulesRequest := &sellersv1.RecalculateSellerPayoutSchedulesRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Organizer: &identityv1.UserRef{
				Email:       lintFeatureFlagsReply.GetFeatureFlag().GetOwner().GetEmail(),
				UserId:      lintFeatureFlagsReply.GetFeatureFlag().GetOwner().GetUserId(),
				DisplayName: lintFeatureFlagsReply.GetFeatureFlag().GetOwner().GetDisplayName(),
			},
		},
	}
	recalculateSellerPayoutSchedulesReply, err := s.meteredSellerPayoutScheduleCalculation.RecalculateSellerPayoutSchedules(ctx, recalculateSellerPayoutSchedulesRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/RecalculateSellerPayoutSchedules")
	}
	if err := stream.Send(&sellersv1.WatchSellersResponse{
		Result: &commonv1.OperationResult{
			Message: lintFeatureFlagsReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: lintFeatureFlagsReply.GetFeatureFlag().GetAudit().GetUpdatedBy(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient: recalculateSellerPayoutSchedulesReply.GetSellerPayoutSchedule().GetLocation().GetRecipient(),
				Region:    req.GetFilter().GetRegion(),
			},
			Contact: &commonv1.ContactInfo{Name: req.GetSubscriber().GetName()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreateSellers takes a stream of sellers and answers once at the end.
func (s *SellerServer) BatchCreateSellers(stream grpc.ClientStreamingServer[sellersv1.BatchCreateSellersRequest, sellersv1.BatchCreateSellersResponse]) error {
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
		s.log.InfoContext(ctx, "batch create sellers", "tenant", msg.GetContext().GetTenantId())
		previewFeatureFlagRequest := &featureflagsv1.PreviewFeatureFlagRequest{}
		previewFeatureFlagRequest.Context = msg.GetContext()
		previewFeatureFlagRequest.FeatureFlag = &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{Etag: msg.GetRecord().GetAudit().GetEtag()},
		}
		if _, err := s.featureFlagValidationClient.PreviewFeatureFlag(ctx, previewFeatureFlagRequest); err != nil {
			return grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/PreviewFeatureFlag")
		}
		if _, err := s.sellerPayoutScheduleCalculationClient.CompareSellerPayoutScheduleOptions(ctx, &sellersv1.CompareSellerPayoutScheduleOptionsRequest{
			Context: msg.GetContext(),
			SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
				Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
					Contact: &commonv1.ContactInfo{Email: msg.GetRecord().GetContact().GetEmail()},
				}},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/CompareSellerPayoutScheduleOptions")
		}
		accepted++
	}
	return stream.SendAndClose(&sellersv1.BatchCreateSellersResponse{Accepted: accepted})
}

// CloneSeller creates a seller.
func (s *SellerServer) CloneSeller(ctx context.Context, req *sellersv1.CloneSellerRequest) (*sellersv1.CloneSellerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CloneSeller", "error", err)
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "clone seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	compareGiftCardOptionsRequest := &giftcardsv1.CompareGiftCardOptionsRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CompareGiftCardOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetSeller().GetPrimaryAddress().GetCountryCode(),
				Lines:       req.GetSeller().GetPrimaryAddress().GetLines(),
			},
		},
		GiftCard: &giftcardsv1.GiftCard{
			Audit:   &commonv1.AuditInfo{CreatedBy: req.GetSeller().GetAudit().GetCreatedBy()},
			Contact: &commonv1.ContactInfo{Email: req.GetSeller().GetContact().GetEmail()},
		},
	}
	send := s.compareGiftCardOptionsSender(compareGiftCardOptionsRequest)
	compareGiftCardOptionsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/CompareGiftCardOptions")
	}
	encoded, err := protojson.Marshal(req.GetSeller())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded sellersv1.Seller
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	summarizeSellerListingsRequest := &sellersv1.SummarizeSellerListingsRequest{
		Context: req.GetContext(),
		Details: &sellersv1.SummarizeSellerListingsRequest_Details{Period: (&decoded).GetPreferences().GetQuietHours()},
	}
	summarizeSellerListingsReply, err := summarizeSellerListingsCall(ctx, summarizeSellerListingsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/SummarizeSellerListings")
	}
	browseSellerListingsRequest := &sellersv1.BrowseSellerListingsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.BrowseSellerListingsRequest_Filter{
			Region:       req.GetSeller().GetPrimaryAddress().GetRegion(),
			Labels:       compareGiftCardOptionsReply.GetGiftCard().GetLabels(),
			CreatedAfter: compareGiftCardOptionsReply.GetGiftCard().GetAudit().GetCreatedAt(),
		},
	}
	browseSellerListingsReply, err := browseSellerListingsCall(ctx, browseSellerListingsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/BrowseSellerListings")
	}
	summarizeGiftCardsReply, err := s.giftCardQueryClient.SummarizeGiftCards(ctx, &giftcardsv1.SummarizeGiftCardsRequest{
		Context:    req.GetContext(),
		Details:    &giftcardsv1.SummarizeGiftCardsRequest_Details{Period: summarizeSellerListingsReply.GetPeriod()},
		GiftCardId: compareGiftCardOptionsReply.GetGiftCard().GetGiftCardId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/SummarizeGiftCards")
	}
	_ = browseSellerListingsReply
	return &sellersv1.CloneSellerResponse{
		Result: &commonv1.OperationResult{
			Details:       compareGiftCardOptionsReply.GetResult().GetDetails(),
			CorrelationId: summarizeSellerListingsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  summarizeSellerListingsReply.GetResult().GetReferenceIds(),
			Status:        summarizeGiftCardsReply.GetResult().GetStatus(),
		},
		Seller: &sellersv1.Seller{
			PrimaryAddress: req.GetSeller().GetPrimaryAddress(),
			Contact:        req.GetSeller().GetContact(),
		},
	}, nil
}

// sellerServerUpdateSellerCreateUserStep carries the request of one step of UpdateSeller and, once it ran, its reply.
type sellerServerUpdateSellerCreateUserStep struct {
	request *identityv1.CreateUserRequest
	reply   *identityv1.CreateUserResponse
}

func (s *SellerServer) runUpdateSellerCreateUser(ctx context.Context, step *sellerServerUpdateSellerCreateUserStep) error {
	reply, err := s.userClient.CreateUser(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// compareGiftCardOptionsSender returns a function that sends one request to
// acme.giftcards.v1.GiftCardCalculationService/CompareGiftCardOptions.
func (s *SellerServer) compareGiftCardOptionsSender(request *giftcardsv1.CompareGiftCardOptionsRequest) func(context.Context) (*giftcardsv1.CompareGiftCardOptionsResponse, error) {
	return func(ctx context.Context) (*giftcardsv1.CompareGiftCardOptionsResponse, error) {
		return s.giftCardCalculationClient.CompareGiftCardOptions(ctx, request)
	}
}
