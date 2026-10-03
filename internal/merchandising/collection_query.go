package merchandising

import (
	"context"
	"errors"
	"io"
	"log/slog"

	admissionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/admissions/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// CollectionQueryServer implements `acme.merchandising.v1.CollectionQueryService`.
// Answers read-only questions about collections, for screens and reports.
type CollectionQueryServer struct {
	merchandisingv1.UnimplementedCollectionQueryServiceServer
	store                             *Store
	log                               *slog.Logger
	conn                              *grpc.ClientConn
	ledgerAccountValidationClient     ledgerv1.LedgerAccountValidationServiceClient
	documentClient                    documentsv1.DocumentServiceClient
	admissionCalculationClient        admissionsv1.AdmissionCalculationServiceClient
	merchandisingRuleValidationClient merchandisingv1.MerchandisingRuleValidationServiceClient
	meteredGiftCardValidation         meteredGiftCardValidation
	tenantNotificationClient          identityv1.TenantNotificationServiceClient
}

// NewCollectionQueryServer wires a CollectionQueryServer.
func NewCollectionQueryServer(deps platform.Deps) CollectionQueryServer {
	return CollectionQueryServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.merchandising.v1.CollectionQueryService"),
		conn:                              deps.Conn,
		ledgerAccountValidationClient:     ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		documentClient:                    documentsv1.NewDocumentServiceClient(deps.Conn),
		admissionCalculationClient:        admissionsv1.NewAdmissionCalculationServiceClient(deps.Conn),
		merchandisingRuleValidationClient: merchandisingv1.NewMerchandisingRuleValidationServiceClient(deps.Conn),
		meteredGiftCardValidation:         meteredGiftCardValidation{GiftCardValidationServiceClient: giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		tenantNotificationClient:          identityv1.NewTenantNotificationServiceClient(deps.Conn),
	}
}

// LookupCollection returns a collection.
func (s CollectionQueryServer) LookupCollection(ctx context.Context, req *merchandisingv1.LookupCollectionRequest) (*merchandisingv1.LookupCollectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupCollection", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "collection", req.GetCollectionId(), "lookup_collection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup collection", "tenant", req.GetContext().GetTenantId())
	return &merchandisingv1.LookupCollectionResponse{
		Collection: &merchandisingv1.Collection{CollectionId: req.GetCollectionId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseCollections lists collections a page at a time.
func (s CollectionQueryServer) BrowseCollections(ctx context.Context, req *merchandisingv1.BrowseCollectionsRequest) (*merchandisingv1.BrowseCollectionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse collections", "tenant", req.GetContext().GetTenantId())
	lintMerchandisingRulesRequest := &merchandisingv1.LintMerchandisingRulesRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &merchandisingv1.LintMerchandisingRulesRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
	}
	lintMerchandisingRulesReply := new(merchandisingv1.LintMerchandisingRulesResponse)
	if err := grpcx.Invoke(ctx, s.conn, merchandisingv1.MerchandisingRuleValidationService_LintMerchandisingRules_FullMethodName, lintMerchandisingRulesRequest, lintMerchandisingRulesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/LintMerchandisingRules")
	}
	previewLedgerAccountRequest := &ledgerv1.PreviewLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Holder: &ledgerv1.PostingBatchRef{
				DisplayName: lintMerchandisingRulesReply.GetMerchandisingRule().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintMerchandisingRulesReply.GetMerchandisingRule().GetAudit().GetCreatedBy(),
			},
		},
		Details: &ledgerv1.PreviewLedgerAccountRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: lintMerchandisingRulesReply.GetMerchandisingRule().GetOwner().GetEmail(),
			},
		},
	}
	send := s.previewLedgerAccountSender(previewLedgerAccountRequest)
	previewLedgerAccountReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/PreviewLedgerAccount")
	}
	return &merchandisingv1.BrowseCollectionsResponse{
		Collections: []*merchandisingv1.Collection{&merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
						Name: lintMerchandisingRulesReply.GetMerchandisingRule().GetName(),
					}},
				}},
				Audit: &commonv1.AuditInfo{
					CreatedBy: lintMerchandisingRulesReply.GetMerchandisingRule().GetAudit().GetCreatedBy(),
					Etag:      previewLedgerAccountReply.GetLedgerAccount().GetAudit().GetEtag(),
				},
			}},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// FindCollections lists collections a page at a time.
func (s CollectionQueryServer) FindCollections(ctx context.Context, req *merchandisingv1.FindCollectionsRequest) (*merchandisingv1.FindCollectionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "find collections", "tenant", req.GetContext().GetTenantId())
	return &merchandisingv1.FindCollectionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountCollections counts collections by status.
func (s CollectionQueryServer) CountCollections(ctx context.Context, req *merchandisingv1.CountCollectionsRequest) (*merchandisingv1.CountCollectionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountCollections", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count collections", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "query", req.GetFilter().GetQuery())
	watchDocumentsStream, err := s.documentClient.WatchDocuments(ctx, &documentsv1.WatchDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.WatchDocumentsRequest_Filter{
			Query:   tenancy.Value(ctx, "query"),
			OwnerId: req.GetFilter().GetOwnerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
	}
	for {
		received, err := watchDocumentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
		}
		_ = received
	}
	return &merchandisingv1.CountCollectionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ResolveCollections returns several collections at once.
func (s CollectionQueryServer) ResolveCollections(ctx context.Context, req *merchandisingv1.ResolveCollectionsRequest) (*merchandisingv1.ResolveCollectionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve collections", "tenant", req.GetContext().GetTenantId())
	return &merchandisingv1.ResolveCollectionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SummarizeCollections summarizes a collection.
func (s CollectionQueryServer) SummarizeCollections(ctx context.Context, req *merchandisingv1.SummarizeCollectionsRequest) (*merchandisingv1.SummarizeCollectionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "collection", req.GetCollectionId(), "summarize_collections"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize collections", "tenant", req.GetContext().GetTenantId())
	step := &collectionQueryServerSummarizeCollectionsValidateLedgerAccountStep{request: &ledgerv1.ValidateLedgerAccountRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}}
	if err := s.runSummarizeCollectionsValidateLedgerAccount(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ValidateLedgerAccount")
	}
	validateLedgerAccountReply := step.reply
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	estimateAdmissionRequest := &admissionsv1.EstimateAdmissionRequest{
		Context: req.GetContext(),
		Admission: &admissionsv1.Admission{
			Subject: &admissionsv1.GateScanRef{
				DisplayName: validateLedgerAccountReply.GetLedgerAccount().GetHolder().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &admissionsv1.EstimateAdmissionRequest_Details{
			CurrencyCode: validateLedgerAccountReply.GetLedgerAccount().GetCurrencyCode(),
		},
	}
	estimateAdmissionReply, err := admissionsv1.AdmissionCalculationServiceClient.EstimateAdmission(s.admissionCalculationClient, ctx, estimateAdmissionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.admissions.v1.AdmissionCalculationService/EstimateAdmission")
	}
	return &merchandisingv1.SummarizeCollectionsResponse{
		Result: &commonv1.OperationResult{
			Message:      validateLedgerAccountReply.GetOutcome().GetMessage(),
			ReferenceIds: validateLedgerAccountReply.GetResult().GetReferenceIds(),
			Status:       estimateAdmissionReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestCollections lists collections a page at a time.
func (s CollectionQueryServer) SuggestCollections(ctx context.Context, req *merchandisingv1.SuggestCollectionsRequest) (*merchandisingv1.SuggestCollectionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "suggest collections", "tenant", req.GetContext().GetTenantId())
	checkMerchandisingRuleEligibilityRequest := &merchandisingv1.CheckMerchandisingRuleEligibilityRequest{
		Context: req.GetContext(),
		Details: &merchandisingv1.CheckMerchandisingRuleEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	checkMerchandisingRuleEligibilityReply, err := merchandisingv1.MerchandisingRuleValidationServiceClient.CheckMerchandisingRuleEligibility(s.merchandisingRuleValidationClient, ctx, checkMerchandisingRuleEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/CheckMerchandisingRuleEligibility")
	}
	return &merchandisingv1.SuggestCollectionsResponse{
		Collections: []*merchandisingv1.Collection{&merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: checkMerchandisingRuleEligibilityReply.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
					},
				}},
				Audit: &commonv1.AuditInfo{
					Etag: checkMerchandisingRuleEligibilityReply.GetMerchandisingRule().GetAudit().GetEtag(),
				},
			}},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PeekCollection returns a collection.
func (s CollectionQueryServer) PeekCollection(ctx context.Context, req *merchandisingv1.PeekCollectionRequest) (*merchandisingv1.PeekCollectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PeekCollection", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "collection", req.GetCollectionId(), "peek_collection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek collection", "tenant", req.GetContext().GetTenantId())
	validateGiftCardRequest := &giftcardsv1.ValidateGiftCardRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ValidateGiftCardRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		GiftCard: &giftcardsv1.GiftCard{
			Funding:  &giftcardsv1.GiftCard_ExternalAccountId{ExternalAccountId: req.GetCollectionId()},
			OpenedAt: req.GetContext().GetDeadline(),
		},
	}
	validateGiftCardReply, err := s.meteredGiftCardValidation.ValidateGiftCard(ctx, validateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	unsubscribeTenantUpdatesRequest := &identityv1.UnsubscribeTenantUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeTenantUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            validateGiftCardReply.GetGiftCard().GetContact().GetPhone(),
				Name:             validateGiftCardReply.GetGiftCard().GetContact().GetName(),
				PreferredChannel: validateGiftCardReply.GetGiftCard().GetContact().GetPreferredChannel(),
				Email:            validateGiftCardReply.GetGiftCard().GetContact().GetEmail(),
			},
			Payload: packed,
		},
	}
	unsubscribeTenantUpdatesReply, err := grpcx.Call(ctx, s.tenantNotificationClient.UnsubscribeTenantUpdates, unsubscribeTenantUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/UnsubscribeTenantUpdates")
	}
	return &merchandisingv1.PeekCollectionResponse{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Manager: &identityv1.UserRef{
						DisplayName: validateGiftCardReply.GetGiftCard().GetHolder().GetDisplayName(),
					},
					Location: &commonv1.PostalAddress{
						Lines: unsubscribeTenantUpdatesReply.GetTenant().GetLocation().GetLines(),
					},
				}},
			}},
			Location: &commonv1.PostalAddress{
				PostalCode: unsubscribeTenantUpdatesReply.GetTenant().GetLocation().GetPostalCode(),
			},
			CollectionId: req.GetCollectionId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// previewLedgerAccountSender returns a function that sends one request to
// acme.ledger.v1.LedgerAccountValidationService/PreviewLedgerAccount.
func (s CollectionQueryServer) previewLedgerAccountSender(request *ledgerv1.PreviewLedgerAccountRequest) func(context.Context) (*ledgerv1.PreviewLedgerAccountResponse, error) {
	return func(ctx context.Context) (*ledgerv1.PreviewLedgerAccountResponse, error) {
		return s.ledgerAccountValidationClient.PreviewLedgerAccount(ctx, request)
	}
}

// collectionQueryServerSummarizeCollectionsValidateLedgerAccountStep carries the request of one step of
// SummarizeCollections and, once it ran, its reply.
type collectionQueryServerSummarizeCollectionsValidateLedgerAccountStep struct {
	request *ledgerv1.ValidateLedgerAccountRequest
	reply   *ledgerv1.ValidateLedgerAccountResponse
}

func (s CollectionQueryServer) runSummarizeCollectionsValidateLedgerAccount(ctx context.Context, step *collectionQueryServerSummarizeCollectionsValidateLedgerAccountStep) error {
	reply, err := s.ledgerAccountValidationClient.ValidateLedgerAccount(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
