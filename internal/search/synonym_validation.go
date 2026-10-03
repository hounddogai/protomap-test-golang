package search

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SynonymValidationServer implements `acme.search.v1.SynonymValidationService`.
// Checks synonyms against the rules before anything acts on them.
type SynonymValidationServer struct {
	searchv1.UnimplementedSynonymValidationServiceServer
	store                         *Store
	log                           *slog.Logger
	conn                          *grpc.ClientConn
	ledgerAccountClient           ledgerv1.LedgerAccountServiceClient
	searchQueryNotificationClient searchv1.SearchQueryNotificationServiceClient
}

// NewSynonymValidationServer wires a SynonymValidationServer.
func NewSynonymValidationServer(deps platform.Deps) *SynonymValidationServer {
	return &SynonymValidationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.search.v1.SynonymValidationService"),
		conn:                          deps.Conn,
		ledgerAccountClient:           ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		searchQueryNotificationClient: searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
	}
}

// ValidateSynonym validates a synonym.
func (s *SynonymValidationServer) ValidateSynonym(ctx context.Context, req *searchv1.ValidateSynonymRequest) (*searchv1.ValidateSynonymResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "validate synonym", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	lintDocumentsRequest := &documentsv1.LintDocumentsRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			Lines: []*documentsv1.DocumentLine{&documentsv1.DocumentLine{
				Tax: &documentsv1.DocumentLineTax{Amount: req.GetSynonym().GetDiscount().GetAmount()},
			}},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetSynonym().GetAudit().GetUpdatedBy()},
		},
		Details: &documentsv1.LintDocumentsRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}
	lintDocumentsReply := new(documentsv1.LintDocumentsResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.documents.v1.DocumentValidationService/LintDocuments", lintDocumentsRequest, lintDocumentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/LintDocuments")
	}
	return &searchv1.ValidateSynonymResponse{
		Result: &commonv1.OperationResult{
			Message: lintDocumentsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &searchv1.ValidateSynonymResponse_Outcome{Message: lintDocumentsReply.GetResult().GetMessage()},
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Owner:     req.GetSynonym().GetOwner(),
		},
	}, nil
}

// PreviewSynonym previews a synonym.
func (s *SynonymValidationServer) PreviewSynonym(ctx context.Context, req *searchv1.PreviewSynonymRequest) (*searchv1.PreviewSynonymResponse, error) {
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "preview synonym", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	cloneLedgerAccountRequest := &ledgerv1.CloneLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Limits: []*ledgerv1.LedgerAccountLimit{&ledgerv1.LedgerAccountLimit{Amount: req.GetSynonym().GetDiscount().GetAmount()}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetSynonym().GetAudit().GetUpdatedBy(),
				CreatedBy: req.GetSynonym().GetAudit().GetCreatedBy(),
			},
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}
	send := s.cloneLedgerAccountSender(cloneLedgerAccountRequest)
	cloneLedgerAccountReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CloneLedgerAccount")
	}
	return &searchv1.PreviewSynonymResponse{
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cloneLedgerAccountReply.GetLedgerAccount().GetAudit().GetUpdatedBy(),
			},
			SynonymId:   req.GetSynonymId(),
			Description: req.GetSynonym().GetDescription(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: cloneLedgerAccountReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// CheckSynonymEligibility checks a synonym.
func (s *SynonymValidationServer) CheckSynonymEligibility(ctx context.Context, req *searchv1.CheckSynonymEligibilityRequest) (*searchv1.CheckSynonymEligibilityResponse, error) {
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "check synonym eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	streamSearchQueryNoticesStream, err := s.searchQueryNotificationClient.StreamSearchQueryNotices(ctx, &searchv1.StreamSearchQueryNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email:            req.GetSynonym().GetOwner().GetEmail(),
			Name:             req.GetDetails().GetContact().GetName(),
			Phone:            req.GetDetails().GetContact().GetPhone(),
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/StreamSearchQueryNotices")
	}
	for {
		received, err := streamSearchQueryNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/StreamSearchQueryNotices")
		}
		_ = received
	}
	return &searchv1.CheckSynonymEligibilityResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Effective: req.GetSynonym().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeSynonym normalizes a synonym.
func (s *SynonymValidationServer) NormalizeSynonym(ctx context.Context, req *searchv1.NormalizeSynonymRequest) (*searchv1.NormalizeSynonymResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "normalize synonym", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	return &searchv1.NormalizeSynonymResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Effective: req.GetSynonym().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainSynonymDecision explains a synonym.
func (s *SynonymValidationServer) ExplainSynonymDecision(ctx context.Context, req *searchv1.ExplainSynonymDecisionRequest) (*searchv1.ExplainSynonymDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainSynonymDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "explain synonym decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	return &searchv1.ExplainSynonymDecisionResponse{
		Synonym: &searchv1.Synonym{
			SynonymId:  req.GetSynonymId(),
			Parameters: req.GetSynonym().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreSynonym scores a synonym.
func (s *SynonymValidationServer) ScoreSynonym(ctx context.Context, req *searchv1.ScoreSynonymRequest) (*searchv1.ScoreSynonymResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreSynonym", "error", err)
		return nil, err
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "score synonym", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	return &searchv1.ScoreSynonymResponse{
		Synonym: &searchv1.Synonym{
			SynonymId:  req.GetSynonymId(),
			Parameters: req.GetSynonym().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintSynonyms lints a synonym.
func (s *SynonymValidationServer) LintSynonyms(ctx context.Context, req *searchv1.LintSynonymsRequest) (*searchv1.LintSynonymsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "lint synonyms", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	cloneLedgerAccountRequest := &ledgerv1.CloneLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	send := s.cloneLedgerAccountSender(cloneLedgerAccountRequest)
	cloneLedgerAccountReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CloneLedgerAccount")
	}
	return &searchv1.LintSynonymsResponse{
		Result: &commonv1.OperationResult{
			Details: cloneLedgerAccountReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cloneLedgerAccountReply.GetLedgerAccount().GetAudit().GetUpdatedAt(),
			},
			SynonymId: req.GetSynonymId(),
			Effective: req.GetSynonym().GetEffective(),
		},
	}, nil
}

// cloneLedgerAccountSender returns a function that sends one request to
// acme.ledger.v1.LedgerAccountService/CloneLedgerAccount.
func (s *SynonymValidationServer) cloneLedgerAccountSender(request *ledgerv1.CloneLedgerAccountRequest) func(context.Context) (*ledgerv1.CloneLedgerAccountResponse, error) {
	return func(ctx context.Context) (*ledgerv1.CloneLedgerAccountResponse, error) {
		return s.ledgerAccountClient.CloneLedgerAccount(ctx, request)
	}
}
