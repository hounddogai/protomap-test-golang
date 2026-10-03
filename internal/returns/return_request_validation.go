package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ReturnRequestValidationServer implements `acme.returns.v1.ReturnRequestValidationService`.
// Checks return requests against the rules before anything acts on them.
type ReturnRequestValidationServer struct {
	returnsv1.UnimplementedReturnRequestValidationServiceServer
	store                        *Store
	log                          *slog.Logger
	synonymExportClient          searchv1.SynonymExportServiceClient
	returnLabelCalculationClient returnsv1.ReturnLabelCalculationServiceClient
	userQueryClient              identityv1.UserQueryServiceClient
	loyaltyMemberClient          loyaltyv1.LoyaltyMemberServiceClient
	translationKeyClient         localizationv1.TranslationKeyServiceClient
	bus                          *eventbus.Bus
	conn                         *grpc.ClientConn
	loyaltyMemberOnce            sync.Once
	loyaltyMemberCached          loyaltyv1.LoyaltyMemberServiceClient
}

// NewReturnRequestValidationServer wires a ReturnRequestValidationServer.
func NewReturnRequestValidationServer(deps platform.Deps) *ReturnRequestValidationServer {
	return &ReturnRequestValidationServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.returns.v1.ReturnRequestValidationService"),
		synonymExportClient:          searchv1.NewSynonymExportServiceClient(deps.Conn),
		returnLabelCalculationClient: returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn),
		userQueryClient:              identityv1.NewUserQueryServiceClient(deps.Conn),
		loyaltyMemberClient:          loyaltyv1.NewLoyaltyMemberServiceClient(deps.Conn),
		translationKeyClient:         localizationv1.NewTranslationKeyServiceClient(deps.Conn),
		bus:                          deps.Bus,
		conn:                         deps.Conn,
	}
}

// ValidateReturnRequest validates a return request.
func (s *ReturnRequestValidationServer) ValidateReturnRequest(ctx context.Context, req *returnsv1.ValidateReturnRequestRequest) (*returnsv1.ValidateReturnRequestResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "validate return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetReturnRequest()).(*returnsv1.ReturnRequest)
	streamSynonymExportStream, err := s.synonymExportClient.StreamSynonymExport(ctx, &searchv1.StreamSynonymExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSynonymExportRequest_Filter{
			Region:       req.GetReturnRequest().GetBillingAddress().GetRegion(),
			CreatedAfter: cloned.GetDueAt(),
			OwnerId:      req.GetReturnRequest().GetReturnRequestId(),
			Labels:       req.GetReturnRequest().GetMetadata(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/StreamSynonymExport")
	}
	for {
		received, err := streamSynonymExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/StreamSynonymExport")
		}
		_ = received
	}
	return &returnsv1.ValidateReturnRequestResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			ReturnRequestId: req.GetReturnRequestId(),
			BillingAddress:  req.GetReturnRequest().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewReturnRequest previews a return request.
func (s *ReturnRequestValidationServer) PreviewReturnRequest(ctx context.Context, req *returnsv1.PreviewReturnRequestRequest) (*returnsv1.PreviewReturnRequestResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "preview return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	for attachment := range iterx.Each(req.GetReturnRequest().GetAttachments(), func(candidate *returnsv1.ReturnRequestAttachment) bool { return candidate != nil }) {
		if _, err := s.returnLabelCalculationClient.RecalculateReturnLabels(ctx, &returnsv1.RecalculateReturnLabelsRequest{
			Context: req.GetContext(),
			ReturnLabel: &returnsv1.ReturnLabel{
				Attachments: []*returnsv1.ReturnLabelAttachment{&returnsv1.ReturnLabelAttachment{
					Checksum:    attachment.GetChecksum(),
					FileName:    attachment.GetFileName(),
					Uri:         attachment.GetUri(),
					ContentType: attachment.GetContentType(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/RecalculateReturnLabels")
		}
	}
	return &returnsv1.PreviewReturnRequestResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			ReturnRequestId: req.GetReturnRequestId(),
			BillingAddress:  req.GetReturnRequest().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckReturnRequestEligibility checks a return request.
func (s *ReturnRequestValidationServer) CheckReturnRequestEligibility(ctx context.Context, req *returnsv1.CheckReturnRequestEligibilityRequest) (*returnsv1.CheckReturnRequestEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckReturnRequestEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "check return request eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	peekUserRequest := &identityv1.PeekUserRequest{
		Context: req.GetContext(),
		UserId:  tenancy.Value(ctx, "user_id"),
	}
	peekUserReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*identityv1.PeekUserResponse, error) {
		return s.userQueryClient.PeekUser(ctx, peekUserRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/PeekUser")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded returnsv1.CheckReturnRequestEligibilityRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	detailsCache := cache.New[string, *returnsv1.CheckReturnRequestEligibilityRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	watchLoyaltyMembersStream, err := s.loyaltyMemberClient.WatchLoyaltyMembers(ctx, &loyaltyv1.WatchLoyaltyMembersRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email:            peekUserReply.GetUser().GetContact().GetEmail(),
			Name:             (&decoded).GetContact().GetName(),
			Phone:            cachedDetails.GetContact().GetPhone(),
			PreferredChannel: peekUserReply.GetUser().GetContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberService/WatchLoyaltyMembers")
	}
	for {
		received, err := watchLoyaltyMembersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberService/WatchLoyaltyMembers")
		}
		_ = received
	}
	return &returnsv1.CheckReturnRequestEligibilityResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			BillingAddress: &commonv1.PostalAddress{
				Locality: peekUserReply.GetUser().GetPrimaryAddress().GetLocality(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
			Total:           req.GetReturnRequest().GetTotal(),
		},
		Result: &commonv1.OperationResult{
			Message: peekUserReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// NormalizeReturnRequest normalizes a return request.
func (s *ReturnRequestValidationServer) NormalizeReturnRequest(ctx context.Context, req *returnsv1.NormalizeReturnRequestRequest) (*returnsv1.NormalizeReturnRequestResponse, error) {
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "normalize return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	watchTranslationKeysStream, err := s.translationKeyClient.WatchTranslationKeys(ctx, &localizationv1.WatchTranslationKeysRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:             req.GetDetails().GetContact().GetName(),
			Email:            req.GetReturnRequest().GetCounterparty().GetEmail(),
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			Phone:            req.GetDetails().GetContact().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.localization.v1.TranslationKeyService/WatchTranslationKeys")
	}
	for {
		received, err := watchTranslationKeysStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.localization.v1.TranslationKeyService/WatchTranslationKeys")
		}
		_ = received
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "returns.normalize_return_request.update_subscription",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"subscription.card.last4": req.GetReturnRequest().GetCard().GetLast4(),
			"context.tenant_id":       req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &returnsv1.NormalizeReturnRequestResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			ReturnRequestId: req.GetReturnRequestId(),
			DueAt:           req.GetReturnRequest().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainReturnRequestDecision explains a return request.
func (s *ReturnRequestValidationServer) ExplainReturnRequestDecision(ctx context.Context, req *returnsv1.ExplainReturnRequestDecisionRequest) (*returnsv1.ExplainReturnRequestDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainReturnRequestDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "explain return request decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	var holder struct{ Email string }
	if record := req.GetReturnRequest().GetCounterparty(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetContact().GetPreferredChannel())
	sendSessionTestNoticeRequest := &identityv1.SendSessionTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendSessionTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            holder.Email,
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
			},
		},
	}
	sendSessionTestNoticeReply := new(identityv1.SendSessionTestNoticeResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.SessionNotificationService_SendSessionTestNotice_FullMethodName, sendSessionTestNoticeRequest, sendSessionTestNoticeReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/SendSessionTestNotice")
	}
	updateLoyaltyMemberRequest := &loyaltyv1.UpdateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetReturnRequest().GetAudit().GetCreatedBy(),
				UpdatedAt: req.GetReturnRequest().GetAudit().GetUpdatedAt(),
			},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	updateLoyaltyMemberReply, err := s.loyaltyMemberLazy().UpdateLoyaltyMember(ctx, updateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberService/UpdateLoyaltyMember")
	}
	return &returnsv1.ExplainReturnRequestDecisionResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				Email: sendSessionTestNoticeReply.GetSession().GetRequester().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: sendSessionTestNoticeReply.GetSession().GetAudit().GetUpdatedAt(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
			Number:          req.GetReturnRequest().GetNumber(),
		},
		Outcome: &returnsv1.ExplainReturnRequestDecisionResponse_Outcome{Status: updateLoyaltyMemberReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreReturnRequest scores a return request.
func (s *ReturnRequestValidationServer) ScoreReturnRequest(ctx context.Context, req *returnsv1.ScoreReturnRequestRequest) (*returnsv1.ScoreReturnRequestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "score return request", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetReturnRequest()).(*returnsv1.ReturnRequest)
	calculateReturnLabelRequest := newCalculateReturnLabelRequest(withCalculateReturnLabelContext(req.GetContext()), withCalculateReturnLabelReturnLabel(&returnsv1.ReturnLabel{
		Audit: &commonv1.AuditInfo{Etag: req.GetReturnRequest().GetAudit().GetEtag()},
		BillingAddress: &commonv1.PostalAddress{
			Region: req.GetReturnRequest().GetBillingAddress().GetRegion(),
		},
	}), withCalculateReturnLabelDetails(&returnsv1.CalculateReturnLabelRequest_Details{
		Destination: &commonv1.PostalAddress{Locality: cloned.GetBillingAddress().GetLocality()},
	}))
	calculateReturnLabelReply, err := s.returnLabelCalculationClient.CalculateReturnLabel(ctx, calculateReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/CalculateReturnLabel")
	}
	jobs := make(chan *returnsv1.ReturnRequestLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.returnLabelCalculationClient.QuoteReturnLabel(workCtx, &returnsv1.QuoteReturnLabelRequest{
					Context: req.GetContext(),
					ReturnLabel: &returnsv1.ReturnLabel{
						Lines: []*returnsv1.ReturnLabelLine{&returnsv1.ReturnLabelLine{
							Tax: &returnsv1.ReturnLabelLineTax{Amount: line.GetTax().GetAmount()},
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
	for _, queued := range req.GetReturnRequest().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/QuoteReturnLabel")
	}
	return &returnsv1.ScoreReturnRequestResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			BillingAddress: &commonv1.PostalAddress{
				Lines: calculateReturnLabelReply.GetReturnLabel().GetBillingAddress().GetLines(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
			Metadata:        req.GetReturnRequest().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: calculateReturnLabelReply.GetResult().GetStatus()},
	}, nil
}

// GetReturnRequestRules returns a return request.
func (s *ReturnRequestValidationServer) GetReturnRequestRules(ctx context.Context, req *returnsv1.GetReturnRequestRulesRequest) (*returnsv1.GetReturnRequestRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_request", req.GetReturnRequestId(), "get_return_request_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get return request rules", "tenant", req.GetContext().GetTenantId())
	return &returnsv1.GetReturnRequestRulesResponse{
		ReturnRequest: &returnsv1.ReturnRequest{ReturnRequestId: req.GetReturnRequestId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// loyaltyMemberLazy dials acme.loyalty.v1.LoyaltyMemberService the first time a handler needs it.
func (s *ReturnRequestValidationServer) loyaltyMemberLazy() loyaltyv1.LoyaltyMemberServiceClient {
	s.loyaltyMemberOnce.Do(func() { s.loyaltyMemberCached = loyaltyv1.NewLoyaltyMemberServiceClient(s.conn) })
	return s.loyaltyMemberCached
}
