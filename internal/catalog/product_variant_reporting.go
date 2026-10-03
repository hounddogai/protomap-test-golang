package catalog

import (
	"context"
	"log/slog"

	admissionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/admissions/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ProductVariantReportingServer implements `acme.catalog.v1.ProductVariantReportingService`.
// Builds and schedules reports about product variants.
type ProductVariantReportingServer struct {
	catalogv1.UnimplementedProductVariantReportingServiceServer
	store                         *Store
	log                           *slog.Logger
	tenantAdminClient             identityv1.TenantAdminServiceClient
	brandValidationPort           brandValidationPort
	accessPolicyValidationClient  authorizationv1.AccessPolicyValidationServiceClient
	conn                          *grpc.ClientConn
	brandValidationClient         catalogv1.BrandValidationServiceClient
	productValidationClient       catalogv1.ProductValidationServiceClient
	restockDecisionApprovalClient returnsv1.RestockDecisionApprovalServiceClient
	attributeCalculationClient    catalogv1.AttributeCalculationServiceClient
	admissionCalculationClient    admissionsv1.AdmissionCalculationServiceClient
	productClient                 catalogv1.ProductServiceClient
	brandCalculationClient        catalogv1.BrandCalculationServiceClient
	attributeValidationClient     catalogv1.AttributeValidationServiceClient
}

// NewProductVariantReportingServer wires a ProductVariantReportingServer.
func NewProductVariantReportingServer(deps platform.Deps) *ProductVariantReportingServer {
	return &ProductVariantReportingServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.catalog.v1.ProductVariantReportingService"),
		tenantAdminClient:             identityv1.NewTenantAdminServiceClient(deps.Conn),
		brandValidationPort:           newBrandValidationPort(deps),
		accessPolicyValidationClient:  authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		conn:                          deps.Conn,
		brandValidationClient:         catalogv1.NewBrandValidationServiceClient(deps.Conn),
		productValidationClient:       catalogv1.NewProductValidationServiceClient(deps.Conn),
		restockDecisionApprovalClient: returnsv1.NewRestockDecisionApprovalServiceClient(deps.Conn),
		attributeCalculationClient:    catalogv1.NewAttributeCalculationServiceClient(deps.Conn),
		admissionCalculationClient:    admissionsv1.NewAdmissionCalculationServiceClient(deps.Conn),
		productClient:                 catalogv1.NewProductServiceClient(deps.Conn),
		brandCalculationClient:        catalogv1.NewBrandCalculationServiceClient(deps.Conn),
		attributeValidationClient:     catalogv1.NewAttributeValidationServiceClient(deps.Conn),
	}
}

// GenerateProductVariantReport generates a product variant.
func (s *ProductVariantReportingServer) GenerateProductVariantReport(ctx context.Context, req *catalogv1.GenerateProductVariantReportRequest) (*catalogv1.GenerateProductVariantReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "generate_product_variant_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate product variant report", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := productVariantReportingServerLockTenantSteps[mode]
	ctx = tenancy.With(ctx, "tenant_id", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	lockTenantRequest := &identityv1.LockTenantRequest{
		Context:     req.GetContext(),
		TenantId:    tenancy.Value(ctx, "tenant_id"),
		Annotations: req.GetAnnotations(),
		Details:     &identityv1.LockTenantRequest_Details{NewOwner: &identityv1.UserRef{UserId: userId}},
		Reason:      req.GetReason(),
	}
	lockTenantReply, err := run(s, ctx, lockTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/LockTenant")
	}
	lintBrandsRequest := &catalogv1.LintBrandsRequest{
		Context: req.GetContext(),
		Brand: &catalogv1.Brand{
			PrimaryAddress: &commonv1.PostalAddress{
				CountryCode: lockTenantReply.GetTenant().GetLocation().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: lockTenantReply.GetTenant().GetAudit().GetUpdatedAt(),
			},
		},
	}
	lintBrandsReply, err := s.brandValidationPort.LintBrands(ctx, lintBrandsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandValidationService/LintBrands")
	}
	scoreAccessPolicyRequest := buildScoreAccessPolicyRequestForGenerateProductVariantReport(req, lockTenantReply, lintBrandsReply)
	scoreAccessPolicyReply, err := s.accessPolicyValidationClient.ScoreAccessPolicy(ctx, scoreAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/ScoreAccessPolicy")
	}
	previewBrandRequest := &catalogv1.PreviewBrandRequest{
		Context: req.GetContext(),
		Details: &catalogv1.PreviewBrandRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: scoreAccessPolicyReply.GetAccessPolicy().GetOwner().GetEmail(),
			},
		},
		Brand: &catalogv1.Brand{
			IdentityDocument: &catalogv1.Brand_NationalId{NationalId: &catalogv1.BrandNationalId{
				Number: lintBrandsReply.GetBrand().GetPassport().GetNumber(),
			}},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: lintBrandsReply.GetBrand().GetContact().GetPreferredChannel(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: lockTenantReply.GetTenant().GetLocation().GetPostalCode(),
			},
		},
	}
	previewBrandReply, err := grpcx.NewClient(s.conn, catalogv1.NewBrandValidationServiceClient).PreviewBrand(ctx, previewBrandRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandValidationService/PreviewBrand")
	}
	_ = previewBrandReply
	return &catalogv1.GenerateProductVariantReportResponse{
		Result: &commonv1.OperationResult{
			Status:  lockTenantReply.GetResult().GetStatus(),
			Message: lintBrandsReply.GetResult().GetMessage(),
			Details: scoreAccessPolicyReply.GetResult().GetDetails(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchProductVariantReport returns a product variant.
func (s *ProductVariantReportingServer) FetchProductVariantReport(ctx context.Context, req *catalogv1.FetchProductVariantReportRequest) (*catalogv1.FetchProductVariantReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "fetch_product_variant_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch product variant report", "tenant", req.GetContext().GetTenantId())
	batchGetProductsRequest := &catalogv1.BatchGetProductsRequest{Context: req.GetContext()}
	batchGetProductsReply := new(catalogv1.BatchGetProductsResponse)
	if err := s.conn.Invoke(ctx, catalogProductServiceBatchGetProductsMethod, batchGetProductsRequest, batchGetProductsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductService/BatchGetProducts")
	}
	previewBrandRequest := newPreviewBrandRequest(withPreviewBrandContext(req.GetContext()), withPreviewBrandBrand(&catalogv1.Brand{
		RelationshipManager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
	}))
	previewBrandReply, err := s.brandValidationClient.PreviewBrand(ctx, previewBrandRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandValidationService/PreviewBrand")
	}
	return &catalogv1.FetchProductVariantReportResponse{
		Result: &commonv1.OperationResult{
			Details: batchGetProductsReply.GetResult().GetDetails(),
			Message: previewBrandReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				Email: previewBrandReply.GetBrand().GetRelationshipManager().GetEmail(),
			},
			ProductVariantId: req.GetProductVariantId(),
		},
	}, nil
}

// ScheduleProductVariantReport schedules a product variant.
func (s *ProductVariantReportingServer) ScheduleProductVariantReport(ctx context.Context, req *catalogv1.ScheduleProductVariantReportRequest) (*catalogv1.ScheduleProductVariantReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "schedule_product_variant_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule product variant report", "tenant", req.GetContext().GetTenantId())
	explainProductDecisionReply, err := s.productValidationClient.ExplainProductDecision(ctx, &catalogv1.ExplainProductDecisionRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/ExplainProductDecision")
	}
	return &catalogv1.ScheduleProductVariantReportResponse{
		Result: &commonv1.OperationResult{
			Status:        explainProductDecisionReply.GetOutcome().GetStatus(),
			CorrelationId: explainProductDecisionReply.GetResult().GetCorrelationId(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamProductVariantMetrics streams the changes of product variants as they happen.
func (s *ProductVariantReportingServer) StreamProductVariantMetrics(req *catalogv1.StreamProductVariantMetricsRequest, stream grpc.ServerStreamingServer[catalogv1.StreamProductVariantMetricsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StreamProductVariantMetrics", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "stream product variant metrics", "tenant", req.GetContext().GetTenantId())
	listPendingRestockDecisionsRequest := &returnsv1.ListPendingRestockDecisionsRequest{
		Context: req.GetContext(),
		Filter:  &returnsv1.ListPendingRestockDecisionsRequest_Filter{Region: req.GetFilter().GetRegion()},
	}
	listPendingRestockDecisionsReply := new(returnsv1.ListPendingRestockDecisionsResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.returns.v1.RestockDecisionApprovalService/ListPendingRestockDecisions", listPendingRestockDecisionsRequest, listPendingRestockDecisionsReply); err != nil {
		return grpcx.Wrap(err, "acme.returns.v1.RestockDecisionApprovalService/ListPendingRestockDecisions")
	}
	submitRestockDecisionReply, err := s.restockDecisionApprovalClient.SubmitRestockDecision(ctx, &returnsv1.SubmitRestockDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Action: &returnsv1.RestockDecision_Block{Block: &returnsv1.RestockDecisionBlockAction{NotifyEmail: req.GetSubscriber().GetEmail()}},
			Owner:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit:  &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &returnsv1.SubmitRestockDecisionRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.returns.v1.RestockDecisionApprovalService/SubmitRestockDecision")
	}
	if err := stream.Send(&catalogv1.StreamProductVariantMetricsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listPendingRestockDecisionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ProductVariant: &catalogv1.ProductVariant{
			Name:  submitRestockDecisionReply.GetRestockDecision().GetName(),
			Owner: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListProductVariantReports lists product variants a page at a time.
func (s *ProductVariantReportingServer) ListProductVariantReports(ctx context.Context, req *catalogv1.ListProductVariantReportsRequest) (*catalogv1.ListProductVariantReportsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list product variant reports", "tenant", req.GetContext().GetTenantId())
	return &catalogv1.ListProductVariantReportsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ShareProductVariantReport shares a product variant.
func (s *ProductVariantReportingServer) ShareProductVariantReport(ctx context.Context, req *catalogv1.ShareProductVariantReportRequest) (*catalogv1.ShareProductVariantReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "share_product_variant_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share product variant report", "tenant", req.GetContext().GetTenantId())
	var envelope catalogv1.ProductRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	quoteAttributeRequest := &catalogv1.QuoteAttributeRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		AttributeId: envelope.GetDisplayName(),
	}
	quoteAttributeReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*catalogv1.QuoteAttributeResponse, error) {
		return s.attributeCalculationClient.QuoteAttribute(ctx, quoteAttributeRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeCalculationService/QuoteAttribute")
	}
	var envelope2 catalogv1.ProductRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainAdmissionCalculationReply, err := s.admissionCalculationClient.ExplainAdmissionCalculation(ctx, &admissionsv1.ExplainAdmissionCalculationRequest{
		Context: req.GetContext(),
		Details: &admissionsv1.ExplainAdmissionCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: quoteAttributeReply.GetAttribute().GetOwner().GetEmail(),
			},
		},
		AdmissionId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.admissions.v1.AdmissionCalculationService/ExplainAdmissionCalculation")
	}
	return &catalogv1.ShareProductVariantReportResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Name:             quoteAttributeReply.GetAttribute().GetName(),
			Owner:            quoteAttributeReply.GetAttribute().GetOwner(),
			ProductVariantId: req.GetProductVariantId(),
		},
		Outcome: &catalogv1.ShareProductVariantReportResponse_Outcome{
			Status:      explainAdmissionCalculationReply.GetOutcome().GetStatus(),
			ReferenceId: explainAdmissionCalculationReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountProductVariantTotals counts product variants by status.
func (s *ProductVariantReportingServer) CountProductVariantTotals(ctx context.Context, req *catalogv1.CountProductVariantTotalsRequest) (*catalogv1.CountProductVariantTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count product variant totals", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	normalizeBrandRequest := &catalogv1.NormalizeBrandRequest{
		Context: req.GetContext(),
		Brand: &catalogv1.Brand{
			PrimaryAddress:      &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Contact:             &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			RelationshipManager: &identityv1.UserRef{UserId: userId},
		},
	}
	var normalizeBrandReply *catalogv1.NormalizeBrandResponse
	if err := pipeline.Run(ctx, "acme.catalog.v1.BrandValidationService/NormalizeBrand", func(ctx context.Context) error {
		answer, err := s.brandValidationClient.NormalizeBrand(ctx, normalizeBrandRequest)
		normalizeBrandReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandValidationService/NormalizeBrand")
	}
	cloneProductRequest := &catalogv1.CloneProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				Locality: normalizeBrandReply.GetBrand().GetPrimaryAddress().GetLocality(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizeBrandReply.GetBrand().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				UserId: normalizeBrandReply.GetBrand().GetRelationshipManager().GetUserId(),
			},
		},
	}
	type cloneProductResult struct {
		reply *catalogv1.CloneProductResponse
		err   error
	}
	cloneProductResults := make(chan cloneProductResult, 1)
	go func() {
		reply, err := s.productClient.CloneProduct(ctx, cloneProductRequest)
		cloneProductResults <- cloneProductResult{reply, err}
	}()
	var cloneProductReply *catalogv1.CloneProductResponse
	select {
	case result := <-cloneProductResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.catalog.v1.ProductService/CloneProduct")
		}
		cloneProductReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &catalogv1.CountProductVariantTotalsResponse{
		Result: &commonv1.OperationResult{
			Details:       normalizeBrandReply.GetResult().GetDetails(),
			Status:        normalizeBrandReply.GetOutcome().GetStatus(),
			CorrelationId: cloneProductReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// DeleteProductVariantReport deletes a product variant.
func (s *ProductVariantReportingServer) DeleteProductVariantReport(ctx context.Context, req *catalogv1.DeleteProductVariantReportRequest) (*catalogv1.DeleteProductVariantReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DeleteProductVariantReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "delete_product_variant_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete product variant report", "tenant", req.GetContext().GetTenantId())
	calculateBrandReply, err := s.brandCalculationClient.CalculateBrand(ctx, &catalogv1.CalculateBrandRequest{
		Context: req.GetContext(),
		Brand:   &catalogv1.Brand{Audit: &commonv1.AuditInfo{Etag: req.GetEtag()}},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandCalculationService/CalculateBrand")
	}
	scoreAttributeRequest := &catalogv1.ScoreAttributeRequest{
		Context: req.GetContext(),
		Details: &catalogv1.ScoreAttributeRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: calculateBrandReply.GetBrand().GetPrimaryAddress().GetCountryCode(),
			},
		},
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{Etag: calculateBrandReply.GetBrand().GetAudit().GetEtag()},
		},
	}
	scoreAttributeReply, err := grpcx.Call(ctx, s.attributeValidationClient.ScoreAttribute, scoreAttributeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeValidationService/ScoreAttribute")
	}
	step := &productVariantReportingServerDeleteProductVariantReportPreviewAttributeStep{request: &catalogv1.PreviewAttributeRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scoreAttributeReply.GetAttribute().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				Email: calculateBrandReply.GetBrand().GetContact().GetEmail(),
			},
		},
	}}
	if err := s.runDeleteProductVariantReportPreviewAttribute(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeValidationService/PreviewAttribute")
	}
	previewAttributeReply := step.reply
	return &catalogv1.DeleteProductVariantReportResponse{
		Result: &commonv1.OperationResult{
			Message:       calculateBrandReply.GetResult().GetMessage(),
			CorrelationId: scoreAttributeReply.GetResult().GetCorrelationId(),
			Status:        previewAttributeReply.GetResult().GetStatus(),
			ReferenceIds:  previewAttributeReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// productVariantReportingServerLockTenantSteps are the ways to run acme.identity.v1.TenantAdminService/LockTenant, by
// name.
var productVariantReportingServerLockTenantSteps = map[string]func(*ProductVariantReportingServer, context.Context, *identityv1.LockTenantRequest) (*identityv1.LockTenantResponse, error){
	"default": (*ProductVariantReportingServer).lockTenantStep,
	"audited": (*ProductVariantReportingServer).lockTenantAuditedStep,
}

func (s *ProductVariantReportingServer) lockTenantStep(ctx context.Context, request *identityv1.LockTenantRequest) (*identityv1.LockTenantResponse, error) {
	return s.tenantAdminClient.LockTenant(ctx, request)
}

func (s *ProductVariantReportingServer) lockTenantAuditedStep(ctx context.Context, request *identityv1.LockTenantRequest) (*identityv1.LockTenantResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.TenantAdminService/LockTenant")
	return s.tenantAdminClient.LockTenant(ctx, request)
}

// productVariantReportingServerDeleteProductVariantReportPreviewAttributeStep carries the request of one step of
// DeleteProductVariantReport and, once it ran, its reply.
type productVariantReportingServerDeleteProductVariantReportPreviewAttributeStep struct {
	request *catalogv1.PreviewAttributeRequest
	reply   *catalogv1.PreviewAttributeResponse
}

func (s *ProductVariantReportingServer) runDeleteProductVariantReportPreviewAttribute(ctx context.Context, step *productVariantReportingServerDeleteProductVariantReportPreviewAttributeStep) error {
	reply, err := s.attributeValidationClient.PreviewAttribute(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
