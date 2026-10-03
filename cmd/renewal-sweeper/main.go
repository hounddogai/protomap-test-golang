// Command renewal-sweeper renews and expires records on their due dates.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	mesh := flag.String("mesh", "localhost:15001", "the sidecar that reaches every service")
	tenant := flag.String("tenant", "", "the tenant to work for")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	conn, err := grpc.NewClient(*mesh, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(grpcx.Bearer{Token: os.Getenv("ACME_TOKEN"), Tenant: *tenant}))
	if err != nil {
		slog.Error("dial", "error", err)
		os.Exit(1)
	}
	defer conn.Close()
	rc := &commonv1.RequestContext{TenantId: *tenant, Actor: &commonv1.Actor{ServiceAccount: true, UserId: "renewal-sweeper"}}
	for _, run := range []func() error{
		func() error { return sweepProductReview0(ctx, conn, rc) },
		func() error { return sweepLoyaltyMember1(ctx, conn, rc) },
		func() error { return sweepPayment2(ctx, conn, rc) },
	} {
		if err := run(); err != nil {
			slog.Error("sweep failed", "error", err)
			os.Exit(1)
		}
	}
}

// sweepProductReview0 lists every product review of the tenant and runs TransferSearchQuery on each.
func sweepProductReview0(ctx context.Context, conn grpc.ClientConnInterface, rc *commonv1.RequestContext) error {
	lister := reviewsv1.NewProductReviewServiceClient(conn)
	acting := searchv1.NewSearchQueryAdminServiceClient(conn)
	token := ""
	for {
		page, err := lister.SearchProductReviews(ctx, &reviewsv1.SearchProductReviewsRequest{Context: rc, Page: &commonv1.PageRequest{PageSize: 200, PageToken: token}})
		if err != nil {
			return err
		}
		for _, item := range page.GetProductReviews() {
			if _, err := acting.TransferSearchQuery(ctx, &searchv1.TransferSearchQueryRequest{
				Context: rc,
				Details: &searchv1.TransferSearchQueryRequest_Details{
					NotifyContact: &commonv1.ContactInfo{Email: item.GetCounterparty().GetEmail()},
					NewOwner: &identityv1.UserRef{
						DisplayName: item.GetCounterparty().GetDisplayName(),
						Email:       item.GetCounterparty().GetEmail(),
					},
				},
			}); err != nil {
				slog.WarnContext(ctx, "transfer search query failed", "error", err)
			}
		}
		token = page.GetPageInfo().GetNextPageToken()
		if token == "" {
			return nil
		}
	}
}

// sweepLoyaltyMember1 lists every loyalty member of the tenant and runs SubmitUsageRecord on each.
func sweepLoyaltyMember1(ctx context.Context, conn grpc.ClientConnInterface, rc *commonv1.RequestContext) error {
	lister := loyaltyv1.NewLoyaltyMemberServiceClient(conn)
	acting := subscriptionsv1.NewUsageRecordApprovalServiceClient(conn)
	token := ""
	for {
		page, err := lister.SearchLoyaltyMembers(ctx, &loyaltyv1.SearchLoyaltyMembersRequest{Context: rc, Page: &commonv1.PageRequest{PageSize: 200, PageToken: token}})
		if err != nil {
			return err
		}
		for _, item := range page.GetLoyaltyMembers() {
			if _, err := acting.SubmitUsageRecord(ctx, &subscriptionsv1.SubmitUsageRecordRequest{
				Context: rc,
				UsageRecord: &subscriptionsv1.UsageRecord{
					Subject: &customersv1.CustomerRef{
						DisplayName: item.GetHolder().GetDisplayName(),
						Email:       item.GetContact().GetEmail(),
					},
				},
				Details: &subscriptionsv1.SubmitUsageRecordRequest_Details{
					Approver: &identityv1.UserRef{Email: item.GetContact().GetEmail()},
				},
			}); err != nil {
				slog.WarnContext(ctx, "submit usage record failed", "error", err)
			}
		}
		token = page.GetPageInfo().GetNextPageToken()
		if token == "" {
			return nil
		}
	}
}

// sweepPayment2 lists every payment of the tenant and runs PullManifests on each.
func sweepPayment2(ctx context.Context, conn grpc.ClientConnInterface, rc *commonv1.RequestContext) error {
	lister := paymentsv1.NewPaymentServiceClient(conn)
	acting := shippingv1.NewManifestSyncServiceClient(conn)
	token := ""
	for {
		page, err := lister.SearchPayments(ctx, &paymentsv1.SearchPaymentsRequest{Context: rc, Page: &commonv1.PageRequest{PageSize: 200, PageToken: token}})
		if err != nil {
			return err
		}
		for _, item := range page.GetPayments() {
			if _, err := acting.PullManifests(ctx, &shippingv1.PullManifestsRequest{
				Context: rc,
				Manifest: &shippingv1.Manifest{
					Notes:      item.GetNotes(),
					Audit:      &commonv1.AuditInfo{UpdatedAt: item.GetAudit().GetUpdatedAt()},
					Settlement: &shippingv1.Manifest_Card{Card: &shippingv1.ManifestCardSettlement{Network: item.GetCard().GetNetwork()}},
				},
			}); err != nil {
				slog.WarnContext(ctx, "pull manifests failed", "error", err)
			}
		}
		token = page.GetPageInfo().GetNextPageToken()
		if token == "" {
			return nil
		}
	}
}
