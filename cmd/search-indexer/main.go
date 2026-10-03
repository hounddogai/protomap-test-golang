// Command search-indexer follows the change streams of the domains and indexes what changed.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	mesh := flag.String("mesh", "localhost:15001", "the sidecar that reaches every service")
	tenant := flag.String("tenant", os.Getenv("ACME_TENANT"), "the tenant to index")
	flag.Parse()
	conn, err := grpc.NewClient(*mesh, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(grpcx.Bearer{Token: os.Getenv("ACME_TOKEN"), Tenant: *tenant}))
	if err != nil {
		slog.Error("dial", "error", err)
		os.Exit(1)
	}
	defer conn.Close()
	rc := &commonv1.RequestContext{TenantId: *tenant, Actor: &commonv1.Actor{ServiceAccount: true}}
	group, ctx := errgroup.WithContext(context.Background())
	group.Go(func() error { return followSearchQuery0(ctx, conn, rc) })
	group.Go(func() error { return followSellerScorecard1(ctx, conn, rc) })
	if err := group.Wait(); err != nil {
		slog.Error("indexer stopped", "error", err)
		os.Exit(1)
	}
}

// followSearchQuery0 indexes every change of search queries.
func followSearchQuery0(ctx context.Context, conn grpc.ClientConnInterface, rc *commonv1.RequestContext) error {
	changes, err := searchv1.NewSearchQueryNotificationServiceClient(conn).StreamSearchQueryNotices(ctx, &searchv1.StreamSearchQueryNoticesRequest{Context: rc})
	if err != nil {
		return err
	}
	index := subscriptionsv1.NewSubscriptionApprovalServiceClient(conn)
	for {
		change, err := changes.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := index.ApproveSubscription(ctx, &subscriptionsv1.ApproveSubscriptionRequest{
			Context: rc,
			Details: &subscriptionsv1.ApproveSubscriptionRequest_Details{
				Approver: &identityv1.UserRef{
					DisplayName: change.GetSearchQuery().GetSubject().GetDisplayName(),
				},
			},
			Subscription: &subscriptionsv1.Subscription{
				Holder: &customersv1.CustomerRef{
					DisplayName: change.GetSearchQuery().GetSubject().GetDisplayName(),
				},
			},
		}); err != nil {
			slog.WarnContext(ctx, "index failed", "error", err)
		}
	}
}

// followSellerScorecard1 indexes every change of seller scorecards.
func followSellerScorecard1(ctx context.Context, conn grpc.ClientConnInterface, rc *commonv1.RequestContext) error {
	changes, err := sellersv1.NewSellerScorecardReportingServiceClient(conn).StreamSellerScorecardMetrics(ctx, &sellersv1.StreamSellerScorecardMetricsRequest{Context: rc})
	if err != nil {
		return err
	}
	index := giftcardsv1.NewGiftCardBatchWorkflowServiceClient(conn)
	for {
		change, err := changes.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := index.ResumeGiftCardBatchWorkflow(ctx, &giftcardsv1.ResumeGiftCardBatchWorkflowRequest{
			Context: rc,
			GiftCardBatch: &giftcardsv1.GiftCardBatch{
				Settlement: &giftcardsv1.GiftCardBatch_Card{Card: &giftcardsv1.GiftCardBatchCardSettlement{
					HolderName: change.GetSellerScorecard().GetCard().GetHolderName(),
				}},
				Audit: &commonv1.AuditInfo{
					CreatedBy: change.GetSellerScorecard().GetAudit().GetCreatedBy(),
					UpdatedAt: change.GetSellerScorecard().GetAudit().GetUpdatedAt(),
				},
			},
		}); err != nil {
			slog.WarnContext(ctx, "index failed", "error", err)
		}
	}
}
