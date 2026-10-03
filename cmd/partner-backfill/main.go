// Command partner-backfill copies partner records into the domains.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
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
	rc := &commonv1.RequestContext{TenantId: *tenant, Actor: &commonv1.Actor{ServiceAccount: true, UserId: "partner-backfill"}}
	for _, run := range []func() error{
		func() error { return sweepPriceList0(ctx, conn, rc) },
		func() error { return sweepShopperView1(ctx, conn, rc) },
		func() error { return sweepCheckoutSession2(ctx, conn, rc) },
	} {
		if err := run(); err != nil {
			slog.Error("sweep failed", "error", err)
			os.Exit(1)
		}
	}
}

// sweepPriceList0 lists every price list of the tenant and runs SubmitPriceQuote on each.
func sweepPriceList0(ctx context.Context, conn grpc.ClientConnInterface, rc *commonv1.RequestContext) error {
	lister := pricingv1.NewPriceListServiceClient(conn)
	acting := pricingv1.NewPriceQuoteApprovalServiceClient(conn)
	token := ""
	for {
		page, err := lister.SearchPriceLists(ctx, &pricingv1.SearchPriceListsRequest{Context: rc, Page: &commonv1.PageRequest{PageSize: 200, PageToken: token}})
		if err != nil {
			return err
		}
		for _, item := range page.GetPriceLists() {
			if _, err := acting.SubmitPriceQuote(ctx, &pricingv1.SubmitPriceQuoteRequest{
				Context: rc,
				PriceQuote: &pricingv1.PriceQuote{
					Counterparty: &pricingv1.PriceListRef{
						DisplayName: item.GetOwner().GetDisplayName(),
						PriceListId: item.GetPriceListId(),
					},
					Audit: &commonv1.AuditInfo{CreatedAt: item.GetAudit().GetCreatedAt()},
				},
			}); err != nil {
				slog.WarnContext(ctx, "submit price quote failed", "error", err)
			}
		}
		token = page.GetPageInfo().GetNextPageToken()
		if token == "" {
			return nil
		}
	}
}

// sweepShopperView1 lists every shopper view of the tenant and runs StartPickListWorkflow on each.
func sweepShopperView1(ctx context.Context, conn grpc.ClientConnInterface, rc *commonv1.RequestContext) error {
	lister := storefrontv1.NewShopperViewServiceClient(conn)
	acting := warehousingv1.NewPickListWorkflowServiceClient(conn)
	token := ""
	for {
		page, err := lister.SearchShopperViews(ctx, &storefrontv1.SearchShopperViewsRequest{Context: rc, Page: &commonv1.PageRequest{PageSize: 200, PageToken: token}})
		if err != nil {
			return err
		}
		for _, item := range page.GetShopperViews() {
			if _, err := acting.StartPickListWorkflow(ctx, &warehousingv1.StartPickListWorkflowRequest{
				Context: rc,
				PickList: &warehousingv1.PickList{
					BillingAddress: &commonv1.PostalAddress{
						Locality:  item.GetAddress().GetLocality(),
						Recipient: item.GetAddress().GetRecipient(),
					},
					Audit: &commonv1.AuditInfo{UpdatedBy: item.GetAudit().GetUpdatedBy()},
				},
			}); err != nil {
				slog.WarnContext(ctx, "start pick list workflow failed", "error", err)
			}
		}
		token = page.GetPageInfo().GetNextPageToken()
		if token == "" {
			return nil
		}
	}
}

// sweepCheckoutSession2 lists every checkout session of the tenant and runs RejectPartnerOrder on each.
func sweepCheckoutSession2(ctx context.Context, conn grpc.ClientConnInterface, rc *commonv1.RequestContext) error {
	lister := checkoutv1.NewCheckoutSessionServiceClient(conn)
	acting := partnerapiv1.NewPartnerOrderApprovalServiceClient(conn)
	token := ""
	for {
		page, err := lister.ListCheckoutSessions(ctx, &checkoutv1.ListCheckoutSessionsRequest{Context: rc, Page: &commonv1.PageRequest{PageSize: 200, PageToken: token}})
		if err != nil {
			return err
		}
		for _, item := range page.GetCheckoutSessions() {
			if _, err := acting.RejectPartnerOrder(ctx, &partnerapiv1.RejectPartnerOrderRequest{
				Context: rc,
				PartnerOrder: &partnerapiv1.PartnerOrder{
					Notes:      item.GetNotes(),
					Settlement: &partnerapiv1.PartnerOrder_Card{Card: &partnerapiv1.PartnerOrderCardSettlement{HolderName: item.GetCard().GetHolderName()}},
				},
				Details: &partnerapiv1.RejectPartnerOrderRequest_Details{
					Approver: &identityv1.UserRef{DisplayName: item.GetCounterparty().GetDisplayName()},
				},
			}); err != nil {
				slog.WarnContext(ctx, "reject partner order failed", "error", err)
			}
		}
		token = page.GetPageInfo().GetNextPageToken()
		if token == "" {
			return nil
		}
	}
}
