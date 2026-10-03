// Command nightly-reconciler walks every open record and settles what changed.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
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
	rc := &commonv1.RequestContext{TenantId: *tenant, Actor: &commonv1.Actor{ServiceAccount: true, UserId: "nightly-reconciler"}}
	for _, run := range []func() error{
		func() error { return sweepShipment0(ctx, conn, rc) },
		func() error { return sweepShipment1(ctx, conn, rc) },
		func() error { return sweepCheckoutSession2(ctx, conn, rc) },
		func() error { return sweepUser3(ctx, conn, rc) },
	} {
		if err := run(); err != nil {
			slog.Error("sweep failed", "error", err)
			os.Exit(1)
		}
	}
}

// sweepShipment0 lists every shipment of the tenant and runs RetryPlacementWorkflow on each.
func sweepShipment0(ctx context.Context, conn grpc.ClientConnInterface, rc *commonv1.RequestContext) error {
	lister := shippingv1.NewShipmentServiceClient(conn)
	acting := merchandisingv1.NewPlacementWorkflowServiceClient(conn)
	token := ""
	for {
		page, err := lister.ListShipments(ctx, &shippingv1.ListShipmentsRequest{Context: rc, Page: &commonv1.PageRequest{PageSize: 200, PageToken: token}})
		if err != nil {
			return err
		}
		for _, item := range page.GetShipments() {
			if _, err := acting.RetryPlacementWorkflow(ctx, &merchandisingv1.RetryPlacementWorkflowRequest{
				Context: rc,
				Placement: &merchandisingv1.Placement{
					Audit: &commonv1.AuditInfo{
						Etag:      item.GetAudit().GetEtag(),
						CreatedBy: item.GetAudit().GetCreatedBy(),
					},
					Metadata: item.GetMetadata(),
				},
			}); err != nil {
				slog.WarnContext(ctx, "retry placement workflow failed", "error", err)
			}
		}
		token = page.GetPageInfo().GetNextPageToken()
		if token == "" {
			return nil
		}
	}
}

// sweepShipment1 lists every shipment of the tenant and runs ExpirePayout on each.
func sweepShipment1(ctx context.Context, conn grpc.ClientConnInterface, rc *commonv1.RequestContext) error {
	lister := shippingv1.NewShipmentServiceClient(conn)
	acting := paymentsv1.NewPayoutLifecycleServiceClient(conn)
	token := ""
	for {
		page, err := lister.SearchShipments(ctx, &shippingv1.SearchShipmentsRequest{Context: rc, Page: &commonv1.PageRequest{PageSize: 200, PageToken: token}})
		if err != nil {
			return err
		}
		for _, item := range page.GetShipments() {
			if _, err := acting.ExpirePayout(ctx, &paymentsv1.ExpirePayoutRequest{
				Context: rc,
				Details: &paymentsv1.ExpirePayoutRequest_Details{
					NewOwner: &identityv1.UserRef{DisplayName: item.GetCounterparty().GetDisplayName()},
				},
			}); err != nil {
				slog.WarnContext(ctx, "expire payout failed", "error", err)
			}
		}
		token = page.GetPageInfo().GetNextPageToken()
		if token == "" {
			return nil
		}
	}
}

// sweepCheckoutSession2 lists every checkout session of the tenant and runs EscalateCart on each.
func sweepCheckoutSession2(ctx context.Context, conn grpc.ClientConnInterface, rc *commonv1.RequestContext) error {
	lister := checkoutv1.NewCheckoutSessionServiceClient(conn)
	acting := cartv1.NewCartApprovalServiceClient(conn)
	token := ""
	for {
		page, err := lister.ListCheckoutSessions(ctx, &checkoutv1.ListCheckoutSessionsRequest{Context: rc, Page: &commonv1.PageRequest{PageSize: 200, PageToken: token}})
		if err != nil {
			return err
		}
		for _, item := range page.GetCheckoutSessions() {
			if _, err := acting.EscalateCart(ctx, &cartv1.EscalateCartRequest{
				Context: rc,
				Cart: &cartv1.Cart{
					DueAt: item.GetDueAt(),
					Notes: item.GetNotes(),
					Audit: &commonv1.AuditInfo{UpdatedAt: item.GetAudit().GetUpdatedAt()},
				},
			}); err != nil {
				slog.WarnContext(ctx, "escalate cart failed", "error", err)
			}
		}
		token = page.GetPageInfo().GetNextPageToken()
		if token == "" {
			return nil
		}
	}
}

// sweepUser3 lists every user of the tenant and runs TerminateCustomer on each.
func sweepUser3(ctx context.Context, conn grpc.ClientConnInterface, rc *commonv1.RequestContext) error {
	lister := identityv1.NewUserServiceClient(conn)
	acting := customersv1.NewCustomerLifecycleServiceClient(conn)
	token := ""
	for {
		page, err := lister.SearchUsers(ctx, &identityv1.SearchUsersRequest{Context: rc, Page: &commonv1.PageRequest{PageSize: 200, PageToken: token}})
		if err != nil {
			return err
		}
		for _, item := range page.GetUsers() {
			if _, err := acting.TerminateCustomer(ctx, &customersv1.TerminateCustomerRequest{
				Context: rc,
				Details: &customersv1.TerminateCustomerRequest_Details{
					NotifyContact: &commonv1.ContactInfo{
						Email:            item.GetContact().GetEmail(),
						Phone:            item.GetContact().GetPhone(),
						PreferredChannel: item.GetContact().GetPreferredChannel(),
					},
				},
			}); err != nil {
				slog.WarnContext(ctx, "terminate customer failed", "error", err)
			}
		}
		token = page.GetPageInfo().GetNextPageToken()
		if token == "" {
			return nil
		}
	}
}
