// Command acmectl is the operators' command line: it reads and deletes records, and calls any
// method by its path for the cases the subcommands do not cover.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/hounddogai/protomap-test-golang/gen/acme/accounting/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/admissions/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/analytics/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/anomalies/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/appointments/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/assets/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/audiences/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/banking/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/benefits/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/billing/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/boxoffice/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/broadcasts/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/budgeting/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/campaigns/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/certificates/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/chat/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/collections/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/community/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/compliance/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/concessions/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/configuration/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/content/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/contracts/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/corporateevents/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/datahub/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/datasets/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/deliveries/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/devices/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/dispatch/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/driverapp/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/edgegateway/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/employeeportal/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/employees/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/expenses/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/experiments/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/fanportal/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/featurestore/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/financeportal/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/fleet/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/forecasting/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/fuel/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/insurance/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/invoicing/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/itsm/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/keymanagement/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/labeling/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/learning/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/lending/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/lineage/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/maintenance/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/media/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/messaging/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/modelregistry/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/patientapp/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/patients/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/performance/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/performers/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/personalization/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/pipelines/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/prescriptions/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/procurement/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/ratelimiting/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/recommendations/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/recruiting/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/risk/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/routing/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/scheduling/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/seating/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/secrets/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/shows/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/suppliers/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/surveys/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/telehealth/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/ticketing/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/timekeeping/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/tours/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/travel/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/treasury/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/webbff/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/webhooks/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/workplaces/v1"
	_ "github.com/hounddogai/protomap-test-golang/gen/acme/yards/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

func printJSON(message interface{ ProtoReflect() protoreflect.Message }) error {
	text, err := protojson.Marshal(message.ProtoReflect().Interface())
	if err != nil {
		return err
	}
	_, err = fmt.Println(string(text))
	return err
}

// describe finds a unary method by its path, such as /acme.identity.v1.UserService/GetUser, among the
// protobuf files the command links, which are every one the repository has.
func describe(path string) (protoreflect.MethodDescriptor, error) {
	service, name, ok := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	if !ok {
		return nil, fmt.Errorf("method path %q is not /package.Service/Method", path)
	}
	found, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(service))
	if err != nil {
		return nil, fmt.Errorf("service %s: %w", service, err)
	}
	descriptor, ok := found.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s is not a service", service)
	}
	method := descriptor.Methods().ByName(protoreflect.Name(name))
	if method == nil {
		return nil, fmt.Errorf("service %s has no method %s", service, name)
	}
	if method.IsStreamingClient() || method.IsStreamingServer() {
		return nil, fmt.Errorf("%s streams, and invoke calls unary methods only", path)
	}
	return method, nil
}

func run(ctx context.Context, conn *grpc.ClientConn, rc *commonv1.RequestContext) error {
	switch flag.Arg(0) {
	case "delete-seller-scorecard-report":
		reply, err := sellersv1.NewSellerScorecardReportingServiceClient(conn).DeleteSellerScorecardReport(ctx, &sellersv1.DeleteSellerScorecardReportRequest{Context: rc, SellerScorecardId: flag.Arg(1)})
		if err != nil {
			return err
		}
		return printJSON(reply)
	case "get-seller-listing-rates":
		reply, err := sellersv1.NewSellerListingCalculationServiceClient(conn).GetSellerListingRates(ctx, &sellersv1.GetSellerListingRatesRequest{Context: rc, SellerListingId: flag.Arg(1)})
		if err != nil {
			return err
		}
		return printJSON(reply)
	case "peek-stock-item":
		reply, err := inventoryv1.NewStockItemQueryServiceClient(conn).PeekStockItem(ctx, &inventoryv1.PeekStockItemRequest{Context: rc, StockItemId: flag.Arg(1)})
		if err != nil {
			return err
		}
		return printJSON(reply)
	case "get-seller-scorecard-rates":
		reply, err := sellersv1.NewSellerScorecardCalculationServiceClient(conn).GetSellerScorecardRates(ctx, &sellersv1.GetSellerScorecardRatesRequest{Context: rc, SellerScorecardId: flag.Arg(1)})
		if err != nil {
			return err
		}
		return printJSON(reply)
	case "get-gift-card-transaction-lifecycle":
		reply, err := giftcardsv1.NewGiftCardTransactionLifecycleServiceClient(conn).GetGiftCardTransactionLifecycle(ctx, &giftcardsv1.GetGiftCardTransactionLifecycleRequest{Context: rc, GiftCardTransactionId: flag.Arg(1)})
		if err != nil {
			return err
		}
		return printJSON(reply)
	case "delete-payment-instrument-report":
		reply, err := paymentsv1.NewPaymentInstrumentReportingServiceClient(conn).DeletePaymentInstrumentReport(ctx, &paymentsv1.DeletePaymentInstrumentReportRequest{Context: rc, PaymentInstrumentId: flag.Arg(1)})
		if err != nil {
			return err
		}
		return printJSON(reply)
	case "invoke":
		// Calls any unary method by its path, with its request in JSON, for what the subcommands do not cover.
		method, err := describe(flag.Arg(1))
		if err != nil {
			return err
		}
		in := dynamicpb.NewMessage(method.Input())
		if err := protojson.Unmarshal([]byte(flag.Arg(2)), in); err != nil {
			return err
		}
		out := dynamicpb.NewMessage(method.Output())
		if err := conn.Invoke(ctx, flag.Arg(1), in, out); err != nil {
			return err
		}
		return printJSON(out)
	}
	return fmt.Errorf("unknown command %q", flag.Arg(0))
}

func main() {
	mesh := flag.String("mesh", "localhost:15001", "the sidecar that reaches every service")
	tenant := flag.String("tenant", os.Getenv("ACME_TENANT"), "the tenant to act for")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := grpc.NewClient(*mesh, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(grpcx.Bearer{Token: os.Getenv("ACME_TOKEN"), Tenant: *tenant}))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	rc := &commonv1.RequestContext{TenantId: *tenant, Actor: &commonv1.Actor{UserId: os.Getenv("USER")}}
	if err := run(ctx, conn, rc); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
