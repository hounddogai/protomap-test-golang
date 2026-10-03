// Command monolith serves every gRPC service of the Go code base on one port.
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/cart"
	"github.com/hounddogai/protomap-test-golang/internal/catalog"
	"github.com/hounddogai/protomap-test-golang/internal/checkout"
	"github.com/hounddogai/protomap-test-golang/internal/customers"
	"github.com/hounddogai/protomap-test-golang/internal/fraud"
	"github.com/hounddogai/protomap-test-golang/internal/fulfillment"
	"github.com/hounddogai/protomap-test-golang/internal/giftcards"
	"github.com/hounddogai/protomap-test-golang/internal/identity"
	"github.com/hounddogai/protomap-test-golang/internal/inventory"
	"github.com/hounddogai/protomap-test-golang/internal/loyalty"
	"github.com/hounddogai/protomap-test-golang/internal/merchandising"
	"github.com/hounddogai/protomap-test-golang/internal/orders"
	"github.com/hounddogai/protomap-test-golang/internal/partnerapi"
	"github.com/hounddogai/protomap-test-golang/internal/payments"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/interceptors"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/sqlx"
	"github.com/hounddogai/protomap-test-golang/internal/pricing"
	"github.com/hounddogai/protomap-test-golang/internal/promotions"
	"github.com/hounddogai/protomap-test-golang/internal/returns"
	"github.com/hounddogai/protomap-test-golang/internal/reviews"
	"github.com/hounddogai/protomap-test-golang/internal/search"
	"github.com/hounddogai/protomap-test-golang/internal/sellers"
	"github.com/hounddogai/protomap-test-golang/internal/shipping"
	"github.com/hounddogai/protomap-test-golang/internal/storefront"
	"github.com/hounddogai/protomap-test-golang/internal/subscriptions"
	"github.com/hounddogai/protomap-test-golang/internal/warehousing"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	listen := flag.String("listen", ":8443", "the address to serve gRPC on")
	mesh := flag.String("mesh", "localhost:15001", "the sidecar that reaches every service")
	sandbox := flag.Bool("sandbox", false, "answer from stubs instead of calling other domains")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// The monolith signs its own calls with the service token its replicas share; one alone makes its own.
	serviceToken := os.Getenv("ACME_SERVICE_TOKEN")
	if serviceToken == "" {
		serviceToken = rand.Text()
	}
	conn, err := grpc.NewClient(*mesh, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(interceptors.Workload(serviceToken)),
		grpc.WithChainStreamInterceptor(interceptors.WorkloadStream(serviceToken)))
	if err != nil {
		logger.Error("dial mesh", "error", err)
		os.Exit(1)
	}
	defer conn.Close()
	db, err := sqlx.Open(ctx, "pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		logger.Error("open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	deps := platform.Deps{
		Conn:    conn,
		DB:      db,
		Logger:  logger,
		Bus:     eventbus.New(),
		Clients: registry.New(),
		Flags:   platform.Flags{"sandbox": *sandbox},
		Region:  os.Getenv("ACME_REGION"),
	}
	sessions := identityv1.NewUserServiceClient(conn)
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(interceptors.Recover(logger), interceptors.Authenticate(sessions, serviceToken)),
		grpc.ChainStreamInterceptor(interceptors.AuthenticateStream(sessions, serviceToken)),
	)
	storefront.Register(server, deps)
	partnerapi.Register(server, deps)
	identity.Register(server, deps)
	search.Register(server, deps)
	customers.Register(server, deps)
	catalog.Register(server, deps)
	pricing.Register(server, deps)
	promotions.Register(server, deps)
	cart.Register(server, deps)
	checkout.Register(server, deps)
	orders.Register(server, deps)
	payments.Register(server, deps)
	fraud.Register(server, deps)
	inventory.Register(server, deps)
	warehousing.Register(server, deps)
	fulfillment.Register(server, deps)
	shipping.Register(server, deps)
	returns.Register(server, deps)
	subscriptions.Register(server, deps)
	loyalty.Register(server, deps)
	reviews.Register(server, deps)
	merchandising.Register(server, deps)
	sellers.Register(server, deps)
	giftcards.Register(server, deps)
	storefront.StartWorkers(ctx, deps)
	partnerapi.StartWorkers(ctx, deps)
	identity.StartWorkers(ctx, deps)
	customers.StartWorkers(ctx, deps)
	catalog.StartWorkers(ctx, deps)
	pricing.StartWorkers(ctx, deps)
	promotions.StartWorkers(ctx, deps)
	cart.StartWorkers(ctx, deps)
	checkout.StartWorkers(ctx, deps)
	orders.StartWorkers(ctx, deps)
	payments.StartWorkers(ctx, deps)
	fraud.StartWorkers(ctx, deps)
	inventory.StartWorkers(ctx, deps)
	warehousing.StartWorkers(ctx, deps)
	fulfillment.StartWorkers(ctx, deps)
	shipping.StartWorkers(ctx, deps)
	returns.StartWorkers(ctx, deps)
	subscriptions.StartWorkers(ctx, deps)
	loyalty.StartWorkers(ctx, deps)
	reviews.StartWorkers(ctx, deps)
	merchandising.StartWorkers(ctx, deps)
	sellers.StartWorkers(ctx, deps)
	giftcards.StartWorkers(ctx, deps)
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("listen", "error", err)
		os.Exit(1)
	}
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	logger.Info("serving", "address", listener.Addr().String())
	if err := server.Serve(listener); err != nil {
		logger.Error("serve", "error", err)
		os.Exit(1)
	}
}
