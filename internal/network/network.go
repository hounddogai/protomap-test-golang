package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	commonpb "example.com/protomap-fixtures/gen/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const MaxHops int32 = 3
const DefaultDeadline = 10 * time.Second

type Pool map[string]grpc.ClientConnInterface

type Service struct {
	Name   string
	Domain string
	Method string
	Peers  []string
}

type Cluster struct {
	Addresses map[string]string
	Pool      Pool
	servers   []*grpc.Server
	listeners []net.Listener
	conns     []*grpc.ClientConn
	failures  chan error
	closeOnce sync.Once
	closeErr  error
}

func ValidateRequest(ctx context.Context, req *commonpb.Request) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if req == nil || req.GetRemainingHops() < 0 || req.GetRemainingHops() > MaxHops {
		return status.Errorf(codes.InvalidArgument, "remaining_hops must be between zero and %d", MaxHops)
	}
	return nil
}

func PeerError(peer string, err error) error {
	return status.Errorf(status.Code(err), "%s: %s", peer, status.Convert(err).Message())
}

func deadlineInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	bounded, cancel := context.WithTimeout(ctx, DefaultDeadline)
	defer cancel()
	return handler(bounded, req)
}

func Connect(ctx context.Context, addresses map[string]string) (Pool, func() error, error) {
	pool := make(Pool, len(addresses))
	conns := make([]*grpc.ClientConn, 0, len(addresses))
	closePool := func() error {
		var failures []error
		for _, conn := range conns {
			if err := conn.Close(); err != nil {
				failures = append(failures, err)
			}
		}
		return errors.Join(failures...)
	}
	for domain, address := range addresses {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, errors.Join(err, closePool())
		}
		conns = append(conns, conn)
		pool[domain] = conn
		conn.Connect()
		for state := conn.GetState(); state != connectivity.Ready; state = conn.GetState() {
			if !conn.WaitForStateChange(ctx, state) {
				return nil, nil, errors.Join(fmt.Errorf("connect to %s: %w", domain, ctx.Err()), closePool())
			}
		}
	}
	return pool, closePool, nil
}

func Start(ctx context.Context, domains []string, register func(string, grpc.ServiceRegistrar, Pool)) (*Cluster, error) {
	cluster := &Cluster{Addresses: make(map[string]string), Pool: make(Pool), failures: make(chan error, len(domains))}
	cleanup := func(err error) (*Cluster, error) {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return nil, errors.Join(err, cluster.Close(cleanupCtx))
	}
	for _, domain := range domains {
		if domain == "" || cluster.Addresses[domain] != "" {
			return cleanup(fmt.Errorf("empty or duplicate domain: %q", domain))
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return cleanup(err)
		}
		cluster.listeners = append(cluster.listeners, listener)
		cluster.Addresses[domain] = listener.Addr().String()
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return cleanup(err)
		}
		cluster.conns = append(cluster.conns, conn)
		cluster.Pool[domain] = conn
	}
	for index, domain := range domains {
		server := grpc.NewServer(grpc.UnaryInterceptor(deadlineInterceptor))
		register(domain, server, cluster.Pool)
		cluster.servers = append(cluster.servers, server)
		listener := cluster.listeners[index]
		go func() {
			if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				cluster.failures <- fmt.Errorf("serve %s: %w", domain, err)
			}
		}()
	}
	for _, conn := range cluster.conns {
		conn.Connect()
		for state := conn.GetState(); state != connectivity.Ready; state = conn.GetState() {
			if !conn.WaitForStateChange(ctx, state) {
				return cleanup(fmt.Errorf("start fixture connections: %w", ctx.Err()))
			}
		}
	}
	return cluster, nil
}

func (c *Cluster) Errors() <-chan error {
	return c.failures
}

func (c *Cluster) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		var wait sync.WaitGroup
		for _, server := range c.servers {
			wait.Add(1)
			go func() {
				defer wait.Done()
				server.GracefulStop()
			}()
		}
		finished := make(chan struct{})
		go func() { wait.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-ctx.Done():
			for _, server := range c.servers {
				server.Stop()
			}
			<-finished
			c.closeErr = ctx.Err()
		}
		for _, conn := range c.conns {
			c.closeErr = errors.Join(c.closeErr, conn.Close())
		}
		for _, listener := range c.listeners {
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				c.closeErr = errors.Join(c.closeErr, err)
			}
		}
	})
	return c.closeErr
}
