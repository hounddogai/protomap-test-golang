package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"example.com/protomap-fixtures/internal/network"
	"example.com/protomap-fixtures/internal/registry"
)

type runtimeState struct {
	PID       int               `json:"pid"`
	Services  int               `json:"services"`
	Addresses map[string]string `json:"addresses"`
}

func writeState(path string, state runtimeState) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), "runtime-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := json.NewEncoder(file).Encode(state); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

func serve(ctx context.Context, statePath string) error {
	startupCtx, cancel := context.WithTimeout(ctx, network.DefaultDeadline)
	cluster, err := network.Start(startupCtx, registry.Domains, registry.RegisterDomain)
	cancel()
	if err != nil {
		return err
	}
	defer os.Remove(statePath)
	var runErr error
	if err := writeState(statePath, runtimeState{os.Getpid(), len(registry.Services), cluster.Addresses}); err != nil {
		runErr = err
	} else {
		fmt.Printf("Ready: %d gRPC services on %d localhost domain listeners (PID %d)\n",
			len(registry.Services), len(cluster.Addresses), os.Getpid())
		select {
		case <-ctx.Done():
		case runErr = <-cluster.Errors():
		}
	}
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	return errors.Join(runErr, cluster.Close(shutdownCtx))
}

func smoke(ctx context.Context, statePath string, hops int32, parallel int) error {
	data, err := os.ReadFile(statePath)
	if err != nil {
		return fmt.Errorf("run shell/scripts/dev start first: %w", err)
	}
	var state runtimeState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Services != len(registry.Services) || len(state.Addresses) != len(registry.Domains) {
		return fmt.Errorf("running fixture differs from the catalog; run shell/scripts/dev restart")
	}
	connectCtx, cancel := context.WithTimeout(ctx, network.DefaultDeadline)
	pool, closePool, err := network.Connect(connectCtx, state.Addresses)
	cancel()
	if err != nil {
		return err
	}
	started := time.Now()
	edges, err := network.Smoke(ctx, pool, registry.Services, hops, parallel)
	err = errors.Join(err, closePool())
	if err != nil {
		return err
	}
	fmt.Printf("Smoke passed: %d services, %d peer calls, %d domain listeners, hops=%d, elapsed=%s\n",
		len(registry.Services), edges, len(state.Addresses), hops, time.Since(started).Round(time.Millisecond))
	return nil
}

func run(ctx context.Context) error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: fixture <serve|smoke> [--state=.cache/runtime.json] [--hops=1] [--parallel=16]")
	}
	flags := flag.NewFlagSet(os.Args[1], flag.ContinueOnError)
	state := flags.String("state", ".cache/runtime.json", "Runtime state written by serve and read by smoke")
	hops := flags.Int("hops", 1, "Downstream hop budget for smoke (0-3)")
	parallel := flags.Int("parallel", 16, "Concurrent smoke requests")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	switch os.Args[1] {
	case "serve":
		return serve(ctx, *state)
	case "smoke":
		if *hops < 0 || *hops > int(network.MaxHops) || *parallel < 1 {
			return fmt.Errorf("hops must be 0-3 and parallel must be positive")
		}
		return smoke(ctx, *state, int32(*hops), *parallel)
	default:
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
