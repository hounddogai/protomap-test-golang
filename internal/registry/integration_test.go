package registry_test

import (
	"context"
	"testing"
	"time"

	commonpb "example.com/protomap-fixtures/gen/common"
	"example.com/protomap-fixtures/internal/network"
	"example.com/protomap-fixtures/internal/registry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAllServicesCallTheirDeclaredPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const authName = "identity.AuthService"
	var auth network.Service
	var authServices int
	for _, service := range registry.Services {
		if service.Name == authName {
			auth = service
			authServices++
			if len(service.Peers) != 0 {
				t.Fatal("AuthService must have no downstream calls")
			}
			continue
		}
		var authCalls int
		for _, peer := range service.Peers {
			if peer == authName {
				authCalls++
			}
		}
		if authCalls != 1 {
			t.Fatalf("%s declares %d auth calls, expected one", service.Name, authCalls)
		}
	}
	if authServices != 1 {
		t.Fatalf("found %d auth services, expected one", authServices)
	}
	cluster, err := network.Start(ctx, registry.Domains, registry.RegisterDomain)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := cluster.Close(shutdown); err != nil {
			t.Error(err)
		}
	}()
	observed, err := network.Smoke(ctx, cluster.Pool, registry.Services, 1, 16)
	if err != nil {
		t.Fatal(err)
	}
	var edges uint64
	for _, service := range registry.Services {
		edges += uint64(len(service.Peers))
	}
	if observed != edges {
		t.Fatalf("observed %d peer calls, expected %d", observed, edges)
	}
	var authResponse commonpb.Response
	if err := cluster.Pool[auth.Domain].Invoke(ctx, auth.Method,
		&commonpb.Request{SubjectId: "auth-subject", Email: "auth@example.test", RemainingHops: network.MaxHops},
		&authResponse); err != nil {
		t.Fatal(err)
	}
	if authResponse.GetSubjectId() != "auth-subject" || authResponse.GetEmail() != "auth@example.test" {
		t.Fatal("AuthService did not preserve the subject and email")
	}
	if visited := authResponse.GetVisitedServices(); len(visited) != 1 || visited[0] != authName {
		t.Fatalf("AuthService made downstream calls: %v", visited)
	}
	var cyclic network.Service
	for _, service := range registry.Services {
		if service.Name == "identity.AccessDecisionCoordination" {
			cyclic = service
		}
	}
	if cyclic.Name == "" {
		t.Fatal("missing cyclic identity/compliance workflow")
	}
	var response commonpb.Response
	if err := cluster.Pool[cyclic.Domain].Invoke(ctx, cyclic.Method,
		&commonpb.Request{RemainingHops: network.MaxHops + 1}, &response); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("service accepted an excessive hop budget: %v", err)
	}
	if err := cluster.Pool[cyclic.Domain].Invoke(ctx, cyclic.Method,
		&commonpb.Request{RemainingHops: network.MaxHops}, &response); err != nil {
		t.Fatal(err)
	}
	var revisits int
	for _, name := range response.GetVisitedServices() {
		if name == cyclic.Name {
			revisits++
		}
	}
	if revisits < 2 {
		t.Fatal("cyclic workflow did not traverse the identity/compliance cycle")
	}
	// Smoke needs the whole catalog to expand expected recursive traces, so exercise all workflows at the hop cap.
	if _, err := network.Smoke(ctx, cluster.Pool, registry.Services, network.MaxHops, 16); err != nil {
		t.Fatal(err)
	}
}
