package network

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	commonpb "example.com/protomap-fixtures/gen/common"
)

// Smoke verifies each service's complete call trace and synthetic payload over real gRPC connections.
func Smoke(ctx context.Context, pool Pool, services []Service, hops int32, parallel int) (uint64, error) {
	if hops < 0 || hops > MaxHops || parallel < 1 {
		return 0, fmt.Errorf("invalid smoke options: hops=%d, parallel=%d", hops, parallel)
	}
	byName := make(map[string]Service, len(services))
	for _, service := range services {
		byName[service.Name] = service
		if pool[service.Domain] == nil {
			return 0, fmt.Errorf("missing connection for %s", service.Domain)
		}
	}
	var expected func(string, int32) ([]string, error)
	expected = func(name string, depth int32) ([]string, error) {
		service, exists := byName[name]
		if !exists {
			return nil, fmt.Errorf("unknown service %s", name)
		}
		result := []string{name}
		if depth > 0 {
			peers := slices.Clone(service.Peers)
			slices.Sort(peers)
			for _, peer := range peers {
				visits, err := expected(peer, depth-1)
				if err != nil {
					return nil, err
				}
				result = append(result, visits...)
			}
		}
		return result, nil
	}
	jobs := make(chan Service, len(services))
	for _, service := range services {
		jobs <- service
	}
	close(jobs)
	failures := make(chan error, len(services))
	var observed atomic.Uint64
	var wait sync.WaitGroup
	for range min(parallel, len(services)) {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for service := range jobs {
				request := &commonpb.Request{
					TenantId: "fixture-enterprise", SubjectId: service.Name,
					Email: "fixture.customer@example.test", RemainingHops: hops,
				}
				var response commonpb.Response
				callCtx, cancel := context.WithTimeout(ctx, DefaultDeadline)
				err := pool[service.Domain].Invoke(callCtx, service.Method, request, &response)
				cancel()
				if err != nil {
					failures <- fmt.Errorf("%s: %w", service.Name, err)
					continue
				}
				visits, err := expected(service.Name, hops)
				if err != nil {
					failures <- err
					continue
				}
				if !slices.Equal(visits, response.GetVisitedServices()) || response.GetEmail() != request.GetEmail() ||
					response.GetSubjectId() != request.GetSubjectId() {
					failures <- fmt.Errorf("%s returned an incorrect trace or payload", service.Name)
					continue
				}
				observed.Add(uint64(len(visits) - 1))
			}
		}()
	}
	wait.Wait()
	close(failures)
	var all []error
	for err := range failures {
		all = append(all, err)
	}
	return observed.Load(), errors.Join(all...)
}
