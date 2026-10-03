package fulfillment

import (
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
)

// meteredAccessPolicyValidation records how long each call to acme.authorization.v1.AccessPolicyValidationService
// takes.
type meteredAccessPolicyValidation struct {
	authorizationv1.AccessPolicyValidationServiceClient
	recorder *metrics.Recorder
}

// meteredCustomerCalculation records how long each call to acme.customers.v1.CustomerCalculationService takes.
type meteredCustomerCalculation struct {
	customersv1.CustomerCalculationServiceClient
	recorder *metrics.Recorder
}
