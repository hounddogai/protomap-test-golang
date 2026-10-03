package partnerapi

import (
	"sync"
)

// recordKey names a record of one tenant in the caches, as tenants may give their records the same ids.
type recordKey struct{ tenant, id string }

// recentPartnerIntegrations holds the partner integrations handlers saw lately, by tenant and id, so others of the
// process can use them without a read.
var recentPartnerIntegrations sync.Map

// recentPartnerOrders holds the partner orders handlers saw lately, by tenant and id, so others of the process can use
// them without a read.
var recentPartnerOrders sync.Map
