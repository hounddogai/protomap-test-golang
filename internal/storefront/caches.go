package storefront

import (
	"sync"
)

// recordKey names a record of one tenant in the caches, as tenants may give their records the same ids.
type recordKey struct{ tenant, id string }

// recentShopperViews holds the shopper views handlers saw lately, by tenant and id, so others of the process can use
// them without a read.
var recentShopperViews sync.Map

// recentStorefrontSessions holds the storefront sessions handlers saw lately, by tenant and id, so others of the
// process can use them without a read.
var recentStorefrontSessions sync.Map
