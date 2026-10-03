package returns

import (
	"sync"
)

// recordKey names a record of one tenant in the caches, as tenants may give their records the same ids.
type recordKey struct{ tenant, id string }

// recentReturnRequests holds the return requests handlers saw lately, by tenant and id, so others of the process can
// use them without a read.
var recentReturnRequests sync.Map
