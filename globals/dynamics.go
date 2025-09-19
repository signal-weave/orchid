package globals

import (
	"os"
)

// -----------------------------------------------------------------------------
// Shared, or "global", dynamic values that are referenced between packages.
// This is not meant to contain constant values.
// -----------------------------------------------------------------------------

// The number of writes that must occur between table flushes.
var FlushThreshold int = 10

// -------Database Page Options-------------------------------------------------

// The size of a page in bytes.
var PageSize = os.Getpagesize()

// The minimum a page must be filled before it is consolidated.
var MinFillPercent float32 = 0.5

// The maximum a page can be filled before it is split.
var MaxFillPercent float32 = 0.95
