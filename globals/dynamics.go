package globals

import (
	"os"
)

// -----------------------------------------------------------------------------
// Shared, or "global", dynamic values that are referenced between packages.
// This is not meant to contain constant values.
// -----------------------------------------------------------------------------

// PerformShutdown denotes if the server should begin the shutdown process.
var PerformShutdown = false

// Address denotes which address the server uses when listening.
var Address = "127.0.0.1"

// Port denotes which port the server uses when listening.
var Port = 6000

// -------Database Page Options-------------------------------------------------

// PageSize denotes the size of a page in bytes.
var PageSize = os.Getpagesize()

// MinFillPercent denotes the minimum percentage a page must be filled before it
// is consolidated.
var MinFillPercent float32 = 0.5

// MaxFillPercent denotes the maximum percentage a page can be filled before it
// is split.
var MaxFillPercent float32 = 0.95
