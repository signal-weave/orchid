package globals

import (
	"fmt"
	"os"
	"path/filepath"
)

// -----------------------------------------------------------------------------
// Shared, or "global", dynamic values that are referenced between packages.
// This is not meant to contain constant values.
// -----------------------------------------------------------------------------

// The number of writes that must occur between table flushes.
var FlushThreshold int = 10

// -------Directories and Files-------------------------------------------------

// The directory the program is running from.
func GetExecDirectory() string {
	exePath, err := os.Executable()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	exeDir := filepath.Dir(exePath)
	return exeDir
}

// The directory the .exe file is ran from.
// Used as default DatabasePath if one is not provided.
var ExeDir = GetExecDirectory()

// Where the currently loaded database files are located.
var DatabasePath string = ExeDir

// -------Database Page Options-------------------------------------------------

// The size of a page in bytes.
var PageSize = os.Getpagesize()

// The minimum a page must be filled before it is consolidated.
var MinFillPercent float32 = 0.5

// The maximum a page can be filled before it is split.
var MaxFillPercent float32 = 0.95
