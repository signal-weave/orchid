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

// The directory the .exe file is ran rome.
var ExeDir = GetExecDirectory()

// The directory for holding table data.
var TablesDir = filepath.Join(ExeDir, "orchid_tables")
