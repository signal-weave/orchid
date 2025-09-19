package startup

import (
	"fmt"
	"orchiddb/execution"
	"orchiddb/globals"
	"orchiddb/paths"
	"orchiddb/storage"
	"os"
	"strings"
)

func Startup(argv []string) {
	parseCLI(argv)
	performRecoveryCheck()
	loadWorkers()
}

// Parses and stores the runtime flags in public vars.
func parseCLI(argv []string) {
	if err := parseCLIArgs(argv); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func loadWorkers() {
	tablePaths := getTablePaths()
	if tablePaths == nil {
		return
	}

	for _, p := range tablePaths {
		_, err := os.Stat(p)
		if err != nil {
			continue // Does not exist
		}

		tbl, err := storage.GetTable(p)
		if err != nil {
			continue
		}

		execution.NewWorker(tbl) // Adds self to map
	}
}

// Returns a list of absolute paths to the table .db files.
func getTablePaths() []string {
	items, err := paths.GetDirContents(paths.DatabasePath)
	if err != nil {
		return nil
	}

	tables := []string{}

	for _, item := range items {
		if strings.HasSuffix(item, globals.TBL_SUFFIX) {
			tables = append(tables, item)
		}
	}

	return tables
}
