package startup

import (
	"fmt"
	"os"

	"orchiddb/execution"
	"orchiddb/paths"
	"orchiddb/storage"
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

// Starts a worker for each table in the database path.
func loadWorkers() {
	tablePaths := paths.GetTablePaths()
	if len(tablePaths) == 0 {
		return
	}

	for _, p := range tablePaths {
		tbl, err := storage.GetTable(p)
		if err != nil {
			continue
		}

		w := execution.NewWorker(tbl) // Adds self to active table map
		w.Start()
	}
}
