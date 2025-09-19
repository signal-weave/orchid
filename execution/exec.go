package execution

import (
	"fmt"
	"orchiddb/globals"
	"orchiddb/parser"
	"orchiddb/paths"
	"orchiddb/storage"
	"os"
	"path/filepath"
	"strings"
)

// -------Worker Handling-------------------------------------------------------

// Map of table names to their respective worker.
var LoadedWorkers map[string]*TableWorker = map[string]*TableWorker{}

func CloseAllTables() {
    for name, w := range LoadedWorkers {
        w.Stop()
        if err := w.Close(); err != nil {
            fmt.Println("close error for", name, ":", err)
        }
    }
}

func PrintWorkers() {
	fmt.Println("---Current Loaded Tables----------")
	for k := range LoadedWorkers {
		fmt.Println(k)
	}
	fmt.Println("----------------------------------")
}

// ExecuteCommand executes the corresponding function for the given cmd.
// Relies on cmd.Command.Table to get table to make/drop or to get corresponding
// worker to process GET, PUT, DEL, etc.
func ExecuteCommand(cmd *parser.Command) {
	switch t := cmd.Command.(type) {
	case *parser.MakeCommand:
		makeTable(t)
	case *parser.DropCommand:
		dropTable(t)
	default:
		tbl := parser.NormalizeTableKey(cmd.Command.GetTable())
		worker, found := LoadedWorkers[tbl]
		if !found {
			fmt.Println("no worker loaded for table:", tbl, "(did you MAKE(table)?)")
			return
		}
		worker.in <- cmd
	}
}

// -------Commands--------------------------------------------------------------

// makeTable creates cmd.Table if it does not already exists.
// Will spawn and register a worker for the table.
func makeTable(cmd *parser.MakeCommand) {
	tblName := cmd.Table
	if !strings.HasSuffix(tblName, globals.TBL_SUFFIX) {
		tblName = fmt.Sprintf("%s%s", cmd.Table, globals.TBL_SUFFIX)
	}

	tablePath := filepath.Join(paths.DatabasePath, tblName)
	tbl, err := storage.GetTable(tablePath)
	if err != nil {
		msg := fmt.Sprintf("could not make table %s: %s", cmd.Table, err)
		fmt.Println(msg)
		return
	}

	worker := NewWorker(tbl)
	worker.Start()
}

// dropTable stops and unloads the cmd.Table's worker and removes the .db file
// from disk.
func dropTable(cmd *parser.DropCommand) {
	worker, exists := LoadedWorkers[cmd.Table]
	if !exists {
		return
	}
	worker.Stop()

	delete(LoadedWorkers, cmd.Table)
	p, found := paths.GetTablePath(cmd.Table)
	if !found {
		return
	}

	if err := os.Remove(p); err != nil {
		msg := fmt.Sprintf("could not remove %s: %s", cmd.GetTable(), err)
		fmt.Println(msg)
	}
}
