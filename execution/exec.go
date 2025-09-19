package execution

import (
	"fmt"
	"orchiddb/parser"
	"orchiddb/paths"
	"orchiddb/storage"
	"os"
	"path/filepath"
	"strings"
)

// Map of table names to their respective worker.
var LoadedWorkers map[string]*TableWorker

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
		tbl := cmd.Command.GetTable()
		worker := LoadedWorkers[tbl]
		worker.in <- cmd
	}
}

// makeTable creates cmd.Table if it does not already exists.
// Will spawn and register a worker for the table.
func makeTable(cmd *parser.MakeCommand) {
	tblName := cmd.Table
	if !strings.HasSuffix(tblName, ".db") {
		tblName = fmt.Sprintf("%s.db", cmd.Table)
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
