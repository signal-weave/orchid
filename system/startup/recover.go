package startup

import (
	"fmt"
	"os"
	"strings"

	"orchiddb/paths"
	"orchiddb/storage"
)

// performRecoveryCheck checks for any table WAL files and runs a recovery
// attempt from them.
func performRecoveryCheck() {
	tableFiles := getTablePaths()
	if tableFiles == nil {
		return
	}

	for _, t := range tableFiles {
		tableName, err := paths.GetStem(t)
		if err != nil {
			continue
		}
		walFile, found := paths.GetTableWAL(tableName)
		if !found {
			continue
		}

		db, err := storage.GetTable(t, storage.NewOptions())
		if err != nil {
			os.Remove(t)
			continue
		}

		err = storage.RecoverFromLog(walFile, db.Txn.Pager)
		if err != nil {
			os.Remove(walFile)
			msg := fmt.Sprintf("WAL invalid, removing WAL file %s\n", walFile)
			fmt.Println(msg)
			db.Close()
			continue
		}

		db.Close()
	}
}

func getTablePaths() []string {
	items, err := paths.GetDirContents(paths.DatabasePath)
	if err != nil {
		return nil
	}

	tables := []string{}

	for _, item := range items {
		if strings.HasSuffix(item, ".db") {
			tables = append(tables, item)
		}
	}

	return tables
}
