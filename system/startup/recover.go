package startup

import (
	"fmt"
	"os"

	"orchiddb/paths"
	"orchiddb/storage"
)

// performRecoveryCheck checks for any table WAL files and runs a recovery
// attempt from them.
func performRecoveryCheck() {
	tableFiles := paths.GetTablePaths()
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

		db, err := storage.GetTable(t)
		if err != nil {
			if removeErr := os.Remove(t); removeErr != nil {
				fmt.Printf("Error removing corrupted table %s: %v\n", t, removeErr)
			}
			continue
		}

		// Use anonymous func so defer runs per iteration,
		// not at the function end
		func() {
			defer func() {
				if closeErr := db.Close(); closeErr != nil {
					fmt.Printf("Error closing table %s: %v\n", tableName, closeErr)
				}
			}()

			err = storage.RecoverFromLog(walFile, db.Txn.Pager)
			if err != nil {
				if removeErr := os.Remove(walFile); removeErr != nil {
					fmt.Printf("Error removing WAL file %s: %v\n", walFile, removeErr)
				}
				fmt.Printf("WAL invalid, removed WAL file %s\n", walFile)
				return
			}
		}()
	}
}
