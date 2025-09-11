package globals

import (
	"path/filepath"
)

func GetTableDir(table string) string {
	return filepath.Join(TablesDir, table)
}

func GetSSTableDir(table string) string {
	return filepath.Join(GetTableDir(table), "sstables")
}

func GetMainfestFilepath(table string) string {
	return filepath.Join(GetSSTableDir(table), MANIFEST_FILE)
}
