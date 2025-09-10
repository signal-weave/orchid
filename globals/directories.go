package globals

import (
	"path/filepath"
)

func GetTableDir(table string) string {
	return filepath.Join(TablesDir, table)
}

func GetFlushDir(table string) string {
	return filepath.Join(GetTableDir(table), "flush")
}

func GetSSTableDir(table string) string {
	return filepath.Join(GetTableDir(table), "sstables")
}

func GetWALDir(table string) string {
	return filepath.Join(GetTableDir(table), "wal")
}

func GetMainfestFilepath(table string) string {
	return filepath.Join(GetSSTableDir(table), MANIFEST_FILE)
}
