package tables

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"orchiddb/globals"
)

func GetTableDir(table string) string {
	return filepath.Join(globals.TablesDir, table)
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
	return filepath.Join(GetSSTableDir(table), globals.MANIFEST_FILE)
}

func CreateDirsAndFiles(table string) error {
	// makeDirs must come first.
	if err := makeDirs(table); err != nil {
		return err
	}
	if err := makeDefaultTables(table); err != nil {
		return err
	}
	if err := makeManifestFile(table); err != nil {
		return err
	}

	return nil
}

func makeDirs(table string) error {
	if err := os.MkdirAll(GetFlushDir(table), 0o777); err != nil {
		return fmt.Errorf("error creating flush dir: %w", err)
	}
	if err := os.MkdirAll(GetSSTableDir(table), 0o777); err != nil {
		return fmt.Errorf("error creating sstables dir: %w", err)
	}
	if err := os.MkdirAll(GetWALDir(table), 0o777); err != nil {
		return fmt.Errorf("error creating wal dir: %w", err)
	}

	return nil
}

func makeDefaultTables(table string) error {
	tableDir := GetSSTableDir(table)
	data := map[string]string{}

	for _, v := range defaultTables {
		f := fmt.Sprintf("sstable_%s_1.json", v)
		fp := filepath.Join(tableDir, f)

		_, err := os.Stat(fp)
		if err == nil {
			continue // file already exists
		}

		file, err := os.Create(fp)
		if err != nil {
			file.Close()
			return fmt.Errorf("error creating file: %v", err)
		}

		encoder := json.NewEncoder(file)
		if err := encoder.Encode(data); err != nil {
			file.Close()
			return fmt.Errorf("error encoding initial memtable: %v", err)
		}

		file.Close()
	}

	return nil
}

func makeManifestFile(table string) error {
	manifestFile := GetMainfestFilepath(table)
	_, err := os.Stat(manifestFile)
	if err == nil {
		return nil // file already exists
	}

	file, err := os.Create(manifestFile)
	if err != nil {
		return fmt.Errorf("error creating manifest file: %w", err)
	}

	data := map[string]string{}
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(data); err != nil {
		file.Close()
		return fmt.Errorf("error encoding initial manifest: %v", err)
	}

	return nil
}
