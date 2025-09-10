package tables

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"orchiddb/disk"
	"orchiddb/globals"
)

// Creates all necessary directories and populates them with files for a new
// user-made table.
func createDirsAndFiles(table string) error {
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
	if err := populateInitialManifestFile(table); err != nil {
		return err
	}

	return nil
}

func makeDirs(table string) error {
	if err := os.MkdirAll(globals.GetFlushDir(table), 0o777); err != nil {
		return fmt.Errorf("error creating flush dir: %w", err)
	}
	if err := os.MkdirAll(globals.GetSSTableDir(table), 0o777); err != nil {
		return fmt.Errorf("error creating sstables dir: %w", err)
	}
	if err := os.MkdirAll(globals.GetWALDir(table), 0o777); err != nil {
		return fmt.Errorf("error creating wal dir: %w", err)
	}

	return nil
}

func makeDefaultTables(table string) error {
	tableDir := globals.GetSSTableDir(table)

	for _, v := range globals.DefaultTables {
		f := fmt.Sprintf("sstable_%s_1.json", v)
		fp := filepath.Join(tableDir, f)

		_, err := os.Stat(fp)
		if err == nil {
			continue // file already exists
		}

		file, err := os.Create(fp)
		if err != nil {
			return fmt.Errorf("error creating file: %v", err)
		}
		defer file.Close()

		data := map[string]string{}
		startKey := fmt.Sprintf("%sa", v)
		endKey := fmt.Sprintf("%sz", v)
		data[startKey] = ""
		data[endKey] = ""

		jsonData, err := json.MarshalIndent(data, "", "    ")
		if err != nil {
			return fmt.Errorf("error marshalling initilal table data: %w", err)
		}

		_, err = file.Write(jsonData)
		if err != nil {
			return fmt.Errorf("error writing initial table data: %w", err)
		}
	}

	return nil
}

func makeManifestFile(table string) error {
	manifestFile := globals.GetMainfestFilepath(table)
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

func populateInitialManifestFile(table string) error {
	ssTableDir := globals.GetSSTableDir(table)
	ssTables, err := disk.GetDirContents(ssTableDir, false)
	if err != nil {
		return fmt.Errorf("error populating initial manifest file: %w", err)
	}

	data := map[string]map[string]string{}
	for _, f := range ssTables {
		if f == "_manifest.json" {
			continue
		}
		token := strings.Split(f, "_")[1]
		start := fmt.Sprintf("%sa", token)
		end := fmt.Sprintf("%sz", token)
		data[f] = map[string]string{
			"start": start,
			"end":   end,
		}
	}

	jsonData, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		return fmt.Errorf("error marshalling initilal manifest file: %w", err)
	}

	manifestFP := globals.GetMainfestFilepath(table)
	file, err := os.OpenFile(manifestFP, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("error opening initial manifest file: %w", err)
	}
	defer file.Close()

	_, err = file.Write(jsonData)
	if err != nil {
		return fmt.Errorf("error writing initial manifest data: %w", err)
	}

	return nil
}
