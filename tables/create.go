package tables

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"orchiddb/diskutils"
	"orchiddb/globals"
)

// Makes the starting tables with default value ranges for a new table.
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
	ssTables, err := diskutils.GetDirContents(ssTableDir, false)
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
