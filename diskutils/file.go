package diskutils

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func CopyFile(source string, dest string) error {
	sourceFile, err := os.Open(source)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}

	return nil
}

// ImportJson returns a map[string]string or error of the json data in fp.
func ImportJson(fp string) (map[string]string, error) {
	data, err := os.ReadFile(fp)
	if err != nil {
		return nil, err
	}

	var values map[string]string

	if err := json.Unmarshal(data, &values); err != nil {
		return nil, err
	}

	return values, nil
}

// ExportJson exports map[string]string data to filepath fp and returns and
// error if anything went wrong.
func ExportJson(fp string, data map[string]string) error {
	file, err := os.OpenFile(fp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error opening json file: %w", err)
	}

	jsonData, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		return fmt.Errorf("error marshaling json data: %w", err)
	}

	_, err = file.Write(jsonData)
	if err != nil {
		return fmt.Errorf("error exporting json data: %w", err)
	}

	return nil
}
