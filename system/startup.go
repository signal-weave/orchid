package system

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"orchiddb/globals"
	"orchiddb/tables"
)

func Startup() error {
	err := makeDirs()
	if err != nil {
		return err
	}

	err = makeDefaultTables()
	if err != nil {
		return err
	}
	return nil
}

func makeDirs() error {
	if err := os.MkdirAll(globals.FlushDir, 0o777); err != nil {
		return fmt.Errorf("error creating flush dir: %v", err)
	}
	if err := os.MkdirAll(globals.SSTableDir, 0o777); err != nil {
		return fmt.Errorf("error creating sstables dir: %v", err)
	}

	return nil
}

func makeDefaultTables() error {
	data := map[string]string{}

	for _, v := range tables.DefaultTables {
		fmt.Println("value:", v)
		f := fmt.Sprintf("sstable_%s_1.json", v)
		fp := filepath.Join(globals.SSTableDir, f)

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
			return fmt.Errorf("error encoding memtable: %v", err)
		}

		file.Close()
	}

	return nil
}
