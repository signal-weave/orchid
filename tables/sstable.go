package tables

import (
	"fmt"
	"os"

	"orchiddb/globals"
)

// -----------------------------------------------------------------------------
// A String-Sorted-Table, or sstable, is the primary disk table used by Orchid.
// These are the persitent storage data files for the database, hodling the main
// bulk of user generated data.
// -----------------------------------------------------------------------------

type SSTable struct {
	name string
}

func NewSSTable(name string) *SSTable {
	return &SSTable{
		name: name,
	}
}

func (sst *SSTable) Create() error {
	if err := sst.makeDirs(); err != nil {
		return err
	}
	if err := sst.makeFiles(); err != nil {
		return err
	}

	return nil
}

func (sst *SSTable) makeDirs() error {
	if err := os.MkdirAll(globals.GetSSTableDir(sst.name), 0o777); err != nil {
		return fmt.Errorf("error creating sstables dir: %w", err)
	}

	return nil
}

func (sst *SSTable) makeFiles() error {
	if err := makeDefaultTables(sst.name); err != nil {
		return err
	}
	if err := makeManifestFile(sst.name); err != nil {
		return err
	}
	if err := populateInitialManifestFile(sst.name); err != nil {
		return err
	}

	return nil
}
