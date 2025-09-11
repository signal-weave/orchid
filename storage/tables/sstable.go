package tables

import (
	"fmt"
	"os"

	"orchiddb/diskutils"
	"orchiddb/globals"
	"orchiddb/storage/manifest"
)

// -----------------------------------------------------------------------------
// A String-Sorted-Table, or sstable, is the primary disk table used by Orchid.
// These are the persitent storage data files for the database, hodling the main
// bulk of user generated data.
// -----------------------------------------------------------------------------

type SSTable struct {
	name     string
	manifest *manifest.Manifest
}

func NewSSTable(name string) *SSTable {
	m := manifest.NewManifest(name)

	return &SSTable{
		name:     name,
		manifest: m,
	}
}

func (sst *SSTable) Create() error {
	if err := sst.makeDirs(); err != nil {
		return err
	}
	if err := sst.makeFiles(); err != nil {
		return err
	}

	sst.manifest.Refresh()

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

func (sst *SSTable) Put(key, value string) error {
	keyFile := sst.manifest.FindKeyFile(key)
	data, err := diskutils.ImportJson(keyFile)
	if err != nil {
		return err
	}

	data[key] = value

	if err := diskutils.ExportJson(keyFile, data); err != nil {
		return err
	}

	if err := sst.manifest.UpdateRangeFromKey(key, keyFile); err != nil {
		return err
	}

	return nil
}

func (sst *SSTable) Get(key string) (string, error) {
	keyFile := sst.manifest.FindKeyFile(key)
	data, err := diskutils.ImportJson(keyFile)
	if err != nil {
		return "", err
	}

	value, exists := data[key]
	if !exists {
		return "", fmt.Errorf("key not found: %s", key)
	}

	return value, nil
}

func (sst *SSTable) Del(key string) error {
	keyFile := sst.manifest.FindKeyFile(key)
	data, err := diskutils.ImportJson(keyFile)
	if err != nil {
		return err
	}

	delete(data, key)

	if err := diskutils.ExportJson(keyFile, data); err != nil {
		return err
	}

	if err := sst.manifest.UpdateRangeFromKey(key, keyFile); err != nil {
		return err
	}

	return nil
}
