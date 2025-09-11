package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"orchiddb/globals"
)

// -------Example Manifest File-------------------------------------------------

// {
// 	"sstable_a_1.json": {
// 		"start": "aardvark",
// 		"end": "armadillo"
// 	},
// 	"sstable_g_1.json": {
// 		"start": "galaxy",
// 		"end": "ghost"
// 	},
// 	"sstable_g_2.json": {
// 		"start": "giant",
// 		"end": "gust"
// 	},
// 	...
// }

// -------Manifest Loading------------------------------------------------------

// ManifestEntry represents a row in the manifest file that contains the value
// ranges for an sstable.
// The value range of an sstable is the key range that table is responsible for
// holding the data for.
//
// A table could have keys "galaxy" to "ghost" and their corresponding values.
type ManifestEntry struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

func (m *ManifestEntry) StartUpper() string {
	return strings.ToUpper(m.Start)
}

func (m *ManifestEntry) EndUpper() string {
	return strings.ToUpper(m.End)
}

// Encompases returns true if key is within the start/end range, else false.
func (m *ManifestEntry) Encompases(key string) bool {
	s := strings.Compare(key, m.Start) >= 0
	e := strings.Compare(key, m.End) <= 0
	return s && e
}

// Manifest is the datastructure representation of the manifest file.
type Manifest struct {
	// Full file path to this particular manifest file.
	fullpath string

	// { sstable file : key start/end ranges }
	Entries map[string]*ManifestEntry
}

// Creates a new unpopulated manifest
func NewManifest(table string) *Manifest {
	fp := globals.GetMainfestFilepath(table)
	m := &Manifest{
		fullpath: fp,
	}

	return m
}

// Refresh reads a JSON manifest file from disk for table and returns a
// Manifest.
func (m *Manifest) Refresh() error {
	data, err := os.ReadFile(m.fullpath)
	if err != nil {
		return err
	}

	entries := make(map[string]*ManifestEntry)
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}

	m.Entries = entries
	return nil
}

// Export writes out the manifest's current data to its file.
// TODO: Should export to temp file and then replace original in case of power
// loss or unexpected shutdown.
func (m *Manifest) Export() error {
	data := map[string]map[string]string{}

	for k, v := range m.Entries {
		e := map[string]string{
			"start": v.Start,
			"end":   v.End,
		}
		data[k] = e
	}
	jsonData, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		return fmt.Errorf("error exporting manifest: %w", err)
	}

	f, err := os.OpenFile(m.fullpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error opening manifest file: %w", err)
	}

	_, err = f.Write(jsonData)
	if err != nil {
		return fmt.Errorf("error exporting manifest data: %w", err)
	}

	return nil
}

// FindKeyFile returns the filepath to the table file for key.
// Behavior:
//   - If key is within a range [Start..End], that file is returned.
//   - If key falls between ranges, the lower (preceding) file is returned.
//   - If key is before the very first range, the first file is returned.
//   - If key is after the last range, the last file is returned.
//
// Both manifest entries and the key are uppercased because the windows
// operating systme is big fucking dumb and treats ascii values of either casing
// as the same file/directory even though one case compares larger to the other.
func (m *Manifest) FindKeyFile(key string) string {
	if len(m.Entries) == 0 {
		return ""
	}

	type kv struct {
		file  string
		entry ManifestEntry
	}
	items := make([]kv, 0, len(m.Entries))

	for f, e := range m.Entries {
		parentDir := filepath.Dir(m.fullpath)
		fp := filepath.Join(parentDir, f)
		items = append(items, kv{file: fp, entry: *e})
	}

	// Sort by Start (lexicographic).
	sort.Slice(items, func(i, j int) bool {
		return items[i].entry.StartUpper() < items[j].entry.StartUpper()
	})

	// Find the first index whose Start is strictly greater than key.
	pos := sort.Search(len(items), func(i int) bool {
		return items[i].entry.StartUpper() > strings.ToUpper(key)
	})
	idx := pos - 1
	if idx < 0 {
		// key is before the first range.
		return items[0].file
	}

	return items[idx].file
}

// GetEntryFromKey returns the Manifest.ManifestEntry struct whose range
// encompasses the given key.
// Returns nil if it cannot be found.
func (m *Manifest) GetEntryFromKey(key string) *ManifestEntry {
	for _, v := range m.Entries {
		if v.Encompases(key) {
			return v
		}
	}
	return nil
}

func (m *Manifest) UpdateRangeFromKey(key, file string) error {
	entry := m.Entries[filepath.Base(file)]

	s := strings.Compare(key, entry.Start)
	if s == 0 {
		return nil
	}
	if s == 1 {
		entry.Start = key
		if err := m.Export(); err != nil {
			return err
		}
		return nil
	}

	e := strings.Compare(key, entry.End)
	if e == 0 {
		return nil
	}
	if e == -1 {
		entry.End = key
		if err := m.Export(); err != nil {
			return err
		}
	}

	return fmt.Errorf("Could not determine update range operation for %s", key)
}
