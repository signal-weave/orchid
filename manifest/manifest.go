package manifest

import (
	"encoding/json"
	"os"

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

// Manifest is the datastructure representation of the manifest file.
type Manifest struct {
	// Full file path to this particular manifest file.
	fullpath string

	// { sstable file : key start/end ranges }
	Entries map[string]ManifestEntry
}

// LoadManifest reads a JSON manifest file from disk for table and returns a
// Manifest.
func LoadManifest(table string) (*Manifest, error) {
	path := globals.GetMainfestFilepath(table)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	entries := make(map[string]ManifestEntry)
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}

	return &Manifest{
		fullpath: path,
		Entries:  entries,
	}, nil
}
