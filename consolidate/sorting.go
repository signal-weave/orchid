package consolidate

import (
	"sort"
	"strings"

	"orchiddb/manifest"
)

// BucketKeysByManifest takes a Manifest and a map of key->value (values ignored
// for bucketing) and returns:
//  1. buckets: map[sstableFile][]keys assigned to that file
//  2. updated manifest entries with possibly extended End values.
func BucketKeysByManifest(
	m *manifest.Manifest, kv map[string]string,
) (map[string][]string, map[string]manifest.ManifestEntry) {
	type row struct {
		File  string
		Start string
		End   string
	}

	// Flatten manifest entries to a slice for sorting by Start.
	rows := make([]row, 0, len(m.Entries))
	for file, e := range m.Entries {
		rows = append(rows, row{File: file, Start: e.Start, End: e.End})
	}

	// Sort by Start (lexicographically).
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Start < rows[j].Start
	})

	// Buckets and a working copy of entries so we can extend End.
	buckets := make(map[string][]string, len(rows))
	updated := m.Entries

	// Helper: predecessor search by Start.
	// Returns index of the row with greatest Start <= key, or 0 if key would
	// come before first start value.
	findPredecessor := func(key string) int {
		i := sort.Search(len(rows), func(i int) bool {
			return rows[i].Start > key
		})

		k := i - 1
		if k == -1 {
			// Key comes before first row's start.
			return 0
		}

		return k
	}

	// Assign each key to the predecessor range
	// (grow End if key exceeds current End).
	for k := range kv {
		idx := findPredecessor(k)
		file := rows[idx].File

		// Append key to that file's bucket.
		buckets[file] = append(buckets[file], k)

		// If key is beyond current End, extend it.
		cur := updated[file]
		if cur.End == "" || strings.Compare(k, cur.End) > 0 {
			cur.End = k
			updated[file] = cur
		}
	}

	for file := range buckets {
		sort.Strings(buckets[file])
	}

	return buckets, updated
}
