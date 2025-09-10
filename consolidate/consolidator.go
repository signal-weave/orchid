package consolidate

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"orchiddb/globals"
)

// -----------------------------------------------------------------------------
// When the in-memory KVStore flushes its table to disk, the newly written
// redundant table contents must be moved into the official tables.

// Consolidation consists of 5 steps:

// 1. Group flush table entries by target sstable (which sstable they should go
// in). These are gotten from the manifest file.
// 2. Write a group of entries to their corresponding sstable.
// 3. Check if the sstable must be split because it has passed the sstable size
// threshold.
// 4. Split the sstable into two.
// 5. Update the manifest file of the split ranges.
// -----------------------------------------------------------------------------

// -------Chronological Sorting-------------------------------------------------

type fileWithTime struct {
	name string
	t    time.Time
}

// sortFlushTables will sort all the flush tables in the flush directory
// chronologically according to filename timestamp.
func sortFlushTables(tableNames []string) ([]string, error) {
	var parsed []fileWithTime

	for _, f := range tableNames {
		base := strings.TrimSuffix(f, filepath.Ext(f))
		parts := strings.SplitN(base, "-", 2)
		if len(parts) < 2 {
			continue
		}
		ts := parts[1] // timestamp part

		t, err := time.Parse(globals.DATE_TIME_LAYOUT, ts)
		if err != nil {
			return nil, fmt.Errorf("error prasing flush table time: %w", err)
		}
		parsed = append(parsed, fileWithTime{name: f, t: t})
	}

	// Sort by time
	sort.Slice(parsed, func(i, j int) bool {
		return parsed[i].t.Before(parsed[j].t)
	})

	// Convert back to array of string
	result := []string{}
	for _, v := range parsed {
		result = append(result, v.name)
	}

	return result, nil
}

// -------Consolidator and Consolidation Algorithm------------------------------

type Consolidator struct {
	table string
}
