package tables

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"orchiddb/globals"
)

// The MemTable is the in-memory table that data is retrieved from first.
// If data cannot be found, the disk tables are queried and the result is loaded
// into the Memtable for caching.
//
// MemTable will periodlically flush its data to a temporary disk table. This
// temp table will have its data moved into the primary tables and the MemTable
// cache will begin to reload.
// This keeps the MemTable loaded with only recently queried data, or 'hot'
// data, and ensures that updates are written to disk for MemTable
// reconstruction in the event of a crash or power loss.
type MemTable struct {
	name      string
	mutex     sync.Mutex
	memtable  map[string]string
	threshold int
	fileCount int
}

// NewMemTable creates a new key-value store with the given threshold and name.
// Will create all sub-directories and default associated files.
// Returns nil if dirs/files could not be created.
func NewMemTable(name string, threshold int) *MemTable {
	if err := globals.CreateDirsAndFiles(name); err != nil {
		fmt.Println("error creating table directories: %w", err)
		return nil
	}

	return &MemTable{
		name:      name,
		memtable:  make(map[string]string),
		threshold: threshold,
		fileCount: 0,
	}
}

// Set inserts or updates a key in the store.
func (kv *MemTable) Set(key, value string) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	kv.memtable[key] = value

	if len(kv.memtable) >= kv.threshold {
		kv.flushToDisk()
	}
}

// Get retrieves a value from the store.
func (kv *MemTable) Get(key string) (string, bool) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	val, exists := kv.memtable[key]
	// TODO: If !exists -> Get from sstables.

	return val, exists
}

// flushToDisk writes the current memtable to disk and clears it.
func (kv *MemTable) flushToDisk() {
	date := time.Now().Format(globals.DATE_LAYOUT)
	time := time.Now().Format(globals.TIME_LAYOUT)
	filename := fmt.Sprintf("sstable-%s-%s.json", date, time)
	fp := filepath.Join(globals.GetFlushDir(kv.name), filename)

	file, err := os.Create(fp)
	if err != nil {
		fmt.Printf("error creating file: %v\n", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(kv.memtable); err != nil {
		fmt.Printf("error encoding memtable: %v\n", err)
		return
	}

	fmt.Printf("Flushed %d records to %s\n", len(kv.memtable), fp)

	// reset memtable
	kv.memtable = make(map[string]string)
	kv.fileCount++
}
