package inmem

import (
	"encoding/json"
	"fmt"
	"orchiddb/globals"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type KVStore struct {
	mutex     sync.Mutex
	memtable  map[string]string
	threshold int
	fileCount int
}

// NewKVStore creates a new key-value store with the given threshold.
func NewKVStore(threshold int) *KVStore {
	return &KVStore{
		memtable:  make(map[string]string),
		threshold: threshold,
		fileCount: 0,
	}
}

// Set inserts or updates a key in the store.
func (kv *KVStore) Set(key, value string) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	kv.memtable[key] = value

	if len(kv.memtable) >= kv.threshold {
		kv.flushToDisk()
	}
}

// Get retrieves a value from the store.
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	val, ok := kv.memtable[key]
	return val, ok
}

// flushToDisk writes the current memtable to disk and clears it.
func (kv *KVStore) flushToDisk() {
	date := time.Now().Format(globals.DATE_LAYOUT)
	time := time.Now().Format(globals.TIME_LAYOUT)
	filename := fmt.Sprintf("sstable-%s-%s.json", date, time)
	fp := filepath.Join(globals.FlushDir, filename)

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
