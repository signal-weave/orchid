package storage

import (
	"sync"
)

// -----------------------------------------------------------------------------
// Locks are the primary interface to a table. They manage the read/write
// locking of the database and process transactions.

// A table can have an arbitrary number of readers or exactly 1 writer at a
// time, but never both.
// -----------------------------------------------------------------------------

// The lock manager (per petrov's 4 layer cake).
type Lock struct {
	rwlock sync.RWMutex // Allows only one writer at a time
	tbl *Table
}

func newLock(tbl *Table) *Lock {
	return &Lock{
		sync.RWMutex{},
		tbl,
	}
}

func Open(path string) (*Lock, error) {
	options := NewOptions()
	tbl, err := openTable(path, options)
	if err != nil {
		return nil, err
	}

	lock := newLock(tbl)

	return lock, nil
}
