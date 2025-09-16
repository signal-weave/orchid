package storage

import (
	"fmt"
	"orchiddb/filestamp"
	"os"
)

// A Transaction is the sum of all pages to update from a user action.
type Transaction struct {
	pager *Pager
	wal   *WAL

	meta       *meta
	freelist   *freelist
	dirtyPages map[pageNum]*Node
}

func NewTransaction(pgr *Pager) *Transaction {
	return &Transaction{
		pager:      pgr,
		wal:        NewWal(),
		dirtyPages: map[pageNum]*Node{},
	}
}

func (t *Transaction) appendPage(n *Node) {
	t.dirtyPages[n.pageNum] = n
}

// Commit makes a WAL file before actually committing the changes to the DB.
// If power loss happened mid WAL creation - transaction is discarded on
// db reboot.
// If power loss happened mid db commit - transaction is replayed via WAL file
// on db reboot.
// Finally WAL file is deleted as to not confuse system on reboot an dconserve
// disk space.
func (t *Transaction) Commit() error {
	logFile := filestamp.FileNameMonotonic("db", ".log")
	if err := t.writeLog(logFile); err != nil {
		return err
	}

	if err := t.writeToTable(); err != nil {
		return err
	}

	if err := os.Remove(logFile); err != nil {
		fmt.Println("[ERROR]", err)
		return err
	}

	return nil
}

// writeLog writes the write-ahead-log for the updated pages in the transaction.
func (t *Transaction) writeLog(path string) error {
	if t.meta != nil {
		mPg := t.meta.serializeToPage()
		t.wal.appendPage(mPg)
	}
	if t.freelist != nil {
		flPg := t.freelist.serializeToPage()
		t.wal.appendPage(flPg)
	}
	if len(t.dirtyPages) > 0 {
		for _, n := range t.dirtyPages {
			nPg := NewEmptyPage(n.pageNum)
			n.serializeToPage(nPg)
			t.wal.appendPage(nPg)
		}
	}

	return t.wal.WriteLog(path)
}

// writeToTable commits the atual updated pages to the .db file.
func (t *Transaction) writeToTable() error {
	for _, p := range t.wal.pages {
		err := t.pager.WritePage(p)
		if err != nil {
			return err
		}
	}

	// reset after dirty pages written
	t.wal.reset()
	t.dirtyPages = map[pageNum]*Node{}
	return nil
}
