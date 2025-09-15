package storage

import (
	"orchiddb/filestamp"
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

func (t *Transaction) Commit() error {
	err := t.writeLog()
	if err != nil {
		return err
	}

	err = t.writeToTable()
	if err != nil {
		return err
	}

	return nil
}

func (t *Transaction) writeLog() error {
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

	logName := filestamp.FileNameMonotonic("db", ".log")
	err := t.wal.WriteLog(logName)
	if err != nil {
		return err
	}

	return nil
}

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
