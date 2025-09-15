package storage

// A Transaction is the sum of all pages to update from a user action.
type Transaction struct {
	pager *Pager

	meta       *meta
	freelist   *freelist
	dirtyPages map[pageNum]*Node
}

func NewTransaction(pgr *Pager) *Transaction {
	return &Transaction{
		pager:      pgr,
		dirtyPages: map[pageNum]*Node{},
	}
}

func (t *Transaction) appendPage(n *Node) {
	t.dirtyPages[n.pageNum] = n
}

func (t *Transaction) Commit() error {
	if t.meta != nil {
		mpg := NewEmptyPage(MetaPageNum)
		t.meta.serializeToPage(mpg)
		err := t.pager.WritePage(mpg)
		if err != nil {
			return err
		}
	}

	if t.freelist != nil {
		flPg := NewEmptyPage(FreelistPageNum)
		t.freelist.serializeToPage(flPg)
		err := t.pager.WritePage(flPg)
		if err != nil {
			return err
		}
	}

	if len(t.dirtyPages) > 0 {
		for _, n := range t.dirtyPages {
			npg := NewEmptyPage(n.pageNum)
			n.serializeToPage(npg)
			err := t.pager.WritePage(npg)
			if err != nil {
				return err
			}
		}

		t.dirtyPages = map[pageNum]*Node{} // reset after dirty pages written
	}

	return nil
}
