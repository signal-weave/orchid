package storage

import (
	"fmt"
	"os"
)

// DB is the databse struct that uses a pager to read/write/create pages and
// orchestrates meta, freelist, and node page tyeps.
// Creating and closing a db opens and closes a stream to the db file.
type DB struct {
	pager    *Pager
	Meta     *meta
	freelist *freelist
}

// Gets the db file from path.
// If it does not exist, a new one is created.
// Returns error, if any.
func GetDB(path string) (*DB, error) {
	_, err := os.Stat(path)
	if err == nil {
		return openDB(path)
	}

	return createDB(path)
}

// createDB creates a new db file with: page 0 = meta; page 1 = freelist.
func createDB(path string) (*DB, error) {
	pager, err := OpenPager(path)
	if err != nil {
		return nil, err
	}

	// ---- write meta page table of contents
	fr := newFreelist()
	m := newMeta()

	// ---- write meta (page 0)
	metaPg := NewEmptyPage(MetaPageNum)
	m.serializeToPage(metaPg)
	if err := pager.WritePage(metaPg); err != nil {
		_ = pager.Close()
		return nil, fmt.Errorf("write meta: %w", err)
	}

	// ---- write freelist (page 1)
	flPg := NewEmptyPage(m.FreelistPage)
	fr.serializeToPage(flPg)

	if err := pager.WritePage(flPg); err != nil {
		_ = pager.Close()
		return nil, fmt.Errorf("write freelist: %w", err)
	}

	if err := pager.Sync(); err != nil {
		_ = pager.Close()
		return nil, err
	}

	db := DB{pager: pager, Meta: m, freelist: fr}

	root := NewEmptyNode()
	root.PageNum = m.RootPage
	if err := db.WriteNode(root); err != nil {
		pager.Close()
		return nil, fmt.Errorf("write root: %w", err)
	}

	return &db, nil
}

// openDB opens an existing db file, reading page 0 (meta) then following
// infrastructure pages.
func openDB(path string) (*DB, error) {
	pager, err := OpenPager(path)
	if err != nil {
		return nil, err
	}

	// ---- read meta (page 0)
	metaPg, err := pager.ReadPage(MetaPageNum)
	if err != nil {
		_ = pager.Close()
		return nil, fmt.Errorf("read meta: %w", err)
	}
	m := newMeta()
	m.deserializeFromPage(metaPg)

	// ---- read freelist (meta.freelistPage)
	flPg, err := pager.ReadPage(m.FreelistPage)
	if err != nil {
		_ = pager.Close()
		return nil, fmt.Errorf("read freelist: %w", err)
	}

	fr := newFreelist()
	fr.deserializeFromPage(flPg)

	return &DB{pager: pager, Meta: m, freelist: fr}, nil
}

func (db *DB) Close() error {
	return db.pager.Close()
}

// AllocatePage returns a fresh page number, writing back freelist state.
func (db *DB) AllocatePage() (*page, error) {
	pn := db.freelist.GetNextPage()

	// Persist freelist after allocation
	flPg := NewEmptyPage(db.Meta.FreelistPage)
	db.freelist.serializeToPage(flPg)

	if err := db.pager.WritePage(flPg); err != nil {
		return nil, err
	}
	if err := db.pager.Sync(); err != nil {
		return nil, err
	}

	newPage := NewEmptyPage(pn)
	return newPage, nil
}

// ReleasePage marks a page as free and persists the freelist.
func (db *DB) ReleasePage(pn pageNum) error {
	db.freelist.ReleasePage(pn)
	flPg := NewEmptyPage(db.Meta.FreelistPage)

	db.freelist.serializeToPage(flPg)
	if err := db.pager.WritePage(flPg); err != nil {
		return err
	}

	return db.pager.Sync()
}

func (db *DB) GetNode(pageNum pageNum) (*Node, error) {
	pg, err := db.pager.ReadPage(pageNum)
	if err != nil {
		return nil, err
	}

	node := NewEmptyNode()
	node.deserializeFromPage(pg)
	node.db = db

	return node, nil
}

// Serializes the given node n into a page, writes it out and syncs.
func (db *DB) WriteNode(n *Node) error {
	var pg *page
	var err error

	if n.PageNum == 0 {
		// Only allocate when the node doesn't already have an assigned page.
		pg, err = db.AllocatePage()
		if err != nil {
			return err
		}
		n.PageNum = pg.pageNum
	} else {
		pg = NewEmptyPage(n.PageNum)
	}

	n.serializeToPage(pg)
	if err := db.pager.WritePage(pg); err != nil {
		return err
	}

	return db.pager.Sync()
}

// Serializes the given nodes into pages and writes them out and syncs.
// Returns error of the first node to unsucessfully write if one is encountered.
func (db *DB) WriteNodes(nodes ...*Node) error {
	for _, n := range nodes {
		err := db.WriteNode(n)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) DeleteNode(pageNum pageNum) {
	db.freelist.ReleasePage(pageNum)
}

// Put inserts a key/value into the root node (no splits yet).
func (db *DB) Put(key, value []byte) error {
	root, err := db.GetNode(db.Meta.RootPage)
	if err != nil {
		return err
	}

	// naive: always append to the root leaf
	item := NewItem(key, value)
	root.Items = append(root.Items, item)

	return db.WriteNode(root)
}
