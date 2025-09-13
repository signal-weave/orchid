package storage

import (
	"bytes"
	"fmt"
	"orchiddb/globals"
	"os"
)

// DB is the databse struct that uses a pager to read/write/create pages and
// orchestrates meta, freelist, and node page tyeps.
// Creating and closing a db opens and closes a stream to the db file.
type DB struct {
	options Options

	pager    *Pager
	Meta     *meta
	freelist *freelist
}

// -------DB File Management----------------------------------------------------

// Gets the db file from path.
// If it does not exist, a new one is created.
// Returns error, if any.
func GetDB(path string, options *Options) (*DB, error) {
	_, err := os.Stat(path)
	if err == nil {
		return openDB(path, options)
	}

	return createDB(path, options)
}

// createDB creates a new db file with: page 0 = meta; page 1 = freelist.
func createDB(path string, options *Options) (*DB, error) {
	pager, err := OpenPager(path)
	if err != nil {
		return nil, err
	}

	// ---- write meta page table of contents
	m := newMeta()
	fr := newFreelist()
	fr.MaxPage = m.RootPage

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

	db := DB{options: *options, pager: pager, Meta: m, freelist: fr}

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
func openDB(path string, options *Options) (*DB, error) {
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

	return &DB{options: *options, pager: pager, Meta: m, freelist: fr}, nil
}

func (db *DB) Close() error {
	return db.pager.Close()
}

// -------Page Management-------------------------------------------------------

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

// -------Node IO---------------------------------------------------------------

func (db *DB) NewNode(items []*Item, childNodes []pageNum) *Node {
	node := NewEmptyNode()
	node.Items = items
	node.ChildNodes = childNodes
	node.PageNum = db.freelist.GetNextPage()
	node.db = db

	return node
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

// getNodes returns a list of nodes based on their indexes (the breadcrumbs)
// from the root.
//
//	         p
//	     /       \
//	   a          b
//	/     \     /   \
//
// c       d   e     f
//
// For [0,1,0] -> p,b,e
func (db *DB) GetNodes(indexes []int) ([]*Node, error) {
	root, err := db.GetNode(db.Meta.RootPage)
	if err != nil {
		return nil, err
	}

	nodes := []*Node{root}
	child := root
	for i := 1; i < len(indexes); i++ {
		child, err = db.GetNode(child.ChildNodes[indexes[i]])
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, child)
	}
	return nodes, nil
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

// -------Node Tree Balancing---------------------------------------------------

// getSplitIndex should be called when performing rebalance after an item is
// removed. It checks if a node can spare an element, and if it does then it
// returns the index when there the split should happen.
// Otherwise -1 is returned.
func (db *DB) getSplitIndex(node *Node) int {
	size := 0
	size += globals.NodeHeaderSize

	for i := range node.Items {
		size += node.elementSize(i)

		// If we have a big enough page size (more than minimum), but didn't
		// reach the last item, we can spare an element.
		if float32(size) > db.options.MinThreshold && i < len(node.Items)-1 {
			return i + 1
		}
	}

	return -1
}

// Find returns an item according to the given key by performing binary search.
func (db *DB) Find(key []byte) (*Item, error) {
	n, err := db.GetNode(db.Meta.RootPage)
	if err != nil {
		return nil, err
	}

	index, containingNode, _, err := n.FindKey(key, true)
	if err != nil {
		return nil, err
	}
	if index == -1 {
		return nil, nil
	}

	return containingNode.Items[index], nil
}

// Put adds a key to the tree. It finds the correct node and the insertion index
// and adds the item. When performing the search, the ancestors are returned as
// well. This way we can iterate over them to check which nodes were modified
// and rebalance by splitting them accordingly. If the root has too many items,
// then a new root of a new layer is created and the created nodes from the
// split are added as children.
func (db *DB) Put(key []byte, value []byte) error {
	i := NewItem(key, value)

	// Root node is created with database.
	root, err := db.GetNode(db.Meta.RootPage)

	// Find the path to the node where the insertion should happen
	insertionIdx, nodeToInsertIn, ancestorIdxs, err := root.FindKey(i.Key, false)
	if err != nil {
		return err
	}

	// If key already exists
	exists := insertionIdx < len(nodeToInsertIn.Items) &&
		bytes.Equal(nodeToInsertIn.Items[insertionIdx].Key, key)

	if exists {
		nodeToInsertIn.Items[insertionIdx] = i
	} else {
		nodeToInsertIn.addItem(i, insertionIdx)
	}

	// Persist the modified leaf even if no split occurs
	if err := db.WriteNode(nodeToInsertIn); err != nil {
		return err
	}

	ancestors, err := db.GetNodes(ancestorIdxs)
	if err != nil {
		return err
	}

	// Rebalance the nodes all the way up. Start from on enode before the last
	// and go all the way up, exlcluding root.
	for i := len(ancestors) - 2; i >= 0; i-- {
		pnode := ancestors[i]
		node := ancestors[i+1]
		nodeIndex := ancestorIdxs[i+1]
		if node.isOverPopulated() {
			pnode.split(node, nodeIndex)
		}
	}

	// Handle root
	rootNode := ancestors[0]
	if rootNode.isOverPopulated() {
		newRoot := db.NewNode([]*Item{}, []pageNum{rootNode.PageNum})
		newRoot.split(rootNode, 0)

		// commit newly created root
		err = db.WriteNode(newRoot)
		if err != nil {
			return err
		}

		db.Meta.RootPage = newRoot.PageNum
		mpg := NewEmptyPage(pageNum(0))
		db.Meta.serializeToPage(mpg)
		db.pager.WritePage(mpg)
	}

	return nil
}
