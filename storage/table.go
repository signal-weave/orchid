package storage

import (
	"bytes"
	"fmt"
	"orchiddb/globals"
	"os"
	"sync"
)

// Table is the databse struct that uses a pager to read/write/create pages and
// orchestrates meta, freelist, and node page tyeps.
// Creating and closing a table opens and closes a stream to the table file.
type Table struct {
	rwMutex sync.RWMutex
	options Options

	meta     *meta
	freelist *freelist

	txn *Transaction
}

// -------Table File Management-------------------------------------------------

// Gets the table file from path.
// If it does not exist, a new one is created.
// Returns error, if any.
func GetTable(path string, options *Options) (*Table, error) {
	_, err := os.Stat(path)
	if err == nil {
		return openTable(path, options)
	}

	return createTable(path, options)
}

// createTable creates a new table file with: page 0 = meta; page 1 = freelist,
// page 2 = initial root node.
func createTable(path string, options *Options) (*Table, error) {
	pager, err := OpenPager(path)
	if err != nil {
		return nil, err
	}

	// ---- write meta page table of contents
	m := newMeta()
	fr := newFreelist()

	// ---- write meta (page 0)
	metaPg := m.serializeToPage()
	if err := pager.WritePage(metaPg); err != nil {
		_ = pager.Close()
		return nil, fmt.Errorf("write meta: %w", err)
	}

	// ---- write freelist (page 1)
	flPg := fr.serializeToPage()

	if err := pager.WritePage(flPg); err != nil {
		_ = pager.Close()
		return nil, fmt.Errorf("write freelist: %w", err)
	}

	if err := pager.Sync(); err != nil {
		_ = pager.Close()
		return nil, err
	}

	tbl := Table{
		rwMutex:  sync.RWMutex{},
		options:  *options,
		meta:     m,
		freelist: fr,
		txn:      NewTransaction(pager),
	}

	root := NewEmptyNode()
	root.pageNum = m.RootPageNum
	fr.MaxPage = m.RootPageNum

	tbl.WriteNode(root)
	tbl.txn.meta = m
	tbl.txn.freelist = fr
	tbl.txn.Commit()

	return &tbl, nil
}

// openTable opens an existing table file, reading page 0 (meta) then the
// following infrastructure pages.
func openTable(path string, options *Options) (*Table, error) {
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
	flPg, err := pager.ReadPage(m.FreelistPageNum)
	if err != nil {
		_ = pager.Close()
		return nil, fmt.Errorf("read freelist: %w", err)
	}

	fr := newFreelist()
	fr.deserializeFromPage(flPg)

	return &Table{
		rwMutex:  sync.RWMutex{},
		options:  *options,
		meta:     m,
		freelist: fr,
		txn:      NewTransaction(pager),
	}, nil
}

func (tbl *Table) Commit() error {
	return tbl.txn.Commit()
}

func (tbl *Table) Close() error {
	return tbl.txn.pager.Close()
}

// -------Page Management-------------------------------------------------------

// AllocatePage returns a fresh page number, writing back freelist state.
func (tbl *Table) AllocatePage() *page {
	pn := tbl.freelist.GetNextPage()

	// Persist freelist after allocation
	tbl.txn.freelist = tbl.freelist

	newPage := NewEmptyPage(pn)
	return newPage
}

// ReleasePage marks a page as free and persists the freelist.
func (tbl *Table) ReleasePage(pn pageNum) {
	tbl.freelist.ReleasePage(pn)
	tbl.WriteFreelist()
}

func (tbl *Table) WriteFreelist() {
	tbl.txn.freelist = tbl.freelist
}

func (tbl *Table) WriteMeta() {
	tbl.txn.meta = tbl.meta
}

// -------Node IO---------------------------------------------------------------

func (tbl *Table) NewNode(items []*Item, childNodes []pageNum) *Node {
	node := NewEmptyNode()
	node.items = items
	node.childNodes = childNodes
	node.pageNum = tbl.freelist.GetNextPage()
	tbl.txn.freelist = tbl.freelist // be sure to stage FL after updating
	node.tbl = tbl

	return node
}

func (tbl *Table) GetNode(pageNum pageNum) (*Node, error) {
	n, exists := tbl.txn.dirtyPages[pageNum]
	if exists {
		// No point reading if node has been updated but yet to be written.
		return n, nil
	}

	pg, err := tbl.txn.pager.ReadPage(pageNum)
	if err != nil {
		return nil, err
	}

	node := NewEmptyNode()
	node.deserializeFromPage(pg)
	node.tbl = tbl

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
func (tbl *Table) GetNodes(indexes []int) ([]*Node, error) {
	root, err := tbl.GetNode(tbl.meta.RootPageNum)
	if err != nil {
		return nil, err
	}

	nodes := []*Node{root}
	child := root
	for i := 1; i < len(indexes); i++ {
		child, err = tbl.GetNode(child.childNodes[indexes[i]])
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, child)
	}
	return nodes, nil
}

// Serializes the given node n into a page, then add to current transaction.
func (tbl *Table) WriteNode(n *Node) {
	var pg *page

	if n.pageNum == 0 {
		// Only allocate when the node doesn't already have an assigned page.
		pg = tbl.AllocatePage()
		n.pageNum = pg.pageNum
	} else {
		pg = NewEmptyPage(n.pageNum)
	}

	tbl.txn.appendPage(n)
}

// Serializes the given nodes into pages and adds them to current transaction.
func (tbl *Table) WriteNodes(nodes ...*Node) {
	for _, n := range nodes {
		tbl.WriteNode(n)
	}
}

// Mark the pageNum as freed, then persist the freelist page to disk.
func (tbl *Table) DeleteNode(pageNum pageNum) {
	tbl.freelist.ReleasePage(pageNum)
	tbl.WriteFreelist()
}

// -------Node Tree Balancing---------------------------------------------------

// getSplitIndex should be called when performing rebalance after an item is
// removed. It checks if a node can spare an element, and if it does then it
// returns the index when there the split should happen.
// Otherwise -1 is returned.
func (tbl *Table) getSplitIndex(node *Node) int {
	size := 0
	size += globals.NodeHeaderSize

	for i := range node.items {
		size += node.elementSize(i)

		// If we have a big enough page size (more than minimum), but didn't
		// reach the last item, we can spare an element.
		if float32(size) > tbl.options.MinThreshold && i < len(node.items)-1 {
			return i + 1
		}
	}

	return -1
}

// -------Value Operators-------------------------------------------------------

// Get returns an item according to the given key by performing binary search.
func (tbl *Table) Get(key []byte) (*Item, error) {
	tbl.rwMutex.RLock()
	defer tbl.rwMutex.RUnlock()

	n, err := tbl.GetNode(tbl.meta.RootPageNum)
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

	return containingNode.items[index], nil
}

// Put adds a key to the tree. It finds the correct node and the insertion index
// and adds the item. When performing the search, the ancestors are returned as
// well. This way we can iterate over them to check which nodes were modified
// and rebalance by splitting them accordingly. If the root has too many items,
// then a new root of a new layer is created and the created nodes from the
// split are added as children.
func (tbl *Table) Put(key []byte, value []byte) error {
	tbl.rwMutex.Lock()
	defer tbl.rwMutex.Unlock()

	i := NewItem(key, value)

	// Root node is created with database.
	root, err := tbl.GetNode(tbl.meta.RootPageNum)

	// Find the path to the node where the insertion should happen
	insertionIdx, nodeToInsertIn, ancestorIdxs, err := root.FindKey(i.Key, false)
	if err != nil {
		return err
	}

	// If key already exists
	exists := insertionIdx < len(nodeToInsertIn.items) &&
		bytes.Equal(nodeToInsertIn.items[insertionIdx].Key, key)

	if exists {
		nodeToInsertIn.items[insertionIdx] = i
	} else {
		nodeToInsertIn.addItem(i, insertionIdx)
	}

	// Persist the modified leaf even if no split occurs
	tbl.WriteNode(nodeToInsertIn)

	ancestors, err := tbl.GetNodes(ancestorIdxs)
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
		newRoot := tbl.NewNode([]*Item{}, []pageNum{rootNode.pageNum})
		newRoot.split(rootNode, 0)

		// commit newly created root
		tbl.WriteNode(newRoot)

		tbl.meta.RootPageNum = newRoot.pageNum
		tbl.WriteMeta()
	}

	return nil
}

// Remove removes a key from the tree.
// It finds the correct node and the index to remove the item from and removes it.
// When performing the search, the ancestors are returned as well.
// This way we can iterate over them to check which nodes were modified and
// rebalance by rotating or merging the unbalanced nodes. Rotation is done first.
// If the siblings don't have enough items, then merging occurs. If the root is
// without items after a split, then the root is removed and the tree is one
// level shorter.
func (tbl *Table) Del(key []byte) error {
	tbl.rwMutex.Lock()
	defer tbl.rwMutex.Unlock()

	// Find the path to the node where the deletion should happen.
	rootNode, err := tbl.GetNode(tbl.meta.RootPageNum)
	if err != nil {
		return err
	}

	removeItemIdx, nodeToRemoveFrom, ancestorsIdxs, err := rootNode.FindKey(key, true)
	if err != nil {
		return err
	}

	if removeItemIdx == -1 {
		return nil
	}

	if nodeToRemoveFrom.isLeaf() {
		nodeToRemoveFrom.removeItemFromLeaf(removeItemIdx)
	} else {
		affectedNodes, err := nodeToRemoveFrom.removeItemFromInternal(removeItemIdx)
		if err != nil {
			return err
		}
		ancestorsIdxs = append(ancestorsIdxs, affectedNodes...)
	}

	ancestors, err := tbl.GetNodes(ancestorsIdxs)
	if err != nil {
		return err
	}

	// Reblance the nodes all teh way up.
	// Start from one node before the last and go all teh way up, excluding root.
	for i := len(ancestors) - 2; i >= 0; i-- {
		pnode := ancestors[i]
		node := ancestors[i+1]
		if node.isUnderPopulated() {
			err = pnode.reblanceRemove(node, ancestorsIdxs[i+1])
			if err != nil {
				return err
			}
		}
	}

	rootNode = ancestors[0]
	// If the root node has no items after rebalancing, there's no need to save
	// it because we ignore it.
	if len(rootNode.items) == 0 && len(rootNode.childNodes) > 0 {
		// Mark new root page in meta page and persist it to disk.
		tbl.meta.RootPageNum = ancestors[1].pageNum
		tbl.WriteMeta()
	}

	return nil
}
