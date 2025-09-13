package storage

import "bytes"

type Collection struct {
	name []byte
	root pageNum

	db *DB
}

func NewCollection(name []byte, root pageNum, db *DB) *Collection {
	return &Collection{
		name: name,
		root: root,
		db: db,
	}
}

// Find returns an item according to the given key by performing binary search.
func (c *Collection) Find(key []byte) (*Item, error) {
	n, err := c.db.GetNode(c.root)
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
func (c *Collection) Put(key []byte, value []byte) error {
	i := NewItem(key, value)

	// On first insertion the root node does note exist, so it should be created
	var root *Node
	var err error
	if c.root == 0 {
		root = c.db.NewNode([]*Item{i}, []pageNum{})
		err = c.db.WriteNode(root)
		if err != nil {
			return nil
		}

		c.root = root.PageNum
		return nil

	} else {
		root, err = c.db.GetNode(c.root)
		if err != nil {
			return err
		}
	}

	// Find the path to the node where the insertion should happen
	insertionIdx, nodeToInsertIn, ancestorIdxs, err := root.FindKey(i.Key, false)
	if err != nil {
		return err
	}

	// If key already exists
	if nodeToInsertIn.Items != nil && bytes.Equal(nodeToInsertIn.Items[insertionIdx].Key, key) {
		nodeToInsertIn.Items[insertionIdx] = i
	} else {
		// Add item to the leaf node
		nodeToInsertIn.addItem(i, insertionIdx)
	}

	ancestors, err := c.getNodes(ancestorIdxs)
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
		newRoot := c.db.NewNode([]*Item{}, []pageNum{rootNode.PageNum})
		newRoot.split(rootNode, 0)

		// commit newly created root
		err = c.db.WriteNode(newRoot)
		if err != nil {
			return err
		}

		c.root = newRoot.PageNum
	}

	return nil
}

// getNodes returns a list of nodes based on their indexes (the breadcrumbs)
// from the root.
//
//           p
//       /       \
//     a          b
//  /     \     /   \
// c       d   e     f
//
// For [0,1,0] -> p,b,e
func (c *Collection) getNodes(indexes []int) ([]*Node, error) {
	root, err := c.db.GetNode(c.root)
	if err != nil {
		return nil, err
	}

	nodes := []*Node{root}
	child := root
	for i := 1; i < len(indexes); i++ {
		child, err = c.db.GetNode(child.ChildNodes[indexes[i]])
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, child)
	}
	return nodes, nil
}
