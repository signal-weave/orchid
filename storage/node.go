package storage

import (
	"bytes"
	"encoding/binary"

	"orchiddb/globals"
)

// Items are the actual user inserted data and live in "nodes".
// Nodes can contain items, and additionally can contain pointers to other nodes
// that also contain items.
// The Item is the actual data, but it will always exist in a node.
// To find items deeper in the structure, the database traverses the housing
// nodes.
type Item struct {
	Key   []byte
	Value []byte
}

func NewItem(key []byte, value []byte) *Item {
	return &Item{
		Key:   key,
		Value: value,
	}
}

// Pages can consist of any data, including nodes while nodes are the struct
// that wraps key-value data in a page and has directions to the next node in
// the path to the queried data.
//
// The actual user inserted data exists in the node's items.
//
// Nodes can contain child nodes, or pointers to nodes in other regions of the
// page. We traverse these nodes in B-Tree fashion.
type Node struct {
	PageNum    pageNum
	Items      []*Item
	ChildNodes []pageNum
	db         *DB
}

func NewEmptyNode() *Node {
	return &Node{}
}

// Is this a node with no children?
func (n *Node) IsLeaf() bool {
	return len(n.ChildNodes) == 0
}

// -------Serialization---------------------------------------------------------

// Converts page struct p into a serialized byte array for psersistent storage.
func (n *Node) serializeToPage(p *page) []byte {
	buf := p.contents
	leftPos := 0
	rightPos := len(buf) - 1

	// Add page header: isLeaf, key-value pairs count

	// isLeaf
	isLeaf := n.IsLeaf()
	var bitSetVar uint64
	if isLeaf {
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos += 1

	// key-value pairs count
	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(n.Items)))
	leftPos += 2

	// We use slotted pages for storing data in the page. This means the actual
	// keys and values (the cells) are appended to right of the page whereas
	// offsets have a fixed size and are appended from the left.
	// It's easier to preserve the logical order (alphabetical in the case of
	// b-tree) using the metadata and performing pointer arithmetic. Using the
	// data itself is harder as it varies by size.

	// Page structure is:
	// -------------------------------------------------------------------------
	// |  Page  | key-value /  child node    key-value 		|  key-value       |
	// | Header |   offset /	 pointer	  offset   .... |    data    ..... |
	// -------------------------------------------------------------------------

	for i, item := range n.Items {
		if !isLeaf {
			childNode := n.ChildNodes[i]

			// Write the child page as a fixed size of 8 bytes
			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childNode))
			leftPos += globals.PageNumSize
		}

		klen := len(item.Key)
		vlen := len(item.Value)

		// -------write offset--------------------------------------------------

		offset := rightPos - klen - vlen - 2
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(offset))
		leftPos += 2

		// Starting from the right postion, we will move backwards the length of
		// the value, then write the value from that position forwards into the
		// buffer.
		rightPos -= vlen
		copy(buf[rightPos:], item.Value)

		// Then move the right position backwards 1 byte to insert the length of
		// the value.
		rightPos -= 1
		buf[rightPos] = byte(vlen)

		// Then we will move the right position backwards the length of the key,
		// then write the key from that position forwards into the buffer.
		rightPos -= klen
		copy(buf[rightPos:], item.Key)

		// Then move the right position backwards 1 byte to insert the length of
		// the key.
		rightPos -= 1
		buf[rightPos] = byte(klen)
	}

	if !isLeaf {
		// Write the last child node
		lastChildNode := n.ChildNodes[len(n.ChildNodes)-1]
		// Write the child page as a fixed size of 8 bytes
		binary.LittleEndian.PutUint64(buf[leftPos:], uint64(lastChildNode))
	}

	return buf
}

// Converts a page struct into a Node struct.
func (n *Node) deserializeFromPage(p *page) {
	// A zeroed page would read as non-leaf and would invent a child pointer to
	// page 0.
	// Pages with all zeroes are treated as a leaf with zero items.
	if bytes.Equal(p.contents, make([]byte, len(p.contents))) {
		n.PageNum = p.pageNum
		n.Items = nil
		n.ChildNodes = nil // => leaf
		return
	}

	n.PageNum = p.pageNum
	buf := p.contents

	leftPos := 0

	// Read header
	isLeaf := uint16(buf[0])

	itemsCount := int(binary.LittleEndian.Uint16(buf[1:3]))
	leftPos += 3

	// Read body
	for range itemsCount {
		if isLeaf == 0 { // False
			pn := binary.LittleEndian.Uint64(buf[leftPos:])
			leftPos += globals.PageNumSize
			n.ChildNodes = append(n.ChildNodes, pageNum(pn))
		}

		// Read offset
		offset := binary.LittleEndian.Uint16(buf[leftPos:])
		leftPos += 2

		klen := uint16(buf[int(offset)])
		offset += 1

		key := buf[offset : offset+klen]
		offset += klen

		vlen := uint16(buf[int(offset)])
		offset += 1

		value := buf[offset : offset+vlen]
		offset += vlen
		n.Items = append(n.Items, NewItem(key, value))
	}

	if isLeaf == 0 { // False
		// Read the last child node
		pageNum := pageNum(binary.LittleEndian.Uint64(buf[leftPos:]))
		n.ChildNodes = append(n.ChildNodes, pageNum)
	}
}

// -------Size Calulators-------------------------------------------------------

// elementSize returns the size of a key-value-childNode triplet at a given
// index.
// If the node is a leaf, then the size of a key-value pair is returned.
// It's assumed i <= len(node.items).
func (n *Node) elementSize(i int) int {
	size := 0
	size += len(n.Items[i].Key)
	size += len(n.Items[i].Value)
	size += globals.PageNumSize

	return size
}

// nodeSize returns the node's size in bytes.
func (n *Node) nodeSize() int {
	size := 0
	size += globals.NodeHeaderSize

	for i := range n.Items {
		size += n.elementSize(i)
	}

	size += globals.PageNumSize
	return size
}

// -------Tree Traversal--------------------------------------------------------

// findKeyInNode iterates all the items and finds the key.
// If the key is found, then the item is returned.
// If the key isn't found then return the index where it should have been
// (the first index that key is greater than it's previous).
func (n *Node) findKeyInNode(key []byte) (bool, int) {
	for i, existingItem := range n.Items {
		result := bytes.Compare(existingItem.Key, key)
		if result == 0 { // Keys match
			return true, i
		}

		// The key is bigger than the previous key, so it doens't exist in the
		// node, but may exist in child nodes.
		if result == 1 {
			return false, i
		}
	}

	// The key isn't bigger than any of the keys which means its in the last idx
	return false, len(n.Items)
}

func (n *Node) FindKey(key []byte, exact bool) (int, *Node, []int, error) {
	ancestorsIndexes := []int{0} // index of root
	index, node, err := findKeyHelper(n, key, exact, &ancestorsIndexes)
	if err != nil {
		return -1, nil, nil, err
	}
	return index, node, ancestorsIndexes, nil
}

func findKeyHelper(
	node *Node, key []byte, exact bool, ancestorsIndexes *[]int,
) (int, *Node, error) {
	wasFound, index := node.findKeyInNode(key)
	if wasFound {
		return index, node, nil
	}

	if node.IsLeaf() {
		if exact {
			return -1, nil, nil
		}
		return index, node, nil
	}

	*ancestorsIndexes = append(*ancestorsIndexes, index)
	nextChild, err := node.db.GetNode(node.ChildNodes[index])
	if err != nil {
		return -1, nil, err
	}
	return findKeyHelper(nextChild, key, exact, ancestorsIndexes)
}

// -------Tree Balancing--------------------------------------------------------

func (n *Node) addItem(item *Item, insertionIndex int) {
	if insertionIndex < 0 {
		insertionIndex = 0
	}
	if insertionIndex > len(n.Items) {
		insertionIndex = len(n.Items)
	}
	n.Items = append(n.Items, nil)
	copy(n.Items[insertionIndex+1:], n.Items[insertionIndex:])
	n.Items[insertionIndex] = item
}

// Does the node require splitting.
func (n *Node) isOverPopulated() bool {
	return float32(n.nodeSize()) > n.db.options.MaxThreshold
}

// Does the node require consolidating.
func (n *Node) isUnderPopulated() bool {
	return float32(n.nodeSize()) < n.db.options.MinThreshold
}

// split rebalances the tree after adding. After insertion the modified node has
// to be checked to make sure it didn't exceed the maximum number of elements.
// If it did, then it has to be split and rebalanced. The transformation is
// depicted in the graph below. If it's not a leaf node, then the children has
// to be moved as well as shown.
// This may leave the parent unbalanced by having too many items so rebalancing
// has to be checked for all the ancestors.
// The split is performed in a for loop to support splitting a node more than
// once. (Though in practice used only once).
//
//		           n                                        n
//	                3                                       3,6
//		      /        \           ------>       /          |          \
//		   a           modifiedNode            a       modifiedNode     newNode
//	  1,2                 4,5,6,7,8            1,2          4,5         7,8
func (n *Node) split(nodeToSplit *Node, nodeToSplitIndex int) {
	// The first index wehree min amount of bytes to populate a page is
	// achieved. Then add 1 so it will be split one index after.
	splitIndex := nodeToSplit.db.getSplitIndex(nodeToSplit)

	middleItem := nodeToSplit.Items[splitIndex]
	var newNode *Node

	if nodeToSplit.IsLeaf() {
		newNode = n.db.NewNode(nodeToSplit.Items[splitIndex+1:], []pageNum{})
		n.db.WriteNode(newNode)
		nodeToSplit.Items = nodeToSplit.Items[:splitIndex]
	} else {
		newNode = n.db.NewNode(
			nodeToSplit.Items[splitIndex+1:],
			nodeToSplit.ChildNodes[splitIndex+1:],
		)
		n.db.WriteNode(newNode)
		nodeToSplit.Items = nodeToSplit.Items[:splitIndex]
		nodeToSplit.ChildNodes = nodeToSplit.ChildNodes[:splitIndex+1]
	}

	n.addItem(middleItem, nodeToSplitIndex)

	if len(n.ChildNodes) == nodeToSplitIndex+1 {
		// If middle of list, then move items forward
		n.ChildNodes = append(n.ChildNodes, newNode.PageNum)
	} else {
		n.ChildNodes = append(
			n.ChildNodes[:nodeToSplitIndex+1],
			n.ChildNodes[nodeToSplitIndex:]...,
		)
		n.ChildNodes[nodeToSplitIndex+1] = newNode.PageNum
	}

	n.db.WriteNodes(n, nodeToSplit)
}
