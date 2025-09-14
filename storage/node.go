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
	pageNum    pageNum
	items      []*Item
	childNodes []pageNum
	db         *DB
}

func NewEmptyNode() *Node {
	return &Node{}
}

// Is this a node with no children?
func (n *Node) isLeaf() bool {
	return len(n.childNodes) == 0
}

// canSpareAnElement checks if the node size is big enough to populate a page
// after giving away one item.
func (n *Node) canSpareAnElement() bool {
	splitIndex := n.db.getSplitIndex(n)
	if splitIndex == -1 {
		return false
	}
	return true
}

// -------Serialization---------------------------------------------------------

// Converts page struct p into a serialized byte array for psersistent storage.
func (n *Node) serializeToPage(p *page) []byte {
	buf := p.contents
	leftPos := 0
	rightPos := len(buf) - 1

	// Add page header: isLeaf, key-value pairs count

	// isLeaf
	isLeaf := n.isLeaf()
	var bitSetVar uint64
	if isLeaf {
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos += 1

	// key-value pairs count
	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(n.items)))
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

	for i, item := range n.items {
		if !isLeaf {
			childNode := n.childNodes[i]

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
		lastChildNode := n.childNodes[len(n.childNodes)-1]
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
		n.pageNum = p.pageNum
		n.items = nil
		n.childNodes = nil // => leaf
		return
	}

	n.pageNum = p.pageNum
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
			n.childNodes = append(n.childNodes, pageNum(pn))
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
		n.items = append(n.items, NewItem(key, value))
	}

	if isLeaf == 0 { // False
		// Read the last child node
		pageNum := pageNum(binary.LittleEndian.Uint64(buf[leftPos:]))
		n.childNodes = append(n.childNodes, pageNum)
	}
}

// -------Size Calulators-------------------------------------------------------

// elementSize returns the size of a key-value-childNode triplet at a given
// index.
// If the node is a leaf, then the size of a key-value pair is returned.
// It's assumed i <= len(node.items).
func (n *Node) elementSize(i int) int {
	size := len(n.items[i].Key) + len(n.items[i].Value)
	if !n.isLeaf() {
		size += globals.PageNumSize
	}

	return size
}

// nodeSize returns the node's size in bytes.
func (n *Node) nodeSize() int {
	size := 0
	size += globals.NodeHeaderSize

	for i := range n.items {
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
	for i, existingItem := range n.items {
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
	return false, len(n.items)
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

	if node.isLeaf() {
		if exact {
			return -1, nil, nil
		}
		return index, node, nil
	}

	*ancestorsIndexes = append(*ancestorsIndexes, index)
	nextChild, err := node.db.GetNode(node.childNodes[index])
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
	if insertionIndex > len(n.items) {
		insertionIndex = len(n.items)
	}
	n.items = append(n.items, nil)
	copy(n.items[insertionIndex+1:], n.items[insertionIndex:])
	n.items[insertionIndex] = item
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

	middleItem := nodeToSplit.items[splitIndex]
	var newNode *Node

	if nodeToSplit.isLeaf() {
		newNode = n.db.NewNode(nodeToSplit.items[splitIndex+1:], []pageNum{})
		n.db.WriteNode(newNode)
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
	} else {
		newNode = n.db.NewNode(
			nodeToSplit.items[splitIndex+1:],
			nodeToSplit.childNodes[splitIndex+1:],
		)
		n.db.WriteNode(newNode)
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
		nodeToSplit.childNodes = nodeToSplit.childNodes[:splitIndex+1]
	}

	n.addItem(middleItem, nodeToSplitIndex)

	if len(n.childNodes) == nodeToSplitIndex+1 {
		// If middle of list, then move items forward
		n.childNodes = append(n.childNodes, newNode.pageNum)
	} else {
		n.childNodes = append(
			n.childNodes[:nodeToSplitIndex+1],
			n.childNodes[nodeToSplitIndex:]...,
		)
		n.childNodes[nodeToSplitIndex+1] = newNode.pageNum
	}

	n.db.WriteNodes(n, nodeToSplit)
}

// removeItemFromLeaf removes an item from a leaf node. It means there is no
// handling of child nodes.
func (n *Node) removeItemFromLeaf(index int) {
	n.items = append(n.items[:index], n.items[index+1:]...)
	n.db.WriteNode(n)
}

func (n *Node) removeItemFromInternal(index int) ([]int, error) {
	// Take element before inorder (The biggest element from the left branch),
	// put it in the removed index and remove it from the original node. Track
	// in affectedNodes any nodes in the path leading to that node. It will be
	// used in case the tree needs to be rebalanced.
	//          p
	//       /
	//     ..
	//  /     \
	// ..      a
	affectedNodes := make([]int, 0)
	affectedNodes = append(affectedNodes, index)

	// Starting from its left child, descend to the rightmost descendant.
	aNode, err := n.db.GetNode(n.childNodes[index])
	if err != nil {
		return nil, err
	}

	for !aNode.isLeaf() {
		// traversingIndex := len(n.childNodes) - 1
		traversingIndex := len(aNode.childNodes) - 1
		aNode, err = aNode.db.GetNode(aNode.childNodes[traversingIndex])
		if err != nil {
			return nil, err
		}
		affectedNodes = append(affectedNodes, traversingIndex)
	}

	// Replace the item that should be removed with the item before inorder
	// which we just found.
	n.items[index] = aNode.items[len(aNode.items)-1]
	aNode.items = aNode.items[:len(aNode.items)-1]
	n.db.WriteNodes(n, aNode)

	return affectedNodes, nil
}

func (n *Node) merge(bNode *Node, bNodeIndex int) error {
	// 	               p                                     p
	//                    3,5                                    5
	//	      /        |       \       ------>         /          \
	//      a          b        c                     a            c
	//     1,2         4        6,7                 1,2,3,4         6,7
	aNode, err := n.db.GetNode(n.childNodes[bNodeIndex-1])
	if err != nil {
		return err
	}

	// Take the item from the parent, remove it and add it to the unbalanced
	// node.
	pNodeItem := n.items[bNodeIndex-1]
	n.items = append(n.items[:bNodeIndex-1], n.items[bNodeIndex:]...)
	aNode.items = append(aNode.items, pNodeItem)

	aNode.items = append(aNode.items, bNode.items...)
	n.childNodes = append(n.childNodes[:bNodeIndex], n.childNodes[bNodeIndex+1:]...)
	if !aNode.isLeaf() {
		aNode.childNodes = append(aNode.childNodes, bNode.childNodes...)
	}

	err = n.db.WriteNodes(aNode, n)
	if err != nil {
		return err
	}

	err = n.db.DeleteNode(bNode.pageNum)
	if err != nil {
		return err
	}

	return nil
}

// rebalanceRemove rebalances the tree after a remove operation.
// This can be either by rotating to the right, to the left or by merging.
// First, the sibling nodes are checked to see if they have enough items for
// rebalancing (>= minItems+1).
// If they don't have enough items, then merging with one of the sibling nodes
// occurs.
// This may leave the parent unbalanced by having too little items so
// rebalancing has to be checked for all the ancestors.
func (n *Node) reblanceRemove(unbalancedNode *Node, unbalanacedNodeIndex int) error {
	pNode := n

	// Right rotate
	if unbalanacedNodeIndex != 0 {
		leftNode, err := n.db.GetNode(pNode.childNodes[unbalanacedNodeIndex-1])
		if err != nil {
			return err
		}
		if leftNode.canSpareAnElement() {
			rotateRight(leftNode, pNode, unbalancedNode, unbalanacedNodeIndex)
			n.db.WriteNodes(leftNode, pNode, unbalancedNode)
			return nil
		}
	}

	// Left rotate
	if unbalanacedNodeIndex != len(pNode.childNodes)-1 {
		rightNode, err := n.db.GetNode(pNode.childNodes[unbalanacedNodeIndex+1])
		if err != nil {
			return err
		}
		if rightNode.canSpareAnElement() {
			rotateLeft(unbalancedNode, pNode, rightNode, unbalanacedNodeIndex)
			n.db.WriteNodes(unbalancedNode, pNode, rightNode)
			return nil
		}
	}
	// The merge function merges a given node with its node to the right. So by
	// default, we merge an unbalanced node with its right sibling. In the case
	// where the unbalanced node is the leftmost, we have to replace the merge
	// parameters, so the unbalanced node right sibling, will be merged into the
	// unbalanced node.
	if unbalanacedNodeIndex == 0 {
		rightNode, err := n.db.GetNode(n.childNodes[unbalanacedNodeIndex+1])
		if err != nil {
			return err
		}

		return pNode.merge(rightNode, unbalanacedNodeIndex+1)
	}

	return pNode.merge(unbalancedNode, unbalanacedNodeIndex)
}

func rotateRight(aNode, pNode, bNode *Node, bNodeIndex int) {
	// 	           p                                    p
	//                 4                                    3
	//	      /        \           ------>         /          \
	//	   a           b (unbalanced)            a        b (unbalanced)
	//      1,2,3             5                     1,2            4,5

	// Get last item and remove it.
	aNodeItem := aNode.items[len(aNode.items)-1]
	aNode.items = aNode.items[:len(aNode.items)-1]

	// Get item from parent node and assign the aNodeItem item instead
	pNodeItemIndex := bNodeIndex - 1
	if bNodeIndex == 0 {
		pNodeItemIndex = 0
	}
	pNodeItem := pNode.items[pNodeItemIndex]
	pNode.items[pNodeItemIndex] = aNodeItem

	// Assign parent item to b and make it first.
	bNode.items = append([]*Item{pNodeItem}, bNode.items...)

	// If it's an inner leaf, then move children as well.
	if !aNode.isLeaf() {
		childNodeToShift := aNode.childNodes[len(aNode.childNodes)-1]
		aNode.childNodes = aNode.childNodes[:len(aNode.childNodes)-1]
		bNode.childNodes = append([]pageNum{childNodeToShift}, bNode.childNodes...)
	}
}

func rotateLeft(aNode, pNode, bNode *Node, bNodeIndex int) {
	// 	           p                                     p
	//                 2                                     3
	//	      /        \           ------>         /          \
	//  a(unbalanced)       b                 a(unbalanced)        b
	//   1                3,4,5                   1,2             4,5

	// Get first item and remove it
	bNodeItem := bNode.items[0]
	bNode.items = bNode.items[1:]

	// Get item from parent node and assign the bNodeItem item instead
	pNodeItemIndex := bNodeIndex
	if bNodeIndex == len(pNode.items) {
		pNodeItemIndex = len(pNode.items) - 1
	}
	pNodeItem := pNode.items[pNodeItemIndex]
	pNode.items[pNodeItemIndex] = bNodeItem

	// Assign parent item to a and make it last
	aNode.items = append(aNode.items, pNodeItem)

	// If it's an inner leaf then move children as well.
	if !bNode.isLeaf() {
		childNodeToShift := bNode.childNodes[0]
		bNode.childNodes = bNode.childNodes[1:]
		aNode.childNodes = append(aNode.childNodes, childNodeToShift)
	}
}
