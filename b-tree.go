package main

import (
	"encoding/binary"
	"bytes"
)

/*
	A node will contain the following information
	1. Header
		- type of node (2B)
		- number of keys (2B)
	2. List of pointers for child nodes (# of keys * 8B)
	3. KV pairs (only used in leaf nodes)
	4. Offsets for KV pairs, used in binary search (# of keys * 2B)
*/

const HEADER = 4
const PAGE_SIZE = 4096 
const MAX_KEY_SIZE = 1000 
const MAX_VAL_SIZE = 3000 

type BNode []byte // type for all information related to nodes in b-tree 

const (
	BNODE_NODE = 1 // internal node w.o values 
	BNODE_LEAF = 2 // leaf node with values
)

type BTree struct { // BTree definition with helper callbacks
	root uint64 
	get func() []byte // retrieve node
	new func([]byte) uint64 // new node
	del func(uint64) // dealloc node
}

// retreiving the type of node we are dealing with 
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

// retreiving the # of keys on this node
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

// setting information related to headere explained above
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// getting a child node at a certain index
func (node BNode) getPtr(index uint16) uint64 {
	pos := HEADER + 8*index // offset the HEADER first 
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(index uint16, val uint64)

// retreiving offset list used for O(1) KV lookup
func offsetPos(node BNode, index uint16) uint16 {
	return HEADER + 8*node.nkeys() + 2*(index-1)
}

// retreiving the offset positioning in binary search
func (node BNode) getOffset(index uint16) uint16 {
	if index == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, index):])
}

func (node BNode) setOffset(index uint16, offset uint16)

// retreiving the actual KV position in the BNode
func (node BNode) kvPos(index uint16) uint16 {
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(index)
}

// retreiving the actual key in the BNode
func (node BNode) getKey(index uint16) []byte {
	pos := node.kvPos(index)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(index uint16) []byte 

// retreiving node size in bytes 
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// returns first child node whose range intersects the desired key <= to the key
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// inserts a key into a leaf node 
func leafInsert(new BNode, old BNode, index uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys() + 1)
	nodeAppendRange(new, old, 0, 0, index)
	nodeAppendKV(new, index, 0, key, val)
	nodeAppendRange(new, old, index+1, index, old.nkeys()-index)
}

// copies KV into the given position
func nodeAppendKV(new BNode, index uint16, ptr uint64, key []byte, val []byte) {
	new.setPtr(index, ptr)
	pos := new.kvPos(index)
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key))) 
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(key)))
	copy(new[pos+4:], key) // copy the key
	copy(new[pos+4+uint16(len(key)):], val) // copy the value
	new.setOffset(index+1, new.getOffset(index)+4+uint16(len(key)+len(val))) // next key
}

// copies multiple KV into the given range
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16)

// replacing tree links with 1 or more links
func nodeReplaceKidN(tree *BTree, new BNode, old BNode, index uint16, kids...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, index)
	for i, node := range kids {
		nodeAppendKV(new, index+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, index+inc, index+1, old.nkeys()-(index+1))
}

func nodeSplit2(left BNode, right BNode, old BNode) {

}

// split node if it becomes to large for a page into 1-3 nodes
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <+ PAGE_SIZE {
		old = old[:PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode(make([]byte, 2*PAGE_SIZE))
	right := BNode(make([]byte, PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() < PAGE_SIZE {
		return 2, [3]BNode{left, right} // only 2 nodes were needed
	}
	leftleft := BNode(make([]byte, PAGE_SIZE))
	mid := BNode(make([]byte, PAGE_SIZE))
	nodeSplit2(leftleft, mid, left)
	return 3, [3]BNode{leftleft, mid, right}
}

// insert a new leaf / internal node into the BTree
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	new := BNode(make([]byte, 2*PAGE_SIZE))
	index := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey()) {
			leafUpdate(new, node, index, key, val) // update the leaf an exisitng key in the leaf
		} else {
			leafInsert(new, node, index + 1, key, val) // insert a new key into the leaf
		}
	case BNODE_NODE:
		nodeInsert(tree, new, node, index, key, val) // insert the internal node
	default:
		panic("bad node!")
	}
	return new
}

// inserting a new internal node 
func nodeInsert(
	tree *BTree, new BNode, node BNode, 
	index uint16, key[] byte, val []byte
) {
	kptr := node.getPtr(index)
	knode := treeInsert(tree, tree.get(kptr), key, val)
	nsplit, split := nodeSplit3(knode)
	tree.del(kptr)
	nodeReplaceKidN(tree, new, node, index, split[:nsplit]...)
}

