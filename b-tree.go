package main

import (
	"encoding/binary"
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
	get func() []byte // retrieve page
	new func([]byte) uint64 // new page
	del func(uint64) // dealloc page
}

// helper for retreiving the type of node we are dealing with 
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

// helper for retreiving the # of keys on this node
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

// helper for setting information related to headere explained above
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}