// Package bptree implements an on-disk B+ bpt indexing scheme that can store
// key-value pairs and provide fast lookups and range scans. keys can be blobs
// binary data and value is uint64.
package kv

import (
	"fmt"
)

type BPlusTree struct {
	bpm    *BufferPoolManager
	degree uint8
	nodes  map[uint64]*node // node cache to avoid IO
	meta   metadata         // metadata about bpt structure
	root   *node            // current root node
}

const preaollocation = 1000 * 1000

// const preaollocation = 10

func New(degree uint8) *BPlusTree {

	// Init
	clock := NewClockPolicy(BufferPoolCapacity)
	disk := NewDiskManager(0)
	bpm := NewBufferPoolManager(disk, clock)
	bpt := &BPlusTree{degree: degree}

	//Bind
	bpt.bpm = bpm
	bpt.nodes = make(map[uint64]*node)

	initNodeID, _ := bpt.bpm.GetNewNode(degree)
	initNode, err := bpt.bpm.FetchNode(*initNodeID) //Removes it from clock

	if err != nil {
		panic("Couldn't init B+Tree")
	}

	// bpt.root = newNode(1, degree)
	bpt.root = initNode

	bpt.nodes[bpt.root.id] = bpt.root

	bpt.meta = metadata{
		dirty:    true,
		size:     0,
		root:     uint32(*initNodeID),
		pageSize: uint32(4096),
		keySize:  uint64(64),
	}

	bpt.meta.free = make([]uint64, preaollocation)

	for i := range bpt.meta.free {
		bpt.meta.free[i] = uint64(i + 2)
	}

	//Usually, you would unpin now every fetched node. However, the root should always stay in memory
	// So nothing to do here.

	return bpt
}

func (bpt *BPlusTree) Insert(key Key, value Value) (success bool, err error) {

	e := entry{key: key, value: value}

	if success, err = bpt.insert(e); err != nil {
		return success, err
	}

	bpt.meta.dirty = true //This a global dirty read

	if success {
		bpt.meta.size++
		return success, nil
	}

	return success, nil
}

func (bpt *BPlusTree) Remove(key Key) (value *Value, err error) {

	//TODO/FX: If we want to be consistent with findsequentialfreespace,
	// then this needs to add the removed node back to the list

	if nodeID, at, found, err := bpt.search(bpt.root.getID(), key); err != nil {
		return nil, err
	} else if found {
		node, err := bpt.bpm.FetchNode(*nodeID)

		if err != nil {
			return nil, err
		}

		e, err := node.deleteEntryAt(at)
		bpt.bpm.UnpinNode(*nodeID)

		if err != nil {
			// attempt to unpin node before returning the error
			//bpt.bpm.UnpinNode(NodeID(node.id))
			return nil, err
		}
		bpt.meta.size--
		// unpin previous
		// err = bpt.bpm.UnpinNode(NodeID(node.id))
		return &e.value, err
	}

	return nil, &KeyNotFoundError{Value: key}

}

func (bpt *BPlusTree) Search(key Key) (*Value, error) {

	if nodeID, at, found, err := bpt.search(bpt.root.getID(), key); err != nil {
		return nil, err
	} else if found {
		// unpin previous before returning value
		// err = bpt.bpm.UnpinNode(NodeID(n.id))
		n, err := bpt.bpm.FetchNode(*nodeID)
		if err != nil {
			bpt.bpm.UnpinNode(*nodeID)
			return nil, err
		}
		return &n.entries[at].value, err
	}

	return nil, &KeyNotFoundError{Value: key}

}

func (bpt *BPlusTree) Len() int { return int(bpt.meta.size) }

func (bpt *BPlusTree) search(nodeID NodeID, key Key) (child *NodeID, at int, found bool, err error) {

	node, err := bpt.bpm.FetchNode(nodeID)
	if err != nil {
		return nil, 0, false, err
	}

	at, found = node.search(key)
	if node.isLeaf() {
		bpt.bpm.UnpinNode(nodeID)
		return &nodeID, at, found, nil
	}
	if found {
		at++
	}

	childID := NodeID(node.children[at])

	bpt.bpm.UnpinNode(nodeID) //passed node no longer needed

	return bpt.search(childID, key)
}

func dummyfmt() {
	fmt.Println("x")
}

func (bpt *BPlusTree) splitLeaf(p, n, sibling *node, i int) error {

	// FX
	// Pass nodeID and siblingID and fetch it like this:
	// n, err := bpt.bpm.FetchNode(nodeID)
	// if err != nil {
	// 	bpt.bpm.UnpinNode(nodeID)
	// 	return false, err
	// }

	sibling.next = n.next
	sibling.prev = n.id
	n.next = sibling.id

	sibling.entries = make([]entry, p.degree-1)
	copy(sibling.entries, n.entries[p.degree:])
	n.entries = n.entries[:p.degree]

	err := p.insertChildAt(i+1, sibling.id)

	if err != nil {
		return err
	}
	err = p.insertEntryAt(i, sibling.entries[0])
	if err != nil {
		return err
	}

	return nil

}

// dumb implementation of http://eecs.csuohio.edu/~sschung/cis611/B+Trees.pdf
// Convert node to nodeID, fetch it using bpm
func (bpt *BPlusTree) splitNode(p, n, sibling *node, i int) error {

	// FX
	// Pass nodeID and siblingID and fetch it like this:
	// n, err := bpt.bpm.FetchNode(nodeID)
	// if err != nil {
	// 	bpt.bpm.UnpinNode(nodeID)
	// 	return false, err
	// }

	parentKey := n.entries[p.degree-1]

	sibling.entries = make([]entry, p.degree-1)
	copy(sibling.entries, n.entries[:p.degree])
	n.entries = n.entries[p.degree:]

	sibling.children = make([]uint64, p.degree)
	copy(sibling.children, n.children[:p.degree])
	n.children = n.children[p.degree:]

	err := p.insertChildAt(i, sibling.id)
	if err != nil {
		return err
	}

	err = p.insertEntryAt(i, parentKey)
	if err != nil {
		return err
	}

	return nil

}

// Convert node to nodeID, fetch it using bpm
func (bpt *BPlusTree) split(p, n, sibling *node, i int) error {

	// FX
	// Pass nodeID and siblingID and fetch it like this:
	// n, err := bpt.bpm.FetchNode(nodeID)
	// if err != nil {
	// 	bpt.bpm.UnpinNode(nodeID)
	// 	return false, err
	// }

	p.dirty = true
	n.dirty = true
	sibling.dirty = true

	if n.isLeaf() {
		return bpt.splitLeaf(p, n, sibling, i)
	}

	return bpt.splitNode(p, n, sibling, i)
}
