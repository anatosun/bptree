// Package bptree implements an on-disk B+ bpt indexing scheme that can store
// key-value pairs and provide fast lookups and range scans. keys can be blobs
// binary data and value is uint64.
package kv

import (
	"os"
)

type BPlusTree struct {
	// bpm      *BufferPoolManager
	order  uint64           // number of entries per leaf
	fanout uint64           // number of children per internal node
	nodes  map[uint64]*node // node cache to avoid IO
	meta   metadata
	root   *node
	bpm    *BufferPoolManager
}

const preaollocation = 1000 * 1000

func New() *BPlusTree {

	clock := NewClockPolicy(BufferPoolCapacity)
	disk := NewDiskManager(0)
	bpm := NewBufferPoolManager(disk, clock)

	bpt := &BPlusTree{}
	bpt.bpm = bpm
	bpt.nodes = make(map[uint64]*node)
	initNodeID, _ := bpt.bpm.GetNewNode()
	initNode, err := bpt.bpm.FetchNode(*initNodeID) //Removes it from clock
	if err != nil {
		panic("Couldn't init B+Tree")
	}
	bpt.root = initNode
	bpt.nodes[bpt.root.id] = bpt.root

	bpt.meta = metadata{
		dirty:    true,
		size:     0,
		root:     1,
		pageSize: uint32(4096),
		keySize:  uint64(8),
	}

	bpt.meta.free = make([]uint64, preaollocation)

	for i := range bpt.meta.free {
		bpt.meta.free[i] = uint64(i + 2)
	}

	bpt.fillDegrees()

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

	if node, at, found, err := bpt.search(bpt.root, key); err != nil {
		return nil, err
	} else if found {
		e, err := node.deleteEntryAt(at)
		if err != nil {
			// attempt to unpin node before returning the error
			bpt.bpm.UnpinNode(NodeID(node.id))
			return nil, err
		}
		bpt.meta.size--
		// unpin previous
		err = bpt.bpm.UnpinNode(NodeID(node.id))
		return &e.value, err
	}

	return nil, &KeyNotFoundError{Value: key}

}

func (bpt *BPlusTree) Search(key Key) (*Value, error) {

	if n, at, found, err := bpt.search(bpt.root, key); err != nil {
		return nil, err
	} else if found {
		// unpin previous before returning value
		err = bpt.bpm.UnpinNode(NodeID(n.id))
		return &n.entries[at].value, err
	}

	return nil, &KeyNotFoundError{Value: key}

}

func (bpt *BPlusTree) Len() int { return int(bpt.meta.size) }

func (bpt *BPlusTree) search(n *node, key Key) (child *node, at int, found bool, err error) {
	at, found = n.search(key)

	if n.isLeaf() {
		return n, at, found, nil
	}

	if found {
		at++
	}
	child, err = bpt.bpm.FetchNode(NodeID(n.children[at])) //TODO: After no longer in use, unpin
	if err != nil {
		return nil, 0, false, err
	}
	// unpin previous before iterating over the next
	err = bpt.bpm.UnpinNode(NodeID(n.id))
	if err != nil {
		return n, at, false, err
	}
	return bpt.search(child, key)
}

func (bpt *BPlusTree) split(p, n, sibling *node, i int) error {
	p.dirty = true
	n.dirty = true
	sibling.dirty = true

	if len(n.children) == 0 {
		bpt.splitLeaf(p, n, sibling, i)
	} else {
		bpt.splitNode(p, n, sibling, i)
	}
	err := bpt.validate([]*node{p, n, sibling})
	if err != nil {
		return err
	}
	return nil
}

func (bpt *BPlusTree) splitNode(left, middle, right *node, i int) error {
	parentKey := middle.entries[bpt.fanout-1]
	right.entries = make([]entry, bpt.fanout-1)
	copy(right.entries, middle.entries[:bpt.fanout])
	middle.entries = middle.entries[bpt.fanout:]
	right.children = make([]uint64, bpt.fanout)
	copy(right.children, middle.children[:bpt.fanout])
	middle.children = middle.children[bpt.fanout:]
	err := left.insertChildAt(i, right)
	if err != nil {
		return err
	}
	err = left.insertEntryAt(i, parentKey)
	if err != nil {
		return err
	}
	return nil
}

func (bpt *BPlusTree) splitLeaf(left, middle, right *node, i int) error {
	right.next = middle.next
	right.prev = middle.id
	middle.next = right.id

	right.entries = make([]entry, bpt.order-1)
	copy(right.entries, middle.entries[bpt.order:])
	middle.entries = middle.entries[:bpt.order]

	err := left.insertChildAt(i+1, right)
	if err != nil {
		return err
	}
	err = left.insertEntryAt(i, right.entries[0])
	if err != nil {
		return err
	}
	return nil

}

func (bpt *BPlusTree) validate(nodes []*node) error {

	for _, n := range nodes {
		if n.isLeaf() {
			if len(n.entries) > ((2 * int(bpt.order)) - 1) {
				err := &OverflowError{Type: "entry", Max: ((2 * int(bpt.order)) - 1), Actual: len(n.entries)}
				return err
			}
		} else {

			if len(n.entries) > ((2 * int(bpt.fanout)) - 1) {
				err := &OverflowError{Type: "entry", Max: ((2 * int(bpt.fanout)) - 1), Actual: len(n.entries)}
				return err
			}
		}
	}

	return nil
}

func (bpt *BPlusTree) fillDegrees() error {

	bpt.fanout = uint64((os.Getpagesize() - nodeHeaderLen() - 4) / (2 * (10 + 2 + 8)))
	bpt.order = uint64(os.Getpagesize() - nodeHeaderLen()/(2*(8+8+8)))

	if bpt.order <= 2 || bpt.fanout <= 2 {
		return &InvalidSizeError{Got: "value lower than two for either fanout or order", Should: "need at least 3"}
	}

	return nil
}
