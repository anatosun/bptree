// Package bptree implements an on-disk B+ bpt indexing scheme that can store
// key-value pairs and provide fast lookups and range scans. keys can be blobs
// binary data and value is uint64.
package kv

import (
	"os"
)

type BPlusTree struct {
	// bpm      *BufferPoolManager
	order   uint64           // number of entries per leaf
	fanout  uint64           // number of children per internal node
	nodes   map[uint64]*node // node cache to avoid IO
	meta    metadata
	root    *node
	bpm 	*BufferPoolManager
}

const preaollocation = 1000 * 1000

func New() *BPlusTree {

	clock := NewClockPolicy(BufferPoolCapacity)
	disk := NewDiskManager()
	bpm := NewBufferPoolManager(disk, clock)
	bpt := &BPlusTree{}
	bpt.bpm = bpm
	bpt.nodes = make(map[uint64]*node)

	// bpt.root = newNode(1)
	initNodeID, _ := bpt.bpm.GetNewNode()
	initNode, err := bpt.bpm.FetchNode(*initNodeID) //Removes it from clock
	
	if err != nil {
		panic("Couldn't init B+Tree")
	}

	bpt.root = initNode
	//fmt.Printf("new root=%v\nold root=%v\n", initNode, bpt.root)


	bpt.nodes[bpt.root.id] = bpt.root

	bpt.meta = metadata{
		dirty:    true,
		size:     0,
		root:     1,
		pageSize: uint32(4096),
		keySize:  uint64(64),
	}

	bpt.meta.free = make([]uint64, preaollocation)

	for i := range bpt.meta.free {
		bpt.meta.free[i] = uint64(i + 2)
	}

	bpt.fillDegrees()

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
		node, err := bpt.bpm.FetchNode(nodeID)

		if err != nil {
			bpt.bpm.UnpinNode(nodeID)
			return nil, err
		}

		e, err := node.deleteEntryAt(at)
		bpt.bpm.UnpinNode(nodeID)

		if err != nil {
			// attempt to unpin node before returning the error
			// bpt.bpm.UnpinNode(node.id)
			return nil, err
		}
		bpt.meta.size--
		// unpin previous
		// err = bpt.bpm.UnpinNode(node.id)
		return &e.value, err
	}

	return nil, &KeyNotFoundError{Value: key}

}

func (bpt *BPlusTree) Search(key Key) (*Value, error) {

	if nodeID, at, found, err := bpt.search(bpt.root.getID(), key); err != nil {
		return nil, err
	} else if found {
		n, err := bpt.bpm.FetchNode(nodeID)
		if err != nil {
			bpt.bpm.UnpinNode(nodeID)
			return nil, err
		}
		return &n.entries[at].value, err
	}

	return nil, &KeyNotFoundError{Value: key}

}

func (bpt *BPlusTree) Len() int { return int(bpt.meta.size) }

func (bpt *BPlusTree) search(nodeID NodeID, key Key) (child NodeID, at int, found bool, err error) {

	node, err := bpt.bpm.FetchNode(nodeID)
	if err != nil {
		bpt.bpm.UnpinNode(nodeID)
		return 0, 0, false, err
	}

	at, found = node.search(key)

	if node.isLeaf() {
		bpt.bpm.UnpinNode(nodeID)
		return nodeID, at, found, nil
	}

	if found {
		at++
	}
	childID := NodeID(node.children[at])

	bpt.bpm.UnpinNode(nodeID)

	return bpt.search(childID, key)
}

func (bpt *BPlusTree) split(pID, nID NodeID, siblingID NodeID, i int) error {

	p, err := bpt.bpm.FetchNode(pID)
	if err != nil {
		bpt.bpm.UnpinNode(pID)
		return err
	}

	n, err := bpt.bpm.FetchNode(nID)
	if err != nil {
		bpt.bpm.UnpinNode(nID)
		return err
	}

	sibling, err := bpt.bpm.FetchNode(siblingID)
	if err != nil {
		bpt.bpm.UnpinNode(siblingID)
		return err
	}

	p.dirty = true
	n.dirty = true
	sibling.dirty = true

	if n.isLeaf() {
		bpt.splitLeaf(p.getID(), n.getID(), sibling.getID(), i)
	} else {
		bpt.splitNode(p.getID(), n.getID(), sibling.getID(), i)
	}
	err = bpt.validate([]NodeID{p.getID(), n.getID(), sibling.getID()})
	if err != nil {
		bpt.bpm.UnpinNode(pID)
		bpt.bpm.UnpinNode(nID)
		bpt.bpm.UnpinNode(siblingID)
		return err
	}

	bpt.bpm.UnpinNode(pID)
	bpt.bpm.UnpinNode(nID)
	bpt.bpm.UnpinNode(siblingID)

	return nil
}

func (bpt *BPlusTree) splitNode(leftID, middleID, rightID NodeID, i int) error {

	left, err := bpt.bpm.FetchNode(leftID)
	if err != nil {
		bpt.bpm.UnpinNode(leftID)
		return err
	}

	middle, err := bpt.bpm.FetchNode(middleID)
	if err != nil {
		bpt.bpm.UnpinNode(middleID)
		return err
	}

	right, err := bpt.bpm.FetchNode(rightID)
	if err != nil {
		bpt.bpm.UnpinNode(rightID)
		return err
	}

	parentKey := middle.entries[bpt.fanout-1]
	right.entries = make([]entry, bpt.fanout-1)
	copy(right.entries, middle.entries[:bpt.fanout])
	middle.entries = middle.entries[bpt.fanout:]
	right.children = make([]uint64, bpt.fanout)
	copy(right.children, middle.children[:bpt.fanout])
	middle.children = middle.children[bpt.fanout:]
	err = left.insertChildAt(i, right)
	if err != nil {
		bpt.bpm.UnpinNode(leftID)
		bpt.bpm.UnpinNode(middleID)
		bpt.bpm.UnpinNode(rightID)
		return err
	}
	err = left.insertEntryAt(i, parentKey)
	if err != nil {
		bpt.bpm.UnpinNode(leftID)
		bpt.bpm.UnpinNode(middleID)
		bpt.bpm.UnpinNode(rightID)
		return err
	}

	bpt.bpm.UnpinNode(leftID)
	bpt.bpm.UnpinNode(middleID)
	bpt.bpm.UnpinNode(rightID)

	return nil
}

func (bpt *BPlusTree) splitLeaf(leftID, middleID, rightID NodeID, i int) error {

	left, err := bpt.bpm.FetchNode(leftID)
	if err != nil {
		bpt.bpm.UnpinNode(leftID)
		return err
	}

	middle, err := bpt.bpm.FetchNode(middleID)
	if err != nil {
		bpt.bpm.UnpinNode(middleID)
		return err
	}

	right, err := bpt.bpm.FetchNode(rightID)
	if err != nil {
		bpt.bpm.UnpinNode(rightID)
		return err
	}

	right.next = middle.next
	right.prev = middle.id
	middle.next = right.id

	right.entries = make([]entry, bpt.order-1)
	copy(right.entries, middle.entries[bpt.order:])
	middle.entries = middle.entries[:bpt.order]

	err = left.insertChildAt(i+1, right)
	if err != nil {
		bpt.bpm.UnpinNode(leftID)
		bpt.bpm.UnpinNode(middleID)
		bpt.bpm.UnpinNode(rightID)
		return err
	}
	err = left.insertEntryAt(i, right.entries[0])
	if err != nil {
		bpt.bpm.UnpinNode(leftID)
		bpt.bpm.UnpinNode(middleID)
		bpt.bpm.UnpinNode(rightID)
		return err
	}

	bpt.bpm.UnpinNode(leftID)
	bpt.bpm.UnpinNode(middleID)
	bpt.bpm.UnpinNode(rightID)
	return nil

}

func (bpt *BPlusTree) validate(nodesIDs []NodeID) error {

	nodes := make([]*node, len(nodesIDs))

	var err error

	for i, nodeID := range nodesIDs {
		nodes[i], err = bpt.bpm.FetchNode(nodeID)
		if err != nil {
			bpt.bpm.UnpinNode(nodeID)
			return err
		}
	}

	for _, n := range nodes {
		if n.isLeaf() {
			if len(n.entries) > ((2 * int(bpt.order)) - 1) {
				err := &OverflowError{Type: "entry", Max: ((2 * int(bpt.order)) - 1), Actual: len(n.entries)}
				bpt.bpm.UnpinNode(n.getID())
				return err
			}
		} else {

			if len(n.entries) > ((2 * int(bpt.fanout)) - 1) {
				err := &OverflowError{Type: "entry", Max: ((2 * int(bpt.fanout)) - 1), Actual: len(n.entries)}
				bpt.bpm.UnpinNode(n.getID())
				return err
			}
		}
	}

	for _, n := range nodes {
		bpt.bpm.UnpinNode(n.getID())
	}

	return nil
}


func (bpt *BPlusTree) fillDegrees() error {

	bpt.fanout = uint64((os.Getpagesize() - nodeHeaderLen() - 4) / (2 * (18 + 2 + 8)))
	bpt.order = uint64(os.Getpagesize() - nodeHeaderLen()/(2*(8+8+8)))

	if bpt.order <= 2 || bpt.fanout <= 2 {
		return &InvalidSizeError{Got: "value lower than two for either fanout or order", Should: "need at least 3"}
	}

	return nil
}