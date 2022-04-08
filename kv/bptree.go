// Package bptree implements an on-disk B+ bpt indexing scheme that can store
// key-value pairs and provide fast lookups and range scans. keys can be blobs
// binary data and value is uint64.
package kv

import (
	"errors"
)

type BPlusTree struct {
	degree uint8
	nodes  map[uint64]*node // node cache to avoid IO
	meta   metadata         // metadata about bpt structure
	root   *node            // current root node
}

const preaollocation = 1000 * 1000

func New(degree uint8) *BPlusTree {

	bpt := &BPlusTree{degree: degree}
	bpt.nodes = make(map[uint64]*node)
	bpt.root = newNode(1, degree)
	bpt.nodes[bpt.root.id] = bpt.root

	bpt.meta = metadata{
		dirty:    true,
		size:     0,
		root:     1,
		pageSize: uint32(4096),
		keySize:  uint64(64),
	}

	bpt.meta.free = make([]uint64, preaollocation)
	for i := 0; i < preaollocation; i++ {
		bpt.meta.free[i] = uint64(i + 2) // +2 since first 2 pages reserved
	}

	return bpt
}

func (bpt *BPlusTree) Insert(key Key, value Value) error {

	e := entry{
		key:   key,
		value: value,
	}

	success, err := bpt.insert(e)
	if err != nil {
		return err
	}

	if success {
		bpt.meta.size++
		bpt.meta.dirty = true
	}

	return nil
}

func (bpt *BPlusTree) Remove(key Key) (*Value, error) {

	target, at, found, err := bpt.search(bpt.root, key)
	if err != nil {
		return nil, err
	} else if !found {
		return nil, errors.New("key not found")
	}

	e, err := target.removeEntryAt(at)
	bpt.meta.size--
	return &e.value, err
}

func (bpt *BPlusTree) Search(key Key) (*Value, error) {

	n, at, found, err := bpt.search(bpt.root, key)
	if err != nil {
		return nil, err
	} else if !found {
		return nil, errors.New("key not found")
	}

	return &n.entries[at].value, nil
}

func (bpt *BPlusTree) Len() int { return int(bpt.meta.size) }

func (bpt *BPlusTree) search(n *node, key Key) (*node, int, bool, error) {
	at, found := n.search(key)

	if n.isLeaf() {
		return n, at, found, nil
	}

	if found {
		at++
	}

	child, err := bpt.nodeRef(n.children[at])
	if err != nil {
		return nil, 0, false, err
	}

	return bpt.search(child, key)
}
