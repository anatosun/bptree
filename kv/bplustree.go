// Package bptree implements an on-disk B+ bpt indexing scheme that can store
// key-value pairs and provide fast lookups and range scans. keys can be blobs
// binary data and value is uint64.
package kv

type BPlusTree struct {
	bpm    *BufferPoolManager
	degree uint8
	nodes  map[uint64]*node // node cache to avoid IO
	meta   metadata         // metadata about bpt structure
	root   *node            // current root node
}

const preaollocation = 1000 * 1000

func New(degree uint8) storage {

	clock := NewClockPolicy(BufferPoolCapacity)
	disk := NewDiskManagerMock()
	bpm := NewBufferPoolManager(disk, clock)

	bpt := &BPlusTree{degree: degree}
	bpt.bpm = bpm
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

	for i := range bpt.meta.free {
		bpt.meta.free[i] = uint64(i + 2)
	}

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
		// err = bpt.bpm.UnpinNode(NodeID(node.id))
		return &e.value, err
	}

	return nil, &KeyNotFoundError{Value: key}

}

func (bpt *BPlusTree) Search(key Key) (*Value, error) {

	if n, at, found, err := bpt.search(bpt.root, key); err != nil {
		return nil, err
	} else if found {
		// unpin previous before returning value
		// err = bpt.bpm.UnpinNode(NodeID(n.id))
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
	child, err = bpt.nodeRef(n.children[at]) //TODO: After no longer in use, unpin
	if err != nil {
		return nil, 0, false, err
	}
	// unpin previous before iterating over the next
	// err = bpt.bpm.UnpinNode(NodeID(n.id))
	// if err != nil {
	// 	return n, at, false, err
	// }
	return bpt.search(child, key)
}
