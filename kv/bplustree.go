// Package bptree implements an on-disk B+ bpt indexing scheme that can store
// key-value pairs and provide fast lookups and range scans. keys can be blobs
// binary data and value is uint64.
package kv

// "os"

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

// returns a new B+ tree with the optimal parameters
func New() *BPlusTree {

	clock := NewClockPolicy(BufferPoolCapacity)
	disk := NewDiskManager()
	bpm := NewBufferPoolManager(disk, clock)
	bpt := &BPlusTree{}
	bpt.bpm = bpm
	bpt.nodes = make(map[uint64]*node)

	initNodeID, _ := bpt.bpm.GetNewNode()
	initNode, err := bpt.bpm.FetchNode(initNodeID) //Removes it from clock

	if err != nil {
		panic("Couldn't init B+Tree")
	}

	bpt.root = initNode

	bpt.nodes[bpt.root.id] = bpt.root //TODO: eventually remove this if no longer needed

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

// serves to put a key/value pair in the B+ tree
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

// removes a given key and its entry in the B+ tree
// this deletion is lazy, it only deletes the entry in the node without rebaleasing the tree
func (bpt *BPlusTree) Remove(key Key) (value *Value, err error) {

	if nodeID, at, found, err := bpt.search(bpt.root.getID(), key); err != nil {
		return nil, err
	} else if found {
		node, err := bpt.bpm.FetchNode(nodeID)

		if err != nil {
			bpt.bpm.UnpinNode(nodeID, false)
			return nil, err
		}

		e, err := node.deleteEntryAt(at)
		bpt.bpm.UnpinNode(nodeID, true)

		if err != nil {
			return nil, err
		}
		bpt.meta.size--

		return &e.value, err
	}

	return nil, &KeyNotFoundError{Value: key}

}

// search for a given key among the nodes of the B+tree
func (bpt *BPlusTree) Search(key Key) (*Value, error) {

	if nodeID, at, found, err := bpt.search(bpt.root.getID(), key); err != nil {
		return nil, err
	} else if found {
		n, err := bpt.bpm.FetchNode(nodeID)
		if err != nil {
			bpt.bpm.UnpinNode(nodeID, false)
			return nil, err
		}
		bpt.bpm.UnpinNode(nodeID, false)
		return &n.entries[at].value, err
	}

	return nil, &KeyNotFoundError{Value: key}

}

// returns the length of the B+ tree
func (bpt *BPlusTree) Len() int { return int(bpt.meta.size) }

// recursively search for a key in the node and its children
func (bpt *BPlusTree) search(nodeID NodeID, key Key) (child NodeID, at int, found bool, err error) {

	node, err := bpt.bpm.FetchNode(nodeID)
	if err != nil {
		bpt.bpm.UnpinNode(nodeID, false)
		return 0, 0, false, err
	}

	at, found = node.search(key)

	if node.isLeaf() {
		bpt.bpm.UnpinNode(nodeID, false)
		return nodeID, at, found, nil
	}

	if found {
		at++
	}
	childID := NodeID(node.children[at])

	bpt.bpm.UnpinNode(nodeID, false)

	return bpt.search(childID, key)
}

// split the given three nodes
func (bpt *BPlusTree) split(pID, nID NodeID, siblingID NodeID, i int) error {

	p, err := bpt.bpm.FetchNode(pID)
	if err != nil {
		bpt.bpm.UnpinNode(pID, false)
		return err
	}

	n, err := bpt.bpm.FetchNode(nID)
	if err != nil {
		bpt.bpm.UnpinNode(nID, false)
		return err
	}

	sibling, err := bpt.bpm.FetchNode(siblingID)
	if err != nil {
		bpt.bpm.UnpinNode(siblingID, false)
		return err
	}

	if n.isLeaf() {
		bpt.splitLeaf(p, n, sibling, i)
	} else {
		bpt.splitNode(p, n, sibling, i)
	}
	err = bpt.validate([]*node{p, n, sibling})

	if err != nil {
		bpt.bpm.UnpinNode(pID, true)
		bpt.bpm.UnpinNode(nID, true)
		bpt.bpm.UnpinNode(siblingID, true)
		return err
	}

	bpt.bpm.UnpinNode(pID, true)
	bpt.bpm.UnpinNode(nID, true)
	bpt.bpm.UnpinNode(siblingID, true)

	return nil
}

// split the (internal) node into the given three nodes
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

// split the leaf into the given three nodes
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

// checks if the slices do not exceed the given bound, otherwise raises an error
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

// completes the value of both the fanout and the order
func (bpt *BPlusTree) fillDegrees() error {

	// computed by hand on a page size of 4096
	bpt.fanout = uint64(60)
	bpt.order = uint64(60)

	if bpt.order <= 2 || bpt.fanout <= 2 {
		return &InvalidSizeError{Got: "value lower than two for either fanout or order", Should: "need at least 3"}
	}

	return nil
}
