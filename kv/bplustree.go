// Package bptree implements an on-disk B+ bpt indexing scheme that can store
// key-value pairs and provide fast lookups and range scans. keys can be blobs
// binary data and value is uint64.
package kv

import(
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

// Convert node to nodeID, fetch it using bpm
func (bpt *BPlusTree) search(nodeID NodeID, key Key) (childID *NodeID, at int, found bool, err error) {


	// fmt.Printf("searching... node=%v key=%v\n", n, key)

	// Cant fetch if it's not in bufferpool. first implement allocate()
	// nid := n.getID()

	n2, err := bpt.bpm.FetchNode(nodeID)

	if err != nil {
		return nil, 0, false, err 
	}

	// fmt.Printf("search=%v ; found =%v\n", n, n2)

	n := n2
	nid := n.getID()

	at, found = n.search(key)

	if n.isLeaf() {
		bpt.bpm.UnpinNode(nid)
		return &nid, at, found, nil
	}

	if found {
		at++
	}

	// child, err = bpt.nodeRef(n.children[at]) //TODO: After no longer in use, unpin
	child_ID := NodeID(n.children[at])
	bpt.bpm.UnpinNode(n.getID()) //n no longer needed
	_, err = bpt.bpm.FetchNode(child_ID)


	if err != nil {
		return nil, 0, false, err //here the error happens
	}

	// unpin previous before iterating over the next
	// err = bpt.bpm.UnpinNode(NodeID(n.id))
	// if err != nil {
	// 	return n, at, false, err
	// }

	//return bpt.search(child, key)

	bpt.bpm.UnpinNode(child_ID)

	return bpt.search(child_ID, key)
}

func dummyfmt() {
	fmt.Println("x")
}