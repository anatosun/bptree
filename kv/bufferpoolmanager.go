package kv

import (
	"errors"
	"fmt"
)

const BufferPoolCapacity = 4
const debug_buffer = false

type BufferPoolManager struct {
	diskManager    DiskManager
	pool           [BufferPoolCapacity]*node
	replacePolicy  *ClockPolicy
	freeFramesList []NodeID
	nodesTable     map[NodeID]NodeID
}

func NewBufferPoolManager(DiskManager DiskManager, clock *ClockPolicy) *BufferPoolManager {
	freeFramesList := make([]NodeID, 0)
	nodes := [BufferPoolCapacity]*node{}
	for i := 0; i < BufferPoolCapacity; i++ {
		freeFramesList = append(freeFramesList, NodeID(i))
		nodes[NodeID(i)] = nil
	}
	return &BufferPoolManager{DiskManager, nodes, clock, freeFramesList, make(map[NodeID]NodeID)}
}

func (bpm *BufferPoolManager) GetNewNode() *node {
	frameID, isFromFreeFramesList := bpm.GetFrameID()
	if frameID == nil {
		return nil
	}

	// Victimized, i.e. not from free list
	if !isFromFreeFramesList {
		node := bpm.pool[*frameID]
		if node.IsDirty() {
			// save to disk
			bpm.diskManager.WriteNode(node)
			node.dirty = false
			node.pinCounter = 0

		} else {
			// let's still save it to disk
			// since we're currently only emulating the disk
			bpm.diskManager.WriteNode(node)
			node.dirty = false
			node.pinCounter = 0
		}

		//remove node from frame
		delete(bpm.nodesTable, NodeID(node.id))

		//fmt.Println("Node not from free list")
	} else {
		//fmt.Println("Node from free list")
	}

	// allocate new node
	id := bpm.diskManager.AllocateNode()
	degree := uint8(70)
//	node := &node{id: , data: [NodeDataSize]byte{}, dirty: false, pinCounter: 1}

	node := &node{id: uint64(*id), dirty: true, entries: make([]entry, 0, degree), degree: degree, children: make([]uint64, 0, degree),  pinCounter: 1}

	bpm.nodesTable[*id] = *frameID
	bpm.pool[*frameID] = node

	// return Node
	return node
}

func (bpm *BufferPoolManager) GetFrameID() (*NodeID, bool) {
	if len(bpm.freeFramesList) > 0 {
		frameID := bpm.freeFramesList[0]
		bpm.freeFramesList = bpm.freeFramesList[1:]
		return &frameID, true
	}

	return (*bpm.replacePolicy).Victim(), false
}

func (bpm *BufferPoolManager) UnpinNode(nodeID NodeID, dirty bool) error {
	// Unpin node by decreasing counter.
	// If isDirty is true, then set dirtybit to true

	frameID, found := bpm.nodesTable[nodeID]

	if !found {
		return errors.New("Node doesn't exist")
	}
	node := bpm.pool[frameID]

	err := node.decreasePinCounter()

	if err != nil {
		return err
	}

	if node.getPinCounter() <= 0 {
		(*bpm.replacePolicy).Unpin(frameID)
	}

	if node.IsDirty() || dirty {
		node.dirty = true
	}

	return nil
}

func (bpm *BufferPoolManager) PrintNodes() {
	fmt.Println("------------------------------------")
	fmt.Println("Nodes in Buffer Pool:")
	for _, node := range bpm.pool {
		fmt.Printf("node id=%d, dirtybit=%t, counter=%d, content=not implemented\n", node.getID(), node.IsDirty(), node.getPinCounter())
	}
}

func (bpm *BufferPoolManager) FetchNode(nodeID NodeID) *node {
	// Fetch node with given nodeID,

	//check first if node is in buffer pool, if yes, return
	frameID, found := bpm.nodesTable[nodeID]

	if found {
		node := bpm.pool[frameID]
		node.increasePinCounter()

		(*bpm.replacePolicy).Pin(frameID) // remove node from clock
		return node

		if debug_buffer {
			fmt.Printf("nodeID=%d,frameID=%d exists in buffer pool\n", nodeID, frameID)
		}
	} else {
		// Node doesn't exist in buffer pool
		if debug_buffer {
			fmt.Printf("nodeID=%d doesn't exists in buffer pool\n", nodeID)
		}
	}

	// first get a free frameID
	freeFrameID, isFromFreeFramesList := bpm.GetFrameID()

	// If Victimized, i.e. not from free list, save to disk
	if !isFromFreeFramesList {
		node := bpm.pool[*freeFrameID]
		if node.IsDirty() {
			// save to disk
			bpm.diskManager.WriteNode(node)
			node.dirty = false
			node.pinCounter = 0
		} else {
			// let's still save it to disk
			// since we're currently only emulating the disk
			bpm.diskManager.WriteNode(node)
			node.dirty = false
			node.pinCounter = 0
		}

		//remove node from frame
		delete(bpm.nodesTable, NodeID(node.id))

		//fmt.Println("Node not from free list")
	} else {
		//fmt.Println("Node from free list")

	}

	node, err := bpm.diskManager.ReadNode(nodeID)
	if err != nil {
		return nil
	}

	(*node).setPinCounter(1)
	bpm.nodesTable[nodeID] = *freeFrameID
	bpm.pool[*freeFrameID] = node

	return node
}

func (bpm *BufferPoolManager) DeleteNode(nodeID NodeID) error {

	frameID, found := bpm.nodesTable[nodeID]

	if !found {
		return errors.New("Node doesn't exist")
	}

	node := bpm.pool[frameID]

	if !node.hasZeroPins() {
		return errors.New("Node is still in use, cannot be deleted")
	}

	delete(bpm.nodesTable, NodeID(node.id))
	bpm.diskManager.DeallocateNode(nodeID)
	bpm.freeFramesList = append(bpm.freeFramesList, frameID)

	// Note: The node will still stay inside of the nodesTable until it has been replaced
	// by the node replacer.
	// Let's call this a feature.

	return nil
}

func (bpm *BufferPoolManager) FlushNode(nodeID NodeID) bool {
	frameID, found := bpm.nodesTable[nodeID]
	if found {
		node := bpm.pool[frameID]
		bpm.diskManager.WriteNode(node)
		node.dirty = false //written to disk, i.e. up to date
		return true
	}
	return false
}

func (bpm *BufferPoolManager) FlushAllNodes() {
	for id := range bpm.nodesTable {
		bpm.FlushNode(id)
	}
}
