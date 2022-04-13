package kv

import (
	"errors"
	"fmt"
)

const BufferPoolCapacity = 5 //min 4
const debug_buffer = false

type BufferPoolManager struct {
	diskManager    *DiskManager
	pool           [BufferPoolCapacity]*node
	replacePolicy  *ClockPolicy
	freeFramesList []NodeID
	nodesTable     map[NodeID]NodeID // maps nodeID <-> FrameID of buffer pool
}

func NewBufferPoolManager(DiskManager *DiskManager, clock *ClockPolicy) *BufferPoolManager {
	freeFramesList := make([]NodeID, 0)
	nodes := [BufferPoolCapacity]*node{}
	for i := 0; i < BufferPoolCapacity; i++ {
		freeFramesList = append(freeFramesList, NodeID(i))
		nodes[NodeID(i)] = nil
	}
	return &BufferPoolManager{DiskManager, nodes, clock, freeFramesList, make(map[NodeID]NodeID)}
}

func (bpm *BufferPoolManager) GetNewNode() (NodeID, error) {
	frameID, isFromFreeFramesList := bpm.GetFrameID()
	if frameID == nil {
		return 0, fmt.Errorf("No free/unused frame in buffer pool.\n")
	}

	// Victimized, i.e. not from free list
	if !isFromFreeFramesList {
		node := bpm.pool[*frameID]
		if node.IsDirty(){ //TODO: Fix this mess
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
	newNodeID, err := bpm.diskManager.AllocateNode()
	if err != nil {
		return 0, fmt.Errorf("Couldn't allocate new node")
	}
	//node := &node{id: , data: [NodeDataSize]byte{}, dirty: false, pinCounter: 1}

	//node := &node{id: uint64(*id), dirty: true, entries: make([]entry, 0, degree), degree: degree, children: make([]uint64, 0, degree),  pinCounter: 1}

	node := newNode(uint64(newNodeID))
	bpm.nodesTable[newNodeID] = *frameID
	bpm.pool[*frameID] = node

	//Finally, since creation of the new node does not imply it will be used, put it directly 
	// into the clock, since it gets created with pin counter 0. Also, since creation happens with the dirty 
	// bit set to true, this means that it will be stored automatically by the disk manager.
	bpm.FetchNode(newNodeID) //sets pin counter to 1
	bpm.UnpinNode(newNodeID) //unpins and puts it into the clock

	// return NodeID
	return newNodeID, nil
}

func (bpm *BufferPoolManager) GetFrameID() (*NodeID, bool) {
	if len(bpm.freeFramesList) > 0 {
		frameID := bpm.freeFramesList[0]
		bpm.freeFramesList = bpm.freeFramesList[1:]
		return &frameID, true
	}

	return (*bpm.replacePolicy).Victim(), false
}

func (bpm *BufferPoolManager) UnpinNode(nodeID NodeID) error {
	// Unpin node by decreasing counter.
	// If isDirty is true, then set dirtybit to true
	//fmt.Printf("\nUnpin node=%v\n", nodeID)
	frameID, found := bpm.nodesTable[nodeID]

	if !found {
		return errors.New("Node doesn't exist in buffer pool")
	}
	node := bpm.pool[frameID]

	err := node.decreasePinCounter()

	if err != nil {
		return err
	}

	if node.getPinCounter() <= 0 {
		(*bpm.replacePolicy).Unpin(frameID)
	}

	// if node.IsDirty() || dirty {
	// 	node.dirty = true
	// }

	return nil
}

func (bpm *BufferPoolManager) FetchNode(nodeID NodeID) (*node, error) {

	//check first if node is in buffer pool, if yes, return
	frameID, found := bpm.nodesTable[nodeID]

	if found {
		node := bpm.pool[frameID]
		node.increasePinCounter()

		(*bpm.replacePolicy).Pin(frameID) // remove node from clock
		return node, nil

		if debug_buffer {
			fmt.Printf("nodeID=%d,frameID=%d exists in buffer pool\n", nodeID, frameID)
		}
	} else {
		// Node doesn't exist in buffer pool
		if debug_buffer {
			fmt.Printf("nodeID=%d doesn't exists in buffer pool\n", nodeID)
		}
	}

	// otherwise, get a free frameID
	freeFrameID, isFromFreeFramesList := bpm.GetFrameID()

	// if nil, then there's no free space in the buffer and everything is being used
	if freeFrameID == nil {
		return nil, fmt.Errorf("No free space in the buffer and everything is being used")
	}

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
		return nil, err
	}

	(*node).setPinCounter(1)
	bpm.nodesTable[nodeID] = *freeFrameID
	bpm.pool[*freeFrameID] = node

	return node, nil
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
		bpm.diskManager.WriteNode(node) //TODO: Add here marshalling
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

func (bpm *BufferPoolManager) PrintPool() {
	fmt.Println("------------------------------------")
	fmt.Println("Nodes in Buffer Pool:")
	for _, node := range bpm.pool {
		if node != nil {
			fmt.Printf("node id=%d, dirtybit=%t, counter=%d, content=not implemented\n", node.getID(), node.IsDirty(), node.getPinCounter())
		}
	}
}

func (bpm *BufferPoolManager) PrintTable() {
	fmt.Println("------------------------------------")
	fmt.Println("Nodes in Table:")
	for _, i := range bpm.nodesTable {
		if nil != bpm.pool[i] {
			fmt.Printf("i=%d, node=%v\n", i, *bpm.pool[i])
		}
	}
}