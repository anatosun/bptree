// This is a Mock Disk Manager, still saved to memory
// Source: https://github.com/brunocalza/buffer-pool-manager/blob/56e7b500fb3aa8884b71e8b478da909c0da1a918/disk_manager_mock.go
// Will be implemented correctly at some point later.. :)

package kv

import (
	"errors"
	"fmt"
)

//DiskMaxNumNodes sets the disk capacity
const DiskMaxNodesCapacity = 20

// DiskManager is responsible for interacting with disk
type DiskManager interface {
	ReadNode(nodeID NodeID) (*node, error)
	WriteNode(*node) error
	AllocateNode() *NodeID
	DeallocateNode(nodeID NodeID)
}

//DiskManagerMock is a memory mock for disk manager
type DiskManagerMock struct {
	numNode int64 // tracks the number of nodes. -1 indicates that there is no node, and the next to be allocates is 0
	nodes   map[NodeID]*node
}

//ReadNode reads a node from nodes
func (d *DiskManagerMock) ReadNode(nodeID NodeID) (*node, error) {
	if node, ok := d.nodes[nodeID]; ok {
		return node, nil
	}

	return nil, errors.New("Node not found")
}

//WriteNode writes a node in memory to nodes
func (d *DiskManagerMock) WriteNode(node *node) error {
	d.nodes[NodeID(node.id)] = node
	return nil
}

//AllocateNode allocates one more node
func (d *DiskManagerMock) AllocateNode() *NodeID {
	if d.numNode == DiskMaxNodesCapacity-1 {
		return nil
	}
	d.numNode = d.numNode + 1
	nodeID := NodeID(d.numNode)
	return &nodeID
}

//DeallocateNode removes node from disk
func (d *DiskManagerMock) DeallocateNode(nodeID NodeID) {
	delete(d.nodes, nodeID)
}

//NewDiskManagerMock returns a in-memory mock of disk manager
func NewDiskManagerMock() *DiskManagerMock {
	return &DiskManagerMock{-1, make(map[NodeID]*node)}
}

//Print nodes
func (d *DiskManagerMock) PrintNodes() {
	fmt.Println("------------------------------------")
	fmt.Println("Nodes on disk:")
	for _, node := range d.nodes {
		fmt.Printf("node id=%d, dirtybit=%t, counter=%d, content=not implemented\n", node.getID(), node.IsDirty(), node.getPinCounter())
	}
}
