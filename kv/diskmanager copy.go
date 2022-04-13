// This is a Mock Disk Manager, still saved to memory
// Source: https://github.com/brunocalza/buffer-pool-manager/blob/56e7b500fb3aa8884b71e8b478da909c0da1a918/disk_manager_mock.go
// Will be implemented correctly at some point later.. :)

package kv

import (
	"fmt"
	"os"
)

//DiskMaxNumNodes sets the disk capacity
const DiskMaxNodesCapacity = 50000000
const dataFolder = "./../data/"

// DiskManager is responsible for interacting with disk
// type DiskManager interface {
// 	ReadNode(nodeID NodeID) (*node, error)
// 	WriteNode(*node) error
// 	AllocateNode() *NodeID
// 	DeallocateNode(nodeID NodeID)
// }

//DiskManager is a memory mock for disk manager
type DiskManager struct {
	numNode NodeID
	nodes   map[NodeID]*node
}

//ReadNode reads a node from nodes
func (d *DiskManager) ReadNode(nodeID NodeID) (*node, error) {

	// REAL DISK
	loc := dataFolder + fmt.Sprint(nodeID)

	dat, err := os.ReadFile(loc)

	if err != nil {
		return nil, err
	}

	node := newNode(uint64(nodeID))

	node.UnmarshalBinary(dat)
	return node, nil

	//MOCK DISK
	// if node, ok := d.nodes[nodeID]; ok {
	// 	return node, nil
	// }

	// return nil, fmt.Errorf("Node not found")

}

//WriteNode writes a node in memory to nodes
func (d *DiskManager) WriteNode(node *node) error {

	bin, err := node.MarshalBinary()
	if err != nil {
		return err
	}

	loc := dataFolder + fmt.Sprint(node.getID())

	err = os.WriteFile(loc, bin, 0644)
	if err != nil {
		return err
	}

	d.nodes[NodeID(node.id)] = node
	return nil
}

//AllocateNode allocates one more node
func (d *DiskManager) AllocateNode() *NodeID {
	if d.numNode == DiskMaxNodesCapacity {
		return nil
	}
	nodeID := NodeID(d.numNode)
	d.numNode = d.numNode + 1
	return &nodeID
}

//DeallocateNode removes node from disk
func (d *DiskManager) DeallocateNode(nodeID NodeID) {
	delete(d.nodes, nodeID)
}

//NewDiskManager returns a in-memory mock of disk manager
func NewDiskManager(fromNodeID NodeID) *DiskManager {
	return &DiskManager{1, make(map[NodeID]*node)}
}

//Print nodes
//Depracted
func (d *DiskManager) PrintNodes() {
	fmt.Println("Depracted")
	// fmt.Println("------------------------------------")
	// fmt.Println("Nodes on disk:")
	// for _, node := range d.nodes {
	// 	fmt.Printf("node id=%d, dirtybit=%t, counter=%d, content=not implemented\n", node.getID(), node.IsDirty(), node.getPinCounter())
	// }
}
