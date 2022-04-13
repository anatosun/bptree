package kv

import (
	"fmt"
	"os"
)

//DiskMaxNumNodes sets the disk capacity
const DiskMaxNodesCapacity = 50000000
const dataFolder = "./../data/"
const avoidStoringToDisk = false

//DiskManager is a memory mock for disk manager
type DiskManager struct {
	numNode NodeID
	nodes   map[NodeID]*node
}

//ReadNode reads a node from nodes
func (d *DiskManager) ReadNode(nodeID NodeID) (*node, error) {

	if avoidStoringToDisk {
		//MOCK DISK IN MEMORY
		if node2, ok := d.nodes[nodeID]; ok {
			return node2, nil
		}

		return nil, fmt.Errorf("Node not found")
	}

	// REAL DISK
	loc := dataFolder + fmt.Sprint(nodeID)

	dat, err := os.ReadFile(loc)

	if err != nil {
		return nil, err
	}

	node := newNode(uint64(nodeID))

	node.UnmarshalBinary(dat)

	return node, nil

}

//WriteNode writes a node in memory to nodes
func (d *DiskManager) WriteNode(node *node) error {

	if avoidStoringToDisk {
		//MOCK IN MEMORY
		d.nodes[NodeID(node.id)] = node
		return nil
	}

	// REAL DISK
	bin, err := node.MarshalBinary()
	if err != nil {
		return err
	}

	loc := dataFolder + fmt.Sprint(node.getID())

	err = os.WriteFile(loc, bin, 0644)
	if err != nil {
		return err
	}

	return nil
}

//AllocateNode allocates one more node
func (d *DiskManager) AllocateNode() (NodeID, error) {
	if d.numNode == DiskMaxNodesCapacity-1 {
		return 0, fmt.Errorf("Couldn't allocate new node")
	}
	nodeID := NodeID(d.numNode)
	d.numNode = d.numNode + 1
	return nodeID, nil
}

//DeallocateNode removes node from disk
func (d *DiskManager) DeallocateNode(nodeID NodeID) error {
	loc := dataFolder + fmt.Sprint(nodeID)

	e := os.Remove(loc)
	if e != nil {
		return fmt.Errorf("Couldn't delete node")
	}
	return nil
}

func NewDiskManager() *DiskManager {
	return &DiskManager{1, make(map[NodeID]*node)}
}

//Print nodes
//Depracted
func (d *DiskManager) PrintNodes() {
	//	fmt.Println("Depracted")
	fmt.Println("------------------------------------")
	fmt.Println("Nodes on disk:")
	for _, node := range d.nodes {
		fmt.Printf("node id=%d, dirtybit=%t, counter=%d, content=not implemented\n", node.getID(), node.IsDirty(), node.getPinCounter())
	}
}
