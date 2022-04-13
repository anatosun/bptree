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
const avoidStoringToDisk = true

// DiskManager is responsible for interacting with disk
// type DiskManager interface {
// 	ReadNode(nodeID NodeID) (*node, error)
// 	WriteNode(*node) error
// 	AllocateNode() *NodeID
// 	DeallocateNode(nodeID NodeID)
// }

//DiskManager is a memory mock for disk manager
type DiskManager struct {
	numNode 	NodeID
	nodes   	map[NodeID]*node
}

//ReadNode reads a node from nodes
func (d *DiskManager) ReadNode(nodeID NodeID) (*node, error) {

	if avoidStoringToDisk {
		//MOCK DISK IN MEMORY
		if node2, ok := d.nodes[nodeID]; ok {
			// node2.dirty = true

			// n1s := fmt.Sprintf("%v", node)
			// n2s := fmt.Sprintf("%v", node2)
			// if n1s != n2s {
			// 	fmt.Printf("disk  =%v\n",n1s)
			// 	fmt.Printf("memory=%v\n",n2s)
			// 	fmt.Println("MISMATCH READcING")
			// }
			
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
	if d.numNode == DiskMaxNodesCapacity - 1 {
		return 0, fmt.Errorf("Couldn't allocate new node")
	}
	nodeID := NodeID(d.numNode)
	d.numNode = d.numNode + 1
	return nodeID, nil
}

//DeallocateNode removes node from disk
func (d *DiskManager) DeallocateNode(nodeID NodeID) {
	delete(d.nodes, nodeID)
}

func NewDiskManager() *DiskManager {
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
