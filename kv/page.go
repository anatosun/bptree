package kv

import (
	"fmt"
)

//CONFIG: set here the desired pageDataSize in bytes
// 4KB = 4096bytes in total

// Compute these values using pagesizes_calculator.go
// Desired Total Page Size: 4096
// Actual Total Page Size: 3687 (-10% from desired)
// Node Degree: 140

// const PageDataSize = 3687

// type PageID uint32

// type Page struct {
// 	id         PageID
// 	data       [PageDataSize]byte
// 	dirty      bool
// 	pinCounter uint64
// }

func (node *node) getID() NodeID {
	return NodeID(node.id)
}

func (node *node) getPinCounter() uint64 {
	return node.pinCounter
}

func (node *node) setPinCounter(val uint64) {
	node.pinCounter = val
}

func (node *node) hasZeroPins() bool {
	return node.pinCounter <= 0
}

func (node *node) increasePinCounter() error {
	//possibly implement limit on pins here
	node.pinCounter++
	return nil
}

func (node *node) decreasePinCounter() error {
	//possibly implement limit on pins here
	if node.pinCounter <= 0 {
		return nil
	}

	node.pinCounter--

	return nil

}

func (node *node) IsDirty() bool {
	return node.dirty
}

func (node *node) Print() {
	fmt.Printf("node.id=%d\n", node.id)
	fmt.Printf("node.counter=%d\n", node.pinCounter)
	fmt.Printf("node.dirty=%t\n", node.dirty)
	fmt.Println("---------")
}
