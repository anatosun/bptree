package kv

import (
	"fmt"
	"math"
)

type node struct {
	entries  []*entry
	num      int
	isLeaf   bool
	degree   int
	parent   *node
	children []*node
	size     int
	max      int
	min      int
	left     *node
	right    *node
	next     *node
}

func newNode(degree int) *node {

	return &node{entries: make([]*entry, 0, degree), num: 0, isLeaf: false, degree: degree, parent: nil, children: make([]*node, 0, degree), size: 0, max: degree - 1, min: int(math.Ceil(float64(degree)/2) - 1), left: nil, right: nil, next: nil}
}

func (n *node) full() bool {
	if n.isLeaf {
		return n.num == n.max
	} else {
		return n.size == n.max
	}
}

func (n *node) overfull() bool {
	return n.size == n.max+1
}

func (n *node) empty() bool {
	if n.isLeaf {
		return n.num == 0
	} else {
		return n.size == 0
	}
}

func (n *node) lendable() bool {

	if n.isLeaf {
		return n.num > n.min
	} else {
		return n.size > n.min
	}
}

func (n *node) defficient() bool {
	if n.isLeaf {
		return n.num < n.min
	} else {
		return n.size < n.min
	}
}

func (n *node) mergeable() bool {
	if n.isLeaf {
		return n.num == n.min
	} else {
		return n.size == n.min
	}
}

func (n *node) insertChildAt(child *node, at int) error {

	if n.full() {
		return fmt.Errorf("node is full")
	}

	temp := append(n.children[0:at], child)
	n.children = append(temp, n.children[at:]...)
	n.size++
	return nil
}

func (n *node) findIndexOfChild(child *node) (int, error) {

	for i, v := range n.children {
		if v == child {
			return i, nil
		}
	}

	return 0, fmt.Errorf("could not find child")
}

func (n *node) appendChild(child *node) error {
	if n.full() {
		return fmt.Errorf("node is full")
	}
	if child == nil {
		return fmt.Errorf("child is nil")
	}
	n.children = append(n.children, child)
	n.size++
	return nil
}

func (n *node) prependChild(child *node) error {
	if n.full() {
		return fmt.Errorf("node is full")
	}

	n.children = append([]*node{child}, n.children...)
	n.size++
	return nil
}

func (n *node) removeChildAt(at int) error {
	if n.empty() {
		return fmt.Errorf("node is empty")
	}

	if at > n.size-1 {
		return fmt.Errorf("index of deletion is out of range")
	}

	n.children = append(n.children[:at], n.children[at+1:]...)
	n.size--
	return nil
}

func (n *node) removeChild(child *node) error {

	if at, err := n.findIndexOfChild(child); err != nil {
		return err
	} else {
		return n.removeChildAt(at)
	}

}

func (n *node) findLeaf(key Key) (*node, error) {
	at := 0

	for i, e := range n.entries {
		at = i
		if key < e.key {
			break
		}
	}

	child := n.children[at]
	if child.isLeaf {
		return child, nil
	} else {
		return child.findLeaf(key)
	}
}

func (n *node) splitChildren(split int) []*node {

	halfChildren := n.children[split:]
	n.children = n.children[:split]

	return halfChildren
}

func (n *node) nextChildAt() (int, error) {
	for i := range n.children {
		if n.children[i] == nil {
			return i, nil
		}
	}

	return 0, fmt.Errorf("could not find an empty slot")
}

func (n *node) shiftChildrenDown(amount int) []*node {
	children := make([]*node, n.degree)
	for i := amount; i < n.size; i++ {
		children[i-amount] = n.children[i]
	}
	return children
}

func (n *node) print() {

	fmt.Printf("[ ")
	for _, e := range n.entries {
		fmt.Printf("%d ", e.key)
	}
	fmt.Printf("]")

}
