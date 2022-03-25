package kv

import "fmt"

type node struct {
	entries  []*entry
	degree   int
	children []*node
	next     *node
	prev     *node
}

func newNode(degree int) *node {
	return &node{entries: make([]*entry, 0, degree-1), degree: degree, children: make([]*node, 0, degree-1), next: nil, prev: nil}
}

func (n *node) insertChildAt(at int, child *node) error {
	n.children = append(n.children[0:at], append([]*node{child}, n.children[at:]...)...)
	return nil
}

func (n *node) recursive(key Key) (*node, int, error) {

	if n == nil {
		panic(fmt.Errorf("node is nil"))
	}

	at, found := n.search(key)

	if n.isLeaf() {
		return n, at, nil
	}

	if found {
		at++
	}

	return n.children[at].recursive(key)
}

func (n *node) full() bool {
	if n.isLeaf() {
		return len(n.entries) == ((2 * n.degree) - 1)
	}
	return len(n.entries) == ((2 * n.degree) - 1)

}

func (n *node) empty() bool {

	if n == nil {
		return true
	}

	if n.isLeaf() {
		return len(n.entries) == 0
	}

	return false

}

func (left *node) splitNode(middle, right *node, at int) error {

	parentKey := left.entries[left.degree-1]
	middle.entries = make([]*entry, 0, left.degree-1)
	middle.entries = append(middle.entries, left.entries[:left.degree]...)
	left.entries = left.entries[left.degree:]

	middle.children = make([]*node, 0, left.degree)
	middle.children = append(middle.children, left.children[:left.degree]...)
	left.children = left.children[left.degree:]

	right.insertChildAt(at, middle)
	right.insertEntryAt(at, parentKey)

	return nil

}

func (n *node) place(e *entry) error {

	if n.isLeaf() {
		at, found := n.search(e.key)

		if found {
			return n.update(at, e.value)

		}

		return n.insertEntryAt(at, e)
	}

	return n.path(e)
}

func (n *node) path(e *entry) error {
	at, found := n.search(e.key)

	if found {
		at++
	}

	child := n.children[at]

	if child.full() {
		sib := newNode(n.degree)

		if err := child.split(sib, n, at); err != nil {
			return err
		}

		if e.key >= n.entries[at].key {
			child = n.children[at+1]

		}
	}

	return child.place(e)
}

func (left *node) split(middle, right *node, at int) error {

	if left.isLeaf() {

		return left.splitLeaf(middle, right, at)

	}

	return left.splitNode(middle, right, at)
}
