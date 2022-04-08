package kv

import "fmt"

type node struct {
	id       uint64
	dirty    bool
	degree   uint8
	next     uint64
	prev     uint64
	children []uint64
	entries  []entry
}

func newNode(id uint64, degree uint8) *node {
	return &node{id: id, dirty: true, entries: make([]entry, 0, degree), degree: degree, children: make([]uint64, 0, degree)}
}

func (n *node) insertChildAt(at int, child *node) error {
	previous_size := len(n.children)
	n.children = append(n.children[0:at], append([]uint64{child.id}, n.children[at:]...)...)
	new_size := len(n.children)
	if previous_size+1 != new_size {
		return fmt.Errorf("there was a problem inserting child at position %d", at)
	}

	return nil
}

func (n *node) full() bool {

	return len(n.entries) == ((2 * int(n.degree)) - 1)

}

// dumb implementation of http://eecs.csuohio.edu/~sschung/cis611/B+Trees.pdf
func (p *node) splitNode(n, sibling *node, i int) error {

	parentKey := n.entries[p.degree-1]

	sibling.entries = make([]entry, p.degree-1)
	copy(sibling.entries, n.entries[:p.degree])
	n.entries = n.entries[p.degree:]

	sibling.children = make([]uint64, p.degree)
	copy(sibling.children, n.children[:p.degree])
	n.children = n.children[p.degree:]

	err := p.insertChildAt(i, sibling)
	if err != nil {
		return err
	}

	err = p.insertEntryAt(i, parentKey)
	if err != nil {
		return err
	}

	return nil

}

func (p *node) split(n, sibling *node, i int) error {
	p.dirty = true
	n.dirty = true
	sibling.dirty = true

	if n.isLeaf() {
		return p.splitLeaf(n, sibling, i)
	}

	return p.splitNode(n, sibling, i)
}
