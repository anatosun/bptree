package kv

import (
	"fmt"
)

func (n *node) isLeaf() bool {
	return len(n.children) == 0
}

func (n *node) insertEntryAt(at int, e entry) error {
	prior := len(n.entries)
	n.entries = append(n.entries[0:at], append([]entry{e}, n.entries[at:]...)...)
	after := len(n.entries)

	if prior+1 != after {
		return fmt.Errorf("error inserting entry")
	}
	return nil
}

func (n *node) update(at int, value Value) error {
	n.entries[at].value = value
	return nil
}

func (n *node) removeEntryAt(at int) (entry, error) {
	prior := len(n.entries)
	entry := n.entries[at]
	n.entries = append(n.entries[0:at], n.entries[at+1:]...)
	after := len(n.entries)

	if prior != after+1 {
		return entry, fmt.Errorf("error deleting entry")
	}

	return entry, nil
}

func (n *node) search(key Key) (int, bool) {
	lower := 0
	upper := len(n.entries) - 1

	var cursor int
	for lower <= upper {
		cursor = (upper + lower) / 2
		cmp := n.entries[cursor].key

		if cmp == key {
			return cursor, true
		} else if key > cmp {
			lower = cursor + 1
		} else if key < cmp {
			upper = cursor - 1
		}
	}

	return lower, false
}

// dumb implementation of http://eecs.csuohio.edu/~sschung/cis611/B+Trees.pdf
func (p *node) splitLeaf(n, sibling *node, i int) error {

	sibling.next = n.next
	sibling.prev = n.id
	n.next = sibling.id

	sibling.entries = make([]entry, p.degree-1)
	copy(sibling.entries, n.entries[p.degree:])
	n.entries = n.entries[:p.degree]

	err := p.insertChildAt(i+1, sibling)

	if err != nil {
		return err
	}
	err = p.insertEntryAt(i, sibling.entries[0])
	if err != nil {
		return err
	}

	return nil

}
