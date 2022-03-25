package kv

import (
	"fmt"
)

func (n *node) isLeaf() bool {
	return len(n.children) == 0
}

// func (n *node) insertEntry(e *entry) error {

// 	for i, entry := range n.entries {
// 		if entry.key > e.key {
// 			return n.insertEntryAt(i, e)
// 		}
// 	}

// 	return n.appendEntry(e)
// }

func (n *node) insertEntryAt(at int, e *entry) error {
	n.entries = append(n.entries[0:at], append([]*entry{e}, n.entries[at:]...)...)
	return nil
}

func (n *node) update(at int, value Value) error {
	n.entries[at].value = value
	return nil
}

func (n *node) removeEntryAt(at int) (*entry, error) {
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

func (n *node) appendEntry(e *entry) error {
	n.entries = append(n.entries, e)
	return nil
}

func (n *node) scan(leaf *node, at int, fn func(key Key) bool) ([]*Value, error) {
	const INITIAL_SIZE int = 10
	values := make([]*Value, 0, INITIAL_SIZE)

	for leaf != nil && at > len(leaf.entries)-1 {
		at = 0
		leaf = leaf.next
	}

	if leaf == nil || at > len(leaf.entries)-1 {
		return values, nil
	}

	for !fn(leaf.entries[at].key) {

		values = append(values, &leaf.entries[at].value)
		if at < len(leaf.entries)-1 {
			at++
		} else {
			leaf = leaf.next
			at = 0
		}
	}

	if at > len(leaf.entries)-1 {
		return values, nil
	}

	values = append(values, &leaf.entries[at].value)

	return values, nil

}

func (left *node) splitLeaf(middle, right *node, at int) error {

	middle.next = left.next
	middle.prev = left
	left.next = middle

	middle.entries = make([]*entry, 0, left.degree-1)
	middle.entries = append(middle.entries, left.entries[left.degree:]...)
	left.entries = left.entries[:left.degree]

	right.insertChildAt(at+1, middle)
	right.insertEntryAt(at, middle.entries[0])

	return nil

}
