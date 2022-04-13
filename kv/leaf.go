package kv

func (n *node) isLeaf() bool {
	return len(n.children) == 0
}

func (n *node) insertEntryAt(at int, e entry) error {
	prior_size := len(n.entries)
	n.entries = append(n.entries, entry{})
	copy(n.entries[at+1:], n.entries[at:])
	n.entries[at] = e
	current_size := len(n.entries)

	if prior_size+1 != current_size {
		return &InsertionError{Type: "child", Value: e, Size: current_size, Position: at, Capacity: cap(n.entries)}
	}

	// if len(n.entries) > ((2 * int(n.degree)) - 1) {
	// 	err := &OverflowError{Type: "entry", Max: ((2 * int(n.degree)) - 1), Actual: current_size}
	// 	panic(err)
	// 	return err
	// }

	return nil
}

func (n *node) update(at int, value Value) error {
	if n.entries[at].value != value {
		n.entries[at].value = value
	}
	return nil
}

func (n *node) deleteEntryAt(at int) (entry, error) {
	prior_size := len(n.entries)
	entry := n.entries[at]
	n.entries = append(n.entries[0:at], n.entries[at+1:]...)
	current_size := len(n.entries)

	if prior_size != current_size+1 {
		return entry, &DeletionError{Type: "child", Value: entry, Size: current_size, Position: at, Capacity: cap(n.entries)}
	}

	// if len(n.entries) > ((2 * int(n.degree)) - 1) {

	// 	return entry, &OverflowError{Type: "entry", Max: ((2 * int(n.degree)) - 1), Actual: current_size}
	// }

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
