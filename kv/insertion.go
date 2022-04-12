package kv

func (bpt *BPlusTree) insert(e entry) (bool, error) {
	if bpt.root.full() {

		nodes, err := bpt.allocate(2)
		if err != nil {
			return false, err
		}

		newRoot := nodes[0]
		rightSibling := nodes[1]
		oldRoot := bpt.root

		newRoot.children = append(newRoot.children, oldRoot.id)
		bpt.root = newRoot
		bpt.meta.root = uint32(newRoot.id)

		if err := newRoot.split(oldRoot, rightSibling, 0); err != nil {
			return false, err
		}
	}

	return bpt.path(bpt.root, e)
}

func (bpt *BPlusTree) path(n *node, e entry) (bool, error) {
	if n.isLeaf() {
		return bpt.insertLeaf(n, e)
	}

	return bpt.insertInternal(n, e)
}

func (bpt *BPlusTree) insertLeaf(n *node, e entry) (bool, error) {
	at, found := n.search(e.key)

	if found {
		n.update(at, e.value)
		return false, nil
	}

	err := n.insertEntryAt(at, e)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (bpt *BPlusTree) insertInternal(n *node, e entry) (bool, error) {
	at, found := n.search(e.key)
	if found {
		at++
	}

	child, err := bpt.nodeRef(n.children[at]) //TODO: After no longer in use, unpin
	if err != nil {
		return false, err
	}

	if child.full() {
		nodes, err := bpt.allocate(1)
		if err != nil {
			return false, err
		}
		sibling := nodes[0]
		if err := n.split(child, sibling, at); err != nil {
			return false, err
		}

		if e.key >= n.entries[at].key {
			child, err = bpt.nodeRef(n.children[at+1]) //TODO: After no longer in use, unpin
			if err != nil {
				return false, err
			}
		}
	}

	return bpt.path(child, e)
}
