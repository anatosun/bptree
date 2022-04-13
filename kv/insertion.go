package kv

func (bpt *BPlusTree) insert(e entry) (bool, error) {
	if bpt.root.full(int(bpt.order), int(bpt.fanout)) {

		nid1, err := bpt.allocate()
		if err != nil {
			return false, err
		}
		nid2, err := bpt.allocate()
		if err != nil {
			return false, err
		}

		newRoot, err := bpt.bpm.FetchNode(NodeID(*nid1))
		if err != nil {
			return false, err
		}
		rightSibling, err := bpt.bpm.FetchNode(NodeID(*nid2))
		if err != nil {
			return false, err
		}
		oldRoot := bpt.root

		newRoot.children = append(newRoot.children, oldRoot.id)
		bpt.root = newRoot
		bpt.meta.root = newRoot.id

		if err := bpt.split(newRoot, oldRoot, rightSibling, 0); err != nil {
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
		err := n.update(at, e.value)
		if err != nil {
			// attempt to unpin node before returning the error
			bpt.bpm.UnpinNode(NodeID(n.id))
			return false, err
		}
		err = bpt.bpm.UnpinNode(NodeID(n.id))
		return false, err
	}

	err := n.insertEntryAt(at, e)
	if err != nil {
		// attempt to unpin node before returning the error
		bpt.bpm.UnpinNode(NodeID(n.id))
		return false, err
	}
	// unpin the node when the insertion has take place
	err = bpt.bpm.UnpinNode(NodeID(n.id))
	return true, err
}

func (bpt *BPlusTree) insertInternal(n *node, e entry) (bool, error) {
	at, found := n.search(e.key)
	if found {
		at++
	}

	child, err := bpt.bpm.FetchNode(NodeID(n.children[at])) //TODO: After no longer in use, unpin
	if err != nil {
		return false, err
	}

	if child.full(int(bpt.order), int(bpt.fanout)) {
		nid, err := bpt.allocate()
		if err != nil {
			return false, err
		}
		sibling, err := bpt.bpm.FetchNode(NodeID(*nid))
		if err != nil {
			return false, err
		}
		if err := bpt.split(n, child, sibling, at); err != nil {
			return false, err
		}

		if e.key >= n.entries[at].key {
			child, err = bpt.bpm.FetchNode(NodeID(n.children[at+1])) //TODO: After no longer in use, unpin
			if err != nil {
				bpt.bpm.UnpinNode(NodeID(child.id))
				return false, err
			}
		}
	}

	return bpt.path(child, e)
}
