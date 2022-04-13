package kv

func (bpt *BPlusTree) insert(e entry) (bool, error) {
	if bpt.root.full(int(bpt.order), int(bpt.fanout)) {

		nodeID_1, err_allocation_1 := bpt.allocate()
		nodeID_2, err_allocation_2 := bpt.allocate()

		if err_allocation_1 != nil {
			return false, err_allocation_1
		}
		if err_allocation_2 != nil {
			return false, err_allocation_2
		}

		n1, err_fetching_1 := bpt.bpm.FetchNode(nodeID_1)
		n2, err_fetching_2 := bpt.bpm.FetchNode(nodeID_2)

		if err_fetching_1 != nil {
			bpt.bpm.UnpinNode(nodeID_1, false)
			return false, err_fetching_1
		}
		if err_fetching_2 != nil {
			bpt.bpm.UnpinNode(nodeID_2, false)
			return false, err_fetching_2
		}

		newRoot := n1
		rightSibling := n2
		oldRoot := bpt.root

		newRoot.children = append(newRoot.children, oldRoot.id)
		bpt.root = newRoot
		bpt.meta.root = newRoot.id

		if err := bpt.split(newRoot.getID(), oldRoot.getID(), rightSibling.getID(), 0); err != nil {
			return false, err
		}

		bpt.bpm.UnpinNode(nodeID_1, true)
		bpt.bpm.UnpinNode(nodeID_2, true)
	}

	return bpt.path(bpt.root.getID(), e)
}

func (bpt *BPlusTree) path(nodeID NodeID, e entry) (bool, error) {

	node, err := bpt.bpm.FetchNode(nodeID)
	if err != nil {
		bpt.bpm.UnpinNode(nodeID, false)
		return false, err
	}

	if node.isLeaf() {
		bpt.bpm.UnpinNode(nodeID, false)
		return bpt.insertLeaf(nodeID, e)
	}

	bpt.bpm.UnpinNode(nodeID, false)
	return bpt.insertInternal(nodeID, e)
}

func (bpt *BPlusTree) insertLeaf(nodeID NodeID, e entry) (bool, error) {

	n, err := bpt.bpm.FetchNode(nodeID)
	if err != nil {
		bpt.bpm.UnpinNode(nodeID, false)
		return false, err
	}

	at, found := n.search(e.key)

	if found {
		err := n.update(at, e.value)
		if err != nil {
			bpt.bpm.UnpinNode(nodeID, true)
			return false, err
		}
		bpt.bpm.UnpinNode(nodeID, true)
		return false, err
	}

	err = n.insertEntryAt(at, e)
	if err != nil {
		bpt.bpm.UnpinNode(nodeID, true)
		return false, err
	}

	bpt.bpm.UnpinNode(nodeID, true)
	return true, err
}

func (bpt *BPlusTree) insertInternal(nodeID NodeID, e entry) (bool, error) {

	node, err := bpt.bpm.FetchNode(nodeID)
	if err != nil {
		bpt.bpm.UnpinNode(nodeID, false)
		return false, err
	}

	at, found := node.search(e.key)
	if found {
		at++
	}

	childID := NodeID(node.children[at])
	child, err := bpt.bpm.FetchNode(childID)

	if err != nil {
		bpt.bpm.UnpinNode(nodeID, false)
		bpt.bpm.UnpinNode(childID, false)
		return false, err
	}

	if child.full(int(bpt.order), int(bpt.fanout)) {

		newNodeID, err := bpt.allocate()
		if err != nil {
			return false, err
		}

		sibling, err := bpt.bpm.FetchNode(newNodeID)

		if err != nil {
			bpt.bpm.UnpinNode(nodeID, true)
			bpt.bpm.UnpinNode(childID, true)
			bpt.bpm.UnpinNode(newNodeID, true)
			return false, err
		}

		if err := bpt.split(node.getID(), child.getID(), sibling.getID(), at); err != nil {
			bpt.bpm.UnpinNode(nodeID, true)
			bpt.bpm.UnpinNode(childID, true)
			bpt.bpm.UnpinNode(newNodeID, true)
			return false, err
		}

		bpt.bpm.UnpinNode(newNodeID, true)

		if e.key >= node.entries[at].key {

			newChildID := NodeID(node.children[at+1])
			child, err = bpt.bpm.FetchNode(newChildID)

			if err != nil {
				bpt.bpm.UnpinNode(nodeID, true)
				bpt.bpm.UnpinNode(childID, true)
				bpt.bpm.UnpinNode(newChildID, false)
				bpt.bpm.UnpinNode(newNodeID, true)
				return false, err
			}
			bpt.bpm.UnpinNode(newChildID, true)

		}
	}

	bpt.bpm.UnpinNode(nodeID, true)
	bpt.bpm.UnpinNode(childID, true)

	return bpt.path(child.getID(), e)
}
