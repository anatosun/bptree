package kv


// Convert node to nodeID, fetch it using bpm

import(
	"fmt"
)

func (bpt *BPlusTree) insert(e entry) (bool, error) {

	if bpt.root.full() {

		nodeID_1, err_allocation_1 := bpt.allocate2()
		nodeID_2, err_allocation_2 := bpt.allocate2()

		if err_allocation_1 != nil {
			return false, err_allocation_1
		}
		if err_allocation_2 != nil {
			return false, err_allocation_2
		}

		n1, err_fetching_1 := bpt.bpm.FetchNode(*nodeID_1)
		n2, err_fetching_2 := bpt.bpm.FetchNode(*nodeID_2)

		if err_fetching_1 != nil {
			return false, err_fetching_1
		}
		if err_fetching_2 != nil {
			return false, err_fetching_2
		}

		newRoot := n1
		rightSibling := n2
		oldRoot := bpt.root

		newRoot.children = append(newRoot.children, oldRoot.id)
		bpt.root = newRoot
		bpt.meta.root = uint32(newRoot.id)

		if err := newRoot.split(oldRoot, rightSibling, 0); err != nil {
			return false, err
		}

		bpt.bpm.UnpinNode(*nodeID_1)
		bpt.bpm.UnpinNode(*nodeID_2)

	}

	return bpt.path(bpt.root.getID(), e)
}

func (bpt *BPlusTree) path(nodeID NodeID, e entry) (bool, error) {

	node, err := bpt.bpm.FetchNode(nodeID)
	if err != nil {
		bpt.bpm.UnpinNode(nodeID)
		return false, err
	}

	if node.isLeaf() {
		bpt.bpm.UnpinNode(nodeID)
		return bpt.insertLeaf(nodeID, e)
	}

	bpt.bpm.UnpinNode(nodeID)
	return bpt.insertInternal(nodeID, e)
}

func (bpt *BPlusTree) insertLeaf(nodeID NodeID, e entry) (bool, error) {

	node, err := bpt.bpm.FetchNode(nodeID)
	if err != nil { return false, err }

	at, found := node.search(e.key)

	if found {

		err := node.update(at, e.value)
		if err != nil {
			bpt.bpm.UnpinNode(nodeID)
			return false, err
		}

		bpt.bpm.UnpinNode(nodeID)
		return false, err //FX: Shouldn't this return true, nil?
	}

	err = node.insertEntryAt(at, e)
	if err != nil {
		bpt.bpm.UnpinNode(nodeID)
		return false, err
	}

	bpt.bpm.UnpinNode(nodeID)
	return true, err
}

func (bpt *BPlusTree) insertInternal(nodeID NodeID, e entry) (bool, error) {

	node, err := bpt.bpm.FetchNode(nodeID)
	if err != nil { 
		bpt.bpm.UnpinNode(nodeID)
		return false, err
	}


	at, found := node.search(e.key)
	if found {
		at++
	}

	childID := NodeID(node.children[at])
	child, err := bpt.bpm.FetchNode(childID)
	if err != nil { 
		bpt.bpm.UnpinNode(nodeID)
		bpt.bpm.UnpinNode(childID)
		return false, err
	}

	if child.full() {
		newNodeID, err := bpt.allocate2()
		if err != nil { return false, err }

		sibling, err := bpt.bpm.FetchNode(*newNodeID)

		if err != nil { 
			bpt.bpm.UnpinNode(nodeID)
			bpt.bpm.UnpinNode(childID)
			bpt.bpm.UnpinNode(*newNodeID)
			return false, err
		}


		if err := node.split(child, sibling, at); err != nil {
			bpt.bpm.UnpinNode(nodeID)
			bpt.bpm.UnpinNode(childID)
			bpt.bpm.UnpinNode(*newNodeID)
			return false, err
		}

		bpt.bpm.UnpinNode(*newNodeID)

		if e.key >= node.entries[at].key {
			newChildID := NodeID(node.children[at+1])
			child, err = bpt.bpm.FetchNode(newChildID)

			if err != nil { 
				bpt.bpm.UnpinNode(nodeID)
				bpt.bpm.UnpinNode(childID)
				bpt.bpm.UnpinNode(newChildID)
				bpt.bpm.UnpinNode(*newNodeID)
				return false, err
			}
			bpt.bpm.UnpinNode(newChildID)
		}
	}

	bpt.bpm.UnpinNode(nodeID)
	bpt.bpm.UnpinNode(childID)

	return bpt.path(child.getID(), e)
}


func dummyfmt3() {
	fmt.Println("x")
}
