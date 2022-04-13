package kv


// Convert node to nodeID, fetch it using bpm

import(
	"fmt"
)

func (bpt *BPlusTree) insert(e entry) (bool, error) {

	if bpt.root.full() {

		nodeID_1, err1 := bpt.allocate2()
		nodeID_2, err2 := bpt.allocate2()

		// if err != nil {
		if err1 != nil {
			return false, err1
		}
		if err2 != nil {
			return false, err2
		}

		n1, err1 := bpt.bpm.FetchNode(*nodeID_1)
		n2, err2 := bpt.bpm.FetchNode(*nodeID_2)

		if err1 != nil {
			return false, err1
		}
		if err2 != nil {
			return false, err2
		}

		// nodes, err := bpt.allocate(2)
		// if err != nil {
		// 	return false, err
		// }

		// fmt.Printf("node1=%v, node2=%v\n", n1, n2)
		// fmt.Printf("node3=%v, node4=%v\n", nodes[0], nodes[1])

		// newRoot := nodes[0]
		// rightSibling := nodes[1]
		newRoot := n1
		rightSibling := n2
		oldRoot := bpt.root

		newRoot.children = append(newRoot.children, oldRoot.id)
		bpt.root = newRoot
		bpt.meta.root = uint32(newRoot.id)

		if err := newRoot.split(oldRoot, rightSibling, 0); err != nil {
			return false, err
		}

		// Unpin nodes, since no longer in use
		// for _, node := range nodes {
		// //	fmt.Println("Unpin node")
		// 	bpt.bpm.UnpinNode(NodeID(node.getID()))
		// }
		bpt.bpm.UnpinNode(*nodeID_1)
		bpt.bpm.UnpinNode(*nodeID_2)

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
			// bpt.bpm.UnpinNode(NodeID(n.id))
			return false, err
		}
		// err = bpt.bpm.UnpinNode(NodeID(n.id))
		return false, err
	}

	err := n.insertEntryAt(at, e)
	if err != nil {
		// attempt to unpin node before returning the error
		// bpt.bpm.UnpinNode(NodeID(n.id))
		return false, err
	}
	// unpin the node when the insertion has take place
	// err = bpt.bpm.UnpinNode(NodeID(n.id))
	return true, err
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
		// nodes, err := bpt.allocate(1)
		nodeID, err := bpt.allocate2()
		if err != nil {
			return false, err
		}
		// sibling := nodes[0]
		sibling, err := bpt.bpm.FetchNode(*nodeID)
		if err != nil {
			return false, err
		}

		if err := n.split(child, sibling, at); err != nil {
			return false, err
		}
		bpt.bpm.UnpinNode(*nodeID)

		if e.key >= n.entries[at].key {
			child, err = bpt.nodeRef(n.children[at+1]) //TODO: After no longer in use, unpin
			if err != nil {
				return false, err
			}
		}
	}

	// err = bpt.bpm.UnpinNode(NodeID(node.id))
	// if err != nil {
	// 	return false, err
	// }

	return bpt.path(child, e)
}


func dummyfmt3() {
	fmt.Println("x")
}
