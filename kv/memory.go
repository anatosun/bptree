package kv

import "fmt"

//FX: suggestion: rename to findSequentialFreeID?
func findSequentialFreeSpace(free []uint64, n int) (id uint64, remaining []uint64, err error) {
	if len(free) <= n {
		return 0, free, fmt.Errorf("not enough pages in free list")
	} else if n == 1 {
		return free[0], free[1:], nil
	}

	i, j := 0, 0
	for ; i < len(free); i++ {
		j = i + (n - 1)
		if j < len(free) && free[j] == free[i]+uint64((n-1)) {
			break
		}
	}

	if i >= len(free) || j >= len(free) {
		return 0, free, fmt.Errorf("not enough pages in free list")
	}

	id = free[i]
	free = append(free[:i], free[j+1:]...)
	return id, free, nil
}

func (tree *BPlusTree) nodeRef(id uint64) (*node, error) {
	// Bridge between buffer pool and bplustree
	//fmt.Printf("Give me node wiht id=%d, length=%d\n", id, len(tree.nodes))
	n, found := tree.nodes[id]
	if found {
		return n, nil
	}

	n = newNode(id, uint8(tree.degree))
	// if err := tree.pager.Unmarshal(id, n); err != nil {
	// 	return nil, err
	// }
	// FETCH PAGE WITH ID
	n.dirty = false
	tree.nodes[n.id] = n

	return n, nil
}

func (tree *BPlusTree) allocate(n int) ([]*node, error) {

	//First, let's get an free ID from this
	pid, rem, err := findSequentialFreeSpace(tree.meta.free, n)
	tree.meta.free = rem

	if err != nil {
		// SequentialFreeSpace is exhausted, get from here
		// var err error
		// pid, err = tree.pager.Alloc(n)
		// if err != nil {
		// 	return nil, err
		// }
		return nil, fmt.Errorf("not yet implemented")
	}

	nodes := make([]*node, n)
	for i := 0; i < n; i++ {
		//fmt.Printf("creating node with id=%d\n", pid)
		n := newNode(pid, tree.degree)
		tree.nodes[pid] = n
		nodes[i] = n
		pid++

	}

	for i := 0; i < n; i++ {
		//node := tree.bpm.GetNewNode(3)
		//fmt.Printf("node=%v\n",node)
	}


	return nodes, nil
}

func (tree *BPlusTree) allocate2() (*NodeID, error) {
	id, err := tree.bpm.GetNewNode(tree.degree)
	if err != nil {
		return nil, err
	}

	//Current fix until everything is implemented
	tree.nodes[uint64(*id)], _ = tree.bpm.FetchNode(*id)
	tree.bpm.UnpinNode(*id)

	return id, nil
}

// write queries the bufferpool manager to write the node to disk

// uncomment this function once the bufferpool implements the proper methods
// func (bpt *BPlusTree) write() error {

// 	for _, node := range bpt.nodes {
// 		if node.dirty {

// 			if err := bpt.bufferpool.Marshal(node); err != nil {
// 				return err
// 			}

// 			node.dirty = false
// 		}
// 	}
// 	if bpt.meta.dirty {

// 		if err := bpt.bufferpool.Marshal(bpt.meta); err != nil {
// 			return err
// 		}

// 		bpt.meta.dirty = false

// 	}

// 	return nil
// }
