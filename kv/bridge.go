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

func (tree *BPlusTree) allocate() (NodeID, error) {
	id, err := tree.bpm.GetNewNode()
	if err != nil {
		return 0, err
	}

	//puts it into the buffer pool and makes it save it to disk
	tree.nodes[uint64(id)], _ = tree.bpm.FetchNode(id)
	tree.bpm.UnpinNode(id, false)

	return id, nil
}
