package kv

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
