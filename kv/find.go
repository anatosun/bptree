package kv

func (b *Bplustree) findLeaf(key Key) (*node, error) {

	at := 0
	for i, e := range b.root.entries {
		at = i
		if key < e.key {
			break
		}
	}
	child := b.root.children[at]
	if child.isLeaf {
		return child, nil
	} else {
		return child.findLeaf(key)
	}
}
