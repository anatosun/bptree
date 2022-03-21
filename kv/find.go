package kv

func (b *Bplustree) findLeaf(key Key) (*node, error) {

	at := 0
	for at < (b.root.num - 1) {
		at++
		if key < b.root.entries[at].key {
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
