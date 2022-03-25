package kv

func (b *Bplustree) insert(e *entry) error {

	if b.root == nil {
		b.root = newNode(b.degree)
		b.root.appendEntry(e)
	}

	if b.root.full() {

		root := newNode(b.degree)
		right := newNode(b.degree)
		old := b.root
		root.children = append(root.children, old)
		b.root = root
		if err := b.split(root, old, right, 0); err != nil {
			return err
		}

	}

	return b.place(b.root, e)
}

func (b *Bplustree) place(n *node, e *entry) error {

	if n.isLeaf() {
		at, found := n.search(e.key)

		if found {
			return n.update(at, e.value)

		}

		return n.insertEntryAt(at, e)
	}

	return b.path(n, e)
}

func (b *Bplustree) path(n *node, e *entry) error {
	at, found := n.search(e.key)

	if found {
		at++
	}

	child := n.children[at]

	if child.full() {
		sib := newNode(b.degree)

		if err := b.split(n, child, sib, at); err != nil {
			return err
		}

		if e.key >= n.entries[at].key {
			child = n.children[at+1]

		}
	}

	return b.place(child, e)
}

func (b *Bplustree) split(p, n, sibling *node, i int) error {
	if n.isLeaf() {
		// split leaf node. use 'sibling' as the right node for 'n'.
		sibling.next = n.next
		sibling.prev = n
		n.next = sibling

		sibling.entries = make([]*entry, 0, b.degree-1)
		sibling.entries = append(sibling.entries, n.entries[b.degree:]...)
		n.entries = n.entries[:b.degree]
		p.insertChildAt(i+1, sibling)
		p.insertEntryAt(i, sibling.entries[0])
	} else {
		// split internal node. use 'sibling' as left node for 'n'.
		parentKey := n.entries[b.degree-1]

		sibling.entries = make([]*entry, 0, b.degree-1)
		sibling.entries = append(sibling.entries, n.entries[:b.degree]...)
		n.entries = n.entries[b.degree:]

		sibling.children = make([]*node, 0, b.degree)
		sibling.children = append(sibling.children, n.children[:b.degree]...)
		n.children = n.children[b.degree:]

		p.insertChildAt(i, sibling)
		p.insertEntryAt(i, parentKey)
	}

	return nil
}
