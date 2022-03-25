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
		if err := b.split(old, right, root, 0); err != nil {
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

		if err := b.split(child, sib, n, at); err != nil {
			return err
		}

		if e.key >= n.entries[at].key {
			child = n.children[at+1]

		}
	}

	return b.place(child, e)
}

func (b *Bplustree) split(left, middle, right *node, i int) error {
	if left.isLeaf() {
		middle.next = left.next
		middle.prev = left
		left.next = middle

		middle.entries = make([]*entry, 0, b.degree-1)
		middle.entries = append(middle.entries, left.entries[b.degree:]...)
		left.entries = left.entries[:b.degree]

		right.insertChildAt(i+1, middle)
		right.insertEntryAt(i, middle.entries[0])

	} else {

		parentKey := left.entries[b.degree-1]
		middle.entries = make([]*entry, 0, b.degree-1)
		middle.entries = append(middle.entries, left.entries[:b.degree]...)
		left.entries = left.entries[b.degree:]

		middle.children = make([]*node, 0, b.degree)
		middle.children = append(middle.children, left.children[:b.degree]...)
		left.children = left.children[b.degree:]

		right.insertChildAt(i, middle)
		right.insertEntryAt(i, parentKey)

	}

	return nil
}
