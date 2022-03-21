package kv

func (b *Bplustree) splitNode(n *node) error {

	parent := n.parent
	median := n.median()
	medianEntry := n.entries[median]
	halfEntries := n.splitEntries(median)
	halfChildren := n.splitChildren(median)

	s, err := n.nextChildAt()
	n.size = s
	if err != nil {
		return err
	}

	sibling := newNode(b.degree)
	sibling.entries = halfEntries
	sibling.children = halfChildren
	for _, c := range halfChildren {
		if c != nil {
			c.parent = sibling
		}
	}
	sibling.right = n.right

	if sibling.right != nil {
		sibling.right.left = sibling
	}

	n.right = sibling
	sibling.left = n

	// if the node is root
	if parent == nil {
		entries := make([]*entry, b.degree)
		entries[0] = medianEntry
		newRoot := newNode(b.degree)
		newRoot.entries = entries
		newRoot.appendChild(n)
		newRoot.appendChild(sibling)
		b.root = newRoot
		n.parent = newRoot
		sibling.parent = newRoot
	} else {
		parent.insertEntry(medianEntry)

		childAt, err := parent.findIndexOfChild(n)
		if err != nil {
			return err
		}
		childAt++
		parent.insertChild(sibling, childAt)
		sibling.parent = parent
	}

	return nil
}
