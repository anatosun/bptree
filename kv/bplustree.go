package kv

import (
	"fmt"
)

type Bplustree struct {
	size   int
	degree int
	leaf   *node
	root   *node
}

func New(degree int) *Bplustree {
	return &Bplustree{size: 0, degree: degree, root: nil}
}

func (b *Bplustree) Len() int {
	return b.size
}

func (b *Bplustree) Empty() bool {
	return b.size == 0
}

func (b *Bplustree) Insert(key Key, value Value) error {

	// log.Printf("inserting key %d", key)
	e := &entry{key, value}
	if b.Empty() {
		b.leaf = newLeaf(b.degree)
		err := b.leaf.insertEntry(e)
		if err != nil {
			return err
		}
		b.size++
		return nil
	}
	var leaf *node
	var err error
	if b.root == nil {
		leaf = b.leaf
	} else {
		leaf, err = b.findLeaf(key)
		if err != nil {
			return err
		}
	}

	err = leaf.insertEntry(e)
	// if the insertion failed that means it was full
	if err != nil {
		err = leaf.stuffEntry(e)
		if err != nil {
			return err
		}
		mid := leaf.median()
		halfEntries := leaf.splitEntries(mid)

		if leaf.parent == nil {
			entries := make([]*entry, 0, b.degree)
			entries = append(entries, halfEntries[0])
			parent := newNode(b.degree)
			parent.entries = entries
			leaf.parent = parent
			parent.appendChild(leaf)
		} else {
			p := halfEntries[0]
			leaf.parent.insertEntry(p)
		}

		lf := newLeaf(b.degree)
		lf.parent = leaf.parent
		index, err := leaf.parent.findIndexOfChild(leaf)
		index++
		if err != nil {
			return err
		}
		leaf.parent.insertChildAt(lf, index)
		lf.right = leaf.right
		if lf.right != nil {
			lf.right.left = lf
		}
		leaf.right = lf
		lf.left = leaf

		if b.root == nil {
			b.root = leaf.parent
		} else {
			node := leaf.parent

			for node != nil {
				if node.overfull() {
					b.splitNode(node)
				} else {
					break
				}
				node = node.parent
			}
		}
	}
	return nil

}

func (b *Bplustree) Remove(key Key) (*Value, error) {
	b.size--
	return nil, nil

}

func (b *Bplustree) Search(key Key) (*Value, error) {

	if b.Empty() {
		return nil, fmt.Errorf("tree is empty")
	}

	var err error
	var leaf *node
	if b.root == nil {
		leaf = b.leaf
	} else {
		leaf, err = b.findLeaf(key)
		if err != nil {
			return nil, err
		}
	}

	entry := leaf.binarySearch(key)
	fmt.Println(key)
	printEntries(leaf.entries)
	if entry == nil {
		err = fmt.Errorf("could not find key in leaf")
		return nil, err
	}

	return &entry.value, nil

}

func (b *Bplustree) Min() (*Key, error) {
	return nil, nil
}

func (b *Bplustree) Max() (*Key, error) {
	return nil, nil
}

func (b *Bplustree) Scan(key1, key2 Key) ([]*Value, error) {
	return nil, nil

}

func (b *Bplustree) Print() {
	child := b.leaf
	for child != nil {
		for _, e := range child.children {
			e.print()
		}
		child = child.right
	}

}
