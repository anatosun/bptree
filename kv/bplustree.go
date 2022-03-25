package kv

import (
	"fmt"
	"math"
)

type Bplustree struct {
	degree int
	root   *node
}

func New(degree int) *Bplustree {
	return &Bplustree{degree: degree, root: nil}
}

func (b *Bplustree) Insert(key Key, value Value) error {
	// log.Printf("inserting %d\n", key)
	entry := &entry{key, value}
	return b.insert(entry)
}

func (b *Bplustree) Remove(key Key) (*Value, error) {

	leaf, at, err := b.root.recursive(key)

	if err != nil {
		return nil, err
	}

	if at > len(leaf.entries)-1 {
		return nil, fmt.Errorf("index out of range %d:%d", at, len(leaf.entries))
	}

	entry, err := leaf.removeEntryAt(at)

	if err != nil {
		return nil, err
	}

	return &entry.value, nil

}

func (b *Bplustree) Search(key Key) (*Value, error) {

	if len(b.root.entries) == 0 {
		return nil, fmt.Errorf("key not found")
	}

	leaf, at, err := b.root.recursive(key)
	if err != nil {
		return nil, err
	}

	return &leaf.entries[at].value, nil

}

func (b *Bplustree) Empty() bool {

	return b.root.empty()
}

func (b *Bplustree) Min() (Key, error) {
	// we use uint64 so the min is 0, consider that if you change the type of key
	if b.Empty() {
		return 0, fmt.Errorf("tree is empty")
	}
	leaf, _, _ := b.root.recursive(0)

	if len(leaf.entries) == 0 {
		return 0, nil
	}

	return leaf.entries[0].key, nil
}

func (b *Bplustree) Max() (Key, error) {
	if b.Empty() {
		return math.MaxUint64, fmt.Errorf("tree is empty")
	}
	leaf, _, _ := b.root.recursive(math.MaxUint64)

	if len(leaf.entries) == 0 {
		return math.MaxUint64, nil
	}
	return leaf.entries[len(leaf.entries)-1].key, nil
}

func (b *Bplustree) Range(key1, key2 Key) ([]*Value, error) {

	return b.Scan(key1, func(key Key) bool {
		if key == key2 {
			return true
		} else {
			return false
		}
	})

}

func (b *Bplustree) Scan(start Key, fn func(key Key) bool) ([]*Value, error) {

	leaf, at, err := b.root.recursive(start)

	if err != nil {
		return nil, err
	}

	return leaf.scan(leaf, at, fn)

}

func (b *Bplustree) Len() int {

	min, err := b.Min()
	if err != nil {
		return 0
	}

	max, err := b.Max()
	if err != nil {
		return 0
	}

	values, err := b.Range(min, max)
	if err != nil {
		return -1
	}

	return len(values)
}
