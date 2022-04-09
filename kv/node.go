package kv

import (
	"encoding/binary"
	"fmt"
)

const pageSize = 4 * 1024 // 4KB
type node struct {
	id       uint64
	dirty    bool
	degree   uint8
	next     uint64
	prev     uint64
	children []uint64
	entries  []entry
}

func newNode(id uint64, degree uint8) *node {
	return &node{id: id, dirty: true, entries: make([]entry, 0, degree), degree: degree, children: make([]uint64, 0, degree)}
}

func (n *node) insertChildAt(at int, child *node) error {
	prior_size := len(n.children)
	n.dirty = true
	n.children = append(n.children[0:at], append([]uint64{child.id}, n.children[at:]...)...)
	current_size := len(n.children)

	if prior_size+1 != current_size {
		return &InsertionError{Type: "child", Value: child, Size: current_size, Position: at, Capacity: cap(n.children)}
	}

	if len(n.entries) > ((2 * int(n.degree)) - 1) {

		return &OverflowError{Type: "child", Max: ((2 * int(n.degree)) - 1), Actual: current_size}
	}

	return nil
}

func (n *node) full() bool {

	return len(n.entries) == ((2 * int(n.degree)) - 1)

}

// dumb implementation of http://eecs.csuohio.edu/~sschung/cis611/B+Trees.pdf
func (p *node) splitNode(n, sibling *node, i int) error {

	parentKey := n.entries[p.degree-1]

	sibling.entries = make([]entry, p.degree-1)
	copy(sibling.entries, n.entries[:p.degree])
	n.entries = n.entries[p.degree:]

	sibling.children = make([]uint64, p.degree)
	copy(sibling.children, n.children[:p.degree])
	n.children = n.children[p.degree:]

	err := p.insertChildAt(i, sibling)
	if err != nil {
		return err
	}

	err = p.insertEntryAt(i, parentKey)
	if err != nil {
		return err
	}

	return nil

}

func (p *node) split(n, sibling *node, i int) error {
	p.dirty = true
	n.dirty = true
	sibling.dirty = true

	if n.isLeaf() {
		return p.splitLeaf(n, sibling, i)
	}

	return p.splitNode(n, sibling, i)
}

func (n *node) marshal() ([]byte, error) {
	capacity := pageSize // 4KB
	buf := make([]byte, capacity)

	bin := binary.LittleEndian
	bin.PutUint64(buf[0:8], n.id)
	buf[8] = n.degree               // 8th byte
	buf[9] = byte(len(n.entries))   // 9th byte
	buf[10] = byte(len(n.children)) // 10th byte (will be 0 for leaf)
	bin.PutUint64(buf[11:19], n.next)
	bin.PutUint64(buf[19:27], n.prev)
	cursor := 27
	for _, e := range n.entries {
		eb, err := e.MarshalEntry()
		if err != nil {
			return nil, err
		}
		buf = append(buf[:cursor], eb...)
		cursor += entrySize
		if cursor > capacity {
			return buf, &BufferOverflowError{Max: capacity, Cursor: cursor}
		}
	}

	for _, c := range n.children {
		bin.PutUint64(buf[cursor:cursor+8], c)
		cursor += 8
		if cursor > capacity {
			return buf, &BufferOverflowError{Max: capacity, Cursor: cursor}
		}
	}

	if len(buf) > pageSize {
		return buf, &InvalidSizeError{Got: len(buf), From: "10 bytes", To: fmt.Sprintf("%d bytes", pageSize)}
	}

	return buf, nil
}

func (n *node) unmarshal(data []byte) error {

	if len(data) > pageSize {
		return &InvalidSizeError{Got: len(data), From: "10 bytes", To: fmt.Sprintf("%d bytes", pageSize)}
	}
	n.dirty = true
	bin := binary.LittleEndian
	n.id = bin.Uint64(data[0:8])
	n.degree = uint8(data[8])
	numberOfEntries := int(data[9])
	n.entries = make([]entry, 0, numberOfEntries)
	numberOfChildren := int(data[10])
	n.children = make([]uint64, 0, numberOfChildren)
	n.next = bin.Uint64(data[11:19])
	n.prev = bin.Uint64(data[19:27])
	cursor := 27
	for i := 0; i < numberOfEntries; i++ {
		e := entry{}
		err := e.UnmarshalEntry(data[cursor : cursor+entrySize])
		if err != nil {
			return err
		}
		n.entries = append(n.entries, e)
		cursor += entrySize
	}

	for i := 0; i < numberOfChildren; i++ {
		n.children = append(n.children, bin.Uint64(data[cursor:cursor+8]))
		cursor += 8
	}

	return nil
}
