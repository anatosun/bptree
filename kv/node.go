package kv

import (
	"crypto/rand"
	"encoding/binary"
	"unsafe"
)

const pageSize = 4 * 1024 // 4KB

type NodeID uint64 //todo: replace id uint64 through NodeID throughout the whole code

type node struct {
	id         uint64
	dirty      bool
	next       uint64
	prev       uint64
	children   []uint64
	entries    []entry
	pinCounter uint64
}

func nodeHeaderLen() int {

	id := uint64(0)
	next := uint64(0)
	prev := uint64(0)
	numberOfEntries := uint8(0)
	numberOfChildren := uint8(0)
	return int(unsafe.Sizeof(id) +
		unsafe.Sizeof(next) +
		unsafe.Sizeof(prev) +
		unsafe.Sizeof(numberOfEntries) +
		unsafe.Sizeof(numberOfChildren))
}

func newNode(id uint64) *node {
	return &node{id: id, dirty: true}
}

func (n *node) insertChildAt(at int, child *node) error {
	prior_size := len(n.children)
	n.children = append(n.children, 0)
	copy(n.children[at+1:], n.children[at:])
	n.children[at] = child.id
	current_size := len(n.children)

	if prior_size+1 != current_size {
		return &InsertionError{Type: "child", Value: child, Size: current_size, Position: at, Capacity: cap(n.children)}
	}

	// if len(n.entries) > ((2 * int(n.degree)) - 1) {

	// 	return &OverflowError{Type: "child", Max: ((2 * int(n.degree)) - 1), Actual: current_size}
	// }

	return nil
}

func (n *node) full(l, c int) bool {
	if n.isLeaf() {
		return len(n.entries) == ((2 * l) - 1)
	}
	return len(n.entries) == ((2 * c) - 1)

}

// the two functions below implement both the BinaryMarshaler and the BinaryUnmarshaler interfaces
// refer to https://pkg.go.dev/encoding for more informations

func (n *node) MarshalBinary() ([]byte, error) {
	capacity := pageSize // 4KB
	buf := make([]byte, capacity)
	if _, err := rand.Read(buf); err != nil {
		return buf, err
	}
	bin := binary.LittleEndian
	bin.PutUint64(buf[0:8], n.id)
	buf[8] = byte(len(n.entries))  // 9th byte
	buf[9] = byte(len(n.children)) // 10th byte (will be 0 for leaf)
	bin.PutUint64(buf[10:18], n.next)
	bin.PutUint64(buf[18:26], n.prev)
	cursor := 26
	if cursor != int(nodeHeaderLen()) {
		return buf, &InvalidSizeError{Got: cursor, Should: int(nodeHeaderLen())}
	}
	for _, e := range n.entries {
		eb, err := e.MarshalEntry()
		if err != nil {
			return nil, err
		}
		for j := 0; j < len(eb); j++ {
			buf[cursor+j] = eb[j]
		}
		cursor += entryLen()
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

	if len(buf) != pageSize {
		return buf, &InvalidSizeError{Got: len(buf), Should: pageSize}
	}

	return buf, nil
}

func (n *node) UnmarshalBinary(data []byte) error {

	if len(data) > pageSize {
		return &InvalidSizeError{Got: len(data), Should: pageSize}
	}
	n.dirty = true
	bin := binary.LittleEndian
	n.id = bin.Uint64(data[0:8])
	numberOfEntries := int(data[8])
	n.entries = make([]entry, 0, numberOfEntries)
	numberOfChildren := int(data[9])
	n.children = make([]uint64, 0, numberOfChildren)
	n.next = bin.Uint64(data[10:18])
	n.prev = bin.Uint64(data[18:26])
	cursor := 26
	if cursor != int(nodeHeaderLen()) {
		return &InvalidSizeError{Got: cursor, Should: int(nodeHeaderLen())}
	}
	for i := 0; i < numberOfEntries; i++ {
		e := entry{}
		err := e.UnmarshalEntry(data[cursor : cursor+entryLen()])
		if err != nil {
			return err
		}
		n.entries = append(n.entries, e)
		cursor += entryLen()
	}

	for i := 0; i < numberOfChildren; i++ {
		n.children = append(n.children, bin.Uint64(data[cursor:cursor+8]))
		cursor += 8
	}

	return nil
}
