package kv

import (
	"fmt"
	"math"
)

func newLeaf(degree int) *node {

	return &node{entries: make([]*entry, 0, degree), num: 0, isLeaf: true, degree: degree, parent: nil, children: make([]*node, 0, degree), size: 0, max: degree - 1, min: int(math.Ceil(float64(degree)/2) - 1), left: nil, right: nil, next: nil}
}

func (n *node) insertEntry(e *entry) error {

	if e == nil {
		return fmt.Errorf("entry is nil")
	}

	if n.full() {
		return fmt.Errorf("node is full")
	}

	if n.empty() {
		n.entries = append(n.entries, e)
		n.num++
		return nil
	}

	for i, k := range n.entries {

		if e.key < k.key {
			temp := append([]*entry{e}, n.entries[i:]...)
			n.entries = append(n.entries[0:i], temp...)
			n.num++
			return nil

		}
	}

	n.entries = append(n.entries, e)
	n.num++
	return nil
}

func (n *node) stuffEntry(e *entry) error {
	for i, k := range n.entries {

		if e.key < k.key {
			temp := append([]*entry{e}, n.entries[i:]...)
			n.entries = append(n.entries[0:i], temp...)
			n.num++
			return nil
		}
	}
	n.entries = append(n.entries, e)
	n.num++
	return nil
}

func (n *node) median() int {

	return int(math.Ceil(float64(n.degree+1)/2) - 1)
}

func (n *node) deleteEntryAt(at int) error {

	if n.empty() {
		return fmt.Errorf("node is empty")
	}
	n.entries = append(n.entries[:at], n.entries[at+1:]...)
	n.num--
	return nil
}

func (n *node) splitEntries(split int) []*entry {
	entries := n.entries
	halfEntries := make([]*entry, 0, n.degree)
	for i := split + 1; i < n.num; i++ {
		halfEntries = append(halfEntries, entries[i])
		n.deleteEntryAt(i)
	}

	return halfEntries
}

func (n *node) nextEntryAt() int {
	return n.num
}

func (n *node) binarySearch(key Key) *entry {

	lower := 0
	upper := n.num
	med := lower + int((upper-lower)/2)

	for lower >= upper {

		if key == n.entries[med].key {
			return n.entries[med]
		} else if key < n.entries[med].key {
			upper = med - 1
			med = lower + int((upper-lower)/2)
		} else if key > n.entries[med].key {
			lower = med + 1
			med = lower + int((upper-lower)/2)
		}
	}

	return nil

}
