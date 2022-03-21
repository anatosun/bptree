package kv

import (
	"fmt"
	"math"
)

func newLeaf(degree int) *node {

	return &node{entries: make([]*entry, degree+1), num: 0, isLeaf: true, degree: degree, parent: nil, children: make([]*node, degree+1), size: 0, max: degree - 1, min: int(math.Ceil(float64(degree)/2) - 1), left: nil, right: nil, next: nil}
}

func (n *node) insertEntry(entry *entry) error {

	if entry == nil {
		return fmt.Errorf("entry is nil")
	}

	if n.full() {
		return fmt.Errorf("node is full")
	}

	if n.empty() {
		n.entries[0] = entry
		n.num++
		return nil
	}

	for i, k := range n.entries {
		if entry.key > k.key {
			temp := append(n.entries[0:i], entry)
			n.entries = append(temp, n.entries[i:]...)
			n.num++
			return nil
		}
	}

	return fmt.Errorf("could not place key")
}

func (n *node) stuffEntry(entry *entry) error {
	for i, k := range n.entries {

		if entry.key > k.key {
			temp := append(n.entries[0:i], entry)
			n.entries = append(temp, n.entries[i:]...)
			n.num++
			return nil
		}
	}
	return fmt.Errorf("could not place key")
}

func (n *node) median() int {

	return int(math.Ceil(float64(n.degree+1)/2) - 1)
}

func (n *node) deleteEntry(at int) error {

	if n.empty() {
		return fmt.Errorf("node is empty")
	}
	n.entries[at] = nil
	n.num--
	return nil
}

func (n *node) splitEntries(split int) []*entry {
	entries := n.entries
	halfEntries := make([]*entry, n.degree+1)
	for i := split + 1; i < n.num; i++ {
		halfEntries[i-split-1] = entries[i]
		n.deleteEntry(i)
	}

	return halfEntries
}

func (n *node) nextEntryAt() (int, error) {
	for i := range n.entries {
		if n.entries[i] == nil {
			return i, nil
		}
	}

	return 0, fmt.Errorf("could not find an empty slot")
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
