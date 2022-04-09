package kv

import (
	"math/rand"
	"sort"
	"testing"
)

func TestMarshalUnmarshalLeaf(t *testing.T) {
	degree := uint8(rand.Int() % 70)
	leaf := newNode(0, degree)
	leaf.next = 48307593
	leaf.prev = 485830
	offset := 29

	for i := 0; !leaf.full(); i++ {
		entry := entry{key: Key(i + offset), value: Value([10]byte{byte(i + offset)})}
		leaf.insertEntryAt(i, entry)
	}

	data, err := leaf.marshal()
	if err != nil {
		t.Errorf("while marshaling: %v", err)
		t.FailNow()
	}

	u := newNode(54, 86) // let's initialise it we dummy values
	u.next = 480
	u.prev = 128
	err = u.unmarshal(data)

	if err != nil {
		t.Errorf("while unmarshaling: %v", err)
		t.FailNow()
	}

	if u.id != leaf.id {
		t.Errorf("expected %d, got %d", leaf.id, u.id)
		t.FailNow()
	}

	if u.degree != leaf.degree {
		t.Errorf("expected %d, got %d", leaf.degree, u.degree)
		t.FailNow()
	}

	if len(u.entries) != len(leaf.entries) {
		t.Errorf("expected %d, got %d", len(leaf.entries), len(u.entries))
		t.FailNow()
	}

	if len(u.children) != len(leaf.children) {
		t.Errorf("expected %d, got %d", len(leaf.children), len(u.children))
		t.FailNow()
	}

	if u.next != leaf.next {
		t.Errorf("expected %d, got %d", leaf.next, u.next)
		t.FailNow()
	}

	if u.prev != leaf.prev {
		t.Errorf("expected %d, got %d", leaf.prev, u.prev)
		t.FailNow()
	}

	for i, e := range u.entries {
		if e.key != leaf.entries[i].key {
			t.Errorf("expected %d, got %d", leaf.entries[i].key, e.key)
			t.FailNow()
		}
		if e.value != leaf.entries[i].value {
			t.Errorf("expected %d, got %d", leaf.entries[i].value, e.value)
			t.FailNow()
		}
	}

}

func TestMarshalUnmarshalNode(t *testing.T) {
	degree := uint8(rand.Int() % 70)
	node := newNode(0, degree)
	node.next = 4830759
	node.prev = 48583
	offset := 29
	children := make([]uint64, degree*2)
	sort.Slice(children, func(i, j int) bool {
		return children[i] < children[j]
	})

	for i := 0; !node.full(); i++ {
		entry := entry{key: Key(i + offset), value: Value([10]byte{byte(i + offset)})}
		node.insertEntryAt(i, entry)
		node.insertChildAt(i, newNode(children[i], degree))
	}

	data, err := node.marshal()
	if err != nil {
		t.Errorf("while marshaling: %v", err)
		t.FailNow()
	}

	u := newNode(546, 78) // let's initialise it we dummy values
	u.next = 480
	u.prev = 128
	err = u.unmarshal(data)

	if err != nil {
		t.Errorf("while unmarshaling: %v", err)
		t.FailNow()
	}

	if u.id != node.id {
		t.Errorf("expected %d, got %d", node.id, u.id)
		t.FailNow()
	}

	if u.degree != node.degree {
		t.Errorf("expected %d, got %d", node.degree, u.degree)
		t.FailNow()
	}

	if len(u.entries) != len(node.entries) {
		t.Errorf("expected %d, got %d", len(node.entries), len(u.entries))
		t.FailNow()
	}

	if len(u.children) != len(node.children) {
		t.Errorf("expected %d, got %d", len(node.children), len(u.children))
		t.FailNow()
	}

	if u.next != node.next {
		t.Errorf("expected %d, got %d", node.next, u.next)
		t.FailNow()
	}

	if u.prev != node.prev {
		t.Errorf("expected %d, got %d", node.prev, u.prev)
		t.FailNow()
	}

	for i, e := range u.entries {
		if e.key != node.entries[i].key {
			t.Errorf("expected %d, got %d", node.entries[i].key, e.key)
			t.FailNow()
		}
		if e.value != node.entries[i].value {
			t.Errorf("expected %d, got %d", node.entries[i].value, e.value)
			t.FailNow()
		}
	}

	for i, child := range u.children {

		if child != node.children[i] {
			t.Errorf("expected %d, got %d", node.children[i], child)
			t.FailNow()
		}
	}

}
