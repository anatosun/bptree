package kv

import (
	"math/rand"
	"sort"
	"testing"
)

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
		node.insertChildAt(i, newNode(children[i], degree).id)
	}

	data, err := node.MarshalBinary()
	if err != nil {
		t.Errorf("while marshaling: %v", err)
		t.FailNow()
	}

	u := newNode(546, 78) // let's initialise it we dummy values
	u.next = 480
	u.prev = 128
	err = u.UnmarshalBinary(data)

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
