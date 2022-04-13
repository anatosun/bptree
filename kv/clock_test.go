package kv

import (
	"testing"
)

func TestClock(t *testing.T) {
	clock := NewClockPolicy(15)

	for i := 1; i < 11; i++ {
		clock.Unpin(NodeID(i)) //add elements to clock 1..10
	}

	//clock.clock.Print()

	AssertEqual(t, clock.Size(), 10)

	victim, _ := (*clock).Victim()
	AssertEqual(t, victim, NodeID(1))

	victim, _ = (*clock).Victim()
	AssertEqual(t, victim, NodeID(2))

	AssertEqual(t, clock.Pin(NodeID(3)), true) // remove from clock
	AssertEqual(t, clock.Pin(NodeID(4)), true)
	AssertEqual(t, clock.Pin(NodeID(99)), false) // doesn't exist, not removable i.e. returns false

	AssertEqual(t, clock.Size(), 6)

	clock.Unpin(NodeID(4)) // removed before, add again

	//clock.clock.Print()
	victim, _ = (*clock).Victim()
	AssertEqual(t, victim, NodeID(5))

	victim, _ = (*clock).Victim()
	AssertEqual(t, victim, NodeID(6))

	victim, _ = (*clock).Victim()
	AssertEqual(t, victim, NodeID(7))

	//clock.clock.Print()
}
