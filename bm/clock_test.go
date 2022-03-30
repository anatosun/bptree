package bm

import (
	"testing"
)

func TestClock(t *testing.T) {
	clock := NewClockPolicy(15)

	for i := 1; i < 11; i++ {
		clock.Unpin(i) //add elements to clock 1..10
	}

	//clock.clock.Print()

	AssertEqual(t, clock.Size(), 10)
	AssertEqual(t, *clock.Victim(), 1)
	AssertEqual(t, *clock.Victim(), 2)

	AssertEqual(t, clock.Pin(3), true) // remove from clock
	AssertEqual(t, clock.Pin(4), true)
	AssertEqual(t, clock.Pin(99), false) // doesn't exist, not removable i.e. returns false

	AssertEqual(t, clock.Size(), 6)

	clock.Unpin(4) // removed before, add again

	//clock.clock.Print()

	AssertEqual(t, *clock.Victim(), 5)
	AssertEqual(t, *clock.Victim(), 6)

	AssertEqual(t, *clock.Victim(), 7)

	//clock.clock.Print()
}
