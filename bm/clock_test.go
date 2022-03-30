package bm

import (
	"testing"
)

func TestClock(t *testing.T) {
	clock := NewClockPolicy(10)
	clock.Unpin(1)
	clock.Unpin(2)
	clock.Unpin(3)
	clock.Unpin(4)
	clock.Unpin(5)
	clock.Unpin(6)
	clock.Unpin(1)

	ans := clock.Size()
	if ans != 6 {
		t.Errorf("got %d, want %d", ans, 6)
	}

	val := clock.Victim()

	if *val != 1 {
		t.Errorf("got %d, want %d", *val, 1)
	}

	val = clock.Victim()
	if *val != 2 {
		t.Errorf("got %d, want %d", *val, 2)
	}

	val = clock.Victim()
	if *val != 3 {
		t.Errorf("got %d, want %d", *val, 3)
	}

	clock.Pin(3)
	clock.Pin(4)
	ans = clock.Size()
	if ans != 2 {
		t.Errorf("got %d, want %d", ans, 2)
	}

	clock.Unpin(4)

	val = clock.Victim()
	if *val != 5 {
		t.Errorf("got %d, want %d", *val, 5)
	}

	val = clock.Victim()
	if *val != 6 {
		t.Errorf("got %d, want %d", *val, 6)
	}

	val = clock.Victim()
	if *val != 4 {
		t.Errorf("got %d, want %d", *val, 4)
	}
}
