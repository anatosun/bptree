package kv

import (
	"math"
	"testing"
)

func TestMarshalUnmarshalEntry(t *testing.T) {
	k := Key(math.MaxUint64)
	v := Value([10]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	e := entry{key: k, value: v}
	data, err := e.MarshalEntry()
	if err != nil {
		t.Errorf("while marshaling: %v", err)
		t.FailNow()
	}
	u := entry{key: 58593, value: Value([10]byte{0, 0, 0, 81, 0, 5, 35, 0, 0, 0})} // filling with dummy values
	err = u.UnmarshalEntry(data)
	if err != nil {
		t.Errorf("while unmarshaling: %v", err)
		t.FailNow()
	}
	if u.key != k {
		t.Errorf("expected %d, got %d", k, u.key)
		t.FailNow()
	}
	if u.value != v {
		t.Errorf("expected %d, got %d", v, u.value)
		t.FailNow()
	}
}
