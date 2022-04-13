package kv

import (
	"math"
	"testing"
)

func TestMarshalUnmarshalMetadata(t *testing.T) {

	meta := metadata{
		keySize:  math.MaxUint64,
		pageSize: 4096,
		size:     math.MaxUint32,
		root:     math.MaxUint64,
	}

	freeSpace := 10

	for i := 0; i < freeSpace; i++ {
		meta.free = append(meta.free, math.MaxUint64)
	}

	data, err := meta.MarshalBinary()
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	meta2 := metadata{}
	err = meta2.UnmarshalBinary(data)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	if meta.keySize != meta2.keySize {
		t.Logf("expected %d, got %d", meta.keySize, meta2.keySize)
		t.FailNow()
	}

	if meta.pageSize != meta2.pageSize {
		t.Logf("expected %d, got %d", meta.pageSize, meta2.pageSize)
		t.FailNow()
	}

	if meta.size != meta2.size {
		t.Logf("expected %d, got %d", meta.size, meta2.size)
		t.FailNow()
	}

	if meta.root != meta2.root {
		t.Logf("expected %d, got %d", meta.root, meta2.root)
		t.FailNow()
	}

	for i := range meta2.free {
		if meta2.free[i] != meta.free[i] {
			t.Logf("expected %d, got %d", meta.free[i], meta2.free[i])
			t.FailNow()
		} else {
			t.Logf("passed")
		}
	}

}
