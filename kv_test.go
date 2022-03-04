package kv

import (
	"math/rand"
	"sort"
	"testing"
)

var store kv
var array []int

const size = 1000

func TestInit(t *testing.T) {
	array = make([]int, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, rand.Int())
	}

}

func TestInsert(t *testing.T) {
	t.Logf("inserting %d random keys", size)

	for i := 0; i < size; i++ {
		err := store.Insert(Key(array[i]), Value{byte(array[i])})
		if err != nil {
			t.Errorf("while inserting to kv store(%d): %v", i, err)
			t.FailNow()
		}
	}
	t.Logf("success")
}
func TestSearchX(t *testing.T) {
	// sort original array
	sort.Ints(array)

	for i := 0; i < len(array); i++ {
		k := Key(array[i])

		_, err := store.Search(k)

		if err != nil {
			t.Errorf("searching err: %v", err)
			t.Fail()
		}
	}
}
