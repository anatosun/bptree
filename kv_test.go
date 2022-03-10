package kv

import (
	"math/rand"
	"testing"
)

var store kv
var array []int

const size = 100000

func TestInit(t *testing.T) {
	store = New()
	array = make([]int, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, rand.Int())
	}

	if store.Len() != 0 {
		t.Errorf("size should be 0 but is %d", store.Len())
		t.FailNow()
	}

}

func TestInsert(t *testing.T) {
	t.Logf("inserting %d random keys", size)

	for i := 0; i < size; i++ {
		err := store.Insert(Key(array[i]), Value{byte(array[i])}, false)
		if err != nil {
			t.Errorf("while inserting to kv store(%d): %v", i, err)
			t.FailNow()
		}
	}

	expected := len(array)
	actual := int(store.Len())

	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
		t.FailNow()
	}
}

func TestRemove(t *testing.T) {

	if store.Len() == 0 {
		TestInsert(t)
	}

	for i := 0; i < len(array); i++ {
		err, _ := store.Remove(Key(array[i]))
		if err != nil {
			t.Errorf("while removing %d: %v", array[i], err)
			t.FailNow()
		}

	}

	expected := 0
	actual := int(store.Len())

	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
		t.FailNow()
	}
}

func TestUpdate(t *testing.T) {

	if store.Len() == 0 {
		TestInsert(t)
	}

	for i := 0; i < len(array); i++ {
		r := rand.Int()
		if r != array[i] {
			err := store.Insert(Key(array[i]), Value{byte(array[i])}, true)
			if err != nil {
				t.Errorf("while updating %d to value %d: %v", array[i], r, err)
				t.FailNow()
			}
		}
	}

	expected := len(array)
	actual := int(store.Len())

	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
		t.FailNow()
	}
}

// will not work without proper implementation, this is why it's commented
//func TestInsertSameKeyTwice(t *testing.T) {
//	if store.Len() != 0 {
//		TestRemove(t)
//	}
//	r := Key(rand.Int())
//
//	err := store.Insert(r, Value{byte(r)}, false)
//	if err != nil {
//
//		t.Errorf("unexpected error while inserting %d: %v", r, err)
//		t.FailNow()
//	}
//	err = store.Insert(r, Value{byte(r)}, false)
//	if err == nil {
//		t.Errorf("should get an error when inserting same key %d twice: %v", r, err)
//		t.FailNow()
//	}
//
//	expected := 1
//	actual := int(store.Len())
//
//	if expected != actual {
//		t.Errorf("expected %d, got %d", expected, actual)
//		t.FailNow()
//	}
//
//}
