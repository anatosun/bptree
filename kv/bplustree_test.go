package kv

import (
	"math/rand"
	"testing"
	// "fmt"
)

var store *BPlusTree
var array []int

const size = 10000000

func TestInit(t *testing.T) {
	store = New()
	array = make([]int, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, i)
	}

	if store.Len() != 0 {
		t.Errorf("size should be 0 but is %d", store.Len())
		t.FailNow()
	}

}

func TestInsert(t *testing.T) {
	//	t.Logf("inserting %d random keys", size)

	for i := 0; i < size; i++ {
		//		t.Logf("inserting %d", array[i])
		_, err := store.Insert(Key(array[i]), Value{byte(array[i])})
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

func TestUpdate(t *testing.T) {

	if store.Len() == 0 {
		TestInsert(t)
	}

	for i := 0; i < len(array); i++ {
		r := rand.Int()
		if r != array[i] {

			success, err := store.Insert(Key(array[i]), Value{byte(array[i])})

			if err != nil {
				t.Errorf("error while updating %d to value %d: %v", array[i], r, err)
				t.FailNow()
			}

			if success {
				t.Errorf("error while updating %d to value %d: value was not updated", array[i], r)
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

func TestRemove(t *testing.T) {

	if store.Len() == 0 {
		TestInsert(t)
	}

	for i := 0; i < len(array); i++ {
		_, err := store.Remove(Key(array[i]))
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

// func TestMinMax(t *testing.T) {

// 	const MaxInt = int(^uint8(0) >> 1)
// 	const MinInt = 0 //0 since uint

// 	errMax := store.Insert(Key(2), Value{byte(MaxInt)})
// 	errMin := store.Insert(Key(3), Value{byte(MinInt)})

// 	//TODO: remove after implementation fix
// 	array[2] = MaxInt
// 	array[3] = MinInt

// 	if errMax != nil || errMin != nil {
// 		t.Errorf("while inserting to kv store(%d): %v ; %v", 0, errMax, errMin)
// 		t.FailNow()
// 	} else {
// 		minKey, errMin := store.Min()
// 		maxKey, errMax := store.Max()

// 		if errMin != nil {
// 			t.Errorf("Min() yielded and error %v", errMin)
// 			t.FailNow()
// 		}
// 		if errMax != nil {
// 			t.Errorf("Max() yielded and error %v", errMax)
// 		}

// 		t.Logf("keys: min: %v, max: %v", minKey, maxKey)

// 		//TODO: remove after implementation fix
// 		maxVal := array[2] // store.Search(maxKey)
// 		minVal := array[3] // store.Search(minKey)

// 		if minVal != MinInt {
// 			t.Errorf("Min() didn't work as expected")
// 		}

// 		if maxVal != MaxInt {
// 			t.Errorf("Max() didn't work as expected")
// 		}
// 	}
// }

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
