package kv

import (
	"encoding/binary"
	"testing"
	"fmt"
)




func TestAlbinsStuff(t *testing.T) {
	var store *BPlusTree
	var array []int

	const testSize = 1000 //  10^7 will trigger error: no free space

	store = New()
	array = make([]int, 0, testSize)

	for i := 0; i < testSize; i++ {
		array = append(array, i)
	}

	if store.Len() != 0 {
		t.Errorf("testSize should be 0 but is %d", store.Len())
		t.FailNow()
	}


	//t.Logf("inserting %d random keys", testSize)

	for i := 0; i < testSize; i++ {
		//t.Logf("inserting %d", array[i])
		//_, err := store.Insert(Key(array[i]), Value{byte(array[i]+100)}) //FX: we need some converter
																			// Entry marshalling=> variable type of value
		
		b := make([]byte, 10)
		binary.LittleEndian.PutUint64(b, uint64(array[i]))

		if i % 100000 == 0 {
			//t.Logf("inserting key=%d\n", i)
		}
		_, err := store.Insert(Key(array[i]), Value{b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9]})
		if err != nil {
			// store.bpm.PrintPool()
			// store.bpm.PrintTable()
			// store.bpm.diskManager.PrintNodes()
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

	k := testSize-5
	_, err := store.Search(Key(k))

	if err != nil {
		fmt.Printf("Error: %v", err)
	}

	
	
	// fmt.Printf("v=%v; val=%v\n", f, int64(binary.LittleEndian.Uint64(*valToBytes(f))))
	// fmt.Printf("Tree length=%d\n", int(store.Len()))

	//fmt.Println(preaollocation[len(preaollocation)-1])
	//fmt.Println(store.meta.free[len(store.meta.free)-1])


}

func valToBytes(v *Value) *[]byte {
	g:= make([]byte, 10)
	for i, val := range v {
		g[i] = val
	}
	return &g
}

