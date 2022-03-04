package kv

import "testing"

func SampleTest(t *testing.T) {

	var kv kv

	kv.Insert(10, Value{10, 20})
	kv.Remove(10)
	kv.Scan(10, 10)
	kv.Search(10)
}
