package main

import (
	"dmds-lab-01/kv"
)

func main() {
	size := 100
	store := kv.New(3)
	for i := 0; i < size; i++ {
		err := store.Insert(kv.Key(i), kv.Value{byte(i)})
		if err != nil {
			panic(err)
		}
	}

}
