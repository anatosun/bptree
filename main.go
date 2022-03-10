package main

import (
	"dmds-lab-01/kv"
	"fmt"
)

func main() {

	store := kv.New()
	err := store.Insert(kv.Key(10), kv.Value{byte(10)}, false)
	if err != nil {
		fmt.Println("already an error? nothing is implemented...")
	} else {
		fmt.Println("this is a mock main, it basically does nothing...")
	}
}
