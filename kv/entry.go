package kv

import "fmt"

type entry struct {
	key   Key
	value Value
}

func (e *entry) print() {
	fmt.Printf("%d", e.key)
}

func printEntries(entries []*entry) {
	fmt.Printf("[ ")
	for _, e := range entries {
		e.print()
		fmt.Printf(" ")
	}
	fmt.Printf("]")
}
