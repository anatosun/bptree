package kv

type Key uint64
type Value [10]byte

type entry struct {
	key   Key
	value Value
}
