package kv

type Key uint64
type Value [10]byte

type Kv interface {
	insert(key Key, value Value) error
	remove(key Key) (Value, error)
	search(key Key) (Value, error)
	scan(key1 Key, key2 Key) ([]Value, error)
}
