package kv

type Key uint64
type Value [10]byte

type kv interface {
	Insert(Key, Value) error
	Remove(Key) (*Value, error)
	Search(Key) (*Value, error)
	Scan(Key, Key) ([]*Value, error)
}
