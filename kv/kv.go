package kv

type Key uint64
type Value [10]byte

type kv interface {
	Insert(Key, Value) error
	Remove(Key) (*Value, error)
	Search(Key) (*Value, error)
	Min() (*Key, error)
	Max() (*Key, error)
	Len() uint64
	Scan(Key, Key) ([]*Value, error)
}
