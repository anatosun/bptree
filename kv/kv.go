package kv

type KV interface {
	Insert(Key, Value) error
	Remove(Key) (*Value, error)
	Search(Key) (*Value, error)
	Min() (*Key, error)
	Max() (*Key, error)
	Len() int
	Range(Key, Key) ([]*Value, error)
	Scan(Key, func(Key) bool) ([]*Value, error)
}
