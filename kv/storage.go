package kv

type storage interface {
	Insert(Key, Value) (bool, error)
	Remove(Key) (*Value, error)
	Search(Key) (*Value, error)
	Len() int
}
