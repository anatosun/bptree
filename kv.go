package kv

type Key uint64
type Value [10]byte

type kv interface {
	Insert(Key, Value) error
	Remove(Key) (*Value, error)
	Search(Key) (*Value, error)
	Min() (*Key, error)
	Max() (*Key, error)
	Scan(Key, Key) ([]*Value, error)
}

type KV struct {
}

func (store *KV) Insert(key Key, value Value) error {
	return nil
}

func (store *KV) Remove(key Key) (*Value, error) {
	return nil, nil

}

func (store *KV) Search(key Key) (*Value, error) {
	return nil, nil

}

func (store *KV) Min() (*Key, error) {
	return nil, nil
}

func (store *KV) Max() (*Key, error) {
	return nil, nil
}

func (store *KV) Scan(key1, key2 Key) ([]*Value, error) {
	return nil, nil

}
