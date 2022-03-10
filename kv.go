package kv

type Key uint64
type Value [10]byte

type kv interface {
	Insert(Key, Value, bool) error
	Remove(Key) (*Value, error)
	Search(Key) (*Value, error)
	Min() (*Key, error)
	Max() (*Key, error)
	Len() uint64
	Scan(Key, Key) ([]*Value, error)
}

type KV struct {
	size uint64
}

func New() *KV {
	return &KV{size: 0}
}

func (store *KV) Len() uint64 {
	return store.size
}

func (store *KV) Insert(key Key, value Value, update bool) error {

	if !update {
		store.size++
	}
	return nil
}

func (store *KV) Remove(key Key) (*Value, error) {
	store.size--
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
