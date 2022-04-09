package kv

import (
	"encoding/binary"
	"fmt"
)

type Key uint64
type Value [10]byte

const entrySize = 8 + 10

type entry struct {
	key   Key
	value Value
}

func (e *entry) MarshalEntry() ([]byte, error) {
	buf := make([]byte, entrySize)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.key))
	copy(buf[8:], e.value[:])
	if len(buf) != entrySize {
		return nil, &BufferOverflowError{Max: entrySize, Cursor: len(buf)}
	}
	return buf, nil
}

func (e *entry) UnmarshalEntry(data []byte) error {
	if len(data) != entrySize {
		return fmt.Errorf("invalid entry size: %d", len(data))
	}
	e.key = Key(binary.LittleEndian.Uint64(data[0:8]))
	copy(e.value[:], data[8:])
	return nil
}
