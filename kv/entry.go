package kv

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

type Key uint64
type Value [10]byte

type entry struct {
	key   Key
	value Value
}

func entryLen() int {
	return int(unsafe.Sizeof(entry{}))
}

func (e *entry) MarshalEntry() ([]byte, error) {
	buf := make([]byte, entryLen())
	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.key))
	copy(buf[8:], e.value[:])
	if len(buf) != entryLen() {
		return nil, &BufferOverflowError{Max: entryLen, Cursor: len(buf)}
	}
	return buf, nil
}

func (e *entry) UnmarshalEntry(data []byte) error {
	if len(data) != entryLen() {
		return fmt.Errorf("invalid entry size: %d", len(data))
	}
	e.key = Key(binary.LittleEndian.Uint64(data[0:8]))
	copy(e.value[:], data[8:])
	return nil
}
