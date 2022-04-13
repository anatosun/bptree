package kv

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"unsafe"
)

const header = 8

type metadata struct {
	dirty bool

	// we chose uint64
	keySize uint64
	// a page size is 4KB
	pageSize uint32
	size     uint32
	root     uint64
	free     []uint64
}

func metaHeaderSize() int {
	keySize := uint64(0)
	// a page size is 4KB
	pageSize := uint32(0)
	size := uint32(0)
	root := uint64(0)
	freeSpace := uint32(0)
	return int(unsafe.Sizeof(keySize) +
		unsafe.Sizeof(pageSize) +
		unsafe.Sizeof(size) +
		unsafe.Sizeof(freeSpace) +
		unsafe.Sizeof(root))
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (meta metadata) MarshalBinary() ([]byte, error) {
	buffer := make([]byte, meta.pageSize)
	if _, err := rand.Read(buffer); err != nil {
		return buffer, err
	}
	space := int(meta.pageSize) - header

	for len(meta.free)*4 > space {
		meta.free = meta.free[:space/2]
	}
	binary.LittleEndian.PutUint64(buffer[0:8], meta.keySize)
	binary.LittleEndian.PutUint32(buffer[8:12], meta.pageSize)
	binary.LittleEndian.PutUint32(buffer[12:16], meta.size)
	binary.LittleEndian.PutUint64(buffer[16:24], meta.root)
	binary.LittleEndian.PutUint32(buffer[24:28], uint32(len(meta.free)))

	cursor := 28

	// if cursor != metaHeaderSize() {
	// 	return nil, &InvalidSizeError{Should: metaHeaderSize(), Got: cursor}
	// }

	for _, free := range meta.free {
		binary.LittleEndian.PutUint32(buffer[cursor:cursor+4], uint32(free))
		cursor += 4
	}

	return buffer, nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (meta *metadata) UnmarshalBinary(data []byte) error {

	if len(data) < metaHeaderSize() {
		return fmt.Errorf("data is invalid")
	}

	if meta == nil {
		return fmt.Errorf("metadata is nil")
	}

	meta.keySize = binary.LittleEndian.Uint64(data[0:8])
	meta.pageSize = binary.LittleEndian.Uint32(data[8:12])
	meta.size = binary.LittleEndian.Uint32(data[12:16])
	meta.root = binary.LittleEndian.Uint64(data[16:24])
	space := binary.LittleEndian.Uint32(data[24:28])
	meta.free = make([]uint64, space)
	cursor := 24
	for i := 0; i < int(space); i++ {
		meta.free[i] = uint64(binary.LittleEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4
	}
	return nil
}
