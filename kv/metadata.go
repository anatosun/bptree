package kv

import (
	"encoding/binary"
	"fmt"
)

const header = 8

type metadata struct {
	dirty bool

	// we chose uint64
	keySize uint64
	// a page size is 4KB
	pageSize uint32
	size     uint32
	root     uint32
	free     []uint64
}

func (meta metadata) Marshal() ([]byte, error) {
	buffer := make([]byte, 0, meta.pageSize)
	space := int(meta.pageSize) - header

	for len(meta.free)*4 > space {
		meta.free = meta.free[:space/2]
	}
	binary.LittleEndian.PutUint64(buffer[0:8], meta.keySize)
	binary.LittleEndian.PutUint32(buffer[8:12], meta.pageSize)
	binary.LittleEndian.PutUint32(buffer[12:16], meta.size)
	binary.LittleEndian.PutUint32(buffer[16:20], meta.root)
	binary.LittleEndian.PutUint32(buffer[20:24], uint32(len(meta.free)))

	cursor := 24

	for _, free := range meta.free {
		binary.LittleEndian.PutUint32(buffer[cursor:cursor+4], uint32(free))
		cursor += 4
	}

	return buffer, nil
}

func (meta *metadata) Unmarshal(data []byte) error {

	if len(data) < header {
		return fmt.Errorf("data is invalid")
	}

	if meta == nil {
		return fmt.Errorf("metadata is nil")
	}

	meta.keySize = binary.LittleEndian.Uint64(data[0:8])
	meta.pageSize = binary.LittleEndian.Uint32(data[8:12])
	meta.size = binary.LittleEndian.Uint32(data[12:16])
	meta.root = binary.LittleEndian.Uint32(data[16:20])
	space := binary.LittleEndian.Uint32(data[20:24])
	meta.free = make([]uint64, space)
	cursor := 24
	for i := 0; i < int(space); i++ {
		meta.free[i] = uint64(binary.LittleEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4
	}
	return nil
}
