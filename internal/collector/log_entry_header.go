package collector

import (
	"bytes"
	"encoding/binary"
)

const EntryHeaderSize = 4

type EntryHeaderEncoder struct {
	buf *bytes.Buffer
}

func (e *EntryHeaderEncoder) Encode(entrySize int) {
	entryHeader := make([]byte, EntryHeaderSize)
	binary.BigEndian.PutUint32(entryHeader, uint32(entrySize))
	e.buf.Write(entryHeader)
}

func readHeader(data []byte) int {
	return int(binary.BigEndian.Uint32(data))
}
