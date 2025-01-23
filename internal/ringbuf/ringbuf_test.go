package ringbuf

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func opt(v int) *int {
	return &v
}

func TestWriteCheck(t *testing.T) {
	bufSize := 10
	eobSize := 1

	// We assume:
	// - header(and eob) size is 1 byte
	// - body size is 2 bytes

	// Will write to the indices 7 and 8 (data size is 2 bytes).
	// After this write, the last index 9 will be used for the eob.
	assert.Equal(t, WriteOK, WriteCheck(7, opt(2), 2, 1, bufSize))
	// The eob is written at index 8, leaving the index 9 unused.
	// Data is written at indices 0 and 1, which is before the head at index 2.
	//
	// will try to write to the indices 8 and 9 (data size is 2 bytes).
	// But it's impossible because then you cannot write the eob at the end of the buffer, index 9, which is already used for the data.
	// So, it instructs to write the eob at index 8 first, then write the data at index 0.
	// The write is made at indices 0 and 1, which is before the head at index 2.
	assert.Equal(t, WriteAfterEOBRewind, WriteCheck(8, opt(2), 2, 1, bufSize))
	// The eob is written at index 9.
	// The write is made at indices 0 and 1, which is before the head at index 2.
	assert.Equal(t, WriteAfterEOBRewind, WriteCheck(9, opt(2), 2, 1, bufSize))

	assert.Equal(t, WriteOK, WriteCheck(0, nil, 9, eobSize, bufSize))
	// This is an edge-case, and in practice this is either too large data size, or too small buffer size.
	// Anyway, the eob must be written until the end of the buffer.
	// So, for the write to happen, you need 10+1 bytes buffer.
	assert.Equal(t, WriteFull, WriteCheck(0, nil, 10, eobSize, bufSize))

	// This must not happen.
	assert.Equal(t, WriteOverrun, WriteCheck(10, opt(2), 2, eobSize, bufSize))

	// This must not happen.
	assert.Equal(t, WriteOverruEOB, WriteCheck(9, opt(2), 2, 2, bufSize))
}

func TestReadCheck(t *testing.T) {
	assert.Equal(t, ReadEmpty, ReadCheck(0, nil, 1, 10))
	assert.Equal(t, ReadEmpty, ReadCheck(1, nil, 1, 10))
	assert.Equal(t, ReadOK, ReadCheck(2, nil, 1, 10))

	assert.Equal(t, ReadEmpty, ReadCheck(1, nil, 2, 10))
	assert.Equal(t, ReadOK, ReadCheck(2, nil, 1, 10))

	assert.Equal(t, ReadEmpty, ReadCheck(0, opt(9), 2, 10))
	assert.Equal(t, ReadEmpty, ReadCheck(1, opt(9), 2, 10))
	assert.Equal(t, ReadEmpty, ReadCheck(2, opt(9), 2, 10))
	// Usually this should not happen. You should have rewinded by reading eob earlier.
	assert.Equal(t, ReadAfterRewind, ReadCheck(3, opt(9), 2, 10))
	// This must not happen.
	assert.Equal(t, ReadOverrun, ReadCheck(0, opt(10), 1, 10))
	assert.Equal(t, ReadOverrun, ReadCheck(1, opt(10), 1, 10))
	assert.Equal(t, ReadOverrun, ReadCheck(2, opt(10), 1, 10))
}
