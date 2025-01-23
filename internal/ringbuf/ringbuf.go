package ringbuf

const (
	// WriteOK means the next write can happen.
	WriteOK = iota
	// WriteFull means the buffer is full and no more data can be written,
	// until the head progresses.
	WriteFull
	// WriteAfterEOBRewind means the next write can happen
	// if the the tail is terminated by End-Of-Buffer and then rewinded to 0.
	WriteAfterEOBRewind
	// WriteOverruEOB means the write position is within the buffer,
	// but the End-Of-Buffer header cannot be written.
	// This could happen if the buffer size has decreased.
	WriteOverruEOB
	// WriteOverrun means the write position is out of the buffer.
	// This could be due to the buffer size has decreased.
	WriteOverrun
)

// tail is where the writes happen.
// head is where the reads happen.
// dataSize is the size of the data to be written.
// bufSize is the size of the buffer.
//
// Returns:
//   - WritePossible if the next write can happen.
//   - Full if the buffer is full and no more data can be written,
//   - WriteAfterRewind if the next write can happen
//     if the the tail is rewinded to 0.
//   - WriteAfterEOBRewind if the next write can happen
//     if the the tail is terminated by End-Of-Buffer and then rewinded to 0.
//
// The basic idea is that you cannot write if the tail overwrites unread data,
// or if you cannot write the End-Of-Buffer header after the this write..
func WriteCheck(tail int, headOpt *int, dataSize, eobSize, bufSize int) int {
	if headOpt == nil {
		if tail+dataSize+eobSize <= bufSize {
			return WriteOK
		}
		return WriteFull
	}

	if tail >= bufSize {
		return WriteOverrun
	}

	head := *headOpt

	if tail <= head {
		// This means head - tail is the number of bytes between the head and the tail,
		// available for writing data.
		if tail+dataSize <= head {
			if tail+dataSize+eobSize <= bufSize {
				return WriteOK
			}
			// Say full because if we write, you cannot write the End-Of-Buffer header after this write.
			// Wait until head < tail and head >= dataSize so that you can write the End-Of-Buffer header first,
			// then write the data.
			return WriteFull
		}
		return WriteFull
	}

	// tail(write position) is after head(read position).
	//
	// At this point, the write is possible only if either:
	// 1.  (bufSize - tail) is equal or less than the data size.
	// 2.  dataSize is less than or equal to head.
	//
	// As our circular buffer block size is variable, we use End-Of-Buffer to mark the end of the buffer.
	// In case of 2., we check if (bufSize - tail) is less than or equal to the End-Of-Buffer header size.
	// If it is, we can write the End-Of-Buffer header befoer rewinding the tail to 0.
	// This ensures that the reader never reads non-written garbage data in case writer rewinded.

	if tail+dataSize+eobSize <= bufSize {
		return WriteOK
	}

	if dataSize <= head {
		if tail+eobSize <= bufSize {
			return WriteAfterEOBRewind
		}
		return WriteOverruEOB
	}

	return WriteFull
}

const (
	ReadOK = iota
	ReadEmpty
	ReadAfterRewind
	ReadOverrun
)

// dataSize can be either the header size or the body size to read.
func ReadCheck(tail int, headOpt *int, dataSize, bufSize int) int {
	var head int
	if headOpt == nil {
		head = 0
	} else {
		head = *headOpt
	}

	if head >= bufSize {
		return ReadOverrun
	}

	if head <= tail {
		n := tail - head

		if n > dataSize {
			// To avoid overlapping tail and head
			return ReadOK
		}

		// No data to read.
		// Trying to read faster than the writer.
		return ReadEmpty
	}

	// head is after tail.
	//
	// At this point, the read is possible only if either:
	// 1.  (bufSize - head) is equal or less than the data size.
	// 2.  dataSize is less than or equal to tail.

	n := bufSize - head

	if n >= dataSize {
		return ReadOK
	}

	if tail > dataSize {
		// To avoid overlapping tail and head, we use > instead of >=.
		return ReadAfterRewind
	}

	return ReadEmpty
}
