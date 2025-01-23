package collector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"

	"github.com/surrealdb/surreallogs/internal/ringbuf"
)

type fsQueueStorage struct {
	maxSize int
	reader  *os.File
	writer  *os.File

	rwPos *rwPositions

	entryHeaderBuf     *bytes.Buffer
	entryHeaderEncoder *EntryHeaderEncoder

	entryBodyBuf     *bytes.Buffer
	entryBodyEncoder *json.Encoder

	eobData []byte

	isFull *atomic.Bool

	// flushAlways is true if both the buffer file and the pos file should be flushed after every write.
	flushAlways bool
}

// openFileSystemQueueStorage opens and (re)initializes a queue backed by a buffer and a buffer pos file for the given path and max size.
//
// This is not "newBufferFile" because it does not always create a new buffer file.
// If the buffer file is already there, it should usually receive non-zero readPos and writePos too,
// so that it can continue from where it left off.
//
// The buffer file writes to the buffer and the buffer pos file for every Write and MarkRead call.
// However, it usually does not flush to disk immediately.
//
// It is the caller's responsibility to either:
// - Set flushAlways to true, so that the buffer file is flushed to disk after every Write and MarkRead call.
// - Periodically call Flush() to ensure the buffer file is flushed to disk if necessary.
func openFileSystemQueueStorage(path string, maxSize int) (*fsQueueStorage, error) {
	writer, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	reader, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	entryHeaderBuf := bytes.NewBuffer(nil)
	entryHeaderEncoder := &EntryHeaderEncoder{buf: entryHeaderBuf}

	entryBodyBuf := bytes.NewBuffer(nil)
	encoder := json.NewEncoder(entryBodyBuf)

	var eobBuf bytes.Buffer
	eobEncoder := &EntryHeaderEncoder{buf: &eobBuf}
	eobEncoder.Encode(0)
	eobData := eobBuf.Bytes()

	posFilePath := path + ".pos"

	rwPos, err := openRWPos(posFilePath)
	if err != nil {
		return nil, err
	}

	return &fsQueueStorage{
		reader:             reader,
		writer:             writer,
		entryHeaderBuf:     entryHeaderBuf,
		entryHeaderEncoder: entryHeaderEncoder,
		entryBodyBuf:       entryBodyBuf,
		entryBodyEncoder:   encoder,
		eobData:            eobData,
		isFull:             &atomic.Bool{},
		maxSize:            maxSize,
		rwPos:              rwPos,
	}, nil
}

func (f *fsQueueStorage) Write(entry *LogEntry) error {
	f.entryBodyBuf.Reset()
	if err := f.entryBodyEncoder.Encode(entry); err != nil {
		return err
	}

	entrySize := f.entryBodyBuf.Len()

	// Prepend the fixed size "entrySize" to the buffer.
	// This is to prevent trying to read the tail of the buffer as a valid entry.
	f.entryHeaderBuf.Reset()
	f.entryHeaderEncoder.Encode(entrySize)

	headerSize := f.entryHeaderBuf.Len()

	writeSize := entrySize + headerSize

	log.Printf("Writing entry %v of size %d, header size %d, writeSize %d", entry, entrySize, headerSize, writeSize)

	eobSize := len(f.eobData)

	if _, err := f.writer.Seek(int64(f.rwPos.GetWritePos()), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to write position: %w", err)
	}

	switch res := ringbuf.WriteCheck(f.rwPos.GetWritePos(), f.rwPos.state.ReadPos, writeSize, eobSize, f.maxSize); res {
	case ringbuf.WriteFull:
		return errors.New("buffer is full")
	case ringbuf.WriteOK:
		if wrote, err := f.writer.Write(f.entryHeaderBuf.Bytes()); err != nil {
			return err
		} else if wrote != headerSize {
			return fmt.Errorf("failed to write entry header: Wrote %d, wanted to write %d", wrote, headerSize)
		}
	case ringbuf.WriteAfterEOBRewind:
		// The remaining space is not enough for writing the header and the body, but enough for writing the header.
		// So write the zero entry size to mark the end of the buffer.
		if wrote, err := f.writer.Write(f.eobData); err != nil {
			return err
		} else if wrote != eobSize {
			return fmt.Errorf("failed to write zero End-of-buffer header: Wrote %d, wanted to write %d", wrote, eobSize)
		}

		f.rwPos.SetWritePos(0)
	default:
		return fmt.Errorf("failed to write entry: %v", res)
	}

	if wrote, err := f.writer.Write(f.entryHeaderBuf.Bytes()); err != nil {
		return err
	} else if wrote != headerSize {
		return fmt.Errorf("failed to write entry header: Wrote %d, wanted to write %d", wrote, headerSize)
	}

	if wrote, err := f.writer.Write(f.entryBodyBuf.Bytes()); err != nil {
		return err
	} else if wrote != entrySize {
		return fmt.Errorf("failed to write entry body: Wrote %d, wanted to write %d", wrote, entrySize)
	}

	f.rwPos.AddWritePos(writeSize)

	if err := f.writePosFile(); err != nil {
		return err
	}

	if f.flushAlways {
		if err := f.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// Flush flushes the buffer to SurrealDB.
// It is called periodically or when the buffer is closing due to a shutdown.
func (f *fsQueueStorage) Flush() error {
	log.Printf("Flushing buffer file")

	if err := f.writer.Sync(); err != nil {
		return fmt.Errorf("failed to sync buffer file: %w", err)
	}

	readPos := "nil"
	if f.rwPos.state.ReadPos != nil {
		readPos = fmt.Sprintf("%d", *f.rwPos.state.ReadPos)
	}

	log.Printf("Flushing buffer pos file with readPos %s, writePos %d", readPos, f.rwPos.state.WritePos)

	if err := f.rwPos.Flush(); err != nil {
		return fmt.Errorf("failed to sync pos file: %w", err)
	}

	return nil
}

func (f *fsQueueStorage) writePosFile() error {
	return f.rwPos.Write()
}

// Peek returns the oldest n log entries in the buffer without removing them.
// The second return value is the read position after the peek,
// which should be used when calling MarkRead after successful consumption of peeked entries.
func (f *fsQueueStorage) Peek(n int) ([]*LogEntry, *int, error) {
	var entries []*LogEntry

	var readPos *int

	opt := func(v int) *int {
		return &v
	}

	if f.rwPos.state.ReadPos != nil {
		readPos = opt(f.rwPos.GetReadPos())
	}

FOR:
	for i := 0; i < n; i++ {
		switch res := ringbuf.ReadCheck(f.rwPos.GetWritePos(), readPos, 4, f.maxSize); res {
		case ringbuf.ReadEmpty:
			// End of the buffer, due to the write has not yet happened after this read position.
			break FOR
		case ringbuf.ReadAfterRewind:
			readPos = opt(0)
		case ringbuf.ReadOK:
			if readPos == nil {
				readPos = opt(0)
			}
		default:
			return nil, nil, fmt.Errorf("failed to read entry: %v", res)
		}

		log.Printf("Seeking to read position %d", *readPos)

		if _, err := f.reader.Seek(int64(*readPos), io.SeekStart); err != nil {
			return nil, nil, fmt.Errorf("failed to seek to read position: %w", err)
		}

		headerData := make([]byte, 4)
		if read, err := f.reader.Read(headerData); err != nil {
			return nil, nil, fmt.Errorf("failed to read header: %w", err)
		} else if read != 4 {
			return nil, nil, fmt.Errorf("failed to read header: read %d, wanted to read 4", read)
		}

		bodySize := readHeader(headerData[:])

		if bodySize == 0 {
			// End of buffer.
			// We can rewind the read position to zero for the next header read,
			// if write is already happened.
			if f.rwPos.GetWritePos() > 0 {
				readPos = opt(0)
			}
			continue
		}

		log.Printf("Read entry data of size %d", bodySize)

		entry := &LogEntry{}
		bodyData := make([]byte, bodySize)

		switch res := ringbuf.ReadCheck(f.rwPos.GetWritePos(), readPos, bodySize, f.maxSize); res {
		case ringbuf.ReadEmpty:
			return nil, nil, fmt.Errorf("failed to read body: %v", res)
		case ringbuf.ReadOK:
		default:
			return nil, nil, fmt.Errorf("failed to read body: %v", res)
		}

		if _, err := f.reader.Seek(4, io.SeekCurrent); err != nil {
			return nil, nil, fmt.Errorf("failed to seek to body data: %w", err)
		}

		if _, err := f.reader.Read(bodyData); err != nil {
			return nil, nil, fmt.Errorf("failed to read body data: %w", err)
		}

		log.Printf("Read entry data of size %d: %s", len(bodyData), string(bodyData))

		if err := json.Unmarshal(bodyData, entry); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal log entry body: %w", err)
		}
		entries = append(entries, entry)
		*readPos += 4 + bodySize
	}

	if len(entries) == 0 {
		return nil, nil, nil
	}

	return entries, readPos, nil
}

// MarkRead moves the read position to the given newReadPos, effectively meaning that the all the log entries before the newReadPos are consumed.
// The next read will start from the next byte after the newReadPos.
func (f *fsQueueStorage) MarkRead(newReadPos int) error {
	if _, err := f.reader.Seek(int64(newReadPos), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to read position: %w", err)
	}

	f.rwPos.state.ReadPos = &newReadPos
	if err := f.rwPos.Write(); err != nil {
		return err
	}

	if f.flushAlways {
		if err := f.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func (f *fsQueueStorage) Close() error {
	if err := f.Flush(); err != nil {
		return err
	}

	return nil
}

type rwPositions struct {
	state rwPosState

	// Unlike pos files for log files, the pos file for the buffer file is used to store not only read but also write positions.
	posFile *os.File
}

type rwPosState struct {
	ReadPos  *int `json:"read_pos"`
	WritePos int  `json:"write_pos"`
}

func openRWPos(posFilePath string) (*rwPositions, error) {
	var (
		rwState rwPosState

		posFile *os.File
	)

	if posData, err := os.ReadFile(posFilePath); err != nil {
		log.Printf("No pos file found at %s, starting from the beginning", posFilePath)

		posFile, err = os.OpenFile(posFilePath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}

		if err := writePosFile(posFile, &rwState); err != nil {
			return nil, fmt.Errorf("failed to write pos file: %w", err)
		}
	} else {
		rwState, err = unmarshalRWPosState(posData)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal rwPosState: %w", err)
		}

		log.Printf("Continuing from readPos %v, writePos %d in %s", rwState.ReadPos, rwState.WritePos, posFilePath)

		posFile, err = os.OpenFile(posFilePath, os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open pos file: %w", err)
		}
	}

	return &rwPositions{
		state:   rwState,
		posFile: posFile,
	}, nil
}

func (r *rwPositions) CanWrite(n int) bool {
	if r.state.ReadPos == nil {
		return true
	}

	return r.state.WritePos+n < *r.state.ReadPos
}

func (r *rwPositions) Flush() error {
	return r.posFile.Sync()
}

func (r *rwPositions) Write() error {
	return writePosFile(r.posFile, &r.state)
}

func (r *rwPositions) AddReadPos(n int) {
	r.SetReadPos(r.GetReadPos() + n)
}

func (r *rwPositions) AddWritePos(n int) {
	r.SetWritePos(r.GetWritePos() + n)
}

func (r *rwPositions) SetReadPos(pos int) {
	r.state.ReadPos = &pos
}

func (r *rwPositions) SetWritePos(pos int) {
	r.state.WritePos = pos
}

func (r *rwPositions) GetReadPos() int {
	if r.state.ReadPos == nil {
		return 0
	}
	return *r.state.ReadPos
}

func (r *rwPositions) GetWritePos() int {
	return r.state.WritePos
}

func writePosFile(posFile *os.File, rwPosState *rwPosState) error {
	if _, err := posFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start of pos file: %w", err)
	}

	if err := json.NewEncoder(posFile).Encode(rwPosState); err != nil {
		return fmt.Errorf("failed to encode rwPosState: %w", err)
	}

	return nil
}

func unmarshalRWPosState(data []byte) (rwPosState, error) {
	var rwState rwPosState
	if err := json.Unmarshal(data, &rwState); err != nil {
		return rwPosState{}, fmt.Errorf("failed to unmarshal rwPosState: %w", err)
	}
	return rwState, nil
}
