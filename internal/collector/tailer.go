package collector

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

const (
	DefaultLogLineReadBufferSize = 1024 * 1024
)

// fileTailer is responsible for reading log lines from a log file.
// It is used to read log lines from a log file and send them to a channel.
//
// fileTailer remembers the current position of the log file, so it can resume reading from the same position after a restart in most cases.
//
// fileTailer is not thread-safe, so it should be protected by a mutex, if used in a multi-threaded environment.
type fileTailer struct {
	// This is the reused and fixed buffer size for reading log lines.
	// It is used to avoid allocating a new buffer on each read,
	// and to avoid unbounded memory usage.
	logLineReadBufferSize int
	lineBufferPool        [][]byte
	// The maxiumum capacity of the pool
	lineBufferPoolCap int
	// The current position in the pool
	lineBufferPoolPos int

	ino     uint64
	logFile *os.File
	posFile *os.File
	pos     uint64

	stopCh    chan struct{}
	stoppedCh chan struct{}

	started atomic.Bool

	ch chan<- *logEntry
}

func newFileTailer(posFileDir string, ino uint64, path string, ch chan<- *logEntry) (*fileTailer, error) {
	if err := os.MkdirAll(posFileDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create position file directory: %w", err)
	}

	posFilePath := filepath.Join(posFileDir, fmt.Sprintf("%d.pos", ino))

	var (
		pos     uint64
		posFile *os.File
	)

	if posData, err := os.ReadFile(posFilePath); err == nil {
		if len(posData) != 8 {
			return nil, fmt.Errorf("position file is corrupted: %s", posFilePath)
		}

		pos = binary.BigEndian.Uint64(posData)

		posFile, err = os.OpenFile(posFilePath, os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open position file: %w", err)
		}
	} else {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read position file: %w", err)
		}

		posFile, err = os.OpenFile(posFilePath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to create position file: %w", err)
		}

		if _, err := posFile.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek to start of position file: %w", err)
		}

		var posData [8]byte
		binary.BigEndian.PutUint64(posData[:], pos)

		if _, err := posFile.Write(posData[:]); err != nil {
			return nil, fmt.Errorf("failed to write position file: %w", err)
		}

		if err := posFile.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync position file: %w", err)
		}
	}

	logFile, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	return &fileTailer{
		ino:                   ino,
		logFile:               logFile,
		posFile:               posFile,
		pos:                   pos,
		started:               atomic.Bool{},
		ch:                    ch,
		logLineReadBufferSize: DefaultLogLineReadBufferSize,
	}, nil
}

// Start starts the tailer.
//
// Start never fails immediately. However, if the started tailer is working or not-
// that's another story.
//
// Once a tailer is started, it will keep running with handling temporary failures graccefully by itself,
// and all the caller can do is to observe or Stop it.
func (sc *fileTailer) Start() {
	log.Printf("Starting tailer for inode %d", sc.ino)

	if sc.started.Load() {
		log.Printf("Tailer for inode %d is already started", sc.ino)
		return
	}

	if sc.logLineReadBufferSize == 0 {
		sc.logLineReadBufferSize = DefaultLogLineReadBufferSize
	}

	chCap := cap(sc.ch)
	// Assuming we can only buffer chCap log lines in the channel,
	// we need one more pool size not to accidentally put back the pooled line buffer which is still in use.
	poolCap := chCap + 1
	eachLogLineReadBufferSize := sc.logLineReadBufferSize / poolCap
	sc.lineBufferPoolCap = poolCap
	sc.lineBufferPool = make([][]byte, poolCap)
	for i := range sc.lineBufferPool {
		sc.lineBufferPool[i] = make([]byte, eachLogLineReadBufferSize)
	}

	sc.stopCh = make(chan struct{})
	sc.stoppedCh = make(chan struct{})

	log.Printf("Tailer for inode %d is starting", sc.ino)

	go sc.run()
}

func (sc *fileTailer) run() {
	defer func() {
		close(sc.stoppedCh)
	}()

	log.Printf("Tailer for inode %d is started", sc.ino)

	sc.started.Store(true)

	for {
		select {
		case <-sc.stopCh:
			return
		case <-time.After(5 * time.Second):
			log.Printf("Tailer for inode %d is still running", sc.ino)

			sc.readAndSendTail()
		}
	}
}

// read reads a new log line from the log file and sends it to the channel.
func (sc *fileTailer) readAndSendTail() {
	if _, err := sc.posFile.Seek(int64(sc.pos), io.SeekStart); err != nil {
		log.Printf("Failed to seek to read position %d: %v", sc.pos, err)
		return
	}

	lineBuffer := sc.lineBufferFromPool()

	read, err := sc.logFile.Read(lineBuffer)
	if err != nil {
		log.Printf("Failed to read log file after reading %d bytes: %v", read, err)
		return
	}

	var (
		readTo int
	)

	log.Printf("Read %d bytes from %s", read, sc.logFile.Name())

	for i := 0; i < read; i++ {
		if lineBuffer[i] == '\n' {
			sc.ch <- &logEntry{
				path: sc.logFile.Name(),
				line: lineBuffer[readTo:i],
			}

			readTo = i + 1
		}
	}
}

func (sc *fileTailer) stop() {
	if err := sc.posFile.Close(); err != nil {
		log.Printf("Failed to close position file: %v", err)
	}

	if err := sc.logFile.Close(); err != nil {
		log.Printf("Failed to close log file: %v", err)
	}
}

// lineBufferFromPool returns a line buffer from the pool.
// It returns the next line buffer in the pool, and increments the position.
// This works correctly only if we can guarantee the oldest line buffer in the pool
// is unused (or implicitly returned to the pool).
// In fileTailer, this is done by making the pool size a little bigger than the channel used to
// send line buffers to the consumer.
// Let's say the channel capacity is 10, and the pool size is 11.
// Then, the oldest item in the channel is the one that is currently or to be processed by the consumer.
// If fileTailer reused this line buffer, it would be a problem.
// We avoid this by making the pool size a little bigger than the channel capacity.
// This way, the oldest item in the channel is younger than the oldest item in the pool (which is recyclable via this function).
func (sc *fileTailer) lineBufferFromPool() []byte {
	if sc.lineBufferPoolPos == sc.lineBufferPoolCap {
		sc.lineBufferPoolPos = 0
	}

	lineBuf := sc.lineBufferPool[sc.lineBufferPoolPos]
	sc.lineBufferPoolPos++

	return lineBuf
}
