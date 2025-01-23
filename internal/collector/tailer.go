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

// fileTailer is responsible for reading log lines from a log file.
// It is used to read log lines from a log file and send them to a channel.
//
// fileTailer remembers the current position of the log file, so it can resume reading from the same position after a restart in most cases.
//
// fileTailer is not thread-safe, so it should be protected by a mutex, if used in a multi-threaded environment.
type fileTailer struct {
	ino     uint64
	logFile *os.File
	posFile *os.File
	pos     uint64

	stopCh    chan struct{}
	stoppedCh chan struct{}

	started atomic.Bool

	ch chan<- []byte
}

func newFileTailer(posFileDir string, ino uint64, path string, ch chan<- []byte) (*fileTailer, error) {
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

	return &fileTailer{ino: ino, logFile: logFile, posFile: posFile, pos: pos, started: atomic.Bool{}, ch: ch}, nil
}

func (sc *fileTailer) start() {
	log.Printf("Starting tailer for inode %d", sc.ino)

	if sc.started.Load() {
		log.Printf("Tailer for inode %d is already started", sc.ino)
		return
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

	lineBuffer := make([]byte, 1024*1024)

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
			sc.ch <- lineBuffer[readTo:i]

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
