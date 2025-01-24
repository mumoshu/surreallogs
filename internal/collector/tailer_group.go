package collector

import (
	"fmt"
	"log"
	"os"
	"syscall"
	"time"
)

// logEntry is each log entry read from the log file.
type logEntry struct {
	// The path of the log file.
	path string

	// The log line.
	line []byte
}

// tailerGroup is responsible for reading log entries from log files.
// The log file is added via the startCollecting method, and removed via the stopCollecting method.
//
// The tailerGroup is responsible for reading log entries from the log file and sending them to a channel.
// The channel is supposed to be read by the collector, which writes the log entries to SurrealDB.
type tailerGroup struct {
	posFileDir            string
	inoToSubcollector     map[uint64]*fileTailer
	readInterval          time.Duration
	logLineReadBufferSize int
	ch                    chan<- *logEntry
}

func newTailerGroup(posFileDir string, readInterval time.Duration, ch chan<- *logEntry) *tailerGroup {
	return &tailerGroup{
		posFileDir:        posFileDir,
		inoToSubcollector: make(map[uint64]*fileTailer),
		readInterval:      readInterval,
		ch:                ch,
	}
}

func (c *tailerGroup) getIno(path string) (uint64, error) {
	var inode uint64

	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("failed to get file info: %w", err)
	}
	inode = uint64(info.Sys().(*syscall.Stat_t).Ino)

	return inode, nil
}

// startTailing starts reading logs from the given path.
//
// The collector creates a position file in the given path to track the current position of the log file.
// The position file survives the collector process, so it can be used to resume reading after a restart.
func (c *tailerGroup) StartTailing(path string) error {
	ino, err := c.getIno(path)
	if err != nil {
		return fmt.Errorf("failed to get inode: %w", err)
	}

	log.Printf("Starting tailer for %s", path)

	_, ok := c.inoToSubcollector[ino]
	if !ok {
		log.Printf("Creating tailer for inode %d", ino)

		sc, err := newFileTailer(c.posFileDir, ino, path, c.ch)
		if err != nil {
			return fmt.Errorf("failed to create subcollector: %w", err)
		}

		sc.logLineReadBufferSize = c.logLineReadBufferSize

		sc.Start()
	}

	return nil
}

// stopTailing stops reading logs from the given path.
//
// The position file is deleted to indicate that the collector has stopped reading logs from the given path.
func (c *tailerGroup) StopTailing(path string) error {
	ino, err := c.getIno(path)
	if err != nil {
		return fmt.Errorf("failed to get inode: %w", err)
	}

	sc, ok := c.inoToSubcollector[ino]
	if !ok {
		return fmt.Errorf("subcollector not found")
	}

	sc.stop()

	delete(c.inoToSubcollector, ino)

	return nil
}

func (c *tailerGroup) Close() error {
	for _, sc := range c.inoToSubcollector {
		sc.stop()
	}

	return nil
}
