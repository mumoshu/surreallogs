package collector

import (
	"errors"
	"sync"
	"time"
)

// Queue is a queue for sending log entries to SurrealDB.
type Queue interface {
	Write(entry *LogEntry) error
	Close() error
	Peek(n int) ([]*LogEntry, *int, error)
	MarkRead(newReadPos int) error
}

type SyncFileQueue struct {
	q Queue

	mu *sync.Mutex
}

var _ Queue = (*SyncFileQueue)(nil)

func NewSyncQueue(q *DurableQueue) *SyncFileQueue {
	return &SyncFileQueue{
		q:  q,
		mu: &sync.Mutex{},
	}
}

func (b *SyncFileQueue) Write(entry *LogEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.q.Write(entry)
}

func (b *SyncFileQueue) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.q.Close()
}

func (b *SyncFileQueue) Peek(n int) ([]*LogEntry, *int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.q.Peek(n)
}

func (b *SyncFileQueue) MarkRead(n int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.q.MarkRead(n)
}

// DurableQueue is a queue that stores log entries in a file and flushes them to SurrealDB periodically.
//
// It is used to store log entries that have been observed by the collector
// until they are written to SurrealDB.
//
// The queue is implemented as a circular buffer with a fixed size.
// When the buffer is full, the oldest log entries are NOT discarded to make space for new ones.
// Instead, the buffer is flushed to SurrealDB and the oldest log entries are discarded,
// making space for new log entries.
//
// The buffer is flushed to SurrealDB in a separate goroutine that runs in the background.
// A Write() call does not block, but fail if the buffer is full.
// The caller is expected to retry the Write() call until it succeeds.
// Otherwise, the caller would lose log entries.
//
// The buffer is flushed periodically or when the buffer is closing due to a shutdown.
// This is to ensure that the window of opportunity for a log entry to be lost before
// it is flushed to SurrealDB is minimized.
// For example, if it is flushed every 10 seconds, then the window of opportunity for a log entry
// to be lost is 10 seconds.
type DurableQueue struct {
	maxSize       int
	flushInterval time.Duration

	stop    chan struct{}
	stopped chan struct{}
	bufFile *fsQueueStorage
}

var _ Queue = (*DurableQueue)(nil)

// OpenDurableQueue creates a new DurableQueue.
//
// The buffer is flushed to SurrealDB periodically or when the buffer is closing due to a shutdown.
// This is to ensure that the window of opportunity for a log entry to be lost before
// it is flushed to SurrealDB is minimized.
// For example, if it is flushed every 10 seconds, then the window of opportunity for a log entry
// to be lost is 10 seconds.
func OpenDurableQueue(bufFile *fsQueueStorage, maxSize int, flushInterval time.Duration) (*DurableQueue, error) {
	if flushInterval <= 0 {
		return nil, errors.New("flush interval must be greater than 0")
	}

	b := &DurableQueue{
		maxSize:       maxSize,
		bufFile:       bufFile,
		stop:          make(chan struct{}),
		stopped:       make(chan struct{}),
		flushInterval: flushInterval,
	}

	go b.runInBackground()

	return b, nil
}

func (b *DurableQueue) runInBackground() {
	defer close(b.stopped)
	for {
		select {
		case <-b.stop:
			return
		case <-time.After(b.flushInterval):
			b.bufFile.Flush()
		}
	}
}

// Write writes a log entry to the buffer.
// It does not block, but fail if the buffer is full.
// The caller is expected to retry the Write() call until it succeeds.
// Otherwise, the caller would lose log entries.
func (b *DurableQueue) Write(entry *LogEntry) error {
	return b.bufFile.Write(entry)
}

func (b *DurableQueue) MarkRead(n int) error {
	return b.bufFile.MarkRead(n)
}

// Close closes the buffer and flushes all remaining log entries to SurrealDB.
func (b *DurableQueue) Close() error {
	close(b.stop)
	<-b.stopped
	return b.bufFile.Flush()
}

// Peek returns the oldest n log entries in the buffer without removing them.
func (b *DurableQueue) Peek(n int) ([]*LogEntry, *int, error) {
	return b.bufFile.Peek(n)
}
