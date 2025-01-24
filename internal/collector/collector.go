package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surreallogs/internal/config"
)

type Collector struct {
	cfg   *config.Config
	db    *surrealdb.DB
	queue Queue

	logLinesCh chan []byte

	tailerGroup *tailerGroup
}

func New(cfg *config.Config) (*Collector, error) {
	readInterval, err := time.ParseDuration(cfg.Collector.ReadInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to parse read interval: %w", err)
	}

	maxSize, err := ParseHumanReadableSize(cfg.Collector.Buffer.MaxSize)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max size: %w", err)
	}

	flushInterval, err := time.ParseDuration(cfg.Collector.Buffer.FlushInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to parse flush interval: %w", err)
	}

	queueStorage, err := openFileSystemQueueStorage(cfg.Collector.Buffer.Path, maxSize)
	if err != nil {
		return nil, err
	}

	durableQueue, err := OpenDurableQueue(queueStorage, maxSize, flushInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to create buffer: %w", err)
	}

	//
	// We write log entries sent from the reader to SurrealDB.
	//

	// The client to the SurrealDB instance that log entries are written to.
	db, err := connectSurrealDB(&cfg.SurrealDB)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SurrealDB: %w", err)
	}

	// We need to synchronize access to the file queue to avoid race conditions.
	syncQueue := NewSyncQueue(durableQueue)

	// We buffer up to 1024 log lines to avoid blocking the reader.
	logLineCh := make(chan []byte, 1024)

	tg := newTailerGroup(cfg.Collector.ReadPosDir, readInterval, logLineCh)

	logLineReadBufferSize, err := ParseHumanReadableSize(cfg.Collector.LogLineReadBufferSize)
	if err != nil {
		return nil, fmt.Errorf("failed to parse log line read buffer size: %w", err)
	}

	tg.logLineReadBufferSize = logLineReadBufferSize

	return &Collector{
		cfg:         cfg,
		db:          db,
		queue:       syncQueue,
		logLinesCh:  logLineCh,
		tailerGroup: tg,
	}, nil
}

// ParseHumanReadableSize parses a human-readable size string into an integer.
//
// The size string is expected to be in the format of "100MB", "1GB", etc.
func ParseHumanReadableSize(size string) (int, error) {
	if strings.HasSuffix(size, "KB") {
		s, err := strconv.ParseInt(strings.TrimSuffix(size, "KB"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse size: %w", err)
		}
		return int(s * 1024), nil
	}

	if strings.HasSuffix(size, "MB") {
		s, err := strconv.ParseInt(strings.TrimSuffix(size, "MB"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse size: %w", err)
		}
		return int(s * 1024 * 1024), nil
	}

	if strings.HasSuffix(size, "GB") {
		s, err := strconv.ParseInt(strings.TrimSuffix(size, "GB"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse size: %w", err)
		}
		return int(s * 1024 * 1024 * 1024), nil
	}

	i32, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse size: %w", err)
	}
	return int(i32), nil
}

func (c *Collector) Run(ctx context.Context) error {
	if err := c.startReadingLogs(ctx); err != nil {
		return fmt.Errorf("failed to start reading logs: %w", err)
	}

	c.startPipingLogsToQueue(ctx)

	c.startPipingQueuedLogsToSurrealDB(ctx)

	<-ctx.Done()
	return nil
}

// startReadingLogs starts reading logs from the configured paths and sends them to the queue.
// It also starts watching the configured paths for new log files and starts collecting logs if
// a new file is detected.
func (c *Collector) startReadingLogs(ctx context.Context) error {
	for _, path := range c.cfg.Collector.WatchPaths {
		if err := c.loadPath(path.Path); err != nil {
			return fmt.Errorf("failed to load path: %w", err)
		}

		go c.watchPath(ctx, path.Path)
	}

	return nil
}

// startPipingLogsToQueue starts piping read logs to the queue.
func (c *Collector) startPipingLogsToQueue(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case line := <-c.logLinesCh:
				var msg map[string]interface{}
				if err := json.Unmarshal(line, &msg); err != nil {
					log.Printf("Failed to unmarshal log entry: %v", err)
				}

				logEntry := LogEntry{
					Message:   msg,
					Timestamp: time.Now(),
					Source:    "default",
					File:      "info",
				}

				retries := 10

				for i := 0; i < retries; i++ {
					if err := c.queue.Write(&logEntry); err != nil {
						log.Printf("Failed to write log entry. Retrying...: %v", err)
						time.Sleep(5 * time.Second)
						continue
					}

					break
				}
			}
		}
	}()
}

// startPipingQueuedLogsToSurrealDB starts piping logs from the queue to SurrealDB.
func (c *Collector) startPipingQueuedLogsToSurrealDB(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				log.Printf("Writing queued logs to SurrealDB")
				logEntries, nBytes, err := c.queue.Peek(10)
				if err != nil {
					log.Printf("Failed to peek logs: %v", err)
				}

				log.Printf("Peeked %d log entries", len(logEntries))

				var numInserts int
				for _, logEntry := range logEntries {
					insert, err := surrealdb.Insert[LogEntry](c.db, "log", logEntry)
					if err != nil {
						log.Printf("Failed to insert log entry: %v", err)
					}

					numInserts++

					log.Printf("Inserted log entry: %v", insert)
				}

				log.Printf("Inserted %d log entries", numInserts)

				if numInserts > 0 {
					if err := c.queue.MarkRead(*nBytes); err != nil {
						log.Printf("Failed to mark logs as read: %v", err)
					}
				}
			}
		}
	}()
}

func (c *Collector) loadPath(path string) error {
	if err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if err := c.tailerGroup.startTailing(path); err != nil {
			return fmt.Errorf("failed to start collecting logs from %s: %w", path, err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("failed to walk path: %w", err)
	}

	return nil
}

func (c *Collector) watchPath(ctx context.Context, path string) {
	log.Printf("Watching path: %s", path)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Failed to create watcher: %v", err)
	}

	watcher.Add(path)

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-watcher.Events:
			c.handleEvent(event)
		}
	}
}

func (c *Collector) handleEvent(event fsnotify.Event) {
	if event.Op&fsnotify.Create == fsnotify.Create {
		log.Printf("New file detected: %s", event.Name)

		if err := c.tailerGroup.startTailing(event.Name); err != nil {
			log.Printf("Failed to start collecting logs from %s: %v", event.Name, err)
		}
	} else if event.Op&fsnotify.Rename == fsnotify.Rename {
		// Note that Rename is followed by a Create event.
		//
		// In general, we need to track the inode of the file to know which file is being renamed to which file.
		//
		// In the context of surreallogs, we assume any rename is due to a log rotation,
		// which means updates to the renamed file will eventually stop.
		//
		// But we cannot just ignore the rename event and treat it like a trigger to immediately stop following the log file.
		// Unlike the Remove event, where we can immediately stop following the log file, because it is now gone,
		// the renamed file is still there, and there could be updates we have not yet seen, due to race between the updates, our following, and the rename.
		//
		// So we need to keep following the log file for a while after the rename event.
		log.Printf("File renamed: %s", event.Name)

		if err := c.tailerGroup.stopTailing(event.Name); err != nil {
			log.Printf("Failed to stop collecting logs from %s: %v", event.Name, err)
		}
	} else if event.Op&fsnotify.Remove == fsnotify.Remove {
		log.Printf("File removed: %s", event.Name)

		if err := c.tailerGroup.stopTailing(event.Name); err != nil {
			log.Printf("Failed to stop collecting logs from %s: %v", event.Name, err)
		}
	}
}
