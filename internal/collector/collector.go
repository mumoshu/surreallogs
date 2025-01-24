package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surreallogs/internal/config"
	"github.com/surrealdb/surreallogs/internal/metadata"
)

// Collector is the unit that collects logs from the configured paths, enriches them with metadata provided by the same metadata provider,
// and writes them to the same SurrealDB instance.
//
// You can have two or more Collectors in case you need to collect logs from different paths,
// and write them to different SurrealDB instances.
type Collector struct {
	cfg   *config.Config
	db    *surrealdb.DB
	queue Queue

	watchingPaths map[string]struct{}

	logEntryCh chan *logEntry

	tailerGroup      TailerGroup
	metadataProvider metadata.Provider

	mu *sync.Mutex

	running bool
}

type TailerGroup interface {
	StartTailing(path string) error
	StopTailing(path string) error
	Close() error
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
	logEntryCh := make(chan *logEntry, 1024)

	tg := newTailerGroup(cfg.Collector.ReadPosDir, readInterval, logEntryCh)

	logLineReadBufferSize, err := ParseHumanReadableSize(cfg.Collector.LogLineReadBufferSize)
	if err != nil {
		return nil, fmt.Errorf("failed to parse log line read buffer size: %w", err)
	}

	tg.logLineReadBufferSize = logLineReadBufferSize

	metadataProvider, err := newMetadataProvider(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata provider: %w", err)
	}

	return &Collector{
		cfg:              cfg,
		db:               db,
		queue:            syncQueue,
		logEntryCh:       logEntryCh,
		tailerGroup:      tg,
		metadataProvider: metadataProvider,
		mu:               &sync.Mutex{},
	}, nil
}

func newMetadataProvider(cfg *config.Config) (metadata.Provider, error) {
	if k := cfg.Collector.Metadata.Kubernetes; k != nil {
		cacheTTL, err := time.ParseDuration(k.CacheTTL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse cache TTL: %w", err)
		}
		kubeconfig := k.Kubeconfig
		if kubeconfig == "" {
			kubeconfig = os.Getenv("KUBECONFIG")
		}
		if kubeconfig == "" {
			return nil, fmt.Errorf("no kubeconfig specified. Set collector.metadata.kubeconfig in config or KUBECONFIG envvar")
		}
		if _, err := os.Stat(kubeconfig); err != nil {
			return nil, fmt.Errorf("the specified kubeconfig %s could not be located: %w", kubeconfig, err)
		}
		kubeMeta, err := metadata.NewKube(kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Kubernetes metadata provider: %w", err)
		}
		p := metadata.NewCache(kubeMeta, cacheTTL)
		return p, nil
	}

	return metadata.NewNoop(), nil
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return errors.New("already running")
	}

	c.running = true
	c.mu.Unlock()

	if err := c.startReadingLogs(); err != nil {
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
func (c *Collector) startReadingLogs() error {
	if err := c.restartReadingLogs(); err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			log.Printf("Restarting log watch and tail")
			if err := c.restartReadingLogs(); err != nil {
				log.Printf("Failed to restart reading logs: %v", err)
			}
		}
	}()

	return nil
}

func (c *Collector) restartReadingLogs() error {
	paths := map[string]struct{}{}
	for _, path := range c.cfg.Collector.WatchPaths {
		if err := c.loadPath(path.Path, paths); err != nil {
			return fmt.Errorf("failed to load path: %w", err)
		}
	}

	current := c.watchingPaths
	if current == nil {
		c.watchingPaths = paths
		return nil
	}

	for path := range current {
		if _, ok := paths[path]; !ok {
			c.tailerGroup.StopTailing(path)
		}
	}

	for path := range paths {
		if _, ok := current[path]; !ok {
			if err := c.tailerGroup.StartTailing(path); err != nil {
				return fmt.Errorf("failed to start tailing path: %w", err)
			}
		}
	}

	c.watchingPaths = paths

	return nil
}

// startPipingLogsToQueue starts piping read logs to the queue.
func (c *Collector) startPipingLogsToQueue(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case entry := <-c.logEntryCh:
				var msg map[string]interface{}
				if err := json.Unmarshal(entry.line, &msg); err != nil {
					log.Printf("Failed to unmarshal log entry: %v", err)
					msg = map[string]interface{}{
						"message": map[string]string{
							"text": string(entry.line),
						},
					}
				}

				if err := c.metadataProvider.Fetch(ctx, entry.path, msg); err != nil {
					log.Printf("Failed to fetch metadata: %v", err)
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

func (c *Collector) loadPath(path string, paths map[string]struct{}) error {
	if err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		paths[path] = struct{}{}

		return nil
	}); err != nil {
		return fmt.Errorf("failed to walk path: %w", err)
	}

	return nil
}
