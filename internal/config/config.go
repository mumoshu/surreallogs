package config

import (
	"os"

	"github.com/goccy/go-yaml"
)

type Config struct {
	Collector struct {
		// ReadInterval is the interval at which the log file is read for new lines.
		ReadInterval string `yaml:"read_interval"`
		// ReadPosDir is the path to the directory where the log read position files are stored.
		// The position file for each watched path is stored in the directory with the name being "$INODE_NUMBER.pos".
		ReadPosDir string `yaml:"read_pos_dir"`
		WatchPaths []struct {
			Path      string `yaml:"path"`
			Recursive bool   `yaml:"recursive"`
		} `yaml:"watch_paths"`
		Buffer struct {
			// Path is the path to the buffer file.
			Path string `yaml:"path"`
			// MaxSize is the maximum size of the buffer file.
			MaxSize string `yaml:"max_size"`
			// FlushInterval is the interval at which the buffer file is flushed to SurrealDB.
			FlushInterval string `yaml:"flush_interval"`
		} `yaml:"buffer"`
		// The size of the bounded and reused buffer for batch-readling log lines from the collected log file
		LogLineReadBufferSize string `yaml:"log_line_read_buffer_size"`
		// The configuration for enriching log lines with Kubernetes metadata.
		Metadata struct {
			// The configuration for Kubernetes metadata provider.
			// Must be non-empty when you want to enrich log lines with Kubernetes metadata,
			// i.e. WatchPaths contain one with Path=/var/log/pods Recursive=true.
			Kubernetes *KubernetesMetadata `yaml:"kubernetes"`
		} `yaml:"metadata"`
	} `yaml:"collector"`

	SurrealDB SurrealDB `yaml:"surrealdb"`
}

type KubernetesMetadata struct {
	// The path to the kubeconfig file.
	// Defaults to the value of KUBECONFIG environment variable.
	Kubeconfig string `yaml:"kubeconfig"`
	// The cache TTL for Kubernetes metadata. For example, 1m means
	// the Kubernetes metadata fetching result is cached for 1 minute,
	// and the call to the Kubernetes API for the log file is throttled to at most only once per minute.
	CacheTTL string `yaml:"cache_ttl"`
}

type SurrealDB struct {
	URL       string `yaml:"url"`
	User      string `yaml:"user"`
	Pass      string `yaml:"pass"`
	Namespace string `yaml:"namespace"`
	Database  string `yaml:"database"`
	Table     string `yaml:"table"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
