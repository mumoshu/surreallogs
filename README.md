# SurrealLogs

SurrealLogs is a tool for collecting logs from a variety of sources and storing them in a SurrealDB database.

As SurrealDB being a multi-model database with rich builtin functions and query capabilities, it is a natural fit for log collection and analysis.

## Status

Currently, it supports configuration via a YAML file, and will collect logs from the specified local directory and files.

It also supports log files that can be truncated and rotated, like other standard log collectors.

## Usage

```
collector --config config.yaml
```

## Configuration

The basic configuration is as follows:

```
# Specify the SurrealDB connection information.
surrealdb:
  url: ws://localhost:8000
  table: logs
  user: root
  pass: root

# Specify the collector configuration.
collector:
  # Specify the directory to store the read position.
  read_pos_dir: /tmp/surreallogs.pos
  # Specify the interval to read the logs.
  read_interval: 2s
  # Specify the paths to watch for logs.
  watch_paths:
    - path: ./logs/myapp
      recursive: true
  # Specify the buffer configuration.
  buffer:
    path: /tmp/surreallogs.buffer
    max_size: 1MB
    flush_interval: 3s
```

## Roadmap

- [ ] [Support Kubernetes pod logs](#support-kubernetes-pod-logs)

## Why Go?

I can't miss Kubernetes support, and Go has the official client library for Kubernetes. See [Support Kubernetes pod logs](#support-kubernetes-pod-logs).

### Support Kubernetes pod logs

In the future, we may extend this to support more sources, like Kubernetes pod logs.

Internally and implementation-wise, it should be as easy as pointing the already existing local log file collection to the Kubernetes pod log file stored under the node's `/var/log/pods/` directory, and add Kubernestes-specific metadata to the log record.

As we are based on Golang, we can use the upsteram client-go library to collect the Kubernetes metadata reliabily.

## Why a new tool?

My aim is to have a simple and all-in-one tool that can collect logs into SurrealDB, so that I don't have to worry about the growing set of plugins and changing plugin systems like seen in standard log collectors.
