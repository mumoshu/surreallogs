# Design

This document describes the design of SurrealLogs.

In one sentence, SurrealLogs is a log collector that uses a strictly bounded buffer to reliably collect logs from log files and Kubernetes pods, and send them to SurrealDB.

## Targets

1. Rotated log files

2. Truncated log files

3. Kubernetes pod logs

## Durability Guarantees

1. **File System Durability**
   - Log positions are persisted to disk
   - Buffer contents are backed by file system
   - Crash recovery through position tracking (both log receiving and sending sides)

2. **Data Loss Prevention**
   - Receiving side: The log tailing position advances only after the log has been wrote to the buffer, not before.
   - Sending side: Blocking writes when buffer is full. The buffer considers the log has been sent only after the log has been sent to SurrealDB.

3. **Recovery Mechanisms**
   - Periodic buffer flushing to permanent storage
   - Buffer state recovery after crashes
   - Resume from last known positions (at-least-once delivery guarantee)

## Future Extensions

1. **Kubernetes Support**
   - Pod log collection capabilities, through integration with Kubernetes log paths
   - Container metadata enrichment

## Design Decisions

1. **Why Go?**
   - Official Kubernetes client library support
   - Concurrency primitives
   - Standard library

2. **Why Circular Buffer?**
   - Straight-forward to limit the buffer size strictly and implement backpressure (alternatively we could have N smaller append-only buffer and rotate them though. Suggestions are welcome)

3. **Why File System Backing?**
   - Portability
   - No dependency on external services (compared to using another networked or distributed storage or cache as the buffer store)

## Limitations

1. **Buffer Size**
   - Fixed size buffer requires careful sizing
   - No dynamic growth capability (We will provide metrics and docs to properly size it)
   - Must be configured based on load and the available resource

2. **Log Format**
   - Currently assumes JSON format
   - No built-in format conversion
   - Limited metadata extraction (we don't want to invent our own big configuration language to make it fully customizable or even programmable, for now)

3. **Scale**
   - Single instance design, no built-in clustering (although one correctly-sized collector per node should be enough to accomoddate all the logs from processes/containers/pods on the node)
   - Limited by single SurrealDB connection (could add a connection pool or sharding to multiple SurrealDB instances if this is going to be a constant bottle-neck)

## Non-Goals

1. **Performance Optimizations**
   - Buffer size auto-tuning (In a containerized environment, this means you need to update the surrounding container's resource request/limit at runtime, which is not easy, and not always great)
   - Adaptive flush intervals (Not sure it worth the effort)
   - Compression support (Would use one if SurrealDB supports it)
