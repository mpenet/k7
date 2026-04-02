# k7
<img align="right" width="250" height="250" alt="b74cd8e1-e183-4521-9c34-68163a7bd26d" src="https://github.com/user-attachments/assets/dac4752e-2b7e-41ca-8477-0f70636f6357" />



A high-performance disk-backed queue for Clojure.

Single writer, multiple independent consumer groups, zero-copy reads via
memory-mapped files, adaptive batching, crash-safe cursors, and configurable
fsync strategies.

<br/>

## Features

- **Append-only log** backed by preallocated mmap'd segment files
- **Zero-copy reads** — payloads are read-only `ByteBuffer` slices into the mmap, no copying
- **Consumer groups** with independent cursors, each persisted in its own mmap'd file
- **Ack / nack / seek** — at-least-once delivery, redelivery on nack, arbitrary seek
- **Crash recovery** — on open, segments are scanned and the last valid frame is found via CRC32C
- **Configurable fsync strategy** per queue (`:async`, `:flush`, `:sync`) and per consumer group
- **`java.io.Closeable`** — both `Queue` and `ConsumerGroup` work with `with-open`
- Zero reflection, minimal allocations on the hot path
- Primitive `long[]`-backed pending-offset set — no `Long` boxing on ack/poll
- Thread-local `CRC32C` via `ThreadLocal/withInitial` — zero per-message allocation for checksums

## Installation

```clojure
;; deps.edn
{com.s-exp/k7 {:mvn/version "0.1.0"}}
```

## Quick start

```clojure
(require '[s-exp.k7 :as k7])
(import '(s_exp.k7 Msg))

;; Open a queue (creates the directory if needed)
(with-open [q  (k7/open-queue "/var/data/my-queue" {:fsync-strategy :flush})
            cg (k7/open-consumer-group q "workers")]

  ;; Produce
  (k7/enqueue! q (.getBytes "hello"))
  (k7/enqueue! q (.getBytes "world"))

  ;; Consume
  (let [batch (k7/poll! cg {:max-batch 10 :timeout-ms 50})]
    (doseq [^Msg msg batch]
      (println (.offset msg) "->" (String. (k7/payload->bytes (.payload msg))))
      (k7/ack! cg (.offset msg)))))
```

## API

### Queue

```clojure
(k7/open-queue dir)
(k7/open-queue dir opts)
```

Opens or recovers a queue stored in `dir` (a `String` or `java.nio.file.Path`).
Creates the directory if it does not exist. On open, any existing segments are
scanned and recovered.

| Option | Default | Description |
|--------|---------|-------------|
| `:segment-size` | `268435456` (256MB) | Bytes per segment file |
| `:fsync-strategy` | `:async` | See fsync strategies below |
| `:commit-interval-us` | `50` | Fsync interval in µs for `:async` strategy |

```clojure
(k7/enqueue! q ^bytes data)  ; => long global-offset
(k7/close-queue! q)
(k7/current-segment q)       ; => Segment (for diagnostics)
```

`enqueue!` is **not thread-safe** — single writer only.

### Consumer group

```clojure
(k7/open-consumer-group q group-id)
(k7/open-consumer-group q group-id opts)
```

Opens or creates a named consumer group on queue `q`.
Multiple groups are fully independent and each maintains its own crash-safe cursor.

| Option | Default | Description |
|--------|---------|-------------|
| `:cursor-fsync-strategy` | `:async` | See fsync strategies below |

```clojure
(k7/poll! cg)
(k7/poll! cg opts)    ; => vector of Msg
(k7/ack!  cg offset)  ; advance committed cursor
(k7/nack! cg offset)  ; rewind for redelivery
(k7/seek! cg offset)  ; reset read position (0 = replay from beginning)
(k7/close-consumer-group! cg)
```

`poll!`, `ack!`, `nack!`, and `seek!` are **not thread-safe** — a `ConsumerGroup`
must be used from a single thread. Multiple consumer groups may run concurrently
on the same queue.

| Poll option | Default | Description |
|-------------|---------|-------------|
| `:max-batch` | `256` | Maximum messages per call |
| `:timeout-ms` | `1` | Max wait in milliseconds if queue is empty |
| `:park-ns` | `10000` (10µs) | Park duration between empty polls; `0` to busy-spin |

### Messages

Each `Msg` returned by `poll!` has two fields, accessed via Java interop:

```clojure
(.offset  ^Msg msg)   ; long — global offset, used for ack/nack
(.payload ^Msg msg)   ; read-only ByteBuffer — zero-copy slice into mmap
```

To copy the payload to a byte array:

```clojure
(k7/payload->bytes (.payload msg))
```

### Utilities

```clojure
(k7/aligned-frame-size payload-len)  ; on-disk size for a payload of this length
```

## Fsync strategies

Both `open-queue` (`:fsync-strategy`) and `open-consumer-group`
(`:cursor-fsync-strategy`) accept the same three values:

| Strategy | Durability | Throughput | Notes |
|----------|------------|------------|-------|
| `:async` | Small window | Highest | Background thread fsyncs every `:commit-interval-us` µs; also woken on each write |
| `:flush` | OS page cache | High | Written to mmap; survives process crash but not power loss |
| `:sync` | Full | Lowest | `msync` on every write |

## Crash recovery

On `open-queue`, each segment file is scanned from byte 0. Every frame is
validated by magic byte, committed flag, and CRC32C checksum. The write position
is advanced to the end of the last valid contiguous frame; any partial or torn
writes beyond that point are silently discarded.

Consumer group cursors survive restarts: the committed offset is stored in a
dedicated mmap'd cursor file and flushed according to `:cursor-fsync-strategy`.

## On-disk layout

```
data/
  seg-00000000000000000000.k7   ; first segment (preallocated to segment-size bytes)
  seg-00000000268435456000.k7   ; second segment (filename encodes base offset)
  cursor-workers.k7             ; cursor for consumer group "workers"
  cursor-analytics.k7           ; cursor for consumer group "analytics"
```

Frame format within a segment:

```
Byte:  0       1       2-5             6-9            10..N          N+1..aligned
       +-------+-------+---------------+---------------+---------------+-----------+
       | magic | flags | length(BE i32)| CRC32C(BE i32)| payload bytes | pad zeros |
       +-------+-------+---------------+---------------+---------------+-----------+
```

- `magic` — `0x4B` (`'K'`)
- `flags` — `0x01` = committed; `0x00` = not yet visible to readers
- `length` — payload byte count (big-endian int32)
- `CRC32C` — checksum of payload bytes (big-endian int32)
- Total frame size is padded to the next 8-byte boundary

## Threading model

| Component | Thread safety |
|-----------|---------------|
| `enqueue!` | Single writer only |
| `poll!` / `ack!` / `nack!` / `seek!` | Single thread per `ConsumerGroup` |
| Multiple `ConsumerGroup`s on one `Queue` | Safe — fully independent |
| `close-queue!` / `close-consumer-group!` | Call from the owner thread after stopping work |

Two daemon background threads are started when `:async` strategy is in use:

- `k7-commit` — periodically fsyncs the active segment for the queue
- `k7-cursor-<group>` — periodically fsyncs the cursor file for a consumer group

Both stop cleanly on close, flushing any pending data before exiting.

## Performance

Measured on Apple M-series, JVM 21, G1GC, 32-byte payloads.

**`enqueue!` throughput:**

| Strategy | Throughput |
|----------|------------|
| `:flush` | ~16.6M msg/s |
| `:async` | ~2.4M msg/s |
| `:sync` | ~3.7K msg/s |

`:async` throughput is limited by the `LockSupport/unpark` call on each write
waking the commit thread. `:sync` is bounded by `msync` latency (~267µs/call on
this hardware).

**`poll+ack` throughput (batch=64):**

| Strategy | Throughput |
|----------|------------|
| `:flush` | ~6.2M msg/s |
| `:async` | ~4.4M msg/s |
| `:sync` | ~49K msg/s |

**`poll+ack` throughput by batch size (`:flush`):**

| Batch size | Throughput |
|------------|------------|
| 1 | ~2.2M msg/s |
| 16 | ~4.2M msg/s |
| 64 | ~6.2M msg/s |
| 256 | ~4.3M msg/s |

`enqueue!` is zero-alloc. Per-message allocation in `poll!` is dominated by
the read-only `ByteBuffer` slice returned as the payload (~192 bytes); the
pending-offset tracking and CRC32C checksum contribute zero allocation.

## Development

```bash
# Run tests
clojure -M:test -m cognitect.test-runner -d test

# Start a REPL
clojure -M:nrepl

# Run benchmarks (criterium)
clojure -M:bench-run
```

## License

Copyright © 2026 Mpenet

Distributed under the Eclipse Public License version 1.0.
