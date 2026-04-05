# k7
<img align="right" width="250" height="250" alt="b74cd8e1-e183-4521-9c34-68163a7bd26d" src="https://github.com/user-attachments/assets/dac4752e-2b7e-41ca-8477-0f70636f6357" />

⚠️ WIP - use at your own risk

A high-performance disk-backed queue for Clojure.

Single writer, multiple independent consumer groups, zero-copy reads via
memory-mapped files, adaptive batching, crash-safe cursors, and configurable
fsync strategies.

<br/>

## Features

- **High throughput** —  [~16M msg/s enqueue and ~6M msg/s poll+ack](#performance) (`:flush`, batch=64, Apple M1, JVM 21)
- **Append-only log** backed by preallocated mmap'd segment files
- **Zero-copy reads** — payloads are read-only `ByteBuffer` slices into the mmap
- **Consumer groups** with independent cursors, each persisted in its own mmap'd file
- **Ack / nack / seek** — at-least-once delivery, redelivery on nack, arbitrary seek
- **Crash recovery** — segments are scanned on open; the last valid CRC32C-verified frame is found automatically
- **Configurable fsync strategy** per queue (`:async`, `:flush`, `:sync`) and per consumer group
- **`java.io.Closeable`** — both `Queue` and `ConsumerGroup` work with `with-open`
- **Low allocation** — `enqueue!` is zero-alloc; `poll!` allocates only the read-only `ByteBuffer` slice per message


## Installation

```clojure
;; deps.edn
{com.s-exp/k7 {:mvn/version "0.1.0"}}
```

## Quick start

```clojure
(require '[s-exp.k7 :as k7])

;; Open a queue (creates the directory if needed)
(with-open [q  (k7/open-queue "/var/data/my-queue" {:fsync-strategy :flush})
            cg (k7/open-consumer-group q "workers")]

  ;; Produce
  (k7/enqueue! q (.getBytes "hello"))
  (k7/enqueue! q (.getBytes "world"))

  ;; Consume
  (let [batch (k7/poll! cg {:max-batch 10 :timeout-ms 50})]
    (doseq [msg batch]
      (println (k7/msg-offset msg) "->" (String. (k7/payload->bytes (k7/msg-payload msg))))
      (k7/ack! cg msg))))
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
(k7/ack!  cg msg)     ; advance committed cursor
(k7/nack! cg msg)     ; rewind for redelivery
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

```clojure
(k7/msg-offset  msg)  ; => long — global offset, used for ack/nack
(k7/msg-payload msg)  ; => read-only ByteBuffer — zero-copy slice into mmap
```

To copy the payload to a byte array:

```clojure
(k7/payload->bytes (k7/msg-payload msg))
```

## fsync strategies

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

## Performance

Measured on Apple M-series, JVM 21, G1GC, 32-byte payloads.

**`enqueue!` throughput:**

| Strategy | Throughput |
|----------|------------|
| `:flush` | ~16.6M msg/s |
| `:async` | ~5.0M msg/s |
| `:sync` | ~3.7K msg/s |

`:sync` is bounded by `msync` latency (~267µs/call on this hardware).

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


### Rationale

This is the same ownership model used by [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/)
and [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue): push
coordination responsibility to the caller so the library's hot path stays
allocation-free and contention-free.

**Single writer.** `enqueue!` requires external synchronization if called from
multiple threads. When only one thread ever calls it, the hot path is entirely
free of locks, CAS loops, and memory fences: writing a frame and advancing the
write position becomes a plain array write followed by a single lightweight
atomic publish. Readers on other threads pick up the new position automatically.

**Single thread per `ConsumerGroup`.** The cursor, pending-offset set, and
read-head are all private to one `ConsumerGroup` and updated together as a unit.
Keeping them on one thread means none of that state needs synchronization.
Readers don't share anything with each other, so opening multiple consumer
groups is fully concurrent with no extra cost.

### Writing from multiple threads

`enqueue!` must be called from a single thread. Two options for multiple producers:

**Lock** — simplest approach, fine when `enqueue!` is fast (`:flush` /
`:async`), this way thread context doesn't matter, you can have multiple producers:

```clojure
(future 
  (locking q
    (k7/enqueue! q data)))
```

**Dedicated writer thread** — better when producers are latency-sensitive or
when using `:sync` strategy (where each write blocks for an fsync). Producers
hand off data and return immediately; the writer thread drains the inbox:

```clojure
(let [inbox (java.util.concurrent.LinkedBlockingQueue.)]
  ;; writer thread owns the Queue
  (future
    (loop []
      (when-let [data (.take inbox)]
        (k7/enqueue! q data)
        (recur))))

  ;; any thread can now safely produce
  (.put inbox (.getBytes "hello"))
  (.put inbox (.getBytes "world")))
```

### Consuming from multiple threads

If you need to fan out work to a thread pool, the natural pattern is a single
**reader thread** that owns the `ConsumerGroup` and dispatches payloads to
workers. Acks are collected back on the same thread:

```clojure
(let [results (java.util.concurrent.LinkedBlockingQueue.)]

  ;; reader thread owns the ConsumerGroup
  (future
    (loop []
      (doseq [msg (k7/poll! cg {:max-batch 64 :timeout-ms 5})]
        (future
          (process (k7/msg-payload msg))
          ;; hand msg back for acking
          (.put results msg)))
      ;; drain completed msgs and ack on the reader thread
      (loop []
        (when-let [msg (.poll results)]
          (k7/ack! cg msg)
          (recur)))
      (recur))))
```

Alternatively, open one `ConsumerGroup` per worker thread — each will track its
own cursor independently and progress at its own pace.

## core.async integration (`s-exp.k7.async`)

The `s-exp.k7.async` namespace provides two higher-level helpers built on top of
the core threading rules. Requires `org.clojure/core.async` on the classpath.

### `sink!`

```clojure
(require '[s-exp.k7.async :as k7a])

(k7a/sink! q ch)
```

Blocks the calling thread, taking byte arrays from `ch` and calling `enqueue!`
on `q` until `ch` is closed. Wrap in `a/thread` or `future` to run in the
background.

### `producer-chan`

```clojure
(k7a/producer-chan q)
(k7a/producer-chan q :ch custom-ch)
```

Starts a dedicated writer thread and returns a channel. Put byte arrays onto the
channel from any thread; close it to stop the writer.

Multiple `producer-chan`s on the same queue are safe — each serializes its
`enqueue!` calls with a lock on `q`. For single-producer use, prefer `sink!`
which avoids locking entirely.

| Option | Default | Description |
|--------|---------|-------------|
| `:ch` | `(a/chan 256)` | Supply your own input channel |

```clojure
(let [ch (k7a/producer-chan q)]
  ;; put from any thread
  (a/put! ch (.getBytes "hello"))
  (a/put! ch (.getBytes "world"))
  (a/close! ch))  ; stops the writer
```

### `consumer-group-chan`

```clojure
(k7a/consumer-group-chan q group-id & opts)
```

Opens a `ConsumerGroup` on `q` for `group-id` and starts a dedicated reader
thread that polls and delivers `Msg` values onto a channel. Messages are
auto-acked immediately after being placed on the output channel (at-most-once
delivery).

Returns a map:

| Key | Description |
|-----|-------------|
| `:ch` | `core.async` channel of `Msg` values |
| `:stop-ch` | promise-chan; put any value to stop the consumer |

| Option | Default | Description |
|--------|---------|-------------|
| `:ch` | `(a/chan 256)` | Supply your own output channel |
| `:poll-opts` | `{:max-batch 64 :timeout-ms 5}` | Passed to `k7/poll!` |
| `:cg-opts` | `{}` | Passed to `k7/open-consumer-group` |

Stop by putting onto `:stop-ch` or by closing `:ch`. The `ConsumerGroup` is
closed before the reader thread exits.

```clojure
(let [c (k7a/consumer-group-chan q "workers"
                                 :poll-opts {:max-batch 32 :timeout-ms 10})]
  ;; consume
  (a/<!! (:ch c))   ; => Msg

  ;; stop
  (a/put! (:stop-ch c) true))
```

## Development

```bash
# Run tests
clojure -M:test -m cognitect.test-runner -d test

# Run benchmarks (criterium)
clojure -M:bench-run
```

## License

Copyright © 2026 Max Penet

Distributed under the Eclipse Public License version 1.0.
