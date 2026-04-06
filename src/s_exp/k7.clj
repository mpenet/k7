(ns s-exp.k7
  (:import (java.nio ByteBuffer ByteOrder MappedByteBuffer)
           (java.nio.channels FileChannel FileChannel$MapMode)
           (java.nio.file Files Path Paths StandardOpenOption OpenOption)
           (java.nio.file.attribute FileAttribute)
           (java.util Arrays)
           (java.util.concurrent.atomic AtomicLong)
           (java.util.concurrent.locks LockSupport)
           (java.util.function Supplier)
           (java.util.zip CRC32C)))

(set! *warn-on-reflection* true)

;;; ============================================================
;;; Constants
;;; ============================================================

(def ^:const ^byte magic (byte 0x4B)) ; 'K'
(def ^:const ^byte flag-committed (byte 0x01))
(def ^:const ^int frame-header-size 10) ; magic(1)+flags(1)+length(4)+crc(4)
(def ^:const ^int frame-align 8)
(def ^:const ^long frame-align-mask (dec frame-align)) ; for bitwise alignment
(def ^:const ^long default-segment-size (* 256 1024 1024)) ; 256MB
(def ^:const ^int cursor-file-size 4096) ; one page

;; Pre-allocated zero array for frame padding — avoids per-write allocation.
;; frame-align bytes is the maximum padding ever needed.
(def ^:private ^bytes zero-pad (byte-array frame-align))

;; Thread-local CRC32C instances — avoids per-message allocation on hot path.
;; CRC32C is not thread-safe, so each thread gets its own instance.
;; ThreadLocal/withInitial takes a Supplier — compiled to a direct interface call,
;; no reflective dispatch unlike proxy.
(def ^:private ^ThreadLocal tl-crc32c
  (ThreadLocal/withInitial (reify Supplier (get [_] (CRC32C.)))))

;; AtomicLong.lazySet provides store-release semantics, equivalent to
;; VarHandle.setRelease, without needing a privileged module lookup.
;; Used in enqueue! and the commit thread to publish frame writes to readers.

;;; ============================================================
;;; Primitive pending-offset set
;;; ============================================================

;; LongRingBuffer: a FIFO queue of long primitives for tracking pending offsets.
;;
;; Offsets are always added in ascending order (poll! reads sequentially).
;; In the common case (ack in-order) the removed offset is always at the head,
;; making both add and remove O(1) — just index increments, no data movement.
;; Out-of-order removal (nack redelivery) falls back to a linear scan + single
;; element removal, which is rare in practice.
;;
;; Capacity is always a power of 2 so index wrapping uses a bitmask instead
;; of modulo. Grows by doubling when full, copying in logical order.
;;
;; definterface exposes typed methods so callers get direct invokevirtual
;; with no reflection. set! on ^:unsynchronized-mutable fields is only
;; allowed inside the deftype body, so all mutation lives in the impl below.
;;
;; All operations are single-threaded (consumer-group constraint).
(definterface ILongRingBuffer
  (^void lrbAdd [^long v])
  (^void lrbRemove [^long v])
  (^long lrbFirst [])
  (^boolean lrbEmpty [])
  (^void lrbClear []))

(deftype LongRingBuffer [^:unsynchronized-mutable ^longs data
                         ^:unsynchronized-mutable ^int head  ; index of first element
                         ^:unsynchronized-mutable ^int tail] ; index of next write slot
  ILongRingBuffer
  (lrbAdd [this v]
    (let [arr data
          cap (alength arr)
          mask (unchecked-dec-int cap)
          sz (unchecked-subtract-int tail head)]
      (if (< sz cap)
        (do (aset arr (bit-and tail mask) v)
            (set! tail (unchecked-inc-int tail)))
        ;; grow: double capacity, copy in logical order
        (let [new-cap (bit-shift-left cap 1)
              new-arr (long-array new-cap)]
          (dotimes [i sz]
            (aset new-arr i (aget arr (bit-and (unchecked-add-int head i) mask))))
          (aset new-arr sz v)
          (set! data new-arr)
          (set! head (int 0))
          (set! tail (unchecked-inc-int sz))))))
  (lrbRemove [this v]
    (let [arr data
          cap (alength arr)
          mask (unchecked-dec-int cap)
          sz (unchecked-subtract-int tail head)]
      (when (pos? sz)
        ;; fast path: value is at head (in-order ack)
        (if (== v (aget arr (bit-and head mask)))
          (set! head (unchecked-inc-int head))
          ;; slow path: scan for value, shift tail portion left by one
          (loop [i 1]
            (when (< i sz)
              (if (== v (aget arr (bit-and (unchecked-add-int head i) mask)))
                (do (loop [j i]
                      (when (< j (unchecked-dec-int sz))
                        (aset arr (bit-and (unchecked-add-int head j) mask)
                              (aget arr (bit-and (unchecked-add-int head (unchecked-inc-int j)) mask)))
                        (recur (unchecked-inc-int j))))
                    (set! tail (unchecked-dec-int tail)))
                (recur (unchecked-inc-int i)))))))))
  (lrbFirst [this]
    (aget data (bit-and head (unchecked-dec-int (alength data)))))
  (lrbEmpty [this] (== head tail))
  (lrbClear [this]
    (set! head (int 0))
    (set! tail (int 0))))

(defn lrb-new
  "Create a new empty LongRingBuffer with the given initial capacity (rounded up to power of 2)."
  (^LongRingBuffer [] (lrb-new 16))
  (^LongRingBuffer [^long init-cap]
   ;; ensure power of 2
   (let [cap (loop [c (int 1)] (if (>= c init-cap) c (recur (bit-shift-left c 1))))]
     (LongRingBuffer. (long-array cap) 0 0))))

(defn lrb-add! [^ILongRingBuffer lrb ^long v] (.lrbAdd lrb v))
(defn lrb-remove! [^ILongRingBuffer lrb ^long v] (.lrbRemove lrb v))
(defn lrb-first ^long [^ILongRingBuffer lrb] (.lrbFirst lrb))
(defn lrb-empty? [^ILongRingBuffer lrb] (.lrbEmpty lrb))
(defn lrb-clear! [^ILongRingBuffer lrb] (.lrbClear lrb))

;;; ============================================================
;;; Frame encoding / decoding
;;; ============================================================

(defn aligned-frame-size ^long [^long payload-len]
  (let [total (+ frame-header-size payload-len)
        r (bit-and total frame-align-mask)]
    (if (zero? r) total (+ total (- frame-align r)))))

(defn write-frame!
  "Write a framed message into buf at position pos.
   frame-size must be (aligned-frame-size (alength data)) — passed in to avoid
   recomputing it since enqueue! already has this value."
  ^long [^ByteBuffer buf ^long pos ^bytes data ^long frame-size]
  (let [len (alength data)
        crc ^CRC32C (.get tl-crc32c)
        _ (doto crc (.reset) (.update data 0 len))
        crc-val (unchecked-int (.getValue crc))
        pad (int (- frame-size frame-header-size len))]
    (.position buf (int pos))
    (.put buf magic)
    (.put buf flag-committed)
    (.putInt buf len)
    (.putInt buf crc-val)
    (.put buf data 0 len)
    (when (pos? pad)
      (.put buf zero-pad 0 pad))
    frame-size))

(defn valid-frame?
  "Returns true if there is a valid frame at pos in buf."
  [^ByteBuffer buf ^long pos ^long capacity]
  (when (< (+ pos frame-header-size) capacity)
    (let [magic-byte (.get buf (int pos))
          flags (.get buf (int (inc pos)))]
      (when (and (== magic-byte magic) (== flags flag-committed))
        (let [len (.getInt buf (int (+ pos 2)))
              stored-crc (unchecked-int (.getInt buf (int (+ pos 6))))
              end (+ pos frame-header-size len)]
          (when (and (pos? len) (<= end capacity))
            ;; slice the mmap directly — no byte-array allocation
            ;; mmap is already BIG_ENDIAN; duplicate inherits byte order
            (let [slice (-> buf
                            .duplicate
                            (.position (int (+ pos frame-header-size)))
                            (.limit (int end))
                            .slice)
                  crc ^CRC32C (.get tl-crc32c)
                  _ (doto crc (.reset) (.update ^ByteBuffer slice))]
              (== stored-crc (unchecked-int (.getValue crc))))))))))

(defn read-frame-payload
  "Return a read-only ByteBuffer slice over the payload — zero copy.
   mmap is already BIG_ENDIAN; duplicate inherits byte order."
  ^ByteBuffer [^ByteBuffer mmap ^long pos]
  (let [len (.getInt mmap (int (+ pos 2)))]
    (-> mmap
        .duplicate
        (.position (int (+ pos frame-header-size)))
        (.limit (int (+ pos frame-header-size len)))
        .slice
        .asReadOnlyBuffer)))

(defn frame-total-size ^long [^ByteBuffer buf ^long pos]
  (aligned-frame-size (.getInt buf (int (+ pos 2)))))

;;; ============================================================
;;; Segment
;;; ============================================================

(deftype Segment [^FileChannel channel
                  ^MappedByteBuffer mmap
                  ^Path path
                  ^long base-offset
                  ^AtomicLong write-pos
                  ^AtomicLong committed-pos
                  ^long capacity])

(defn open-segment ^Segment [^Path path ^long base-offset ^long segment-size]
  (let [opts (into-array OpenOption
                         [StandardOpenOption/READ
                          StandardOpenOption/WRITE
                          StandardOpenOption/CREATE])
        ch (FileChannel/open path opts)]
    (try
      (when (< (.size ch) segment-size)
        (.write ch (ByteBuffer/allocate 1) (dec segment-size)))
      (let [mmap (doto (.map ch FileChannel$MapMode/READ_WRITE 0 segment-size)
                   (.order ByteOrder/BIG_ENDIAN))]
        (Segment. ch mmap path base-offset
                  (AtomicLong. 0)
                  (AtomicLong. 0)
                  segment-size))
      (catch Throwable t
        (.close ch)
        (throw t)))))

(defn close-segment! [^Segment seg]
  (.force ^MappedByteBuffer (.mmap seg))
  (.close ^FileChannel (.channel seg)))

(defn force-segment! [^Segment seg]
  (.force ^MappedByteBuffer (.mmap seg)))

(defn segment-full? [^Segment seg ^long frame-size]
  (>= (+ (.get ^AtomicLong (.write-pos seg)) frame-size) (.capacity seg)))

;;; ============================================================
;;; Crash recovery
;;; ============================================================

(defn recover-segment!
  "Scan frames from position 0, set write-pos/committed-pos to end
   of last valid contiguous frame."
  [^Segment seg]
  (let [mmap ^MappedByteBuffer (.mmap seg)
        cap (.capacity seg)]
    (loop [pos 0]
      (if (valid-frame? mmap pos cap)
        (let [size (frame-total-size mmap pos)
              next-pos (+ pos size)]
          (if (< next-pos cap)
            (recur next-pos)
            (do
              (.set ^AtomicLong (.write-pos seg) cap)
              (.set ^AtomicLong (.committed-pos seg) cap))))
        (do
          (.set ^AtomicLong (.write-pos seg) pos)
          (.set ^AtomicLong (.committed-pos seg) pos))))))

;;; ============================================================
;;; Queue
;;; ============================================================

;; fsync-strategy:
;;   :async — background commit thread fsyncs on interval (highest throughput)
;;   :flush — no explicit fsync, rely on OS page-cache writeback (no durability guarantee)
;;   :sync  — fsync on every write (lowest throughput, strongest durability)

(declare close-queue! close-consumer-group!)

;; IQueueState: typed getter/setter for the commit thread volatile fields.
;; definterface methods are direct JVM calls — no reflection, no boxing.
;; The setter is only called once (at open time); the getter is on the hot path
;; for every :async enqueue!.
;; commitParked tracks whether the commit thread is currently parked so
;; enqueue! can skip unpark when the thread is already running.
(definterface IQueueState
  (^Thread getCommitThread [])
  (^void setCommitThread [^Thread t])
  (^boolean getCommitParked [])
  (^void setCommitParked [^boolean v]))

(deftype Queue [^Path dir
                ^long segment-size
                segments ; atom: vector of Segment
                ^:volatile-mutable ^Thread commit-thread
                ^:volatile-mutable ^boolean commit-parked
                open? ; atom: boolean
                fsync-strategy]
  IQueueState
  (getCommitThread [_] commit-thread)
  (setCommitThread [_ t] (set! commit-thread t))
  (getCommitParked [_] commit-parked)
  (setCommitParked [_ v] (set! commit-parked v))
  java.io.Closeable
  (close [this] (close-queue! this)))

(defn- segment-path ^Path [^Path dir ^long base-offset]
  (.resolve dir (format "seg-%020d.k7" base-offset)))

(defn- cursor-path ^Path [^Path dir ^String group-id]
  (.resolve dir (format "cursor-%s.k7" group-id)))

(defn current-segment ^Segment [^Queue q]
  (peek @(.segments q)))

(defn- roll-segment! ^Segment [^Queue q]
  (let [cur (peek @(.segments q))
        base (if cur
               (+ (.base-offset ^Segment cur) (.get ^AtomicLong (.write-pos ^Segment cur)))
               0)
        new-seg (open-segment (segment-path ^Path (.dir q) base)
                              base
                              (.segment-size q))]
    (when cur
      (force-segment! cur))
    (swap! (.segments q) conj new-seg)
    new-seg))

(defn queue
  "Open (or recover) a queue stored at dir.

   Options:
     :segment-size       — bytes per segment file (default 256MB)
     :fsync-strategy     — :async (default), :flush, or :sync
                           :async  background thread fsyncs; low latency, small durability window
                           :flush  no explicit fsync; rely on OS writeback; no crash guarantee
                           :sync   fsync on every enqueue!; max durability, lowest throughput
     :commit-interval-us — fsync interval in microseconds for :async (default 50)"
  ([dir] (queue dir {}))
  ([dir {:keys [segment-size fsync-strategy commit-interval-us]
         :or {segment-size default-segment-size
              fsync-strategy :async
              commit-interval-us 50}}]
   (let [path (if (instance? Path dir)
                dir
                (Paths/get ^String dir (make-array String 0)))
         _ (Files/createDirectories path (make-array FileAttribute 0))
         q (Queue. path segment-size (atom []) nil false (atom true) fsync-strategy)
         existing (with-open [stream (Files/list path)]
                    (->> (.iterator stream)
                         iterator-seq
                         (filter (fn [^Path p]
                                   (let [n (.. p getFileName toString)]
                                     (and (.endsWith n ".k7")
                                          (.startsWith n "seg-")))))
                         (sort-by (fn [^Path p] (.. p getFileName toString)))
                         doall))] ; realize before stream closes
     (if (seq existing)
       ;; Open segments one by one, closing already-opened ones on failure
       ;; to avoid FileChannel leaks during recovery.
       (let [segs (reduce (fn [acc ^Path p]
                            (try
                              (let [fname (.. p getFileName toString)
                                    base (Long/parseLong (second (re-find #"seg-0*(\d+)" fname)))
                                    seg (open-segment p base segment-size)]
                                (recover-segment! seg)
                                (conj acc seg))
                              (catch Throwable t
                                (doseq [s acc] (close-segment! s))
                                (throw t))))
                          []
                          existing)]
         (reset! (.segments q) segs))
       (roll-segment! q))
     ;; commit thread only used for :async strategy
     (when (= fsync-strategy :async)
       (let [^Thread t (Thread.
                        ^Runnable
                        (fn []
                          ;; Cache the current segment; refresh only when it changes
                          ;; (segment roll), avoiding an atom deref on every tick.
                          (loop [cached ^Segment (current-segment q)]
                            (when @(.open? q)
                              (let [cur ^Segment (current-segment q)
                                    ;; refresh cache on segment roll
                                    seg ^Segment (if (identical? cur cached) cached cur)]
                                (when seg
                                  (let [wp (.get ^AtomicLong (.write-pos seg))
                                        cp (.get ^AtomicLong (.committed-pos seg))]
                                    (when (> wp cp)
                                      (force-segment! seg)
                                      (.lazySet ^AtomicLong (.committed-pos seg) wp))))
                                ;; park up to commit-interval-us; unparked early by enqueue!
                                ;; signal parked so enqueue! knows to unpark us
                                (.setCommitParked q true)
                                (LockSupport/parkNanos (* ^long commit-interval-us 1000))
                                (.setCommitParked q false)
                                (recur seg)))))
                        "k7-commit")]
         (.setDaemon t true)
         (.start t)
         (.setCommitThread q t)))
     q)))

(defn close-queue! [^Queue q]
  (reset! (.open? q) false)
  ;; Unpark and join the commit thread so any in-flight force completes
  ;; before we close the underlying FileChannels.
  (when-let [t (.getCommitThread q)]
    (LockSupport/unpark t)
    (.join t))
  ;; Final flush: close-segment! calls mmap.force + channel.close for each segment.
  (doseq [seg @(.segments q)]
    (close-segment! seg)))

;;; ============================================================
;;; Writer (single writer — not thread-safe)
;;; ============================================================

(defn enqueue!
  "Write data (byte array) to the queue.
   Returns the global offset of the written message.
   Not thread-safe: single writer only — must not be called concurrently.
   Durability depends on the queue's fsync-strategy:
     :async  — flushed by background thread; returns immediately
     :flush  — written to mmap; no fsync; returns immediately
     :sync   — fsynced before returning"
  ^long [^Queue q ^bytes data]
  (let [frame-size (aligned-frame-size (alength data))
        cur ^Segment (current-segment q)
        seg ^Segment (if (segment-full? cur frame-size) (roll-segment! q) cur)
        local-pos (.get ^AtomicLong (.write-pos seg))
        global-off (+ (.base-offset seg) local-pos)
        next-pos (+ local-pos frame-size)]
    (write-frame! ^MappedByteBuffer (.mmap seg) local-pos data frame-size)
    ;; lazySet (store-release): ensures write-frame! bytes are visible to any
    ;; thread that subsequently reads write-pos with .get (load-acquire),
    ;; forming a happens-before edge per JMM without a full volatile fence.
    (.lazySet ^AtomicLong (.write-pos seg) next-pos)
    (case (.fsync-strategy q)
      :async (when (.getCommitParked q) (LockSupport/unpark (.getCommitThread q)))
      :flush (.lazySet ^AtomicLong (.committed-pos seg) next-pos)
      :sync (do (force-segment! seg)
                (.set ^AtomicLong (.committed-pos seg) next-pos)))
    global-off))

;;; ============================================================
;;; Consumer group (crash-safe cursor in its own mmap'd file)
;;; ============================================================

;; cursor file layout:
;;   offset 0 : committed-offset (long) — fsynced by cursor-flush thread

;; Per-message delivery value — avoids map allocation on hot path.
;; payload is a read-only ByteBuffer slice over the mmap.
(deftype Msg [^long offset ^ByteBuffer payload])

(defn msg->offset ^long [^Msg m] (.offset m))
(defn msg->payload ^ByteBuffer [^Msg m] (.payload m))

;; ICGState: typed accessors for the two volatile fields of ConsumerGroup.
;;   pending             — mutable LongRingBuffer, polled/acked on every message
;;   cursor-flush-thread — set once at open, read on every :async ack
;; definterface methods compile to direct invokevirtual — no reflection, no wrapping.
(definterface ICGState
  (^s_exp.k7.ILongRingBuffer getPending [])
  (^void setPending [^s_exp.k7.ILongRingBuffer lrb])
  (^Thread getCursorFlushThread [])
  (^void setCursorFlushThread [^Thread t]))

(deftype ConsumerGroup [^String id
                        ^MappedByteBuffer cursor-mmap
                        ^FileChannel cursor-channel
                        ^AtomicLong read-head ; in-memory, tracks next-to-read
                        ^:volatile-mutable ^ILongRingBuffer pending
                        ^Queue queue
                        cursor-dirty? ; atom: boolean, true when cursor needs flush
                        cursor-open? ; atom: boolean stop flag for flush thread
                        ^:volatile-mutable ^Thread cursor-flush-thread
                        cursor-fsync-strategy]
  ICGState
  (getPending [_] pending)
  (setPending [_ lrb] (set! pending lrb))
  (getCursorFlushThread [_] cursor-flush-thread)
  (setCursorFlushThread [_ t] (set! cursor-flush-thread t))
  java.io.Closeable
  (close [this] (close-consumer-group! this)))

(defn consumer-group
  "Open or create a consumer group on queue q identified by group-id.

   Options:
     :cursor-fsync-strategy — :async (default), :flush, or :sync
                              :async  background thread fsyncs cursor on ack
                              :flush  cursor written to mmap but not fsynced (no crash guarantee)
                              :sync   cursor fsynced on every ack (slowest)"
  ([q group-id] (consumer-group q group-id {}))
  ([^Queue q ^String group-id {:keys [cursor-fsync-strategy]
                               :or {cursor-fsync-strategy :async}}]
   (let [path (cursor-path ^Path (.dir q) group-id)
         opts (into-array OpenOption
                          [StandardOpenOption/READ
                           StandardOpenOption/WRITE
                           StandardOpenOption/CREATE])
         ch (FileChannel/open path opts)]
     (try
       (when (< (.size ch) cursor-file-size)
         (.write ch (ByteBuffer/allocate 1) (dec cursor-file-size)))
       (let [mmap (doto (.map ch FileChannel$MapMode/READ_WRITE 0 cursor-file-size)
                    (.order ByteOrder/BIG_ENDIAN))
             committed-off (.getLong ^ByteBuffer mmap 0)
             cg (ConsumerGroup. group-id mmap ch
                                (AtomicLong. committed-off)
                                (lrb-new)
                                q
                                (atom false)
                                (atom true) ; cursor-open?
                                nil
                                cursor-fsync-strategy)]
         (when (= cursor-fsync-strategy :async)
           (let [^Thread t (Thread.
                            ^Runnable
                            (fn []
                              ;; Run while open; on stop do one final flush if dirty.
                              (while @(.cursor-open? cg)
                                (when @(.cursor-dirty? cg)
                                  (.force ^MappedByteBuffer (.cursor-mmap cg))
                                  (reset! (.cursor-dirty? cg) false))
                                (LockSupport/parkNanos 100000)) ; 100µs
                              ;; Final flush after stop signal
                              (when @(.cursor-dirty? cg)
                                (.force ^MappedByteBuffer (.cursor-mmap cg))
                                (reset! (.cursor-dirty? cg) false)))
                            (str "k7-cursor-" group-id))]
             (.setDaemon t true)
             (.start t)
             (.setCursorFlushThread cg t)))
         cg)
       (catch Throwable t
         (.close ch)
         (throw t))))))

(defn close-consumer-group! [^ConsumerGroup cg]
  ;; Signal flush thread to stop, unpark it so it exits promptly,
  ;; then join to ensure the final flush completes before we close the channel.
  (reset! (.cursor-open? cg) false)
  (when-let [t (.getCursorFlushThread cg)]
    (LockSupport/unpark t)
    (.join t))
  ;; Unconditional final force covers :flush and :sync strategies too.
  (.force ^MappedByteBuffer (.cursor-mmap cg))
  (.close ^FileChannel (.cursor-channel cg)))

(defn- write-cursor! [^ConsumerGroup cg ^long offset]
  (.putLong ^ByteBuffer (.cursor-mmap cg) 0 offset)
  (case (.cursor-fsync-strategy cg)
    :async (do (reset! (.cursor-dirty? cg) true)
               (LockSupport/unpark (.getCursorFlushThread cg)))
    :flush nil ; mmap write sufficient; OS will flush eventually
    :sync (.force ^MappedByteBuffer (.cursor-mmap cg))))

(defn- contiguous-frontier
  "Lowest unacked offset (= safe commit point).
   If pending is empty the full read-head is committed."
  ^long [^ILongRingBuffer pending ^long read-head]
  (if (lrb-empty? pending)
    read-head
    (lrb-first pending)))

(defn ack!
  "Acknowledge a delivered message.
   Updates cursor asynchronously via flush thread.
   Not thread-safe: must be called from the same thread as poll!."
  [^ConsumerGroup cg ^Msg msg]
  (let [global-offset (.offset msg)
        ^ILongRingBuffer p (.getPending cg)
        _ (lrb-remove! p global-offset)
        frontier (contiguous-frontier p (.get ^AtomicLong (.read-head cg)))]
    (write-cursor! cg frontier)))

(defn seek!
  "Reset the consumer read position to global-offset.
   Clears all pending unacked messages. The next poll! will deliver
   messages from offset onwards.
   Not thread-safe: must be called from the same thread as poll!.
   offset may be:
     - any valid global offset (e.g. from a previous Msg)
     - 0 to replay from the beginning of the queue"
  [^ConsumerGroup cg ^long offset]
  (lrb-clear! (.getPending cg))
  (.set ^AtomicLong (.read-head cg) offset)
  (write-cursor! cg offset))

(defn nack!
  "Negative-ack: rewinds read-head so the message is redelivered.
   Not thread-safe: must be called from the same thread as poll!."
  [^ConsumerGroup cg ^Msg msg]
  (let [global-offset (.offset msg)]
    (lrb-remove! (.getPending cg) global-offset)
    (let [cur (.get ^AtomicLong (.read-head cg))]
      (when (< global-offset cur)
        (.set ^AtomicLong (.read-head cg) global-offset)))))

;;; ============================================================
;;; Reader
;;; ============================================================

(defn- find-segment-for-offset
  "Find the segment whose base-offset range contains global-offset.
   Uses an indexed loop to avoid boxing in reduce."
  ^Segment [^Queue q ^long global-offset]
  (let [segs @(.segments q)
        n (count segs)]
    (loop [i 0
           best nil]
      (if (== i n)
        best
        (let [s ^Segment (nth segs i)
              base (.base-offset s)]
          (recur (inc i)
                 (if (and (<= base global-offset)
                          (or (nil? best)
                              (> base (.base-offset ^Segment best))))
                   s
                   best)))))))

(defn- try-read-one!
  "Attempt to read one message. Returns a Msg or nil.
   Fuses frame validation and payload extraction into one pass:
   one ByteBuffer.duplicate, one slice — reused for both CRC check and payload."
  [^ConsumerGroup cg ^Segment cached-seg]
  (let [q ^Queue (.queue cg)
        rh (.get ^AtomicLong (.read-head cg))
        ;; use cached segment if rh is still within it, else search
        seg ^Segment (if (and cached-seg
                              (let [base (.base-offset cached-seg)]
                                (and (<= base rh)
                                     (< (- rh base) (.capacity cached-seg)))))
                       cached-seg
                       (find-segment-for-offset q rh))]
    (when seg
      (let [seg ^Segment seg
            local-pos (- rh (.base-offset seg))
            committed (.get ^AtomicLong (.committed-pos seg))]
        (when (< local-pos committed)
          (let [mmap ^MappedByteBuffer (.mmap seg)
                cap (.capacity seg)]
            (when (< (+ local-pos frame-header-size) cap)
              (let [magic-byte (.get mmap (int local-pos))
                    flags (.get mmap (int (inc local-pos)))]
                (when (and (== magic-byte magic) (== flags flag-committed))
                  (let [len (.getInt mmap (int (+ local-pos 2)))
                        stored-crc (unchecked-int (.getInt mmap (int (+ local-pos 6))))
                        payload-start (int (+ local-pos frame-header-size))
                        end (int (+ local-pos frame-header-size len))]
                    (when (and (pos? len) (<= end cap))
                      ;; One duplicate, one slice — reused for both CRC validation
                      ;; and returned as the payload (rewound, made read-only).
                      (let [dup (.duplicate mmap)
                            _ (-> dup (.position payload-start) (.limit end))
                            slice (.slice dup)
                            crc ^CRC32C (.get tl-crc32c)
                            _ (doto crc (.reset) (.update ^ByteBuffer slice))]
                        (when (== stored-crc (unchecked-int (.getValue crc)))
                          (let [frame-size (aligned-frame-size len)
                                next-rh (+ rh frame-size)]
                            (.set ^AtomicLong (.read-head cg) next-rh)
                            (lrb-add! (.getPending cg) rh)
                            ;; read-only view: prevents callers from corrupting mmap data
                            (Msg. rh (-> slice .rewind .asReadOnlyBuffer))))))))))))))))

(defn poll!
  "Adaptive batch poll. Returns a vector of Msg records.
   Reads up to max-batch messages or blocks up to timeout-ms before returning
   whatever has accumulated (possibly empty).
   Access msg fields via (.offset msg) and (.payload msg).

   Not thread-safe: a ConsumerGroup must be used from a single thread.
   Multiple independent consumer groups may read the same Queue concurrently.

   Options:
     :max-batch   — maximum messages to return (default 256)
     :timeout-ms  — max wait in milliseconds (default 1)
     :park-ns     — nanoseconds to park between empty polls (default 10000 = 10µs).
                    Lower values reduce latency at the cost of CPU; 0 busy-spins."
  ([^ConsumerGroup cg] (poll! cg {}))
  ([^ConsumerGroup cg {:keys [max-batch timeout-ms park-ns]
                       :or {max-batch 256 timeout-ms 1 park-ns 10000}}]
   (let [deadline (+ (System/nanoTime) (long (* (double timeout-ms) 1e6)))
         park-ns  (long park-ns)
         ;; cache current segment at poll start — refreshed on miss in try-read-one!
         cached-seg ^Segment (current-segment (.queue cg))]
     (loop [msgs (transient [])
            n 0]
       (if-let [msg (try-read-one! cg cached-seg)]
         (let [n' (inc n)]
           (if (== n' ^int max-batch)
             (persistent! (conj! msgs msg))
             (recur (conj! msgs msg) n')))
         (if (>= (System/nanoTime) deadline)
           (persistent! msgs)
           (do
             (when (pos? park-ns)
               (LockSupport/parkNanos park-ns))
             (recur msgs n))))))))

;;; ============================================================
;;; Utilities
;;; ============================================================

(defn payload->bytes
  "Copy a payload ByteBuffer to a fresh byte array."
  ^bytes [^ByteBuffer bb]
  (let [arr (byte-array (.remaining bb))]
    (.get ^ByteBuffer (.duplicate bb) arr)
    arr))

;;; ============================================================
;;; REPL usage
;;; ============================================================

(comment
  (def q (queue "/tmp/k7-test"))
  (def cg (consumer-group q "workers"))

  (enqueue! q (.getBytes "hello"))
  (enqueue! q (.getBytes "world"))
  (enqueue! q (.getBytes "!"))

  (let [batch (poll! cg {:max-batch 10 :timeout-ms 5})]
    (doseq [^Msg msg batch]
      (println "offset:" (.offset msg) "msg:" (String. (payload->bytes (.payload msg))))
      (ack! cg msg)))

  (.getLong ^ByteBuffer (.cursor-mmap ^ConsumerGroup cg) 0)

  (close-consumer-group! cg)
  (close-queue! q)

  ;; reopen — should resume from committed offset
  (def q2 (queue "/tmp/k7-test"))
  (def cg2 (consumer-group q2 "workers"))
  (poll! cg2 {:timeout-ms 5})
  (close-consumer-group! cg2)
  (close-queue! q2))
