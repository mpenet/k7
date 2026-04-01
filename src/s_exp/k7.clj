(ns s-exp.k7
  (:import (java.nio ByteBuffer ByteOrder MappedByteBuffer)
           (java.nio.channels FileChannel FileChannel$MapMode)
           (java.nio.file Files Path Paths StandardOpenOption OpenOption)
           (java.nio.file.attribute FileAttribute)
           (java.util.concurrent.atomic AtomicLong)
           (java.util.concurrent.locks LockSupport)
           (java.util.zip CRC32C)))

(set! *warn-on-reflection* true)

;;; ============================================================
;;; Constants
;;; ============================================================

(def ^:const ^byte MAGIC (byte 0x4B)) ; 'K'
(def ^:const ^byte FLAG_COMMITTED (byte 0x01))
(def ^:const ^int FRAME_HEADER_SIZE 10) ; magic(1)+flags(1)+length(4)+crc(4)
(def ^:const ^int FRAME_ALIGN 8)
(def ^:const ^long FRAME_ALIGN_MASK (dec FRAME_ALIGN)) ; for bitwise alignment
(def ^:const ^long DEFAULT_SEGMENT_SIZE (* 256 1024 1024)) ; 256MB
(def ^:const ^int CURSOR_FILE_SIZE 4096) ; one page

;; Pre-allocated zero array for frame padding — avoids per-write allocation.
;; FRAME_ALIGN bytes is the maximum padding ever needed.
(def ^:private ^bytes ZERO_PAD (byte-array FRAME_ALIGN))

;; AtomicLong.lazySet provides store-release semantics, equivalent to
;; VarHandle.setRelease, without needing a privileged module lookup.
;; Used in enqueue! and the commit thread to publish frame writes to readers.

;;; ============================================================
;;; Frame encoding / decoding
;;; ============================================================

(defn aligned-frame-size ^long [^long payload-len]
  (let [total (+ FRAME_HEADER_SIZE payload-len)
        r (bit-and total FRAME_ALIGN_MASK)]
    (if (zero? r) total (+ total (- FRAME_ALIGN r)))))

(defn write-frame!
  "Write a framed message into buf at position pos.
   Returns total aligned bytes written."
  ^long [^ByteBuffer buf ^long pos ^bytes data]
  (let [len (alength data)
        frame-size (aligned-frame-size len)
        crc (doto (CRC32C.) (.update data 0 len))
        crc-val (unchecked-int (.getValue crc))
        pad (int (- frame-size FRAME_HEADER_SIZE len))]
    (.position buf (int pos))
    (.put buf MAGIC)
    (.put buf FLAG_COMMITTED)
    (.putInt buf len)
    (.putInt buf crc-val)
    (.put buf data 0 len)
    (when (pos? pad)
      (.put buf ZERO_PAD 0 pad))
    frame-size))

(defn valid-frame?
  "Returns true if there is a valid frame at pos in buf."
  [^ByteBuffer buf ^long pos ^long capacity]
  (when (< (+ pos FRAME_HEADER_SIZE) capacity)
    (let [magic (.get buf (int pos))
          flags (.get buf (int (inc pos)))]
      (when (and (== magic MAGIC) (== flags FLAG_COMMITTED))
        (let [len (.getInt buf (int (+ pos 2)))
              stored-crc (unchecked-int (.getInt buf (int (+ pos 6))))
              end (+ pos FRAME_HEADER_SIZE len)]
          (when (and (pos? len) (<= end capacity))
            ;; slice the mmap directly — no byte-array allocation
            ;; mmap is already BIG_ENDIAN; duplicate inherits byte order
            (let [slice (-> buf
                            .duplicate
                            (.position (int (+ pos FRAME_HEADER_SIZE)))
                            (.limit (int end))
                            .slice)
                  crc (doto (CRC32C.) (.update ^ByteBuffer slice))]
              (== stored-crc (unchecked-int (.getValue crc))))))))))

(defn read-frame-payload
  "Return a read-only ByteBuffer slice over the payload — zero copy.
   mmap is already BIG_ENDIAN; duplicate inherits byte order."
  ^ByteBuffer [^ByteBuffer mmap ^long pos]
  (let [len (.getInt mmap (int (+ pos 2)))]
    (-> mmap
        .duplicate
        (.position (int (+ pos FRAME_HEADER_SIZE)))
        (.limit (int (+ pos FRAME_HEADER_SIZE len)))
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
    (when (< (.size ch) segment-size)
      (.write ch (ByteBuffer/allocate 1) (dec segment-size)))
    (let [mmap (doto (.map ch FileChannel$MapMode/READ_WRITE 0 segment-size)
                 (.order ByteOrder/BIG_ENDIAN))]
      (Segment. ch mmap path base-offset
                (AtomicLong. 0)
                (AtomicLong. 0)
                segment-size))))

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

(deftype Queue [^Path dir
                ^long segment-size
                segments ; atom: vector of Segment
                ^AtomicLong global-write-offset
                commit-thread ; atom: Thread (nil when fsync-strategy != :async)
                open? ; atom: boolean
                fsync-strategy]
  java.io.Closeable
  (close [this] (close-queue! this)))

(defn- segment-path ^Path [^Path dir ^long base-offset]
  (.resolve dir (format "seg-%020d.k7" base-offset)))

(defn- cursor-path ^Path [^Path dir ^String group-id]
  (.resolve dir (format "cursor-%s.k7" group-id)))

(defn current-segment ^Segment [^Queue q]
  (peek @(.segments q)))

(defn- roll-segment! ^Segment [^Queue q]
  (let [base (.get ^AtomicLong (.global-write-offset q))
        new-seg (open-segment (segment-path ^Path (.dir q) base)
                              base
                              (.segment-size q))]
    (when-let [cur (peek @(.segments q))]
      (force-segment! cur))
    (swap! (.segments q) conj new-seg)
    new-seg))

(defn open-queue
  "Open (or recover) a queue stored at dir.

   Options:
     :segment-size       — bytes per segment file (default 256MB)
     :fsync-strategy     — :async (default), :flush, or :sync
                           :async  background thread fsyncs; low latency, small durability window
                           :flush  no explicit fsync; rely on OS writeback; no crash guarantee
                           :sync   fsync on every enqueue!; max durability, lowest throughput
     :commit-interval-us — fsync interval in microseconds for :async (default 50)"
  ([dir] (open-queue dir {}))
  ([dir {:keys [segment-size fsync-strategy commit-interval-us]
         :or {segment-size DEFAULT_SEGMENT_SIZE
              fsync-strategy :async
              commit-interval-us 50}}]
   (let [path (if (instance? Path dir)
                dir
                (Paths/get ^String dir (make-array String 0)))
         _ (Files/createDirectories path (make-array FileAttribute 0))
         q (Queue. path segment-size (atom []) (AtomicLong. 0) (atom nil) (atom true) fsync-strategy)
         existing (with-open [stream (Files/list path)]
                    (->> (.iterator stream)
                         iterator-seq
                         (filter (fn [^Path p]
                                   (let [n (.. p getFileName toString)]
                                     (and (.endsWith n ".k7")
                                          (.startsWith n "seg-")))))
                         (sort-by (fn [^Path p] (.. p getFileName toString)))
                         doall))] ; realize before stream closes]
     (if (seq existing)
       (let [segs (mapv (fn [^Path p]
                          (let [fname (.. p getFileName toString)
                                base (Long/parseLong (second (re-find #"seg-0*(\d+)" fname)))
                                seg (open-segment p base segment-size)]
                            (recover-segment! seg)
                            seg))
                        existing)
             last-seg ^Segment (peek segs)]
         (.set ^AtomicLong (.global-write-offset q)
               (+ (.base-offset last-seg) (.get ^AtomicLong (.write-pos last-seg))))
         (reset! (.segments q) segs))
       (roll-segment! q))
     ;; commit thread only used for :async strategy
     (when (= fsync-strategy :async)
       (let [commit-thread (doto (Thread.
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
                                          (LockSupport/parkNanos (* ^long commit-interval-us 1000))
                                          (recur seg)))))
                                  "k7-commit")
                             (.setDaemon true)
                             .start)]
         (reset! (.commit-thread q) commit-thread)))
     q)))

(defn close-queue! [^Queue q]
  (reset! (.open? q) false)
  ;; final flush before closing
  (when-let [seg ^Segment (current-segment q)]
    (force-segment! seg))
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
    (write-frame! ^MappedByteBuffer (.mmap seg) local-pos data)
    ;; lazySet (store-release): ensures write-frame! bytes are visible to any
    ;; thread that subsequently reads write-pos with .get (load-acquire),
    ;; forming a happens-before edge per JMM without a full volatile fence.
    (.lazySet ^AtomicLong (.write-pos seg) next-pos)
    (.set ^AtomicLong (.global-write-offset q) (+ global-off frame-size))
    (case (.fsync-strategy q)
      :async (LockSupport/unpark ^Thread @(.commit-thread q))
      :flush (.lazySet ^AtomicLong (.committed-pos seg) next-pos)
      :sync (do (force-segment! seg)
                (.set ^AtomicLong (.committed-pos seg) next-pos)))
    global-off))

;;; ============================================================
;;; Consumer group (crash-safe cursor in its own mmap'd file)
;;; ============================================================

;; cursor file layout:
;;   offset 0 : committed-offset (long) — fsynced by cursor-flush thread

;; Per-message delivery value — avoids map allocation on hot path
(deftype Msg [^long offset ^ByteBuffer payload])

(deftype ConsumerGroup [^String id
                        ^MappedByteBuffer cursor-mmap
                        ^FileChannel cursor-channel
                        ^AtomicLong read-head ; in-memory, tracks next-to-read
                        pending ; atom: sorted-set of unacked global offsets
                        ^Queue queue
                        cursor-dirty? ; atom: boolean, true when cursor needs flush
                        cursor-open? ; atom: boolean stop flag for flush thread
                        cursor-flush-thread ; atom: Thread (nil when strategy != :async)
                        cursor-fsync-strategy]
  java.io.Closeable
  (close [this] (close-consumer-group! this)))

(defn open-consumer-group
  "Open or create a consumer group on queue q identified by group-id.

   Options:
     :cursor-fsync-strategy — :async (default), :flush, or :sync
                              :async  background thread fsyncs cursor on ack
                              :flush  cursor written to mmap but not fsynced (no crash guarantee)
                              :sync   cursor fsynced on every ack (slowest)"
  ([q group-id] (open-consumer-group q group-id {}))
  ([^Queue q ^String group-id {:keys [cursor-fsync-strategy]
                               :or {cursor-fsync-strategy :async}}]
   (let [path (cursor-path ^Path (.dir q) group-id)
         opts (into-array OpenOption
                          [StandardOpenOption/READ
                           StandardOpenOption/WRITE
                           StandardOpenOption/CREATE])
         ch (FileChannel/open path opts)
         _ (when (< (.size ch) CURSOR_FILE_SIZE)
             (.write ch (ByteBuffer/allocate 1) (dec CURSOR_FILE_SIZE)))
         mmap (doto (.map ch FileChannel$MapMode/READ_WRITE 0 CURSOR_FILE_SIZE)
                (.order ByteOrder/BIG_ENDIAN))
         committed-off (.getLong ^ByteBuffer mmap 0)
         cg (ConsumerGroup. group-id mmap ch
                            (AtomicLong. committed-off)
                            (atom (sorted-set))
                            q
                            (atom false)
                            (atom true) ; cursor-open?
                            (atom nil)
                            cursor-fsync-strategy)]
     (when (= cursor-fsync-strategy :async)
       (let [flush-thread (doto (Thread.
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
                                 (str "k7-cursor-" group-id))
                            (.setDaemon true)
                            .start)]
         (reset! (.cursor-flush-thread cg) flush-thread)))
     cg)))

(defn close-consumer-group! [^ConsumerGroup cg]
  ;; Signal flush thread to stop, unpark it so it exits promptly,
  ;; then join to ensure the final flush completes before we close the channel.
  (reset! (.cursor-open? cg) false)
  (when-let [t ^Thread @(.cursor-flush-thread cg)]
    (LockSupport/unpark t)
    (.join t))
  ;; Unconditional final force covers :flush and :sync strategies too.
  (.force ^MappedByteBuffer (.cursor-mmap cg))
  (.close ^FileChannel (.cursor-channel cg)))

(defn- write-cursor! [^ConsumerGroup cg ^long offset]
  (.putLong ^ByteBuffer (.cursor-mmap cg) 0 offset)
  (case (.cursor-fsync-strategy cg)
    :async (do (reset! (.cursor-dirty? cg) true)
               (LockSupport/unpark ^Thread @(.cursor-flush-thread cg)))
    :flush nil ; mmap write sufficient; OS will flush eventually
    :sync (.force ^MappedByteBuffer (.cursor-mmap cg))))

(defn- contiguous-frontier
  "Lowest unacked offset (= safe commit point).
   If pending is empty the full read-head is committed."
  ^long [pending ^long read-head]
  (if (empty? pending)
    read-head
    (long (first pending))))

(defn ack!
  "Acknowledge a delivered message by its global offset.
   Updates cursor asynchronously via flush thread.
   Not thread-safe: must be called from the same thread as poll!."
  [^ConsumerGroup cg ^long global-offset]
  (let [frontier (-> (swap! (.pending cg) disj global-offset)
                     (contiguous-frontier (.get ^AtomicLong (.read-head cg))))]
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
  (reset! (.pending cg) (sorted-set))
  (.set ^AtomicLong (.read-head cg) offset)
  (write-cursor! cg offset))

(defn nack!
  "Negative-ack: rewinds read-head so the message is redelivered.
   Not thread-safe: must be called from the same thread as poll!."
  [^ConsumerGroup cg ^long global-offset]
  (swap! (.pending cg) disj global-offset)
  (let [cur (.get ^AtomicLong (.read-head cg))]
    (when (< global-offset cur)
      (.set ^AtomicLong (.read-head cg) global-offset))))

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
  "Attempt to read one message. Returns a Msg or nil."
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
          (let [mmap ^MappedByteBuffer (.mmap seg)]
            (when (valid-frame? mmap local-pos (.capacity seg))
              (let [payload (read-frame-payload mmap local-pos)
                    frame-size (frame-total-size mmap local-pos)
                    next-rh (+ rh frame-size)]
                (.set ^AtomicLong (.read-head cg) next-rh)
                (swap! (.pending cg) conj rh)
                (Msg. rh payload)))))))))

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
  (def q (open-queue "/tmp/k7-test"))
  (def cg (open-consumer-group q "workers"))

  (enqueue! q (.getBytes "hello"))
  (enqueue! q (.getBytes "world"))
  (enqueue! q (.getBytes "!"))

  (let [batch (poll! cg {:max-batch 10 :timeout-ms 5})]
    (doseq [^Msg msg batch]
      (println "offset:" (.offset msg) "msg:" (String. (payload->bytes (.payload msg))))
      (ack! cg (.offset msg))))

  (.getLong ^ByteBuffer (.cursor-mmap ^ConsumerGroup cg) 0)

  (close-consumer-group! cg)
  (close-queue! q)

  ;; reopen — should resume from committed offset
  (def q2 (open-queue "/tmp/k7-test"))
  (def cg2 (open-consumer-group q2 "workers"))
  (poll! cg2 {:timeout-ms 5})
  (close-consumer-group! cg2)
  (close-queue! q2))
