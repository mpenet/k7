(ns com.s-exp.k7-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [s-exp.k7 :as k7])
  (:import (java.nio ByteBuffer)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (s_exp.k7 ConsumerGroup Msg Queue Segment)))

;;; ============================================================
;;; Fixtures
;;; ============================================================

(defn tmp-dir []
  (str (Files/createTempDirectory "k7-test-" (make-array FileAttribute 0))))

(defmacro with-queue [[q-sym dir opts] & body]
  `(let [~q-sym (k7/open-queue ~dir ~opts)]
     (try ~@body
          (finally (k7/close-queue! ~q-sym)))))

(defmacro with-consumer [[cg-sym q group opts] & body]
  `(let [~cg-sym (k7/open-consumer-group ~q ~group ~opts)]
     (try ~@body
          (finally (k7/close-consumer-group! ~cg-sym)))))

(defn msgs->strings [batch]
  (mapv #(String. (k7/payload->bytes (.payload ^Msg %))) batch))

(defn poll-all
  "Poll with a short timeout, ack all, return string payloads."
  ([cg] (poll-all cg {}))
  ([cg opts]
   (let [batch (k7/poll! cg (merge {:max-batch 256 :timeout-ms 20} opts))]
     (doseq [^Msg m batch] (k7/ack! cg (.offset m)))
     (msgs->strings batch))))

(defn wait-committed
  "Spin until committed-pos >= target or timeout."
  [^Segment seg target]
  (let [deadline (+ (System/nanoTime) (long 1e8))] ; 100ms
    (loop []
      (when (and (< (.get ^java.util.concurrent.atomic.AtomicLong (.committed-pos seg)) target)
                 (< (System/nanoTime) deadline))
        (Thread/sleep 1)
        (recur)))))

;;; ============================================================
;;; Basic enqueue / poll
;;; ============================================================

(deftest test-basic-enqueue-poll
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {}]
        (k7/enqueue! q (.getBytes "hello"))
        (k7/enqueue! q (.getBytes "world"))
        (is (= ["hello" "world"] (poll-all cg)))))))

(deftest test-empty-poll-returns-empty
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {}]
        (is (= [] (poll-all cg)))))))

(deftest test-offsets-are-monotonic
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {}]
        (let [o1 (k7/enqueue! q (.getBytes "a"))
              o2 (k7/enqueue! q (.getBytes "b"))
              o3 (k7/enqueue! q (.getBytes "c"))]
          (is (< o1 o2))
          (is (< o2 o3))
          (let [batch (k7/poll! cg {:max-batch 10 :timeout-ms 20})]
            (is (= [o1 o2 o3] (mapv #(.offset ^Msg %) batch)))
            (doseq [^Msg m batch] (k7/ack! cg (.offset m)))))))))

(deftest test-large-payload
  (let [dir (tmp-dir)
        data (byte-array 65536 (byte 0x42))]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {}]
        (k7/enqueue! q data)
        (let [[^Msg m] (k7/poll! cg {:max-batch 1 :timeout-ms 20})]
          (is (= 65536 (.remaining (.payload m))))
          (k7/ack! cg (.offset m)))))))

(deftest test-many-messages
  (let [dir (tmp-dir)
        n 1000]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {}]
        (dotimes [i n]
          (k7/enqueue! q (.getBytes (str i))))
        (let [batch (k7/poll! cg {:max-batch n :timeout-ms 50})]
          (is (= n (count batch)))
          (doseq [^Msg m batch] (k7/ack! cg (.offset m))))))))

;;; ============================================================
;;; Payload zero-copy
;;; ============================================================

(deftest test-payload-is-readonly-view
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {}]
        (k7/enqueue! q (.getBytes "readonly"))
        (let [[^Msg m] (k7/poll! cg {:max-batch 1 :timeout-ms 20})]
          (is (.isReadOnly (.payload m)))
          (k7/ack! cg (.offset m)))))))

;;; ============================================================
;;; Ack / nack
;;; ============================================================

(deftest test-ack-advances-cursor
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {:cursor-fsync-strategy :sync}]
        (k7/enqueue! q (.getBytes "x"))
        (let [[^Msg m] (k7/poll! cg {:max-batch 1 :timeout-ms 20})]
          (is (= 0 (.getLong ^ByteBuffer (.cursor-mmap ^ConsumerGroup cg) 0)))
          (k7/ack! cg (.offset m))
          (is (pos? (.getLong ^ByteBuffer (.cursor-mmap ^ConsumerGroup cg) 0))))))))

(deftest test-nack-redelivers
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {}]
        (k7/enqueue! q (.getBytes "retry-me"))
        (let [[^Msg m1] (k7/poll! cg {:max-batch 1 :timeout-ms 20})]
          (k7/nack! cg (.offset m1))
          (let [[^Msg m2] (k7/poll! cg {:max-batch 1 :timeout-ms 20})]
            (is (= (.offset m1) (.offset m2)))
            (is (= "retry-me" (String. (k7/payload->bytes (.payload m2)))))
            (k7/ack! cg (.offset m2))))))))

(deftest test-partial-ack-cursor
  ;; ack out-of-order: cursor should only advance to contiguous prefix
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {:cursor-fsync-strategy :sync}]
        (k7/enqueue! q (.getBytes "a"))
        (k7/enqueue! q (.getBytes "b"))
        (k7/enqueue! q (.getBytes "c"))
        (let [[m1 m2 m3] (k7/poll! cg {:max-batch 3 :timeout-ms 20})]
          ;; ack m2 first — cursor should stay at m1's offset (gap)
          (k7/ack! cg (.offset ^Msg m2))
          (let [cursor-after-m2 (.getLong ^ByteBuffer (.cursor-mmap ^ConsumerGroup cg) 0)]
            (is (= (.offset ^Msg m1) cursor-after-m2)))
          ;; ack m1 — cursor should advance past m2 too
          (k7/ack! cg (.offset ^Msg m1))
          (let [cursor-after-m1 (.getLong ^ByteBuffer (.cursor-mmap ^ConsumerGroup cg) 0)]
            (is (= (.offset ^Msg m3) cursor-after-m1)))
          (k7/ack! cg (.offset ^Msg m3)))))))

;;; ============================================================
;;; Seek
;;; ============================================================

(deftest test-seek-to-beginning
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {}]
        (k7/enqueue! q (.getBytes "a"))
        (k7/enqueue! q (.getBytes "b"))
        (is (= ["a" "b"] (poll-all cg)))
        (k7/seek! cg 0)
        (is (= ["a" "b"] (poll-all cg)))))))

(deftest test-seek-to-mid
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {}]
        (k7/enqueue! q (.getBytes "a"))
        (let [o2 (k7/enqueue! q (.getBytes "b"))]
          (k7/enqueue! q (.getBytes "c"))
          (poll-all cg)
          (k7/seek! cg o2)
          (is (= ["b" "c"] (poll-all cg))))))))

(deftest test-seek-persists-cursor
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (let [o1 (k7/enqueue! q (.getBytes "a"))
            o2 (k7/enqueue! q (.getBytes "b"))]
        (with-consumer [cg q "g" {:cursor-fsync-strategy :sync}]
          (poll-all cg)
          (k7/seek! cg o1))
        ;; reopen consumer — should resume from seek position
        (with-consumer [cg2 q "g" {}]
          (is (= ["a" "b"] (poll-all cg2))))))))

;;; ============================================================
;;; Crash recovery
;;; ============================================================

(deftest test-recover-after-close
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (k7/enqueue! q (.getBytes "persist-me")))
    ;; reopen
    (with-queue [q2 dir {:fsync-strategy :flush}]
      (with-consumer [cg q2 "g" {}]
        (is (= ["persist-me"] (poll-all cg)))))))

(deftest test-cursor-survives-restart
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (k7/enqueue! q (.getBytes "a"))
      (k7/enqueue! q (.getBytes "b"))
      (with-consumer [cg q "g" {:cursor-fsync-strategy :sync}]
        ;; consume and ack only first message
        (let [[^Msg m] (k7/poll! cg {:max-batch 1 :timeout-ms 20})]
          (k7/ack! cg (.offset m)))))
    ;; reopen — consumer should resume from after "a"
    (with-queue [q2 dir {:fsync-strategy :flush}]
      (with-consumer [cg2 q2 "g" {:cursor-fsync-strategy :sync}]
        (is (= ["b"] (poll-all cg2)))))))

;;; ============================================================
;;; Multiple consumer groups
;;; ============================================================

(deftest test-independent-consumer-groups
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg1 q "g1" {}]
        (with-consumer [cg2 q "g2" {}]
          (k7/enqueue! q (.getBytes "shared"))
          (is (= ["shared"] (poll-all cg1)))
          (is (= ["shared"] (poll-all cg2))))))))

;;; ============================================================
;;; fsync strategies
;;; ============================================================

(deftest test-fsync-strategies
  (doseq [strategy [:async :flush :sync]]
    (testing (str "strategy " strategy)
      (let [dir (tmp-dir)]
        (with-queue [q dir {:fsync-strategy strategy}]
          (with-consumer [cg q "g" {:cursor-fsync-strategy strategy}]
            (k7/enqueue! q (.getBytes "x"))
            (when (= strategy :async)
              (wait-committed (k7/current-segment q) 1))
            (is (= ["x"] (poll-all cg)))))))))

;;; ============================================================
;;; Adaptive batching
;;; ============================================================

(deftest test-batch-respects-max
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {}]
        (dotimes [_ 20]
          (k7/enqueue! q (.getBytes "x")))
        (let [batch (k7/poll! cg {:max-batch 5 :timeout-ms 20})]
          (is (= 5 (count batch)))
          (doseq [^Msg m batch] (k7/ack! cg (.offset m))))))))

(deftest test-poll-returns-partial-on-timeout
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :flush}]
      (with-consumer [cg q "g" {}]
        (k7/enqueue! q (.getBytes "only-one"))
        (let [batch (k7/poll! cg {:max-batch 100 :timeout-ms 20})]
          (is (= 1 (count batch)))
          (doseq [^Msg m batch] (k7/ack! cg (.offset m))))))))

;;; ============================================================
;;; Frame integrity
;;; ============================================================

(deftest test-crc-validation
  ;; Write a message, corrupt a byte in the payload, verify it doesn't read
  (let [dir (tmp-dir)]
    (with-queue [q dir {:fsync-strategy :sync}]
      (k7/enqueue! q (.getBytes "corrupt-me"))
      (let [seg ^Segment (k7/current-segment q)
            mmap ^java.nio.MappedByteBuffer (.mmap seg)
            ;; payload starts at offset 10 (header size)
            payload-pos (+ 0 k7/frame-header-size)]
        ;; flip a bit in the payload
        (.put mmap (int payload-pos)
              (unchecked-byte (bit-xor (.get mmap (int payload-pos)) 0xFF))))
      (with-consumer [cg q "g" {}]
        ;; corrupted frame should not be delivered
        (is (= [] (k7/poll! cg {:max-batch 10 :timeout-ms 20})))))))
