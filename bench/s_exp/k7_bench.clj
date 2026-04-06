(ns s-exp.k7-bench
  "Benchmark suite for k7 — run with: clojure -M:bench
   Uses criterium for statistically rigorous measurement.

   Benchmarks:
     1. enqueue! throughput — raw write rate (bytes/s, msgs/s) per fsync strategy
     2. poll! throughput    — read + ack rate per batch size
     3. round-trip latency  — single enqueue→poll→ack latency
     4. large payload       — enqueue/poll with 64KB payloads
     5. multi-consumer      — 4 independent consumer groups reading the same queue
     6. segment roll        — cost of rolling to a new segment mid-benchmark"
  (:require [criterium.core :as crit]
            [s-exp.k7 :as k7])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (s_exp.k7 ConsumerGroup Msg)))

(defmacro with-tmp-queue [[q-sym opts] & body]
  `(let [dir# (str (Files/createTempDirectory "k7-bench-" (make-array FileAttribute 0)))
         ~q-sym (k7/queue dir# ~opts)]
     (try ~@body
          (finally (k7/close-queue! ~q-sym)))))

(defmacro with-tmp-queue+cg [[q-sym cg-sym q-opts cg-opts] & body]
  `(let [dir# (str (Files/createTempDirectory "k7-bench-" (make-array FileAttribute 0)))
         ~q-sym (k7/queue dir# ~q-opts)
         ~cg-sym (k7/consumer-group ~q-sym "bench" ~cg-opts)]
     (try ~@body
          (finally
            (k7/close-consumer-group! ~cg-sym)
            (k7/close-queue! ~q-sym)))))

;;; ============================================================
;;; Helpers
;;; ============================================================

(defn- drain!
  "Poll and ack all available messages, return count."
  ^long [^ConsumerGroup cg max-batch]
  (let [batch (k7/poll! cg {:max-batch max-batch :timeout-ms 50 :park-ns 0})]
    (doseq [^Msg m batch] (k7/ack! cg m))
    (count batch)))

(defn- drain-all!
  "Poll and ack until n messages consumed."
  [^ConsumerGroup cg n]
  (let [consumed (volatile! 0)]
    (while (< @consumed n)
      (let [batch (k7/poll! cg {:max-batch 1024 :timeout-ms 0 :park-ns 0})]
        (when (seq batch)
          (doseq [^Msg m batch] (k7/ack! cg m))
          (vswap! consumed + (count batch)))))))

(defn- enqueue-n! [q ^bytes payload n]
  (dotimes [_ n]
    (k7/enqueue! q payload)))

(defn- bench-title [title]
  (println)
  (println (str "╔══ " title))
  (println))

;;; ============================================================
;;; 1. enqueue! throughput by fsync strategy
;;; ============================================================

(defn bench-enqueue-throughput []
  (let [payload (.getBytes "hello-world-benchmark-payload-32b!")
        n 10000]
    ;; Skip :sync — it's disk-bound and takes minutes
    (doseq [strategy [:flush :async]]
      (bench-title (str "enqueue! throughput  strategy=" (name strategy)
                        "  payload=32b  n=" n))
      (with-tmp-queue [q {:fsync-strategy strategy}]
        (crit/quick-bench
         (enqueue-n! q payload n)
         :os :runtime)))))

;;; ============================================================
;;; 2. poll! throughput by batch size
;;; ============================================================

(defn bench-poll-throughput []
  ;; Pre-fill queue once; each iteration seeks back to 0 so we measure
  ;; pure poll+ack with no enqueue cost (matches README methodology).
  (let [payload (.getBytes "bench-msg")
        n 5000]
    (doseq [batch-size [1 16 64 256 1024]]
      (bench-title (str "poll! throughput  batch-size=" batch-size "  n=" n))
      (with-tmp-queue+cg [q cg {:fsync-strategy :flush} {:cursor-fsync-strategy :async}]
        (enqueue-n! q payload n)
        (crit/quick-bench
         (do (k7/seek! cg 0)
             (drain-all! cg n))
         :os :runtime)))))

;;; ============================================================
;;; 3. Round-trip latency (single enqueue → poll → ack)
;;; ============================================================

(defn bench-round-trip-latency []
  (let [payload (.getBytes "rtt")]
    (doseq [strategy [:flush :sync]]
      (bench-title (str "round-trip latency  strategy=" (name strategy)))
      (with-tmp-queue+cg [q cg {:fsync-strategy strategy} {:cursor-fsync-strategy strategy}]
        (crit/quick-bench
         (do (k7/enqueue! q payload)
             (let [[^Msg m] (k7/poll! cg {:max-batch 1 :timeout-ms 5 :park-ns 0})]
               (when m (k7/ack! cg m))))
         :os :runtime)))))

;;; ============================================================
;;; 4. Large payload (64KB)
;;; ============================================================

(defn bench-large-payload []
  (let [payload (byte-array 65536 (byte 0x42))
        n 100]
    (bench-title (str "large payload (64KB)  n=" n))
    (with-tmp-queue+cg [q cg {:fsync-strategy :flush} {:cursor-fsync-strategy :async}]
      (crit/quick-bench
       (do (enqueue-n! q payload n)
           (drain-all! cg n))
       :os :runtime))))

;;; ============================================================
;;; 5. Multi-consumer groups (4 independent readers)
;;; ============================================================

(defn bench-multi-consumer []
  (let [payload (.getBytes "shared")
        n 5000
        ngroups 4]
    (bench-title (str "multi-consumer  groups=" ngroups "  n=" n))
    (let [dir (str (Files/createTempDirectory "k7-bench-mc-" (make-array FileAttribute 0)))
          q   (k7/queue dir {:fsync-strategy :flush})
          cgs (mapv #(k7/consumer-group q (str "g" %) {:cursor-fsync-strategy :async}) (range ngroups))]
      (try
        (crit/quick-bench
         (do (enqueue-n! q payload n)
             (doseq [^ConsumerGroup cg cgs]
               (drain-all! cg n)))
         :os :runtime)
        (finally
          (doseq [cg cgs] (k7/close-consumer-group! cg))
          (k7/close-queue! q))))))

;;; ============================================================
;;; 6. Segment roll cost
;;; ============================================================

(defn bench-segment-roll []
  ;; Use a small segment so we can fill it quickly each trial.
  (let [payload-size  1024
        payload       (byte-array payload-size (byte 0x42))
        ;; Small segment: ~200 messages per segment so fill is fast
        segment-size  (* 256 1024)
        trials        200
        times         (long-array trials)]
    (bench-title "segment roll  — normal enqueue vs roll-triggering enqueue")
    (println (format "  payload=%dB  segment-size=%dKB" payload-size (quot segment-size 1024)))
    (println "\n  [baseline] normal enqueue (no roll):")
    (with-tmp-queue [q {:fsync-strategy :flush :segment-size (* 256 1024 1024)}]
      (crit/quick-bench (k7/enqueue! q payload) :os :runtime))
    (println (format "\n  [roll] single segment-roll timing (manual, %d trials):" trials))
    (let [frame-size   (s-exp.k7/aligned-frame-size payload-size)
          msgs-per-seg (quot segment-size frame-size)]
      (dotimes [i trials]
        (let [dir (str (Files/createTempDirectory "k7-bench-roll-" (make-array FileAttribute 0)))
              q   (k7/queue dir {:fsync-strategy :flush :segment-size segment-size})]
          (try
            (enqueue-n! q payload (dec msgs-per-seg))
            (let [t0 (System/nanoTime)]
              (k7/enqueue! q payload)
              (aset times i (- (System/nanoTime) t0)))
            (finally (k7/close-queue! q)))))
      (let [sorted (doto (long-array times) java.util.Arrays/sort)
            mean   (/ (double (reduce + (seq times))) trials)
            p50    (aget sorted (int (* 0.50 trials)))
            p99    (aget sorted (int (* 0.99 trials)))]
        (println (format "  mean=%.1fµs  p50=%.1fµs  p99=%.1fµs"
                         (/ mean 1000.0) (/ p50 1000.0) (/ p99 1000.0)))))))

;;; ============================================================
;;; 7. Sustained throughput — enqueue + concurrent poll
;;;    (simulates real producer/consumer overlap)
;;; ============================================================

(defn bench-sustained-throughput []
  (let [payload (.getBytes "sustained-bench-payload")
        n 100000]
    (bench-title (str "sustained throughput  n=" n "  strategy=:flush"))
    (with-tmp-queue+cg [q cg {:fsync-strategy :flush} {:cursor-fsync-strategy :flush}]
      (let [start (System/nanoTime)]
        (enqueue-n! q payload n)
        (drain-all! cg n)
        (let [elapsed-s (/ (- (System/nanoTime) start) 1e9)]
          (println (format "  msgs: %d  elapsed: %.3fs  throughput: %.0f msg/s  %.1f MB/s"
                           n elapsed-s
                           (/ n elapsed-s)
                           (/ (* n (alength payload)) elapsed-s 1e6))))))))

;;; ============================================================
;;; Entry point
;;; ============================================================

(def ^:private all-benches
  [["enqueue! throughput by fsync strategy" bench-enqueue-throughput]
   ["poll! throughput by batch size"        bench-poll-throughput]
   ["round-trip latency"                    bench-round-trip-latency]
   ["large payload (64KB)"                  bench-large-payload]
   ["multi-consumer groups"                 bench-multi-consumer]
   ["segment roll cost"                     bench-segment-roll]
   ["sustained throughput"                  bench-sustained-throughput]])

(defn -main [& args]
  (let [selected (set args)
        benches  (if (seq selected)
                   (filter (fn [[name _]] (selected name)) all-benches)
                   all-benches)]
    (println "╔══════════════════════════════════════════════╗")
    (println "║           k7 benchmark suite                 ║")
    (println "╚══════════════════════════════════════════════╝")
    (println (str "JVM: " (System/getProperty "java.vm.name")
                  " " (System/getProperty "java.version")))
    (println (str "OS:  " (System/getProperty "os.name")
                  " " (System/getProperty "os.arch")))
    (println)
    (doseq [[_name f] benches]
      (f))
    (println)
    (println "Done.")))
