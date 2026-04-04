(ns com.s-exp.k7-async-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is]]
            [s-exp.k7 :as k7]
            [s-exp.k7.async :as k7a])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

;;; ============================================================
;;; Helpers
;;; ============================================================

(defn tmp-dir []
  (str (Files/createTempDirectory "k7-async-test-" (make-array FileAttribute 0))))

(defmacro with-queue [[q-sym dir] & body]
  `(let [~q-sym (k7/open-queue ~dir {:fsync-strategy :flush})]
     (try ~@body
          (finally (k7/close-queue! ~q-sym)))))

(defn stop! [c]
  (a/put! (:stop-ch c) true))

;;; ============================================================
;;; sink! tests
;;; ============================================================

(deftest test-sink-delivers-messages
  (let [dir (tmp-dir)]
    (with-queue [q dir]
      (let [ch (a/chan 10)]
        (a/put! ch (.getBytes "hello"))
        (a/put! ch (.getBytes "world"))
        (a/close! ch)
        (a/<!! (a/thread (k7a/sink! q ch)))
        (let [cg (k7/open-consumer-group q "g")]
          (try
            (let [batch (k7/poll! cg {:max-batch 10 :timeout-ms 100})]
              (is (= 2 (count batch)))
              (is (= ["hello" "world"]
                     (mapv #(String. (k7/payload->bytes (k7/msg-payload %))) batch))))
            (finally (k7/close-consumer-group! cg))))))))

(deftest test-sink-stops-on-close
  (let [dir (tmp-dir)]
    (with-queue [q dir]
      (let [ch (a/chan 100)]
        (dotimes [i 50]
          (a/put! ch (.getBytes (str i))))
        (a/close! ch)
        (a/<!! (a/thread (k7a/sink! q ch)))
        (let [cg (k7/open-consumer-group q "g")]
          (try
            (is (= 50 (count (k7/poll! cg {:max-batch 100 :timeout-ms 200}))))
            (finally (k7/close-consumer-group! cg))))))))

;;; ============================================================
;;; producer-chan tests
;;; ============================================================

(deftest test-producer-chan-delivers-messages
  (let [dir (tmp-dir)]
    (with-queue [q dir]
      (let [ch (k7a/producer-chan q)]
        (a/>!! ch (.getBytes "hello"))
        (a/>!! ch (.getBytes "world"))
        (a/close! ch)
        ;; wait for writer thread to finish
        (Thread/sleep 100)
        (let [cg (k7/open-consumer-group q "g")]
          (try
            (let [batch (k7/poll! cg {:max-batch 10 :timeout-ms 100})]
              (is (= 2 (count batch)))
              (is (= ["hello" "world"]
                     (mapv #(String. (k7/payload->bytes (k7/msg-payload %))) batch))))
            (finally (k7/close-consumer-group! cg))))))))

(deftest test-producer-chan-custom-channel
  (let [dir (tmp-dir)
        custom-ch (a/chan 16)]
    (with-queue [q dir]
      (let [ch (k7a/producer-chan q :ch custom-ch)]
        (is (identical? custom-ch ch))
        (a/>!! ch (.getBytes "x"))
        (a/close! ch)
        (Thread/sleep 100)
        (let [cg (k7/open-consumer-group q "g")]
          (try
            (is (= 1 (count (k7/poll! cg {:max-batch 5 :timeout-ms 100}))))
            (finally (k7/close-consumer-group! cg))))))))

(deftest test-producer-chan-many-messages
  (let [dir (tmp-dir)
        n 100]
    (with-queue [q dir]
      (let [ch (k7a/producer-chan q)]
        (dotimes [i n]
          (a/>!! ch (.getBytes (str i))))
        (a/close! ch)
        (Thread/sleep 100)
        (let [cg (k7/open-consumer-group q "g")]
          (try
            (is (= n (count (k7/poll! cg {:max-batch n :timeout-ms 200}))))
            (finally (k7/close-consumer-group! cg))))))))

;;; ============================================================
;;; consumer-group-chan tests
;;; ============================================================

(deftest test-consumer-group-chan-delivers-messages
  (let [dir (tmp-dir)]
    (with-queue [q dir]
      (k7/enqueue! q (.getBytes "a"))
      (k7/enqueue! q (.getBytes "b"))
      (k7/enqueue! q (.getBytes "c"))
      (let [c (k7a/consumer-group-chan q "g" :poll-opts {:max-batch 10 :timeout-ms 50})
            received (atom [])]
        (dotimes [_ 3]
          (when-let [msg (a/<!! (:ch c))]
            (swap! received conj (String. (k7/payload->bytes (k7/msg-payload msg))))))
        (stop! c)
        (is (= #{"a" "b" "c"} (set @received)))))))

(deftest test-consumer-group-chan-stop-via-stop-ch
  ;; putting on stop-ch must unblock promptly even when queue is empty
  (let [dir (tmp-dir)]
    (with-queue [q dir]
      (k7/enqueue! q (.getBytes "only"))
      (let [c (k7a/consumer-group-chan q "g" :poll-opts {:max-batch 5 :timeout-ms 50})]
        (a/<!! (:ch c))
        (let [start (System/currentTimeMillis)]
          (stop! c)
          (is (< (- (System/currentTimeMillis) start) 2000)))))))

(deftest test-consumer-group-chan-stop-via-ch-close
  ;; closing :ch also stops the consumer
  (let [dir (tmp-dir)]
    (with-queue [q dir]
      (k7/enqueue! q (.getBytes "only"))
      (let [c (k7a/consumer-group-chan q "g" :poll-opts {:max-batch 5 :timeout-ms 50})]
        (a/<!! (:ch c))
        (let [start (System/currentTimeMillis)]
          (a/close! (:ch c))
          (is (< (- (System/currentTimeMillis) start) 2000)))))))

(deftest test-consumer-group-chan-custom-channel
  (let [dir (tmp-dir)
        custom-ch (a/chan 16)]
    (with-queue [q dir]
      (k7/enqueue! q (.getBytes "x"))
      (let [c (k7a/consumer-group-chan q "g"
                                       :ch custom-ch
                                       :poll-opts {:max-batch 5 :timeout-ms 50})]
        (is (identical? custom-ch (:ch c)))
        (let [msg (a/<!! (:ch c))]
          (is (= "x" (String. (k7/payload->bytes (k7/msg-payload msg))))))
        (stop! c)))))

(deftest test-consumer-group-chan-many-messages
  (let [dir (tmp-dir)
        n 100]
    (with-queue [q dir]
      (dotimes [i n]
        (k7/enqueue! q (.getBytes (str i))))
      (let [c (k7a/consumer-group-chan q "g" :poll-opts {:max-batch 32 :timeout-ms 50})
            received (atom #{})]
        (dotimes [_ n]
          (when-let [msg (a/<!! (:ch c))]
            (swap! received conj (String. (k7/payload->bytes (k7/msg-payload msg))))))
        (stop! c)
        (is (= n (count @received)))
        (is (= (set (map str (range n))) @received))))))

(deftest test-consumer-group-chan-independent-groups
  ;; two groups on the same queue each see all messages
  (let [dir (tmp-dir)]
    (with-queue [q dir]
      (k7/enqueue! q (.getBytes "shared"))
      (let [c1 (k7a/consumer-group-chan q "g1" :poll-opts {:max-batch 5 :timeout-ms 100})
            c2 (k7a/consumer-group-chan q "g2" :poll-opts {:max-batch 5 :timeout-ms 100})]
        (let [m1 (a/<!! (:ch c1))
              m2 (a/<!! (:ch c2))]
          (is (= "shared" (String. (k7/payload->bytes (k7/msg-payload m1)))))
          (is (= "shared" (String. (k7/payload->bytes (k7/msg-payload m2))))))
        (stop! c1)
        (stop! c2)))))
