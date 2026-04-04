(ns s-exp.k7.async
  "core.async sugar for k7 queues"
  (:require [clojure.core.async :as a]
            [s-exp.k7 :as k7]))

;;; ============================================================
;;; Producer
;;; ============================================================

(defn sink!
  "Block the calling thread, taking byte arrays from ch and calling
   enqueue! on q until ch is closed.

   enqueue! is not thread-safe, so only one sink! may run against a
   given queue at a time. For concurrent production, use multiple
   upstream producers writing into ch — channel delivery to sink! is
   serialized automatically. You can wrap sink! in a/thread or future to run
   in the background, as long as there is only one."
  [q ch]
  (loop []
    (when-let [data (a/<!! ch)]
      (k7/enqueue! q data)
      (recur))))

(defn producer-chan
  "Start a dedicated writer thread that drains ch and calls enqueue! on q.

   Returns ch (default: (a/chan 256)), which callers put byte arrays onto.
   Close ch to stop the writer thread.

   Multiple producer-chans on the same queue are safe — each uses locking
   to serialize enqueue! calls. For single-producer use, prefer sink! which
   avoids locking entirely.

   Options:
     :ch — supply your own channel; (a/chan 256) used if not provided"
  [q & {:keys [ch]}]
  (let [ch (or ch (a/chan 256))]
    (a/thread
      (loop []
        (when-let [data (a/<!! ch)]
          (locking q (k7/enqueue! q data))
          (recur))))
    ch))

;;; ============================================================
;;; Consumer
;;; ============================================================

(defn consumer-group-chan
  "Open a ConsumerGroup on queue q for group-id and start a dedicated
   reader thread that polls and delivers Msg values to a channel.

   The ConsumerGroup is owned entirely by the reader thread —
   poll! and ack! are never called from any other thread.
   Messages are auto-acked immediately after being placed on the
   output channel (at-most-once delivery).

   Stop the consumer by putting any value onto :stop-ch (a promise-chan),
   or by closing :ch. The reader exits after the current batch; the
   ConsumerGroup is closed before the thread terminates.

   Returns a map:
     :ch      — channel of Msg values
     :stop-ch — promise-chan; put any value to stop the consumer

   Options:
     :ch          — supply your own channel; (a/chan 256) used if not provided
     :poll-opts   — map passed to k7/poll! (default: {:max-batch 64
                                                       :timeout-ms 5})
     :cg-opts     — map passed to k7/open-consumer-group (default: {})"
  [q group-id & {:keys [ch poll-opts cg-opts]}]
  (let [ch (or ch (a/chan 256))
        stop-ch (a/promise-chan)
        poll-opts (or poll-opts {:max-batch 64 :timeout-ms 5})
        cg-opts (or cg-opts {})]
    (a/thread
      (with-open [cg (k7/open-consumer-group q group-id cg-opts)]
        (loop []
          (let [batch (k7/poll! cg poll-opts)
                closed? (reduce (fn [_ msg]
                                  (let [[v c] (a/alts!! [[ch msg] stop-ch])]
                                    (if (and (= c ch) v)
                                      (k7/ack! cg msg)
                                      (reduced true))))
                                nil
                                batch)]
            (when-not closed?
              (let [[_ c] (a/alts!! [stop-ch] :default ::open)]
                (when (= c :default)
                  (recur))))))))
    {:ch ch
     :stop-ch stop-ch}))

(comment
  (def q (k7/open-queue "/tmp/k7-async-test" {:fsync-strategy :flush}))

  ;; producer — run on a dedicated thread, feed from any thread
  (def producer-ch (a/chan 1024))
  (a/thread (k7a/pipe! q producer-ch))
  (a/put! producer-ch (.getBytes "hello"))
  (a/put! producer-ch (.getBytes "world"))
  (a/close! producer-ch)

  ;; consumer
  (def c (consumer-group-chan q "async-test" :poll-opts {:max-batch 32 :timeout-ms 10}))
  (a/<!! (:ch c)) ; => Msg
  ;; stop
  (a/put! (:stop-ch c) true)

  (k7/close-queue! q))
