(ns our-service.util
  (:require [clojure.tools.logging :as log]
            [our-service.run-command :as run-command])
  (:import (org.apache.kafka.streams.kstream KStream KStreamBuilder)
           (java.net Socket)
           (kafka.admin AdminUtils)
           (kafka.utils ZkUtils)
           (org.I0Itec.zkclient ZkClient)
           (org.apache.kafka.streams KafkaStreams)
           (org.apache.kafka.streams.processor ProcessorContext Processor ProcessorSupplier)))

(defn for-ever
  [msg thunk]
  (loop []
    (if-let [result (try
                      [(thunk)]
                      (catch Exception e
                        (log/info msg)
                        (log/debug e msg)
                        (Thread/sleep 3000)))]
      (result 0)
      (recur))))

(defn wait-for-kafka [host port]
  (for-ever "waiting for kafka"
    #(with-open [_ (Socket. host (int port))]
       true)))

(defn wait-for-topic [topic]
  (for-ever "waiting for topics"
    #(let [zk (ZkUtils/createZkClientAndConnection "zoo1:2181" 10000 10000)]
       (with-open [^ZkClient zk-client (._1 zk)]
         (when-not (AdminUtils/topicExists (ZkUtils. zk-client (._2 zk) false) topic)
           (log/info "Topic" topic "not created yet")
           (throw (RuntimeException.)))))))

(defn log-all-message []
  (let [^KStreamBuilder builder (KStreamBuilder.)
        raw-data-stream ^KStream (.stream builder #".*")
        _ (.process raw-data-stream (reify ProcessorSupplier
                                      (get [_]
                                        (let [ctx (volatile! nil)]
                                          (reify Processor
                                            (init [_ context]
                                              (vreset! ctx context))
                                            (process [_ k v]
                                              (let [^ProcessorContext ctx1  @ctx]
                                                (println "MSG -" (.topic ctx1) ":" k "," v)))
                                            (punctuate [_ timestamp])
                                            (close [_])))))
                    (into-array String []))
        streams (KafkaStreams. builder (run-command/kafka-config))]
    (.start streams)
    streams))