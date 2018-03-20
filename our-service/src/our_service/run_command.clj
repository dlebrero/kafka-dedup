(ns our-service.run-command
  (:require
    [franzy.serialization.deserializers :as deserializers]
    [franzy.serialization.serializers :as serializers])
  (:gen-class)
  (:import (java.util Properties)
           (org.apache.kafka.streams StreamsConfig KafkaStreams)
           (org.apache.kafka.common.serialization Serde Serdes Serializer)
           (org.apache.kafka.clients.consumer ConsumerConfig)
           (org.apache.kafka.streams.kstream KStreamBuilder)
           (org.apache.kafka.streams.state KeyValueStore Stores)
           (org.apache.kafka.streams.processor ProcessorSupplier Processor)))

;;;
;;; Serialization stuff
;;;

(deftype NotSerializeNil [edn-serializer]
  Serializer
  (configure [_ configs isKey] (.configure edn-serializer configs isKey))
  (serialize [_ topic data]
    (when data (.serialize edn-serializer topic data)))
  (close [_] (.close edn-serializer)))

;; Can be global as they are thread-safe
(def serializer (NotSerializeNil. (serializers/edn-serializer)))
(def deserializer (deserializers/edn-deserializer))

(deftype EdnSerde []
  Serde
  (configure [this map b])
  (close [this])
  (serializer [this]
    serializer)
  (deserializer [this]
    deserializer))

;;;
;;; Application
;;;

(defn kafka-config []
  (doto
    (Properties.)
    (.put StreamsConfig/APPLICATION_ID_CONFIG "run-command-consumer")
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka1:9092")
    (.put StreamsConfig/ZOOKEEPER_CONNECT_CONFIG "zoo1:2181")
    ;(.put StreamsConfig/CACHE_MAX_BYTES_BUFFERING_CONFIG 0)
    (.put StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 1000)
    (.put StreamsConfig/KEY_SERDE_CLASS_CONFIG (class (Serdes/String)))
    (.put StreamsConfig/VALUE_SERDE_CLASS_CONFIG EdnSerde)
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")))

(defn processor [f store-name]
  (reify ProcessorSupplier
    (get [_]
      (let [ctx (volatile! nil)
            state (volatile! nil)]
        (reify Processor
          (init [_ context]
            (vreset! ctx context)
            (vreset! state (.getStateStore context store-name)))
          (process [_ k v]
            (f @state @ctx k v))
          (punctuate [_ timestamp])
          (close [_]))))))

(defn run-first-command [^KeyValueStore store ctx k v]
  (if (.putIfAbsent store k true)
    (println "Ignoring command" v "for key" k)
    (println "Running command" v "for key" k)))

(defn create-kafka-stream-topology []
  (let [^KStreamBuilder builder (KStreamBuilder.)
        store (-> (Stores/create "run-command-state")
                  .withStringKeys
                  (.withValues (EdnSerde.))
                  .persistent
                  .enableCaching
                  .build)
        builder (-> builder
                    (.addSource "run-command-topic" (into-array ["run-command-topic"]))
                    (.addProcessor "run-command-processor"
                                   (processor run-first-command "run-command-state")
                                   (into-array ["run-command-topic"]))
                    (.addStateStore store (into-array ["run-command-processor"])))]
    builder))

(defn start-kafka-streams []
  (let [builder (create-kafka-stream-topology)
        kafka-streams (KafkaStreams. builder (kafka-config))]
    (.start kafka-streams)
    kafka-streams))