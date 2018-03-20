(ns our-service.fake-producer
  (:require
    [franzy.serialization.serializers :as serializers]
    [franzy.clients.producer.client :as client]
    [franzy.clients.producer.protocols :as producer]
    [compojure.core :refer [routes ANY GET POST]]
    [clojure.tools.logging :as log])
  (:use ring.middleware.params))

(defn for-ever
  [thunk]
  (loop []
    (if-let [result (try
                      [(thunk)]
                      (catch Exception e
                        (println e)
                        (Thread/sleep 100)))]
      (result 0)
      (recur))))

(def kafka-client (delay
                    (client/make-producer {:bootstrap.servers "kafka1:9092"
                                           :acks              "all"
                                           :retries           1
                                           :client.id         "example-producer"}
                                          (serializers/string-serializer)
                                          (serializers/edn-serializer))))

(defn produce-edn [m]
  (let [value (assoc m :ts (System/currentTimeMillis))]
    (log/info "Producing" value)
    (for-ever
      #(producer/send-async! @kafka-client value))))

(defn send-command [command-key command]
  (produce-edn {:topic "run-command-topic"
                :key   command-key
                :value command}))

(defn api []
  (routes
    (POST "/run-command" [command-key command]
      (send-command command-key {:command command})
      {:status 200
       :body   (pr-str "done!")})))

(comment

  (send-command "client1" "Dan")
  (send-command "client12345222" "Lebrero")
  (dotimes [i 100]
    (send-command "client222222" (str "val" (+ 100 i))))
  )
