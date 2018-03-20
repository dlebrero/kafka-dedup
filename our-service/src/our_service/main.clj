(ns our-service.main
  (:require
    [clojure.tools.nrepl.server :as nrepl]
    [our-service.util :as k]
    [our-service.run-command :as kafka-streams]
    [our-service.fake-producer :as fake-producer]
    [ring.adapter.jetty :as jetty]
    [clojure.tools.logging :as log])
  (:use ring.middleware.params)
  (:gen-class))

(defonce state (atom {}))

(defn -main [& args]
  (nrepl/start-server :port 3002 :bind "0.0.0.0")
  (log/info "Waiting for kafka to be ready")
  (k/wait-for-kafka "kafka1" 9092)
  (log/info "Waiting for topics to be created")
  (k/wait-for-topic "run-command-topic")
  (Thread/sleep 5000)
  (log/info "Starting Kafka Streams")
  (let [kstream (kafka-streams/start-kafka-streams)
        web (fake-producer/api)]
    (reset! state {:kstream   kstream
                   :web       web
                   :jetty     (jetty/run-jetty
                                (wrap-params web)
                                {:port  80
                                 :join? false})})))

(comment

  (.close (:kstream @state))

  (do
    (.close (first x))
    (def x (kafka-streams/start-kafka-streams)))

  (def loggger (k/log-all-message))
  (.close loggger)
  )