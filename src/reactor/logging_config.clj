(ns reactor.logging-config
  "Configuration to control logging verbosity"
  (:require [reactor.log :as log]))

;; Log level configuration
;; Options: :trace :debug :info :warn :error :fatal
(def log-level :warn)  ; Changed from :info to :warn to reduce logging

;; High-frequency logging controls
(def ^:dynamic *enable-kafka-debug* false)
(def ^:dynamic *enable-cache-debug* false)
(def ^:dynamic *enable-cascade-debug* false)
(def ^:dynamic *enable-subscription-debug* false)
(def ^:dynamic *enable-sql-debug* false)

(defn set-log-level!
  "Set the global log level"
  [level]
  (alter-var-root #'log/*log-level* (constantly level)))

(defn init-logging!
  "Initialize logging configuration"
  []
  (set-log-level! log-level)
  (println "Logging initialized with level:" log-level))