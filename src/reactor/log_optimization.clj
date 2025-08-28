(ns reactor.log-optimization
  "Macros for optimized logging to prevent string construction when not needed"
  (:require [reactor.log :as log]))

(defmacro when-debug-enabled
  "Only evaluate body when debug logging is enabled"
  [& body]
  `(when (<= (:debug log/log-levels) (get log/log-levels log/*log-level*))
     ~@body))

(defmacro debug
  "Optimized debug logging that doesn't evaluate arguments unless needed"
  [& args]
  `(when-debug-enabled
     (log/debug ~@args)))

(defmacro info
  "Optimized info logging"
  [& args]
  `(when (<= (:info log/log-levels) (get log/log-levels log/*log-level*))
     (log/info ~@args)))

(defmacro warn
  "Optimized warn logging"
  [& args]
  `(when (<= (:warn log/log-levels) (get log/log-levels log/*log-level*))
     (log/warn ~@args)))

(defmacro error
  "Optimized error logging"
  [& args]
  `(log/error ~@args))  ; Always log errors

;; Conditional logging for high-frequency operations
(def ^:dynamic *enable-kafka-debug* false)
(def ^:dynamic *enable-cache-debug* false)
(def ^:dynamic *enable-cascade-debug* false)

(defmacro kafka-debug
  "Debug logging for Kafka operations - only when enabled"
  [& args]
  `(when *enable-kafka-debug*
     (log/debug :kafka ~@args)))

(defmacro cache-debug
  "Debug logging for cache operations - only when enabled"
  [& args]
  `(when *enable-cache-debug*
     (log/debug :cache ~@args)))

(defmacro cascade-debug
  "Debug logging for cascade operations - only when enabled"
  [& args]
  `(when *enable-cascade-debug*
     (log/debug :cascade ~@args)))