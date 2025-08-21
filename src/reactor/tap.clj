(ns reactor.tap
  "Unified tap system for debugging across client and server"
  (:require [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [clojure.tools.logging :as log])
  (:import [java.time Instant]
           [java.util UUID]))

(defn tap>
  "Send a value to the tap system. 
   Usage: 
     (tap> value)
     (tap> value \"caller-name\")
   
   Values are stored in XTDB in the reactor_taps table."
  ([value]
   (tap> value nil))
  ([value caller]
   (try
     (let [node @session/default-node
           tap-id (str "tap-" (UUID/randomUUID))
           timestamp (Instant/now)
           edn-value (pr-str value)]
       ;; Store in XTDB
       (when node
         (xts/execute-sql node
           "INSERT INTO reactor_taps (_id, value_edn, caller, platform, created_at, session_id, value_type)
            VALUES (?, ?, ?, 'CLJ', ?, ?, ?)"
           tap-id
           edn-value
           (or caller "anonymous")
           timestamp
           "system"
           (cond
             (map? value) "map"
             (vector? value) "vector"
             (set? value) "set"
             (list? value) "list"
             (string? value) "string"
             (number? value) "number"
             (boolean? value) "boolean"
             (nil? value) "nil"
             (keyword? value) "keyword"
             :else "other")))
       
       ;; Log for debugging
       (log/debug "TAP>" caller "-" value)
       
       ;; Return the value (for threading)
       value)
     (catch Exception e
       (log/error e "Failed to tap value")
       value))))