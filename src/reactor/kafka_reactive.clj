(ns reactor.kafka-reactive
  "Kafka-based real-time reactivity for XTDB transaction logs.
   Monitors XTDB transactions and triggers re-execution of subscribed queries."
  (:require [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [taoensso.nippy :as nippy]
            [cheshire.core]
            [reactor.xtdb-store :as xts]
            [reactor.session_simple :as session]
            [org.httpkit.server :as http-server]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [clojure.core.async :as async :refer [go go-loop chan <! >! close! timeout]]))

;; ============================================================================
;; Configuration
;; ============================================================================

(defonce kafka-config 
  (atom {"bootstrap.servers" "localhost:9092"
         "group.id" "reactor-xtdb-watcher"
         "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
         "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
         "auto.offset.reset" "latest"
         "enable.auto.commit" "true"}))

(defonce xtdb-log-topic "xtdb-log")

;; ============================================================================
;; Query Subscription Management
;; ============================================================================

(defonce active-subscriptions 
  ;; Map of subscription-id -> {:query sql-string :params params :tables [table-names] 
  ;;                             :callback fn :session-id session-id}
  (atom {}))

(defonce table-to-subs
  ;; Map of table-name -> #{subscription-ids}
  (atom {}))

(defonce session-subscriptions
  ;; Map of session-id -> #{:keypath} for tracking which sessions have keypath subscriptions
  (atom {}))

(defn extract-tables-from-sql
  "Extract table names from SQL query string.
   Simple regex-based extraction - could be enhanced with proper SQL parsing."
  [sql]
  (let [;; Match FROM and JOIN clauses
        from-pattern #"(?i)FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)"
        join-pattern #"(?i)JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)"
        ;; Also check INSERT INTO, UPDATE, DELETE FROM
        insert-pattern #"(?i)INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)"
        update-pattern #"(?i)UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)"
        delete-pattern #"(?i)DELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)"
        
        extract-matches (fn [pattern text]
                         (map #(str/lower-case (second %)) (re-seq pattern text)))]
    (-> #{}
        (into (extract-matches from-pattern sql))
        (into (extract-matches join-pattern sql))
        (into (extract-matches insert-pattern sql))
        (into (extract-matches update-pattern sql))
        (into (extract-matches delete-pattern sql))
        vec)))

(defn register-query-subscription!
  "Register a SQL query subscription that will be re-executed on relevant changes."
  [sub-id sql params callback session-id]
  (let [tables (extract-tables-from-sql sql)
        sub-info {:query sql
                  :params params
                  :tables tables
                  :callback callback
                  :session-id session-id}]
    (swap! active-subscriptions assoc sub-id sub-info)
    ;; Update table index
    (doseq [table tables]
      (swap! table-to-subs update (str/lower-case table) (fnil conj #{}) sub-id))
    (log/info "Registered subscription" sub-id "for tables:" tables)
    sub-id))

(defn unregister-query-subscription!
  "Remove a query subscription."
  [sub-id]
  (when-let [sub-info (get @active-subscriptions sub-id)]
    (swap! active-subscriptions dissoc sub-id)
    ;; Clean up table index
    (doseq [table (:tables sub-info)]
      (swap! table-to-subs update table disj sub-id)
      ;; Remove empty sets
      (when (empty? (get @table-to-subs table))
        (swap! table-to-subs dissoc table)))
    (log/info "Unregistered query subscription" sub-id)))

(defn re-execute-subscription
  "Re-execute a subscription's query and invoke its callback with results."
  [sub-id]
  (log/info "[KAFKA-REACTIVE] Re-executing subscription" sub-id)
  (if-let [{:keys [query params callback session-id]} (get @active-subscriptions sub-id)]
    (try
      (log/info "[KAFKA-REACTIVE] Found subscription" sub-id "for session" session-id "SQL:" query)
      (if-let [node @session/default-node]
        (let [result (if params
                      (xts/execute-sql node query params)
                      (xts/execute-sql node query))]
          (log/info "[KAFKA-REACTIVE] Query executed for" sub-id "got" (count (:results result [])) "results")
          (callback {:subscription-id sub-id
                    :session-id session-id
                    :query query
                    :result result}))
        (log/warn "[KAFKA-REACTIVE] No XTDB node available for re-execution"))
      (catch Exception e
        (log/error e "[KAFKA-REACTIVE] Error re-executing subscription" sub-id)))
    (log/warn "[KAFKA-REACTIVE] Subscription" sub-id "not found in active subscriptions")))

(defn find-affected-subscriptions
  "Find all subscriptions affected by changes to the given tables."
  [tables]
  (reduce (fn [subs table]
           (into subs (get @table-to-subs table #{})))
         #{}
         tables))

;; ============================================================================
;; Transaction Processing
;; ============================================================================

(defn extract-tables-from-tx-ops
  "Extract affected table names from XTDB transaction operations."
  [tx-ops]
  (reduce (fn [tables op]
           (cond
             ;; SQL-based operations in XTDB 2.0
             (:table op) (conj tables (:table op))
             ;; Handle INSERT INTO pattern
             (and (:type op) (= :insert-into (:type op)))
             (conj tables (:table op))
             ;; Handle UPDATE pattern  
             (and (:type op) (= :update (:type op)))
             (conj tables (:table op))
             ;; Handle DELETE pattern
             (and (:type op) (= :delete (:type op)))
             (conj tables (:table op))
             ;; Fallback: try to extract from SQL string if present
             (:sql op) (into tables (extract-tables-from-sql (:sql op)))
             :else tables))
         #{}
         tx-ops))

(defn process-transaction
  "Process a transaction from the Kafka log and trigger affected subscriptions."
  [tx-data]
  (try
    (let [tx-ops (:tx-ops tx-data [])
          affected-tables (extract-tables-from-tx-ops tx-ops)
          affected-subs (find-affected-subscriptions affected-tables)]
      
      (when (seq affected-subs)
        (log/info "Transaction affected tables:" affected-tables 
                 "triggering" (count affected-subs) "subscriptions")
        
        ;; Re-execute affected subscriptions
        (doseq [sub-id affected-subs]
          (re-execute-subscription sub-id))))
    
    (catch Exception e
      (log/error e "Error processing transaction"))))

;; ============================================================================
;; Kafka Consumer
;; ============================================================================

(defonce consumer (atom nil))
(defonce consumer-thread (atom nil))
(defonce running? (atom false))

(declare trigger-keypath-updates!)

(defn start-consumer!
  "Start the Kafka consumer to monitor XTDB transaction logs."
  []
  (when @running?
    (log/warn "Consumer already running")
    (throw (ex-info "Consumer already running" {})))
  
  (reset! running? true)
  (let [consumer-instance (jc/consumer @kafka-config)]
    (reset! consumer consumer-instance)
    
    ;; Subscribe to XTDB log topic
    (jc/subscribe consumer-instance [{:topic-name xtdb-log-topic}])
    (log/info "Subscribed to Kafka topic:" xtdb-log-topic)
    
    ;; Start consumer thread
    (reset! consumer-thread
           (future
             (log/info "Starting Kafka consumer thread")
             (while @running?
               (try
                 (let [records (jc/poll consumer-instance 100)]
                   (doseq [record records]
                     (try
                       ;; Minimal processing - just check if it's a mutation and extract table names
                       (let [tx-value (:value record)
                             tx-key (:key record)]
                         (when (and tx-value (> (count tx-value) 100)) ;; Skip tiny messages
                           ;; Convert just enough to check for table names (first 5KB should be enough)
                           (let [sample-size (min 5000 (count tx-value))
                                 raw-sample (String. (byte-array (take sample-size tx-value)) "ISO-8859-1")
                                 
                                 ;; Quick check if this is a mutation (not SELECT-only)
                                 has-mutation? (or (re-find #"(?i)(INSERT\s+INTO|UPDATE\s+|DELETE\s+FROM)" raw-sample)
                                                 (re-find #"(?i)INSERT\s+.*RECORDS" raw-sample)
                                                 (re-find #"(?i)VALUES\s*\(" raw-sample))
                                 is-select? (and (re-find #"(?i)SELECT\s+" raw-sample)
                                               (not has-mutation?))
                                   
                                   ;; Only look for mutation operations (INSERT, UPDATE, DELETE) in the sample
                                   insert-tables (if-not is-select?
                                                  (map #(str/lower-case (second %)) 
                                                       (re-seq #"(?i)INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)" raw-sample))
                                                  [])
                                   update-tables (if-not is-select?
                                                  (map #(str/lower-case (second %)) 
                                                       (re-seq #"(?i)UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)" raw-sample))
                                                  [])
                                   delete-tables (if-not is-select?
                                                  (map #(str/lower-case (second %)) 
                                                       (re-seq #"(?i)DELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)" raw-sample))
                                                  [])
                                   ;; Don't extract FROM tables as those could be from SELECT queries
                                   ;; Only combine mutation tables
                                   all-tables (set (concat insert-tables update-tables delete-tables))
                                   ;; Filter out common SQL keywords and session tables
                                   filtered-tables (-> all-tables
                                                      (disj "from" "into" "where" "select" "insert" "update" "delete" 
                                                            "values" "set" "and" "or" "null" "table" "column")
                                                      ;; EXCLUDE SESSION TABLES - they're just UI state
                                                      (disj "rabbit_sessions" "todo_sessions" "sessions")
                                                      ;; Remove any table ending with _sessions
                                                      (->> (remove #(str/ends-with? % "_sessions")))
                                                      set)]
                               
                               ;; Log if mutation detected (without the actual SQL)
                               (when has-mutation?
                                 (log/debug "Mutation pattern detected in Kafka message"))
                               
                               (when is-select?
                                 ;; Don't log SELECT skips - too verbose
                                 nil)
                               
                               (when (seq filtered-tables)
                                 (log/info "Tables affected by mutation:" filtered-tables)
                                 ;; Only log subscription details if there are active subscriptions
                                 (when (pos? (count @active-subscriptions))
                                   (log/debug "Active subscriptions:" (keys @active-subscriptions))
                                   (log/debug "Table-to-subs mapping:" @table-to-subs))
                                 ;; Handle SQL subscriptions
                                 (let [affected-subs (find-affected-subscriptions filtered-tables)]
                                   (when (seq affected-subs)
                                     (log/info "Triggering" (count affected-subs) "subscriptions for tables:" filtered-tables)
                                     (doseq [sub-id affected-subs]
                                       (re-execute-subscription sub-id))))
                                 
                                 ;; Handle keypath subscriptions for todo_sessions
                                 (doseq [table filtered-tables]
                                   (trigger-keypath-updates! table)))))) ;; Close all the let blocks and when
                       (catch Exception e
                         (log/error e "Error processing Kafka record")))))
                  (catch Exception e
                    (log/error e "Error polling Kafka")))
               (Thread/sleep 100))))
             (log/info "Kafka consumer thread stopped"))
    
    (log/info "Kafka consumer started"))

(defn stop-consumer!
  "Stop the Kafka consumer."
  []
  (reset! running? false)
  (when-let [c @consumer]
    (try
      (.close c)
      (catch Exception e
        (log/error e "Error closing consumer")))
    (reset! consumer nil))
  (when-let [thread @consumer-thread]
    (future-cancel thread)
    (reset! consumer-thread nil))
  (log/info "Kafka consumer stopped"))

;; ============================================================================
;; SSE Integration
;; ============================================================================

(defonce sse-channels
  ;; Map of session-id -> #{channel}
  (atom {}))



(defn register-sse-channel!
  "Register an SSE channel for a session."
  [session-id channel]
  (swap! sse-channels update session-id (fnil conj #{}) channel)
  (log/debug "Registered SSE channel for session" session-id))

(defn unregister-sse-channel!
  "Unregister an SSE channel."
  [session-id channel]
  (swap! sse-channels update session-id disj channel)
  ;; Clean up empty sets
  (when (empty? (get @sse-channels session-id))
    (swap! sse-channels dissoc session-id))
  (log/debug "Unregistered SSE channel for session" session-id))

(defn push-to-session
  "Push data to all SSE channels for a session."
  [session-id data]
  (log/info "[KAFKA-REACTIVE] Pushing to session" session-id "channels:" (count (get @sse-channels session-id [])))
  (log/debug "[KAFKA-REACTIVE] SSE channels map:" @sse-channels)
  (if-let [channels (seq (get @sse-channels session-id))]
    (let [message (str "data: " (cheshire.core/generate-string data) "\n\n")]
      (log/info "[KAFKA-REACTIVE] Sending update to" (count channels) "channel(s) for session" session-id)
      (doseq [channel channels]
        (try
          (http-server/send! channel message false)
          (log/info "[KAFKA-REACTIVE] Successfully pushed update to channel for session" session-id)
          (catch Exception e
            (log/error e "[KAFKA-REACTIVE] Error sending to SSE channel")
            ;; Clean up dead channel
            (unregister-sse-channel! session-id channel)))))
    (log/warn "[KAFKA-REACTIVE] No SSE channels found for session" session-id)))

(defn create-subscription-callback
  "Create a callback that pushes query results via SSE."
  [session-id]
  (fn [{:keys [subscription-id query result]}]
    (log/debug "Pushing update for subscription" subscription-id "to session" session-id)
    (push-to-session session-id
                    {:type :query-update
                     :subscription-id subscription-id
                     :query query
                     :result result
                     :timestamp (System/currentTimeMillis)})))

(defn register-keypath-subscription!
  "Register that a session has keypath subscriptions to todo_sessions table"
  [session-id]
  (swap! session-subscriptions update session-id (fnil conj #{}) :keypath)
  (log/info "Registered keypath subscription for session" session-id))

(defn trigger-keypath-updates!
  "Trigger updates for sessions with keypath subscriptions when todo_sessions changes"
  [table-name]
  (when (= "todo_sessions" (str/lower-case table-name))
    (log/info "Triggering keypath updates for todo_sessions change")
    ;; Get all sessions with keypath subscriptions
    (doseq [[session-id sub-types] @session-subscriptions]
      (when (contains? sub-types :keypath)
        (log/info "Session" session-id "has keypath subscription, triggering update")
        ;; Force a session update to trigger SSE
        (when-let [session-atom (session/get-session session-id)]
          ;; Touch the session to trigger watchers
          (swap! session-atom identity))))))

;; ============================================================================
;; Public API
;; ============================================================================

(defn subscribe-query!
  "Subscribe to a SQL query with automatic updates on data changes.
   Returns a subscription ID that can be used to unsubscribe."
  [session-id sql & [params]]
  (let [sub-id (str "sub-" (java.util.UUID/randomUUID))
        callback (create-subscription-callback session-id)]
    (register-query-subscription! sub-id sql params callback session-id)
    ;; Execute query immediately
    (re-execute-subscription sub-id)
    (log/debug "Subscription" sub-id "created for session" session-id)
    sub-id))

(defn unsubscribe-query!
  "Unsubscribe from a query subscription."
  [sub-id]
  (unregister-query-subscription! sub-id))

(defn init!
  "Initialize the Kafka-based reactive system."
  [& [config]]
  (when config
    (reset! kafka-config (merge @kafka-config config)))
  (start-consumer!))

(defn shutdown!
  "Shutdown the reactive system."
  []
  (stop-consumer!)
  (reset! active-subscriptions {})
  (reset! table-to-subs {})
  (reset! sse-channels {}))

;; ============================================================================
;; Example Usage
;; ============================================================================

(comment
  ;; Start the reactive system
  (init!)
  
  ;; Subscribe to a query
  (def sub-id (subscribe-query! "session-123" 
                                "SELECT * FROM todos WHERE completed = false"))
  
  ;; The query will automatically re-execute when the todos table changes
  ;; Results will be pushed via SSE to connected clients
  
  ;; Unsubscribe
  (unsubscribe-query! sub-id)
  
  ;; Shutdown
  (shutdown!))