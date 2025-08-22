(ns reactor.meta-tracking
  "Async meta-table tracking for Reactor debugging.
   Tracks subscriptions, events, reactions, and performance metrics."
  (:require [reactor.xtdb-store :as xts]
            [reactor.session_simple :as session]
            [clojure.core.async :as async :refer [go-loop <! >! chan put!]]
            [reactor.log :as log])
  (:import [java.util UUID]
           [java.time Instant]))

;; Async channel for non-blocking meta-data writes
(defonce meta-channel (chan 1000))
(defonce tracking-enabled? (atom true))

;; Forward declarations
(declare process-subscription!)
(declare process-event!)
(declare process-reaction!)
(declare process-performance!)

;; Background processor for meta events
(defonce processor 
  (go-loop []
    (when-let [event (<! meta-channel)]
      (try
        (case (:type event)
          :subscription (process-subscription! event)
          :event (process-event! event)
          :reaction (process-reaction! event)
          :performance (process-performance! event)
          (log/warn :meta (str "Unknown meta event type: " (:type event))))
        (catch Exception e
          (log/error :meta (str "Error processing meta event: " event) e)))
      (recur))))

(defn- generate-id []
  (str "meta-" (UUID/randomUUID)))

(defn- current-timestamp []
  (str (Instant/now)))

;; ========== Subscription Tracking ==========

(defn track-subscription-created!
  "Async track when a subscription is created"
  [sub-id session-id query tables]
  (log/debug :meta (str "track-subscription-created! called for " sub-id " enabled? " @tracking-enabled?))
  (when @tracking-enabled?
    (log/debug :meta (str "Putting subscription event on channel for " sub-id))
    (put! meta-channel
          {:type :subscription
           :action :created
           :sub-id sub-id
           :session-id session-id
           :query query
           :tables tables
           :timestamp (current-timestamp)})))

(defn track-subscription-updated!
  "Async track when a subscription receives an update"
  [sub-id execution-time-ms result-count]
  (when @tracking-enabled?
    (put! meta-channel
                {:type :subscription
                 :action :updated
                 :sub-id sub-id
                 :execution-time-ms execution-time-ms
                 :result-count result-count
                 :timestamp (current-timestamp)})))

(defn track-subscription-removed!
  "Async track when a subscription is removed"
  [sub-id reason]
  (when @tracking-enabled?
    (put! meta-channel
                {:type :subscription
                 :action :removed
                 :sub-id sub-id
                 :reason reason
                 :timestamp (current-timestamp)})))

;; ========== Event Tracking ==========

(defn track-event!
  "Async track an event with category"
  [category event-type payload session-id]
  (when @tracking-enabled?
    (put! meta-channel
                {:type :event
                 :category category
                 :event-type event-type
                 :payload payload
                 :session-id session-id
                 :timestamp (current-timestamp)})))

;; ========== Reaction Tracking ==========

(defn track-reaction!
  "Async track when a table change triggers reactions"
  [table-name change-type affected-subs]
  (when @tracking-enabled?
    (put! meta-channel
                {:type :reaction
                 :table-name table-name
                 :change-type change-type
                 :affected-subscriptions affected-subs
                 :timestamp (current-timestamp)})))

;; ========== Performance Tracking ==========

(defn track-query-performance!
  "Async track query execution performance"
  [query execution-time-ms result-count]
  (when @tracking-enabled?
    (put! meta-channel
                {:type :performance
                 :metric-type :query
                 :query query
                 :execution-time-ms execution-time-ms
                 :result-count result-count
                 :timestamp (current-timestamp)})))

;; ========== Internal processors ==========

(defn- process-subscription! [{:keys [action sub-id session-id query tables timestamp 
                                    execution-time-ms result-count reason]}]
  (log/debug :meta (str "Processing subscription meta-event: " action " " sub-id))
  (let [node @session/default-node]
    (log/debug :meta (str "Node is: " (if node "available" "nil")))
    (when node
      (case action
        :created
        (do
          (log/debug :meta (str "Inserting subscription record for " sub-id))
          (try
            (let [result (xts/execute-sql node
                          "INSERT INTO reactor_subscriptions (_id, sub_id, session_id, query, tables, 
                                                              status, created_at, update_count, 
                                                              total_execution_time, last_updated)
                           VALUES (?, ?, ?, ?, ?, 'active', ?, 0, 0, ?)"
                          (generate-id) sub-id session-id query (pr-str tables) timestamp timestamp)]
              (log/debug :meta (str "Insert result: " result)))
            (catch Exception e
              (log/error :meta "Failed to insert subscription" e))))
        
        :updated
        (try
          (let [result (xts/execute-sql node
                        "UPDATE reactor_subscriptions 
                         SET update_count = update_count + 1,
                             total_execution_time = total_execution_time + ?,
                             last_result_count = ?,
                             last_updated = ?
                         WHERE sub_id = ?"
                        execution-time-ms result-count timestamp sub-id)]
            (log/debug :meta (str "Update result for " sub-id ": " result)))
          (catch Exception e
            (log/error :meta (str "Failed to update subscription " sub-id) e)))
        
        :removed
        (xts/execute-sql node
          "UPDATE reactor_subscriptions 
           SET status = 'removed',
               removed_at = ?,
               removal_reason = ?
           WHERE sub_id = ?"
          timestamp reason sub-id)))))

(defn- process-event! [{:keys [category event-type payload session-id timestamp]}]
  (let [node @session/default-node]
    (when node
      (xts/execute-sql node
        "INSERT INTO reactor_events (_id, category, event_type, payload, session_id, created_at)
         VALUES (?, ?, ?, ?, ?, ?)"
        (generate-id) category event-type (pr-str payload) session-id timestamp))))

(defn- process-reaction! [{:keys [table-name change-type affected-subscriptions timestamp]}]
  (let [node @session/default-node]
    (when node
      (xts/execute-sql node
        "INSERT INTO reactor_reactions (_id, table_name, change_type, 
                                        affected_subscriptions, triggered_at)
         VALUES (?, ?, ?, ?, ?)"
        (generate-id) table-name change-type (pr-str affected-subscriptions) timestamp))))

(defn- process-performance! [{:keys [metric-type query execution-time-ms result-count timestamp]}]
  (let [node @session/default-node]
    (when node
      (xts/execute-sql node
        "INSERT INTO reactor_performance (_id, metric_type, query, 
                                          execution_time_ms, result_count, measured_at)
         VALUES (?, ?, ?, ?, ?, ?)"
        (generate-id) metric-type query execution-time-ms result-count timestamp))))

;; ========== Table Creation ==========

(defn ensure-meta-tables!
  "Create meta-tracking tables if they don't exist"
  []
  (let [node @session/default-node]
    (log/info :meta (str "Node for meta-tracking: " (if node (str "exists: " (type node)) "nil")))
    (when node
      (log/info :meta (str "Creating meta-tracking tables with node: " node))
      
      ;; Subscriptions table - XTDB 2.0 creates tables on first insert
      (try
        (log/info :meta "Initializing reactor_subscriptions table...")
        ;; Insert a dummy row to create the table, then delete it
        (xts/execute-sql node
          "INSERT INTO reactor_subscriptions (_id, sub_id, session_id, query, tables, status, created_at, update_count, total_execution_time, last_updated)
           VALUES ('init-dummy', 'dummy', 'system', 'init', '[]', 'init', CURRENT_TIMESTAMP, 0, 0, CURRENT_TIMESTAMP)")
        (xts/execute-sql node
          "DELETE FROM reactor_subscriptions WHERE _id = 'init-dummy'")
        (log/info :meta "reactor_subscriptions table initialized")
        (catch Exception e
          (log/error :meta "Failed to initialize subscriptions table" e)))
      
      ;; Events table - XTDB 2.0 creates tables on first insert
      (try
        (log/info :meta "Initializing reactor_events table...")
        (xts/execute-sql node
          "INSERT INTO reactor_events (_id, category, event_type, payload, session_id, created_at)
           VALUES ('init-dummy', 'system', 'init', '{}', 'system', CURRENT_TIMESTAMP)")
        (xts/execute-sql node
          "DELETE FROM reactor_events WHERE _id = 'init-dummy'")
        (log/info :meta "reactor_events table initialized")
        (catch Exception e
          (log/error :meta "Failed to initialize events table" e)))
      
      ;; Reactions table - XTDB 2.0 creates tables on first insert
      (try
        (log/info :meta "Initializing reactor_reactions table...")
        (xts/execute-sql node
          "INSERT INTO reactor_reactions (_id, table_name, change_type, affected_subscriptions, triggered_at)
           VALUES ('init-dummy', 'system', 'init', '[]', CURRENT_TIMESTAMP)")
        (xts/execute-sql node
          "DELETE FROM reactor_reactions WHERE _id = 'init-dummy'")
        (log/info :meta "reactor_reactions table initialized")
        (catch Exception e
          (log/error :meta "Failed to initialize reactions table" e)))
      
      ;; Performance table - XTDB 2.0 creates tables on first insert
      (try
        (log/info :meta "Initializing reactor_performance table...")
        (xts/execute-sql node
          "INSERT INTO reactor_performance (_id, metric_type, query, execution_time_ms, result_count, measured_at)
           VALUES ('init-dummy', 'system', 'init', 0, 0, CURRENT_TIMESTAMP)")
        (xts/execute-sql node
          "DELETE FROM reactor_performance WHERE _id = 'init-dummy'")
        (log/info :meta "reactor_performance table initialized")
        (catch Exception e
          (log/error :meta "Failed to initialize performance table" e)))
      
      (log/info :meta "Meta-tracking tables ready"))))

;; Initialize on namespace load
(defn init!
  "Initialize meta-tracking system"
  []
  (ensure-meta-tables!)
  (log/info :meta "Meta-tracking system initialized"))