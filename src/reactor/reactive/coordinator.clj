(ns reactor.reactive.coordinator
  "Coordinates reactions to data changes.
   This is the bridge between Kafka detection and the SQL pipeline."
  (:require [reactor.sql-pipeline :as pipeline]
            [reactor.subscriptions.store :as sub-store]
            [reactor.kafka-reactive :as kafka]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <! >! timeout]]))

;; ============================================================================
;; Reaction Execution
;; ============================================================================

(defn execute-subscription-reaction
  "Execute a reaction for a single subscription.
   Returns the result with subscription metadata."
  [subscription]
  (let [sub-id (:id subscription)
        session-id (:session-id subscription)]
    (log/info "[COORDINATOR] Executing reaction for subscription" sub-id
             "Session:" session-id)
    
    ;; Execute through the pipeline - it handles everything!
    (let [result (pipeline/execute-reaction sub-id)]
      (assoc result
             :session-id session-id
             :subscription-id sub-id))))

(defn broadcast-result
  "Broadcast result to appropriate clients"
  [{:keys [subscription-id session-id results diff success error]}]
  (let [message (cond
                  error
                  {:type :error
                   :subscription-id subscription-id
                   :error error
                   :timestamp (System/currentTimeMillis)}
                  
                  ;; Field-level diff update
                  (and diff (= (:mode diff) :field))
                  {:type :field-diff-update
                   :subscription-id subscription-id
                   :diff diff
                   :timestamp (System/currentTimeMillis)}
                  
                  ;; Row-level diff update
                  (and diff (not= (:type diff) :full))
                  {:type :diff-update
                   :subscription-id subscription-id
                   :diff diff
                   :timestamp (System/currentTimeMillis)}
                  
                  ;; Otherwise send full results (client expects nested under :result)
                  :else
                  {:type :query-update
                   :subscription-id subscription-id
                   :result {:results results}  ; Client expects this nested structure
                   :timestamp (System/currentTimeMillis)})]
    
    ;; Use kafka-reactive's push-to-session which has the registered channels
    (kafka/push-to-session session-id message)
    (log/info "[COORDINATOR] Broadcast to session" session-id
             "- Subscription:" subscription-id
             "- Type:" (:type message))))

;; ============================================================================
;; Batch Processing
;; ============================================================================

(defn execute-and-broadcast
  "Execute a subscription and broadcast results"
  [subscription]
  (let [result (execute-subscription-reaction subscription)]
    (when (:success result)
      (broadcast-result result))
    result))

(defn process-subscriptions
  "Process multiple subscriptions (can be parallelized)"
  [subscriptions]
  (log/info "[COORDINATOR] Processing" (count subscriptions) "subscriptions")
  
  ;; Group by session for efficient broadcasting
  (let [by-session (group-by :session-id subscriptions)
        results (atom [])]
    
    (doseq [[session-id session-subs] by-session]
      (log/info "[COORDINATOR] Processing" (count session-subs) 
               "subscriptions for session" session-id)
      
      (doseq [sub session-subs]
        (try
          (let [result (execute-and-broadcast sub)]
            (swap! results conj result))
          (catch Exception e
            (log/error e "[COORDINATOR] Error processing subscription" (:id sub))))))
    
    @results))

;; ============================================================================
;; Change Detection Interface
;; ============================================================================

(defn handle-table-change
  "Handle a change to a specific table.
   This is called by Kafka monitor when it detects changes."
  [table-name]
  (log/info "[COORDINATOR] Table changed:" table-name)
  
  ;; Find all active subscriptions watching this table
  (let [subscriptions (filter #(= :active (:status %))
                             (sub-store/find-by-table table-name))]
    
    (if (empty? subscriptions)
      (log/debug "[COORDINATOR] No subscriptions watching table" table-name)
      (do
        (log/info "[COORDINATOR] Found" (count subscriptions) 
                 "subscriptions watching" table-name)
        (process-subscriptions subscriptions)))))

(defn handle-tables-change
  "Handle changes to multiple tables"
  [table-names]
  (log/info "[COORDINATOR] Tables changed:" table-names)
  
  ;; Find all unique subscriptions watching any of these tables
  (let [all-subs (sub-store/find-by-tables table-names)
        active-subs (filter #(= :active (:status %)) all-subs)
        ;; Deduplicate (a subscription might watch multiple changed tables)
        unique-subs (distinct active-subs)]
    
    (if (empty? unique-subs)
      (log/debug "[COORDINATOR] No subscriptions watching tables" table-names)
      (do
        (log/info "[COORDINATOR] Found" (count unique-subs)
                 "unique subscriptions watching" table-names)
        (process-subscriptions unique-subs)))))

;; ============================================================================
;; Debouncing (Simple Implementation)
;; ============================================================================

(defonce pending-reactions (atom {}))
(defonce reaction-executor (atom nil))
(def debounce-delay-ms 100)

(defn request-reaction!
  "Request a debounced reaction for a subscription"
  [subscription-id]
  (swap! pending-reactions assoc subscription-id (System/currentTimeMillis)))

(defn process-pending-reactions!
  "Process all pending reactions that are ready"
  []
  (let [now (System/currentTimeMillis)
        ready (filter (fn [[sub-id requested-at]]
                       (> (- now requested-at) debounce-delay-ms))
                     @pending-reactions)]
    
    (when (seq ready)
      (log/debug "[COORDINATOR] Processing" (count ready) "pending reactions")
      
      ;; Clear pending
      (doseq [[sub-id _] ready]
        (swap! pending-reactions dissoc sub-id))
      
      ;; Load and process subscriptions
      (let [subscriptions (keep sub-store/get-subscription (map first ready))]
        (process-subscriptions subscriptions)))))

(defn start-reaction-executor!
  "Start background thread to process pending reactions"
  []
  (when-not @reaction-executor
    (reset! reaction-executor
           (go-loop []
             (process-pending-reactions!)
             (<! (timeout 50))
             (recur)))
    (log/info "[COORDINATOR] Reaction executor started")))

(defn stop-reaction-executor!
  "Stop the reaction executor"
  []
  (when-let [executor @reaction-executor]
    (async/close! executor)
    (reset! reaction-executor nil)
    (reset! pending-reactions {})
    (log/info "[COORDINATOR] Reaction executor stopped")))

;; ============================================================================
;; Manual Triggers (for testing)
;; ============================================================================

(defn trigger-subscription
  "Manually trigger a subscription reaction (for testing)"
  [subscription-id]
  (if-let [subscription (sub-store/get-subscription subscription-id)]
    (execute-and-broadcast subscription)
    {:success false
     :error {:type :not-found
             :message (str "Subscription not found: " subscription-id)}}))

(defn trigger-all-subscriptions
  "Manually trigger all active subscriptions (for testing)"
  []
  (let [active (sub-store/find-active)]
    (log/info "[COORDINATOR] Manually triggering" (count active) "subscriptions")
    (process-subscriptions active)))

;; ============================================================================
;; Session Management
;; ============================================================================

(defn handle-session-connected
  "Handle a new session connection"
  [session-id]
  (log/info "[COORDINATOR] Session connected:" session-id)
  ;; Could trigger initial data load for session subscriptions
  (let [subscriptions (sub-store/find-by-session session-id)]
    (when (seq subscriptions)
      (log/info "[COORDINATOR] Sending initial data for" 
               (count subscriptions) "subscriptions")
      (process-subscriptions subscriptions))))

(defn handle-session-disconnected
  "Handle session disconnection"
  [session-id]
  (log/info "[COORDINATOR] Session disconnected:" session-id)
  ;; Could pause subscriptions or clean up
  ;; For now, subscriptions remain active for reconnection)
  )

;; ============================================================================
;; Statistics
;; ============================================================================

(defn stats
  "Get coordinator statistics"
  []
  {:pending-reactions (count @pending-reactions)
   :executor-running? (some? @reaction-executor)})