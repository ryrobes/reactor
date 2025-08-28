(ns reactor.kafka-reactive
  "Kafka-based real-time reactivity for XTDB transaction logs.
   Monitors XTDB transactions and triggers re-execution of subscribed queries."
  (:require [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [taoensso.nippy :as nippy]
            [io.aviso.ansi :as ansi]
            [cheshire.core]
            [reactor.xtdb-store :as xts]
            [reactor.meta-tracking :as meta]
            [reactor.session_simple :as session]
            [reactor.structural-diff :as sdiff]
            [reactor.temporal-cache :as tcache]
            [reactor.sql-template :as sql-template]
            [reactor.sql-resolver :as resolver]
            [org.httpkit.server :as http-server]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.walk :as walk]
            [clojure.core.async :as async :refer [go go-loop chan <! >! close! timeout]]))

;; ============================================================================
;; Configuration
;; ============================================================================

(defonce kafka-config  
  (atom {"bootstrap.servers" "10.174.1.144:9092"
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

(defonce client-has-base-data
  ;; Track which client+subscription combinations have received initial data
  ;; #{[session-id subscription-id client-id]}
  (atom #{}))

(defonce sse-channels
  ;; Map of session-id -> #{channel}
  (atom {}))

(defonce subscription-dependencies
  ;; Map of parent-block-id -> #{subscription-ids that depend on it}
  ;; This tracks which subscriptions have templates referencing each block
  (atom {}))

;; ============================================================================
;; Debouncing for Subscription Re-execution
;; ============================================================================

;; Forward declaration for functions defined later
(declare re-execute-subscription)

(defonce pending-re-executions
  ;; Map of subscription-id -> timestamp of when re-execution was requested
  (atom {}))

(defonce debounce-delay-ms 
  ;; Configurable debounce delay in milliseconds
  ;; 150ms provides good balance between responsiveness and efficiency
  (atom 150))

(defonce debounce-executor
  ;; Single background thread that processes pending re-executions
  (atom nil))

(defonce debounce-running? (atom false))

(defn start-debounce-executor!
  "Start the debounce executor that processes pending re-executions"
  []
  (when-not @debounce-executor
    (reset! debounce-running? true)
    (reset! debounce-executor
            (go-loop []
              (when @debounce-running?
                ;; Check for pending re-executions
                (let [now (System/currentTimeMillis)
                      delay @debounce-delay-ms
                      ready-subs (->> @pending-re-executions
                                     (filter (fn [[sub-id requested-at]]
                                              (> (- now requested-at) delay)))
                                     (map first)
                                     set)]
                  
                  ;; Process ready subscriptions
                  (when (seq ready-subs)
                    (log/debug "[DEBUG-DEBOUNCE] Processing" (count ready-subs) "debounced re-executions:" ready-subs)
                    (doseq [sub-id ready-subs]
                      (log/debug "[DEBUG-DEBOUNCE] Executing subscription" sub-id)
                      ;; Remove from pending
                      (swap! pending-re-executions dissoc sub-id)
                      ;; Execute the subscription
                      (try
                        (re-execute-subscription sub-id)
                        (catch Exception e
                          (log/error e "Error in debounced re-execution of" sub-id)))))
                  
                  ;; Sleep briefly before next check
                  (<! (timeout 50))
                  (recur)))))
    (log/info "Debounce executor started with delay:" @debounce-delay-ms "ms")))

(defn stop-debounce-executor!
  "Stop the debounce executor"
  []
  (reset! debounce-running? false)
  (when-let [executor @debounce-executor]
    (close! executor)
    (reset! debounce-executor nil)
    (reset! pending-re-executions {})
    (log/info "Debounce executor stopped")))

(defn request-re-execution!
  "Request a debounced re-execution of a subscription.
   Multiple requests within the debounce window will be coalesced."
  [sub-id]
  ;; ALL subscriptions now participate in re-execution (no more inert check)
  (when-let [sub-info (get @active-subscriptions sub-id)]
    ;; Only update if not already pending or if the existing request is old
    (let [now (System/currentTimeMillis)
          existing (get @pending-re-executions sub-id)]
      (when (or (nil? existing)
                (> (- now existing) (* 2 @debounce-delay-ms))) ; Re-request if very old
        (swap! pending-re-executions assoc sub-id now)
        ;; Debounce request logging disabled for performance
        #_(log/debug "Debounced re-execution requested for" sub-id
                    (when (:temporal? sub-info) "(temporal)"))))))

(defn set-debounce-delay!
  "Set the debounce delay in milliseconds.
   For hot tables like reactor_subscriptions, a higher value (200-300ms) is recommended.
   For normal tables, 100-150ms provides good responsiveness."
  [delay-ms]
  (reset! debounce-delay-ms delay-ms)
  (log/info "Debounce delay set to" delay-ms "ms"))

(defn configure-hot-table-debouncing!
  "Configure special debouncing for known hot tables"
  []
  ;; Hot tables that trigger many reactions
  (def hot-tables #{"reactor_subscriptions" "reactor_events" "reactor_reactions"})
  ;; Could implement per-table delays if needed
  (set-debounce-delay! 200))

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
  [sub-id sql params callback session-id & [client-id is-temporal-param? parent-block-ids]]
  (let [tables (extract-tables-from-sql sql)
        ;; Check if this is a temporal query - either has AS OF TIMESTAMP clause OR passed as parameter
        is-temporal? (or is-temporal-param?  ;; Explicitly passed from caller (for as-of queries)
                        (and (string? sql)
                             (re-find #"(?i)FOR\s+SYSTEM_TIME\s+AS\s+OF\s+TIMESTAMP" sql))) 
        sub-info {:query sql
                  :params params
                  :tables tables
                  :callback callback
                  :session-id session-id
                  :client-id client-id
                  :temporal? is-temporal?  ;; Mark temporal queries (for logging/debugging)
                  :inert? false             ;; NO LONGER INERT - all queries participate in reactive cycle
                  :parent-blocks parent-block-ids}]  ;; Track which blocks this subscription depends on
    (swap! active-subscriptions assoc sub-id sub-info)
    ;; Update table index for ALL queries (including temporal)
    ;; This allows temporal queries to also react to changes (and benefit from diffing)
    (doseq [table tables]
      (swap! table-to-subs update (str/lower-case table) (fnil conj #{}) sub-id))
    ;; Track parent block dependencies
    (when parent-block-ids
      (doseq [parent-id parent-block-ids]
        (swap! subscription-dependencies update parent-id (fnil conj #{}) sub-id)))
    (log/info "Registered" (if is-temporal? "TEMPORAL (reactive)" "REACTIVE") 
             "subscription" sub-id "for tables:" tables
             "- will react to changes")
    ;; Track subscription creation
    (meta/track-subscription-created! sub-id session-id sql tables)
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
    ;; Clean up client tracking - remove entries containing this sub-id
    (swap! client-has-base-data 
           (fn [clients]
             (into #{} (remove #(or (= (second %) sub-id)
                                   ;; Also remove if it's part of a cache key vector
                                   (and (vector? %)
                                        (some #{sub-id} %))) 
                              clients))))
    ;; Clean up parent block dependencies
    (when-let [parent-blocks (:parent-blocks sub-info)]
      (doseq [parent-id parent-blocks]
        (swap! subscription-dependencies update parent-id disj sub-id)
        ;; Remove empty sets
        (when (empty? (get @subscription-dependencies parent-id))
          (swap! subscription-dependencies dissoc parent-id))))
    ;; Track subscription removal
    (meta/track-subscription-removed! sub-id "manual-unregister")
    (log/info "Unregistered query subscription" sub-id)))

(defn calculate-result-metrics
  "Calculate size metrics for query results - cheap character count approximation"
  [results]
  (try
    ;; Use pr-str on just a sample if results are large
    (if (> (count results) 100)
      ;; For large result sets, sample first 10 and last 10 rows and extrapolate
      (let [sample (concat (take 10 results) (take-last 10 results))
            sample-size (reduce + (map #(count (pr-str %)) sample))
            avg-row-size (/ sample-size (count sample))]
        (long (* avg-row-size (count results))))
      ;; For small result sets, calculate exact size
      (reduce + (map #(count (pr-str %)) results)))
    (catch Exception e
      (log/warn "Failed to calculate result size metrics" e)
      0)))

;; Cache for result metrics keyed by [query params timestamp]
(defonce metrics-cache (atom {}))
(defonce cache-cleanup-executor (atom nil))

;; Cache for last sent results to enable diff mode
;; {[session-id subscription-id] -> {:results [...] :structure {...} :checksum ... :timestamp ...}}
(defonce client-result-cache (atom {}))

;; Configuration for diff mode
(defonce diff-config 
  (atom {:enabled true
         :field-based? true  ; Enable field-level diffing (renamed for consistency)
         :structural-diff? true   ; Enable deep structural diffing for EDN fields
         :edn-fields #{:state :app_state :app-state "app_state"}  ; Fields known to contain EDN strings (multiple formats)
         :max-result-size 1000  ; Don't diff if more than N rows
         :min-compression-ratio 0.7  ; Send full if diff > 70% of original
         :structure-check true  ; Verify structure before diffing
         :debug-logging true    ; Enable detailed debug logging
         :temporal-always-diff true  ; Always try to diff temporal queries (they're great candidates!)
         :temporal-max-size 5000}))  ; Higher limit for temporal queries

;; ============================================================================
;; Diff Configuration Management
;; ============================================================================

(defn set-diff-mode!
  "Configure diff mode - :none, :row, :field, or :structural"
  [mode]
  (case mode
    :none (swap! diff-config assoc 
                :enabled false
                :field-based? false
                :structural-diff? false)
    :row (swap! diff-config assoc
               :enabled true
               :field-based? false
               :structural-diff? false)
    :field (swap! diff-config assoc
                 :enabled true
                 :field-based? true
                 :structural-diff? false)
    :structural (swap! diff-config assoc
                      :enabled true
                      :field-based? true
                      :structural-diff? true)
    (log/warn "[DIFF-CONFIG] Unknown diff mode:" mode))
  (log/info "[DIFF-CONFIG] Diff mode set to:" mode 
           "\n  Current config:" @diff-config)
  mode)

(defn get-diff-stats
  "Get statistics about diff performance"
  []
  (let [cache-entries @client-result-cache
        total-entries (count cache-entries)
        with-diffs (filter #(contains? (val %) :last-diff-type) cache-entries)
        field-diffs (filter #(= :field-diff (:last-diff-type (val %))) cache-entries)
        row-diffs (filter #(= :row-diff (:last-diff-type (val %))) cache-entries)]
    {:total-cached total-entries
     :with-diffs (count with-diffs)
     :field-diffs (count field-diffs)
     :row-diffs (count row-diffs)
     :current-mode (cond
                    (not (:enabled @diff-config)) :none
                    (:structural-diff? @diff-config) :structural
                    (:field-based? @diff-config) :field
                    :else :row)}))

(defn normalize-temporal-query
  "Extract base query and timestamp from temporal queries for better caching"
  [query]
  ;; Handle both single-line and multi-line queries with flexible whitespace
  (if-let [match (re-find #"(?is)^(.*?)\s*\n?FOR\s+SYSTEM_TIME\s+AS\s+OF\s+TIMESTAMP\s+'([^']+)'\s*\n?(.*)$" query)]
    (let [[_ base-query timestamp remainder] match
          ;; Clean up the base query and remainder
          clean-base (str/trim base-query)
          clean-remainder (str/trim remainder)
          full-base (if (empty? clean-remainder)
                     clean-base
                     (str clean-base " " clean-remainder))]
      ;(println "*********" full-base )
      {:base-query full-base
       :temporal-param timestamp
       :is-temporal true})
    {:base-query query
     :temporal-param nil
     :is-temporal false}))

(defn extract-row-structure
  "Extract the structure (field names and types) from results"
  [results]
  (when (seq results)
    (let [first-row (first results)]
      {:fields (set (keys first-row))
       :field-count (count first-row)})))

(defn same-structure?
  "Check if two result sets have the same structure"
  [old-structure new-structure]
  ;; Special case: if both are nil (empty result sets), they have the same structure
  (or (and (nil? old-structure) (nil? new-structure))
      (and old-structure
           new-structure
           (= (:fields old-structure) (:fields new-structure))
           (= (:field-count old-structure) (:field-count new-structure)))))

(defn compute-field-diff
  "Compute field-level differences between two rows"
  [old-row new-row & {:keys [structural-diff? edn-fields]
                      :or {structural-diff? true
                           edn-fields #{}}}]
  (let [result (if structural-diff?
                 ;; Use enhanced structural diffing
                 (sdiff/compute-enhanced-field-diff old-row new-row 
                                                    :deep-diff? true
                                                    :edn-fields edn-fields)
                 ;; Use basic field diffing
                 (let [all-keys (set/union (set (keys old-row)) (set (keys new-row)))
                       changed-fields (reduce (fn [acc k]
                                               (let [old-val (get old-row k ::not-found)
                                                     new-val (get new-row k ::not-found)]
                                                 (cond
                                                   ;; Field added
                                                   (= old-val ::not-found)
                                                   (assoc acc k {:op :add :value new-val})
                                                   
                                                   ;; Field removed
                                                   (= new-val ::not-found)
                                                   (assoc acc k {:op :remove})
                                                   
                                                   ;; Field changed
                                                   (not= old-val new-val)
                                                   (assoc acc k {:op :update :value new-val})
                                                   
                                                   ;; Field unchanged - don't include
                                                   :else acc)))
                                             {}
                                             all-keys)]
                   (when (seq changed-fields)
                     changed-fields)))]
    ;; Debug logging for field diffing
    (when (and result (:debug-logging @diff-config))
      (let [field-breakdown (reduce (fn [acc [k v]]
                                      (update acc (:op v) (fnil inc 0)))
                                    {}
                                    result)
            structural-fields (filter #(= :structural-update (:op (get result %))) 
                                     (keys result))
            edn-detected (filter #(and (contains? edn-fields %)
                                       (contains? result %))
                                 (keys result))]
        (log/info "[FIELD-DIFF] Row changes detected:"
                  "\n  Changed fields:" (keys result)
                  "\n  Breakdown:" field-breakdown
                  (when (seq structural-fields)
                    (str "\n  Structural diffs on: " structural-fields))
                  (when (seq edn-detected)
                    (str "\n  EDN fields detected: " edn-detected))
                  (when structural-diff?
                    "\n  Structural diffing: ENABLED")
                  (when (seq edn-fields)
                    (str "\n  Configured EDN fields: " edn-fields)))))
    result))

(defn compute-row-diff
  "Compute diff between old and new SQL results
   Returns nil if diff is not efficient"
  [old-results new-results & {:keys [field-based? query] :or {field-based? true}}]
  (try
    (let [;; Find the ID column - prefer _id, id, or first column
          id-key (or (some #{:_id :id "_id" "id"} (keys (first new-results)))
                     (first (keys (first new-results))))
          ;; Index by ID
          old-by-id (if id-key
                      (into {} (map (juxt id-key identity) old-results))
                      {})
          new-by-id (if id-key
                      (into {} (map (juxt id-key identity) new-results))
                      {})
          old-ids (set (keys old-by-id))
          new-ids (set (keys new-by-id))
          ;; Compute changes
          added-ids (set/difference new-ids old-ids)
          removed-ids (set/difference old-ids new-ids)
          updated-ids (for [id (set/intersection old-ids new-ids)
                           :let [old-row (old-by-id id)
                                 new-row (new-by-id id)]
                           :when (not= old-row new-row)]
                       id)
          ;; Build diff - use field-based diffing for updates if enabled
          updated-entries (if field-based?
                           (let [entries (for [id updated-ids
                                              :let [old-row (old-by-id id)
                                                    new-row (new-by-id id)
                                                    field-changes (compute-field-diff old-row new-row 
                                                                                     :structural-diff? (:structural-diff? @diff-config true)
                                                                                     :edn-fields (:edn-fields @diff-config #{}))]
                                              :when field-changes]
                                          {:id id 
                                           :field-changes field-changes})]
                             ;; Log field-based metrics
                             (when (and (:debug-logging @diff-config) (seq entries))
                               (let [total-fields-changed (reduce + 0 (map #(count (:field-changes %)) entries))
                                     avg-fields (if (pos? (count entries))
                                                 (/ total-fields-changed (count entries))
                                                 0)]
                                 (log/debug "[FIELD-METRICS] Updated" (count entries) "rows"
                                           "| Total fields changed:" total-fields-changed
                                           "| Avg fields/row:" (format "%.1f" (double avg-fields)))))
                             entries)
                           ;; Fall back to full row updates
                           (for [id updated-ids]
                             {:id id 
                              :new-values (new-by-id id)}))
          diff {:type (if field-based? :field-diff :row-diff)
                :id-key id-key
                :added (map new-by-id added-ids)
                :removed removed-ids
                :updated updated-entries
                ;; Include order only if it changed
                :order (let [old-order (mapv id-key old-results)
                            new-order (mapv id-key new-results)]
                        (when (not= old-order new-order)
                          new-order))}
          ;; Calculate efficiency for field-based diff
          field-change-count (if field-based?
                               (reduce + 0 (map #(count (:field-changes %)) updated-entries))
                               (count updated-ids))
          total-field-count (if field-based?
                             (* (count new-results) 
                                (if (seq new-results)
                                  (count (keys (first new-results)))
                                  0))
                             (count new-results))
          diff-size (+ (count added-ids)
                      (count removed-ids)
                      field-change-count)
          total-size total-field-count
          has-changes? (pos? diff-size)
          compression-ratio (if (zero? total-size)
                             1.0  ; Empty result set - use full update
                             (/ diff-size total-size))]
      
      (log/info (ansi/bold-blue "[DIFF-ANALYSIS] ") (str/replace query #"[\r\n]+" "") "\n" 
               "\n  Mode:" (if field-based? "FIELD-BASED" "ROW-BASED")
               "\n  Added rows:" (count added-ids) 
               "\n  Removed rows:" (count removed-ids)
               "\n  Updated rows:" (count updated-ids)
               (when field-based? 
                 (str "\n  Total field changes: " field-change-count
                      " across " (count updated-ids) " rows"
                      " (avg " (if (pos? (count updated-ids))
                                 (format "%.1f" (/ (double field-change-count) (double (count updated-ids))))
                                 "0")
                      " fields/row)"))
               "\n  Original size:" total-size (if field-based? "fields" "rows")
               "\n  Diff size:" diff-size
               "\n  Compression ratio:" (format "%.2f" (double compression-ratio))
               " (" (format "%.0f%%" (* (double compression-ratio) 100)) " of original)"
               "\n  Will send:" (if (and has-changes?
                                       (< compression-ratio (:min-compression-ratio @diff-config)))
                                 "DIFF" 
                                 "FULL UPDATE"))
      
      ;; Return diff only if it's efficient AND there are actual changes
      ;; Don't send empty diffs (compression 0)
      (when (and has-changes?
                (< compression-ratio (:min-compression-ratio @diff-config)))
        (assoc diff :compression-ratio compression-ratio)))
    (catch Exception e
      (log/warn "Failed to compute diff:" e)
      nil)))

(defn cleanup-metrics-cache!
  "Remove old entries from metrics cache (older than 1 hour)"
  []
  (let [cutoff (- (System/currentTimeMillis) (* 60 60 1000))]
    (swap! metrics-cache
           (fn [cache]
             (into {}
                   (filter (fn [[[_ _ timestamp] _]]
                            (or (nil? timestamp)
                                (> timestamp cutoff)))
                          cache))))
    ;; Also cleanup client result cache
    (swap! client-result-cache
           (fn [cache]
             (into {}
                   (filter (fn [[_ v]]
                            (> (:timestamp v) cutoff))
                          cache))))))

(defn start-cache-cleanup!
  "Start periodic cleanup of metrics cache"
  []
  (when-not @cache-cleanup-executor
    (reset! cache-cleanup-executor
            (java.util.concurrent.Executors/newScheduledThreadPool 1))
    (.scheduleAtFixedRate @cache-cleanup-executor
                         cleanup-metrics-cache!
                         1 ;; initial delay
                         5 ;; period in minutes
                         java.util.concurrent.TimeUnit/MINUTES)))

(defn re-execute-subscription
  "Re-execute a subscription's query and invoke its callback with results."
  [sub-id]
  (when (:debug-logging @diff-config)
    (log/debug "[RE-EXECUTE] Starting re-execution for subscription" sub-id))
  (log/debug "[KAFKA-REACTIVE] Re-executing subscription" sub-id)
  (if-let [{:keys [query params callback session-id client-id temporal? inert?]} (get @active-subscriptions sub-id)]
    (try
      (log/debug "[KAFKA-REACTIVE] Found" (if temporal? "TEMPORAL" "REACTIVE") 
                "subscription" sub-id "for session" session-id 
                (when (> (count query) 100) 
                  (str "\n  SQL: " (subs query 0 100) "...")))
      (if-let [node @session/default-node]
        (let [;; Use centralized resolver for consistent template resolution
              resolution-result (resolver/resolve-sql query session-id)
              resolved-query (:resolved-sql resolution-result)
              old-resolved-query (if false ; disabled old logic
                              (do
                                (println (str "[KAFKA-REACTIVE] Query contains templates, resolving..."
                                              "\n  Subscription ID:" sub-id
                                              "\n  Session ID:" session-id
                                              "\n  Is COUNT query?" (re-find #"(?i)SELECT\s+COUNT.*FROM.*subq" query)))
                                (if-let [session-obj (session/get-session session-id)]
                                  (let [session-state (session/get-state session-obj)
                                        _ (when-not (get-in session-state [:canvas :blocks])
                                            (log/warn "[KAFKA-REACTIVE] Session state has no canvas blocks!"
                                                     "\n  Session ID:" session-id))
                                        ;; Special handling for COUNT queries with temporal clause
                                        ;; Extract temporal clause from outer query
                                        temporal-match (re-find #"(FOR\s+SYSTEM_TIME\s+AS\s+OF\s+TIMESTAMP\s+'[^']+')" query)
                                        has-temporal? (boolean temporal-match)
                                        temporal-clause (when temporal-match (first temporal-match))
                                        ;; Remove temporal clause from outer query if it's a COUNT query
                                        is-count-query? (re-find #"(?i)SELECT\s+COUNT.*FROM\s*\(" query)
                                        clean-query (if (and is-count-query? has-temporal?)
                                                     (str/replace query temporal-clause "")
                                                     query)
                                        ;; Resolve templates - pass temporal clause for special handling
                                        template-result (if (and is-count-query? has-temporal?)
                                                         ;; For count queries with temporal, manually handle template resolution
                                                         (let [template-refs (re-seq #"\{\{([^}]+)\.sql\}\}" clean-query)
                                                               resolved-sql (reduce
                                                                           (fn [sql [full-match block-id]]
                                                                             (if-let [block-sql (or (get-in session-state [:canvas :blocks (keyword block-id) :sql])
                                                                                                  (get-in session-state [:canvas :blocks block-id :sql]))]
                                                                               ;; Add temporal clause to parent SQL INSIDE parentheses
                                                                               (let [temporal-sql (str "(" block-sql " " temporal-clause ")")]
                                                                                 (str/replace sql full-match temporal-sql))
                                                                               sql))
                                                                           clean-query
                                                                           template-refs)]
                                                           {:sql resolved-sql :dependencies (map second template-refs)})
                                                         ;; Normal template resolution
                                                         (sql-template/resolve-sql-templates-with-deps query session-state))
                                        resolved-sql (:sql template-result)]
                                    (println "[KAFKA-REACTIVE] Template resolution complete:"
                                             "\n  Original SQL:" (if (> (count query) 100)
                                                                  (str (subs query 0 100) "...")
                                                                  query)
                                             "\n  Resolved SQL:" (if (> (count resolved-sql) 100)
                                                                  (str (subs resolved-sql 0 100) "...")
                                                                  resolved-sql)
                                             "\n  Templates resolved?" (not= query resolved-sql)
                                             "\n  Temporal handling:" (when (and is-count-query? has-temporal?)
                                                                       "Moved temporal clause to inner SQL"))
                                    ;; Update block SQL cache when templates are resolved
                                    (when-let [update-cache-fn (resolve 'reactor.reactive-server/update-block-sql-cache!)]
                                      ;; Extract block ID from subscription ID (usually :block-id or "block-id" format)
                                      (let [block-id-str (cond
                                                          (keyword? sub-id) (name sub-id)
                                                          (string? sub-id) sub-id
                                                          :else (str sub-id))]
                                        ;; Only update cache if this looks like a block ID (not sql-uuid format)
                                        (when (and (not (str/starts-with? block-id-str "sql-"))
                                                  (not (str/starts-with? block-id-str "temporal-")))
                                          (println "[KAFKA-REACTIVE] Updating block SQL cache for:" block-id-str)
                                          (@update-cache-fn block-id-str query resolved-sql session-id))))
                                    resolved-sql)
                                  (do
                                    (log/error "[KAFKA-REACTIVE] ❌ No session found for template resolution!"
                                              "\n  Session ID:" session-id
                                              "\n  This will cause count queries to fail")
                                    query)))
                              query)  ;; No templates, use original query - end of old logic
              _ (when (:has-templates? resolution-result)
                  (log/info "[KAFKA-REACTIVE] Resolved templates for subscription" sub-id
                           "\n  Dependencies:" (:dependencies resolution-result)
                           "\n  Original length:" (count query)
                           "\n  Resolved length:" (count resolved-query)))
              start-time (System/currentTimeMillis)
              ;; Check if this is a temporal count query that can be cached
              ;; Use resolved query for consistent cache keys
              cached-result (tcache/get-cached resolved-query)
              ;; For temporal queries, need to use time-travel execution
              result (if cached-result
                       ;; Use cached result for temporal count queries
                       (do
                         (log/debug "[KAFKA-REACTIVE] 🎯 CACHE HIT for temporal count query")
                         ;; Return cached result with server-cache flag
                         (assoc cached-result :server-cache? true))
                       ;; Execute query normally
                       (if temporal?
                         (let [;; Extract timestamp from the query
                               timestamp-match (re-find #"FOR\s+SYSTEM_TIME\s+AS\s+OF\s+TIMESTAMP\s+'([^']+)'" resolved-query)
                               timestamp (when timestamp-match (second timestamp-match))
                               exec-result (if timestamp
                                           (do
                                             (log/debug "[KAFKA-REACTIVE] Executing temporal query with timestamp:" timestamp)
                                             ;; The query ALREADY contains the temporal clause, so execute directly
                                             ;; Don't use execute-sql-with-time-travel as it would add another clause
                                             (xts/execute-sql node resolved-query params))
                                           ;; Fallback to regular execution if can't extract timestamp
                                           (xts/execute-sql node resolved-query params))]
                           ;; Cache the result if it's a temporal count query
                           (tcache/cache-result! resolved-query exec-result)
                           exec-result)
                         ;; Regular non-temporal execution
                         (if params
                           (xts/execute-sql node resolved-query params)
                           (xts/execute-sql node resolved-query))))
              execution-time (- (System/currentTimeMillis) start-time)
              results (:results result [])
              result-count (count results)
              ;; Calculate data size metrics (cached for temporal queries)
              cache-key [resolved-query params (:timestamp result)]
              data-size (if-let [cached-size (and cache-key (get @metrics-cache cache-key))]
                          cached-size
                          (let [size (calculate-result-metrics results)]
                            (when cache-key
                              (swap! metrics-cache assoc cache-key size))
                            size))
              ;; Add metrics to result
              result-with-metrics (assoc result
                                         :metrics {:row-count result-count
                                                   :data-size data-size
                                                   :execution-time execution-time})
              ;; Use client-id if available, otherwise fall back to sub-id
              subscription-id (or client-id sub-id)

              ;; Normalize temporal queries to enable proper diffing across time
              {:keys [base-query temporal-param is-temporal]} (normalize-temporal-query query)

              ;; Debug: Log what type of query we're processing
              _ (when (and (:debug-logging @diff-config) 
                          (str/includes? query "FOR SYSTEM_TIME"))
                  (log/info "[TEMPORAL-NORMALIZE-DEBUG] Processing temporal query"
                            "\n  Full query:" query
                            "\n  Normalized - Is temporal?" is-temporal
                            "\n  Base query:" base-query
                            "\n  Temporal param:" temporal-param
                            "\n  Subscription ID:" subscription-id
                            "\n  Client ID:" client-id))

              ;; CRITICAL FIX: Include timestamp in cache key for temporal queries
              ;; Each temporal query at a different timestamp needs its own cache entry
              ;; Otherwise queries at different times would share results (wrong!)
              normalized-params (or params [])
              client-cache-key (if is-temporal
                                 ;; For temporal: MUST include timestamp in cache key
                                 [session-id base-query normalized-params temporal-param]
                                 ;; For regular: use full query with params
                                 [session-id query normalized-params])
              cached-data (get @client-result-cache client-cache-key)

              ;; Log cache lookup for temporal queries
              _ (when (and is-temporal (:debug-logging @diff-config))
                  (log/info "[TEMPORAL-CACHE] Looking up cache"
                            "\n  Cache key:" client-cache-key
                            "\n  Current timestamp:" temporal-param
                            "\n  Cached data exists?" (boolean cached-data)
                            (when cached-data
                              (str "\n  Cached timestamp: " (:params cached-data)
                                   "\n  Cached result count: " (count (:results cached-data))))))

              ;; Log cache key details for debugging
              _ (when (:debug-logging @diff-config)
                  (if is-temporal
                    (log/info "[TEMPORAL-CACHE] Temporal query detected"
                              "\n  Original query:" (if (> (count query) 100)
                                                      (str (subs query 0 100) "...")
                                                      query)
                              "\n  Base query:" (if (> (count base-query) 60)
                                                  (str (subs base-query 0 60) "...")
                                                  base-query)
                              "\n  Timestamp:" temporal-param
                              "\n  Cache key:" client-cache-key
                              "\n  Cached data exists?" (boolean cached-data))
                    (when cached-data
                      (log/debug "[CACHE-HIT] Found cached data for" subscription-id
                                 "| Key:" client-cache-key))))
              new-structure (extract-row-structure results)

              ;; Check if client has received base data for this subscription
              ;; For temporal queries, we need to track per-session since subscription IDs are reused
              ;; Use the cache key itself as the tracking key since it's unique per session+query
              client-tracking-key (if is-temporal
                                    client-cache-key  ;; Use cache key for temporal queries
                                    [session-id sub-id])  ;; Use session+sub-id for regular queries
              client-has-data? (contains? @client-has-base-data client-tracking-key)
              _ (when (:debug-logging @diff-config)
                  (log/info "[CLIENT-TRACKING] Checking client data status"
                            "\n  Tracking key:" client-tracking-key
                            "\n  Has base data?" client-has-data?
                            "\n  Is temporal?" is-temporal
                            "\n  Tracked clients count:" (count @client-has-base-data)))

              ;; Determine if we should send diff or full
              ;; Temporal queries are EXCELLENT candidates for diffing!
              max-size-limit (if (and is-temporal (:temporal-always-diff @diff-config))
                               (:temporal-max-size @diff-config)
                               (:max-result-size @diff-config))
              should-diff? (and (:enabled @diff-config)
                                client-has-data?  ;; Client must have received initial data
                                cached-data
                                (not (str/starts-with? query "SELECT COUNT(*) as cnt FROM ("))
                                (:results cached-data)  ;; Must have previous results
                                (< result-count max-size-limit)
                                (same-structure? (:structure cached-data) new-structure))

              ;; Debug why diff might not happen
              _ (when (and (:debug-logging @diff-config) (not should-diff?))
                  (log/info "[DIFF-SKIP] Not diffing" subscription-id "because:"
                            (cond
                              (not (:enabled @diff-config)) "Diff disabled"
                              (not client-has-data?) "Client hasn't received initial data yet"
                              (not cached-data) "No cached data (first execution)"
                              (not (:results cached-data)) "Cache entry has no results"
                              (>= result-count max-size-limit) (str "Too many rows: " result-count " >= " max-size-limit)
                              (not (same-structure? (:structure cached-data) new-structure))
                              (str "Structure changed. Old fields: " (:fields (:structure cached-data))
                                   " New fields: " (:fields new-structure))
                              :else "Unknown reason")))

              ;; Special handling for temporal queries - they're prime diff candidates
              _ (when is-temporal  ;; Always log temporal queries for debugging
                  (if should-diff?
                    (log/info "[TEMPORAL-DIFF] 🎯 Temporal query WILL be diffed!"
                              "\n  Previous timestamp:" (:temporal-timestamp cached-data)
                              "\n  Current timestamp:" temporal-param
                              "\n  Previous results:" (count (:results cached-data))
                              "\n  Current results:" result-count)
                    (log/info "[TEMPORAL-SKIP] Temporal query NOT diffed. Reason:"
                              (cond
                                (not (:enabled @diff-config)) "Diff disabled"
                                (not client-has-data?) "Client hasn't received initial data yet"
                                (not cached-data) "No cached data (first temporal query)"
                                (not (:results cached-data)) "No cached results"
                                (>= result-count max-size-limit) (str "Too many rows (" result-count " > " max-size-limit ")")
                                (not (same-structure? (:structure cached-data) new-structure)) "Structure changed"
                                :else "Unknown"))))

              ;; Compute diff if applicable - use field-based diffing by default
              _ (when should-diff?
                  (log/info "[DIFF-DECISION] Attempting diff for" subscription-id
                            (when is-temporal " (TEMPORAL)")
                            "\n  Previous result count:" (count (:results cached-data))
                            "\n  Current result count:" result-count
                            "\n  Field-based enabled:" (:field-based? @diff-config true)
                            "\n  Structure same:" (same-structure? (:structure cached-data) new-structure)
                            (when is-temporal (str "\n  Timestamp: " temporal-param))))

              diff-result (when should-diff?
                            (try
                              (let [diff (compute-row-diff (:results cached-data) results
                                                           :field-based? (:field-based? @diff-config true) :query query)]
                                (when (:debug-logging @diff-config)
                                  (log/info "[DIFF-COMPUTED] Result type:" (:type diff)
                                            "\n  Added:" (count (:added diff))
                                            "| Removed:" (count (:removed diff))
                                            "| Updated:" (count (:updated diff))
                                            "\n  Compression ratio:" (:compression-ratio diff)))
                                diff)
                              (catch Exception e
                                (log/error e "[DIFF-ERROR] Failed to compute diff")
                                nil)))

              ;; Decide what to send
              message (if diff-result
                        ;; Send diff
                        (do
                          (log/info "[DIFF-SEND]" (:type diff-result) "for" subscription-id
                                    (when is-temporal " (TEMPORAL)")
                                    "\n  Compression achieved:" (format "%.0f%%" (* (double (:compression-ratio diff-result)) 100))
                                    "\n  Rows - Added:" (count (:added diff-result))
                                    "| Removed:" (count (:removed diff-result))
                                    "| Updated:" (count (:updated diff-result))
                                    (when (= (:type diff-result) :field-diff)
                                      (let [total-fields (reduce + 0 (map #(count (:field-changes %)) (:updated diff-result)))]
                                        (str "\n  Field changes: " total-fields " total"
                                             " (avg " (if (pos? (count (:updated diff-result)))
                                                        (format "%.1f" (/ (double total-fields) (double (count (:updated diff-result)))))
                                                        "0")
                                             " per row)"))))
                          {:subscription-id subscription-id
                           :session-id session-id
                           :query query
                           :type (if (= (:type diff-result) :field-diff)
                                   :field-diff-update
                                   :diff-update)
                           :diff diff-result
                           :server-cache? (:server-cache? result false)
                           :checksum (hash results)
                           :metrics (:metrics result-with-metrics)})
                        ;; Send full results
                        (do
                          (let [reason (cond
                                         (not (:enabled @diff-config)) "DIFF_DISABLED"
                                         (not cached-data) "INITIAL_LOAD"
                                         (>= result-count (:max-result-size @diff-config))
                                         (str "TOO_MANY_ROWS (" result-count " > " (:max-result-size @diff-config) ")")
                                         (not (same-structure? (:structure cached-data) new-structure)) "STRUCTURE_CHANGED"
                                         diff-result (str "DIFF_INEFFICIENT (ratio "
                                                          (format "%.2f" (double (:compression-ratio diff-result)))
                                                          " > " (:min-compression-ratio @diff-config) ")")
                                         :else "DIFF_NOT_BENEFICIAL")]
                            (log/info "[FULL-SEND] Sending FULL update for" subscription-id
                                      (when is-temporal " (TEMPORAL)")
                                      "\n  Reason:" reason
                                      "\n  Rows:" result-count
                                      "\n  Data size:" data-size "bytes"
                                      (when is-temporal (str "\n  Timestamp: " temporal-param))))
                          ;; Mark client as having received base data
                          (swap! client-has-base-data conj client-tracking-key)
                          (when (:debug-logging @diff-config)
                            (log/info "[CLIENT-TRACKING] Marked client as having base data"
                                      "\n  Tracking key:" client-tracking-key))
                          {:subscription-id subscription-id
                           :session-id session-id
                           :query query
                           :type :full-update
                           :result result-with-metrics
                           :server-cache? (:server-cache? result false)
                           :checksum (hash results)}))]
          
          ;; Update cache for next diff
          (when (:debug-logging @diff-config)
            (log/info "[CACHE-UPDATE] Storing" (count results) "results"
                      "\n  Type:" (if is-temporal "TEMPORAL" "REGULAR")
                      "\n  Cache key:" client-cache-key
                      "\n  Structure fields:" (count (:fields new-structure))
                      (when is-temporal (str "\n  Timestamp: " temporal-param))
                      "\n  Cache size before:" (count @client-result-cache)
                      "\n  Existing cache entries:" 
                      (map (fn [[k v]] 
                            (let [;; Check if this cache entry is temporal (has 4 elements with timestamp at end)
                                  entry-is-temporal? (and (vector? k) 
                                                         (= 4 (count k))
                                                         (string? (last k))
                                                         ;; Match various timestamp formats (ISO 8601, with or without time/timezone)
                                                         (re-find #"^\d{4}-\d{2}-\d{2}" (str (last k))))]
                              (str "\n    " (if entry-is-temporal? "[TEMPORAL] " "[REGULAR]  ")
                                   k " -> " (count (:results v)) " results")))
                          (take 5 @client-result-cache))))
          (swap! client-result-cache assoc client-cache-key
                {:results results
                 :structure new-structure
                 :checksum (hash results)
                 :timestamp (System/currentTimeMillis)
                 ;; Store the actual params used
                 :params normalized-params
                 :temporal-timestamp (when is-temporal temporal-param)
                 :query (if is-temporal base-query query)
                 :last-diff-type (cond
                                  (= (:type message) :field-diff-update) :field-diff
                                  (= (:type message) :diff-update) :row-diff
                                  :else :full)})
          (when (:debug-logging @diff-config)
            (log/info "[CACHE-UPDATE] Cache size after:" (count @client-result-cache)))
          
          ;; Track subscription update
          (meta/track-subscription-updated! sub-id execution-time result-count)
          ;; Track query performance
          (meta/track-query-performance! query execution-time result-count)
          
          ;; Send the message
          (log/info "[KAFKA-REACTIVE] Calling callback with message type:" (:type message)
                   "\n  Subscription:" subscription-id
                   "\n  Session:" session-id)
          (callback message))
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
        
        ;; Track the reaction
        (doseq [table affected-tables]
          (meta/track-reaction! table "transaction" affected-subs))
        
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
  ;; Start the debounce executor
  (start-debounce-executor!)
  ;; Start cache cleanup for metrics
  (start-cache-cleanup!)
  (let [consumer-instance (jc/consumer @kafka-config)]
    (reset! consumer consumer-instance)
    
    ;; Subscribe to XTDB log topic
    (jc/subscribe consumer-instance [{:topic-name xtdb-log-topic}])
    (log/info "Subscribed to Kafka topic:" xtdb-log-topic)
    
    ;; Start consumer thread
    (reset! consumer-thread
           (future
             (log/info "[KAFKA-CONSUMER] Starting Kafka consumer thread")
             (while @running?
               (try
                 (let [records (jc/poll consumer-instance 100)]
                  ;;  (when (seq records)
                  ;;    (log/info "[KAFKA-CONSUMER] Received" (count records) "records from Kafka"))
                   (doseq [record records]
                     (try
                       ;; Minimal processing - just check if it's a mutation and extract table names
                       (let [tx-value (:value record)
                             tx-key (:key record)]
                         (when tx-value ;; Process ALL messages for debugging
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
                                                     (->> (remove #(str/ends-with? % "_subscriptions")))
                                                     (->> (remove #(str/ends-with? % "_taps")))
                                                     (->> (remove #(str/ends-with? % "_reactions")))
                                                     set)]
                               
                               ;; Debug logging to see what's happening
                               (when (and (or has-mutation? (seq all-tables)) 
                                          (not= all-tables  #{"reactor_subscriptions"})) ;; no noisy single sub logs
                                 ;; Commented out high-frequency debug logging
                                 #_(log/debug "[KAFKA-DEBUG] Message analysis:"
                                             "\n  Has mutation:" has-mutation?
                                             "\n  Is SELECT:" is-select?
                                             "\n  All tables:" all-tables
                                             "\n  Filtered tables:" filtered-tables
                                             "\n  Message size:" (count tx-value)))
                               
                               (when is-select?
                                 ;; Don't log SELECT skips - too verbose
                                 nil)
                               
                               (when (seq filtered-tables)
                                 (log/info "[KAFKA-MUTATION] Tables affected by mutation:" filtered-tables)
                                 ;; Log detailed subscription info at DEBUG level
                                 ;; Commented out verbose subscription logging
                                 #_(log/debug "[KAFKA-MUTATION] Active subscriptions count:" (count @active-subscriptions))
                                 #_(log/debug "[KAFKA-MUTATION] Table-to-subs for affected tables:")
                                 #_(doseq [table filtered-tables]
                                     (let [subs-for-table (get @table-to-subs (str/lower-case table))]
                                       (log/debug "  Table" table "has" (count subs-for-table) "subscriptions:" subs-for-table)))
                                 
                                 ;; Log subscription details at DEBUG level
                                 (when (pos? (count @active-subscriptions))
                                   ;; Commented out detailed subscription logging
                                   #_(log/debug "[KAFKA-MUTATION] Subscription details:")
                                   #_(doseq [[sub-id sub-info] @active-subscriptions]
                                       (when (some #(contains? (set (:tables sub-info)) %) filtered-tables)
                                         (log/debug "  " sub-id "- temporal:" (:temporal? sub-info) 
                                                   "inert:" (:inert? sub-info)
                                                   "tables:" (:tables sub-info)))))
                                 
                                 ;; Process rules for affected tables
                                 (try
                                   (require '[reactor.sql-rules :as rules])
                                   (when-let [process-fn (resolve 'reactor.sql-rules/process-table-changes)]
                                     (process-fn @session/default-node filtered-tables tx-key))
                                   (catch Exception e
                                     (log/debug "Rules engine not available or failed:" (.getMessage e))))
                                 
                                 ;; Handle SQL subscriptions
                                 (let [affected-subs (find-affected-subscriptions filtered-tables)]
                                   ;; Only log if there are affected subscriptions
                                   (when (seq affected-subs)
                                     (log/debug "[KAFKA-MUTATION] Found" (count affected-subs) "affected subscriptions:"))
                                   (when (seq affected-subs)
                                     (log/info "[KAFKA-MUTATION] Triggering" (count affected-subs) "subscriptions for tables:" filtered-tables)
                                     ;; Log each subscription being triggered at DEBUG level
                                     (doseq [sub-id affected-subs]
                                       (let [sub-info (get @active-subscriptions sub-id)
                                             session-id (:session-id sub-info)
                                             channels (get @sse-channels session-id [])]
                                         ;; Commented out per-subscription trigger logging  
                                         #_(log/debug "[DEBUG-TRIGGER] Requesting re-execution for" sub-id
                                                     "\n  Session:" session-id
                                                     "\n  Has channels:" (boolean (seq channels))
                                                     "\n  Channel count:" (count channels)
                                                     "\n  Tables:" (:tables sub-info))))
                                     ;; Track the reaction
                                     (doseq [table filtered-tables]
                                       (meta/track-reaction! table "mutation" affected-subs))
                                     ;; Use debounced re-execution instead of immediate
                                     (doseq [sub-id affected-subs]
                                       (request-re-execution! sub-id))))
                                 
                                 ;; Handle keypath subscriptions for todo_sessions
                                 (doseq [table filtered-tables]
                                   (trigger-keypath-updates! table)))))) ;; Close all the let blocks and when
                       (catch Exception e
                         (log/error e "Error processing Kafka record")))))
                  (catch Exception e
                    (log/error e "Error polling Kafka")))
               (Thread/sleep 100))
             (log/info "[KAFKA-CONSUMER] Thread exiting")))
    
    (log/info "Kafka consumer started")))

(defn stop-consumer!
  "Stop the Kafka consumer."
  []
  (reset! running? false)
  ;; Stop the debounce executor
  (stop-debounce-executor!)
  
  ;; Cancel the consumer thread first to stop polling
  (when-let [thread @consumer-thread]
    (future-cancel thread)
    ;; Wait a bit for thread to stop
    (Thread/sleep 100)
    (reset! consumer-thread nil))
  
  ;; Now close the consumer safely
  (when-let [c @consumer]
    (try
      ;; Use wakeup to interrupt any ongoing poll
      (.wakeup c)
      (Thread/sleep 100)
      (.close c)
      (catch Exception e
        ;; Log but don't fail - consumer may already be closed
        (when-not (str/includes? (.getMessage e) "KafkaConsumer is not safe")
          (log/error e "Error closing consumer"))))
    (reset! consumer nil))
  
  (log/info "Kafka consumer stopped"))

;; ============================================================================
;; SSE Integration
;; ============================================================================


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
  (log/debug "[KAFKA-REACTIVE] SSE channels map keys:" (keys @sse-channels))
  (if-let [channels (seq (get @sse-channels session-id))]
    (let [;; Force realization of any lazy sequences before JSON serialization
          realized-data (walk/postwalk
                         (fn [x]
                           (cond
                             (instance? clojure.lang.LazySeq x) (doall x)
                             (seq? x) (doall x)
                             :else x))
                         data)
          _ (when (and (:type data) (= (:type data) :field-diff-update))
              (log/info "[DIFF-DEBUG] Sending field-diff structure:"
                       "\n  Type:" (:type realized-data)
                       "\n  Diff keys:" (keys (:diff realized-data))
                       "\n  Updated count:" (count (get-in realized-data [:diff :updated]))
                       "\n  First update:" (when-let [first-update (first (get-in realized-data [:diff :updated]))]
                                          (str "\n    ID:" (:id first-update)
                                               "\n    Field changes keys:" (keys (:field-changes first-update))))))
          message (str "data: " (cheshire.core/generate-string realized-data) "\n\n")]
      (log/info "[KAFKA-REACTIVE] Sending update to" (count channels) "channel(s) for session" session-id)
      (log/info "[KAFKA-REACTIVE] Update type:" (:type data) "subscription-id:" (:subscription-id data))
      (doseq [channel channels]
        (try
          (http-server/send! channel message false)
          (log/info "[KAFKA-REACTIVE] Successfully pushed update to channel for session" session-id)
          (catch Exception e
            (log/error e "[KAFKA-REACTIVE] Error sending to SSE channel - channel might be closed")
            ;; Clean up dead channel
            (unregister-sse-channel! session-id channel)))))
    (do
      (log/warn "[KAFKA-REACTIVE] No SSE channels found for session" session-id "- client may not be connected"
               "\n  Available sessions with channels:" (keys @sse-channels)
               "\n  Total channels:" (reduce + (map count (vals @sse-channels))))
      ;; If this is a block subscription (starts with :), try finding the actual session
      (when (and data (:subscription-id data) (keyword? (:subscription-id data)))
        (log/info "[KAFKA-REACTIVE] Attempting to find alternative session for block subscription" (:subscription-id data))
        ;; Try to find ANY active session that might be interested
        (doseq [[alt-session-id channels] @sse-channels
                :when (seq channels)]
          (log/info "[KAFKA-REACTIVE] Sending block update to alternative session" alt-session-id)
          (push-to-session alt-session-id data))))))

(defn create-subscription-callback
  "Create a callback that pushes query results via SSE."
  [session-id]
  (fn [message]
    ;; Check if this is a new-style message (already formatted) or old-style
    (if (contains? message :type)
      ;; New style - pass through as-is
      (do
        (log/debug "Pushing" (:type message) "for subscription" (:subscription-id message) "to session" session-id)
        (push-to-session session-id message))
      ;; Old style - wrap in legacy format
      (let [{:keys [subscription-id query result]} message]
        (log/debug "Pushing legacy update for subscription" subscription-id "to session" session-id)
        (push-to-session session-id
                        {:type :query-update
                         :subscription-id subscription-id
                         :query query
                         :result result
                         :timestamp (System/currentTimeMillis)})))))

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