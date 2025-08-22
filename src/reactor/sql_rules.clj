(ns reactor.sql-rules
  "SQL-based reactive rules engine for XTDB"
  (:require [reactor.xtdb-store :as xts]
            [reactor.session_simple :as session]
            [reactor.sql-stacks :as stacks]
            [reactor.log :as log]
            [clojure.string :as str]
            [clojure.core.async :as async])
  (:import [java.util UUID]
           [java.time Instant]))

;; ============= Rule Registry =============

(defonce active-rules (atom {}))
(defonce rule-functions (atom {}))
(defonce execution-stats (atom {}))

(defn register-rule-function!
  "Register a Clojure function that can be called by rules"
  [name-str f]
  (swap! rule-functions assoc name-str f))

(defn load-active-rules!
  "Load all active rules from database and index by watched tables"
  [node]
  (try
    (log/info :sql-rules "Loading active rules from database...")
    (let [query-result (xts/execute-sql node 
                         "SELECT * FROM reactor_rules 
                          WHERE enabled = true 
                          ORDER BY priority DESC")
          ;; Handle the :results wrapper from XTDB
          raw-rules (if (and (map? query-result) (:results query-result))
                     (:results query-result)
                     query-result)]
      (log/info :sql-rules (str "Query returned " (count raw-rules) " rules"))
      (when (seq raw-rules)
        (log/info :sql-rules (str "First rule type: " (type (first raw-rules)))))
      
      ;; Parse watch_tables from JSON string - handle both map and vector results
      (let [rules (map (fn [rule]
                        (log/debug :sql-rules (str "Processing rule: " (:rule_id rule)))
                        (let [rule-map (if (map? rule) 
                                        rule 
                                        ;; Convert vector to map if needed
                                        (zipmap [:_id :rule_id :description :watch_tables 
                                                :condition_sql :action_type :action_sql 
                                                :enabled :priority] 
                                               rule))
                              watch-tables-raw (:watch_tables rule-map)]
                          (log/debug :sql-rules (str "Rule map: " rule-map))
                          (log/debug :sql-rules (str "Watch tables raw: " watch-tables-raw))
                          (let [parsed-tables (if (string? watch-tables-raw)
                                               (try 
                                                 (read-string watch-tables-raw)
                                                 (catch Exception e
                                                   (log/error e "Failed to parse watch_tables:" watch-tables-raw)
                                                   []))
                                               (or watch-tables-raw []))]
                            (log/debug :sql-rules (str "Parsed tables: " parsed-tables))
                            (assoc rule-map :watch_tables parsed-tables))))
                      raw-rules)]
        
        (log/info :sql-rules (str "Processed " (count rules) " rules"))
        
        ;; Build the index
        (let [new-active-rules
              (reduce (fn [acc rule]
                       (let [tables (or (:watch_tables rule) [])]
                         (log/debug :sql-rules (str "Indexing rule " (:rule_id rule) " for tables " tables))
                         (reduce (fn [acc2 table]
                                  (log/debug :sql-rules (str "Adding rule " (:rule_id rule) " to table " table))
                                  (update acc2 table (fnil conj []) rule))
                                acc
                                tables)))
                     {}
                     rules)]
          (reset! active-rules new-active-rules)
          (log/info :sql-rules (str "Active rules indexed. Total tables watched: " (count @active-rules)))
          ;(log/info "Active rules state after loading:" @active-rules)
          (when (seq @active-rules)
            (log/info :sql-rules (str "Rules by table: " (into {} (map (fn [[k v]] [k (count v)]) @active-rules))))))))
    (catch Exception e
      (log/error :sql-rules (str "Failed to load rules: " (.getMessage e))))))

;; ============= Context Resolution =============

(defn resolve-params
  "Resolve dynamic parameters from context"
  [param-spec context]
  (mapv (fn [param]
         (cond
           (keyword? param) (get context param)
           (string? param) param
           (map? param) (resolve-params (:value param) context)
           :else param))
       param-spec))

(defn check-rate-limit
  "Check if rule execution is within rate limits"
  [rule-id max-per-minute]
  (if-not max-per-minute
    true
    (let [now (System/currentTimeMillis)
          minute-ago (- now 60000)
          recent-execs (get-in @execution-stats [rule-id :executions] [])
          recent-count (count (filter #(> % minute-ago) recent-execs))]
      (< recent-count max-per-minute))))

;; ============= Rule Evaluation =============

(defn evaluate-condition
  "Evaluate rule condition SQL, returns true if condition met"
  [node rule context]
  (try
    (let [params (resolve-params (:condition_params rule) context)
          sql (:condition_sql rule)
          ;; Apply params correctly as variadic args
          result (if (seq params)
                   (apply xts/execute-sql node sql params)
                   (xts/execute-sql node sql))]
      ;; Check various truthiness patterns
      (cond
        ;; EXISTS query or boolean result
        (and (= 1 (count result))
             (= 1 (count (first result)))
             (boolean? (first (vals (first result)))))
        (first (vals (first result)))
        
        ;; Non-empty result set
        (seq result) true
        
        ;; Empty result
        :else false))
    (catch Exception e
      (log/error :sql-rules (str "Failed to evaluate rule condition " (:rule_id rule) ": " (.getMessage e)))
      false)))

;; ============= Action Execution =============

(defn execute-sql-action
  [node rule context]
  (let [action-sql (:action_sql rule)
        ;; Parse action_sql if it's a string representation of EDN
        parsed-sql (cond
                     (vector? action-sql) action-sql
                     (and (string? action-sql) 
                          (str/starts-with? action-sql "["))
                     (try 
                       (read-string action-sql)
                       (catch Exception _ action-sql))
                     :else action-sql)]
    ;; Check if parsed SQL is a stack (vector) or simple SQL (string)
    (if (vector? parsed-sql)
      ;; Execute as SQL stack with template interpolation
      (let [params (resolve-params (:action_params rule) context)
            stack-context (merge
                            {:triggered_by (:triggered-by context)
                             :transaction_id (:transaction-id context)
                             :affected_tables (:affected-tables context)}
                            (when params 
                              (zipmap (map #(keyword (str "param" %)) (range))
                                      params)))]
        (stacks/execute-stack node parsed-sql stack-context))
      ;; Execute as simple SQL
      (let [params (resolve-params (:action_params rule) context)]
        (if (seq params)
          (apply xts/execute-sql node parsed-sql params)
          (xts/execute-sql node parsed-sql))))))

(defn execute-function-action
  [rule context]
  (if-let [f (get @rule-functions (:action_function rule))]
    (f context (:action_params rule))
    (throw (ex-info "Unknown rule function" 
                    {:function (:action_function rule)
                     :rule-id (:rule_id rule)}))))

(defn execute-event-action
  [rule context]
  ;; Dispatch event through session system
  (when-let [dispatch-fn (:dispatch context)]
    (dispatch-fn (:action_params rule))))

(defn execute-action
  "Execute rule action based on type"
  [node rule context]
  (case (keyword (:action_type rule))
    :sql-execute (execute-sql-action node rule context)
    :sql-insert (execute-sql-action node rule context)
    :function (execute-function-action rule context)
    :event (execute-event-action rule context)
    (log/warn :sql-rules (str "Unknown action type: " (:action_type rule)))))

;; ============= Execution Tracking =============

(defn track-execution!
  "Record rule execution in database"
  [node rule-id triggered-by trigger-source condition-result action-executed 
   action-result execution-time correlation-id]
  (try
    (let [exec-id (str "rule-exec-" (UUID/randomUUID))]
      (xts/execute-sql node
        "INSERT INTO reactor_rule_executions 
         (_id, rule_id, triggered_by, trigger_source, condition_result, 
          action_executed, action_result, execution_time_ms, executed_at, 
          correlation_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        exec-id rule-id triggered-by (pr-str trigger-source) condition-result
        action-executed (pr-str action-result) execution-time 
        (Instant/now) correlation-id))
    (catch Exception e
      (log/error :sql-rules (str "Failed to track rule execution: " (.getMessage e))))))

(defn update-execution-stats!
  "Update in-memory execution statistics"
  [rule-id]
  (let [now (System/currentTimeMillis)]
    (swap! execution-stats update-in [rule-id :executions] 
           (fnil conj []) now)))

(declare extract-affected-tables)

;; ============= Rule Processing =============

(defn process-rule
  "Process a single rule given a change context"
  [node rule context correlation-id]
  (let [rule-id (:rule_id rule)
        start-time (System/currentTimeMillis)]
    (try
      ;; Check rate limiting
      (when-not (check-rate-limit rule-id (:max_executions_per_minute rule))
        (log/info :sql-rules (str "Rule " rule-id " rate limited"))
        (throw (ex-info "Rate limited" {:rule-id rule-id :type :rate-limit})))
      
      ;; Evaluate condition
      (let [condition-met? (evaluate-condition node rule context)]
        (log/info :sql-rules (str "Rule " rule-id " condition: " condition-met?))
        
        (if condition-met?
          ;; Execute action
          (let [action-result (execute-action node rule context)
                execution-time (- (System/currentTimeMillis) start-time)]
            
            ;; Track execution
            (track-execution! node rule-id 
                            (:triggered-by context)
                            (:trigger-source context)
                            true true action-result 
                            execution-time correlation-id)
            
            (update-execution-stats! rule-id)
            
            ;; Return affected tables for cascade detection
            {:rule-id rule-id
             :executed true
             :affected-tables (extract-affected-tables rule context)
             :execution-time execution-time})
          
          ;; Condition not met, just track
          (do
            (track-execution! node rule-id 
                            (:triggered-by context)
                            (:trigger-source context)
                            false false nil
                            (- (System/currentTimeMillis) start-time)
                            correlation-id)
            {:rule-id rule-id
             :executed false})))
      
      (catch Exception e
        (log/error :sql-rules (str "Failed to process rule " rule-id ": " (.getMessage e)))
        {:rule-id rule-id
         :executed false
         :error (.getMessage e)}))))

;; ============= Cascade Detection =============

(defn extract-affected-tables
  "Extract table names from the action SQL"
  [rule context]
  ;; Parse the action SQL to find affected tables
  (when-let [sql (:action_sql rule)]
    (let [sql-upper (clojure.string/upper-case sql)
          ;; Extract tables from INSERT INTO, UPDATE, DELETE FROM
          insert-tables (map #(clojure.string/lower-case (second %))
                            (re-seq #"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)" sql-upper))
          update-tables (map #(clojure.string/lower-case (second %))
                            (re-seq #"UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)" sql-upper))
          delete-tables (map #(clojure.string/lower-case (second %))
                            (re-seq #"DELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)" sql-upper))]
      (distinct (concat insert-tables update-tables delete-tables)))))

(defn find-cascading-rules
  "Find rules that should be triggered by changes from previous rule execution"
  [affected-tables]
  (mapcat #(get @active-rules %) affected-tables))

(defn process-cascade
  "Process cascading rules triggered by initial rule execution"
  [node initial-results context correlation-id depth]
  (when (< depth 10) ; Prevent infinite cascades
    (let [affected-tables (mapcat :affected-tables initial-results)
          cascade-rules (find-cascading-rules affected-tables)]
      (when (seq cascade-rules)
        (log/info :sql-rules (str "Processing " (count cascade-rules) " cascade rules at depth " depth))
        (let [cascade-results 
              (doall (map #(process-rule node % 
                                        (assoc context 
                                               :triggered-by "rule_cascade"
                                               :cascade-depth depth)
                                        correlation-id)
                         cascade-rules))]
          ;; Recursively process next level of cascades
          (process-cascade node cascade-results context correlation-id (inc depth)))))))

;; ============= Main Entry Point =============

(declare record-flow-graph!)

(defn process-table-changes
  "Main entry point called when tables change"
  [node affected-tables transaction-id & [{:keys [dispatch-fn session-id]}]]
  (let [correlation-id (str "corr-" (UUID/randomUUID))
        context {:affected-tables (set affected-tables)
                :transaction-id transaction-id
                :triggered-by "table_change"
                :trigger-source {:tables affected-tables
                                :transaction transaction-id}
                :dispatch-fn dispatch-fn
                :session-id session-id}]
    
    (log/info :sql-rules (str "Processing rules for tables: " affected-tables))
    (log/info :sql-rules (str "Active rules: " (count @active-rules)))
    
    ;; Find and process all rules watching these tables
    (let [triggered-rules (mapcat #(do 
                                    (log/debug :sql-rules (str "Looking for rules watching table: " %))
                                    (let [rules (get @active-rules %)]
                                      (log/debug :sql-rules (str "Found " (count rules) " rules for table " %))
                                      rules))
                                  affected-tables)]
      (when (seq triggered-rules)
        (log/info :sql-rules (str "Found " (count triggered-rules) " rules to evaluate"))
        
        ;; Process initial rules
        (let [initial-results 
              (doall (map #(process-rule node % context correlation-id)
                         triggered-rules))]
          
          ;; Process cascades
          (process-cascade node initial-results context correlation-id 1)
          
          ;; Record flow graph
          (record-flow-graph! node correlation-id initial-results))))))

(defn record-flow-graph!
  "Record the complete flow graph of rule executions"
  [node correlation-id results]
  ;; Simplified - would build actual graph structure
  (try
    (let [flow-id (str "flow-" (UUID/randomUUID))]
      (xts/execute-sql node
        "INSERT INTO reactor_rule_flows 
         (_id, correlation_id, total_rules_executed, created_at)
         VALUES (?, ?, ?, ?)"
        flow-id correlation-id (count results) (Instant/now)))
    (catch Exception e
      (log/error :sql-rules (str "Failed to record flow graph: " (.getMessage e))))))

;; ============= Initialization =============

(defn init-rules-engine!
  "Initialize the rules engine"
  [node]
  (log/info :sql-rules "Initializing SQL rules engine")
  (load-active-rules! node)
  
  ;; Create tables if they don't exist
  (try
    (xts/execute-sql node
      "SELECT * FROM reactor_rules LIMIT 1")
    (catch Exception e
      (log/info :sql-rules "Creating rules tables...")
      ;; In XTDB 2, tables are created implicitly on first insert
      nil)))

;; ============= Rule Management API =============

(defn create-rule!
  [node rule]
  (let [rule-id (:rule_id rule)
        ;; Check if rule already exists
        existing (xts/execute-sql node 
                   "SELECT _id FROM reactor_rules WHERE rule_id = ?"
                   rule-id)]
    (if (seq existing)
      (do
        (log/info :sql-rules (str "Rule already exists: " rule-id " - skipping creation"))
        rule-id)
      (let [_id (str "rule-" (UUID/randomUUID))
            ;; Convert watch_tables to a JSON string for XTDB
            watch-tables-json (pr-str (vec (:watch_tables rule)))]
        (try
          (log/info :sql-rules (str "Creating rule: " rule-id " watching tables: " (:watch_tables rule)))
          (xts/execute-sql node
            "INSERT INTO reactor_rules (_id, rule_id, description, watch_tables, 
             condition_sql, action_type, action_sql, enabled, priority)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            _id rule-id (:description rule) watch-tables-json
            (:condition_sql rule) (:action_type rule) (:action_sql rule)
            (get rule :enabled true) (get rule :priority 0))
          (log/info :sql-rules (str "Rule created successfully: " rule-id))
          (load-active-rules! node)
          rule-id
        (catch Exception e
          (log/error :sql-rules (str "Failed to create rule: " rule-id))
          (throw e)))))))

(defn enable-rule!
  [node rule-id enabled?]
  (xts/execute-sql node
    "UPDATE reactor_rules SET enabled = ? WHERE rule_id = ?"
    enabled? rule-id)
  (load-active-rules! node))

(defn delete-rule!
  [node rule-id]
  (xts/execute-sql node
    "DELETE FROM reactor_rules WHERE rule_id = ?"
    rule-id)
  (load-active-rules! node))