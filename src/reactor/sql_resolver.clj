(ns reactor.sql-resolver
  "Centralized SQL resolution module to ensure templates are ALWAYS resolved 
   before execution or use as cache keys"
  (:require [reactor.sql-template :as template]
            [reactor.session_simple :as session]
            [reactor.log :as log]
            [clojure.string :as str]))

;; ============================================================================
;; Core Resolution Functions
;; ============================================================================

(defn has-templates?
  "Check if SQL contains template references"
  [sql]
  (boolean (and sql (re-find #"\{\{[^}]+\.sql\}\}" sql))))

(defn resolve-sql
  "Main entry point for SQL resolution. 
   ALWAYS use this before:
   - Executing SQL against database
   - Using SQL as a cache key
   - Storing SQL in any persistent storage
   
   Returns a map with:
   - :resolved-sql - The fully resolved SQL ready for execution
   - :original-sql - The original SQL with templates (for re-resolution)
   - :has-templates? - Whether the SQL had templates
   - :dependencies - List of block IDs this SQL depends on"
  ([sql]
   (resolve-sql sql nil nil))
  ([sql session-id]
   (resolve-sql sql session-id nil))
  ([sql session-id session-state]
   (try
     (let [;; Get session state if not provided
           actual-session-state (or session-state
                                   (when session-id
                                     (when-let [session-obj (session/get-session session-id)]
                                       (session/get-state session-obj)))
                                   {})
           has-templates (has-templates? sql)]
       
       (if has-templates
         ;; SQL has templates - resolve them
         (let [result (template/resolve-sql-templates-with-deps sql actual-session-state)
               resolved-sql (:sql result)
               dependencies (:dependencies result)]
           
           (log/info "[SQL-RESOLVER] Resolved templates:"
                    "\n  Original length:" (count sql)
                    "\n  Resolved length:" (count resolved-sql)
                    "\n  Dependencies:" dependencies
                    "\n  Changed:" (not= sql resolved-sql))
           
           {:resolved-sql resolved-sql
            :original-sql sql
            :has-templates? true
            :dependencies dependencies})
         
         ;; No templates - return as is
         {:resolved-sql sql
          :original-sql sql
          :has-templates? false
          :dependencies []}))
     
     (catch Exception e
       (log/error "[SQL-RESOLVER] Failed to resolve SQL templates:" (.getMessage e)
                 "\n  SQL:" sql)
       ;; On error, return original SQL to avoid breaking queries
       {:resolved-sql sql
        :original-sql sql
        :has-templates? false
        :dependencies []
        :error (.getMessage e)}))))

(defn resolve-for-cache-key
  "Resolve SQL specifically for use as a cache key.
   This ensures cache keys are ALWAYS based on resolved SQL,
   preventing cache misses due to template placeholders."
  [sql session-id]
  (let [result (resolve-sql sql session-id)]
    ;; For cache keys, always use resolved SQL
    (:resolved-sql result)))

(defn resolve-for-execution  
  "Resolve SQL for database execution.
   Returns the resolved SQL ready to execute."
  [sql session-id]
  (let [result (resolve-sql sql session-id)]
    (:resolved-sql result)))

(defn resolve-with-temporal
  "Resolve SQL templates and add temporal clause if needed.
   Used when both template resolution and temporal clauses are required."
  [sql session-id as-of]
  (let [{:keys [resolved-sql]} (resolve-sql sql session-id)]
    (if (and as-of (not (re-find #"FOR\s+SYSTEM_TIME\s+AS\s+OF" resolved-sql)))
      ;; Add temporal clause to resolved SQL
      (let [parser-ns (require 'reactor.sql-parser)
            add-clause-fn (ns-resolve 'reactor.sql-parser 'add-as-of-clause)]
        (if add-clause-fn
          (add-clause-fn resolved-sql as-of)
          resolved-sql))
      resolved-sql)))

;; ============================================================================
;; Subscription-specific Resolution
;; ============================================================================

(defn prepare-subscription-sql
  "Prepare SQL for subscription storage.
   Returns a map with both original (for re-resolution) and resolved (for cache key) SQL."
  [sql session-id]
  (let [result (resolve-sql sql session-id)]
    {:original-sql (:original-sql result)  ; Store original for re-resolution
     :resolved-sql (:resolved-sql result)  ; Use for cache keys and initial execution
     :has-templates? (:has-templates? result)
     :dependencies (:dependencies result)}))

(defn generate-subscription-id
  "Generate a consistent subscription ID based on RESOLVED SQL.
   This ensures subscriptions with same resolved SQL share the same ID."
  [sql session-id client-id]
  (let [resolved (resolve-for-cache-key sql session-id)
        ;; For temporal queries, extract base query for consistent ID
        is-temporal? (re-find #"FOR\s+SYSTEM_TIME\s+AS\s+OF" resolved)
        base-query (if is-temporal?
                    ;; Remove temporal clause for base query
                    (str/replace resolved #"\s+FOR\s+SYSTEM_TIME\s+AS\s+OF\s+TIMESTAMP\s+'[^']+'" "")
                    resolved)]
    (or client-id
        (if is-temporal?
          (str "temporal-" (hash base-query))
          (str "sub-" (hash resolved))))))

;; ============================================================================
;; Cache Key Generation
;; ============================================================================

(defn generate-cache-key
  "Generate a cache key for SQL queries.
   ALWAYS uses resolved SQL to ensure consistent caching."
  ([sql]
   (generate-cache-key sql nil))
  ([sql session-id]
   (let [resolved (resolve-for-cache-key sql session-id)]
     ;; Normalize for consistent cache keys
     (-> resolved
         (str/replace #"\s+" " ")
         (str/trim)
         (str/lower-case)
         (hash)
         (str)))))

(defn generate-temporal-cache-key
  "Generate a cache key for temporal queries.
   Includes both the resolved SQL and the timestamp."
  [sql session-id timestamp]
  (let [resolved (resolve-for-cache-key sql session-id)
        normalized (-> resolved
                      (str/replace #"\s+" " ")
                      (str/trim)
                      (str/lower-case))]
    (str "temporal:" (hash [normalized timestamp]))))

;; ============================================================================
;; Validation and Debugging
;; ============================================================================

(defn validate-no-templates
  "Validate that SQL has no unresolved templates.
   Useful for assertions before execution or caching."
  [sql]
  (when (has-templates? sql)
    (throw (ex-info "SQL contains unresolved templates!"
                   {:sql sql
                    :templates (template/extract-template-refs sql)})))
  sql)

(defn debug-resolution
  "Debug helper to show resolution details"
  [sql session-id]
  (let [result (resolve-sql sql session-id)]
    (println "\n=== SQL Resolution Debug ===")
    (println "Original SQL:" sql)
    (println "Has templates?" (:has-templates? result))
    (println "Dependencies:" (:dependencies result))
    (println "Resolved SQL:" (:resolved-sql result))
    (println "Changed?" (not= sql (:resolved-sql result)))
    (println "Error:" (:error result))
    (println "========================\n")
    result))