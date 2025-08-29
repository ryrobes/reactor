(ns reactor.sql-template-temporal
  "Enhanced SQL template resolution with temporal clause support"
  (:require [clojure.string :as str]
            [reactor.log :as log]
            [reactor.sql-template :as base-template]))

(defn add-temporal-to-sql
  "Add temporal clause to ALL actual table references in a SQL query"
  [sql timestamp]
  (if (and timestamp
           sql
           (not (re-find #"FOR\s+SYSTEM_TIME\s+AS\s+OF" sql)))
    ;; Add temporal clause after ALL table references, not subqueries
    ;; This regex matches: FROM table_name (but NOT from (subquery))
    (str/replace sql
                 #"(?i)(FROM\s+)([a-zA-Z_][a-zA-Z0-9_\.]*)(?!\s*\()(?=\s|$|\)|,)"
                 (str "$1$2 FOR SYSTEM_TIME AS OF TIMESTAMP '" timestamp "'"))
    sql))

(defn resolve-templates-with-temporal
  "Recursively resolve all template references in SQL with temporal clause support"
  [sql session-state resolved-blocks temporal-timestamp]
  (log/info "[TEMPLATE-TEMPORAL] Resolving templates with temporal support"
           "\n  Has temporal:" (boolean temporal-timestamp)
           "\n  SQL:" (if (> (count sql) 100)
                       (str (subs sql 0 100) "...")
                       sql))
  
  (let [refs (base-template/extract-template-refs sql)]
    (if (empty? refs)
      ;; No more templates to resolve - NOW add temporal to actual tables
      (if temporal-timestamp
        (add-temporal-to-sql sql temporal-timestamp)
        sql)
      ;; Resolve each reference
      (reduce
        (fn [current-sql block-id]
          ;; Check for circular references
          (if (contains? resolved-blocks block-id)
            (do
              (log/warn {:message "Circular reference detected"
                        :block-id block-id
                        :resolved-blocks resolved-blocks})
              (throw (ex-info "Circular block reference detected" 
                             {:block-id block-id
                              :resolved-blocks resolved-blocks})))
            ;; Get the block's SQL
            (let [get-cache-fn (resolve 'reactor.reactive-server/get-block-sql-from-cache)
                  cache-entry (when get-cache-fn (@get-cache-fn block-id))
                  cached-sql (:resolved-sql cache-entry)
                  session-sql (or (get-in session-state [:canvas :blocks (keyword block-id) :sql])
                                 (get-in session-state [:canvas :blocks block-id :sql]))
                  fresh-block-sql (or cached-sql session-sql)]
              
              (if fresh-block-sql
                (let [;; Remove any LIMIT from the referenced SQL
                      clean-sql (str/replace fresh-block-sql #"(?i)\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?" "")
                      ;; Recursively resolve any templates in the referenced SQL
                      resolved-sql (resolve-templates-with-temporal 
                                     clean-sql 
                                     session-state 
                                     (conj resolved-blocks block-id)
                                     temporal-timestamp)
                      ;; For templates, don't add temporal here - it will be added to the inner tables
                      ;; Just wrap as subquery
                      wrapped-sql (str "(" resolved-sql ")")]
                
                (log/info "[TEMPLATE-TEMPORAL] Resolved template"
                         "\n  Block ID:" block-id
                         "\n  Original:" fresh-block-sql
                         "\n  Resolved:" resolved-sql
                         "\n  Wrapped:" wrapped-sql)
                
                ;; Replace the template reference with the resolved SQL
                (str/replace current-sql 
                           (str "{{" block-id ".sql}}")
                           wrapped-sql))
              (do
                (log/warn {:message "Block not found for template reference"
                          :block-id block-id
                          :available-blocks (keys (:blocks (:canvas session-state)))})
                ;; Return SQL unchanged if block not found
                current-sql)))))
        sql
        refs))))

(defn resolve-sql-templates-with-temporal
  "Main entry point for resolving SQL templates with temporal support"
  ([sql session-state]
   (resolve-sql-templates-with-temporal sql session-state nil))
  ([sql session-state temporal-timestamp]
   (try
     (let [result (resolve-templates-with-temporal sql session-state #{} temporal-timestamp)]
       (log/info "[TEMPLATE-TEMPORAL] Resolution complete"
                "\n  Original:" sql
                "\n  Result:" result
                "\n  Temporal:" temporal-timestamp)
       result)
     (catch Exception e
       (log/error e "[TEMPLATE-TEMPORAL] Failed to resolve SQL templates")
       sql))))

(defn resolve-sql-templates-with-deps-and-temporal
  "Resolve SQL templates and track dependencies with temporal support"
  [sql session-state temporal-timestamp]
  (let [refs (base-template/extract-template-refs sql)
        resolved-sql (resolve-sql-templates-with-temporal sql session-state temporal-timestamp)]
    {:sql resolved-sql
     :dependencies refs}))