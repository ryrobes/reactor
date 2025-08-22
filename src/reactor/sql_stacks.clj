(ns reactor.sql-stacks
  "SQL stacks - execute sequential SQL statements with template interpolation.
   Allows complex multi-step SQL operations while staying data-driven."
  (:require [reactor.xtdb-store :as xts]
            [reactor.log :as log]
            [clojure.string :as str])
  (:import [java.util UUID]
           [java.time Instant]))

;; ============= Template Variables =============

(defn built-in-vars
  "Generate built-in template variables"
  []
  {:uuid (str (UUID/randomUUID))
   :timestamp (str (Instant/now))
   :timestamp_short (subs (str (Instant/now)) 0 19)
   :random (str (int (rand 1000000)))})

;; ============= Template Resolution =============

(defn extract-template-refs
  "Extract all template references from SQL string.
   Returns set of {:name 'query1' :row 0 :field 'ttl'}"
  [sql]
  (let [pattern #"\{\{([a-zA-Z0-9_]+)(?:\.(\d+))?(?:\.([a-zA-Z0-9_]+))?\}\}"
        matches (re-seq pattern sql)]
    (set (map (fn [[_ name row field]]
                {:name name
                 :row (if row (Integer/parseInt row) 0)
                 :field (keyword (or field :result))})
              matches))))

(defn resolve-template
  "Replace template variables in SQL with actual values.
   Templates can be:
   - {{uuid}} - random UUID
   - {{timestamp}} - current timestamp
   - {{query1.0.ttl}} - field from previous query result
   - {{query1}} - entire first row of result (as string)"
  [sql context]
  (reduce (fn [s [pattern replacement]]
            (str/replace s pattern (str replacement)))
          sql
          ;; Build replacement map
          (merge
            ;; Built-in variables
            (into {} (map (fn [[k v]] [(str "{{" (name k) "}}") v])
                         (built-in-vars)))
            ;; Query result references
            (into {} (for [ref (extract-template-refs sql)]
                      (let [pattern (str "{{" (:name ref)
                                       (when (:row ref) (str "." (:row ref)))
                                       (when (not= (:field ref) :result)
                                         (str "." (name (:field ref)))) "}}")
                            value (get-in context [(:name ref) :results (:row ref) (:field ref)]
                                        (get-in context [(:name ref) :results (:row ref)]))]
                        [pattern value]))))))

;; ============= Stack Execution =============

(defn execute-step
  "Execute a single step in the stack"
  [node step context]
  (let [{:keys [name sql condition]} step
        ;; Check condition if present
        should-execute? (if condition
                          (let [condition-sql (resolve-template condition context)
                                result (xts/execute-sql node condition-sql)]
                            (boolean (seq (:results result))))
                          true)]
    (if should-execute?
      (let [resolved-sql (resolve-template sql context)
            _ (log/info :sql-stacks (str "Executing stack step " name ": " resolved-sql))
            result (xts/execute-sql node resolved-sql)]
        (if (:error result)
          (do
            (log/error :sql-stacks (str "Stack step failed: " name " - " (:error result)))
            (throw (ex-info "SQL stack step failed"
                           {:step name
                            :sql resolved-sql
                            :error (:error result)})))
          (do
            (log/info :sql-stacks (str "Stack step " name " completed successfully"))
            (assoc context name result))))
      (do
        (log/info :sql-stacks (str "Skipping stack step " name " due to condition"))
        context))))

(defn execute-stack
  "Execute a stack of SQL operations in sequence.
   Each step can reference results from previous steps.
   
   Stack format:
   [{:name 'query1' :sql 'SELECT SUM(amount) as total FROM sales'}
    {:name 'alert1' :sql 'INSERT INTO alerts VALUES ({{uuid}}, {{query1.0.total}})'}]
   
   Returns context map with all results."
  [node stack & [initial-context]]
  (try
    (reduce (fn [context step]
              (execute-step node step context))
            (or initial-context {})
            stack)
    (catch Exception e
      (log/error :sql-stacks "SQL stack execution failed" e)
      {:error (.getMessage e)})))

;; ============= Validation =============

(defn validate-stack
  "Validate that stack references are valid"
  [stack]
  (let [step-names (set (map :name stack))]
    (doseq [[idx step] (map-indexed vector stack)]
      ;; Check that referenced steps exist and come before
      (let [refs (extract-template-refs (:sql step))
            ref-names (set (map :name refs))
            ;; Only previous steps are available
            available-names (set (map :name (take idx stack)))]
        (doseq [ref-name ref-names]
          (when-not (or (available-names ref-name)
                       (#{"uuid" "timestamp" "timestamp_short" "random"} ref-name))
            (throw (ex-info "Invalid reference in SQL stack"
                           {:step (:name step)
                            :invalid-ref ref-name
                            :available available-names})))))))
  true)

;; ============= Rule Integration =============

(defn parse-action-sql
  "Parse action SQL - if it's a vector, treat as stack, otherwise simple SQL"
  [action-sql]
  (cond
    (vector? action-sql) {:type :stack :stack action-sql}
    (string? action-sql) {:type :simple :sql action-sql}
    :else (throw (ex-info "Invalid action_sql type" {:type (type action-sql)}))))

(defn execute-rule-action
  "Execute a rule action - either simple SQL or stack"
  [node action-sql context]
  (let [parsed (parse-action-sql action-sql)]
    (case (:type parsed)
      :simple (xts/execute-sql node (:sql parsed))
      :stack (execute-stack node (:stack parsed) context))))