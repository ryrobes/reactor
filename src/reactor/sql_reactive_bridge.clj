(ns reactor.sql-reactive-bridge
  "Bridge between SQL operations and reactive query system.
   Monitors SQL executions and triggers reactive updates."
  (:require [reactor.kafka-reactive :as kafka]
            [reactor.xtdb-store :as xts]
            [reactor.session_simple :as session]
            [reactor.meta-tracking :as meta]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

(defn notify-table-change!
  "Notify the reactive system that a table has changed"
  [table-name]
  (log/info "Table changed:" table-name)
  ;; Find all subscriptions watching this table
  (let [affected-subs (kafka/find-affected-subscriptions [table-name])]
    (log/info "Found" (count affected-subs) "affected subscriptions")
    ;; Re-execute each affected subscription
    (doseq [sub-id affected-subs]
      (kafka/re-execute-subscription sub-id))))

(defn extract-table-from-sql
  "Extract the primary table name from a SQL statement"
  [sql]
  (let [sql-upper (str/upper-case sql)
        patterns [#"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)"
                  #"UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)"
                  #"DELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)"]]
    (some (fn [pattern]
           (when-let [match (re-find pattern sql-upper)]
             (str/lower-case (second match))))
         patterns)))

(defn wrap-sql-execution
  "Wrap SQL execution to trigger reactive updates"
  [execute-fn]
  (fn [node sql & params]
    (let [result (apply execute-fn node sql params)]
      ;; If successful, notify about table changes
      (when-not (:error result)
        (when-let [table (extract-table-from-sql sql)]
          ;; Trigger reactive updates asynchronously
          (future
            (Thread/sleep 100) ;; Small delay to ensure transaction commits
            (notify-table-change! table))))
      result)))

;; Override the execute-sql function to add reactive notifications
(defn execute-sql-reactive
  "Execute SQL mutation with reactive notifications - but DON'T trigger on the mutation itself"
  [node sql params]
  ;; Just execute the mutation - Kafka will pick up the change and trigger updates
  (session/execute-sql-mutation node sql params))

;; Enhanced server endpoints that trigger reactive updates
(defn handle-sql-exec-reactive
  "Handle SQL execution with reactive notifications"
  [req]
  (let [body (json/parse-string (slurp (:body req)) true)
        sql-string (:sql body)
        params (:params body)
        session-id (get-in req [:headers "x-session-id"] "default")
        node @session/default-node]
    (log/debug "[SQL-EXEC] Received SQL:" sql-string "params:" params)
    ;; Track the SQL execution event
    (meta/track-event! "sql-exec" "mutation" 
                      {:sql sql-string :params params} 
                      session-id)
    (if node
      (let [result (execute-sql-reactive node sql-string params)]
        (log/debug "[SQL-EXEC] Result:" result)
        {:status 200
         :headers {"Content-Type" "application/json"
                  "Access-Control-Allow-Origin" "*"}
         :body (json/generate-string result)})
      {:status 500
       :headers {"Content-Type" "application/json"
                "Access-Control-Allow-Origin" "*"}
       :body (json/generate-string {:error "No XTDB node available"})})))

;; Manual trigger for testing
(defn trigger-table-update!
  "Manually trigger reactive updates for a table (for testing)"
  [table-name]
  (notify-table-change! table-name))