(ns reactor.sql-api
  "SQL API for XTDB 1.x - Query XTDB with SQL via HTTP"
  (:require [reactor.xtdb-store :as xts]
            [xtdb.api :as xt]
            [org.httpkit.server :as http]
            [compojure.core :refer [defroutes GET POST]]
            [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.edn :as edn]))

;; XTDB 1.x SQL Support via Datalog
;; =================================
;; While XTDB 1.x doesn't expose PostgreSQL wire protocol,
;; it does support SQL queries via its query engine.

(defn parse-sql-to-datalog
  "Convert simple SQL to XTDB Datalog
   This is a simplified converter for demonstration"
  [sql]
  (cond
    ;; SELECT * FROM todos
    (re-matches #"(?i)SELECT\s+\*\s+FROM\s+(\w+)" sql)
    (let [[_ table] (re-matches #"(?i)SELECT\s+\*\s+FROM\s+(\w+)" sql)]
      {:find '[(pull ?e [*])]
       :where [['?e :xt/id]
               ['?e :type (keyword table)]]})
    
    ;; SELECT * FROM todos WHERE completed = true
    (re-matches #"(?i)SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*=\s*(.+)" sql)
    (let [[_ table field value] (re-matches #"(?i)SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*=\s*(.+)" sql)]
      {:find '[(pull ?e [*])]
       :where [['?e :type (keyword table)]
               ['?e (keyword field) (read-string value)]]})
    
    :else
    (throw (ex-info "SQL query not supported" {:sql sql}))))

(defn execute-sql
  "Execute SQL query against XTDB"
  [node sql]
  (try
    ;; First try native XTDB SQL support
    (let [db (xt/db node)
          ;; XTDB 1.x has experimental SQL support via q
          result (xt/q db {:find '[?result]
                          :where [['?result :xt/sql sql]]})]
      (if (seq result)
        result
        ;; Fallback to our parser
        (let [datalog (parse-sql-to-datalog sql)]
          (xt/q db datalog))))
    (catch Exception e
      (println "SQL execution error:" (.getMessage e))
      (throw e))))

(defn create-sql-tables
  "Create SQL-friendly views of our data"
  [node]
  ;; Add metadata to make entities queryable as tables
  (let [todos [{:xt/id "todo-1"
                :type :todos
                :text "Learn XTDB SQL"
                :completed false}
               {:xt/id "todo-2"
                :type :todos
                :text "Query with psql"
                :completed false}]]
    (doseq [todo todos]
      (xt/submit-tx node [[::xt/put todo]]))
    (xt/sync node)))

(def sql-node (atom nil))

(defroutes sql-routes
  (POST "/sql" req
    (let [body (slurp (:body req))
          query (json/parse-string body true)
          sql (:query query)
          node @sql-node]
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string
              {:result (execute-sql node sql)})}))
  
  (GET "/tables" []
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body (json/generate-string
            {:tables ["todos" "users" "sessions"]
             :info "Use POST /sql with {\"query\": \"SELECT * FROM todos\"}"})})
  
  (GET "/info" []
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (str "XTDB SQL API\n"
               "============\n"
               "This server provides SQL access to XTDB 1.x\n"
               "\n"
               "Endpoints:\n"
               "  POST /sql - Execute SQL query\n"
               "  GET /tables - List available tables\n"
               "\n"
               "Example:\n"
               "  curl -X POST http://localhost:8080/sql \\\n"
               "    -H 'Content-Type: application/json' \\\n"
               "    -d '{\"query\": \"SELECT * FROM todos\"}'\n"
               "\n"
               "For PostgreSQL wire protocol (psql access):\n"
               "  Consider upgrading to XTDB 2.x or using Presto/Trino\n")}))

(defn start-sql-server
  "Start HTTP SQL API server"
  [port]
  (let [node (xts/start-xtdb-node)]
    (reset! sql-node node)
    (create-sql-tables node)
    (http/run-server sql-routes {:port port})
    (println "SQL API Server started on port" port)
    (println "Try: curl http://localhost:" port "/info")))

(defn -main [& args]
  (let [port (Integer/parseInt (or (first args) "8080"))]
    (start-sql-server port)
    ;; Keep the process running
    (Thread/sleep Long/MAX_VALUE)))

;; PostgreSQL Wire Protocol Alternative
;; ====================================
;; For actual psql connectivity, we have these options:

(defn pg-wire-options []
  (println "
PostgreSQL Wire Protocol Options for XTDB:
==========================================

1. UPGRADE TO XTDB 2.x (Recommended)
   - Native PostgreSQL wire protocol support
   - Direct psql connectivity
   - Full SQL support
   
2. Use Presto/Trino
   - SQL query engine that can connect to XTDB
   - Provides PostgreSQL-compatible interface
   
3. Use Apache Calcite Avatica
   - JDBC server that can expose XTDB data
   - Can be accessed via JDBC/ODBC
   
4. Custom PostgreSQL Protocol Implementation
   - Complex but possible
   - Requires implementing pg wire protocol

For now, we're using HTTP API with SQL support.
"))