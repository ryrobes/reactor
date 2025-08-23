(ns reactor.xtdb-store
  "XTDB 2.0 implementation of the reactive store.
   Connects to remote XTDB server via JDBC/PostgreSQL wire protocol."
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [clojure.string :as str]))

;; Forward declarations
(declare execute-sql)
(declare update-entity)

;; ============================================================================
;; Connection Management
;; ============================================================================

(defn start-xtdb-node
  "Connect to XTDB 2.0 server. Returns a connection spec for JDBC.
   Default connects to localhost. Can specify custom connection:
   (start-xtdb-node {:host \"localhost\" :port 5432 :dbname \"xtdb\"})"
  ([]   
   (start-xtdb-node nil))
  ([{:keys [host port dbname] :as config}]
   (let [host (or host "localhost")
         port (or port 5432)
         dbname (or dbname "xtdb")
         jdbc-url (str "jdbc:xtdb://" host ":" port "/" dbname)]
     {:jdbcUrl jdbc-url
      :type :remote})))

(defn stop-xtdb-node
  "Close XTDB connection (no-op for connection spec)"
  [node]
  ;; Connection specs don't need explicit closing
  ;; Connections are opened/closed per operation
  nil)

;; ============================================================================
;; Core Operations - SQL-based for XTDB 2.0 via JDBC
;; ============================================================================

(defn- get-connection
  "Get a JDBC connection from the node spec"
  [node]
  (jdbc/get-connection (:jdbcUrl node)))

(defn execute-tx
  "Execute a transaction in XTDB 2.0 via JDBC"
  [node tx-ops]
  (with-open [conn (get-connection node)]
    (jdbc/with-transaction [tx conn]
      (doseq [op tx-ops]
        (case (first op)
          :sql (let [[_ sql & params] op]
                 (if (seq params)
                   (jdbc/execute! tx (into [sql] params))
                   (jdbc/execute! tx [sql])))
          ;; Add other operation types as needed
          (throw (ex-info "Unsupported operation" {:op op})))))))

(defn put-entity
  "Store an entity - XTDB automatically creates temporal versions"
  [node table entity-id data]
  (with-open [conn (get-connection node)]
    ;; Try UPDATE first, if no rows affected then INSERT
    (let [columns (keys data)
          set-clause (str/join ", " (map #(str (name %) " = ?") columns))
          update-values (concat (vals data) [entity-id])
          update-sql (str "UPDATE " table " SET " set-clause " WHERE _id = ?")
          result (jdbc/execute! conn (into [update-sql] update-values))
          ;; Handle both possible return types from jdbc/execute!
          rows-affected (cond
                         (number? result) result
                         (map? result) (:next.jdbc/update-count result 0)
                         (sequential? result) 
                           (let [first-item (first result)]
                             (cond
                               (number? first-item) first-item
                               (map? first-item) (:next.jdbc/update-count first-item 0)
                               :else 0))
                         :else 0)]
      ;; If no rows were updated, insert new record
      (when (zero? rows-affected)
        (let [all-columns (cons "_id" (map name columns))
              placeholders (str/join ", " (repeat (count all-columns) "?"))
              all-values (cons entity-id (vals data))
              insert-sql (str "INSERT INTO " table " (" (str/join ", " all-columns) ") "
                             "VALUES (" placeholders ")")]
          (jdbc/execute! conn (into [insert-sql] all-values)))))))

(defn get-entity
  "Retrieve an entity by ID"
  [node table entity-id]
  (with-open [conn (get-connection node)]
    (let [sql (str "SELECT * FROM " table " WHERE _id = ?")
          result (jdbc/execute! conn [sql entity-id]
                               {:builder-fn rs/as-unqualified-lower-maps})]
      (first result))))

(defn delete-entity
  "Delete an entity"
  [node table entity-id]
  (with-open [conn (get-connection node)]
    (jdbc/execute! conn [(str "DELETE FROM " table " WHERE _id = ?") entity-id])))

(defn update-entity
  "Update specific fields of an entity - XTDB automatically creates temporal versions"
  [node table entity-id updates]
  (when (seq updates)
    (let [set-clause (str/join ", "
                       (map #(str (name %) " = ?") (keys updates)))
          values (concat (vals updates) [entity-id])
          sql (str "UPDATE " table " SET " set-clause " WHERE _id = ?")]
      (with-open [conn (get-connection node)]
        (jdbc/execute! conn (into [sql] values))))))

;; ============================================================================
;; Query Operations
;; ============================================================================

(defn query
  "Execute a SQL query"
  [node sql & params]
  (with-open [conn (get-connection node)]
    (if (seq params)
      (jdbc/execute! conn (into [sql] (first params))
                    {:builder-fn rs/as-unqualified-lower-maps})
      (jdbc/execute! conn [sql]
                    {:builder-fn rs/as-unqualified-lower-maps}))))

(defn query-entities
  "Query entities with optional filters"
  [node table & {:keys [where limit order-by]}]
  (let [base-sql (str "SELECT * FROM " table)
        where-clause (when where
                      (str " WHERE " where))
        order-clause (when order-by
                      (str " ORDER BY " order-by))
        limit-clause (when limit
                      (str " LIMIT " limit))
        full-sql (str base-sql where-clause order-clause limit-clause)]
    (query node full-sql)))

;; ============================================================================
;; Time Travel / Temporal Queries
;; ============================================================================

(defn entity-history
  "Get the history of an entity using XTDB 2.0 temporal features"
  [node table entity-id & {:keys [order] :or {order :asc}}]
  (let [sql (str "SELECT *, system_time_start, system_time_end "
                 "FROM " table " "
                 "FOR SYSTEM_TIME ALL "
                 "WHERE _id = ? "
                 "ORDER BY system_time_start " (name order))]
    (query node sql [entity-id])))

(defn query-at-time
  "Query as of a specific time"
  [node table timestamp]
  (let [sql (str "SELECT * FROM " table " "
                 "FOR SYSTEM_TIME AS OF TIMESTAMP ?")]
    (query node sql [timestamp])))

(defn query-between-times
  "Query data valid between two times"
  [node table from-time to-time]
  (let [sql (str "SELECT * FROM " table " "
                 "FOR VALID_TIME FROM TIMESTAMP ? TO TIMESTAMP ?")]
    (query node sql [from-time to-time])))

;; ============================================================================
;; Reactive Atom Implementation
;; ============================================================================

(defprotocol IXTDBAtom
  (get-state [this])
  (set-state! [this value]))

(deftype XTDBAtom [node table entity-id state-atom watches]
  IXTDBAtom
  (get-state [this]
    @state-atom)
  
  (set-state! [this value]
    (let [old-value @state-atom]
      ;; Update in XTDB
      (put-entity node table entity-id value)
      ;; Update local cache
      (reset! state-atom value)
      ;; Notify watches
      (doseq [[key f] @watches]
        (f key this old-value value))
      value))
  
  clojure.lang.IDeref
  (deref [this]
    (get-state this))
  
  clojure.lang.IAtom
  (swap [this f]
    (set-state! this (f @state-atom)))
  
  (swap [this f arg]
    (set-state! this (f @state-atom arg)))
  
  (swap [this f arg1 arg2]
    (set-state! this (f @state-atom arg1 arg2)))
  
  (swap [this f arg1 arg2 args]
    (set-state! this (apply f @state-atom arg1 arg2 args)))
  
  (compareAndSet [this oldval newval]
    (if (= oldval @state-atom)
      (do (set-state! this newval) true)
      false))
  
  (reset [this newval]
    (set-state! this newval))
  
  clojure.lang.IRef
  (addWatch [this key f]
    (swap! watches assoc key f))
  
  (removeWatch [this key]
    (swap! watches dissoc key)))

(defn create-atom
  "Create an XTDB-backed atom that maintains state across restarts"
  [node table entity-id & [initial-value]]
  (let [existing (get-entity node table entity-id)
        state-atom (atom (or existing initial-value {}))
        watches (atom {})]
    (when (and initial-value (not existing))
      (put-entity node table entity-id initial-value))
    (->XTDBAtom node table entity-id state-atom watches)))

;; ============================================================================
;; SQL Support for Application Queries
;; ============================================================================

(defn execute-sql
  "Execute arbitrary SQL - supports both queries and mutations"
  [node sql & params]
  (try
    (let [sql-lower (.toLowerCase sql)
          is-mutation? (or (str/starts-with? sql-lower "insert")
                          (str/starts-with? sql-lower "update")
                          (str/starts-with? sql-lower "delete")
                          (str/starts-with? sql-lower "create")
                          (str/starts-with? sql-lower "drop"))
          ;; Filter out nil params - variadic args make (nil) when called with nil
          actual-params (remove nil? params)]
      (with-open [conn (get-connection node)]
        (if is-mutation?
          ;; Execute as mutation with transaction
          (jdbc/with-transaction [tx conn]
            (jdbc/execute! tx (if (seq actual-params) 
                                  (into [sql] actual-params)
                                  [sql]))
            {:success true})
          ;; Execute as query
          (let [raw-results (jdbc/execute! conn (if (seq actual-params) 
                                                  (into [sql] actual-params)
                                                  [sql])
                                          {:builder-fn rs/as-unqualified-lower-maps})]
            ;; Convert dates to strings for JSON serialization
            {:results (mapv (fn [row]
                             (into {} (map (fn [[k v]]
                                            [k (cond
                                                (instance? java.time.ZonedDateTime v) (str v)
                                                (instance? java.time.Instant v) (str v)
                                                (instance? java.util.Date v) (str v)
                                                :else v)])
                                          row)))
                           raw-results)}))))
    (catch Exception e
      {:error (.getMessage e)})))

;; ============================================================================
;; Table Management
;; ============================================================================

;; Tables in XTDB 2.0 are implicit - no need to create them
;; Just document the expected structure

(defn list-tables
  "List all tables in XTDB 2.0, including system tables"
  [node]
  (try
    (with-open [conn (get-connection node)]
      (let [;; Get public tables
            public-tables (jdbc/execute! conn 
                                        ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"]
                                        {:builder-fn rs/as-unqualified-lower-maps})
            ;; Get xt system tables
            xt-tables (jdbc/execute! conn 
                                    ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'xt'"]
                                    {:builder-fn rs/as-unqualified-lower-maps})]
        {:public (mapv :table_name public-tables)
         :system (mapv #(str "xt." (:table_name %)) xt-tables)}))
    (catch Exception e
      (println "Error listing tables:" (.getMessage e))
      {:public [] :system []})))

(defn table-info
  "Get metadata about a table - columns, row count, etc"
  [node table-name]
  (try
    (with-open [conn (get-connection node)]
      (let [;; Handle schema-qualified names
            [schema table] (if (str/includes? table-name ".")
                             (str/split table-name #"\.")
                             ["public" table-name])
            ;; Get columns
            columns-query (if (= schema "xt")
                           ;; For system tables, just get sample row
                           [(str "SELECT * FROM " schema "." table " LIMIT 1")]
                           ["SELECT column_name, data_type 
                             FROM information_schema.columns 
                             WHERE table_schema = ? AND table_name = ?"
                            schema table])
            columns (if (= schema "xt")
                     ;; For system tables, get columns from sample
                     (let [sample (jdbc/execute! conn columns-query
                                               {:builder-fn rs/as-unqualified-lower-maps})]
                       (when (seq sample)
                         (mapv (fn [col] {:column_name (name col) :data_type "variant"})
                              (keys (first sample)))))
                     (jdbc/execute! conn columns-query
                                  {:builder-fn rs/as-unqualified-lower-maps}))
            ;; Get row count
            count-query [(str "SELECT COUNT(*) as cnt FROM " 
                            (if (= schema "xt") (str schema ".") "")
                            table)]
            row-count (-> (jdbc/execute! conn count-query
                                        {:builder-fn rs/as-unqualified-lower-maps})
                         first
                         :cnt)]
        {:table-name table-name
         :schema schema
         :columns columns
         :row-count row-count}))
    (catch Exception e
      (println "Error getting table info:" (.getMessage e))
      {:table-name table-name :error (.getMessage e)})))

(defn ensure-tables
  "XTDB 2.0 doesn't require explicit table creation - tables are implicit"
  [node]
  ;; Tables are created automatically when you insert data
  ;; This function is kept for API compatibility
  nil)

;; ============================================================================
;; Migration Helpers
;; ============================================================================

(defn migrate-from-v1
  "Helper to migrate data from XTDB 1.x format to 2.0"
  [v1-data table]
  ;; Convert :xt/id to _id
  (-> v1-data
      (dissoc :xt/id)
      (assoc :_id (:xt/id v1-data))))

(defn import-v1-entities
  "Import entities from XTDB 1.x backup"
  [node table entities]
  (doseq [entity entities]
    (let [migrated (migrate-from-v1 entity table)]
      (put-entity node table (:_id migrated) (dissoc migrated :_id)))))