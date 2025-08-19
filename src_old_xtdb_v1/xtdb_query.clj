(ns reactor.xtdb-query
  "Query layer for XTDB supporting keypaths, SQL, and HoneySQL"
  (:require [xtdb.api :as xt]
            [clojure.string :as str]
            [clojure.walk :as walk]))

;; ===== Schema Management =====

(defn entity->table-name
  "Convert an entity keyword to a SQL-friendly table name
   :session.123/todos -> session_123_todos
   :global/config -> global_config"
  [entity-key]
  (-> (if (keyword? entity-key)
        (str entity-key)
        (str entity-key))
      (str/replace #"^:" "")
      (str/replace #"[./]" "_")
      (str/replace #"-" "_")))

(defn flatten-entity
  "Flatten a nested map into SQL-friendly columns
   {:user {:name 'John' :age 30}} -> {:user_name 'John' :user_age 30}"
  ([m] (flatten-entity m nil))
  ([m prefix]
   (reduce-kv
    (fn [acc k v]
      (let [new-key (if prefix
                      (keyword (str (name prefix) "_" (name k)))
                      k)]
        (if (map? v)
          (merge acc (flatten-entity v new-key))
          (assoc acc new-key v))))
    {}
    m)))

(defn unflatten-entity
  "Unflatten a SQL row back into nested structure
   {:user_name 'John' :user_age 30} -> {:user {:name 'John' :age 30}}"
  [flat-map]
  (reduce-kv
   (fn [acc k v]
     (let [parts (str/split (name k) #"_")]
       (if (> (count parts) 1)
         (assoc-in acc (map keyword parts) v)
         (assoc acc k v))))
   {}
   flat-map))

;; ===== Query Translation =====

(defn keypath->datalog
  "Convert a keypath subscription to a Datalog query
   [:todos :active] -> find active todos"
  [keypath]
  (case (count keypath)
    0 '{:find [(pull ?e [*])]
        :where [[?e :xt/id]]}
    
    1 (let [entity-key (first keypath)]
        {:find '[(pull ?e [*])]
         :where [['?e :xt/id entity-key]]})
    
    2 (let [[entity-key filter-key] keypath]
        (case filter-key
          :active {:find '[(pull ?e [*])]
                   :where [['?e :xt/id entity-key]
                           ['?e :completed false]]}
          :completed {:find '[(pull ?e [*])]
                      :where [['?e :xt/id entity-key]
                              ['?e :completed true]]}
          ;; Default: treat as nested key access
          {:find [`(list 'get '?e filter-key)]
           :where [['?e :xt/id entity-key]]}))
    
    ;; Deeper paths
    {:find [(list 'get-in '?e (rest keypath))]
     :where [['?e :xt/id (first keypath)]]}))

(declare honeysql->where-clause honeysql->order-clause sql-value)

(defn honeysql->xtql
  "Convert HoneySQL map to XTDB SQL query
   {:select [:*] :from [:todos] :where [:= :completed false]}
   -> XTDB SQL string"
  [{:keys [select from where limit offset order-by] :as hsql}]
  (let [table (if (keyword? from) (name from) (name (first from)))
        cols (if (= select [:*]) "*" (str/join ", " (map name select)))
        where-clause (when where
                      (str " WHERE " (honeysql->where-clause where)))
        order-clause (when order-by
                      (str " ORDER BY " (honeysql->order-clause order-by)))
        limit-clause (when limit (str " LIMIT " limit))
        offset-clause (when offset (str " OFFSET " offset))]
    (str "SELECT " cols " FROM " table
         where-clause order-clause limit-clause offset-clause)))

(defn honeysql->where-clause
  "Convert HoneySQL where clause to SQL string"
  [where]
  (cond
    (vector? where)
    (let [[op & args] where]
      (case op
        := (str (name (first args)) " = " (sql-value (second args)))
        :> (str (name (first args)) " > " (sql-value (second args)))
        :< (str (name (first args)) " < " (sql-value (second args)))
        :and (str "(" (str/join " AND " (map honeysql->where-clause args)) ")")
        :or (str "(" (str/join " OR " (map honeysql->where-clause args)) ")")
        (str where)))
    
    :else (str where)))

(defn honeysql->order-clause
  "Convert HoneySQL order-by to SQL string"
  [order-by]
  (str/join ", "
   (map (fn [[col dir]]
          (str (name col) " " (str/upper-case (name dir))))
        (partition 2 order-by))))

(defn sql-value
  "Convert Clojure value to SQL literal"
  [v]
  (cond
    (string? v) (str "'" v "'")
    (keyword? v) (str "'" (name v) "'")
    (nil? v) "NULL"
    :else (str v)))

;; ===== Query Execution =====

(defn execute-query
  "Execute a query against XTDB, supporting multiple formats"
  [node query & {:keys [session-id as-of]}]
  (let [db (if as-of
             (xt/db node as-of)
             (xt/db node))]
    (cond
      ;; Keypath query
      (vector? query)
      (let [datalog (keypath->datalog query)]
        (xt/q db datalog))
      
      ;; HoneySQL query
      (map? query)
      (if (:find query)
        ;; Direct datalog
        (xt/q db query)
        ;; Convert HoneySQL to SQL
        (let [sql (honeysql->xtql query)]
          (xt/q db {:find '[?result]
                    :where [['?result :xt/sql sql]]})))
      
      ;; Raw SQL string
      (string? query)
      (xt/q db {:find '[?result]
                :where [['?result :xt/sql query]]}))))

;; ===== Materialized Views =====

(defn create-materialized-view
  "Create a flattened table from entity data for SQL querying"
  [node entity-pattern table-name]
  (let [db (xt/db node)
        entities (xt/q db {:find '[(pull ?e [*])]
                          :where [['?e :xt/id]
                                  [(re-matches entity-pattern '?e)]]})]
    (doseq [entity entities]
      (let [flattened (flatten-entity entity)
            table-entity (assoc flattened
                               :xt/id (keyword table-name (str (:xt/id entity)))
                               :xt/table table-name
                               :xt/source (:xt/id entity))]
        (xt/submit-tx node [[::xt/put table-entity]])))))

(defn sync-to-table
  "Sync an entity to its flattened table representation"
  [node entity-id]
  (let [entity (xt/entity (xt/db node) entity-id)
        table-name (entity->table-name entity-id)
        flattened (flatten-entity (dissoc entity :xt/id))
        table-entity (assoc flattened
                           :xt/id (keyword table-name (str entity-id))
                           :xt/table table-name
                           :xt/source entity-id)]
    (xt/submit-tx node [[::xt/put table-entity]])))

;; ===== Subscription Management =====

(defprotocol IQuerySubscription
  (query [this])
  (refresh! [this])
  (close! [this]))

(defrecord QuerySubscription [node query result-atom watch-key stop-fn]
  IQuerySubscription
  (query [_] query)
  (refresh! [this]
    (let [new-result (execute-query node query)]
      (reset! result-atom new-result)))
  (close! [_]
    (when stop-fn (stop-fn)))
  
  clojure.lang.IDeref
  (deref [_] @result-atom))

(defn subscribe-query
  "Subscribe to a query with automatic updates"
  [node query & {:keys [poll-ms on-change]}]
  (let [result-atom (atom (execute-query node query))
        sub (->QuerySubscription node query result-atom (gensym "query-sub-") nil)]
    
    (when poll-ms
      ;; Set up polling
      (let [running (atom true)
            poll-fn (fn poll []
                     (when @running
                       (try
                         (let [old-val @result-atom
                               new-val (execute-query node query)]
                           (when (not= old-val new-val)
                             (reset! result-atom new-val)
                             (when on-change
                               (on-change old-val new-val))))
                         (catch Exception e
                           ;; Silently ignore - likely node is closing
                           nil))
                       (future
                         (Thread/sleep poll-ms)
                         (poll))))]
        (future (poll-fn))
        (assoc sub :stop-fn #(reset! running false))))
    
    sub))

;; ===== Query Builder DSL =====

(defn select
  "Build a select query
   (select :todos [:id :text :completed] 
           (where := :completed false)
           (order-by :created-at :desc))"
  [table cols & clauses]
  (reduce
   (fn [query clause]
     (merge query clause))
   {:select cols :from table}
   clauses))

(defn where [& args]
  {:where (vec args)})

(defn order-by [& args]
  {:order-by (vec args)})

(defn limit [n]
  {:limit n})

(defn offset [n]
  {:offset n})

;; ===== Testing Helpers =====

(defn explain-query
  "Explain how a query will be executed"
  [query]
  (cond
    (vector? query)
    {:type :keypath
     :datalog (keypath->datalog query)}
    
    (map? query)
    (if (:find query)
      {:type :datalog
       :query query}
      {:type :honeysql
       :sql (honeysql->xtql query)})
    
    (string? query)
    {:type :sql
     :query query}))