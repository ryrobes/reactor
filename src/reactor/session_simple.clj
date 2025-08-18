(ns reactor.session_simple
  "Simplified session-scoped reactive state management.
   Each session gets its own isolated state universe with time-travel."
  (:require [reactor.xtdb-store :as xts]
            [xtdb.api :as xt]
            [xtdb.calcite :as calcite]
            [clojure.string :as str])
  (:import [java.sql DriverManager]))

;; Session Management
;; ==================

(defonce sessions (atom {}))
(defonce session-history-index (atom {}))  ;; Track current position in history for each session
(defonce default-node (atom nil))

(defn init!
  "Initialize the session system with XTDB and SQL support"
  []
  (reset! default-node (xts/start-xtdb-node nil 1501)))

(defprotocol ISession
  (get-state [this])
  (set-state! [this value])
  (set-state-no-persist! [this value])
  (get-history [this]))

(defrecord Session [id node state-atom]
  ISession
  (get-state [this]
    @state-atom)
  
  (set-state! [this value]
    ;; Update in-memory state
    (reset! state-atom value)
    ;; Reset history index when new state is added
    (swap! session-history-index assoc id 0)
    ;; Persist to XTDB
    (let [entity-id (keyword "session" id)]
      (xt/submit-tx node [[::xt/put {:xt/id entity-id
                                     :session-id id
                                     :state value
                                     :timestamp (java.util.Date.)}]]))
    value)
  
  (set-state-no-persist! [this value]
    ;; Update in-memory state without persisting (for time travel)
    (reset! state-atom value)
    value)
  
  (get-history [this]
    (let [entity-id (keyword "session" id)
          db (xt/db node)
          history (xt/entity-history db entity-id :desc)]
      (vec (take 50 history))))
  
  clojure.lang.IDeref
  (deref [this]
    (get-state this))
  
  clojure.lang.IAtom
  (reset [this new-value]
    (set-state! this new-value))
  
  (swap [this f]
    (let [old-value @state-atom
          new-value (f old-value)]
      (set-state! this new-value)
      new-value))
  
  (swap [this f arg]
    (let [new-value (f @state-atom arg)]
      (set-state! this new-value)
      new-value))
  
  (swap [this f arg1 arg2]
    (let [new-value (f @state-atom arg1 arg2)]
      (set-state! this new-value)
      new-value))
  
  (swap [this f arg1 arg2 args]
    (let [new-value (apply f @state-atom arg1 arg2 args)]
      (set-state! this new-value)
      new-value))
  
  (compareAndSet [this oldval newval]
    (if (= oldval @state-atom)
      (do (set-state! this newval) true)
      false))
  
  clojure.lang.IRef
  (addWatch [this key f]
    (add-watch state-atom key f))
  
  (removeWatch [this key]
    (remove-watch state-atom key)))

(defn create-session!
  "Create a new session with optional initial state"
  ([session-id]
   (create-session! session-id {}))
  ([session-id initial-state]
   (let [node (or @default-node (xts/start-xtdb-node))
         state-atom (atom initial-state)
         session (->Session session-id node state-atom)]
     (swap! sessions assoc session-id session)
     ;; Persist initial state
     (set-state! session initial-state)
     session)))

(defn get-session
  "Get or create a session"
  [session-id]
  (or (get @sessions session-id)
      (create-session! session-id)))

(defn destroy-session!
  "Clean up a session"
  [session-id]
  (swap! sessions dissoc session-id)
  (swap! session-history-index dissoc session-id))

(defn get-all-sessions
  "Get list of all active sessions"
  []
  (let [;; Get sessions from memory first
        active-sessions (for [[id session] @sessions]
                         {:session-id id
                          :todo-count (count (:todos @session {}))
                          :active true})]
    ;; If we have active sessions, return them
    ;; Otherwise try to get from XTDB
    (if (seq active-sessions)
      (vec active-sessions)
      (when-let [node @default-node]
        (let [db (xt/db node)
              ;; Query for all session entities
              results (xt/q db '{:find [?id ?state]
                                 :where [[?e :session-id ?id]
                                         [?e :state ?state]]})]
          (mapv (fn [[id state]]
                  {:session-id id
                   :todo-count (count (:todos state {}))
                   :active true})
                results))))))

;; Event Handlers
;; ==============

(defonce event-handlers (atom {}))

(defn reg-event-db
  "Register an event handler that receives and returns db"
  [event-id handler]
  (swap! event-handlers assoc event-id handler))

(declare jump-to-history!)

(defn dispatch
  "Dispatch an event to a session"
  [session-id event]
  (println "Dispatch called with session-id:" session-id "event:" event)
  (println "Available handlers:" (keys @event-handlers))
  (let [event-key (first event)]
    (println "Looking for handler with key:" event-key)
    (when-let [handler (get @event-handlers event-key)]
      (println "Found handler for" event-key)
      (when-let [session (get-session session-id)]
        (println "Current state before:" @session)
        ;; Reset history index when new state is created
        (swap! session-history-index assoc session-id 0)
        ;; Now apply the new event
        (let [result (swap! session #(handler % (vec (rest event))))]
          (println "State after:" result)
          result)))))

;; Time Travel
;; ===========

(defn jump-to-history!
  "Jump to a specific point in history"
  [session-id history-index]
  (println "jump-to-history! called with session-id:" session-id "index:" history-index)
  (when-let [session (get-session session-id)]
    (let [entity-id (keyword "session" session-id)
          db (xt/db (:node session))
          history (vec (xt/entity-history db entity-id :desc))]
      (println "History count:" (count history) "Requested index:" history-index)
      (when (and (>= history-index 0) (< history-index (count history)))
        (let [target-entry (nth history history-index)
              target-tx-time (::xt/tx-time target-entry)
              target-db (xt/db (:node session) target-tx-time)
              target-entity (xt/entity target-db entity-id)
              target-state (:state target-entity)]
          (println "Jumping to history index" history-index "with state:" target-state)
          (when target-state
            (swap! session-history-index assoc session-id history-index)
            ;; Don't persist when jumping through history
            (.set-state-no-persist! session target-state)
            target-state))))))

(defn undo!
  "Undo to previous state"
  [session-id]
  (let [current-index (get @session-history-index session-id 0)
        new-index (inc current-index)]
    (println "undo! current-index:" current-index "new-index:" new-index)
    (jump-to-history! session-id new-index)))

(defn redo!
  "Redo to next state"
  [session-id]
  (let [current-index (get @session-history-index session-id 0)]
    (println "redo! current-index:" current-index)
    (when (> current-index 0)
      (jump-to-history! session-id (dec current-index)))))

(defn get-history-info
  "Get information about the history for time travel UI"
  [session-id]
  (when-let [session (get-session session-id)]
    (let [entity-id (keyword "session" session-id)
          db (xt/db (:node session))
          history (vec (xt/entity-history db entity-id :desc))
          current-index (get @session-history-index session-id 0)]
      {:total-states (count history)
       :current-index current-index
       :can-undo (< current-index (dec (count history)))
       :can-redo (> current-index 0)
       :history (mapv (fn [i entry]
                        (let [tx-time (::xt/tx-time entry)
                              hist-db (xt/db (:node session) tx-time)
                              entity (xt/entity hist-db entity-id)]
                          {:index i
                           :tx-time tx-time
                           :state (:state entity)}))
                      (range)
                      history)})))

;; SQL Query Execution
;; ====================

(declare execute-sql-query-fallback)

(defn execute-sql-query
  "Execute a SQL query using XTDB's native SQL support via JDBC."
  [node sql-string & [params as-of]]
  (try
    ;; Use XTDB's Calcite JDBC connection for proper SQL execution
    (with-open [conn (calcite/jdbc-connection node)]
      (let [;; Handle time travel with AS OF SYSTEM TIME
            sql-with-time (if as-of
                           ;; Add AS OF SYSTEM TIME clause to the query
                           (let [timestamp (java.util.Date. (- (System/currentTimeMillis) 
                                                              (* 1000 60 (Integer/parseInt (str as-of)))))
                                 ;; Format as ISO timestamp
                                 iso-time (.format (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") timestamp)]
                             (str sql-string " AS OF SYSTEM TIME \"" iso-time "\""))
                           sql-string)
            stmt (.createStatement conn)
            rs (.executeQuery stmt sql-with-time)
            ;; Get metadata to know column names
            metadata (.getMetaData rs)
            col-count (.getColumnCount metadata)
            col-names (vec (for [i (range 1 (inc col-count))]
                            (keyword (.getColumnLabel metadata i))))
            ;; Collect results
            results (loop [rows []]
                     (if (.next rs)
                       (let [row (into {}
                                      (for [i (range 1 (inc col-count))]
                                        [(nth col-names (dec i))
                                         (.getObject rs i)]))]
                         (recur (conj rows row)))
                       rows))]
        {:results results}))
    
    (catch Exception e
      (println "SQL execution error:" (.getMessage e))
      ;; Fall back to Datalog conversion for basic queries if SQL server not available
      (try
        (execute-sql-query-fallback node sql-string params as-of)
        (catch Exception e2
          {:error (.getMessage e) :results []})))))

(defn execute-sql-query-fallback
  "Fallback SQL to Datalog conversion when SQL server is not available."
  [node sql-string params as-of]
  (let [db (if as-of
            (xt/db node (java.util.Date. (- (System/currentTimeMillis) 
                                           (* 1000 60 (Integer/parseInt (str as-of))))))
            (xt/db node))
        sql-lower (.toLowerCase sql-string)
        ;; Basic SQL to Datalog conversion
        query (cond
               (re-find #"select\s+\*\s+from\s+sales" sql-lower)
               '{:find [(pull ?e [*])]
                 :where [[?e :table "sales"]]}
               
               (re-find #"select\s+\*\s+from\s+inventory" sql-lower)
               '{:find [(pull ?e [*])]
                 :where [[?e :table "inventory"]]}
               
               :else
               '{:find [(pull ?e [*])]
                 :where [[?e :xt/id]]})
        
        raw-results (vec (map first (xt/q db query)))]
    
    ;; Apply ORDER BY if present
    (let [results (if-let [order-match (re-find #"order\s+by\s+(\w+)(?:\s+(desc|asc))?" sql-lower)]
                   (let [order-field (keyword (second order-match))
                         desc? (= "desc" (or (nth order-match 2) "asc"))]
                     (sort-by order-field (if desc? > <) raw-results))
                   raw-results)]
      {:results results})))

;; Example Usage
(comment
  (init!)
  (def s (create-session! "test" {:counter 0}))
  @s
  (swap! s update :counter inc)
  (get-history s))