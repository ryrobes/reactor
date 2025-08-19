(ns reactor.session_simple
  "Simplified session-scoped reactive state management.
   Each session gets its own isolated state universe with time-travel."
  (:require [reactor.xtdb-store :as xts]
            [xtdb.api :as xt]
            [clojure.string :as str]
            [clojure.walk :as walk]))

;; Session Management
;; ==================

(defonce sessions (atom {}))
(defonce session-history-index (atom {}))  ;; Track current position in history for each session
(defonce default-node (atom nil))

(defn init!
  "Initialize the session system with XTDB 2.0"
  []
  (let [node (xts/start-xtdb-node {:storage-dir "data/xtdb2"})]
    (xts/ensure-tables node)
    (reset! default-node node)))

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
    ;; Persist to XTDB 2.0
    (let [entity-id (str "session-" id)
          ;; Clean the state before persisting - remove ClojureScript internals
          clean-value (walk/prewalk
                       (fn [x]
                         (cond
                           ;; Handle ClojureScript UUID objects
                           (and (map? x) (:uuid x))
                           (str (:uuid x))
                           ;; Remove ClojureScript internal fields
                           (map? x)
                           (dissoc x :__hash :cljs$lang$protocol_mask$partition0$ 
                                  :cljs$lang$protocol_mask$partition1$)
                           :else x))
                       value)]
      (xts/put-entity node "sessions" entity-id 
                     {:session_id id
                      :state (pr-str clean-value)
                      :created_at (str (java.util.Date.))}))
    value)
  
  (set-state-no-persist! [this value]
    ;; Update in-memory state without persisting (for time travel)
    (reset! state-atom value)
    value)
  
  (get-history [this]
    (let [entity-id (str "session-" id)
          history (xts/entity-history node "sessions" entity-id :order :desc)]
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
  "Get or create a session - loads from XTDB if exists"
  [session-id]
  (or (get @sessions session-id)
      ;; Try to load from XTDB before creating new
      (when-let [node @default-node]
        (let [entity-id (str "session-" session-id)
              ;; Get the latest state for this session
              history (xts/entity-history node "sessions" entity-id :order :desc)]
          (when (seq history)
            (let [latest (first history)
                  state (try (read-string (:state latest))
                            (catch Exception _ {}))
                  state-atom (atom state)
                  session (->Session session-id node state-atom)]
              (swap! sessions assoc session-id session)
              session))))
      ;; Only create new if not found in XTDB
      (create-session! session-id)))

(defn destroy-session!
  "Clean up a session"
  [session-id]
  (swap! sessions dissoc session-id)
  (swap! session-history-index dissoc session-id))

(defn get-all-sessions
  "Get list of all sessions from XTDB"
  []
  (when-let [node @default-node]
    (let [;; Query for all unique session IDs using SQL
          results (xts/query node "SELECT DISTINCT session_id FROM sessions")
          ;; For each session, get its latest state
          session-infos (for [row results]
                         (let [session-id (:session_id row)
                               entity-id (str "session-" session-id)
                               history (xts/entity-history node "sessions" entity-id :order :desc)]
                           (when (seq history)
                             (let [latest (first history)
                                   state (try (read-string (:state latest))
                                             (catch Exception _ {}))]
                               {:session-id session-id
                                :todo-count (count (:todos state {}))
                                :active true}))))]
      (vec (remove nil? session-infos)))))

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
    (let [entity-id (str "session-" session-id)
          history (vec (xts/entity-history (:node session) "sessions" entity-id :order :desc))]
      (println "History count:" (count history) "Requested index:" history-index)
      (when (and (>= history-index 0) (< history-index (count history)))
        (let [target-entry (nth history history-index)
              _ (println "Target entry keys:" (keys target-entry))
              _ (println "Raw state string:" (take 100 (str (:state target-entry))))
              target-state (try (read-string (:state target-entry))
                               (catch Exception e 
                                 (println "Error parsing state:" (.getMessage e))
                                 nil))]
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
    (let [entity-id (str "session-" session-id)
          history (vec (xts/entity-history (:node session) "sessions" entity-id :order :desc))
          current-index (get @session-history-index session-id 0)]
      {:total-states (count history)
       :current-index current-index
       :can-undo (< current-index (dec (count history)))
       :can-redo (> current-index 0)
       :history (mapv (fn [i entry]
                        (let [state (try (read-string (:state entry))
                                       (catch Exception _ {}))]
                          {:index i
                           :tx-time (:system_time_start entry)
                           :state state}))
                      (range)
                      history)})))

;; SQL Query Execution
;; ====================

(defn execute-sql-mutation
  "Execute a SQL mutation using XTDB 2.0's native SQL support."
  [node sql-string & [params]]
  (let [result (xts/execute-sql node sql-string params)]
    (if (:error result)
      result
      {:result "SQL executed successfully"})))

(defn execute-sql-query
  "Execute a SQL query using XTDB 2.0's native SQL support."
  [node sql-string & [params as-of]]
  (try
    (let [;; Handle time travel with FOR SYSTEM_TIME AS OF
          sql-with-time (if as-of
                         (let [timestamp (java.util.Date. (- (System/currentTimeMillis) 
                                                            (* 1000 60 (Integer/parseInt (str as-of)))))
                               iso-time (.format (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss") timestamp)]
                           ;; XTDB 2.0 uses FOR SYSTEM_TIME AS OF syntax
                           (str/replace sql-string #"FROM (\w+)" 
                                       (str "FROM $1 FOR SYSTEM_TIME AS OF TIMESTAMP '" iso-time "'")))
                         sql-string)
          result (xts/execute-sql node sql-with-time params)]
      (if (:error result)
        {:error (:error result) :results []}
        {:results (:results result)}))
    (catch Exception e
      (println "SQL execution error:" (.getMessage e))
      {:error (str "SQL Error: " (.getMessage e)) 
       :results []})))

;; Removed fallback - XTDB 2.0 handles all SQL natively

;; Example Usage
(comment
  (init!)
  (def s (create-session! "test" {:counter 0}))
  @s
  (swap! s update :counter inc)
  (get-history s))