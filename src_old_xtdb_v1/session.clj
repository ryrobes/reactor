(ns reactor.session
  "Session-scoped reactive state management.
   Each session gets its own isolated state universe with time-travel."
  (:require [reactor.xtdb-store :as xts]
            [xtdb.api :as xt]
            [clojure.string :as str]))

;; Session Management
;; ==================
;; Sessions provide isolated state contexts where:
;; - Each session has its own state tree
;; - Time travel only affects that session
;; - Multiple users can work simultaneously
;; - State changes are naturally scoped

(defonce sessions (atom {}))
(defonce session-watchers (atom {}))
(defonce default-node (atom nil))

(defn session-key
  "Create a namespaced key for session data"
  [session-id & path]
  (keyword (str "session." session-id)
           (str/join "." (map name path))))

(defn global-key
  "Create a key for global (non-session) data"
  [& path]
  (keyword "global" (str/join "." (map name path))))

;; Session State Protocol
;; ======================
;; Provides atom-like interface with session scoping

(defprotocol ISessionState
  (get-state [this] [this path]
    "Get the current state or value at path")
  (set-state! [this value] [this path value]
    "Set the state or value at path")
  (update-state! [this f] [this path f]
    "Update state with function")
  (watch-state [this key f]
    "Add a watch function")
  (unwatch-state [this key]
    "Remove a watch function")
  (get-history [this] [this limit]
    "Get state history for time travel")
  (restore-state! [this tx-time]
    "Restore state to a specific point in time"))

;; XTDB-backed Session Implementation
;; ===================================

(defrecord XTDBSession [session-id node]
  ISessionState
  
  (get-state [this]
    (get-state this []))
  
  (get-state [this path]
    (let [db (xt/db node)
          key (apply session-key session-id path)
          entity (xt/entity db key)]
      (or (:value entity)
          (when (empty? path)
            ;; Return full session state tree
            (let [prefix (str "session." session-id)]
              (reduce (fn [acc [k v]]
                        (if (str/starts-with? (namespace k) prefix)
                          (assoc-in acc 
                                    (vec (map keyword 
                                              (str/split (name k) #"\.")))
                                    (:value v))
                          acc))
                      {}
                      (xt/q db '{:find [?e ?v]
                                 :where [[?e :value ?v]]})))))))
  
  (set-state! [this value]
    (set-state! this [] value))
  
  (set-state! [this path value]
    (let [key (apply session-key session-id path)
          tx (xt/submit-tx node [[::xt/put {:xt/id key
                                            :session-id session-id
                                            :path path
                                            :value value
                                            :timestamp (java.util.Date.)}]])]
      (xt/await-tx node tx)
      ;; Notify watchers
      (doseq [[watch-key watch-fn] (get @session-watchers session-id)]
        (watch-fn watch-key this (get-state this path) value))
      value))
  
  (update-state! [this f]
    (update-state! this [] f))
  
  (update-state! [this path f]
    (let [current (get-state this path)
          new-value (f current)]
      (set-state! this path new-value)))
  
  (watch-state [this key f]
    (swap! session-watchers update session-id assoc key f))
  
  (unwatch-state [this key]
    (swap! session-watchers update session-id dissoc key))
  
  (get-history [this]
    (get-history this 50))
  
  (get-history [this limit]
    (let [db (xt/db node)
          history-query {:find '[?e ?path ?value ?tx-time]
                        :where '[[?e :session-id ?sid]
                                [?e :path ?path]
                                [?e :value ?value]
                                [?e :xt/tx-time ?tx-time]]
                        :in '[?sid]
                        :limit limit
                        :order-by '[?tx-time :desc]}]
      (xt/q db history-query session-id)))
  
  (restore-state! [this tx-time]
    (let [db (xt/db node tx-time)
          ;; Get all session entities at that time
          entities (xt/q db '{:find [(pull ?e [*])]
                             :where [[?e :session-id ?sid]]
                             :in [?sid]}
                         session-id)]
      ;; Re-submit them as new transactions
      (doseq [[entity] entities]
        (xt/submit-tx node [[::xt/put entity]]))
      (get-state this)))
  
  clojure.lang.IDeref
  (deref [this]
    (get-state this))
  
  clojure.lang.IAtom
  (reset [this new-value]
    (set-state! this new-value))
  
  (swap [this f]
    (update-state! this f))
  
  (swap [this f arg]
    (update-state! this #(f % arg)))
  
  (swap [this f arg1 arg2]
    (update-state! this #(f % arg1 arg2)))
  
  (swap [this f arg1 arg2 args]
    (update-state! this #(apply f % arg1 arg2 args)))
  
  (compareAndSet [this oldval newval]
    (if (= oldval @this)
      (do (reset! this newval) true)
      false))
  
  clojure.lang.IRef
  (addWatch [this key f]
    (watch-state this key f))
  
  (removeWatch [this key]
    (unwatch-state this key)))

;; Session Management Functions
;; =============================

(defn create-session!
  "Create a new session with optional initial state"
  ([session-id]
   (create-session! session-id {}))
  ([session-id initial-state]
   (let [node (or @default-node (xts/start-xtdb-node))
         session (->XTDBSession session-id node)]
     (swap! sessions assoc session-id session)
     (when (seq initial-state)
       (set-state! session initial-state))
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
  (swap! session-watchers dissoc session-id))

;; Global State Functions
;; ======================
;; For state that should be shared across sessions

(defn get-global
  "Get global state value"
  [& path]
  (let [node (or @default-node (xts/start-xtdb-node))
        db (xt/db node)
        key (apply global-key path)
        entity (xt/entity db key)]
    (:value entity)))

(defn set-global!
  "Set global state value"
  [path value]
  (let [node (or @default-node (xts/start-xtdb-node))
        key (apply global-key path)
        tx (xt/submit-tx node [[::xt/put {:xt/id key
                                          :scope :global
                                          :path path
                                          :value value
                                          :timestamp (java.util.Date.)}]])]
    (xt/await-tx node tx)
    value))

;; Subscription System
;; ===================
;; Reactive subscriptions that work across sessions

(defonce subscriptions (atom {}))

(defn subscribe
  "Subscribe to state changes at path"
  [session-id path callback]
  (let [sub-key (str session-id "/" (str/join "/" path))]
    (swap! subscriptions assoc sub-key callback)
    ;; Return current value immediately
    (when-let [session (get-session session-id)]
      (callback (get-state session path)))
    sub-key))

(defn unsubscribe
  "Remove a subscription"
  [sub-key]
  (swap! subscriptions dissoc sub-key))

;; Event Handlers
;; ==============
;; Re-frame style event handling with session scope

(defonce event-handlers (atom {}))

;; Forward declaration
(declare dispatch)

(defn reg-event-db
  "Register an event handler that receives and returns db"
  [event-id handler]
  (swap! event-handlers assoc event-id handler))

(defn reg-event-fx
  "Register an event handler that can trigger effects"
  [event-id handler]
  (swap! event-handlers assoc event-id
         (fn [session event]
           (let [effects (handler {:db @session} event)]
             (when-let [new-db (:db effects)]
               (reset! session new-db))
             ;; Handle other effects
             (doseq [[effect-key effect-value] (dissoc effects :db)]
               (case effect-key
                 :dispatch (dispatch (:session-id session) effect-value)
                 :dispatch-later (doseq [delayed effect-value]
                                  (future
                                    (Thread/sleep (:ms delayed))
                                    (dispatch (:session-id session) (:dispatch delayed))))
                 :http (println "HTTP effect:" effect-value)
                 (println "Unknown effect:" effect-key)))
             effects))))

(defn dispatch
  "Dispatch an event to a session"
  [session-id event]
  (when-let [handler (get @event-handlers (first event))]
    (when-let [session (get-session session-id)]
      (handler session (rest event)))))

;; Time Travel Functions
;; =====================

(defn undo!
  "Undo to previous state"
  [session-id]
  (when-let [session (get-session session-id)]
    (let [history (get-history session 2)]
      (when (> (count history) 1)
        (let [[_ _ _ prev-time] (second history)]
          (restore-state! session prev-time))))))

(defn redo!
  "Redo to next state (if available)"
  [session-id]
  ;; This would need future state tracking
  (println "Redo not yet implemented for session" session-id))

(defn time-travel!
  "Jump to a specific point in session history"
  [session-id tx-time]
  (when-let [session (get-session session-id)]
    (restore-state! session tx-time)))

;; Convenience Macros
;; ==================

(defmacro with-session
  "Execute body with session bound to *session*"
  [session-id & body]
  `(let [~'*session* (get-session ~session-id)]
     ~@body))

(defmacro defhandler
  "Define an event handler"
  [name args & body]
  `(reg-event-db ~(keyword name)
     (fn ~args ~@body)))

;; Example Usage
;; =============

(comment
  ;; Create a session for a user
  (def my-session (create-session! "user-123" {:todos {} :filter :all}))
  
  ;; Use it like an atom
  @my-session  ; => {:todos {} :filter :all}
  (swap! my-session assoc-in [:todos 1] {:text "Learn XTDB" :done false})
  
  ;; Watch for changes
  (add-watch my-session :logger
    (fn [_ _ old new]
      (println "State changed from" old "to" new)))
  
  ;; Register event handlers
  (reg-event-db :add-todo
    (fn [db [text]]
      (let [id (inc (count (:todos db)))]
        (assoc-in db [:todos id] {:text text :done false}))))
  
  ;; Dispatch events
  (dispatch "user-123" [:add-todo "Build awesome apps"])
  
  ;; Time travel
  (undo! "user-123")
  
  ;; Get history
  (get-history my-session))

;; Default XTDB node initialization

(defn init!
  "Initialize the session system with XTDB"
  []
  (reset! default-node (xts/start-xtdb-node)))