(ns reactor.core
  "Clean re-frame-like API for Reactor - just subscribe and dispatch!"
  (:require 
   [clojure.string :as cstr]
   [reactor.structural-diff :as sdiff]
   [reagent.core :as r]))

;; Configuration
(defonce config (atom {:server-url "http://localhost:4000"
                       :session-id "default"}))

;; Internal state
(defonce app-db (r/atom {}))
(defonce event-source (atom nil))
(defonce connected? (r/atom false))
(defonce sessions (r/atom []))
(defonce history-info (r/atom {}))
(defonce sql-subscriptions (atom {}))  ;; Track active SQL subscriptions
(defonce subscription-id-map (atom {}))  ;; Map client IDs to server IDs

;; ID normalization helper
(defn normalize-id
  "Normalize an ID to a string format"
  [id]
  (cond
    (string? id) id
    (keyword? id) (name id)
    (uuid? id) (str id)
    :else (str id)))

;; SQL Subscription Management  
(defonce sql-event-source (atom nil))  ;; Single SSE connection for all SQL subscriptions
(defonce sql-query-cache (atom {}))    ;; Cache result atoms by query to avoid duplicates

(defn configure! 
  "Set server URL and session ID"
  [{:keys [server-url session-id]}]
  (when server-url (swap! config assoc :server-url server-url))
  (when session-id (swap! config assoc :session-id session-id)))

;; Subscriptions (just like re-frame!)
(defonce subscriptions (atom {}))

;; Forward declaration
(declare create-sql-subscription!)

(defn reg-sub
  "Register a subscription - handler should be a function of [db query-args]"
  [id handler]
  (swap! subscriptions assoc id handler))

(defn reg-sub-path
  "Register a subscription for a simple path lookup"
  [id path]
  (reg-sub id (fn [db _] (get-in db path))))

(declare subscribe)

(defn subscribe-base
  "Subscribe to data - returns a reactive atom
   Supports both keypath subscriptions [:get [:some :path]] and
   SQL subscriptions [:sql \"SELECT * FROM table\"]"
  [query]
  (cond
    ;; SQL subscription
    (= :sql (first query))
    (let [sql (second query)
          params (nth query 2 nil)
          as-of (nth query 3 nil)
          result-atom (r/atom {:loading true})]
      ;; Always create a new subscription - the server will handle deduplication
      (create-sql-subscription! sql params as-of result-atom)
      result-atom)
    
    ;; Regular keypath subscription
    :else
    (let [handler (get @subscriptions (first query))
          args (rest query)
          result-atom (r/atom nil)]
      ;; Set up reactive computation
      (r/track! (fn []
                  (when handler
                    (reset! result-atom (handler @app-db args)))))
      result-atom)))

;; Simple key-path subscription
(reg-sub :db (fn [db _] db))
(reg-sub :get (fn [db [path]] (get-in db path)))

;; Built-in subscriptions for session management
(reg-sub :session-id (fn [_ _] (:session-id @config)))
(reg-sub :sessions (fn [_ _] @sessions))
(reg-sub :history-info (fn [_ _] @history-info))
(reg-sub :connected? (fn [_ _] @connected?))

;; ============================================================================
;; Enhanced Re-frame-like SQL API
;; ============================================================================

;; SQL subscription registry - stores SQL subscription definitions
(defonce sql-sub-registry (atom {}))

(defn reg-sql-sub
  "Register a SQL subscription with optional transform function
   Handler should return a map with :sql, :params, :as-of, and optional :transform
   
   Example:
   (reg-sql-sub :todos
     (fn [[_ session-id]]
       {:sql \"SELECT * FROM todos WHERE session_id = ?\"
        :params [session-id]
        :transform #(or (:todos (first %)) {})}))
   
   The subscription can then be used like:
   (subscribe [:todos \"my-session\"])"
  [id handler]
  (swap! sql-sub-registry assoc id handler))

(defn reg-sql-key
  "Register a SQL-backed key-value subscription
   This is a convenience for the common pattern of storing JSON in a single row
   
   Example:
   (reg-sql-key :todo-state
     {:table \"todo_sessions\"
      :key-field \"_id\"  
      :value-field \"app_state\"
      :default {:todos {} :filter :all}})
   
   Then use like:
   (subscribe [:todo-state session-id])
   (dispatch-sql! [:todo-state session-id new-state])"
  [id {:keys [table key-field value-field default]
       :or {key-field "_id" value-field "app_state"}}]
  (reg-sql-sub id
    (fn [[_ key as-of]]
      {:sql (str "SELECT * FROM " table " WHERE " key-field " = ?")
       :params [key]
       :as-of as-of
       :transform #(or (get (first %) (keyword value-field)) default)})))

;; Forward declaration for sql-exec! (defined later in the file)
(declare sql-exec!)

;; Event registry for SQL-backed events
(defonce sql-event-registry (atom {}))

(defn reg-event-sql
  "Register an event that executes SQL
   Handler receives [params] and returns SQL string or {:sql ... :params ...}
   
   Example:
   (reg-event-sql :add-todo
     (fn [[_ session-id todo]]
       {:sql \"INSERT INTO todos (session_id, id, text, completed) VALUES (?, ?, ?, ?)\"
        :params [session-id (:id todo) (:text todo) (:completed todo)]}))
   
   Then dispatch like:
   (dispatch-sql! [:add-todo \"my-session\" {:id 1 :text \"Buy milk\" :completed false}])"
  [id handler]
  (swap! sql-event-registry assoc id handler))

(defn dispatch-sql!
  "Dispatch a SQL event - executes SQL and triggers subscription updates
   Returns a promise with the result"
  [event-vec]
  (let [[event-id & args] event-vec
        handler (get @sql-event-registry event-id)]
    (if handler
      (let [sql-config (handler args)
            {:keys [sql params]} (if (string? sql-config)
                                   {:sql sql-config :params nil}
                                   sql-config)]
        (sql-exec! sql params))
      (js/console.error "[CLIENT] No SQL event handler registered for:" event-id))))

;; Store reference to base subscribe function
;; This is intentional - we're enhancing the base subscribe function with registry support
(def ^:private subscribe-original subscribe-base)

;; Enhanced subscription function that handles SQL subscriptions via registry
;; WARNING: This intentionally redefines the subscribe function to add registry support
(defn subscribe
  "Enhanced subscribe that supports registered SQL subscriptions
   In addition to existing patterns:
   - [:sql \"SELECT...\"] - Direct SQL subscription
   - [:get [:path]] - Path subscription
   
   Now also supports:
   - [:registered-sql-sub arg1 arg2] - Uses registered SQL subscription"
  [query]
  (let [[sub-type & args] query]
    (cond
      ;; Check if it's a registered SQL subscription
      (contains? @sql-sub-registry sub-type)
      (let [handler (get @sql-sub-registry sub-type)
            config (handler query)
            {:keys [sql params as-of transform]
             :or {transform identity}} config
            ;; Create the SQL subscription
            result-atom (subscribe-original [:sql sql params as-of])]
        ;; If there's a transform, wrap the atom to transform on deref
        (if transform
          (let [transformed-atom (r/atom nil)]
            ;; Set up reactive computation to transform the data
            (r/track! (fn []
                       (let [raw-result @result-atom]
                         (reset! transformed-atom
                                (cond
                                  (:loading raw-result) raw-result
                                  (:error raw-result) raw-result
                                  :else (assoc raw-result :data (transform (:data raw-result))))))))
            transformed-atom)
          result-atom))
      
      ;; Fall back to original subscribe for other patterns
      :else
      (subscribe-original query))))

;; Helper to create SQL-backed key-value store operations
(defn reg-sql-store
  "Register a complete SQL-backed store with get/set operations
   Creates both a subscription and an event for a key-value pattern
   
   Example:
   (reg-sql-store :app-state
     {:table \"app_sessions\"
      :key-field \"session_id\"
      :value-field \"state\"
      :default {}})
   
   Then use like:
   @(subscribe [:app-state \"session-123\"])  ; Get
   (dispatch-sql! [:set-app-state \"session-123\" new-state])  ; Set"
  [id {:keys [table key-field value-field default] :as config}]
  ;; Register the subscription
  (reg-sql-key id config)
  ;; Register the setter event
  (reg-event-sql (keyword (str "set-" (name id)))
    (fn [[key value]]
      {:sql (str "INSERT INTO " table " (" key-field ", " value-field ") "
                 "VALUES (?, ?) "
                 "ON CONFLICT (" key-field ") "
                 "DO UPDATE SET " value-field " = EXCLUDED." value-field)
       :params [key (pr-str value)]})))

;; Forward declarations
(declare get-history-info!)
(declare get-sessions!)

;; Dispatch (async but looks sync!)
(defn dispatch!
  "Dispatch an event - automatically handles server communication"
  [event]
  (js/console.log "[CLIENT] dispatch! called with event:" (clj->js event))
  (-> (js/fetch (str (:server-url @config) "/api/dispatch?session=" (:session-id @config))
                #js {:method "POST"
                     :headers #js {"Content-Type" "application/json"}
                     :body (js/JSON.stringify (clj->js event))})
      (.then (fn [response]
               (if (.-ok response)
                 (.json response)
                 (throw (js/Error. "Dispatch failed")))))
      (.then #(do
                (js/console.log "[CLIENT] dispatch! response received")
                ;; Only update state if not an error response
                (when-not (:error %)
                  (reset! app-db (js->clj % :keywordize-keys true)))
                ;; Update history info and sessions after dispatch
                (get-history-info!)
                ;; Update sessions to reflect new todo count
                (get-sessions!)))
      (.catch #(js/console.error "Dispatch failed:" %))))

(defn dispatch-sync!
  "Dispatch and return a promise with the new state"
  [event]
  (-> (js/fetch (str (:server-url @config) "/api/dispatch")
                #js {:method "POST"
                     :headers #js {"Content-Type" "application/json"}
                     :body (js/JSON.stringify (clj->js event))})
      (.then #(.json %))
      (.then #(let [new-state (js->clj % :keywordize-keys true)]
                (reset! app-db new-state)
                new-state))))

;; Snapshot Management
(defn save-snapshot!
  "Save the current app-db state as a snapshot"
  ([description]
   (save-snapshot! description {}))
  ([description metadata]
   (let [snapshot-id (str "snapshot-" (.getTime (js/Date.)))]
     (js/console.log "[CLIENT] Saving snapshot:" snapshot-id "for session:" (:session-id @config))
     (-> (js/fetch (str (:server-url @config) "/api/snapshot")
                   #js {:method "POST"
                        :headers #js {"Content-Type" "application/json"}
                        :body (js/JSON.stringify 
                               (clj->js {:snapshot_id snapshot-id
                                        :app_name (or js/window.REACTOR_APP_NAME "unknown")
                                        :app_db @app-db
                                        :description description
                                        :session_id (:session-id @config)
                                        :metadata (assoc metadata :session_id (:session-id @config))}))})
         (.then #(.json %))
         (.then (fn [response]
                 (let [result (js->clj response :keywordize-keys true)]
                   (js/console.log "[CLIENT] Snapshot saved:" (:snapshot_id result))
                   ;; Copy snapshot ID to clipboard
                   (when js/navigator.clipboard
                     (.writeText js/navigator.clipboard (:snapshot_id result))
                     (js/console.log "Snapshot ID copied to clipboard!"))
                   result)))
         (.catch #(js/console.error "Failed to save snapshot:" %))))))

(defn load-snapshot!
  "Load a snapshot and replace current app-db"
  [snapshot-id]
  (js/console.log "[CLIENT] Loading snapshot:" snapshot-id)
  (-> (js/fetch (str (:server-url @config) "/api/snapshot/" snapshot-id))
      (.then (fn [response]
              (if (.-ok response)
                (.json response)
                (throw (js/Error. (str "Snapshot not found: " snapshot-id))))))
      (.then (fn [response]
              (let [result (js->clj response :keywordize-keys true)
                    snapshot (:snapshot result)]
                (js/console.log "[CLIENT] Snapshot loaded:" (:snapshot_id snapshot) "for session:" (:session_id snapshot))
                ;; If snapshot has a session_id, switch to that session
                (when (:session_id snapshot)
                  (swap! config assoc :session-id (:session_id snapshot))
                  (js/console.log "[CLIENT] Switched to session:" (:session_id snapshot)))
                ;; Replace app-db with snapshot data
                (reset! app-db (:app_db snapshot))
                snapshot)))
      (.catch #(js/console.error "Failed to load snapshot:" %))))

(defn load-session-at!
  "Load a session state at a specific timestamp and replace current app-db"
  [session-id at-timestamp]
  (js/console.log "[CLIENT] Loading session:" session-id "at timestamp:" at-timestamp)
  ;; URL encode the timestamp to handle special characters
  (let [encoded-timestamp (js/encodeURIComponent at-timestamp)]
    (-> (js/fetch (str (:server-url @config) "/api/session-at/" session-id "/" encoded-timestamp))
        (.then (fn [response]
                (if (.-ok response)
                  (.json response)
                  (throw (js/Error. (str "Session not found at timestamp: " at-timestamp))))))
        (.then (fn [response]
                (let [result (js->clj response :keywordize-keys true)
                      session (:session result)]
                  (js/console.log "[CLIENT] Session loaded:" (:session_id session) "at:" (:timestamp session))
                  ;; Switch to the loaded session
                  (swap! config assoc :session-id (:session_id session))
                  (js/console.log "[CLIENT] Switched to session:" (:session_id session))
                  ;; Replace app-db with historical state
                  (reset! app-db (:app_db session))
                  session)))
        (.catch #(js/console.error "Failed to load session at timestamp:" %)))))

(declare load-session-current!)

;; Track last loaded session to prevent duplicate loads
(defonce last-loaded-session (atom nil))

(defn handle-hash-change!
  "Handle hash parameter changes for client-side state loading without page refresh"
  []
  (when (> (.-length js/window.location.hash) 1)
    (let [hash-str (subs js/window.location.hash 1) ;; Remove the #
          ;; Support both #?param=value and #param=value formats
          clean-hash (if (cstr/starts-with? hash-str "?") 
                       (subs hash-str 1) 
                       hash-str)
          hash-params (js/URLSearchParams. clean-hash)
          session-id (.get hash-params "session_id")
          at-timestamp (.get hash-params "at")]
      (when session-id
        ;; Create a unique key for this session+timestamp combo
        (let [session-key (str session-id "-" (or at-timestamp "current"))]
          ;; Only load if this is different from the last loaded session
          (when (not= @last-loaded-session session-key)
            (js/console.log "[CLIENT] Hash change - loading NEW session:" session-id 
                           (if at-timestamp (str " at " at-timestamp) " (current)")
                           "- Previous was:" @last-loaded-session)
            (reset! last-loaded-session session-key)
            (if at-timestamp
              (load-session-at! session-id at-timestamp)
              (load-session-current! session-id)))
          ;; If it's the same, log that we're skipping
          (when (= @last-loaded-session session-key)
            (js/console.log "[CLIENT] Skipping duplicate hash change for:" session-key)))))))

(defn handle-parent-message!
  "Handle postMessage from parent window for state updates"
  [event]
  (let [data (.-data event)]
    ;; Only log our messages, not React DevTools or other messages
    (when (and data (= (.-type data) "reactor-state-update"))
      (js/console.log "[CLIENT] Got reactor-state-update message from parent")
      (let [hash-str (.-hash data)]
        (js/console.log "[CLIENT] Hash from parent:" hash-str)
        ;; Parse the hash parameters directly
        (when (and hash-str (> (.-length hash-str) 1))
          (let [clean-hash (if (cstr/starts-with? (subs hash-str 1) "?")
                             (subs hash-str 2)  ;; Remove #?
                             (subs hash-str 1)) ;; Remove #
                hash-params (js/URLSearchParams. clean-hash)
                session-id (.get hash-params "session_id")
                at-timestamp (.get hash-params "at")]
            ;; Create a unique key for this session+timestamp combo
            (let [session-key (str session-id "-" (or at-timestamp "current"))]
              ;; Only load if this is different from the last loaded session
              (when (and session-id (not= @last-loaded-session session-key))
                (js/console.log "[CLIENT] Loading NEW session from message:" session-id 
                               (if at-timestamp (str " at " at-timestamp) " (current)")
                               "- Previous was:" @last-loaded-session)
                (reset! last-loaded-session session-key)
                ;; Load the session state directly
                (if at-timestamp
                  (load-session-at! session-id at-timestamp)
                  (load-session-current! session-id))
                ;; Update the URL to reflect the change (without triggering navigation)
                (js/window.history.replaceState nil nil hash-str))
              ;; If it's the same session, just log
              (when (= @last-loaded-session session-key)
                (js/console.log "[CLIENT] Ignoring duplicate session load:" session-key)))))))))

(defn load-session-current!
  "Load the current state of a specific session and replace current app-db"
  [session-id]
  (js/console.log "[CLIENT] Loading current state for session:" session-id)
  (-> (js/fetch (str (:server-url @config) "/api/session-current/" session-id))
      (.then (fn [response]
              (if (.-ok response)
                (.json response)
                (throw (js/Error. (str "Session not found: " session-id))))))
      (.then (fn [response]
              (let [result (js->clj response :keywordize-keys true)
                    session (:session result)]
                (js/console.log "[CLIENT] Session loaded:" (:session_id session))
                ;; Switch to the loaded session
                (swap! config assoc :session-id (:session_id session))
                (js/console.log "[CLIENT] Switched to session:" (:session_id session))
                ;; Replace app-db with current state
                (reset! app-db (:app_db session))
                session)))
      (.catch #(js/console.error "Failed to load session:" %))))

(defn check-snapshot-param!
  "Check URL params for snapshot parameter and load if present"
  []
  (let [params (js/URLSearchParams. js/window.location.search)
        snapshot-id (.get params "snapshot")]
    (when snapshot-id
      (js/console.log "[CLIENT] Snapshot parameter detected:" snapshot-id)
      (load-snapshot! snapshot-id))))

;; Keyboard shortcuts for snapshot management
(defonce snapshot-handlers-installed? (atom false))

(defn install-snapshot-handlers!
  "Install keyboard shortcuts for snapshot management"
  []
  (when-not @snapshot-handlers-installed?
    (reset! snapshot-handlers-installed? true)
    (.addEventListener js/document "keydown"
      (fn [e]
        ;; Ctrl+Shift+S to save snapshot
        (when (and (.-ctrlKey e) (.-shiftKey e) (= (.-key e) "S"))
          (.preventDefault e)
          (let [description (js/prompt "Snapshot description:" 
                                      (str "Snapshot at " (.toLocaleTimeString (js/Date.))))]
            (when description
              (save-snapshot! description))))
        ;; Ctrl+Shift+L to load snapshot (prompts for ID)
        (when (and (.-ctrlKey e) (.-shiftKey e) (= (.-key e) "L"))
          (.preventDefault e)
          (let [snapshot-id (js/prompt "Enter snapshot ID to load:")]
            (when snapshot-id
              (load-snapshot! snapshot-id))))))))

;; History/Time Travel Info
(defn get-history-info!
  "Get information about the current session's history"
  []
  (js/console.log "[CLIENT] get-history-info! called")
  (-> (js/fetch (str (:server-url @config) "/api/history-info?" 
                     "session=" (:session-id @config)))
      (.then #(.json %))
      (.then #(do
                ;(js/console.log "[CLIENT] history-info received:" (clj->js %))
                (reset! history-info (js->clj % :keywordize-keys true))))))

;; Time travel API
(defn undo! []
  (js/console.log "[CLIENT] undo! called")
  (-> (js/fetch (str (:server-url @config) "/api/undo?session=" (:session-id @config))
                #js {:method "POST"})
      (.then #(.json %))
      (.then #(do
                (js/console.log "[CLIENT] undo! response received, updating app-db")
                (reset! app-db (js->clj % :keywordize-keys true))
                ;; Update history info to reflect new position
                (get-history-info!)))))

(defn redo! []
  (js/console.log "[CLIENT] redo! called")
  (-> (js/fetch (str (:server-url @config) "/api/redo?session=" (:session-id @config))
                #js {:method "POST"})
      (.then #(.json %))
      (.then #(do
                (js/console.log "[CLIENT] redo! response received, updating app-db")
                (reset! app-db (js->clj % :keywordize-keys true))
                ;; Update history info to reflect new position
                (get-history-info!)))))

;; SQL Queries (!!)
(defn q
  "Run a SQL/Datalog query on the server"
  [query]
  (-> (js/fetch (str (:server-url @config) "/api/query")
                #js {:method "POST"
                     :headers #js {"Content-Type" "application/json"}
                     :body (js/JSON.stringify (clj->js {:query query}))})
      (.then #(.json %))
      (.then #(js->clj % :keywordize-keys true))))

(defn sql-query!
  "Execute a SQL query and create a reactive subscription"
  ([sql]
   (sql-query! sql nil nil))
  ([sql params]
   (sql-query! sql params nil))
  ([sql params as-of]
   ;; Create a subscription and return initial results as a promise
   (let [result-atom (subscribe [:sql sql params as-of])]
     ;; Return a promise that resolves with the initial value
     (js/Promise. 
      (fn [resolve reject]
        ;; Wait for the first non-loading result
        (let [watch-key (gensym)]
          (add-watch result-atom watch-key
                     (fn [_ _ _ new-val]
                       (when-not (:loading new-val)
                         (remove-watch result-atom watch-key)
                         (if (:error new-val)
                           (resolve {:error (:error new-val)})
                           (resolve {:results (:data new-val)})))))))))))

(defn sql-subscribe!
  "Create a reactive SQL subscription that updates automatically"
  ([sql]
   (sql-subscribe! sql nil nil))
  ([sql params]
   (sql-subscribe! sql params nil))
  ([sql params as-of]
   ;; Create a subscription and return a reactive atom
   (subscribe [:sql sql params as-of])))

(defn sql-subscribe-with-id!
  "Create a reactive SQL subscription with a specific ID"
  ([sub-id sql]
   (sql-subscribe-with-id! sub-id sql nil nil))
  ([sub-id sql params]
   (sql-subscribe-with-id! sub-id sql params nil))
  ([sub-id sql params as-of]
   ;; Create subscription with explicit ID
   (let [result-atom (r/atom {:loading true})]
     (create-sql-subscription! sql params as-of result-atom sub-id)
     result-atom)))

(defn sql-exec!
  "Execute a SQL statement (INSERT/UPDATE/DELETE) on the server"
  ([sql]
   (sql-exec! sql nil))
  ([sql params]
   (-> (js/fetch (str (:server-url @config) "/api/sql-exec?session=" (:session-id @config))
                 #js {:method "POST"
                      :headers #js {"Content-Type" "application/json"}
                      :body (js/JSON.stringify (clj->js {:sql sql
                                                         :params params}))})
       (.then #(.json %))
       (.then #(js->clj % :keywordize-keys true)))))

;; Forward declarations for functions defined later
(declare init!)
(declare disconnect!)

(defn get-sessions!
  "Fetch all available sessions"
  []
  (js/console.log "[CLIENT] get-sessions! called")
  (-> (js/fetch (str (:server-url @config) "/api/sessions"))
      (.then #(.json %))
      (.then #(do
                ;(js/console.log "[CLIENT] Sessions received:" (clj->js %))
                (reset! sessions (js->clj % :keywordize-keys true))))
      (.catch #(js/console.error "[CLIENT] Failed to get sessions:" %))))

(defn switch-session!
  "Switch to a different session"
  [session-id]
  (js/console.log "[CLIENT] Switching to session:" session-id)
  (disconnect!)
  (configure! {:session-id session-id})
  (init!)
  ;; Update sessions list and history info for the new session
  (get-sessions!)
  (get-history-info!))

(defn create-session!
  "Create a new session with optional initial state"
  ([session-id]
   (create-session! session-id {}))
  ([session-id initial-state]
   (js/console.log "[CLIENT] Creating session:" session-id)
   (-> (js/fetch (str (:server-url @config) "/api/create-session")
                 #js {:method "POST"
                      :headers #js {"Content-Type" "application/json"}
                      :body (js/JSON.stringify (clj->js {:session-id session-id
                                                          :initial-state initial-state}))})
       (.then #(.json %))
       (.then #(do
                 (js/console.log "[CLIENT] Session created, switching to it")
                 (switch-session! session-id))))))

(defn jump-to-history!
  "Jump to a specific point in history"
  [index]
  (js/console.log "[CLIENT] jump-to-history! called with index:" index)
  (-> (js/fetch (str (:server-url @config) "/api/jump-to-history?session=" (:session-id @config))
                #js {:method "POST"
                     :headers #js {"Content-Type" "application/json"}
                     :body (js/JSON.stringify (clj->js {:index index}))})
      (.then #(.json %))
      (.then #(do
                (js/console.log "[CLIENT] jump-to-history! response received, state:" (clj->js %))
                (reset! app-db (js->clj % :keywordize-keys true))
                ;; Update history info to reflect new position
                (get-history-info!)))))

;; Initialize and connect
(defn init! 
  "Initialize Reactor - sets up SSE and fetches initial state"
  ([]
   (init! {}))
  ([opts]
   (when opts (configure! opts))
   
   ;; Install snapshot keyboard handlers
   (install-snapshot-handlers!)
   
   ;; Check for URL parameters - priority: snapshot > session+at > session > normal
   ;; Also support hash parameters for client-side state loading
   (let [params (js/URLSearchParams. js/window.location.search)
         ;; Parse hash parameters if present
         hash-str (when (> (.-length js/window.location.hash) 1)
                   (subs js/window.location.hash 1))
         clean-hash (when hash-str
                     (if (cstr/starts-with? hash-str "?") 
                       (subs hash-str 1) 
                       hash-str))
         hash-params (when clean-hash 
                      (js/URLSearchParams. clean-hash))
         ;; URL params take priority over hash params
         snapshot-id (.get params "snapshot")
         session-id-param (or (.get params "session_id")
                            (when hash-params (.get hash-params "session_id")))
         at-timestamp (or (.get params "at")
                        (when hash-params (.get hash-params "at")))
         ;; Determine loading mode
         loading-mode (cond
                       snapshot-id :snapshot
                       (and session-id-param at-timestamp) :session-at
                       session-id-param :session-current  ;; NEW: session_id without at
                       :else :normal)
         ;; Track if we should skip the first SSE message when loading historical/different state
         skip-first-sse? (atom (not= loading-mode :normal))]
     
     ;; Set up hash change listener for dynamic state loading
     (.addEventListener js/window "hashchange" handle-hash-change!)
     
     ;; Set up postMessage listener for iframe communication
     (.addEventListener js/window "message" handle-parent-message!)
     
     ;; Load initial state based on mode
     (case loading-mode
       :snapshot
       ;; Load from snapshot
       (do
         (js/console.log "[CLIENT] Loading from snapshot:" snapshot-id)
         (-> (load-snapshot! snapshot-id)
             (.catch (fn [err]
                      (js/console.error "Failed to load snapshot, falling back to normal init:" err)
                      ;; Fall back to normal state loading
                      (-> (js/fetch (str (:server-url @config) "/api/state?session=" (:session-id @config)))
                          (.then #(.json %))
                          (.then #(reset! app-db (js->clj % :keywordize-keys true))))))))
       
       :session-at
       ;; Load session at specific timestamp
       (do
         (js/console.log "[CLIENT] Loading session:" session-id-param "at timestamp:" at-timestamp)
         (-> (load-session-at! session-id-param at-timestamp)
             (.catch (fn [err]
                      (js/console.error "Failed to load session at timestamp, falling back to normal init:" err)
                      ;; Fall back to normal state loading
                      (-> (js/fetch (str (:server-url @config) "/api/state?session=" (:session-id @config)))
                          (.then #(.json %))
                          (.then #(reset! app-db (js->clj % :keywordize-keys true))))))))
       
       :session-current
       ;; Load current state of a specific session
       (do
         (js/console.log "[CLIENT] Loading current state for session:" session-id-param)
         (-> (load-session-current! session-id-param)
             (.catch (fn [err]
                      (js/console.error "Failed to load session, falling back to normal init:" err)
                      ;; Fall back to normal state loading
                      (-> (js/fetch (str (:server-url @config) "/api/state?session=" (:session-id @config)))
                          (.then #(.json %))
                          (.then #(reset! app-db (js->clj % :keywordize-keys true))))))))
       
       :normal
       ;; Get initial state normally
       (-> (js/fetch (str (:server-url @config) "/api/state?session=" (:session-id @config)))
           (.then #(.json %))
           (.then #(reset! app-db (js->clj % :keywordize-keys true)))))
     
     ;; Set up SSE for real-time updates
     (when @event-source
       (.close @event-source))
     
     (let [es (js/EventSource. (str (:server-url @config) "/api/subscribe?session=" (:session-id @config)))]
       (set! (.-onmessage es)
             (fn [e]
               (let [data (js->clj (js/JSON.parse (.-data e)) :keywordize-keys true)]
                 ;; Skip first message if we loaded from historical state
                 (if @skip-first-sse?
                   (do
                     (js/console.log "[CLIENT] Skipping first SSE message (loaded from historical state)")
                     (reset! skip-first-sse? false))
                   ;; Process normally
                   (if (:partial-update data)
                     ;; Partial update - merge into existing state
                     (do
                       (js/console.log "[CLIENT] SSE partial update received for path:" (clj->js (:path data)))
                       (if-let [path (:path data)]
                         (swap! app-db assoc-in path (:value data))
                         (js/console.warn "[CLIENT] Partial update missing path")))
                     ;; Full state update (e.g., time travel, undo/redo)
                     (do
                       (js/console.log "[CLIENT] SSE full state received")
                       (reset! app-db data))))
                 (reset! connected? true))))
       (set! (.-onerror es)
             (fn [e]
               (reset! connected? false)
               (js/console.error "Connection lost")))
       (reset! event-source es)))
   
   ;; Return the app-db for convenience
   app-db))



(defn apply-row-diff!
  "Apply a row diff to the current result atom data"
  [result-atom diff new-checksum metrics]
  (try
    (swap! result-atom
           (fn [{:keys [data] :as current}]
             (let [;; Get ID key from diff
                   id-key (:id-key diff)
                   ;; Helper to safely get ID from a row  
                   get-id (fn [row]
                           ;; Try multiple ways to get the ID
                           (cond
                             ;; If id-key is a keyword, use it directly
                             (keyword? id-key) (get row id-key)
                             ;; If id-key is a string, try both string and keyword versions
                             (string? id-key) (or (get row id-key)
                                                 (get row (keyword id-key)))
                             ;; Fallback
                             :else (get row id-key)))
                   ;; Index current data by ID
                   _ (js/console.log "[CLIENT-DIFF] Starting diff application"
                                    "\n  id-key:" (clj->js id-key)
                                    "\n  current data:" (if data (str "count=" (count data)) "NIL/EMPTY")
                                    "\n  diff type:" (:type diff)
                                    "\n  diff added:" (count (:added diff))
                                    "\n  diff removed:" (count (:removed diff))
                                    "\n  diff updated:" (count (:updated diff)))
                   _ (when (and (seq (:updated diff)) (empty? data))
                       (js/console.error "[CLIENT-DIFF] ERROR: Have updates but no base data!"
                                        "\n  Updates:" (clj->js (take 1 (:updated diff)))))
                   indexed (if (and id-key data)
                            (reduce (fn [acc row]
                                     (if-let [id (get-id row)]
                                       (assoc acc id row)
                                       (do (js/console.warn "[CLIENT-DIFF] Row missing ID:" (clj->js row))
                                           acc)))
                                   {}
                                   data)
                            {})
                   ;; Apply removals
                   after-remove (apply dissoc indexed (:removed diff))
                   ;; Apply additions  
                   after-add (reduce (fn [acc row]
                                      (if-let [id (get-id row)]
                                        (assoc acc id row)
                                        acc))
                                    after-remove
                                    (:added diff))
                   ;; Apply updates - handle both field-based and row-based updates
                   _ (js/console.log "[CLIENT-DIFF] Processing updates:"
                                    "\n  Update count:" (count (:updated diff))
                                    "\n  After-add size:" (count after-add))
                   after-update (reduce (fn [acc update-entry]
                                         (let [id (:id update-entry)
                                               existing-row (get acc id)]
                                           (js/console.log "[CLIENT-DIFF] Updating row ID:" (clj->js id)
                                                          "\n  Existing row found?" (boolean existing-row)
                                                          "\n  Has field-changes?" (boolean (:field-changes update-entry)))
                                           (cond
                                             ;; Field-based update with structural support
                                             (:field-changes update-entry)
                                             (if existing-row
                                               (let [updated-row (sdiff/apply-enhanced-field-changes 
                                                                 existing-row 
                                                                 (:field-changes update-entry))]
                                                 (js/console.log "[CLIENT-DIFF] Applied field changes, result:" 
                                                                (take 3 (keys updated-row)))
                                                 (assoc acc id updated-row))
                                               (do (js/console.warn "[CLIENT-DIFF] No existing row for ID:" (clj->js id))
                                                   acc))
                                             
                                             ;; Row-based update (legacy)
                                             (:new-values update-entry)
                                             (assoc acc id (:new-values update-entry))
                                             
                                             :else acc)))
                                       after-add
                                       (:updated diff))
                   ;; Apply order if provided, otherwise use values
                   _ (js/console.log "[CLIENT-DIFF] Building final data:"
                                    "\n  after-update size:" (count after-update)
                                    "\n  Has order?" (boolean (:order diff)))
                   final-data (if-let [order (:order diff)]
                               (do (js/console.log "[CLIENT-DIFF] Applying order, length:" (count order))
                                   (mapv #(get after-update %) order))
                               (vals after-update))
                   ;; If diff has no changes, preserve existing data
                   final-data (if (and (empty? (:added diff))
                                      (empty? (:removed diff))
                                      (empty? (:updated diff)))
                               (do (js/console.log "[CLIENT-DIFF] No changes, keeping existing data")
                                   data)  ;; Keep existing data unchanged
                               final-data)]
               (js/console.log "[CLIENT]" (:type diff) "applied - added:" (count (:added diff))
                              "removed:" (count (:removed diff)) 
                              "updated:" (count (:updated diff))
                              "final count:" (count final-data))
               (assoc current 
                     :data final-data
                     :loading false
                     :checksum new-checksum
                     :metrics metrics))))
    ;; Store the new checksum for validation
    (swap! sql-subscriptions 
           (fn [subs]
             (reduce-kv (fn [acc sub-id sub]
                         (if (= (:result-atom sub) result-atom)
                           (assoc-in acc [sub-id :last-checksum] new-checksum)
                           acc))
                       subs
                       subs)))
    (catch :default e
      (js/console.error "[CLIENT] Error applying diff:" e)
      ;; On error, request full refresh by clearing checksum
      (swap! sql-subscriptions 
             (fn [subs]
               (reduce-kv (fn [acc sub-id sub]
                           (if (= (:result-atom sub) result-atom)
                             (update acc sub-id dissoc :last-checksum)
                             acc))
                         subs
                         subs))))))

(defn strunc [s & [chars]]
  (let [s (cstr/replace s #"[\r\n]+" "")
        chars (or chars 100)]
    (try
      (if (> (count s) chars) (str (subs (str s) 0 chars) "...") (str s))
      (catch :default _ s))))

(defn ensure-sql-sse-connection!
  "Ensure we have a single SSE connection for SQL subscriptions"
  []
  (when-not @sql-event-source
    (let [server-url (:server-url @config)
          session-id (:session-id @config)
          sse-url (str server-url "/api/subscribe-sql?session=" session-id)
          es (js/EventSource. sse-url)]
      
      ;; Handle SSE updates for ALL SQL subscriptions
      (set! (.-onmessage es)
            (fn [e]
              (let [data (js->clj (js/JSON.parse (.-data e)) :keywordize-keys true)]
                ;(js/console.log "[CLIENT] SSE SQL update received:" (clj->js data))
                ;(js/console.log "[CLIENT] Message type:" (:type data) "is keyword?" (keyword? (:type data)))
                ;; Handle different message types
                (cond
                  ;; Full update (initial or fallback)
                  (or (= (:type data) :full-update)
                      (= (:type data) "full-update"))
                  (when-let [sub-id (:subscription-id data)]
                    (if-let [sub (get @sql-subscriptions sub-id)]
                      (do
                        (js/console.log "🌕 [CLIENT]" (when (get data :server-cache?) "📦")  " Received full update for" sub-id ": " (strunc (str (get data :query)) 240))
                        ;; Store checksum for validation
                        (swap! sql-subscriptions assoc-in [sub-id :last-checksum] (:checksum data))
                        ;; Update the result atom with full data
                        (reset! (:result-atom sub)
                                (if (:error (:result data))
                                  {:error (:error (:result data)) :loading false :executed-sql (:executed-sql (:result data))}
                                  {:data (:results (:result data)) 
                                   :loading false 
                                   :executed-sql (:executed-sql (:result data))
                                   :metrics (:metrics (:result data))}))
                        ;; (js/console.log "[CLIENT] Full update applied -" (count (:results (:result data))) "results")
                        )
                      (js/console.log "[CLIENT] No subscription found for ID:" sub-id)))
                  
                  ;; Diff update (both row-based and field-based)
                  (or (= (:type data) :diff-update)
                      (= (:type data) "diff-update")
                      (= (:type data) :field-diff-update)
                      (= (:type data) "field-diff-update"))
                  (when-let [sub-id (:subscription-id data)]
                    (if-let [sub (get @sql-subscriptions sub-id)]
                      (let [current-data @(:result-atom sub)]
                        ;; Check if we have base data to apply diff to
                        (if (or (nil? (:data current-data))
                                (and (sequential? (:data current-data))
                                     (empty? (:data current-data))))
                          ;; No base data - we need full update instead
                          (do
                            (js/console.log "🌑 [CLIENT] Received diff but have no base data for" (str )
                                           "\n  Current data:" (str current-data)
                                           "\n  Diff:" (str (select-keys (:diff data) [:type :added :removed :updated])))
                            ;; For now, create empty base data so diff can be applied
                            ;; This handles the case where the diff contains all the data we need
                            (when (seq (get-in data [:diff :added]))
                              (js/console.log "[CLIENT] Diff contains additions, initializing with empty data")
                              (reset! (:result-atom sub) {:data [] :loading false})
                              (apply-row-diff! (:result-atom sub) (:diff data) (:checksum data) (:metrics data))))
                          ;; Have base data - apply diff normally
                          (do
                            (js/console.log "🌓 [CLIENT] Received" (:type data) "for" (str sub-id) 
                                           "\n  Current data count:" (count (:data current-data))
                                           "\n  Diff type:" (str (get-in data [:diff :type]))
                                           "\n  Compression:" (str (get-in data [:diff :compression-ratio])))
                            ;; Apply diff to current data (handles both field and row diffs)
                            (apply-row-diff! (:result-atom sub) (:diff data) (:checksum data) (:metrics data)))))
                      (js/console.log "[CLIENT] No subscription found for diff ID:" sub-id)))
                  
                  ;; Legacy query-update (backward compatibility)
                  (or (= (:type data) :query-update)
                      (= (:type data) "query-update"))
                  (when-let [sub-id (:subscription-id data)]
                    (if-let [sub (get @sql-subscriptions sub-id)]
                      (do
                        (js/console.log "🌙 [CLIENT] Found subscription (legacy), updating result atom for" sub-id)
                        (reset! (:result-atom sub)
                                (if (:error (:result data))
                                  {:error (:error (:result data)) :loading false :executed-sql (:executed-sql (:result data))}
                                  {:data (:results (:result data)) :loading false :executed-sql (:executed-sql (:result data))}))
                        (js/console.log "[CLIENT] Result atom updated with" (count (:results (:result data))) "results"))
                      (js/console.warn "[CLIENT] No subscription found for ID:" sub-id)))))))
      
      ;; Handle errors
      (set! (.-onerror es)
            (fn [e]
              (js/console.error "[CLIENT] SQL SSE connection error:" e)))
      
      (reset! sql-event-source es))))

(defn create-sql-subscription!
  "Create a reactive SQL subscription that updates automatically"
  [sql params as-of result-atom & [client-id]]
  (let [;; Use client-provided ID or generate one
        sub-id (or client-id (str "sql-" (random-uuid)))
        server-url (:server-url @config)
        session-id (:session-id @config)]
    
    ;; Store subscription locally FIRST before any async operations
    (swap! sql-subscriptions assoc sub-id 
           {:sql sql
            :params params
            :result-atom result-atom})
    ;; (js/console.log "[CLIENT] Created subscription" sub-id 
    ;;                 ;"for SQL:" sql
    ;;                 )
    ;; (js/console.log "[CLIENT] Stored in sql-subscriptions. Keys now:" (clj->js (keys @sql-subscriptions)))
    
    ;; Ensure SSE connection exists (this might trigger immediate updates)
    (ensure-sql-sse-connection!)
    
    ;; Call /api/sql which will create server-side subscription AND return initial results
    ;(js/console.log "[CLIENT] Sending SQL request with as-of:" as-of "for query:" sql)
    (-> (js/fetch (str server-url "/api/sql?session=" session-id)
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"}
                       :body (js/JSON.stringify 
                              (clj->js (merge {:sql sql
                                              :params params
                                              :subscription-id sub-id}
                                             (when as-of {:as-of as-of}))))})  ;; Pass sub-id to server
        (.then #(.json %))
        (.then #(let [result (js->clj % :keywordize-keys true)]
                  ;(js/console.log "[CLIENT] Initial query result for" sub-id "Result:" (clj->js result))
                  ;; Server should respect our ID - no mapping needed
                  (when-let [server-sub-id (:subscription-id result)]
                    (when (not= server-sub-id sub-id)
                      (js/console.warn "[CLIENT] Server returned different ID:" server-sub-id "expected:" sub-id)))
                  ;; Check if results are coming via SSE
                  (if (:via-sse result)
                    ;; Results will come via SSE - keep loading state for now
                    (do
                      (js/console.log "[CLIENT] Results for" sub-id "will be delivered via SSE")
                      ;; The SSE handler will update the result-atom when data arrives
                      ;; Keep loading state to indicate we're waiting for SSE data
                      (reset! result-atom {:loading true :via-sse true}))
                    ;; Results included in response (for non-SSE clients)
                    (reset! result-atom 
                            (if (:error result)
                              {:error (:error result) :loading false :executed-sql (:executed-sql result)}
                              {:data (:results result) :loading false :executed-sql (:executed-sql result)})))))
        (.catch #(do
                  (js/console.error "[CLIENT] Query failed for" sub-id %)
                  (reset! result-atom {:error (str %) :loading false}))))
    
    sub-id))

(defn close-sql-subscription!
  "Close a SQL subscription"
  [sub-id]
  (when-let [sub (get @sql-subscriptions sub-id)]
    (when-let [es (:event-source sub)]
      (.close es))
    (swap! sql-subscriptions dissoc sub-id)
    ;; Clean up any mappings for this subscription
    (swap! subscription-id-map dissoc sub-id)
    ;; Also remove reverse mappings
    (swap! subscription-id-map 
           (fn [m]
             (into {} (remove (fn [[k v]] (= v sub-id)) m))))))

;; Cleanup
(defn disconnect! []
  (when @event-source
    (.close @event-source)
    (reset! event-source nil)
    (reset! connected? false))
  ;; Clear history info when disconnecting
  (reset! history-info {})
  ;; Close SQL SSE connection
  (when @sql-event-source
    (.close @sql-event-source)
    (reset! sql-event-source nil))
  ;; Clear all SQL subscriptions and mappings
  (reset! sql-subscriptions {})
  (reset! subscription-id-map {}))