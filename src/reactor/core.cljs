(ns reactor.core
  "Clean re-frame-like API for Reactor - just subscribe and dispatch!"
  (:require [reagent.core :as r]))

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

(defn subscribe
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

;; History/Time Travel Info
(defn get-history-info!
  "Get information about the current session's history"
  []
  (js/console.log "[CLIENT] get-history-info! called")
  (-> (js/fetch (str (:server-url @config) "/api/history-info?" 
                     "session=" (:session-id @config)))
      (.then #(.json %))
      (.then #(do
                (js/console.log "[CLIENT] history-info received:" (clj->js %))
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
                (js/console.log "[CLIENT] Sessions received:" (clj->js %))
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
   
   ;; Get initial state
   (-> (js/fetch (str (:server-url @config) "/api/state?session=" (:session-id @config)))
       (.then #(.json %))
       (.then #(reset! app-db (js->clj % :keywordize-keys true))))
   
   ;; Set up SSE for real-time updates
   (when @event-source
     (.close @event-source))
   
   (let [es (js/EventSource. (str (:server-url @config) "/api/subscribe?session=" (:session-id @config)))]
     (set! (.-onmessage es)
           (fn [e]
             (let [data (js->clj (js/JSON.parse (.-data e)) :keywordize-keys true)]
               ;; Check if this is a full state update or a partial update
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
             (reset! connected? true)))
     (set! (.-onerror es)
           (fn [e]
             (reset! connected? false)
             (js/console.error "Connection lost")))
     (reset! event-source es))
   
   ;; Return the app-db for convenience
   app-db))



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
                (js/console.log "[CLIENT] SSE SQL update received:" (clj->js data))
                (js/console.log "[CLIENT] Message type:" (:type data) "is keyword?" (keyword? (:type data)))
                ;; Skip "connected" messages - only process query updates
                ;; Handle both keyword and string types (JSON serialization converts keywords to strings)
                (when (or (= (:type data) :query-update)
                          (= (:type data) "query-update"))
                  (when-let [sub-id (:subscription-id data)]
                    (js/console.log "[CLIENT] Looking for subscription:" sub-id "in" (count @sql-subscriptions) "subscriptions")
                    (js/console.log "[CLIENT] Available subscription IDs:" (clj->js (keys @sql-subscriptions)))
                    (if-let [sub (get @sql-subscriptions sub-id)]
                      (do
                        (js/console.log "[CLIENT] Found subscription, updating result atom for" sub-id)
                        ;; Update the result atom
                        (reset! (:result-atom sub)
                                (if (:error (:result data))
                                  {:error (:error (:result data)) :loading false}
                                  {:data (:results (:result data)) :loading false}))
                        (js/console.log "[CLIENT] Result atom updated with" (count (:results (:result data))) "results"))
                      (js/console.warn "[CLIENT] No subscription found for ID:" sub-id)))))))
      
      ;; Handle errors
      (set! (.-onerror es)
            (fn [e]
              (js/console.error "[CLIENT] SQL SSE connection error:" e)))
      
      (reset! sql-event-source es))))

(defn create-sql-subscription!
  "Create a reactive SQL subscription that updates automatically"
  [sql params as-of result-atom]
  (let [sub-id (str "sql-" (random-uuid))
        server-url (:server-url @config)
        session-id (:session-id @config)]
    
    ;; Store subscription locally FIRST before any async operations
    (swap! sql-subscriptions assoc sub-id 
           {:sql sql
            :params params
            :result-atom result-atom})
    
    ;; Ensure SSE connection exists (this might trigger immediate updates)
    (ensure-sql-sse-connection!)
    
    ;; Call /api/sql which will create server-side subscription AND return initial results
    (-> (js/fetch (str server-url "/api/sql?session=" session-id)
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"}
                       :body (js/JSON.stringify 
                              (clj->js {:sql sql
                                       :params params
                                       :as-of as-of
                                       :subscription-id sub-id}))})  ;; Pass sub-id to server
        (.then #(.json %))
        (.then #(let [result (js->clj % :keywordize-keys true)]
                  (js/console.log "[CLIENT] Initial query result for" sub-id "Result:" (clj->js result))
                  ;; Handle subscription ID from server
                  (if-let [server-sub-id (:subscription-id result)]
                    (if (not= server-sub-id sub-id)
                      (do
                        (js/console.log "[CLIENT] Server returned different subscription ID:" server-sub-id "updating mapping")
                        ;; Move the subscription to the server's ID
                        (let [sub-info (get @sql-subscriptions sub-id)]
                          (swap! sql-subscriptions dissoc sub-id)
                          (swap! sql-subscriptions assoc server-sub-id sub-info)
                          (js/console.log "[CLIENT] Updated subscriptions map. Keys:" (clj->js (keys @sql-subscriptions)))))
                      (js/console.log "[CLIENT] Server confirmed subscription ID:" server-sub-id))
                    (js/console.warn "[CLIENT] Server did not return subscription ID!"))
                  (reset! result-atom 
                          (if (:error result)
                            {:error (:error result) :loading false}
                            {:data (:results result) :loading false}))))
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
    (swap! sql-subscriptions dissoc sub-id)))

;; Cleanup
(defn disconnect! []
  (when @event-source
    (.close @event-source)
    (reset! event-source nil)
    (reset! connected? false))
  ;; Clear history info when disconnecting
  (reset! history-info {})
  ;; Close all SQL subscriptions
  (doseq [[sub-id sub] @sql-subscriptions]
    (when-let [es (:event-source sub)]
      (.close es)))
  (reset! sql-subscriptions {}))