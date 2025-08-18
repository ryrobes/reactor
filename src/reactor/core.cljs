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

(defn configure! 
  "Set server URL and session ID"
  [{:keys [server-url session-id]}]
  (when server-url (swap! config assoc :server-url server-url))
  (when session-id (swap! config assoc :session-id session-id)))

;; Subscriptions (just like re-frame!)
(defonce subscriptions (atom {}))

(defn reg-sub
  "Register a subscription - handler should be a function of [db query-args]"
  [id handler]
  (swap! subscriptions assoc id handler))

(defn reg-sub-path
  "Register a subscription for a simple path lookup"
  [id path]
  (reg-sub id (fn [db _] (get-in db path))))

(defn subscribe
  "Subscribe to data - returns a reactive atom"
  [query]
  (let [handler (get @subscriptions (first query))
        args (rest query)
        result-atom (r/atom nil)]
    ;; Set up reactive computation
    (r/track! (fn []
                (when handler
                  (reset! result-atom (handler @app-db args)))))
    result-atom))

;; Simple key-path subscription
(reg-sub :db (fn [db _] db))
(reg-sub :get (fn [db [path]] (get-in db path)))

;; Built-in subscriptions for session management
(reg-sub :session-id (fn [_ _] (:session-id @config)))
(reg-sub :sessions (fn [_ _] @sessions))
(reg-sub :history-info (fn [_ _] @history-info))
(reg-sub :connected? (fn [_ _] @connected?))

;; Dispatch (async but looks sync!)
(defn dispatch!
  "Dispatch an event - automatically handles server communication"
  [event]
  (-> (js/fetch (str (:server-url @config) "/api/dispatch?session=" (:session-id @config))
                #js {:method "POST"
                     :headers #js {"Content-Type" "application/json"}
                     :body (js/JSON.stringify (clj->js event))})
      (.then #(.json %))
      (.then #(reset! app-db (js->clj % :keywordize-keys true)))
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

;; Time travel API
(defn undo! []
  (-> (js/fetch (str (:server-url @config) "/api/undo?session=" (:session-id @config))
                #js {:method "POST"})
      (.then #(.json %))
      (.then #(reset! app-db (js->clj % :keywordize-keys true)))))

(defn redo! []
  (-> (js/fetch (str (:server-url @config) "/api/redo?session=" (:session-id @config))
                #js {:method "POST"})
      (.then #(.json %))
      (.then #(reset! app-db (js->clj % :keywordize-keys true)))))

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

;; Session Management
;; Forward declarations for functions defined later
(declare init!)
(declare disconnect!)

(defn get-sessions!
  "Fetch all available sessions"
  []
  (-> (js/fetch (str (:server-url @config) "/api/sessions"))
      (.then #(.json %))
      (.then #(reset! sessions (js->clj % :keywordize-keys true)))))

(defn switch-session!
  "Switch to a different session"
  [session-id]
  (disconnect!)
  (configure! {:session-id session-id})
  (init!))

(defn create-session!
  "Create a new session with optional initial state"
  ([session-id]
   (create-session! session-id {}))
  ([session-id initial-state]
   (-> (js/fetch (str (:server-url @config) "/api/create-session")
                 #js {:method "POST"
                      :headers #js {"Content-Type" "application/json"}
                      :body (js/JSON.stringify (clj->js {:session-id session-id
                                                          :initial-state initial-state}))})
       (.then #(.json %))
       (.then #(switch-session! session-id)))))

;; History/Time Travel Info
(defn get-history-info!
  "Get information about the current session's history"
  []
  (-> (js/fetch (str (:server-url @config) "/api/history-info?" 
                     "session=" (:session-id @config)))
      (.then #(.json %))
      (.then #(reset! history-info (js->clj % :keywordize-keys true)))))

(defn jump-to-history!
  "Jump to a specific point in history"
  [index]
  (-> (js/fetch (str (:server-url @config) "/api/jump-to-history")
                #js {:method "POST"
                     :headers #js {"Content-Type" "application/json"}
                     :body (js/JSON.stringify (clj->js {:session-id (:session-id @config)
                                                        :index index}))})
      (.then #(.json %))
      (.then #(reset! app-db (js->clj % :keywordize-keys true)))))

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
             (reset! app-db (js->clj (js/JSON.parse (.-data e)) :keywordize-keys true))
             (reset! connected? true)))
     (set! (.-onerror es)
           (fn [e]
             (reset! connected? false)
             (js/console.error "Connection lost")))
     (reset! event-source es))
   
   ;; Return the app-db for convenience
   app-db))

;; Cleanup
(defn disconnect! []
  (when @event-source
    (.close @event-source)
    (reset! event-source nil)
    (reset! connected? false)))