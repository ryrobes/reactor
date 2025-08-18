(ns reactor.client-xtdb
  "ClojureScript client for XTDB-backed Reactor with SQL support"
  (:require [reagent.core :as r]
            [cljs.core.async :as async :refer [<! >! go chan]]
            [clojure.string :as str]))

(defonce ^:private event-sources (atom {}))
(defonce ^:private query-results (atom {}))

;; ===== Query Result Atoms =====

(defprotocol IQueryResult
  (refresh! [this])
  (close! [this]))

(deftype QueryResultAtom [query query-format result-atom event-source]
  IDeref
  (-deref [_] @result-atom)
  
  IWatchable
  (-add-watch [this key f]
    (add-watch result-atom key f))
  (-remove-watch [this key]
    (remove-watch result-atom key))
  
  IQueryResult
  (refresh! [this]
    ;; Force a refresh by reconnecting
    (when event-source
      (.close event-source)
      ;; Reconnect logic would go here
      ))
  
  (close! [this]
    (when event-source
      (.close event-source))
    (swap! event-sources dissoc query)
    (swap! query-results dissoc query)))

;; ===== Query Subscriptions =====

(defn subscribe-query!
  "Subscribe to a query with automatic updates
   Options:
   - :query - The query (keypath vector, SQL string, or HoneySQL map)
   - :query-format - :keypath (default), :sql, or :honeysql
   - :server-url - SSE server URL
   - :poll-ms - Polling interval in milliseconds
   - :session-id - Optional session ID for isolation
   - :format - Response format (:edn or :json)"
  [& {:keys [query query-format server-url poll-ms session-id format]
      :or {query-format :keypath
           server-url "http://localhost:8080"
           format :edn}}]
  
  (let [existing (get @query-results query)]
    (if existing
      existing
      (let [result-atom (r/atom nil)
            query-str (case query-format
                       :keypath (pr-str query)
                       :sql query
                       :honeysql (.stringify js/JSON (clj->js query)))
            params (cond-> {:query query-str
                           :query-format (name query-format)
                           :format (name format)}
                     poll-ms (assoc :poll-ms poll-ms)
                     session-id (assoc :session-id session-id))
            url (str server-url "/subscribe?" 
                    (str/join "&" (map (fn [[k v]] 
                                         (str (name k) "=" (js/encodeURIComponent v)))
                                       params)))
            event-source (js/EventSource. url)]
        
        ;; Set up event handlers
        (set! (.-onmessage event-source)
              (fn [event]
                (let [data (.-data event)]
                  (try
                    (let [parsed (case format
                                  :json (js->clj (.parse js/JSON data) :keywordize-keys true)
                                  :edn (cljs.reader/read-string data)
                                  (cljs.reader/read-string data))]
                      (reset! result-atom parsed))
                    (catch :default e
                      (js/console.error "Failed to parse SSE data:" e data))))))
        
        (set! (.-onerror event-source)
              (fn [event]
                (js/console.error "SSE error:" event)))
        
        (set! (.-onopen event-source)
              (fn [event]
                (js/console.log "SSE connection opened for query:" query)))
        
        ;; Store references
        (let [query-result (->QueryResultAtom query query-format result-atom event-source)]
          (swap! event-sources assoc query event-source)
          (swap! query-results assoc query query-result)
          query-result)))))

;; ===== Direct Query Execution =====

(defn execute-query!
  "Execute a query and return results via promise
   Options same as subscribe-query! but without polling"
  [& {:keys [query query-format server-url session-id as-of]
      :or {query-format :keypath
           server-url "http://localhost:8080"}}]
  
  (js/Promise.
   (fn [resolve reject]
     (let [body (case query-format
                  :keypath {:query query}
                  :sql {:query query}
                  :honeysql {:query query})
           body-with-opts (cond-> body
                            session-id (assoc :session-id session-id)
                            as-of (assoc :as-of as-of))]
       
       (-> (js/fetch (str server-url "/query")
                     #js {:method "POST"
                          :headers #js {"Content-Type" "application/edn"}
                          :body (pr-str body-with-opts)})
           (.then (fn [response]
                    (if (.-ok response)
                      (.text response)
                      (throw (js/Error. (str "Query failed: " (.-status response)))))))
           (.then (fn [text]
                    (let [result (cljs.reader/read-string text)]
                      (if (= (:status result) :ok)
                        (resolve (:result result))
                        (reject (js/Error. (:message result)))))))
           (.catch reject))))))

;; ===== Entity Updates =====

(defn update-entity!
  "Update an entity in XTDB"
  [entity-id data & {:keys [server-url session-id]
                     :or {server-url "http://localhost:8080"}}]
  
  (js/Promise.
   (fn [resolve reject]
     (let [body (cond-> {:entity-id entity-id
                        :data data}
                  session-id (assoc :session-id session-id))]
       
       (-> (js/fetch (str server-url "/update")
                     #js {:method "POST"
                          :headers #js {"Content-Type" "application/edn"}
                          :body (pr-str body)})
           (.then (fn [response]
                    (if (.-ok response)
                      (.text response)
                      (throw (js/Error. (str "Update failed: " (.-status response)))))))
           (.then (fn [text]
                    (let [result (cljs.reader/read-string text)]
                      (if (= (:status result) :ok)
                        (resolve (:tx-id result))
                        (reject (js/Error. (:message result)))))))
           (.catch reject))))))

;; ===== SQL Query Builder =====

(defn build-sql
  "Build SQL query using HoneySQL DSL on server"
  [table & {:keys [select where order-by limit offset server-url]
            :or {server-url "http://localhost:8080"}}]
  
  (js/Promise.
   (fn [resolve reject]
     (let [body (cond-> {:table table}
                  select (assoc :select select)
                  where (assoc :where where)
                  order-by (assoc :order-by order-by)
                  limit (assoc :limit limit)
                  offset (assoc :offset offset))]
       
       (-> (js/fetch (str server-url "/sql-builder")
                     #js {:method "POST"
                          :headers #js {"Content-Type" "application/json"}
                          :body (.stringify js/JSON (clj->js body))})
           (.then (fn [response]
                    (if (.-ok response)
                      (.json response)
                      (throw (js/Error. (str "SQL build failed: " (.-status response)))))))
           (.then (fn [json]
                    (let [result (js->clj json :keywordize-keys true)]
                      (if (= (:status result) "ok")
                        (resolve result)
                        (reject (js/Error. (:message result)))))))
           (.catch reject))))))

;; ===== Table Sync =====

(defn sync-to-table!
  "Sync an entity to its flattened SQL table"
  [entity-id & {:keys [server-url]
                :or {server-url "http://localhost:8080"}}]
  
  (js/Promise.
   (fn [resolve reject]
     (-> (js/fetch (str server-url "/sync-table")
                   #js {:method "POST"
                        :headers #js {"Content-Type" "application/edn"}
                        :body (pr-str {:entity-id entity-id})})
         (.then (fn [response]
                  (if (.-ok response)
                    (.text response)
                    (throw (js/Error. (str "Sync failed: " (.-status response)))))))
         (.then (fn [text]
                  (let [result (cljs.reader/read-string text)]
                    (if (= (:status result) :ok)
                      (resolve (:message result))
                      (reject (js/Error. (:message result)))))))
         (.catch reject)))))

;; ===== Convenience Functions =====

(defn use-query
  "React hook-like function for Reagent components
   Returns a reactive atom that updates when query results change"
  [query & opts]
  (let [result (apply subscribe-query! :query query opts)]
    result))

(defn close-all!
  "Close all active subscriptions"
  []
  (doseq [[_ source] @event-sources]
    (.close source))
  (reset! event-sources {})
  (reset! query-results {}))

;; ===== Example Usage =====

(comment
  ;; Subscribe to todos with keypath
  (def todos (subscribe-query! :query ["todos"]
                               :poll-ms 1000))
  
  ;; Subscribe with SQL
  (def active-todos (subscribe-query! 
                     :query "SELECT * FROM todos WHERE completed = false"
                     :query-format :sql
                     :poll-ms 2000))
  
  ;; Subscribe with HoneySQL
  (def recent-todos (subscribe-query!
                     :query {:select [:*]
                            :from :todos
                            :where [:> :created-at "2024-01-01"]
                            :order-by [[:created-at :desc]]
                            :limit 10}
                     :query-format :honeysql))
  
  ;; Use in Reagent component
  (defn todo-list []
    (let [todos (use-query ["todos"] :poll-ms 1000)]
      [:div
       [:h2 "Todos"]
       [:ul
        (for [todo @todos]
          [:li {:key (:id todo)} (:text todo)])]]))
  
  ;; Execute one-time query
  (-> (execute-query! :query ["todos" "active"])
      (.then #(js/console.log "Active todos:" %))
      (.catch #(js/console.error "Query error:" %)))
  
  ;; Update entity
  (-> (update-entity! "todo-1" {:text "Updated todo" :completed true})
      (.then #(js/console.log "Updated with tx:" %)))
  )