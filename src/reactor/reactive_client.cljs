(ns reactor.reactive-client
  "Client-side support for reactive SQL subscriptions."
  (:require [reactor.core :as r]
            [reagent.core :as reagent]
            [clojure.string :as str]))

;; ============================================================================
;; SQL Subscription Management
;; ============================================================================

(defonce sql-subscriptions (atom {}))
(defonce sql-results (reagent/atom {}))
(defonce sql-event-sources (atom {}))

(defn subscribe-sql!
  "Subscribe to a SQL query with automatic updates.
   Returns a reactive atom containing the query results."
  [query-id sql & [params]]
  (let [server-url (:server-url @r/config)
        session-id (:session-id @r/config)
        result-atom (reagent/atom {:loading? true})
        
        ;; Create EventSource for SSE
        event-source (js/EventSource. 
                      (str server-url "/api/subscribe-sql?session=" session-id))]
    
    ;; Set up event handlers
    (set! (.-onopen event-source)
          (fn [e]
            (js/console.log "SQL subscription connected" query-id)))
    
    (set! (.-onmessage event-source)
          (fn [e]
            (let [data (js/JSON.parse (.-data e))
                  data-clj (js->clj data :keywordize-keys true)]
              (case (:type data-clj)
                :subscription-created
                (do
                  (js/console.log "SQL subscription created:" (:subscription-id data-clj))
                  (swap! sql-subscriptions assoc query-id 
                        {:subscription-id (:subscription-id data-clj)
                         :sql sql
                         :params params}))
                
                :query-update
                (do
                  (js/console.log "Query update received for" query-id)
                  (reset! result-atom (:result data-clj))
                  (swap! sql-results assoc query-id (:result data-clj)))
                
                (js/console.warn "Unknown message type:" (:type data-clj))))))
    
    (set! (.-onerror event-source)
          (fn [e]
            (js/console.error "SQL subscription error:" e)
            (swap! result-atom assoc :error "Connection error")))
    
    ;; Store event source for cleanup
    (swap! sql-event-sources assoc query-id event-source)
    
    ;; Send subscription request
    (js/fetch (str server-url "/api/subscribe-sql?session=" session-id)
             #js {:method "POST"
                  :headers #js {"Content-Type" "application/json"}
                  :body (js/JSON.stringify 
                         (clj->js {:sql sql :params params}))})
    
    result-atom))

(defn unsubscribe-sql!
  "Unsubscribe from a SQL query."
  [query-id]
  (when-let [event-source (get @sql-event-sources query-id)]
    (.close event-source)
    (swap! sql-event-sources dissoc query-id))
  
  (when-let [sub-info (get @sql-subscriptions query-id)]
    (let [server-url (:server-url @r/config)
          session-id (:session-id @r/config)]
      (js/fetch (str server-url "/api/unsubscribe-sql?session=" session-id)
               #js {:method "POST"
                    :headers #js {"Content-Type" "application/json"}
                    :body (js/JSON.stringify 
                           (clj->js {:subscription-id (:subscription-id sub-info)}))}))
    (swap! sql-subscriptions dissoc query-id)
    (swap! sql-results dissoc query-id)))

(defn use-sql-query
  "React hook for SQL queries with automatic updates.
   Returns [results loading? error?]"
  [sql & [params]]
  (let [query-id (str "sql-" (hash [sql params]))
        [result set-result!] (reagent/atom {:loading? true})]
    
    ;; Set up subscription on mount
    (reagent/create-class
     {:component-did-mount
      (fn []
        (let [result-atom (subscribe-sql! query-id sql params)]
          ;; Watch for updates
          (add-watch result-atom ::update
                    (fn [_ _ _ new-val]
                      (set-result! new-val)))))
      
      :component-will-unmount
      (fn []
        (unsubscribe-sql! query-id))
      
      :reagent-render
      (fn []
        [(:results @result) 
         (:loading? @result false)
         (:error @result)])})))

;; ============================================================================
;; Enhanced Subscriptions with SQL
;; ============================================================================

(defn reg-sql-sub
  "Register a subscription backed by a SQL query."
  [id sql]
  (r/reg-sub id
   (fn [db _]
     (let [query-id (str "sql-sub-" (name id))
           cached (get @sql-results query-id)]
       (when-not cached
         ;; Trigger subscription if not already active
         (subscribe-sql! query-id sql))
       (or cached {:loading? true})))))

;; ============================================================================
;; Example Components
;; ============================================================================

(defn live-query-table
  "Component that displays live SQL query results in a table."
  [sql]
  (let [query-id (str "table-" (hash sql))
        results (subscribe-sql! query-id sql)]
    (fn []
      [:div.live-query-table
       (cond
         (:loading? @results)
         [:div.loading "Loading..."]
         
         (:error @results)
         [:div.error "Error: " (:error @results)]
         
         (:results @results)
         [:table
          [:thead
           [:tr
            (doall
             (for [col (keys (first (:results @results)))]
               ^{:key col} [:th (name col)]))]]
          [:tbody
           (doall
            (for [row (:results @results)]
              ^{:key (hash row)}
              [:tr
               (for [[k v] row]
                 ^{:key k} [:td (str v)])]))]]
         
         :else
         [:div "No results"])])))

(defn active-todos-count
  "Component showing count of active todos with live updates."
  []
  (let [query-id "active-todos-count"
        results (subscribe-sql! query-id 
                               "SELECT COUNT(*) as count FROM todo_sessions 
                                WHERE app_todos_count > 0")]
    (fn []
      [:div.active-todos
       "Active TODO sessions: "
       (if (:loading? @results)
         [:span.loading "..."]
         [:span.count (-> @results :results first :count)])])))

;; ============================================================================
;; Cleanup
;; ============================================================================

(defn cleanup-all!
  "Clean up all SQL subscriptions."
  []
  (doseq [[query-id _] @sql-subscriptions]
    (unsubscribe-sql! query-id)))