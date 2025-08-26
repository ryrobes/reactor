(ns reactor.sql-client
  "SQL subscription client for Reactor with transparent diff support
   Applications using this library don't need to know about diffs at all"
  (:require [reactor.client-diff :as diff]
            [cljs.core.async :as async :refer [<!]]
            [clojure.string :as str])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))

;; ============================================================================
;; Subscription Management
;; ============================================================================

(defonce active-subscriptions (atom {}))
(defonce subscription-results (atom {}))
(defonce sse-connections (atom {}))

;; ============================================================================
;; Configuration
;; ============================================================================

(defonce config 
  (atom {:server-url "http://localhost:5000"
         :session-id (str "client-" (random-uuid))
         :debug? false}))

(defn set-config! [opts]
  (swap! config merge opts))

;; ============================================================================
;; SSE Message Handling with Transparent Diff Support
;; ============================================================================

(defn- handle-sse-message
  "Process SSE messages, transparently handling diffs"
  [subscription-id message-str]
  (try
    (let [data (js/JSON.parse message-str)
          data-clj (js->clj data :keywordize-keys true)
          {:keys [type]} data-clj]
      
      (when (:debug? @config)
        (js/console.log "[SQL-CLIENT]" subscription-id "received" type))
      
      (case type
        ;; Connection established
        :connected
        (when (:debug? @config)
          (js/console.log "[SQL-CLIENT]" subscription-id "connected"))
        
        ;; Subscription created
        :subscription-created
        (when (:debug? @config)
          (js/console.log "[SQL-CLIENT]" subscription-id "created"))
        
        ;; Handle ALL update types transparently
        (:query-update :full-update :diff-update :field-diff-update)
        (let [;; Get current results
              current-results (get @subscription-results subscription-id [])
              ;; Apply diff transparently - returns full results either way
              new-results (diff/process-subscription-update! data-clj current-results)]
          
          ;; Log diff stats if in debug mode
          (when (and (:debug? @config) 
                    (#{:diff-update :field-diff-update} type))
            (diff/log-diff-stats data-clj))
          
          ;; Store the new results
          (swap! subscription-results assoc subscription-id new-results)
          
          ;; Call the user's callback with the FULL results
          ;; They never need to know a diff was involved!
          (when-let [sub-info (get @active-subscriptions subscription-id)]
            (when-let [callback (:callback sub-info)]
              (callback {:results new-results
                        :subscription-id subscription-id
                        :metrics (:metrics data-clj)}))))
        
        ;; Unknown message type
        (when (:debug? @config)
          (js/console.warn "[SQL-CLIENT] Unknown message type:" type))))
    
    (catch js/Error e
      (js/console.error "[SQL-CLIENT] Error processing message:" e))))

;; ============================================================================
;; Public API - Simple and Clean
;; ============================================================================

(defn subscribe-sql!
  "Subscribe to a SQL query with automatic updates
   Returns a subscription ID that can be used to unsubscribe
   
   Options:
   - :callback - Function called with {:results [...] :subscription-id ...}
   - :subscription-id - Optional custom ID (auto-generated if not provided)
   - :error-callback - Optional error handler"
  [sql & [{:keys [callback subscription-id error-callback params]}]]
  (let [sub-id (or subscription-id (str "sql-sub-" (random-uuid)))
        {:keys [server-url session-id]} @config
        ;; Add a unique identifier to force a new connection
        connection-id (str (random-uuid))
        sse-url (str server-url "/api/subscribe-sql?session=" session-id "&connection=" connection-id)
        _ (js/console.log "[SQL-CLIENT] Creating EventSource for session:" session-id "with URL:" sse-url)
        event-source (js/EventSource. sse-url)]
    
    ;; Set up SSE handlers
    (set! (.-onopen event-source)
          (fn [_]
            (when (:debug? @config)
              (js/console.log "[SQL-CLIENT]" sub-id "SSE connected"))))
    
    (set! (.-onmessage event-source)
          (fn [e]
            (handle-sse-message sub-id (.-data e))))
    
    (set! (.-onerror event-source)
          (fn [e]
            (js/console.error "[SQL-CLIENT]" sub-id "SSE error:" e)
            (when error-callback
              (error-callback {:error "Connection error" :event e}))))
    
    ;; Store subscription info
    (swap! active-subscriptions assoc sub-id
           {:sql sql
            :params params
            :callback callback
            :error-callback error-callback
            :event-source event-source})
    
    ;; Store SSE connection
    (swap! sse-connections assoc sub-id event-source)
    
    ;; Send subscription request
    (-> (js/fetch (str server-url "/api/sql")
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"
                                    "X-Session-ID" session-id}
                       :body (js/JSON.stringify 
                              (clj->js {:sql sql
                                       :params params
                                       :subscription-id sub-id}))})
        (.then (fn [response]
                 (if (.-ok response)
                   (.json response)
                   (throw (js/Error. (str "HTTP " (.-status response)))))))
        (.then (fn [data]
                 ;; Initial results
                 (let [result-clj (js->clj data :keywordize-keys true)]
                   (when (:results result-clj)
                     (swap! subscription-results assoc sub-id (:results result-clj))
                     (when callback
                       (callback result-clj))))))
        (.catch (fn [error]
                  (js/console.error "[SQL-CLIENT]" sub-id "subscription failed:" error)
                  (when error-callback
                    (error-callback {:error error})))))
    
    ;; Return subscription ID
    sub-id))

(defn unsubscribe!
  "Unsubscribe from a SQL query"
  [subscription-id]
  (js/console.log "[SQL-CLIENT] Unsubscribing from:" subscription-id)
  (when-let [event-source (get @sse-connections subscription-id)]
    (js/console.log "[SQL-CLIENT] Closing EventSource for:" subscription-id)
    (.close event-source)
    (swap! sse-connections dissoc subscription-id))
  (swap! active-subscriptions dissoc subscription-id)
  (swap! subscription-results dissoc subscription-id))

(defn get-results
  "Get current results for a subscription (useful for reactive components)"
  [subscription-id]
  (get @subscription-results subscription-id))

(defn execute-sql!
  "Execute a one-time SQL query (no subscription)
   Useful for mutations like INSERT, UPDATE, DELETE"
  [sql & [{:keys [params callback error-callback]}]]
  (let [{:keys [server-url session-id]} @config]
    (-> (js/fetch (str server-url "/api/sql-exec")
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"
                                    "X-Session-ID" session-id}
                       :body (js/JSON.stringify 
                              (clj->js {:sql sql :params params}))})
        (.then (fn [response]
                 (if (.-ok response)
                   (.json response)
                   (throw (js/Error. (str "HTTP " (.-status response)))))))
        (.then (fn [data]
                 (when callback
                   (callback (js->clj data :keywordize-keys true)))))
        (.catch (fn [error]
                  (js/console.error "[SQL-CLIENT] Execute failed:" error)
                  (when error-callback
                    (error-callback {:error error})))))))

;; ============================================================================
;; Reagent Integration Helpers
;; ============================================================================

(defn use-sql-subscription
  "React hook-style helper for Reagent components
   Automatically manages subscription lifecycle"
  [sql & [opts]]
  (let [results (reagent.core/atom nil)
        sub-id (atom nil)]
    
    ;; Subscribe on mount
    (reagent.core/create-class
     {:component-did-mount
      (fn []
        (let [id (subscribe-sql! sql 
                  (assoc opts :callback 
                         (fn [data]
                           (reset! results (:results data))
                           (when-let [cb (:callback opts)]
                             (cb data)))))]
          (reset! sub-id id)))
      
      :component-will-unmount
      (fn []
        (when @sub-id
          (unsubscribe! @sub-id)))
      
      :reagent-render
      (fn []
        @results)})))

;; ============================================================================
;; Debug Helpers
;; ============================================================================

(defn enable-debug!
  "Enable debug logging"
  []
  (swap! config assoc :debug? true)
  (js/console.log "[SQL-CLIENT] Debug mode enabled"))

(defn get-subscription-stats
  "Get statistics about active subscriptions"
  []
  {:active-count (count @active-subscriptions)
   :subscriptions (keys @active-subscriptions)
   :total-results (reduce + 0 (map count (vals @subscription-results)))})