(ns reactor.reactive-server
  "Enhanced server with Kafka-based reactive SQL subscriptions."
  (:require [reactor.server :as base-server]
            [reactor.kafka-reactive :as kafka]
            [reactor.sql-reactive-bridge :as bridge]
            [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [org.httpkit.server :as http]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]))

(defn create-reactive-handler
  "Create a Ring handler with reactive SQL subscription endpoints."
  [& opts]
  (let [base-handler (apply base-server/create-handler opts)]
    (fn [req]
      (let [path (:uri req)
            method (:request-method req)
            session-id (base-server/get-session-id req (constantly "default"))]
        
        (log/debug "[REACTIVE-HANDLER] Handling request:" method path)
        
        ;; Handle CORS preflight requests
        (if (= method :options)
          {:status 200
           :headers {"Access-Control-Allow-Origin" "*"
                    "Access-Control-Allow-Methods" "GET, POST, PUT, DELETE, OPTIONS"
                    "Access-Control-Allow-Headers" "Content-Type, Authorization"
                    "Access-Control-Max-Age" "3600"}
           :body ""}
          
          (case path
          ;; Reactive SQL subscription endpoint
          "/api/subscribe-sql"
          (case method
            ;; GET request establishes SSE connection
            :get
            (do
              (log/info "[REACTIVE-SERVER] SSE connection request from session:" session-id)
              ;; Clean up OLD subscriptions for this session before creating new connection
              ;; Also clean up any orphaned subscriptions (those without active SSE channels)
              (let [session-subs (filter (fn [[sub-id sub-info]]
                                          (= (:session-id sub-info) session-id))
                                        @kafka/active-subscriptions)
                    orphaned-subs (filter (fn [[sub-id sub-info]]
                                           (let [sub-session (:session-id sub-info)]
                                             (empty? (get @kafka/sse-channels sub-session))))
                                         @kafka/active-subscriptions)]
                (when (seq session-subs)
                  (log/info "[REACTIVE-SERVER] Cleaning up" (count session-subs) "old subscriptions for session" session-id)
                  (doseq [[sub-id _] session-subs]
                    (kafka/unsubscribe-query! sub-id)))
                (when (seq orphaned-subs)
                  (log/info "[REACTIVE-SERVER] Cleaning up" (count orphaned-subs) "orphaned subscriptions")
                  (doseq [[sub-id _] orphaned-subs]
                    (kafka/unsubscribe-query! sub-id))))
              
              (http/with-channel req channel
                ;; Set up SSE headers
                (http/send! channel {:status 200
                                   :headers {"Content-Type" "text/event-stream"
                                           "Cache-Control" "no-cache"
                                           "Access-Control-Allow-Origin" "*"}} false)
                
                ;; Register the SSE channel for this session
                (kafka/register-sse-channel! session-id channel)
                (log/info "[REACTIVE-SERVER] SSE channel registered for session:" session-id)
                
                ;; Send initial connection message
                (http/send! channel 
                          (str "data: " 
                               (json/generate-string {:type :connected
                                                     :session-id session-id})
                               "\n\n")
                          false)
                
                ;; Clean up on channel close
                (http/on-close channel
                             (fn [status]
                               (log/info "[REACTIVE-SERVER] SSE channel closed for session" session-id "status:" status)
                               ;; Clean up ALL subscriptions for this session
                               (doseq [[sub-id sub-info] @kafka/active-subscriptions]
                                 (when (= (:session-id sub-info) session-id)
                                   (log/info "[REACTIVE-SERVER] Cleaning up subscription" sub-id "for disconnected session")
                                   (kafka/unsubscribe-query! sub-id)))
                               (kafka/unregister-sse-channel! session-id channel)))))
            
            ;; POST request registers a SQL subscription
            :post
            (let [body (json/parse-string (slurp (:body req)) true)
                  sql (:sql body)
                  params (:params body)
                  subscription-id (:subscription-id body)
                  ;; Use provided subscription-id or generate one
                  sub-id (or subscription-id (kafka/subscribe-query! session-id sql params))]
              
              ;; Register the subscription
              (when subscription-id
                (kafka/register-query-subscription! subscription-id sql params 
                                                   (kafka/create-subscription-callback session-id)
                                                   session-id))
              
              ;; Execute immediately to get initial results
              (kafka/re-execute-subscription sub-id)
              
              {:status 200
               :headers {"Content-Type" "application/json"
                        "Access-Control-Allow-Origin" "*"}
               :body (json/generate-string {:subscription-id sub-id
                                           :status "registered"})}))
          
          ;; Unsubscribe endpoint
          "/api/unsubscribe-sql"
          (let [body (json/parse-string (slurp (:body req)) true)
                sub-id (:subscription-id body)]
            (kafka/unsubscribe-query! sub-id)
            {:status 200
             :headers {"Content-Type" "application/json"
                      "Access-Control-Allow-Origin" "*"}
             :body (json/generate-string {:success true})})
          
          ;; List active subscriptions (for debugging)
          "/api/subscriptions"
          (do
            (log/info "[REACTIVE-HANDLER] /api/subscriptions called")
            (log/info "Active subscriptions atom:" @kafka/active-subscriptions)
            (log/info "Table-to-subs atom:" @kafka/table-to-subs)
            {:status 200
             :headers {"Content-Type" "application/json"
                      "Access-Control-Allow-Origin" "*"}
             :body (json/generate-string 
                    {:active-subscriptions (map (fn [[id sub]]
                                                 {:id id
                                                  :query (:query sub)
                                                  :tables (:tables sub)
                                                  :session-id (:session-id sub)})
                                               @kafka/active-subscriptions)
                     :table-to-subs @kafka/table-to-subs
                     :count (count @kafka/active-subscriptions)})})
          
          ;; Override SQL query - create subscription for non-session tables
          "/api/sql"
          (let [body (json/parse-string (slurp (:body req)) true)
                sql (:sql body)
                params (:params body)
                as-of (:as-of body)
                ;; Check if this query is for a session table
                is-session-query? (or (re-find #"(?i)FROM\s+\w*_?sessions" sql)
                                    (re-find #"(?i)INTO\s+\w*_?sessions" sql))]
            (log/debug "[REACTIVE-SERVER] /api/sql called with SQL:" sql)
            (log/debug "[REACTIVE-SERVER] Session ID:" session-id "Has SSE channels:" (seq (get @kafka/sse-channels session-id)))
            (log/info "[REACTIVE-SERVER] as-of value:" (pr-str as-of) "is-temporal?" (and as-of (not (empty? as-of))))
            ;; Only create subscription for non-session queries AND non-temporal queries
            ;; Note: Check for actual temporal value, not just truthy (empty string is truthy!)
            (if (or is-session-query? (and as-of (not (empty? as-of))))
              ;; Just execute without subscription for session tables OR temporal queries
              (let [node @session/default-node
                    result (if node
                            (if as-of
                              ;; Use time-travel execution
                              (let [time-travel-ns (require 'reactor.time-travel-sql)
                                    exec-fn (ns-resolve 'reactor.time-travel-sql 'execute-sql-with-time-travel)]
                                (exec-fn node sql params as-of))
                              (xts/execute-sql node sql params))
                            {:error "No XTDB node available"})]
                {:status 200
                 :headers {"Content-Type" "application/json"
                          "Access-Control-Allow-Origin" "*"}
                 :body (json/generate-string 
                        (if (:subscription-id body)
                          ;; Include the client's subscription ID for tracking
                          (assoc result :subscription-id (:subscription-id body))
                          result))})
              ;; Create subscription for business tables (only when NOT time traveling)
              (let [;; Always use client-provided ID when available
                    client-id (:subscription-id body)
                    sub-id (or client-id (str "sub-" (java.util.UUID/randomUUID)))
                    
                    ;; Check if this subscription already exists
                    existing-sub (get @kafka/active-subscriptions sub-id)]
                
                ;; Unregister old subscription if it exists (allows SQL updates)
                (when existing-sub
                  (log/info "[REACTIVE-SERVER] Updating existing subscription:" sub-id)
                  (kafka/unregister-query-subscription! sub-id))
                
                ;; Register new/updated subscription
                (log/info "[REACTIVE-SERVER] Registering subscription:" sub-id "for SQL:" sql)
                (kafka/register-query-subscription! 
                 sub-id sql params 
                 (kafka/create-subscription-callback session-id) 
                 session-id)
                
                ;; Execute immediately to get initial results
                (kafka/re-execute-subscription sub-id)
                (log/info "[REACTIVE-SERVER] Using subscription" sub-id "for session" session-id)
                (log/info "[REACTIVE-SERVER] Active SSE channels for session:" (count (get @kafka/sse-channels session-id [])))
                ;; Return initial results WITH the subscription ID
                (let [node @session/default-node
                      time-travel-ns (when as-of (require 'reactor.time-travel-sql))
                      exec-fn (when as-of (ns-resolve 'reactor.time-travel-sql 'execute-sql-with-time-travel))
                      result (if node
                              (if as-of
                                (do (log/info "[REACTIVE-SERVER] Executing time-travel query with as-of:" as-of)
                                    (exec-fn node sql params as-of))
                                (xts/execute-sql node sql params))
                              {:error "No XTDB node available"})
                      ;; Include subscription ID in response
                      result-with-sub (assoc result :subscription-id sub-id)]
                  {:status 200
                   :headers {"Content-Type" "application/json"
                            "Access-Control-Allow-Origin" "*"}
                   :body (json/generate-string result-with-sub)}))))
          
          ;; Override SQL exec to trigger reactive updates
          "/api/sql-exec"
          (bridge/handle-sql-exec-reactive req)
          
          ;; Query history endpoint for time travel
          "/api/query-history"
          (let [body (json/parse-string (slurp (:body req)) true)
                sql (:sql body)
                limit (or (:limit body) 20)
                node @session/default-node]
            (if node
              (let [time-travel (require 'reactor.time-travel-sql)
                    history-fn (ns-resolve 'reactor.time-travel-sql 'get-query-history-range)
                    result (history-fn node sql limit)]
                {:status 200
                 :headers {"Content-Type" "application/json"
                          "Access-Control-Allow-Origin" "*"}
                 :body (json/generate-string result)})
              {:status 500
               :headers {"Content-Type" "application/json"
                        "Access-Control-Allow-Origin" "*"}
               :body (json/generate-string {:error "No XTDB node available"})}))
          
          ;; Test endpoint to manually create subscription
          "/api/test-subscription"
          (do
            (log/info "[TEST] Creating test subscription")
            (let [test-id (kafka/subscribe-query! "test-session" "SELECT * FROM sales" nil)]
              (log/info "[TEST] Created subscription:" test-id)
              (log/info "[TEST] Current active-subs:" @kafka/active-subscriptions)
              {:status 200
               :headers {"Content-Type" "application/json"
                        "Access-Control-Allow-Origin" "*"}
               :body (json/generate-string {:test-id test-id
                                           :active-count (count @kafka/active-subscriptions)})}))
          
          ;; Override regular subscribe to register keypath subscriptions
          "/api/subscribe"
          (http/with-channel req channel
            (let [session-atom (session/get-session session-id)]
              (http/send! channel {:status 200
                                  :headers {"Content-Type" "text/event-stream"
                                           "Cache-Control" "no-cache"
                                           "Access-Control-Allow-Origin" "*"}} false)
              (http/send! channel (str "data: " (json/generate-string @session-atom) "\n\n") false)
              ;; Register this as a keypath subscription
              (kafka/register-keypath-subscription! session-id)
              (add-watch session-atom ::sse
                         (fn [_ _ _ new-state]
                           (http/send! channel (str "data: " (json/generate-string new-state) "\n\n") false)))))
          
          ;; Fall back to base handler
          (base-handler req)))))))

(defn start-reactive!
  "Start a reactive Reactor server with Kafka integration."
  [& {:keys [port handlers session-id-fn init-fn initial-state-fn kafka-config]
      :or {port 4000
           handlers {}
           session-id-fn (constantly "default")
           initial-state-fn (constantly {})}}]
  
  ;; Run any custom initialization FIRST
  (when init-fn (init-fn))
  
  ;; Initialize XTDB (if not already initialized by app)
  (when-not @session/default-node
    (session/init!))
  
  ;; Register all event handlers
  (doseq [[event-id handler] handlers]
    (session/reg-event-db event-id handler))
  
  ;; Initialize meta-tracking system
  (try
    ((requiring-resolve 'reactor.meta-tracking/init!))
    (log/info "Meta-tracking system initialized")
    (catch Exception e
      (log/warn "Meta-tracking not available:" (.getMessage e))))
  
  ;; Initialize Kafka reactive system
  (when kafka-config
    (kafka/init! kafka-config)
    (log/info "Kafka reactive system initialized"))
  
  ;; Create and start server with reactive handler
  (let [reactive-handler (create-reactive-handler :session-id-fn session-id-fn)
        server (http/run-server reactive-handler {:port port})]
    (log/info "🚀 Reactive Reactor server running on http://localhost:" port)
    {:server server
     :port port
     :reactive? true}))

(defn shutdown-reactive!
  "Shutdown the reactive server and Kafka integration."
  []
  (kafka/shutdown!)
  (log/info "Reactive server shutdown complete"))