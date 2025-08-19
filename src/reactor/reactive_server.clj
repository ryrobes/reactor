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
            ;; Only create subscription for non-session queries
            (if is-session-query?
              ;; Just execute without subscription for session tables
              (let [node @session/default-node
                    result (if node
                            (if as-of
                              (session/execute-sql-query node sql params as-of)
                              (xts/execute-sql node sql params))
                            {:error "No XTDB node available"})]
                {:status 200
                 :headers {"Content-Type" "application/json"
                          "Access-Control-Allow-Origin" "*"}
                 :body (json/generate-string result)})
              ;; Create subscription for business tables
              (let [;; Use client-provided subscription-id if available
                    client-sub-id (:subscription-id body)
                    sub-id (if client-sub-id
                            ;; Register with client's ID
                            (do (log/info "[REACTIVE-SERVER] Registering client subscription:" client-sub-id "for SQL:" sql)
                                (kafka/register-query-subscription! 
                                 client-sub-id sql params 
                                 (kafka/create-subscription-callback session-id) 
                                 session-id)
                                ;; Execute immediately to get initial results
                                (kafka/re-execute-subscription client-sub-id)
                                client-sub-id)
                            ;; Create new subscription
                            (kafka/subscribe-query! session-id sql params))]
                (log/info "[REACTIVE-SERVER] Created/registered subscription" sub-id "for session" session-id)
                (log/info "[REACTIVE-SERVER] Active SSE channels for session:" (count (get @kafka/sse-channels session-id [])))
                ;; Return initial results
                (let [node @session/default-node
                      result (if node
                              (if as-of
                                (session/execute-sql-query node sql params as-of)
                                (xts/execute-sql node sql params))
                              {:error "No XTDB node available"})]
                  {:status 200
                   :headers {"Content-Type" "application/json"
                            "Access-Control-Allow-Origin" "*"}
                   :body (json/generate-string result)}))))
          
          ;; Override SQL exec to trigger reactive updates
          "/api/sql-exec"
          (bridge/handle-sql-exec-reactive req)
          
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