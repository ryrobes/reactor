(ns reactor.reactive-server
  "Enhanced server with Kafka-based reactive SQL subscriptions."
  (:require [reactor.server :as base-server]
            [reactor.kafka-reactive :as kafka]
            [reactor.sql-reactive-bridge :as bridge]
            [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [reactor.meta-tracking :as meta]
            [reactor.time-travel-sql :as time-travel]
            [reactor.rabbitize :as rabbitize]
            [org.httpkit.server :as http]
            [cheshire.core :as json]
            [clojure.edn :as edn]
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
          
          ;; Handle dynamic paths first
          (cond
           ;; Handle rabbitize endpoints
           (clojure.string/starts-with? path "/api/rabbitize/")
           (base-server/wrap-cors (rabbitize/handle-rabbitize-request req))
           
           ;; Load current session state by ID (dynamic path)
           (and (clojure.string/starts-with? path "/api/session-current/")
                (not (clojure.string/includes? path "/api/session-current//")))
           (let [path-parts (clojure.string/split path #"/")
                 target-session-id (nth path-parts 3 nil)
                 node @session/default-node]
             (log/info "[SESSION-CURRENT] Loading current state for session:" target-session-id)
             (if (and node target-session-id)
               (try
                 ;; Query current state (no temporal clause)
                 (let [where-clause (if @session/app-name
                                     " WHERE session_id = ? AND app_name = ?"
                                     " WHERE session_id = ?")
                       query (str "SELECT * FROM " @session/app-table where-clause)
                       _ (log/info "[SESSION-CURRENT] Executing query:" query)
                       query-result (if @session/app-name
                                     (xts/execute-sql node query target-session-id (name @session/app-name))
                                     (xts/execute-sql node query target-session-id))
                       result (:results query-result)
                       session-row (first result)]
                   (if session-row
                     (try
                       (let [state-str (:state session-row)
                             app-db (if state-str
                                     (edn/read-string state-str)
                                     {})]
                         {:status 200
                          :headers {"Content-Type" "application/json"
                                   "Access-Control-Allow-Origin" "*"}
                          :body (json/generate-string 
                                 {:success true
                                  :session {:session_id target-session-id
                                           :app_name (:app_name session-row)
                                           :app_db app-db
                                           :created_at (:created_at session-row)}})})
                       (catch Exception e
                         (log/error e "Failed to parse session state EDN")
                         {:status 500
                          :headers {"Content-Type" "application/json"
                                   "Access-Control-Allow-Origin" "*"}
                          :body (json/generate-string {:error (str "Failed to parse session state: " (.getMessage e))})}))
                     {:status 404
                      :headers {"Content-Type" "application/json"
                               "Access-Control-Allow-Origin" "*"}
                      :body (json/generate-string {:error (str "Session not found: " target-session-id)})}))
                 (catch Exception e
                   (log/error e "Failed to load session")
                   {:status 500
                    :headers {"Content-Type" "application/json"
                             "Access-Control-Allow-Origin" "*"}
                    :body (json/generate-string {:error (str "Failed to load session: " (.getMessage e))})}))
               {:status 400
                :headers {"Content-Type" "application/json"
                         "Access-Control-Allow-Origin" "*"}
                :body (json/generate-string {:error "Missing session_id"})}))
           
           ;; Load session at specific timestamp (dynamic path)
           (clojure.string/starts-with? path "/api/session-at/")
           (let [path-parts (clojure.string/split path #"/")
                 target-session-id (nth path-parts 3 nil)
                 at-timestamp (nth path-parts 4 nil)
                 node @session/default-node]
             (log/info "[SESSION-AT] Loading session:" target-session-id "at timestamp:" at-timestamp)
             (if (and node target-session-id at-timestamp)
               (try
                 ;; Decode URL-encoded timestamp
                 (let [decoded-timestamp (java.net.URLDecoder/decode at-timestamp "UTF-8")
                       ;; Query the session table with FOR SYSTEM_TIME AS OF
                       ;; Include app_name filter if app-name is set
                       where-clause (if @session/app-name
                                     (str " WHERE session_id = ? AND app_name = ?")
                                     " WHERE session_id = ?")
                       temporal-query (str "SELECT * FROM " @session/app-table 
                                         " FOR SYSTEM_TIME AS OF TIMESTAMP '" decoded-timestamp "'"
                                         where-clause)
                       _ (log/info "[SESSION-AT] Executing temporal query:" temporal-query "with session:" target-session-id)
                       query-result (if @session/app-name
                                     (xts/execute-sql node temporal-query target-session-id (name @session/app-name))
                                     (xts/execute-sql node temporal-query target-session-id))
                       result (:results query-result)
                       session-row (first result)]
                   (if session-row
                     (try
                       (let [state-str (:state session-row)
                             app-db (if state-str
                                     (edn/read-string state-str)
                                     {})]
                         {:status 200
                          :headers {"Content-Type" "application/json"
                                   "Access-Control-Allow-Origin" "*"}
                          :body (json/generate-string 
                                 {:success true
                                  :session {:session_id target-session-id
                                           :app_name (:app_name session-row)
                                           :app_db app-db
                                           :timestamp decoded-timestamp
                                           :created_at (:created_at session-row)}})})
                       (catch Exception e
                         (log/error e "Failed to parse session state EDN")
                         {:status 500
                          :headers {"Content-Type" "application/json"
                                   "Access-Control-Allow-Origin" "*"}
                          :body (json/generate-string {:error (str "Failed to parse session state: " (.getMessage e))})}))
                     {:status 404
                      :headers {"Content-Type" "application/json"
                               "Access-Control-Allow-Origin" "*"}
                      :body (json/generate-string {:error (str "Session not found at timestamp: " decoded-timestamp)})}))
                 (catch Exception e
                   (log/error e "Failed to load session at timestamp")
                   {:status 500
                    :headers {"Content-Type" "application/json"
                             "Access-Control-Allow-Origin" "*"}
                    :body (json/generate-string {:error (str "Failed to load session: " (.getMessage e))})}))
               {:status 400
                :headers {"Content-Type" "application/json"
                         "Access-Control-Allow-Origin" "*"}
                :body (json/generate-string {:error "Missing session_id or timestamp"})}))
           
           ;; Load snapshot by ID (dynamic path)
           (clojure.string/starts-with? path "/api/snapshot/")
           (let [snapshot-id (last (clojure.string/split path #"/"))
                 node @session/default-node]
             (log/info "[SNAPSHOT] Loading snapshot:" snapshot-id)
             (if node
               (try
                 (let [query-result (xts/execute-sql node
                               "SELECT * FROM reactor_snapshots WHERE snapshot_id = ?"
                               snapshot-id)
                       result (:results query-result)  ;; Extract the results array
                       _ (log/info "[SNAPSHOT] Query result count:" (count result))
                       snapshot (first result)
                       _ (when snapshot
                           (log/info "[SNAPSHOT] Snapshot type:" (type snapshot))
                           (log/info "[SNAPSHOT] Snapshot keys:" (keys snapshot)))]
                   (if snapshot
                     (try
                       (let [state-str (:state snapshot)
                             _ (log/info "[SNAPSHOT] State string length:" (count state-str))
                             app-db (if state-str
                                     (edn/read-string state-str)
                                     {})]
                         {:status 200
                          :headers {"Content-Type" "application/json"
                                   "Access-Control-Allow-Origin" "*"}
                          :body (json/generate-string 
                                 {:success true
                                  :snapshot {:snapshot_id (:snapshot_id snapshot)
                                           :app_name (:app_name snapshot)
                                           :session_id (:session_id snapshot)
                                           :app_db app-db
                                           :description (:description snapshot)
                                           :created_at (:created_at snapshot)}})})
                       (catch Exception e
                         (log/error e "Failed to parse snapshot EDN")
                         {:status 500
                          :headers {"Content-Type" "application/json"
                                   "Access-Control-Allow-Origin" "*"}
                          :body (json/generate-string {:error (str "Failed to parse snapshot: " (.getMessage e))})}))
                     {:status 404
                      :headers {"Content-Type" "application/json"
                               "Access-Control-Allow-Origin" "*"}
                      :body (json/generate-string {:error "Snapshot not found"})}))
                 (catch Exception e
                   (log/error e "Failed to load snapshot")
                   {:status 500
                    :headers {"Content-Type" "application/json"
                             "Access-Control-Allow-Origin" "*"}
                    :body (json/generate-string {:error (str "Failed to load snapshot: " (.getMessage e))})}))
               {:status 503
                :headers {"Content-Type" "application/json"
                         "Access-Control-Allow-Origin" "*"}
                :body (json/generate-string {:error "No XTDB node available"})}))
           
           :else
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
            ;; Track the SQL query event
            (meta/track-event! "sql-query" "query" 
                              {:sql sql :params params :as-of as-of} 
                              session-id)
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
              (let [;; For temporal queries, generate consistent ID based on base query
                    ;; This ensures temporal queries at different times can share cache
                    is-temporal-query? (and (string? sql) 
                                           (re-find #"FOR\s+SYSTEM_TIME\s+AS\s+OF" sql))
                    base-query-for-id (if is-temporal-query?
                                        ;; Extract base query without temporal clause for consistent ID
                                        (if-let [match (re-find #"^(.*?)\s+FOR\s+SYSTEM_TIME\s+AS\s+OF\s+TIMESTAMP\s+'[^']+'(.*)$" sql)]
                                          (str (second match) (nth match 2))
                                          sql)
                                        sql)
                    ;; Generate consistent client-id for temporal queries
                    generated-client-id (if is-temporal-query?
                                         (str "temporal-" (hash base-query-for-id))
                                         nil)
                    ;; Always use client-provided ID when available
                    client-id (or (:subscription-id body) generated-client-id)
                    sub-id (or client-id (str "sub-" (java.util.UUID/randomUUID)))
                    
                    ;; Check if this subscription already exists
                    existing-sub (get @kafka/active-subscriptions sub-id)]
                
                ;; Unregister old subscription if it exists (allows SQL updates)
                ;; BUT for temporal queries with same base query, this might clear cache!
                (when existing-sub
                  (log/info "[REACTIVE-SERVER] Updating existing subscription:" sub-id
                           (when is-temporal-query? " (TEMPORAL)")
                           "\n  Old SQL:" (:query existing-sub)
                           "\n  New SQL:" sql)
                  ;; Only unregister if SQL actually changed (not just timestamp)
                  (when (not= base-query-for-id 
                             (if-let [match (re-find #"^(.*?)\s+FOR\s+SYSTEM_TIME\s+AS\s+OF\s+TIMESTAMP\s+'[^']+'(.*)$" 
                                                     (:query existing-sub))]
                               (str (second match) (nth match 2))
                               (:query existing-sub)))
                    (kafka/unregister-query-subscription! sub-id)))
                
                ;; Register new/updated subscription
                (log/info "[REACTIVE-SERVER] Registering subscription:" sub-id 
                         (when is-temporal-query? 
                           (str " (TEMPORAL with consistent ID: " generated-client-id ")"))
                         "\n  SQL:" (if (> (count sql) 150)
                                     (str (subs sql 0 150) "...")
                                     sql)
                         (when is-temporal-query?
                           (str "\n  Base query for cache: " base-query-for-id)))
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
              (let [result (time-travel/get-query-history-range node sql limit)]
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
          
          ;; Snapshot endpoints for saving/loading app-db states
          "/api/snapshot"
          (case method
            :post
            ;; Save a snapshot
            (let [body (json/parse-string (slurp (:body req)) true)
                  node @session/default-node]
              (if node
                (try
                  (let [snapshot-id (or (:snapshot_id body) 
                                       (str "snapshot-" (System/currentTimeMillis)))
                        app-name (or (:app_name body) "unknown")
                        app-db (:app_db body)
                        saved-session-id (or (:session_id body) session-id)  ;; Use session from body if provided
                        description (or (:description body) "")
                        timestamp (java.time.Instant/now)]
                    ;; Store as EDN, just like sessions
                    (xts/execute-sql node
                      "INSERT INTO reactor_snapshots 
                       (_id, snapshot_id, app_name, session_id, state, description, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)"
                      snapshot-id snapshot-id app-name saved-session-id 
                      (pr-str app-db) description timestamp)
                    {:status 200
                     :headers {"Content-Type" "application/json"
                              "Access-Control-Allow-Origin" "*"}
                     :body (json/generate-string {:success true 
                                                :snapshot_id snapshot-id})})
                  (catch Exception e
                    (log/error e "Failed to save snapshot")
                    {:status 500
                     :headers {"Content-Type" "application/json"
                              "Access-Control-Allow-Origin" "*"}
                     :body (json/generate-string {:error (str "Failed to save snapshot: " (.getMessage e))})}))
                {:status 503
                 :headers {"Content-Type" "application/json"
                          "Access-Control-Allow-Origin" "*"}
                 :body (json/generate-string {:error "No XTDB node available"})}))
            
            :get
            ;; Load a snapshot  
            (let [snapshot-id (get-in req [:params :id])
                  node @session/default-node]
              (if node
                (try
                  (let [query-result (xts/execute-sql node
                                "SELECT * FROM reactor_snapshots WHERE snapshot_id = ?"
                                snapshot-id)
                        result (:results query-result)  ;; Extract the results array
                        snapshot (first result)]
                    (if snapshot
                      (try
                        (let [state-str (:state snapshot)
                              app-db (if state-str
                                      (edn/read-string state-str)
                                      {})]
                          {:status 200
                           :headers {"Content-Type" "application/json"
                                    "Access-Control-Allow-Origin" "*"}
                           :body (json/generate-string 
                                  {:success true
                                   :snapshot {:snapshot_id (:snapshot_id snapshot)
                                            :app_name (:app_name snapshot)
                                            :session_id (:session_id snapshot)
                                            :app_db app-db
                                            :description (:description snapshot)
                                            :created_at (:created_at snapshot)}})})
                        (catch Exception e
                          (log/error e "Failed to parse snapshot EDN in GET")
                          {:status 500
                           :headers {"Content-Type" "application/json"
                                    "Access-Control-Allow-Origin" "*"}
                           :body (json/generate-string {:error (str "Failed to parse snapshot: " (.getMessage e))})}))
                      {:status 404
                       :headers {"Content-Type" "application/json"
                                "Access-Control-Allow-Origin" "*"}
                       :body (json/generate-string {:error "Snapshot not found"})}))
                  (catch Exception e
                    (log/error e "Failed to load snapshot")
                    {:status 500
                     :headers {"Content-Type" "application/json"
                              "Access-Control-Allow-Origin" "*"}
                     :body (json/generate-string {:error (str "Failed to load snapshot: " (.getMessage e))})}))
                {:status 503
                 :headers {"Content-Type" "application/json"
                          "Access-Control-Allow-Origin" "*"}
                 :body (json/generate-string {:error "No XTDB node available"})}))
            
            ;; Default for other methods
            {:status 405
             :headers {"Content-Type" "application/json"
                      "Access-Control-Allow-Origin" "*"}
             :body (json/generate-string {:error "Method not allowed"})})
          
          ;; TAP endpoint for inserting tap entries
          "/api/tap"
          (case method
            :post
            (let [body (json/parse-string (slurp (:body req)) true)
                  node @session/default-node]
              (if node
                (try
                  (let [tap-id (str "tap-" (java.util.UUID/randomUUID))
                        timestamp (java.time.Instant/now)
                        value-edn (:value body)
                        caller (or (:caller body) "anonymous")
                        platform (or (:platform body) "CLJS")
                        session-id (or (:session-id body) session-id)
                        value-type (cond
                                    (re-find #"^\{" value-edn) "map"
                                    (re-find #"^\[" value-edn) "vector"
                                    (re-find #"^#\{" value-edn) "set"
                                    (re-find #"^\(" value-edn) "list"
                                    (re-find #"^\"" value-edn) "string"
                                    (re-find #"^-?\d" value-edn) "number"
                                    (re-find #"^true|^false" value-edn) "boolean"
                                    (re-find #"^nil" value-edn) "nil"
                                    (re-find #"^:" value-edn) "keyword"
                                    :else "other")]
                    (xts/execute-sql node
                      "INSERT INTO reactor_taps (_id, value_edn, caller, platform, created_at, session_id, value_type)
                       VALUES (?, ?, ?, ?, ?, ?, ?)"
                      tap-id value-edn caller platform timestamp session-id value-type)
                    {:status 200
                     :headers {"Content-Type" "application/json"
                                "Access-Control-Allow-Origin" "*"}
                     :body (json/generate-string {:success true :id tap-id})})
                  (catch Exception e
                    (log/error e "Failed to insert tap entry")
                    {:status 500
                     :headers {"Content-Type" "application/json"
                              "Access-Control-Allow-Origin" "*"}
                     :body (json/generate-string {:error (str "Failed to insert tap: " (.getMessage e))})}))
                {:status 503
                 :headers {"Content-Type" "application/json"
                          "Access-Control-Allow-Origin" "*"}
                 :body (json/generate-string {:error "No XTDB node available"})}))
            
            ;; GET for fetching tap entries
            :get
            (let [node @session/default-node
                  limit (Integer/parseInt (get-in req [:params :limit] "100"))]
              (if node
                (try
                  (let [results (xts/execute-sql node
                                  (str "SELECT * FROM reactor_taps "
                                       "ORDER BY created_at DESC "
                                       "LIMIT " limit))]
                    {:status 200
                     :headers {"Content-Type" "application/json"
                              "Access-Control-Allow-Origin" "*"}
                     :body (json/generate-string results)})
                  (catch Exception e
                    (log/error e "Failed to fetch tap entries")
                    {:status 500
                     :headers {"Content-Type" "application/json"
                              "Access-Control-Allow-Origin" "*"}
                     :body (json/generate-string {:error (str "Failed to fetch taps: " (.getMessage e))})}))
                {:status 503
                 :headers {"Content-Type" "application/json"
                          "Access-Control-Allow-Origin" "*"}
                 :body (json/generate-string {:error "No XTDB node available"})})))
          
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
          (base-handler req))))))))

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