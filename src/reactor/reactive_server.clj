(ns reactor.reactive-server
  "Enhanced server with Kafka-based reactive SQL subscriptions."
  (:require [reactor.server :as base-server]
            [reactor.kafka-reactive :as kafka]
            [reactor.sql-reactive-bridge :as bridge]
            [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [reactor.meta-tracking :as meta]
            [reactor.time-travel-sql :as time-travel]
            [reactor.sql-transform :as sql-transform]
            [reactor.sql-template :as sql-template]
            [reactor.rabbitize :as rabbitize]
            [org.httpkit.server :as http]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go <!]]))

;; Track currently cascading blocks to prevent infinite loops
;; This should track the ENTIRE chain, not just individual blocks
(defonce cascade-chain (atom #{}))

;; CRITICAL: Block SQL cache - tracks both raw (template) and resolved SQL for every block execution
;; This provides a reliable source of truth for template resolution, avoiding session state issues
;; Structure: {block-id {:raw-sql "SELECT * FROM {{parent.sql}}"
;;                        :resolved-sql "SELECT * FROM (SELECT * FROM sales)"
;;                        :updated-at timestamp
;;                        :session-id "session-123"}}
(defonce block-sql-cache (atom {}))

(defn update-block-sql-cache! 
  "Update the block SQL cache with both raw and resolved SQL"
  [block-id raw-sql resolved-sql session-id]
  (let [old-entry (get @block-sql-cache block-id)
        entry {:raw-sql raw-sql
               :resolved-sql resolved-sql
               :updated-at (System/currentTimeMillis)
               :session-id session-id}]
    (swap! block-sql-cache assoc block-id entry)
    ;; Verbose cache logging disabled for performance
    #_(log/debug "[BLOCK-SQL-CACHE] 🔄 Updated cache for block:" block-id
                "\n  Previous raw SQL:" (when old-entry
                                         (if (> (count (:raw-sql old-entry)) 50)
                                           (str (subs (:raw-sql old-entry) 0 50) "...")
                                           (:raw-sql old-entry)))
                "\n  New raw SQL:" (if (> (count raw-sql) 50)
                                    (str (subs raw-sql 0 50) "...")
                                    raw-sql)
                "\n  Raw SQL changed?" (not= (:raw-sql old-entry) raw-sql)
                "\n  Previous resolved SQL:" (when old-entry
                                             (if (> (count (:resolved-sql old-entry)) 50)
                                               (str (subs (:resolved-sql old-entry) 0 50) "...")
                                               (:resolved-sql old-entry)))
                "\n  New resolved SQL:" (when (not= raw-sql resolved-sql)
                                        (if (> (count resolved-sql) 50)
                                          (str (subs resolved-sql 0 50) "...")
                                          resolved-sql))
                "\n  Resolved SQL changed?" (not= (:resolved-sql old-entry) resolved-sql)
                "\n  Session:" session-id
                "\n  Time since last update:" (when old-entry
                                               (str (- (System/currentTimeMillis) (:updated-at old-entry)) "ms")))
                                               ))

(defn get-block-sql-from-cache
  "Get the latest SQL for a block from the cache"
  [block-id]
  (get @block-sql-cache block-id))
(defonce cascade-depth (atom 0))
(def max-cascade-depth 3)

;; Reset cascade tracking periodically to prevent memory issues
(defonce cascade-reset-task
  (future
    (while true
      (Thread/sleep 30000) ; Every 30 seconds
      (when (and (zero? @cascade-depth) 
                 (seq @cascade-chain))
        (log/debug "[CASCADE] Resetting cascade chain, was:" (count @cascade-chain) "items")
        (reset! cascade-chain #{})))))

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
                    "Access-Control-Allow-Headers" "Content-Type, Authorization, X-Session-ID, x-session-id"
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
           
           ;; Serve individual theme files (dynamic path)
           (clojure.string/starts-with? path "/api/themes/")
           (let [theme-name (last (clojure.string/split path #"/"))]
             (try
               (let [theme-file (clojure.java.io/file "themes" theme-name)]
                 (if (.exists theme-file)
                   {:status 200
                    :headers {"Content-Type" "application/edn"
                             "Access-Control-Allow-Origin" "*"}
                    :body (slurp theme-file)}
                   {:status 404
                    :headers {"Content-Type" "application/json"
                             "Access-Control-Allow-Origin" "*"}
                    :body (json/generate-string {:error "Theme not found"})}))
               (catch Exception e
                 (log/error e "Failed to load theme:" theme-name)
                 {:status 500
                  :headers {"Content-Type" "application/json"
                           "Access-Control-Allow-Origin" "*"}
                  :body (json/generate-string {:error (str "Failed to load theme: " (.getMessage e))})})))
           
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
           (do (case path
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
                original-sql (:sql body)
                params (:params body)
                as-of (:as-of body)
                block-id (:block_id body) ; The block ID of the query being executed
                ;; Extract session from query params if provided (client sends ?session=xxx)
                query-params (when-let [query-string (:query-string req)]
                               (into {} (map #(clojure.string/split % #"=") 
                                           (clojure.string/split query-string #"&"))))
                actual-session-id (or (get query-params "session") session-id)
                _ (when (not= actual-session-id session-id)
                    (log/info "[REACTIVE-SERVER] Using session from query params:" actual-session-id "instead of default:" session-id))
                ;; Get session state for template resolution  
                session-state (when actual-session-id
                               (when-let [session (session/get-session actual-session-id)]
                                 (let [state (session/get-state session)]
                                   (log/info "[REACTIVE-SERVER] Session state for template resolution:" 
                                            {:has-state? (boolean state)
                                             :canvas-blocks (keys (get-in state [:canvas :blocks]))})
                                   ;; CRITICAL FIX: Update session state with current block's SQL if block_id is provided
                                   ;; This ensures the session has the latest SQL before template resolution
                                   (if (and block-id original-sql)
                                     (let [old-sql (get-in state [:canvas :blocks (keyword block-id) :sql])
                                           updated-state (-> state
                                                           (assoc-in [:canvas :blocks (keyword block-id) :sql] original-sql)
                                                           ;; Also update string version if needed
                                                           (assoc-in [:canvas :blocks block-id :sql] original-sql))]
                                       (session/set-state! session updated-state)
                                       (log/info "[REACTIVE-SERVER] Updated session with block SQL BEFORE query execution:"
                                                "\n  Block ID:" block-id
                                                "\n  Old SQL:" (when old-sql
                                                               (if (> (count old-sql) 50)
                                                                 (str (subs old-sql 0 50) "...")
                                                                 old-sql))
                                                "\n  New SQL:" (if (> (count original-sql) 50)
                                                             (str (subs original-sql 0 50) "...")
                                                             original-sql)
                                                "\n  SQL changed?" (not= old-sql original-sql))
                                       ;; Return the updated state for use in this request
                                       updated-state)
                                     ;; Return original state if no update needed
                                     state))))
                ;; Check for template references
                has-templates? (re-find #"\{\{[^}]+\.sql\}\}" original-sql)
                _ (log/info "[REACTIVE-SERVER] Template check:" 
                           {:has-templates? has-templates?
                            :has-session-state? (boolean session-state)
                            :original-sql original-sql})
                ;; Resolve any template references in the SQL and track dependencies
                template-result (if (and session-state has-templates?)
                                   (do
                                     (log/info "[REACTIVE-SERVER] Resolving SQL templates in query")
                                     (let [result (sql-template/resolve-sql-templates-with-deps original-sql session-state)]
                                       (log/info "[REACTIVE-SERVER] Template resolution result:" 
                                                {:resolved-sql (:sql result)
                                                 :dependencies (:dependencies result)
                                                 :changed? (not= (:sql result) original-sql)})
                                       result))
                                   {:sql original-sql :dependencies []})
                sql (:sql template-result)
                parent-block-ids (:dependencies template-result)
                ;; Update block SQL cache with both raw and resolved SQL
                _ (when block-id
                    (go (update-block-sql-cache! block-id original-sql sql actual-session-id)))
                ;; Check if this query is for a session table
                is-session-query? (or (re-find #"(?i)FROM\s+\w*_?sessions" sql)
                                    (re-find #"(?i)INTO\s+\w*_?sessions" sql))
                ;; Determine if this is truly temporal (historical) or a "NOW" query
                ;; If the as-of timestamp is within 30 seconds of now, treat it as a "NOW" query
                ;; is-truly-temporal? (when (and as-of (not (empty? as-of)))
                ;;                      (try
                ;;                        (let [as-of-time (.getTime (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                ;;                                                  (.parse (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") as-of))
                ;;                              now-time (System/currentTimeMillis)
                ;;                              diff-seconds (/ (Math/abs (- now-time as-of-time)) 1000)]
                ;;                          ;; If more than 30 seconds old, it's truly temporal
                ;;                          (> diff-seconds 30))
                ;;                        (catch Exception e
                ;;                          ;; If we can't parse, assume it's temporal
                ;;                          true)))
                is-truly-temporal? false]
            (log/debug "[REACTIVE-SERVER] /api/sql called with SQL:" sql)
            (log/debug "[REACTIVE-SERVER] Session ID:" session-id "Has SSE channels:" (seq (get @kafka/sse-channels session-id)))
            (log/info "[REACTIVE-SERVER] as-of value:" (pr-str as-of) 
                     "is-temporal?" (boolean is-truly-temporal?)
                     "(>30s old)")
            ;; Track the SQL query event (but not for cascade-triggered queries)
            (when-not (:cascade-triggered body)
              (meta/track-event! "sql-query" "query" 
                                {:sql sql :params params :as-of as-of} 
                                session-id))
            ;; Create subscriptions for ALL queries (including temporal) to enable diffing
            ;; Session queries bypass subscriptions UNLESS they're temporal (which benefit from diffing)
            (if (and is-session-query? (not as-of))
              ;; Non-temporal session queries: just execute without subscription
              (let [node @session/default-node
                    result (if node
                              (xts/execute-sql node sql params)
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
              (let [;; IMPORTANT: Use resolved SQL for adding temporal clause, but store original for subscriptions
                    ;; This ensures temporal clause is added to the correct (resolved) SQL
                    sql-for-temporal (if has-templates? 
                                       sql  ;; Use resolved SQL for temporal clause
                                       original-sql)  ;; Use original if no templates
                    sql-with-temporal (if (and as-of (not (re-find #"FOR\s+SYSTEM_TIME\s+AS\s+OF" sql-for-temporal)))
                                        (let [parser-ns (require 'reactor.sql-parser)
                                              add-clause-fn (ns-resolve 'reactor.sql-parser 'add-as-of-clause)]
                                          (add-clause-fn sql-for-temporal as-of))
                                        sql-for-temporal)
                    ;; For temporal queries, generate consistent ID based on base query
                    ;; This ensures temporal queries at different times can share cache
                    is-temporal-query? (and (string? sql-with-temporal) 
                                           (re-find #"FOR\s+SYSTEM_TIME\s+AS\s+OF" sql-with-temporal))
                    base-query-for-id (if is-temporal-query?
                                        ;; Extract base query without temporal clause for consistent ID
                                        ;; Use the ORIGINAL sql for base query (before adding temporal clause)
                                        sql
                                        sql-with-temporal)
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
                         "\n  SQL:" (if (> (count sql-with-temporal) 150)
                                     (str (subs sql-with-temporal 0 150) "...")
                                     sql-with-temporal)
                         (when is-temporal-query?
                           (str "\n  Base query for cache: " base-query-for-id)))
                ;; CRITICAL FIX: Store ORIGINAL SQL with templates, not resolved SQL
                ;; This ensures templates are re-resolved on each execution with fresh parent SQL
                ;; For template queries with temporal clause, we need to add the clause properly
                (let [sql-to-store (if has-templates?
                                    ;; For template queries, add temporal clause to original SQL properly
                                    (if (and as-of (not (re-find #"FOR\s+SYSTEM_TIME\s+AS\s+OF" original-sql)))
                                      (let [parser-ns (require 'reactor.sql-parser)
                                            add-clause-fn (ns-resolve 'reactor.sql-parser 'add-as-of-clause)]
                                        (add-clause-fn original-sql as-of))
                                      original-sql)
                                    ;; For non-template queries, use the already processed SQL
                                    sql-with-temporal)]
                  (log/info "[REACTIVE-SERVER] Storing subscription SQL:"
                           "\n  Has templates?" has-templates?
                           "\n  SQL to store:" (if (> (count sql-to-store) 150)
                                                 (str (subs sql-to-store 0 150) "...")
                                                 sql-to-store))
                  (kafka/register-query-subscription! 
                   sub-id 
                   sql-to-store
                   params 
                   (kafka/create-subscription-callback actual-session-id) 
                   actual-session-id
                   nil  ;; client-id
                   is-truly-temporal?  ;; Pass temporal flag - only true for >30s old timestamps
                   parent-block-ids))  ;; Pass parent block dependencies - close kafka/register and let
                
                ;; Execute immediately to get initial results
                (kafka/re-execute-subscription sub-id)
                (log/info "[REACTIVE-SERVER] Using subscription" sub-id "for session" actual-session-id)
                (log/info "[REACTIVE-SERVER] Active SSE channels for session:" (count (get @kafka/sse-channels actual-session-id [])))
                ;; Return initial results WITH the subscription ID
                (let [node @session/default-node
                      ;; For temporal queries, we've already added the clause and executed via subscription
                      ;; Just get the result from the subscription execution
                      result (if node
                              (if as-of
                                ;; The subscription has already been executed, but we need to return the result
                                ;; Execute again with original SQL (without temporal clause) for the response
                                (let [time-travel-ns (require 'reactor.time-travel-sql)
                                      exec-fn (ns-resolve 'reactor.time-travel-sql 'execute-sql-with-time-travel)]
                                  (log/info "[REACTIVE-SERVER] Executing time-travel query with as-of:" as-of)
                                  (exec-fn node sql params as-of))
                                (xts/execute-sql node sql-with-temporal params))
                              {:error "No XTDB node available"})
                      ;; Include subscription ID in response
                      result-with-sub (assoc result :subscription-id sub-id)]
                  
                  ;; Trigger cascade execution for dependent blocks and subscriptions
                  ;; ONLY if this is NOT already a cascade-triggered execution
                  (let [cascade-id (or block-id 
                                       ;; Try to extract block ID from subscription ID 
                                       ;; (which might be :block-uuid or block-uuid format)
                                       (when (:subscription-id body)
                                         (:subscription-id body)))
                        is-cascade-triggered? (:cascade-triggered body)]
                    (when (and cascade-id 
                               (not (:error result))
                               (not is-cascade-triggered?) ; Don't cascade from cascade-triggered executions
                               (< @cascade-depth max-cascade-depth)) ; Depth limit
                      ;; Check if this block is already in the cascade chain
                      (let [clean-cascade-id (cond
                                               (keyword? cascade-id) (name cascade-id)
                                               (string? cascade-id) (if (str/starts-with? cascade-id ":")
                                                                     (subs cascade-id 1)
                                                                     cascade-id)
                                               :else (str cascade-id))]
                        (if (contains? @cascade-chain clean-cascade-id)
                          (log/warn "[CASCADE] Skipping cascade for" clean-cascade-id "- already in cascade chain (loop prevention)")
                          (future ; Execute cascades asynchronously to not block response
                            (swap! cascade-chain conj clean-cascade-id)
                            (swap! cascade-depth inc)
                            (try
                              ;; Find and trigger dependent subscriptions that reference this block
                              (let [_ (log/debug "[CASCADE] Starting cascade for block:" clean-cascade-id 
                                              "\n  Depth:" @cascade-depth "/" max-cascade-depth
                                              "\n  Chain size:" (count @cascade-chain)
                                              "\n  Total active subscriptions:" (count @kafka/active-subscriptions)
                                              "\n  Dependency map entries:" (count @kafka/subscription-dependencies))
                                    ;; Use dependency tracking instead of searching SQL strings
                                    dependent-subs (get @kafka/subscription-dependencies clean-cascade-id #{})
                                    ;; Also check for keyword version of the ID
                                    keyword-deps (get @kafka/subscription-dependencies (keyword clean-cascade-id) #{})
                                    ;; Combine both sets
                                    all-deps (into dependent-subs keyword-deps)
                                    ;; Limit to prevent explosion
                                    limited-deps (take 10 all-deps)]
                                (if (seq limited-deps)
                                  (do
                                    (log/debug "[CASCADE] Found" (count all-deps) "dependent subscriptions for block" clean-cascade-id
                                             (when (> (count all-deps) 10) 
                                               (str " (limiting to 10)"))
                                             ":" limited-deps)
                                    ;; Trigger debounced re-execution for each dependent subscription
                                    (doseq [sub-id limited-deps]
                                      (when-not (contains? @cascade-chain sub-id)
                                        (log/debug "[CASCADE] Requesting re-execution of subscription:" sub-id)
                                        (swap! cascade-chain conj sub-id)
                                        (swap! kafka/pending-re-executions assoc sub-id (System/currentTimeMillis)))))
                                  (log/debug "[CASCADE] No dependent subscriptions found for block" clean-cascade-id)))
                        
                        ;; Also handle session-state blocks if available
                        ;; IMPORTANT: Fetch fresh session state from the store for cascade
                        ;; This ensures we get the latest state including any updates from above
                        (when-let [fresh-session (session/get-session actual-session-id)]
                          (let [;; Get the latest session state from the store
                                fresh-session-state (session/get-state fresh-session)
                                _ (log/debug "[CASCADE] Fetched fresh session state for cascade:"
                                           "\n  Block being cascaded:" block-id
                                           "\n  Has fresh state?" (boolean fresh-session-state)
                                           "\n  Parent block SQL in fresh state:" 
                                           (when (and block-id fresh-session-state)
                                             (let [parent-sql (get-in fresh-session-state [:canvas :blocks (keyword block-id) :sql])]
                                               (if (> (count (str parent-sql)) 100)
                                                 (str (subs parent-sql 0 100) "...")
                                                 parent-sql))))
                                ;; Use the fresh state which already has the updated SQL from above
                                updated-session-state fresh-session-state
                                ;; Use get-cascade-chain which already handles circular dependencies
                                dependent-blocks (sql-template/get-cascade-chain updated-session-state block-id)
                                ;; Remove duplicates to prevent multiple executions of same block
                                unique-deps (distinct dependent-blocks)]
                            (when (seq unique-deps)
                              (log/debug "[CASCADE] Found dependent blocks for" block-id ":" unique-deps
                                       (when (and block-id original-sql)
                                         "\n  Updated parent SQL in session-state for correct template resolution"))
                              ;; Small delay to ensure parent query is fully processed
                              (Thread/sleep 50)
                            ;; Process all dependent blocks in parallel with deduplication
                            (let [futures (doall
                                          (for [dep-block-id unique-deps]
                                            (future
                                              (try
                                                (log/debug "[CASCADE] Processing dependent block:" dep-block-id)
                                                (let [dep-block-sql-kw (get-in updated-session-state [:canvas :blocks (keyword dep-block-id) :sql])
                                                      dep-block-sql-str (get-in updated-session-state [:canvas :blocks dep-block-id :sql])
                                                      dep-block-sql (or dep-block-sql-kw dep-block-sql-str)]
                                                  (if-not dep-block-sql
                                                    (log/warn "[CASCADE] ⚠️ SKIPPING dependent block - SQL not found in session state!"
                                                             "\n  Block ID:" dep-block-id
                                                             "\n  Tried keys:" [(keyword dep-block-id) dep-block-id]
                                                             "\n  Available blocks in session:" (keys (get-in updated-session-state [:canvas :blocks])))
                                                    ;; Process the block if SQL was found
                                                    ;; Find ALL subscriptions for this block, not just by assumed ID
                                                    ;; Some subscriptions might have been created with different ID formats
                                                    (let [;; First try the expected subscription ID format
                                                        expected-sub-id (keyword dep-block-id)
                                                        ;; Log all subscription IDs for debugging
                                                        all-sub-ids (keys @kafka/active-subscriptions)
                                                        _ (log/debug "[CASCADE] Looking for subscription for block:" dep-block-id
                                                                   "\n  Expected ID:" expected-sub-id
                                                                   "\n  Total active subscriptions:" (count all-sub-ids)
                                                                   "\n  All subscription IDs:" (take 10 all-sub-ids)
                                                                   (when (> (count all-sub-ids) 10) 
                                                                     (str " ... and " (- (count all-sub-ids) 10) " more")))
                                                        ;; Find all subscriptions that match this block
                                                        ;; Check both exact ID match and subscriptions that contain the block ID
                                                        ;; Also categorize subscription types for debugging
                                                        _ (let [sql-subs (filter #(str/starts-with? (str (key %)) "sql-") @kafka/active-subscriptions)
                                                                block-subs (filter #(not (str/starts-with? (str (key %)) "sql-")) @kafka/active-subscriptions)]
                                                            (when (> (count all-sub-ids) 50) ; Only log when there are many subs
                                                              (log/debug "[CASCADE] Subscription breakdown:"
                                                                       "\n  Random SQL subs (sql-UUID):" (count sql-subs)
                                                                       "\n  Block/Named subs:" (count block-subs)
                                                                       "\n  Sample SQL sub IDs:" (take 3 (map first sql-subs)))))
                                                        matching-subs (filter (fn [[sub-id sub-info]]
                                                                              (or 
                                                                               ;; Exact match with keyword version
                                                                               (= sub-id expected-sub-id)
                                                                               ;; String version match  
                                                                               (= sub-id dep-block-id)
                                                                               ;; Contains block ID in the subscription ID
                                                                               (and (string? (str sub-id))
                                                                                    (str/includes? (str sub-id) dep-block-id))))
                                                                            @kafka/active-subscriptions)
                                                        _ (log/debug "[CASCADE] Found" (count matching-subs) "matching subscription(s) for block" dep-block-id
                                                                   (when (seq matching-subs)
                                                                     (str "\n  Matching IDs: " (map first matching-subs))))
                                                        ;; Get the most recent/relevant subscription
                                                        existing-sub-entry (first matching-subs)
                                                        existing-sub (second existing-sub-entry)
                                                        actual-sub-id (if existing-sub-entry 
                                                                       (first existing-sub-entry)
                                                                       expected-sub-id) ; Use expected ID for new subscriptions
                                                        _ (when existing-sub-entry
                                                            (log/debug "[CASCADE] Using subscription:"
                                                                     "\n  ID:" actual-sub-id
                                                                     "\n  Different from expected?" (not= actual-sub-id expected-sub-id)
                                                                     "\n  Has SQL?" (boolean (:query existing-sub))
                                                                     "\n  SQL length:" (when (:query existing-sub) (count (:query existing-sub)))))
                                                          ;; Templates are resolved at execution time now
                                                          ;; No need to resolve here
                                                          ]
                                                    
                                                    ;; Only update if SQL has changed or subscription doesn't exist
                                                    (if (and existing-sub 
                                                             (= (:query existing-sub) dep-block-sql))  ;; Compare template SQL
                                                      (do
                                                        (log/debug "[CASCADE] Subscription already exists with same template SQL, requesting re-execution:" actual-sub-id)
                                                        ;; Just trigger re-execution - templates will be resolved at execution time
                                                        (swap! kafka/pending-re-executions assoc actual-sub-id (System/currentTimeMillis)))
                                                      (do
                                                        (log/debug "[CASCADE] Creating/updating subscription for dependent block:" dep-block-id
                                                                 "\n  Actual subscription ID:" actual-sub-id
                                                                 "\n  SQL changed:" (boolean existing-sub)
                                                                 "\n  Template SQL:" (if (> (count dep-block-sql) 150)
                                                                                      (str (subs dep-block-sql 0 150) "...")
                                                                                      dep-block-sql))
                                                        ;; Unregister existing subscription if it exists
                                                        (when existing-sub
                                                          (log/debug "[CASCADE] Unregistering old subscription:" actual-sub-id)
                                                          (kafka/unregister-query-subscription! actual-sub-id))
                                                        ;; Register new subscription with TEMPLATE SQL (not resolved)
                                                        (let [cascade-session-id (or (:session-id existing-sub) actual-session-id)]
                                                          (log/debug "[CASCADE] Registering new subscription with template SQL:" actual-sub-id)
                                                          (kafka/register-query-subscription! 
                                                           actual-sub-id dep-block-sql []  ;; Store template SQL
                                                           (kafka/create-subscription-callback cascade-session-id) 
                                                           cascade-session-id
                                                           nil
                                                           false
                                                           []))
                                                        ;; Use debounced execution for new subscriptions too
                                                        (swap! kafka/pending-re-executions assoc actual-sub-id (System/currentTimeMillis))))))  ; Close the has-templates let and matching-subs let
                                                  ) ; Close the if-not and outer let
                                                (catch Exception e
                                                  (log/error "[CASCADE] Error processing dependent block" dep-block-id ":" (.getMessage e)))))))]
                              ;; Wait for all cascades to complete (with timeout)  
                              (doseq [f futures]
                                (deref f 5000 nil)))))) ; 5 second timeout per future, extra ) for session-state when
                        (catch Exception e
                              (log/error "[CASCADE] Error executing cascade updates:" (.getMessage e)))
                            (finally
                              ;; Clean up cascade tracking
                              (swap! cascade-chain disj clean-cascade-id)
                              (swap! cascade-depth dec)
                              (log/debug "[CASCADE] Completed cascade for block:" clean-cascade-id 
                                       "Depth now:" @cascade-depth))))))
                  
                  {:status 200
                   :headers {"Content-Type" "application/json"
                            "Access-Control-Allow-Origin" "*"}
                   :body (json/generate-string result-with-sub)})))))) 
          
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
          
          ;; Debug endpoint to inspect block SQL cache
          "/api/debug/block-cache"
          {:status 200
           :headers {"Content-Type" "application/json"
                    "Access-Control-Allow-Origin" "*"}
           :body (json/generate-string 
                  {:cache (into {} (map (fn [[k v]]
                                         [k (assoc v 
                                                  :raw-sql-preview (if (> (count (:raw-sql v)) 100)
                                                                     (str (subs (:raw-sql v) 0 100) "...")
                                                                     (:raw-sql v))
                                                  :resolved-sql-preview (if (> (count (:resolved-sql v)) 100)
                                                                         (str (subs (:resolved-sql v) 0 100) "...")
                                                                         (:resolved-sql v))
                                                  :age-ms (- (System/currentTimeMillis) (:updated-at v)))])
                                       @block-sql-cache))
                   :count (count @block-sql-cache)
                   :timestamp (System/currentTimeMillis)})}
          
          ;; SQL Transform endpoint for creating derived queries
          "/api/sql-transform"
          (let [body (json/parse-string (slurp (:body req)) true)
                transform-type (keyword (:type body))
                source-sql (:source_sql body)
                source-block-id (:source_block_id body)
                column-name (:column_name body)
                cell-value (:cell_value body)
                column-type (when (:column_type body) (keyword (:column_type body)))]
            (log/info "[SQL-TRANSFORM] Request:" {:type transform-type 
                                                  :column column-name
                                                  :source-block-id source-block-id
                                                  :has-sql? (boolean source-sql)})
            (if-let [transformed-sql (sql-transform/transform-sql
                                       {:type transform-type
                                        :source-sql source-sql
                                        :source-block-id source-block-id
                                        :column-name column-name
                                        :cell-value cell-value
                                        :column-type column-type})]
              {:status 200
               :headers {"Content-Type" "application/json"
                        "Access-Control-Allow-Origin" "*"}
               :body (json/generate-string {:sql transformed-sql})}
              {:status 400
               :headers {"Content-Type" "application/json"
                        "Access-Control-Allow-Origin" "*"}
               :body (json/generate-string {:error "Failed to transform SQL"})}))
          
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
          
          ;; Theme endpoints for rabbit demo
          "/api/themes"
          (case method
            :get
            ;; Return list of available theme files
            (try
              (let [themes-dir (clojure.java.io/file "themes")
                    theme-files (when (.exists themes-dir)
                                 (->> (.listFiles themes-dir)
                                      (filter #(.endsWith (.getName %) ".edn"))
                                      (map #(.getName %))
                                      (sort)))]
                {:status 200
                 :headers {"Content-Type" "application/json"
                          "Access-Control-Allow-Origin" "*"}
                 :body (json/generate-string theme-files)})
              (catch Exception e
                (log/error e "Failed to list themes")
                {:status 500
                 :headers {"Content-Type" "application/json"
                          "Access-Control-Allow-Origin" "*"}
                 :body (json/generate-string {:error (str "Failed to list themes: " (.getMessage e))})}))
            
            ;; Default for other methods
            {:status 405
             :headers {"Content-Type" "application/json"
                      "Access-Control-Allow-Origin" "*"}
             :body (json/generate-string {:error "Method not allowed"})})
          
          ;; Fall back to base handler
          (base-handler req)))))))))

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