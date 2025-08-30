(ns reactor.reactive-server
  "Enhanced server with Kafka-based reactive SQL subscriptions."
  (:require [reactor.server :as base-server]
            [reactor.kafka-reactive :as kafka]
            [reactor.sql-reactive-bridge :as bridge]
            [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            ;[reactor.meta-tracking :as meta]
            [reactor.time-travel-sql :as time-travel]
            [reactor.sql-transform :as sql-transform]
            ;[reactor.sql-template :as sql-template]
            ;[reactor.sql-resolver :as resolver]
            [reactor.sql-pipeline :as pipeline]
            ;[reactor.sql-pipeline-adapter :as adapter]
            [reactor.rabbitize :as rabbitize]
            [reactor.utils :as ut]
            [org.httpkit.server :as http]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            ;[clojure.core.async :as async :refer [go <!]]
            ))

;; Track currently cascading blocks to prevent infinite loops
;; This should track the ENTIRE chain, not just individual blocks
(defonce cascade-chain (atom #{}))

;; Feature flag to enable new pipeline
(defonce use-new-pipeline? (atom false))

;; ============================================================================
;; Query History Cache - Table-change driven invalidation
;; ============================================================================

;; Cache structure: {cache-key -> {:resolved-sql "...", :tables [...], :result [...], :timestamp ms}}
;; cache-key = [session-id original-sql]
(defonce query-history-cache (atom {}))

;; Index: table-name -> #{cache-keys that use this table}
(defonce query-history-table-index (atom {}))

(defn extract-tables-from-sql
  "Extract table names from SQL query"
  [sql]
  (try
    (require '[reactor.sql-parser :as parser])
    (let [extract-fn (ns-resolve 'reactor.sql-parser 'extract-tables)]
      (set (map str/lower-case (extract-fn sql))))
    (catch Exception e
      (log/warn "[QUERY-HISTORY] Failed to extract tables from SQL:" (.getMessage e))
      #{})))

(defn update-table-index!
  "Update the table -> cache-keys index"
  [cache-key tables operation]
  (case operation
    :add
    (doseq [table tables]
      (swap! query-history-table-index update table (fnil conj #{}) cache-key))
    
    :remove
    (doseq [table tables]
      (swap! query-history-table-index update table disj cache-key))))

(defn get-cached-query-history
  "Get cached query history if still valid"
  [session-id original-sql resolved-sql]
  (let [cache-key [session-id original-sql]
        cached (get @query-history-cache cache-key)]
    (when (and cached
               ;; Check if resolved SQL matches
               (= (:resolved-sql cached) resolved-sql))
      ;; (log/info (str "[QUERY-HISTORY] Cache HIT for session: " session-id 
      ;;               " | Tables: " (:tables cached)
      ;;               " | No table changes detected"))
      (:result cached))))

(defn cache-query-history!
  "Cache query history result with table tracking"
  [session-id original-sql resolved-sql result]
  (let [cache-key [session-id original-sql]
        tables (extract-tables-from-sql resolved-sql)
        ;; Remove old entry from index if it exists
        old-entry (get @query-history-cache cache-key)]
    (when old-entry
      (update-table-index! cache-key (:tables old-entry) :remove))
    ;; Add new entry
    (swap! query-history-cache assoc cache-key 
           {:resolved-sql resolved-sql
            :tables tables
            :result result
            :timestamp (System/currentTimeMillis)})
    ;; Update index
    (update-table-index! cache-key tables :add)
    (log/debug (str "[QUERY-HISTORY] Cached history for session: " session-id 
                   " | Tables: " tables))))

(defn invalidate-query-history-for-table!
  "Invalidate all cache entries that use a specific table"
  [table-name]
  (let [table-key (str/lower-case table-name)
        affected-keys (get @query-history-table-index table-key #{})]
    (when (seq affected-keys)
      (log/info (str "[QUERY-HISTORY] Table '" table-name 
                    "' changed, invalidating " (count affected-keys) " cached queries"))
      ;; Get all tables used by affected cache entries before removing them
      (let [all-tables-to-clean (reduce (fn [tables cache-key]
                                          (if-let [entry (get @query-history-cache cache-key)]
                                            (into tables (:tables entry))
                                            tables))
                                        #{}
                                        affected-keys)]
        ;; Remove from cache
        (swap! query-history-cache 
               (fn [cache]
                 (apply dissoc cache affected-keys)))
        ;; Clean up index for all affected tables
        (doseq [table all-tables-to-clean]
          (swap! query-history-table-index update table 
                 (fn [keys] (apply disj keys affected-keys))))))))

(defn invalidate-query-history-for-tables!
  "Invalidate cache entries for multiple tables"
  [table-names]
  (doseq [table table-names]
    (invalidate-query-history-for-table! table)))

;; Hook to be called by Kafka when tables change
(defn on-tables-changed!
  "Called by Kafka reactive system when tables are mutated"
  [tables]
  (when (seq tables)
    (log/debug (str "[QUERY-HISTORY] Tables changed via Kafka: " tables))
    (invalidate-query-history-for-tables! tables)))

(defn enable-new-pipeline! []
  (reset! use-new-pipeline? true)
  (log/info "[REACTIVE-SERVER] New SQL pipeline ENABLED"))

(defn disable-new-pipeline! []
  (reset! use-new-pipeline? false)
  (log/info "[REACTIVE-SERVER] Legacy SQL pipeline restored"))

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
              ;; Only clean up orphaned subscriptions from OTHER sessions that have no active SSE channels
              ;; Don't clean up subscriptions for THIS session - they were likely just created
              (let [orphaned-subs (filter (fn [[sub-id sub-info]]
                                           (let [sub-session (:session-id sub-info)]
                                             (and (not= sub-session session-id)  ; Don't clean up current session
                                                  (empty? (get @kafka/sse-channels sub-session)))))
                                         @kafka/active-subscriptions)]
                (when (seq orphaned-subs)
                  (log/info "[REACTIVE-SERVER] Cleaning up" (count orphaned-subs) "orphaned subscriptions from other sessions")
                  (doseq [[sub-id _] orphaned-subs]
                    (kafka/unsubscribe-query! sub-id))))
              
              ;; Log active subscriptions for this session
              (let [active-subs (filter (fn [[sub-id sub-info]]
                                          (= (:session-id sub-info) session-id))
                                        @kafka/active-subscriptions)]
                (log/info "[REACTIVE-SERVER] Session" session-id "has" (count active-subs) "active subscriptions"))
              
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
                  client-id (get-in req [:query-params "client_id"])
                  
                  ;; Use the pipeline for subscription
                  result (if use-new-pipeline?
                          (pipeline/execute-sql
                           {:sql sql
                            :params params
                            :session-id session-id
                            :subscription-id subscription-id
                            :client-id client-id})
                          ;; Fallback to old method
                          (let [sub-id (or subscription-id (kafka/subscribe-query! session-id sql params))]
                            (when subscription-id
                              (kafka/register-query-subscription! subscription-id sql params 
                                                               (kafka/create-subscription-callback session-id)
                                                               session-id))
                            (kafka/re-execute-subscription sub-id)
                            {:subscription-id sub-id}))]
              
              {:status 200
               :headers {"Content-Type" "application/json"
                        "Access-Control-Allow-Origin" "*"}
               :body (json/generate-string {:subscription-id (:subscription-id result)
                                           :status "registered"
                                           :results (:results result)})}))
          
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
          
          ;; SQL query endpoint - now using the clean pipeline!
          "/api/sql"
          (let [body (json/parse-string (slurp (:body req)) true)
                query-params (when-let [query-string (:query-string req)]
                              (into {} (map #(clojure.string/split % #"=") 
                                          (clojure.string/split query-string #"&"))))
                session-id (or (get query-params "session")
                              (get query-params "session_id") 
                              session-id)
                ;; Handle both hyphenated and underscored versions for compatibility
                subscription-id (or (:subscription-id body)  ; Client sends with hyphen
                                  (:subscription_id body)     ; Also check underscore
                                  (get query-params "subscription_id")
                                  (get query-params "subscription-id"))
                client-id (get query-params "client_id")
                ;; Handle both hyphenated and underscored versions for as-of (client sends with hyphen)
                as-of (or (:as-of body)      ; Client sends with hyphen
                         (:as_of body))       ; Also check underscore for compatibility
                
                ;; Execute through the new pipeline
                result (pipeline/execute-sql
                        {:sql (:sql body)
                         :params (:params body)
                         :session-id session-id
                         :block-id (:block_id body)
                         :as-of as-of
                         :subscription-id subscription-id
                         :client-id client-id})]
            
            (if (:success result)
              {:status 200
               :headers {"Content-Type" "application/json"
                        "Access-Control-Allow-Origin" "*"
                        "Access-Control-Allow-Methods" "GET, POST, OPTIONS"
                        "Access-Control-Allow-Headers" "Content-Type, x-session-id"}
               :body (json/generate-string
                     {:results (:results result)
                      :subscription_id (:subscription-id result)
                      :diff (:diff result)
                      :execution_time (:execution-time result)
                      :has_templates (:has-templates? result)
                      :dependencies (:dependencies result)
                      :tables (:tables result)})}
              {:status 400
               :headers {"Content-Type" "application/json"
                        "Access-Control-Allow-Origin" "*"}
               :body (json/generate-string
                     {:error (get-in result [:error :message])
                      :type (get-in result [:error :type])})}))
          ;; SQL exec endpoint for mutations
          "/api/sql-exec"
          (bridge/handle-sql-exec-reactive req)
          
          ;; Query history endpoint for time travel
          "/api/query-history"
          (let [body (json/parse-string (slurp (:body req)) true)
                original-sql (:sql body)
                limit (or (:limit body) 20)
                ;_ (ut/pp [:get-history body])
                node @session/default-node
                ;; Get session state for template resolution
                session-state (when session-id
                               (when-let [session (session/get-session session-id)]
                                 (session/get-state session)))
                ;; Resolve templates if present
                resolved-sql (if (and original-sql (re-find #"\{\{[^}]+\.sql\}\}" original-sql))
                              (try
                                (require '[reactor.sql-template :as template])
                                (let [resolver (ns-resolve 'reactor.sql-template 'resolve-sql-templates-with-deps)]
                                  (:sql (resolver original-sql session-state)))
                                (catch Exception e
                                  (log/warn "[QUERY-HISTORY] Failed to resolve templates:" (.getMessage e))
                                  original-sql))
                              original-sql)]
            (if node
              ;; Check cache first
              (if-let [cached-result (get-cached-query-history session-id original-sql resolved-sql)]
                ;; Return cached result
                {:status 200
                 :headers {"Content-Type" "application/json"
                          "Access-Control-Allow-Origin" "*"}
                 :body (json/generate-string cached-result)}
                ;; Cache miss or resolved SQL changed - fetch new history
                (let [_ (when (not= original-sql resolved-sql)
                         (log/info (str "[QUERY-HISTORY] Resolved templates for history"
                                        "\n  Original:" (if (> (count original-sql) 100)
                                                          (str (subs original-sql 0 100) "...")
                                                          original-sql)
                                        "\n  Resolved:" (if (> (count resolved-sql) 100)
                                                          (str (subs resolved-sql 0 100) "...")
                                                          resolved-sql))))
                      _ (cond
                         ;; Never cached before
                         (nil? (get @query-history-cache [session-id original-sql]))
                         (log/info "[QUERY-HISTORY] First time fetching history for this query")
                         
                         ;; Cached but resolved SQL changed
                         (not= (:resolved-sql (get @query-history-cache [session-id original-sql])) resolved-sql)
                         (log/info "[QUERY-HISTORY] Template values changed, fetching new history")
                         
                         ;; Must have been invalidated by table change
                         :else
                         (log/info "[QUERY-HISTORY] Cache was invalidated by table change, fetching new history"))
                      result (time-travel/get-query-history-range node resolved-sql limit (get body :sub-id))
                      ;; Cache the result
                      _ (cache-query-history! session-id original-sql resolved-sql result)]
                  {:status 200
                   :headers {"Content-Type" "application/json"
                            "Access-Control-Allow-Origin" "*"}
                   :body (json/generate-string result)}))
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
            (println (str "[SQL-TRANSFORM] Request:" {:type transform-type 
                                                  :column column-name
                                                  :source-block-id source-block-id
                                                  :has-sql? (boolean source-sql)}))
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
          
          ;; Test endpoint for temporal cache
          "/api/test-temporal-cache"
          (let [body (json/parse-string (slurp (:body req)) true)
                sql (or (:sql body) "SELECT * FROM sales LIMIT 5")
                as-of (or (:as-of body) "2024-01-01T00:00:00Z")
                enable-cache (:enable-cache body true)]
            (log/info "[TEMPORAL-CACHE-TEST] Testing cache with" 
                     "\n  SQL:" sql
                     "\n  Timestamp:" as-of
                     "\n  Cache enabled:" enable-cache)
            
            ;; Set cache state
            (require '[reactor.temporal-cache :as cache])
            ((resolve 'reactor.temporal-cache/set-cache-enabled!) enable-cache)
            
            ;; Execute query twice to test cache hit
            (let [result1 (pipeline/execute-sql
                          {:sql sql
                           :as-of as-of
                           :session-id session-id})
                  from-cache1? (:from-cache? result1)
                  
                  result2 (pipeline/execute-sql
                          {:sql sql
                           :as-of as-of
                           :session-id session-id})
                  from-cache2? (:from-cache? result2)
                  
                  ;; Get cache stats
                  cache-stats ((resolve 'reactor.temporal-cache/cache-stats))]
              
              (log/info "[TEMPORAL-CACHE-TEST] Results:"
                       "\n  First query from cache:" from-cache1?
                       "\n  Second query from cache:" from-cache2?
                       "\n  Cache stats:" cache-stats)
              
              {:status 200
               :headers {"Content-Type" "application/json"
                        "Access-Control-Allow-Origin" "*"}
               :body (json/generate-string 
                      {:first-from-cache from-cache1?
                       :second-from-cache from-cache2?
                       :cache-stats cache-stats
                       :results-match (= (:results result1) (:results result2))})}))
          
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