(ns reactor.server
  "Dead simple server setup - one function to rule them all"
  (:require [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [org.httpkit.server :as http]
            [cheshire.core :as json]
            [honeysql.core :as hsql]
            [honeysql.format :as hfmt]))

(defn wrap-cors [response]
  (-> response
      (assoc-in [:headers "Access-Control-Allow-Origin"] "*")
      (assoc-in [:headers "Access-Control-Allow-Methods"] "GET, POST, OPTIONS")
      (assoc-in [:headers "Access-Control-Allow-Headers"] "Content-Type")))

(defn get-session-id 
  "Extract session ID from request - checks query params first, then uses fallback fn"
  [req session-id-fn]
  (or (get-in req [:params :session])
      (get-in req [:query-params "session"])
      (when-let [query (:query-string req)]
        (when-let [match (re-find #"session=([^&]+)" query)]
          ;; URL decode the session ID to handle spaces and special characters
          (java.net.URLDecoder/decode (second match) "UTF-8")))
      (session-id-fn req)))

(defn compute-initial-state
  "Compute initial state with filtered todos"
  [state]
  (let [todos (vals (:todos state {}))
        filter-type (:filter state :all)]
    (assoc state :filtered-todos
           (case filter-type
             :active (filter (complement :completed) todos)
             :completed (filter :completed todos)
             :all todos
             todos))))

(defn create-handler
  "Create a Ring handler with all the Reactor endpoints"
  [& {:keys [session-id-fn]
      :or {session-id-fn (constantly "default")}}]
  (fn [req]
    (let [path (:uri req)
          method (:request-method req)
          session-id (get-session-id req session-id-fn)
          session (session/get-session session-id)]
      (wrap-cors
        (cond
          ;; CORS preflight
          (= method :options)
          {:status 200 :headers {"Content-Type" "text/plain"}}
          
          ;; Regular routes
          :else
          (case path
            "/api/state"
            (let [state @session]
              (println "Sending state for session" session-id ":" state)
              {:status 200 
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string state)})
            
            "/api/dispatch" 
            (let [raw-event (json/parse-string (slurp (:body req)) true)
                  ;; Convert first element to keyword, and any filter keywords
                  event-name (keyword (first raw-event))
                  event-args (map (fn [x] 
                                   (if (and (string? x) 
                                           (contains? #{"all" "active" "completed"} x))
                                     (keyword x)
                                     x))
                                 (rest raw-event))
                  event (vec (cons event-name event-args))]
              (println "Dispatching event:" event)
              (session/dispatch session-id event)
              {:status 200 
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string @session)})
            
            "/api/undo" 
            (do (session/undo! session-id)
                {:status 200 
                 :headers {"Content-Type" "application/json"}
                 :body (json/generate-string @session)})
            
            "/api/redo" 
            (do (session/redo! session-id)
                {:status 200 
                 :headers {"Content-Type" "application/json"}
                 :body (json/generate-string @session)})
            
            "/api/query"
            (let [body (json/parse-string (slurp (:body req)) true)
                  query (:query body)
                  node (or (:node session) @session/default-node)
                  ;; Convert Datalog-style query to SQL for XTDB 2.0
                  result (if node
                          (try
                            ;; For now, just return empty results for Datalog queries
                            ;; TODO: Convert to XTQL or SQL
                            []
                            (catch Exception e
                              (println "Query error:" (.getMessage e))
                              []))
                          [])]
              {:status 200
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string result)})
            
            "/api/sql"
            (let [body (json/parse-string (slurp (:body req)) true)
                  sql-input (:sql body)
                  params (:params body)
                  as-of (:as-of body)
                  node (or (:node session) @session/default-node)
                  result (if node
                          (try
                            ;; Check if it's a HoneySQL map or a SQL string
                            (cond
                              ;; HoneySQL map - render to SQL first
                              (map? sql-input)
                              (let [sql-string (first (hsql/format sql-input))]
                                (session/execute-sql-query node sql-string params as-of))
                              
                              ;; SQL string - execute directly via XTDB SQL
                              (string? sql-input)
                              (session/execute-sql-query node sql-input params as-of)
                              
                              :else
                              {:error "Invalid SQL input" :results []})
                            (catch Exception e
                              (println "SQL error:" (.getMessage e))
                              {:error (.getMessage e) :results []}))
                          {:error "No XTDB node available" :results []})]
              {:status 200
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string result)})
            
            "/api/sql-exec"
            (let [body (json/parse-string (slurp (:body req)) true)
                  sql-string (:sql body)
                  params (:params body)
                  node (or (:node session) @session/default-node)
                  result (if node
                          (try
                            ;; Execute INSERT/UPDATE/DELETE via XTDB SQL
                            (let [result (session/execute-sql-mutation node sql-string params)]
                              (if (:error result)
                                result
                                {:result (str "Executed successfully. Rows affected: " (or (:rows-affected result) "unknown"))}))
                            (catch Exception e
                              (println "SQL exec error:" (.getMessage e))
                              {:error (.getMessage e)}))
                          {:error "No XTDB node available"})]
              {:status 200
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string result)})
            
            "/api/tables"
            (let [node (or @session/default-node (xts/start-xtdb-node))
                  tables (xts/list-tables node)]
              {:status 200
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string tables)})
            
            "/api/subscribe"
            (http/with-channel req channel
              (http/send! channel {:status 200
                                  :headers {"Content-Type" "text/event-stream"
                                           "Cache-Control" "no-cache"
                                           "Access-Control-Allow-Origin" "*"}} false)
              (http/send! channel (str "data: " (json/generate-string @session) "\n\n") false)
              (add-watch session ::sse
                         (fn [_ _ _ new-state]
                           (http/send! channel (str "data: " (json/generate-string new-state) "\n\n") false))))
            
            "/api/sessions"
            {:status 200
             :headers {"Content-Type" "application/json"}
             :body (json/generate-string (session/get-all-sessions))}
            
            "/api/create-session"
            (let [body (json/parse-string (slurp (:body req)) true)
                  new-session-id (:session-id body)
                  initial-state (:initial-state body {})]
              (session/create-session! new-session-id initial-state)
              {:status 200
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string {:session-id new-session-id})})
            
            "/api/delete-session"
            (let [body (json/parse-string (slurp (:body req)) true)
                  session-to-delete (:session-id body)]
              (if (and session-to-delete (not= session-to-delete "default"))
                (do
                  (session/destroy-session! session-to-delete)
                  ;; Also delete from XTDB
                  (when-let [node @session/default-node]
                    (xts/delete-entity node "sessions" (str "session-" session-to-delete)))
                  {:status 200
                   :headers {"Content-Type" "application/json"}
                   :body (json/generate-string {:success true})})
                {:status 400
                 :headers {"Content-Type" "application/json"}
                 :body (json/generate-string {:error "Cannot delete default session"})}))
            
            "/api/history-info"
            {:status 200
             :headers {"Content-Type" "application/json"}
             :body (json/generate-string (session/get-history-info session-id))}
            
            "/api/jump-to-history"
            (let [body (json/parse-string (slurp (:body req)) true)
                  index (:index body)]
              (session/jump-to-history! session-id index)
              {:status 200
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string @session)})
            
            ;; 404
            {:status 404 :body "Not found"}))))))

(defn start!
  "Start a Reactor server with your event handlers"
  [& {:keys [port handlers session-id-fn init-fn initial-state-fn]
      :or {port 4000
           handlers {}
           session-id-fn (constantly "default")
           initial-state-fn (constantly {})}}]
  
  ;; Initialize XTDB
  (session/init!)
  
  ;; Register all handlers
  (doseq [[event-id handler] handlers]
    (session/reg-event-db event-id handler))
  
  ;; Run any custom initialization
  (when init-fn (init-fn))
  
  ;; Start server
  (let [handler (create-handler :session-id-fn session-id-fn)]
    (http/run-server handler {:port port})
    (println (str "🚀 Reactor server running on http://localhost:" port))
    
    ;; Return server info
    {:port port
     :handler handler
     :handlers @session/event-handlers}))