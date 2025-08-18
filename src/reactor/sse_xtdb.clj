(ns reactor.sse-xtdb
  "SSE handler with XTDB and SQL query support"
  (:require [reactor.xtdb-store :as xts]
            [reactor.xtdb-query :as xtq]
            [org.httpkit.server :as http]
            [ring.middleware.params :refer [wrap-params]]
            [compojure.core :refer [defroutes GET POST]]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]))

(def ^:private channels (atom {}))
(def ^:private query-subs (atom {}))

(defn- format-data [format-type data]
  (case format-type
    "json" (json/generate-string data)
    "edn" (pr-str data)
    (pr-str data)))

(defn- parse-query [query-str format]
  "Parse query string based on format"
  (case format
    "keypath" (edn/read-string query-str)
    "sql" query-str
    "honeysql" (json/parse-string query-str true)
    "edn" (edn/read-string query-str)
    (edn/read-string query-str)))

(defn- handle-query-subscription [req node]
  "Handle SSE subscription with query support"
  (let [params (:params req)
        query-str (get params "query")
        query-format (get params "query-format" "keypath")
        response-format (get params "format" "edn")
        session-id (get params "session-id")
        poll-ms (when-let [p (get params "poll-ms")]
                  (Integer/parseInt p))]
    
    (println "Query subscription - format:" query-format "poll:" poll-ms "query:" query-str)
    
    (http/with-channel req channel
      (println "SSE channel opened for query")
      
      ;; Clean up on close
      (http/on-close channel
                     (fn [status]
                       (println "SSE channel closed:" status)
                       (when-let [sub (get @query-subs channel)]
                         (xtq/close! sub))
                       (swap! channels dissoc channel)
                       (swap! query-subs dissoc channel)))
      
      ;; Send SSE headers
      (http/send! channel
                  {:status 200
                   :headers {"Content-Type" "text/event-stream"
                            "Cache-Control" "no-cache"
                            "Connection" "keep-alive"
                            "Access-Control-Allow-Origin" "*"}}
                  false)
      
      ;; Parse and execute query
      (try
        (let [query (parse-query query-str query-format)
              ;; Subscribe to query with polling
              sub (if poll-ms
                    (xtq/subscribe-query
                     node query
                     :poll-ms (or poll-ms 1000)
                     :on-change (fn [old-val new-val]
                                 (let [formatted (format-data response-format new-val)]
                                   (http/send! channel (str "data: " formatted "\n\n") false))))
                    ;; One-time query
                    (atom (xtq/execute-query node query
                                           :session-id session-id)))]
          
          (swap! channels assoc channel {:query query 
                                        :format response-format
                                        :query-format query-format})
          (when poll-ms
            (swap! query-subs assoc channel sub))
          
          ;; Send initial result
          (let [initial-val (if poll-ms @sub (xtq/execute-query node query :session-id session-id))
                formatted (format-data response-format initial-val)]
            (http/send! channel (str "data: " formatted "\n\n") false)))
        
        (catch Exception e
          (println "Query subscription error:" (.getMessage e))
          (let [error-msg (format-data response-format {:error (.getMessage e)})]
            (http/send! channel (str "data: " error-msg "\n\n") false)))))))

(defn- handle-execute [req node]
  "Execute a query and return results"
  (let [body (slurp (:body req))
        content-type (get-in req [:headers "content-type"] "")]
    (try
      (let [data (cond
                   (str/includes? content-type "json")
                   (json/parse-string body true)
                   
                   (str/includes? content-type "edn")
                   (edn/read-string body)
                   
                   :else
                   (edn/read-string body))
            query (:query data)
            session-id (:session-id data)
            as-of (:as-of data)
            result (xtq/execute-query node query 
                                    :session-id session-id
                                    :as-of as-of)]
        
        {:status 200
         :headers {"Content-Type" "application/edn"}
         :body (pr-str {:status :ok :result result})})
      
      (catch Exception e
        {:status 400
         :headers {"Content-Type" "application/edn"}
         :body (pr-str {:status :error
                        :message (.getMessage e)})}))))

(defn- handle-update [req node]
  "Handle entity updates"
  (let [body (slurp (:body req))
        content-type (get-in req [:headers "content-type"] "")]
    (try
      (let [data (cond
                   (str/includes? content-type "json")
                   (json/parse-string body true)
                   
                   (str/includes? content-type "edn")
                   (edn/read-string body)
                   
                   :else
                   (edn/read-string body))
            entity-id (:entity-id data)
            entity-data (:data data)
            session-id (:session-id data)
            ;; Create or update entity
            tx (xts/put-entity node entity-id entity-data)]
        
        {:status 200
         :headers {"Content-Type" "application/edn"}
         :body (pr-str {:status :ok :tx-id tx})})
      
      (catch Exception e
        {:status 400
         :headers {"Content-Type" "application/edn"}
         :body (pr-str {:status :error
                        :message (.getMessage e)})}))))

(defn- handle-sql-builder [req]
  "Build SQL query using HoneySQL DSL"
  (let [body (slurp (:body req))
        data (json/parse-string body true)]
    (try
      (let [{:keys [table select where order-by limit offset]} data
            query (cond-> {:select (or select [:*])
                          :from [(keyword table)]}
                    where (assoc :where where)
                    order-by (assoc :order-by order-by)
                    limit (assoc :limit limit)
                    offset (assoc :offset offset))
            sql (xtq/honeysql->xtql query)]
        
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string {:status :ok 
                                     :honeysql query
                                     :sql sql})})
      
      (catch Exception e
        {:status 400
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string {:status :error
                                     :message (.getMessage e)})}))))

(defn- handle-sync-table [req node]
  "Sync entity to flattened table for SQL queries"
  (let [body (slurp (:body req))
        data (edn/read-string body)]
    (try
      (let [entity-id (:entity-id data)]
        (xtq/sync-to-table node entity-id)
        {:status 200
         :headers {"Content-Type" "application/edn"}
         :body (pr-str {:status :ok :message "Table synced"})})
      
      (catch Exception e
        {:status 400
         :headers {"Content-Type" "application/edn"}
         :body (pr-str {:status :error
                        :message (.getMessage e)})}))))

(defn create-xtdb-sse-handler [node]
  (defroutes xtdb-routes
    (GET "/health" [] {:status 200 :body "OK"})
    (GET "/subscribe" req 
         (do 
           (println "Subscribe endpoint hit!")
           (handle-query-subscription req node)))
    (POST "/query" req (handle-execute req node))
    (POST "/update" req (handle-update req node))
    (POST "/sql-builder" req (handle-sql-builder req))
    (POST "/sync-table" req (handle-sync-table req node))))

(defn wrap-cors [handler]
  (fn [request]
    (let [response (handler request)]
      (-> response
          (assoc-in [:headers "Access-Control-Allow-Origin"] "*")
          (assoc-in [:headers "Access-Control-Allow-Methods"] "GET, POST, OPTIONS")
          (assoc-in [:headers "Access-Control-Allow-Headers"] "Content-Type")))))

(defn wrap-logging [handler]
  (fn [req]
    (println "Request:" (:request-method req) (:uri req))
    (handler req)))

(defn create-app [node]
  (-> (create-xtdb-sse-handler node)
      wrap-params
      wrap-cors
      wrap-logging))

(defn start-xtdb-sse-server [node port]
  (let [app (create-app node)]
    (println "Starting XTDB SSE server on port" port)
    (http/run-server app {:port port})))

;; Example usage:
(comment
  ;; Start XTDB node
  (def node (xts/start-xtdb-node))
  
  ;; Start SSE server
  (def server (start-xtdb-sse-server node 8080))
  
  ;; Client can now:
  ;; 1. Subscribe with keypath: /subscribe?query=["todos"]&query-format=keypath
  ;; 2. Subscribe with SQL: /subscribe?query=SELECT * FROM todos&query-format=sql
  ;; 3. Subscribe with HoneySQL: /subscribe?query={"select":["*"],"from":"todos"}&query-format=honeysql
  ;; 4. Poll for changes: /subscribe?query=["todos"]&poll-ms=1000
  )