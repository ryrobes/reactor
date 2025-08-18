(ns reactor.pgwire
  "PostgreSQL wire protocol implementation for XTDB
   Allows psql and other PostgreSQL clients to connect"
  (:require [reactor.xtdb-store :as xts]
            [xtdb.api :as xt]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [java.net ServerSocket Socket]
           [java.io DataInputStream DataOutputStream ByteArrayOutputStream]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]))

;; PostgreSQL Wire Protocol Implementation
;; ========================================
;; Reference: https://www.postgresql.org/docs/current/protocol.html

(def protocol-version 196608) ; 3.0

(defn write-int32 [out n]
  (.writeInt out n))

(defn write-int16 [out n]
  (.writeShort out n))

(defn write-byte [out b]
  (.writeByte out b))

(defn write-string [out s]
  (.write out (.getBytes s StandardCharsets/UTF_8))
  (.writeByte out 0))

(defn read-int32 [in]
  (.readInt in))

(defn read-int16 [in]
  (.readShort in))

(defn read-cstring [in]
  (let [baos (ByteArrayOutputStream.)]
    (loop [b (.read in)]
      (if (or (= b 0) (= b -1))
        (.toString baos StandardCharsets/UTF_8)
        (do (.write baos b)
            (recur (.read in)))))))

(defn send-message [out msg-type data]
  (let [baos (ByteArrayOutputStream.)
        dos (DataOutputStream. baos)]
    (data dos)
    (let [msg-data (.toByteArray baos)
          msg-len (+ 4 (alength msg-data))]
      (write-byte out (byte msg-type))
      (write-int32 out msg-len)
      (.write out msg-data))))

(defn send-auth-ok [out]
  (send-message out \R
    (fn [dos]
      (write-int32 dos 0)))) ; AuthenticationOk

(defn send-parameter-status [out param value]
  (send-message out \S
    (fn [dos]
      (write-string dos param)
      (write-string dos value))))

(defn send-backend-key-data [out]
  (send-message out \K
    (fn [dos]
      (write-int32 dos 1234) ; process ID
      (write-int32 dos 5678)))) ; secret key

(defn send-ready-for-query [out]
  (send-message out \Z
    (fn [dos]
      (write-byte dos (byte \I))))) ; Idle

(defn send-row-description [out columns]
  (send-message out \T
    (fn [dos]
      (write-int16 dos (count columns))
      (doseq [{:keys [name type-oid]} columns]
        (write-string dos name)
        (write-int32 dos 0) ; table OID
        (write-int16 dos 0) ; column number
        (write-int32 dos type-oid) ; type OID
        (write-int16 dos -1) ; type size
        (write-int32 dos -1) ; type modifier
        (write-int16 dos 0))))) ; format code (text)

(defn send-data-row [out row]
  (send-message out \D
    (fn [dos]
      (write-int16 dos (count row))
      (doseq [value row]
        (if (nil? value)
          (write-int32 dos -1)
          (let [bytes (.getBytes (str value) StandardCharsets/UTF_8)]
            (write-int32 dos (alength bytes))
            (.write dos bytes)))))))

(defn send-command-complete [out tag]
  (send-message out \C
    (fn [dos]
      (write-string dos tag))))

(defn send-error [out message]
  (send-message out \E
    (fn [dos]
      (write-byte dos (byte \S)) ; Severity
      (write-string dos "ERROR")
      (write-byte dos (byte \M)) ; Message
      (write-string dos message)
      (write-byte dos 0)))) ; terminator

(defn parse-sql [sql]
  "Parse SQL and convert to XTDB query"
  (let [sql-lower (str/lower-case sql)]
    (cond
      ;; SELECT version()
      (str/includes? sql-lower "select version()")
      {:type :version}
      
      ;; SELECT current_database()
      (str/includes? sql-lower "select current_database()")
      {:type :current-db}
      
      ;; SELECT * FROM todos
      (re-matches #"(?i)select\s+\*\s+from\s+todos.*" sql)
      {:type :select-todos}
      
      ;; SHOW TABLES or \dt
      (or (str/includes? sql-lower "show tables")
          (re-matches #"(?i)select.*from\s+pg_catalog\.pg_tables.*" sql)
          (re-matches #"(?i)select.*from\s+information_schema\.tables.*" sql))
      {:type :show-tables}
      
      ;; Simple SELECT 1
      (re-matches #"(?i)select\s+(\d+)" sql)
      (let [[_ n] (re-matches #"(?i)select\s+(\d+)" sql)]
        {:type :select-literal :value n})
      
      :else
      {:type :unknown :sql sql})))

(defn execute-query [node query]
  "Execute parsed query against XTDB"
  (case (:type query)
    :version
    {:columns [{:name "version" :type-oid 25}] ; text OID
     :rows [["XTDB 1.24.4 on PostgreSQL wire protocol emulator"]]}
    
    :current-db
    {:columns [{:name "current_database" :type-oid 25}]
     :rows [["reactor_xtdb"]]}
    
    :select-todos
    (let [db (xt/db node)
          result (xt/q db
                   '{:find [?id ?text ?completed]
                     :where [[?e :xt/id ?id]
                             [?e :text ?text]
                             [?e :completed ?completed]]})]
      {:columns [{:name "id" :type-oid 25}
                 {:name "text" :type-oid 25}
                 {:name "completed" :type-oid 16}] ; bool OID
       :rows (map (fn [[id text completed]]
                    [id text (str completed)])
                  result)})
    
    :show-tables
    {:columns [{:name "table_name" :type-oid 25}]
     :rows [["todos"] ["users"] ["sessions"]]}
    
    :select-literal
    {:columns [{:name "?column?" :type-oid 23}] ; int4 OID
     :rows [[(:value query)]]}
    
    :unknown
    (throw (Exception. (str "Unsupported query: " (:sql query))))))

(defn handle-query [in out node sql]
  "Handle a query command"
  (try
    (log/info "Executing SQL:" sql)
    (let [query (parse-sql sql)
          result (execute-query node query)]
      ;; Send row description
      (send-row-description out (:columns result))
      ;; Send data rows
      (doseq [row (:rows result)]
        (send-data-row out row))
      ;; Send command complete
      (send-command-complete out (str "SELECT " (count (:rows result)))))
    (catch Exception e
      (log/error e "Query execution failed")
      (send-error out (.getMessage e)))))

(defn handle-simple-query [in out node]
  "Handle simple query protocol"
  (let [sql (read-cstring in)]
    (if (str/blank? sql)
      (send-command-complete out "")
      (handle-query in out node sql))
    (send-ready-for-query out)))

(defn handle-parse [in out]
  "Handle Parse message"
  (let [stmt-name (read-cstring in)
        query (read-cstring in)
        num-params (read-int16 in)]
    ;; Read parameter type OIDs
    (dotimes [_ num-params]
      (read-int32 in))
    ;; Send ParseComplete
    (send-message out \1 (fn [dos]))))

(defn handle-bind [in out]
  "Handle Bind message"
  (let [portal-name (read-cstring in)
        stmt-name (read-cstring in)
        num-format-codes (read-int16 in)]
    ;; Skip format codes
    (dotimes [_ num-format-codes]
      (read-int16 in))
    (let [num-params (read-int16 in)]
      ;; Skip parameters
      (dotimes [_ num-params]
        (let [len (read-int32 in)]
          (when (> len 0)
            (let [buf (byte-array len)]
              (.read in buf)))))
      (let [num-result-formats (read-int16 in)]
        ;; Skip result format codes
        (dotimes [_ num-result-formats]
          (read-int16 in))))
    ;; Send BindComplete
    (send-message out \2 (fn [dos]))))

(defn handle-describe [in out]
  "Handle Describe message"
  (let [obj-type (.read in)
        obj-name (read-cstring in)]
    ;; For now, send empty row description
    (send-row-description out [])))

(defn handle-execute [in out node]
  "Handle Execute message"
  (let [portal-name (read-cstring in)
        max-rows (read-int32 in)]
    ;; For now, just send command complete
    (send-command-complete out "SELECT 0")))

(defn handle-sync [out]
  "Handle Sync message"
  (send-ready-for-query out))

(defn handle-client [socket node]
  "Handle a client connection"
  (try
    (let [in (DataInputStream. (.getInputStream socket))
          out (DataOutputStream. (.getOutputStream socket))]
      
      ;; Read startup message
      (let [msg-len (read-int32 in)
            version (read-int32 in)]
        (log/info "Client connected, protocol version:" version)
        
        ;; Read startup parameters
        (loop []
          (let [param (read-cstring in)]
            (when-not (str/blank? param)
              (let [value (read-cstring in)]
                (log/debug "Startup parameter:" param "=" value))
              (recur)))))
      
      ;; Send authentication OK
      (send-auth-ok out)
      
      ;; Send server parameters
      (send-parameter-status out "server_version" "14.0")
      (send-parameter-status out "server_encoding" "UTF8")
      (send-parameter-status out "client_encoding" "UTF8")
      (send-parameter-status out "DateStyle" "ISO, MDY")
      (send-parameter-status out "TimeZone" "UTC")
      
      ;; Send backend key data
      (send-backend-key-data out)
      
      ;; Send ready for query
      (send-ready-for-query out)
      
      ;; Main message loop
      (loop []
        (let [msg-type (char (.read in))]
          (when (not= msg-type (char -1))
            (let [msg-len (read-int32 in)]
              (log/debug "Received message type:" msg-type "length:" msg-len)
              (case msg-type
                \Q (handle-simple-query in out node)
                \P (handle-parse in out)
                \B (handle-bind in out)
                \D (handle-describe in out)
                \E (handle-execute in out node)
                \S (handle-sync out)
                \X (log/info "Client terminating")
                (do
                  (log/warn "Unknown message type:" msg-type)
                  (send-error out (str "Unknown message type: " msg-type))
                  (send-ready-for-query out)))
              (when (not= msg-type \X)
                (recur)))))))
    
    (catch Exception e
      (log/error e "Error handling client"))
    (finally
      (.close socket))))

(def pg-node (atom nil))

(defn start-pg-server
  "Start PostgreSQL wire protocol server"
  [port]
  (let [node (xts/start-xtdb-node)
        server-socket (ServerSocket. port)]
    (reset! pg-node node)
    
    ;; Seed some test data
    (xt/submit-tx node
      [[::xt/put {:xt/id :todo-1
                  :text "Learn XTDB"
                  :completed false}]
       [::xt/put {:xt/id :todo-2
                  :text "Connect via psql"
                  :completed false}]
       [::xt/put {:xt/id :todo-3
                  :text "Query with SQL"
                  :completed true}]])
    (xt/sync node)
    
    (println "\n========================================")
    (println "PostgreSQL Wire Protocol Server Started")
    (println "========================================")
    (println "Port:" port)
    (println "\nConnect with psql:")
    (println (str "  psql -h localhost -p " port " -U xtdb -d reactor_xtdb"))
    (println "\nNo password required. Try these queries:")
    (println "  SELECT version();")
    (println "  SELECT current_database();")
    (println "  SELECT * FROM todos;")
    (println "  \\q to quit")
    (println "========================================\n")
    
    ;; Accept connections in a separate thread
    (future
      (while true
        (try
          (let [client-socket (.accept server-socket)]
            (future (handle-client client-socket node)))
          (catch Exception e
            (log/error e "Error accepting connection")))))
    
    server-socket))

(defn -main [& args]
  (let [port (or (first args) "5433")]
    (start-pg-server (Integer/parseInt port))
    ;; Keep the main thread alive
    (Thread/sleep Long/MAX_VALUE)))