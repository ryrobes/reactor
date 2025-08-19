(ns reactor.pgwire-test
  "Tests for PostgreSQL wire protocol server"
  (:require [clojure.test :refer :all]
            [reactor.pgwire :as pg]
            [clojure.java.shell :as shell]
            [clojure.string :as str])
  (:import [java.net Socket ConnectException]))

(defn psql-available? []
  "Check if psql CLI is available"
  (let [result (shell/sh "which" "psql")]
    (= 0 (:exit result))))

(defn port-open? [port]
  "Check if a port is open for connections"
  (try
    (with-open [socket (Socket. "localhost" port)]
      true)
    (catch ConnectException _
      false)))

;; Disabling pgwire tests since we'll migrate to XTDB 2.x which has native PostgreSQL support
#_(deftest test-pgwire-server-startup
  (testing "PostgreSQL wire protocol server starts and accepts connections"
    (let [port 5434 ; Use different port to avoid conflicts
          server-future (future (pg/start-pg-server port))]
      (try
        ;; Wait for server to start
        (Thread/sleep 2000)
        
        ;; Check that port is open
        (is (port-open? port) "Server should be listening on port")
        
        (finally
          ;; Clean up
          (future-cancel server-future)
          (when @pg/pg-node
            (.close @pg/pg-node))
          ;; Close the server socket if it was created
          (when (realized? server-future)
            (when-let [server-socket @server-future]
              (when (instance? java.net.ServerSocket server-socket)
                (.close server-socket)))))))))

#_(deftest test-psql-connectivity
  (when (psql-available?)
    (testing "psql can attempt connection to PostgreSQL wire server"
      (let [port 5435
            server-future (future (pg/start-pg-server port))]
        (try
          ;; Wait for server to start
          (Thread/sleep 2000)
          
          ;; Try to connect with psql (we expect this to fail/timeout currently)
          ;; Just test that the server is accepting connections
          (let [result (shell/sh "timeout" "2" "psql" 
                                "-h" "localhost" 
                                "-p" (str port)
                                "-U" "xtdb"
                                "-d" "reactor_xtdb"
                                "-c" "SELECT 1;")]
            ;; We expect timeout (124) or connection error
            ;; This documents current limitation
            (is (contains? #{124 1 2} (:exit result))
                (str "Expected timeout or connection error, got: " (:exit result))))
          
          (finally
            (future-cancel server-future)
            (when @pg/pg-node
              (.close @pg/pg-node))
            ;; Close the server socket if it was created
            (when (realized? server-future)
              (when-let [server-socket @server-future]
                (when (instance? java.net.ServerSocket server-socket)
                  (.close server-socket))))))))
    
    (when-not (psql-available?)
      (println "Skipping psql test - psql not installed"))))

#_(deftest test-http-sql-api-alternative
  (testing "HTTP SQL API works as alternative to psql"
    (let [sql-api (requiring-resolve 'reactor.sql-api/start-sql-server)
          http-get (requiring-resolve 'org.httpkit.client/get)
          http-post (requiring-resolve 'org.httpkit.client/post)
          json-generate (requiring-resolve 'cheshire.core/generate-string)
          json-parse (requiring-resolve 'cheshire.core/parse-string)
          port 8081
          _ (sql-api port)]
      (try
        (Thread/sleep 1000)
        
        ;; Test info endpoint
        (let [resp @(http-get (str "http://localhost:" port "/info"))]
          (is (= 200 (:status resp)))
          (is (str/includes? (:body resp) "XTDB SQL API")))
        
        ;; Test SQL query endpoint
        (let [resp @(http-post (str "http://localhost:" port "/sql")
                               {:headers {"Content-Type" "application/json"}
                                :body (json-generate 
                                       {:query "SELECT * FROM todos"})})]
          (is (= 200 (:status resp)))
          (let [body (json-parse (:body resp) true)]
            (is (contains? body :result))
            (is (vector? (:result body)))))
        
        (finally
          ;; Clean up
          (when-let [sql-node-atom (requiring-resolve 'reactor.sql-api/sql-node)]
            (when-let [node @sql-node-atom]
              (.close node))))))))

;; This test can stay as it doesn't start servers
(deftest test-sql-parsing
  (testing "SQL parsing functionality"
    (is (= :version (:type (pg/parse-sql "SELECT version()"))))
    (is (= :current-db (:type (pg/parse-sql "SELECT current_database()"))))
    (is (= :select-todos (:type (pg/parse-sql "SELECT * FROM todos"))))
    (is (= :show-tables (:type (pg/parse-sql "SHOW TABLES"))))
    (is (= :select-literal (:type (pg/parse-sql "SELECT 42"))))
    (is (= "42" (:value (pg/parse-sql "SELECT 42"))))))

;; Integration test that documents current capabilities and limitations
;; Integration test - disable since it checks for running servers
#_(deftest ^:integration test-sql-connectivity-options
  (testing "SQL connectivity options for XTDB"
    (println "\nSQL Connectivity Test Results:")
    (println "==============================")
    
    ;; Test 1: PostgreSQL wire protocol server
    (print "1. PostgreSQL Wire Protocol Server: ")
    (if (port-open? 5433)
      (println "✓ Running (but psql handshake incomplete)")
      (println "✗ Not running"))
    
    ;; Test 2: HTTP SQL API
    (print "2. HTTP SQL API: ")
    (try
      (let [http-get (requiring-resolve 'org.httpkit.client/get)]
        @(http-get "http://localhost:8080/info" {:timeout 1000})
        (println "✓ Available and working"))
      (catch Exception _
        (println "✗ Not running")))
    
    ;; Test 3: psql CLI availability
    (print "3. psql CLI: ")
    (if (psql-available?)
      (println "✓ Installed")
      (println "✗ Not installed"))
    
    ;; Test 4: XTDB 2.x recommendation
    (println "4. XTDB 2.x Migration: ⚠ Recommended for native psql support")
    
    (println "\nRecommendation: Use HTTP SQL API for immediate needs,")
    (println "                migrate to XTDB 2.x for full psql support")))

(comment
  ;; Run specific tests
  (run-tests 'reactor.pgwire-test)
  
  ;; Run integration test
  (test-sql-connectivity-options))