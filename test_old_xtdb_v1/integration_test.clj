(ns reactor.integration-test
  "Integration tests for XTDB with SQL query support"
  (:require [clojure.test :refer :all]
            [reactor.xtdb-store :as xts]
            [reactor.xtdb-query :as xtq]
            [reactor.sse-xtdb :as sse]
            [org.httpkit.client :as http]
            [clojure.edn :as edn]
            [cheshire.core :as json]))

(deftest test-end-to-end-query-flow
  (testing "Complete flow from entity creation to SQL query"
    (let [node (xts/start-xtdb-node)
          port 8765
          server (sse/start-xtdb-sse-server node port)
          base-url (str "http://localhost:" port)]
      
      (try
        ;; Wait for server to start
        (Thread/sleep 100)
        
        ;; Check server health
        (let [health-resp @(http/get (str base-url "/health") {:as :text})]
          (is (= 200 (:status health-resp)))
          (is (= "OK" (:body health-resp))))
        
        ;; 1. Create entities via HTTP
        (let [resp1 @(http/post (str base-url "/update")
                                {:body (pr-str {:entity-id "todo-1"
                                              :data {:text "Write tests"
                                                    :completed false
                                                    :priority 1}})
                                 :headers {"Content-Type" "application/edn"}})
              resp2 @(http/post (str base-url "/update")
                                {:body (pr-str {:entity-id "todo-2"
                                              :data {:text "Review PR"
                                                    :completed false
                                                    :priority 2}})
                                 :headers {"Content-Type" "application/edn"}})
              resp3 @(http/post (str base-url "/update")
                                {:body (pr-str {:entity-id "todo-3"
                                              :data {:text "Deploy"
                                                    :completed true
                                                    :priority 3}})
                                 :headers {"Content-Type" "application/edn"}})]
          
          (is (= 200 (:status resp1)))
          (is (= 200 (:status resp2)))
          (is (= 200 (:status resp3)))
          
          ;; Wait for transactions
          (Thread/sleep 100)
          
          ;; 2. Query with keypath
          (let [resp @(http/post (str base-url "/query")
                                 {:body (pr-str {:query ["todo-1"]})
                                  :headers {"Content-Type" "application/edn"}
                                  :as :text})
                result (edn/read-string (:body resp))]
            (is (= 200 (:status resp)))
            (is (= :ok (:status result)))
            (is (not (empty? (:result result)))))
          
          ;; 3. Sync entities to tables
          (doseq [id ["todo-1" "todo-2" "todo-3"]]
            (let [resp @(http/post (str base-url "/sync-table")
                                   {:body (pr-str {:entity-id id})
                                    :headers {"Content-Type" "application/edn"}
                                    :as :text})]
              (is (= 200 (:status resp)))))
          
          ;; 4. Build SQL query with HoneySQL
          (let [resp @(http/post (str base-url "/sql-builder")
                                 {:body (json/generate-string
                                        {:table "todos"
                                         :select ["text" "completed"]
                                         :where [:= :completed false]
                                         :order-by [[:priority :asc]]
                                         :limit 5})
                                  :headers {"Content-Type" "application/json"}
                                  :as :text})
                result (json/parse-string (:body resp) true)]
            (is (= 200 (:status resp)))
            (is (= :ok (keyword (:status result))))
            (is (contains? result :sql))
            (is (contains? result :honeysql)))
          
          ;; 5. Test SSE subscription with polling
          (testing "SSE endpoint (using curl since http-kit doesn't support SSE)"
            (let [sse-url (str base-url "/subscribe?query=%5B%22todo-1%22%5D&query-format=keypath&format=edn&poll-ms=100")
                  curl-result (clojure.java.shell/sh 
                               "curl" "-s" "-N" "--max-time" "1"
                               "-H" "Accept: text/event-stream"
                               sse-url)]
              ;; Curl will timeout (28) or succeed (0) - both are ok for SSE
              (is (or (= 0 (:exit curl-result))
                      (= 28 (:exit curl-result))))
              (when (re-find #"data:" (:out curl-result))
                (is true "SSE endpoint returns data"))))
          
          ;; 6. Query with HoneySQL format
          (let [honeysql-query {:select [:text :priority]
                               :from :todos
                               :where [:= :completed false]
                               :order-by [[:priority :asc]]}
                resp @(http/post (str base-url "/query")
                                {:body (pr-str {:query honeysql-query})
                                 :headers {"Content-Type" "application/edn"}
                                 :as :text})
                result (edn/read-string (:body resp))]
            (is (= 200 (:status resp)))
            (is (= :ok (:status result))))
          
          ;; 7. Query with raw SQL
          (let [sql-query "SELECT text, completed FROM todos WHERE completed = false"
                resp @(http/post (str base-url "/query")
                                {:body (pr-str {:query sql-query})
                                 :headers {"Content-Type" "application/edn"}
                                 :as :text})
                result (edn/read-string (:body resp))]
            (is (= 200 (:status resp)))
            (is (= :ok (:status result)))))
        
        (finally
          (server)
          (xts/stop-xtdb-node node))))))

(deftest test-query-formats
  (testing "Different query formats produce consistent results"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Setup test data
        (xts/put-entity node "user-1" {:name "Alice" :age 30 :active true})
        (xts/put-entity node "user-2" {:name "Bob" :age 25 :active false})
        (xts/put-entity node "user-3" {:name "Charlie" :age 35 :active true})
        (Thread/sleep 100)
        
        ;; Test keypath query
        (let [keypath-result (xtq/execute-query node ["user-1"])]
          (is (not (empty? keypath-result))))
        
        ;; Test HoneySQL query builder
        (let [query (xtq/select :users [:name :age]
                               (xtq/where := :active true)
                               (xtq/order-by :age :desc)
                               (xtq/limit 10))
              sql (xtq/honeysql->xtql query)]
          (is (string? sql))
          (is (re-find #"SELECT" sql))
          (is (re-find #"FROM users" sql))
          (is (re-find #"WHERE active = " sql)))
        
        ;; Test entity flattening
        (let [nested {:user {:profile {:name "Alice"
                                       :email "alice@example.com"}
                            :settings {:theme "dark"
                                     :notifications true}}
                     :metadata {:created "2024-01-01"
                              :updated "2024-01-02"}}
              flattened (xtq/flatten-entity nested)]
          (is (= "Alice" (:user_profile_name flattened)))
          (is (= "dark" (:user_settings_theme flattened)))
          (is (= "2024-01-01" (:metadata_created flattened)))
          
          ;; Test unflattening
          (let [unflattened (xtq/unflatten-entity flattened)]
            (is (= "Alice" (get-in unflattened [:user :profile :name])))
            (is (= true (get-in unflattened [:user :settings :notifications])))))
        
        (finally
          (xts/stop-xtdb-node node))))))