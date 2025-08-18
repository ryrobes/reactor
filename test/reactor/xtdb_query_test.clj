(ns reactor.xtdb-query-test
  (:require [clojure.test :refer :all]
            [reactor.xtdb-store :as xts]
            [reactor.xtdb-query :as xtq]
            [xtdb.api :as xt]))

(deftest test-entity-table-conversion
  (testing "Entity keys convert to SQL table names"
    (is (= "session_123_todos" (xtq/entity->table-name :session.123/todos)))
    (is (= "global_config" (xtq/entity->table-name :global/config)))
    (is (= "users_active_list" (xtq/entity->table-name :users/active.list)))
    (is (= "todos_1" (xtq/entity->table-name "todos-1")))
    (is (= "users_alice" (xtq/entity->table-name "users-alice")))))

(deftest test-entity-flattening
  (testing "Nested maps flatten correctly"
    (let [nested {:user {:name "John"
                         :address {:street "123 Main"
                                  :city "Boston"}}
                  :active true}
          flattened (xtq/flatten-entity nested)]
      (is (= "John" (:user_name flattened)))
      (is (= "123 Main" (:user_address_street flattened)))
      (is (= "Boston" (:user_address_city flattened)))
      (is (= true (:active flattened)))))
  
  (testing "Flattened maps unflatten correctly"
    (let [flat {:user_name "John"
                :user_age 30
                :status "active"}
          unflat (xtq/unflatten-entity flat)]
      (is (= {:user {:name "John" :age 30}
              :status "active"}
             unflat)))))

(deftest test-keypath-to-datalog
  (testing "Keypaths convert to Datalog queries"
    (let [q1 (xtq/keypath->datalog [:todos])
          q2 (xtq/keypath->datalog [:todos :active])]
      (is (map? q1))
      (is (contains? q1 :find))
      (is (contains? q1 :where))
      (is (map? q2))
      (is (= 2 (count (:where q2)))))))

(deftest test-honeysql-conversion
  (testing "HoneySQL converts to SQL"
    (let [hsql {:select [:id :text]
                :from :todos
                :where [:= :completed false]
                :limit 10}
          sql (xtq/honeysql->xtql hsql)]
      (is (string? sql))
      (is (re-find #"SELECT" sql))
      (is (re-find #"FROM todos" sql))
      (is (re-find #"WHERE" sql))
      (is (re-find #"LIMIT 10" sql)))))

(deftest test-query-builder-dsl
  (testing "Query builder creates valid HoneySQL"
    (let [query (xtq/select :todos [:id :text :completed]
                           (xtq/where := :completed false)
                           (xtq/order-by :created-at :desc)
                           (xtq/limit 5))]
      (is (= [:id :text :completed] (:select query)))
      (is (= :todos (:from query)))
      (is (= [:= :completed false] (:where query)))
      (is (= [:created-at :desc] (:order-by query)))
      (is (= 5 (:limit query))))))

(deftest test-query-execution
  (testing "Different query formats execute"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Add test data
        (let [tx1 (xts/put-entity node "todos-1" 
                                  {:text "Test todo" :completed false})
              tx2 (xts/put-entity node "todos-2" 
                                  {:text "Done todo" :completed true})]
          (xt/await-tx node tx1)
          (xt/await-tx node tx2)
          (xt/sync node)
          
          ;; Test keypath query
          (let [result (xtq/execute-query node ["todos-1"])]
            (is (not (empty? result))))
          
          ;; Test datalog query
          (let [result (xtq/execute-query node 
                        {:find '[(pull ?e [*])]
                         :where [['?e :completed false]]})]
            (is (not (empty? result)))))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-query-subscription
  (testing "Query subscriptions update on changes"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Initial data
        (let [tx (xts/put-entity node "test-counter" {:value 0})]
          (xt/await-tx node tx)
          
          ;; Subscribe with polling
          (let [changes (atom [])
                sub (xtq/subscribe-query 
                     node ["test-counter"]
                     :poll-ms 100
                     :on-change (fn [old new]
                                 (swap! changes conj {:old old :new new})))]
            
            ;; Initial value
            (is (not (nil? @sub)))
            
            ;; Update data
            (Thread/sleep 50)
            (let [tx (xts/put-entity node "test-counter" {:value 1})]
              (xt/await-tx node tx))
            
            ;; Wait for poll
            (Thread/sleep 150)
            
            ;; Should have detected change
            (is (pos? (count @changes)))
            
            ;; Clean up
            (xtq/close! sub)))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-materialized-views
  (testing "Entities sync to flattened tables"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Add nested entity
        (let [entity {:user {:name "Alice"
                             :email "alice@example.com"}
                      :settings {:theme "dark"
                                :notifications true}}
              tx (xts/put-entity node "users-alice" entity)]
          (xt/await-tx node tx)
          
          ;; Sync to table
          (xtq/sync-to-table node "users-alice")
          (xt/sync node)
          
          ;; Check flattened version exists
          (let [table-id (keyword (xtq/entity->table-name "users-alice") 
                                 "users-alice")
                flattened (xt/entity (xt/db node) table-id)]
            (is (not (nil? flattened)))
            (is (= "Alice" (:user_name flattened)))
            (is (= "dark" (:settings_theme flattened)))))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-explain-query
  (testing "Query explanation shows execution plan"
    (let [keypath-explain (xtq/explain-query [:todos :active])
          honeysql-explain (xtq/explain-query {:select [:*] :from :todos})
          sql-explain (xtq/explain-query "SELECT * FROM todos")]
      
      (is (= :keypath (:type keypath-explain)))
      (is (contains? keypath-explain :datalog))
      
      (is (= :honeysql (:type honeysql-explain)))
      (is (contains? honeysql-explain :sql))
      
      (is (= :sql (:type sql-explain)))
      (is (= "SELECT * FROM todos" (:query sql-explain))))))