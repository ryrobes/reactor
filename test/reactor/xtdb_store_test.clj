(ns reactor.xtdb-store-test
  (:require [clojure.test :refer :all]
            [reactor.xtdb-store :as xts]
            [xtdb.api :as xt])
  (:import [java.time Duration]))

(deftest test-xtdb-node-lifecycle
  (testing "Can start and stop XTDB 2.0 node"
    (let [node (xts/start-xtdb-node)]
      (is (not (nil? node)))
      ;; In XTDB 2.0, nodes don't implement PXtdb
      (xts/stop-xtdb-node node))))

(deftest test-basic-entity-operations
  (testing "Can put and get entities with XTDB 2.0"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Clean up any existing test data
        (xts/delete-entity node "test_entities" "entity-1")
        
        ;; Put an entity - using table-based approach
        (xts/put-entity node "test_entities" "entity-1" {:name "Test" :value 42})
          
        ;; Get the entity
        (let [entity (xts/get-entity node "test_entities" "entity-1")]
          (is (= "Test" (:name entity)))
          (is (= 42 (:value entity)))
          (is (= "entity-1" (:_id entity))))
        
        (finally
          (xts/stop-xtdb-node node)))))
  
  (testing "Can delete entities"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Clean up any existing test data
        (xts/delete-entity node "test_entities" "entity-2")
        
        ;; Put an entity
        (xts/put-entity node "test_entities" "entity-2" {:name "Delete Me"})
        (is (not (nil? (xts/get-entity node "test_entities" "entity-2"))))
          
        ;; Delete it
        (xts/delete-entity node "test_entities" "entity-2")
        ;; In XTDB 2.0, deletes might not be immediately visible
        (Thread/sleep 100)
        (is (nil? (xts/get-entity node "test_entities" "entity-2")))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-entity-history
  (testing "Can track entity history in XTDB 2.0"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Clean up any existing test data first
        (xts/delete-entity node "test_entities" "history-test")
        (Thread/sleep 100)
        
        ;; Create initial version
        (xts/put-entity node "test_entities" "history-test" {:version 1})
        (Thread/sleep 100)
        
        ;; Update it
        (xts/put-entity node "test_entities" "history-test" {:version 2})
        (Thread/sleep 100)
        
        ;; Update again
        (xts/put-entity node "test_entities" "history-test" {:version 3})
        (Thread/sleep 100)
        
        ;; Get history
        (let [history (xts/entity-history node "test_entities" "history-test")]
          ;; Should have at least the current version
          (is (seq history))
          ;; Most recent should be version 3
          (is (= 3 (:version (first history)))))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-xtdb-atom
  (testing "XTDBAtom behaves like an atom"
    (let [node (xts/start-xtdb-node)
          ;; Clean up any existing test data first
          _ (xts/delete-entity node "test_atoms" "atom-1")
          atom (xts/create-atom node "test_atoms" "atom-1" {:counter 0})]
      (try
        ;; Can deref
        (is (= 0 (:counter @atom)))
        
        ;; Can swap!
        (swap! atom update :counter inc)
        (is (= 1 (:counter @atom)))
        
        ;; Can reset!
        (reset! atom {:counter 10})
        (is (= 10 (:counter @atom)))
        
        ;; Can compare-and-set
        (let [result (compare-and-set! atom {:counter 10} {:counter 20})]
          (is result)
          (is (= 20 (:counter @atom))))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-sql-execution
  (testing "Can execute SQL queries and mutations"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Clean up any existing test data
        (xts/execute-sql node "DELETE FROM products WHERE _id = 'prod-1'")
        
        ;; Insert data using SQL with RECORDS syntax
        (let [result (xts/execute-sql node 
                                      "INSERT INTO products RECORDS {_id: 'prod-1', name: 'Widget', price: 99.99}")]
          (is (:success result)))
        
        ;; Query using SQL
        (let [result (xts/execute-sql node "SELECT * FROM products WHERE _id = ?" "prod-1")]
          (is (seq (:results result)))
          (is (= "Widget" (:name (first (:results result))))))
        
        (finally
          (xts/stop-xtdb-node node))))))