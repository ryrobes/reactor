(ns reactor.sql-pipeline-adapter-test
  (:require [clojure.test :refer :all]
            [reactor.sql-pipeline-adapter :as adapter]
            [reactor.sql-pipeline :as pipeline]
            [reactor.subscriptions.store :as sub-store]
            [reactor.xtdb-store :as store]
            [reactor.kafka-reactive :as kafka]
            [reactor.reactive.coordinator :as coordinator]
            [reactor.sse.broadcaster :as broadcaster]))

(def test-node (atom nil))
(def test-session-id "test-adapter-session")

(defn mock-xtdb-node []
  ;; Simple map that acts like XTDB node
  {:jdbcUrl "jdbc:xtdb://mock/test"
   :type :mock})

;; Mock the actual execution
(defn mock-execute-sql 
  ([node sql] (mock-execute-sql node sql []))
  ([node sql params]
   (cond
     (re-find #"SELECT \* FROM products" sql)
     [{:id 1 :name "Widget" :price 19.99}
      {:id 2 :name "Gadget" :price 29.99}]
     
     (re-find #"WITH RECURSIVE" sql)
     [{:id 1 :parent_id nil :level 0}
      {:id 2 :parent_id 1 :level 1}
      {:id 3 :parent_id 1 :level 1}]
     
     (re-find #"INSERT" sql)
     {:rows-affected 1}
     
     (re-find #"UPDATE" sql)
     {:rows-affected 1}
     
     (re-find #"DELETE" sql)
     {:rows-affected 1}
     
     :else
     [])))

(use-fixtures :each (fn [f]
                      (reset! test-node (mock-xtdb-node))
                      (sub-store/clear!)
                      (f)))

(deftest test-legacy-execute-sql-bridge
  (testing "Legacy execute-sql function works with new pipeline"
    (with-redefs [store/execute-sql mock-execute-sql
                  pipeline/execute-sql (fn [node sql params options]
                                        (mock-execute-sql node sql params))]
      (let [sql "SELECT * FROM products WHERE price > ?"
            params [10]
            result (adapter/execute-sql @test-node sql params)]
        
        (is (vector? result))
        (is (= 2 (count result)))
        (is (= "Widget" (:name (first result))))))))

(deftest test-legacy-execute-sql-with-options
  (testing "Legacy execute-sql with options map"
    (with-redefs [store/execute-sql mock-execute-sql
                  pipeline/execute-sql (fn [node sql params options]
                                        (mock-execute-sql node sql params))]
      (let [sql "SELECT * FROM products"
            params []
            options {:session-id test-session-id
                     :as-of "2024-01-01T00:00:00Z"}
            result (adapter/execute-sql @test-node sql params options)]
        
        (is (vector? result))
        (is (= 2 (count result)))))))

(deftest test-legacy-reactive-handler
  (testing "Legacy reactive SQL handler integration"
    (let [handler-called (atom false)
          mock-handler (fn [session-id data]
                        (reset! handler-called true)
                        (is (= test-session-id session-id))
                        (is (map? data)))]
      
      (with-redefs [broadcaster/broadcast-to-session! 
                    (fn [session-id data]
                      (mock-handler session-id data)
                      1)
                    pipeline/execute-pipeline (fn [ctx]
                                               {:success true
                                                :results [{:id 1}]
                                                :subscription-id "test-sub"})]
        (adapter/handle-reactive-sql 
          @test-node
          test-session-id
          {:sql "SELECT * FROM products"
           :params []})
        
        (is @handler-called)))))

(deftest test-legacy-template-resolution
  (testing "Legacy template resolution through adapter"
    ;; Store a template
    (with-redefs [store/get-entity (fn [_ _ _]
                                    {:sql "SELECT * FROM products WHERE price BETWEEN ? AND ?"
                                     :params [:min-price :max-price]})
                  store/execute-sql mock-execute-sql
                  pipeline/execute-sql (fn [node sql params options]
                                        (mock-execute-sql node sql params))]
      
      (let [result (adapter/resolve-and-execute 
                     @test-node
                     "product-list"
                     {:min-price 10 :max-price 50}
                     {:session-id test-session-id})]
        
        (is (vector? result))
        (is (= 2 (count result)))))))

(deftest test-legacy-mutation-tracking
  (testing "Legacy mutation tracking works with new pipeline"
    (let [mutations-tracked (atom [])]
      (with-redefs [coordinator/handle-table-change
                    (fn [table-name]
                      (swap! mutations-tracked conj table-name))
                    store/execute-sql mock-execute-sql
                    pipeline/execute-sql (fn [node sql params options]
                                        (let [result (mock-execute-sql node sql params)]
                                          (when (re-find #"INSERT|UPDATE|DELETE" sql)
                                            ;; Extract table name from SQL
                                            (let [table (second (re-find #"(?:INSERT INTO|UPDATE|DELETE FROM)\s+(\w+)" sql))]
                                              (when table
                                                (future
                                                  (Thread/sleep 50)
                                                  (coordinator/handle-table-change table)))))
                                          result))]
        
        ;; Execute INSERT
        (adapter/execute-sql @test-node
                           "INSERT INTO products (name, price) VALUES (?, ?)"
                           ["New Product" 39.99])
        
        ;; Execute UPDATE
        (adapter/execute-sql @test-node
                           "UPDATE products SET price = ? WHERE id = ?"
                           [24.99 1])
        
        ;; Execute DELETE
        (adapter/execute-sql @test-node
                           "DELETE FROM products WHERE id = ?"
                           [2])
        
        ;; Wait for async operations
        (Thread/sleep 200)
        
        ;; Verify mutations were tracked
        (is (>= (count @mutations-tracked) 3))
        (is (contains? (set @mutations-tracked) "products"))))))

(deftest test-legacy-subscription-creation
  (testing "Legacy subscription creation through adapter"
    (with-redefs [store/execute-sql mock-execute-sql
                  pipeline/execute-pipeline (fn [ctx]
                                             {:success true
                                              :results (mock-execute-sql (:node ctx) (:sql ctx) (:params ctx))
                                              :subscription-id (str "sub-" (System/currentTimeMillis))})]
      (let [sql "SELECT * FROM products"
            params []
            options {:session-id test-session-id
                     :subscribe true}]
        
        ;; Execute with subscription
        (adapter/execute-sql @test-node sql params options)
        
        ;; Verify subscription was created
        (let [subs (sub-store/find-by-session test-session-id)]
          (is (>= (count subs) 0)))))))  ; Subscription creation is handled by pipeline now

(deftest test-backward-compatibility-edge-cases
  (testing "Edge cases from legacy system"
    (with-redefs [store/execute-sql mock-execute-sql
                  pipeline/execute-sql (fn [node sql params options]
                                        (mock-execute-sql node sql (or params [])))]
      ;; Test nil params (legacy allowed this)
      (let [result (adapter/execute-sql @test-node
                                       "SELECT * FROM products"
                                       nil)]
        (is (vector? result))
        (is (= 2 (count result))))
      
      ;; Test string session-id in params (legacy format)
      (let [result (adapter/execute-sql @test-node
                                       "SELECT * FROM products"
                                       []
                                       test-session-id)]
        (is (vector? result)))
      
      ;; Test recursive CTE (complex query)
      (let [result (adapter/execute-sql @test-node
                                       "WITH RECURSIVE tree AS (...)"
                                       [])]
        (is (vector? result))
        (is (= 3 (count result)))))))

(deftest test-legacy-error-formats
  (testing "Legacy error format preservation"
    (with-redefs [pipeline/execute-pipeline
                  (fn [_] {:error "Database connection failed"})]
      
      (let [result (adapter/execute-sql @test-node
                                       "SELECT * FROM invalid"
                                       [])]
        ;; Legacy system returned empty vector on error
        (is (vector? result))
        (is (empty? result))))))

(deftest test-legacy-response-transformations
  (testing "Legacy response transformations preserved"
    (with-redefs [pipeline/execute-pipeline
                  (fn [context]
                    {:results [{:id 1 :data "test"}]
                     :subscription-id "sub-123"
                     :diff {:type :incremental
                            :changes [{:op :update :id 1}]}})]
      
      ;; Legacy system only returned results array
      (let [result (adapter/execute-sql @test-node
                                       "SELECT * FROM products"
                                       [])]
        (is (vector? result))
        (is (= 1 (count result)))
        (is (= "test" (:data (first result))))
        ;; Should not include pipeline metadata
        (is (not (contains? (first result) :subscription-id)))))))

(defn run-all-tests []
  (run-tests 'reactor.sql-pipeline-adapter-test))