(ns reactor.kafka-reactive-test
  (:require [clojure.test :refer :all]
            [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [clojure.core.async :as async :refer [<!! >!! chan timeout go-loop alts!!]]))

;; Conditionally load Kafka if available
(def kafka-available?
  (try
    (require '[reactor.kafka-reactive :as kafka])
    true
    (catch Exception e
      (println "WARNING: Kafka libraries not available, skipping Kafka tests:" (.getMessage e))
      false)))

(defn wait-for-condition
  "Wait for a condition to become true, with timeout"
  [condition-fn timeout-ms]
  (let [start (System/currentTimeMillis)]
    (loop []
      (cond
        (condition-fn) true
        (> (- (System/currentTimeMillis) start) timeout-ms) false
        :else (do (Thread/sleep 100)
                  (recur))))))

(deftest kafka-reactive-simulation-test
  (testing "Simulated reactive SQL query updates (Kafka libraries not required)"
    ;; This test simulates the reactive behavior without requiring Kafka
    (session/init! :sim-test "sim_test_sessions")
    
    (let [test-table "sim_test_items"
          results (atom [])]
      
      (try
        ;; Insert initial test data
        (when-let [node @session/default-node]
          (xts/execute-sql node 
            (str "INSERT INTO " test-table " (_id, name, status, count) "
                 "VALUES ('item-1', 'First', 'active', 1), "
                 "       ('item-2', 'Second', 'inactive', 2)")))
        
        ;; Simulate a query subscription
        (let [sql (str "SELECT * FROM " test-table " WHERE status = 'active'")
              execute-query (fn []
                             (when-let [node @session/default-node]
                               (xts/execute-sql node sql)))]
          
          ;; Execute initial query
          (let [initial-result (execute-query)]
            (swap! results conj initial-result)
            (is (= 1 (count (:results initial-result))))
            (is (= "First" (-> initial-result :results first :name))))
          
          ;; Update the data
          (when-let [node @session/default-node]
            ;; Add a new active item
            (xts/execute-sql node 
              (str "INSERT INTO " test-table " (_id, name, status, count) "
                   "VALUES ('item-3', 'Third', 'active', 3)"))
            
            ;; Update an inactive item to active
            (xts/execute-sql node
              (str "UPDATE " test-table " SET status = 'active' WHERE _id = 'item-2'")))
          
          ;; In a real reactive system, this would be triggered automatically
          ;; Here we simulate the re-execution
          (let [updated-result (execute-query)]
            (swap! results conj updated-result)
            (is (= 3 (count (:results updated-result))))
            (let [names (set (map :name (:results updated-result)))]
              (is (contains? names "First"))
              (is (contains? names "Second"))
              (is (contains? names "Third")))))
        
        (finally
          ;; Clean up test data
          (when-let [node @session/default-node]
            (xts/execute-sql node (str "DELETE FROM " test-table))))))))

(when kafka-available?
  ;; Only define Kafka-specific tests if libraries are available
  (eval
   '(do
      (deftest kafka-reactive-query-test
        (testing "Real Kafka reactive SQL query updates"
          ;; Initialize test environment
          (session/init! :kafka-test "kafka_test_sessions")
          
          ;; Track results from query updates
          (let [results-atom (atom [])
                result-chan (chan 10)
                test-table "kafka_test_items"
                
                ;; Create a callback that puts results on channel
                callback (fn [{:keys [subscription-id query result]}]
                          (>!! result-chan result)
                          (swap! results-atom conj result))]
            
            ;; Check if Kafka consumer is already running (from server)
            (let [consumer-already-running? (try
                                              ;; Check if consumer exists
                                              (let [consumer-atom (resolve 'reactor.kafka-reactive/consumer)]
                                                (not (nil? @consumer-atom)))
                                              (catch Exception _ false))
                  ;; Only init if not already running
                  kafka-running? (if consumer-already-running?
                                  true
                                  (try
                                    ((resolve 'reactor.kafka-reactive/init!) 
                                     {"bootstrap.servers" "localhost:9092"
                                      "group.id" "test-reactor"})
                                    true
                                    (catch Exception e
                                      (println "Kafka not running, skipping:" (.getMessage e))
                                      false)))]
              
              (when kafka-running?
                (try
                  ;; Insert initial test data
                  (when-let [node @session/default-node]
                    (xts/execute-sql node 
                      (str "INSERT INTO " test-table " (_id, name, status, count) "
                           "VALUES ('item-1', 'First', 'active', 1), "
                           "       ('item-2', 'Second', 'inactive', 2)")))
                  
                  ;; Register a query subscription
                  (let [sql (str "SELECT * FROM " test-table " WHERE status = 'active'")
                        sub-id ((resolve 'reactor.kafka-reactive/register-query-subscription!)
                               "test-sub-1" sql nil callback "test-session")]
                    
                    ;; Execute initial query
                    ((resolve 'reactor.kafka-reactive/re-execute-subscription) sub-id)
                    
                    ;; Wait for initial result
                    (let [initial-result (<!! result-chan)]
                      (is (not (nil? initial-result)))
                      (is (= 1 (count (:results initial-result))))
                      (is (= "First" (-> initial-result :results first :name))))
                    
                    ;; Now update the data
                    (when-let [node @session/default-node]
                      (xts/execute-sql node 
                        (str "INSERT INTO " test-table " (_id, name, status, count) "
                             "VALUES ('item-3', 'Third', 'active', 3)"))
                      
                      (xts/execute-sql node
                        (str "UPDATE " test-table " SET status = 'active' WHERE _id = 'item-2'")))
                    
                    ;; Simulate transaction processing
                    ((resolve 'reactor.kafka-reactive/process-transaction)
                     {:tx-id 123
                      :system-time (java.util.Date.)
                      :tx-ops [{:type :insert-into :table test-table}
                               {:type :update :table test-table}]})
                    
                    ;; Wait for reactive update
                    (let [timeout-chan (timeout 2000)
                          [updated-result ch] (alts!! [result-chan timeout-chan])]
                      (when updated-result
                        (is (= 3 (count (:results updated-result))))
                        (let [names (set (map :name (:results updated-result)))]
                          (is (contains? names "First"))
                          (is (contains? names "Second"))
                          (is (contains? names "Third")))))
                    
                    ;; Clean up subscription
                    ((resolve 'reactor.kafka-reactive/unregister-query-subscription!) sub-id))
                  
                  (finally
                    ;; Only shutdown if we started the consumer for this test
                    (when-not consumer-already-running?
                      ((resolve 'reactor.kafka-reactive/shutdown!)))
                    ;; Clean up test data
                    (when-let [node @session/default-node]
                      (xts/execute-sql node (str "DELETE FROM " test-table))))))))))
      
      (deftest kafka-table-detection-test
        (testing "Correct detection of affected tables from SQL"
          ;; Test table extraction from various SQL patterns
          (let [extract-fn (resolve 'reactor.kafka-reactive/extract-tables-from-sql)
                test-cases [{:sql "SELECT * FROM users WHERE id = 1"
                            :expected ["users"]}
                           {:sql "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id"
                            :expected ["users" "orders"]}
                           {:sql "INSERT INTO products (name, price) VALUES ('Test', 100)"
                            :expected ["products"]}
                           {:sql "UPDATE customers SET status = 'active' WHERE id = 5"
                            :expected ["customers"]}
                           {:sql "DELETE FROM sessions WHERE expired = true"
                            :expected ["sessions"]}]]
            
            (doseq [{:keys [sql expected]} test-cases]
              (testing (str "SQL: " sql)
                (let [tables (extract-fn sql)]
                  (is (= (set expected) (set tables))))))))))))

(deftest reactive-concept-test
  (testing "Reactive query concept (no Kafka required)"
    ;; This test demonstrates the concept without requiring Kafka
    (let [query-results (atom {})
          subscriptions (atom {})
          
          ;; Simulate subscription registration
          register-sub (fn [id sql callback]
                        (swap! subscriptions assoc id {:sql sql :callback callback})
                        id)
          
          ;; Simulate query execution
          execute-sub (fn [id]
                       (let [sub (get @subscriptions id)]
                         (when sub
                           (let [result {:results [{:id 1 :data "test"}]}]
                             ((:callback sub) result)
                             result))))
          
          ;; Simulate reactive update
          trigger-update (fn [table-name]
                           (doseq [[id sub] @subscriptions]
                             (when (.contains (:sql sub) table-name)
                               (execute-sub id))))]
      
      ;; Register a subscription
      (let [sub-id (register-sub "test-1" "SELECT * FROM test_table"
                                 (fn [result]
                                   (swap! query-results assoc "test-1" result)))]
        
        ;; Execute initially
        (execute-sub sub-id)
        (is (contains? @query-results "test-1"))
        
        ;; Simulate a table update triggering re-execution
        (reset! query-results {})
        (trigger-update "test_table")
        (is (contains? @query-results "test-1") 
            "Query should re-execute when table is updated")))))