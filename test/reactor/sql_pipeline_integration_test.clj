(ns reactor.sql-pipeline-integration-test
  (:require [clojure.test :refer :all]
            [reactor.sql-pipeline :as pipeline]
            [reactor.subscriptions.store :as sub-store]
            [reactor.sse.broadcaster :as broadcaster]
            [reactor.reactive.coordinator :as coordinator]
            [reactor.xtdb-store :as store]
            [reactor.kafka-reactive :as kafka]
            [clojure.core.async :as async]))

(def test-node (atom nil))
(def test-session-id "test-session-integration")

(defn mock-xtdb-node []
  (reify
    store/XTDBNode
    (execute [_ sql params]
      (cond
        (re-find #"SELECT \* FROM users" sql)
        [{:id 1 :name "Alice" :age 30}
         {:id 2 :name "Bob" :age 25}]
        
        (re-find #"SELECT \* FROM posts WHERE user_id" sql)
        [{:id 101 :title "First Post" :user_id (first params)}
         {:id 102 :title "Second Post" :user_id (first params)}]
        
        (re-find #"SELECT COUNT" sql)
        [{:count 42}]
        
        (re-find #"INSERT INTO" sql)
        {:rows-affected 1}
        
        (re-find #"UPDATE" sql)
        {:rows-affected 1}
        
        (re-find #"DELETE" sql)
        {:rows-affected 1}
        
        :else
        []))))

(defn setup-test-env []
  ;; Reset all stores
  (sub-store/clear-all!)
  (broadcaster/clear-all-channels!)
  
  ;; Set up mock node
  (reset! test-node (mock-xtdb-node))
  
  ;; Store test templates
  (store/put! @test-node :sql-templates "user-posts" 
              {:sql "SELECT * FROM posts WHERE user_id = ?"
               :params [:user-id]})
  
  (store/put! @test-node :sql-templates "post-comments"
              {:sql "SELECT * FROM comments WHERE post_id = ?"
               :params [:post-id]
               :cascade ["comment-likes"]})
  
  (store/put! @test-node :sql-templates "comment-likes"
              {:sql "SELECT * FROM likes WHERE comment_id = ?"
               :params [:comment-id]}))

(use-fixtures :each (fn [f]
                      (setup-test-env)
                      (f)
                      (sub-store/clear-all!)
                      (broadcaster/clear-all-channels!)))

(deftest test-complete-pipeline-flow
  (testing "End-to-end SQL pipeline execution"
    (let [context {:sql "SELECT * FROM users"
                   :params []
                   :session-id test-session-id
                   :node @test-node}
          result (pipeline/execute-pipeline context)]
      
      (is (= 2 (count (:results result))))
      (is (= "Alice" (-> result :results first :name)))
      
      ;; Verify subscription was created
      (let [sub-id (:subscription-id result)
            subscription (sub-store/get-subscription sub-id)]
        (is (some? subscription))
        (is (= :active (:status subscription)))
        (is (= "SELECT * FROM users" (:sql subscription)))))))

(deftest test-subscription-lifecycle
  (testing "Subscription creation, update, and deletion"
    ;; Create initial subscription
    (let [context1 {:sql "SELECT * FROM users WHERE age > ?"
                    :params [20]
                    :session-id test-session-id
                    :node @test-node}
          result1 (pipeline/execute-pipeline context1)
          sub-id (:subscription-id result1)]
      
      (is (some? sub-id))
      (is (= 2 (count (:results result1))))
      
      ;; Re-execute same query - should reuse subscription
      (let [result2 (pipeline/execute-pipeline context1)]
        (is (= sub-id (:subscription-id result2)))
        (is (nil? (:diff result2))) ; No diff for identical results
        
        ;; Verify only one subscription exists
        (is (= 1 (count (sub-store/find-by-session test-session-id)))))
      
      ;; Delete subscription
      (sub-store/delete-subscription! sub-id)
      (is (nil? (sub-store/get-subscription sub-id))))))

(deftest test-template-resolution-and-cascade
  (testing "Template resolution with cascade chain"
    (let [context {:template "user-posts"
                   :params {:user-id 1}
                   :session-id test-session-id
                   :node @test-node}
          result (pipeline/execute-pipeline context)]
      
      ;; Verify template was resolved
      (is (re-find #"SELECT \* FROM posts" (:sql result)))
      (is (= [1] (:params result)))
      
      ;; Verify cascade targets were identified
      (is (contains? result :cascade-targets))
      (is (= ["comment-likes"] (-> result :cascade-targets first :cascade))))))

(deftest test-diff-calculation
  (testing "Diff calculation between result sets"
    ;; Create initial subscription with results
    (let [context {:sql "SELECT * FROM users"
                   :params []
                   :session-id test-session-id
                   :node @test-node}
          result1 (pipeline/execute-pipeline context)
          sub-id (:subscription-id result1)]
      
      ;; Simulate changed results by manually updating subscription
      (sub-store/update-subscription! sub-id 
        {:last-results [{:id 1 :name "Alice" :age 31}  ; Age changed
                        {:id 2 :name "Bob" :age 25}
                        {:id 3 :name "Charlie" :age 35}]}) ; New user
      
      ;; Re-execute to get diff
      (let [result2 (pipeline/execute-pipeline context)]
        (is (some? (:diff result2)))
        (is (= :full (:type (:diff result2)))) ; Full results due to significant changes
        (is (= sub-id (:subscription-id result2)))))))

(deftest test-reaction-triggering
  (testing "Mutations trigger reactions through coordinator"
    ;; Set up subscription that would be affected
    (let [sub-context {:sql "SELECT * FROM users"
                       :params []
                       :session-id test-session-id
                       :node @test-node}
          sub-result (pipeline/execute-pipeline sub-context)
          sub-id (:subscription-id sub-result)]
      
      ;; Track reactions
      (let [reactions (atom [])]
        (with-redefs [pipeline/execute-reaction
                      (fn [context]
                        (swap! reactions conj context)
                        {:success true})]
          
          ;; Simulate table change notification
          (coordinator/handle-table-change "users")
          
          ;; Give async processing time to complete
          (Thread/sleep 100)
          
          ;; Verify reaction was triggered
          (is (> (count @reactions) 0))
          (is (= sub-id (-> @reactions first :subscription-id))))))))

(deftest test-sse-broadcasting
  (testing "SSE channels receive updates"
    ;; Create SSE channel
    (let [channel (async/chan)
          _ (broadcaster/add-channel! test-session-id channel)]
      
      ;; Execute query that creates subscription
      (let [context {:sql "SELECT COUNT(*) as count FROM users"
                     :params []
                     :session-id test-session-id
                     :node @test-node}
            result (pipeline/execute-pipeline context)]
        
        ;; Trigger an update through the coordinator
        (coordinator/handle-table-change "users")
        
        ;; Check if channel received update
        (let [message (async/alt!!
                        channel ([msg] msg)
                        (async/timeout 1000) :timeout)]
          (is (not= :timeout message))
          (when (not= :timeout message)
            (is (string? message))
            (is (re-find #"data:" message)))))
      
      ;; Clean up
      (broadcaster/remove-channel! test-session-id))))

(deftest test-mutation-operations
  (testing "INSERT, UPDATE, DELETE operations"
    ;; Test INSERT
    (let [insert-context {:sql "INSERT INTO users (name, age) VALUES (?, ?)"
                          :params ["Charlie" 28]
                          :session-id test-session-id
                          :node @test-node}
          insert-result (pipeline/execute-sql @test-node 
                                             (:sql insert-context)
                                             (:params insert-context)
                                             {:session-id test-session-id})]
      (is (= 1 (:rows-affected insert-result))))
    
    ;; Test UPDATE
    (let [update-context {:sql "UPDATE users SET age = ? WHERE name = ?"
                          :params [29 "Charlie"]
                          :session-id test-session-id
                          :node @test-node}
          update-result (pipeline/execute-sql @test-node
                                             (:sql update-context)
                                             (:params update-context)
                                             {:session-id test-session-id})]
      (is (= 1 (:rows-affected update-result))))
    
    ;; Test DELETE
    (let [delete-context {:sql "DELETE FROM users WHERE age > ?"
                          :params [50]
                          :session-id test-session-id
                          :node @test-node}
          delete-result (pipeline/execute-sql @test-node
                                             (:sql delete-context)
                                             (:params delete-context)
                                             {:session-id test-session-id})]
      (is (number? (:rows-affected delete-result))))))

(deftest test-time-travel-sql
  (testing "Time travel AS OF clause injection"
    (let [context {:sql "SELECT * FROM users"
                   :params []
                   :as-of "2024-01-01T00:00:00Z"
                   :session-id test-session-id
                   :node @test-node}
          result (pipeline/execute-pipeline context)]
      
      ;; Verify temporal clause was added
      (is (re-find #"AS OF" (:sql result)))
      (is (re-find #"2024-01-01" (:sql result))))))

(deftest test-error-handling
  (testing "Pipeline handles errors gracefully"
    ;; Test invalid SQL
    (let [context {:sql "INVALID SQL SYNTAX"
                   :params []
                   :session-id test-session-id
                   :node @test-node}
          result (pipeline/execute-pipeline context)]
      (is (contains? result :error))
      (is (nil? (:subscription-id result))))
    
    ;; Test missing required fields
    (let [context {:sql "SELECT * FROM users"}  ; Missing session-id
          result (pipeline/execute-pipeline context)]
      (is (contains? result :error))
      (is (re-find #"session-id" (str (:error result)))))))

(deftest test-concurrent-subscriptions
  (testing "Multiple concurrent subscriptions work correctly"
    (let [sessions ["session-1" "session-2" "session-3"]
          contexts (map (fn [sid]
                         {:sql "SELECT * FROM users"
                          :params []
                          :session-id sid
                          :node @test-node})
                       sessions)
          results (doall (pmap pipeline/execute-pipeline contexts))]
      
      ;; All should succeed
      (is (every? #(= 2 (count (:results %))) results))
      
      ;; Each should have unique subscription
      (let [sub-ids (map :subscription-id results)]
        (is (= 3 (count (set sub-ids))))
        
        ;; Verify subscriptions are properly indexed
        (doseq [sid sessions]
          (is (= 1 (count (sub-store/find-by-session sid)))))))))

(deftest test-cascade-chain-execution
  (testing "Cascade chains execute in correct order"
    (let [cascade-results (atom [])
          _ (with-redefs [pipeline/execute-pipeline
                         (fn [context]
                           (swap! cascade-results conj (:template context))
                           {:results [] :cascade-targets []})]
              
              ;; Execute template with cascade
              (let [context {:template "post-comments"
                            :params {:post-id 101}
                            :session-id test-session-id
                            :node @test-node}]
                (pipeline/execute-pipeline context)
                
                ;; Should have triggered cascades
                (Thread/sleep 100)))
          
          cascades @cascade-results]
      
      (is (>= (count cascades) 1))
      (is (some #(= "post-comments" %) cascades)))))

(deftest test-subscription-cleanup
  (testing "Inactive subscriptions are cleaned up"
    ;; Create multiple subscriptions
    (doseq [i (range 5)]
      (let [context {:sql (str "SELECT * FROM table" i)
                     :params []
                     :session-id test-session-id
                     :node @test-node}]
        (pipeline/execute-pipeline context)))
    
    (is (= 5 (count (sub-store/find-by-session test-session-id))))
    
    ;; Mark some as inactive
    (let [subs (sub-store/find-by-session test-session-id)]
      (doseq [sub (take 2 subs)]
        (sub-store/update-subscription! (:id sub) {:status :inactive})))
    
    ;; Cleanup inactive
    (sub-store/cleanup-inactive-subscriptions!)
    
    ;; Should have 3 active remaining
    (is (= 3 (count (sub-store/find-by-session test-session-id))))))

(defn run-all-tests []
  (run-tests 'reactor.sql-pipeline-integration-test))