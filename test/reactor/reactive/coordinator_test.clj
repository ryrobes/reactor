(ns reactor.reactive.coordinator-test
  (:require [clojure.test :refer :all]
            [reactor.reactive.coordinator :as coordinator]
            [reactor.subscriptions.store :as sub-store]
            [reactor.sql-pipeline :as pipeline]
            [reactor.sse.broadcaster :as broadcaster]
            [clojure.core.async :as async]))

(use-fixtures :each (fn [f]
                      (sub-store/clear!)
                      (broadcaster/disconnect-all!)
                      (f)))

(deftest test-handle-table-change
  (testing "Table changes trigger subscriptions"
    ;; Create active subscriptions for the table
    (sub-store/add!
      {:id "sub1"
       :sql "SELECT * FROM orders"
       :params []
       :session-id "session1"
       :status :active
       :tables ["orders"]})
    
    (sub-store/add!
      {:id "sub2"
       :sql "SELECT * FROM orders WHERE status = ?"
       :params ["pending"]
       :session-id "session2"
       :status :active
       :tables ["orders"]})
    
    ;; Inactive subscription should not trigger
    (sub-store/add!
      {:id "sub3"
       :sql "SELECT * FROM orders"
       :params []
       :session-id "session3"
       :status :inactive
       :tables ["orders"]})
    
    (let [reactions-executed (atom [])]
      (with-redefs [pipeline/execute-reaction
                    (fn [sub-id]
                      (swap! reactions-executed conj sub-id)
                      {:success true :results []})]
        
        ;; Trigger table change
        (coordinator/handle-table-change "orders")
        
        ;; Verify only active subscriptions were triggered
        (is (= 2 (count @reactions-executed)))
        (is (contains? (set @reactions-executed) "sub1"))
        (is (contains? (set @reactions-executed) "sub2"))
        (is (not (contains? (set @reactions-executed) "sub3"))))))

(deftest test-handle-multiple-tables
  (testing "Multiple table changes are handled correctly"
    ;; Create subscriptions for different tables
    (sub-store/add!
      {:id "orders-sub"
       :sql "SELECT * FROM orders"
       :params []
       :session-id "session1"
       :status :active
       :tables ["orders"]})
    
    (sub-store/add!
      {:id "inventory-sub"
       :sql "SELECT * FROM inventory"
       :params []
       :session-id "session2"
       :status :active
       :tables ["inventory"]})
    
    (sub-store/add!
      {:id "multi-sub"
       :sql "SELECT * FROM orders JOIN inventory"
       :params []
       :session-id "session3"
       :status :active
       :tables ["orders" "inventory"]})
    
    (let [reactions (atom [])]
      (with-redefs [pipeline/execute-reaction
                    (fn [sub-id]
                      (let [sub (sub-store/get-subscription sub-id)]
                        (swap! reactions conj 
                               {:sub-id sub-id
                                :tables (:tables sub)})
                        {:success true :results []}))]
        
        ;; Change orders table
        (coordinator/handle-table-change "orders")
        
        ;; Should trigger orders-sub and multi-sub
        (is (= 2 (count @reactions)))
        
        (reset! reactions [])
        
        ;; Change inventory table
        (coordinator/handle-table-change "inventory")
        
        ;; Should trigger inventory-sub and multi-sub
        (is (= 2 (count @reactions))))))))

(deftest test-process-subscriptions
  (testing "Process multiple subscriptions efficiently"
    (let [subs (for [i (range 5)]
                 {:id (str "sub" i)
                  :sql "SELECT * FROM orders"
                  :params []
                  :session-id (str "session" i)
                  :status :active
                  :tables ["orders"]})]
      
      ;; Create subscriptions
      (doseq [sub subs]
        (sub-store/add! sub))
      
      (let [processed (atom #{})]
        (with-redefs [pipeline/execute-reaction
                      (fn [sub-id]
                        (swap! processed conj sub-id)
                        {:success true :results [{:id 1}]})]
          
          ;; Process all subscriptions
          (coordinator/process-subscriptions subs)
          
          ;; All should be processed
          (is (= 5 (count @processed)))
          (is (every? #(contains? @processed (str "sub" %)) (range 5))))))))

(deftest test-error-handling
  (testing "Errors in reactions are handled gracefully"
    (sub-store/add!
      {:id "error-sub"
       :sql "SELECT * FROM orders"
       :params []
       :session-id "error-session"
       :status :active
       :tables ["orders"]})
    
    (let [error-count (atom 0)]
      (with-redefs [pipeline/execute-reaction
                    (fn [_]
                      (swap! error-count inc)
                      (throw (Exception. "Test error")))]
        
        ;; Should not throw despite error
        (is (try
              (coordinator/handle-table-change "orders")
              true
              (catch Exception _ false)))
        
        ;; Should have attempted to process
        (is (pos? @error-count))))))

(deftest test-broadcast-integration
  (testing "Successful reactions trigger broadcasts"
    ;; Set up SSE channel mock
    (let [broadcasts (atom [])]
      (with-redefs [broadcaster/broadcast-to-session!
                    (fn [session-id data]
                      (swap! broadcasts conj {:session session-id :data data})
                      1)]
        
        ;; Create subscription
        (sub-store/add!
          {:id "broadcast-sub"
           :sql "SELECT * FROM orders"
           :params []
           :session-id "broadcast-session"
           :status :active
           :tables ["orders"]})
        
        (with-redefs [pipeline/execute-reaction
                      (fn [sub-id]
                        ;; Simulate successful reaction with diff
                        {:success true
                         :subscription-id sub-id
                         :session-id "broadcast-session"
                         :results [{:id 1 :total 150}]
                         :diff {:type :incremental
                                :changes [{:op :update :id 1}]}})]
          
          ;; Trigger table change
          (coordinator/handle-table-change "orders")
          
          ;; Check broadcast was called
          (is (= 1 (count @broadcasts)))
          (let [broadcast (first @broadcasts)]
            (is (= "broadcast-session" (:session broadcast)))
            (is (= :update (get-in broadcast [:data :type])))))))))

(deftest test-concurrent-table-changes
  (testing "Concurrent table changes are handled safely"
    ;; Create subscriptions
    (doseq [i (range 10)]
      (sub-store/add!
        {:id (str "concurrent-sub" i)
         :sql "SELECT * FROM orders"
         :params []
         :session-id (str "session" i)
         :status :active
         :tables ["orders"]}))
    
    (let [reactions (atom #{})]
      (with-redefs [pipeline/execute-reaction
                    (fn [sub-id]
                      (swap! reactions conj sub-id)
                      {:success true :results []})]
        
        ;; Trigger multiple concurrent changes
        (let [futures (doall 
                       (for [_ (range 5)]
                         (future 
                           (coordinator/handle-table-change "orders"))))]
          
          ;; Wait for all to complete
          (doseq [f futures]
            @f)
          
          ;; Each subscription should be processed at least once
          (is (>= (count @reactions) 10)))))))

(deftest test-subscription-filtering
  (testing "Only relevant subscriptions are processed"
    ;; Create mix of subscriptions
    (sub-store/add!
      {:id "orders-only"
       :sql "SELECT * FROM orders"
       :params []
       :session-id "session1"
       :status :active
       :tables ["orders"]})
    
    (sub-store/add!
      {:id "inventory-only"
       :sql "SELECT * FROM inventory"
       :params []
       :session-id "session2"
       :status :active
       :tables ["inventory"]})
    
    (sub-store/add!
      {:id "both-tables"
       :sql "SELECT * FROM orders JOIN inventory"
       :params []
       :session-id "session3"
       :status :active
       :tables ["orders" "inventory"]})
    
    (let [processed (atom [])]
      (with-redefs [pipeline/execute-reaction
                    (fn [sub-id]
                      (swap! processed conj sub-id)
                      {:success true :results []})]
        
        ;; Change only orders
        (coordinator/handle-table-change "orders")
        
        ;; Should only process orders-related subscriptions
        (is (contains? (set @processed) "orders-only"))
        (is (contains? (set @processed) "both-tables"))
        (is (not (contains? (set @processed) "inventory-only")))))))

(deftest test-trigger-subscription
  (testing "Manual subscription triggering"
    (sub-store/add!
      {:id "manual-sub"
       :sql "SELECT * FROM test"
       :params []
       :session-id "manual-session"
       :status :active
       :tables ["test"]})
    
    (with-redefs [pipeline/execute-reaction
                  (fn [sub-id]
                    {:success true
                     :subscription-id sub-id
                     :session-id "manual-session"
                     :results [{:id 1}]})]
      
      ;; Trigger manually
      (let [result (coordinator/trigger-subscription "manual-sub")]
        (is (:success result))
        (is (= "manual-sub" (:subscription-id result))))
      
      ;; Non-existent subscription
      (let [result (coordinator/trigger-subscription "non-existent")]
        (is (not (:success result)))
        (is (= :not-found (get-in result [:error :type])))))))

(deftest test-session-connected
  (testing "Session connection triggers initial data load"
    ;; Create subscriptions for session
    (doseq [i (range 3)]
      (sub-store/add!
        {:id (str "session-sub" i)
         :sql "SELECT * FROM data"
         :params []
         :session-id "new-session"
         :status :active
         :tables ["data"]}))
    
    (let [processed (atom [])]
      (with-redefs [pipeline/execute-reaction
                    (fn [sub-id]
                      (swap! processed conj sub-id)
                      {:success true :results []})]
        
        ;; Handle session connected
        (coordinator/handle-session-connected "new-session")
        
        ;; All session subscriptions should be processed
        (is (= 3 (count @processed)))))))

(deftest test-stats
  (testing "Coordinator statistics"
    (let [stats (coordinator/stats)]
      (is (contains? stats :pending-reactions))
      (is (contains? stats :executor-running?))
      (is (number? (:pending-reactions stats)))
      (is (boolean? (:executor-running? stats))))))

(defn run-all-tests []
  (run-tests 'reactor.reactive.coordinator-test))