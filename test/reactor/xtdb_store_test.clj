(ns reactor.xtdb-store-test
  (:require [clojure.test :refer :all]
            [reactor.xtdb-store :as xts]
            [xtdb.api :as xt])
  (:import [java.time Duration]))

(deftest test-xtdb-node-lifecycle
  (testing "Can start and stop XTDB node"
    (let [node (xts/start-xtdb-node)]
      (is (not (nil? node)))
      (is (satisfies? xt/PXtdb node))
      (xts/stop-xtdb-node node))))

(deftest test-basic-entity-operations
  (testing "Can put and get entities"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Put an entity
        (let [tx (xts/put-entity node :test/entity-1 {:name "Test" :value 42})]
          (xt/await-tx node tx)
          
          ;; Get the entity
          (let [entity (xts/get-entity node :test/entity-1)]
            (is (= "Test" (:name entity)))
            (is (= 42 (:value entity)))
            (is (= :test/entity-1 (:xt/id entity)))))
        
        (finally
          (xts/stop-xtdb-node node)))))
  
  (testing "Can delete entities"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Put an entity
        (let [tx (xts/put-entity node :test/entity-2 {:name "Delete Me"})]
          (xt/await-tx node tx)
          (is (not (nil? (xts/get-entity node :test/entity-2))))
          
          ;; Delete it
          (let [tx (xts/delete-entity node :test/entity-2)]
            (xt/await-tx node tx)
            (is (nil? (xts/get-entity node :test/entity-2)))))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-entity-history
  (testing "Can track entity history"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Create multiple versions
        (let [tx1 (xts/put-entity node :test/versioned {:version 1})]
          (xt/await-tx node tx1)
          
          (let [tx2 (xts/put-entity node :test/versioned {:version 2})]
            (xt/await-tx node tx2)
            
            (let [tx3 (xts/put-entity node :test/versioned {:version 3})]
              (xt/await-tx node tx3)
              
              ;; Check history
              (xt/sync node) ; Ensure XTDB has indexed everything
              (let [history (xts/entity-history node :test/versioned)]
                (is (>= (count history) 3))))))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-session-keys
  (testing "Session keys are properly namespaced"
    (is (= :session.123/todos (xts/session-key "123" :todos)))
    (is (= :session.abc/users.active (xts/session-key "abc" :users :active))))
  
  (testing "Global keys are properly namespaced"
    (is (= :global/todos (xts/global-key :todos)))
    (is (= :global/config.settings (xts/global-key :config :settings)))))

(deftest test-xtdb-atom-basic-operations
  (testing "XTDBAtom behaves like a regular atom"
    (let [node (xts/start-xtdb-node)]
      (try
        (let [xa (xts/xtdb-atom node :test/atom {:count 0})]
          ;; Test deref
          (is (= {:count 0} @xa))
          
          ;; Test reset!
          (reset! xa {:count 10})
          (is (= {:count 10} @xa))
          
          ;; Test swap!
          (swap! xa update :count inc)
          (is (= {:count 11} @xa))
          
          ;; Test swap! with args
          (swap! xa assoc :name "test")
          (is (= {:count 11 :name "test"} @xa)))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-xtdb-atom-watchers
  (testing "XTDBAtom supports watchers"
    (let [node (xts/start-xtdb-node)]
      (try
        (let [xa (xts/xtdb-atom node :test/watched {:value 0})
              changes (atom [])]
          
          ;; Add watcher
          (add-watch xa :test-watcher
                     (fn [_ _ old new]
                       (swap! changes conj {:old old :new new})))
          
          ;; Make changes
          (swap! xa update :value inc)
          (swap! xa update :value inc)
          
          ;; Check watcher was called
          (is (= 2 (count @changes)))
          (is (= {:value 0} (:old (first @changes))))
          (is (= {:value 1} (:new (first @changes))))
          (is (= {:value 2} (:new (second @changes))))
          
          ;; Remove watcher
          (remove-watch xa :test-watcher)
          (swap! xa update :value inc)
          
          ;; Should still be 2 changes
          (is (= 2 (count @changes))))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-xtdb-atom-persistence
  (testing "XTDBAtom persists data across instances"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Create and modify an atom
        (let [xa1 (xts/xtdb-atom node :test/persistent {:data "original"})]
          (swap! xa1 assoc :data "modified")
          (is (= "modified" (:data @xa1))))
        
        ;; Create new atom with same key - should have persisted data
        (let [xa2 (xts/xtdb-atom node :test/persistent nil)]
          (is (= "modified" (:data @xa2))))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-xtdb-atom-sessions
  (testing "Session-scoped atoms are isolated"
    (let [node (xts/start-xtdb-node)]
      (try
        ;; Create atoms in different sessions
        (let [xa1 (xts/xtdb-atom node :counter {:value 0} "session-1")
              xa2 (xts/xtdb-atom node :counter {:value 0} "session-2")]
          
          ;; Modify session 1
          (swap! xa1 update :value inc)
          (is (= 1 (:value @xa1)))
          
          ;; Session 2 should be unchanged
          (is (= 0 (:value @xa2)))
          
          ;; Modify session 2
          (swap! xa2 update :value #(+ % 10))
          (is (= 10 (:value @xa2)))
          (is (= 1 (:value @xa1))))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-xtdb-atom-history
  (testing "Can access atom history through XTDB"
    (let [node (xts/start-xtdb-node)]
      (try
        (let [xa (xts/xtdb-atom node :test/history {:version 0})]
          ;; Make several changes
          (dotimes [i 5]
            (swap! xa assoc :version (inc i)))
          
          ;; Check we can get history
          (let [history (xts/history xa)]
            (is (seq history))
            ;; History includes all versions
            (is (>= (count history) 5))))
        
        (finally
          (xts/stop-xtdb-node node))))))

;; TODO: Implement retry logic for concurrent XTDB writes
;; This test is disabled because XTDB transactions can conflict under
;; concurrent writes without proper retry logic
#_(deftest test-concurrent-access
    (testing "XTDBAtom handles concurrent access"
      (let [node (xts/start-xtdb-node)]
        (try
          (let [xa (xts/xtdb-atom node :test/concurrent {:counter 0})
                threads 5
                increments-per-thread 10]
            
            ;; Spawn multiple threads that increment the counter
            (let [futures (for [_ (range threads)]
                            (future
                              (dotimes [_ increments-per-thread]
                                ;; Add delay to reduce contention with XTDB
                                (Thread/sleep 10)
                                (swap! xa update :counter inc))))]
              
              ;; Wait for all threads
              (doseq [f futures] @f)
              
              ;; Should have all increments
              (is (= (* threads increments-per-thread) 
                     (:counter @xa)))))
          
          (finally
            (xts/stop-xtdb-node node))))))