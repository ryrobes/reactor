(ns reactor.xtdb-debug-test
  (:require [clojure.test :refer :all]
            [reactor.xtdb-store :as xts]
            [xtdb.api :as xt]))

(deftest test-xtdb-atom-basic
  (testing "Basic XTDB atom operations"
    (let [node (xts/start-xtdb-node)
          entity-id "test-entity"]
      
      (try
        ;; Create atom with initial value
        (let [atom (xts/xtdb-atom node entity-id {:counter 0 :name "test"} nil)]
          
          ;; Test deref
          (println "Initial deref:" @atom)
          (is (map? @atom))
          (is (= 0 (:counter @atom)))
          (is (= "test" (:name @atom)))
          
          ;; Test swap
          (swap! atom update :counter inc)
          (Thread/sleep 100)
          
          ;; Test updated value
          (println "After swap:" @atom)
          (is (= 1 (:counter @atom))))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-direct-xtdb
  (testing "Direct XTDB operations"
    (let [node (xts/start-xtdb-node)]
      
      (try
        ;; Put data directly
        (let [tx (xt/submit-tx node [[::xt/put {:xt/id "test" :value 42}]])]
          (xt/await-tx node tx (java.time.Duration/ofSeconds 5)))
        
        ;; Get data directly
        (let [entity (xt/entity (xt/db node) "test")]
          (println "Direct entity:" entity)
          (is (= 42 (:value entity))))
        
        (finally
          (xts/stop-xtdb-node node))))))