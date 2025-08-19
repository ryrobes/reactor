(ns reactor.xtdb-id-test
  (:require [clojure.test :refer :all]
            [reactor.xtdb-store :as xts]
            [xtdb.api :as xt]))

(deftest test-entity-id-mismatch
  (testing "Entity ID format between put and atom"
    (let [node (xts/start-xtdb-node)
          entity-id "test-entity"]
      
      (try
        ;; Put with string ID
        (println "Putting entity with ID:" entity-id)
        (let [tx (xts/put-entity node entity-id {:counter 0 :name "test"})]
          (xt/await-tx node tx (java.time.Duration/ofSeconds 5)))
        
        ;; Check what ID is actually stored
        (let [all-entities (xt/q (xt/db node) '{:find [?e]
                                                 :where [[?e :xt/id]]})]
          (println "All entity IDs in DB:" all-entities))
        
        ;; Try to get with string ID
        (let [entity1 (xts/get-entity node entity-id)]
          (println "Get with string ID:" entity1))
        
        ;; Try to get with global key
        (let [global-id (xts/global-key entity-id)
              entity2 (xts/get-entity node global-id)]
          (println "Global key:" global-id)
          (println "Get with global key:" entity2))
        
        ;; Create atom - it will use global-key internally
        (let [atom (xts/xtdb-atom node entity-id nil nil)]
          (println "Atom deref:" @atom)
          (is (nil? @atom) "Atom returns nil because it's looking for wrong ID"))
        
        (finally
          (xts/stop-xtdb-node node))))))

(deftest test-correct-usage
  (testing "Correct way to use XTDBAtom"
    (let [node (xts/start-xtdb-node)
          entity-id "test-entity"]
      
      (try
        ;; Option 1: Let the atom create the entity
        (let [atom1 (xts/xtdb-atom node "atom1" {:value 1} nil)]
          (println "Atom1 created with initial value:" @atom1)
          (is (= 1 (:value @atom1))))
        
        ;; Option 2: Use the correct entity ID format
        (let [global-id (xts/global-key "atom2")
              tx (xts/put-entity node global-id {:value 2})]
          (xt/await-tx node tx (java.time.Duration/ofSeconds 5))
          (let [atom2 (xts/xtdb-atom node "atom2" nil nil)]
            (println "Atom2 using global-key:" @atom2)
            (is (= 2 (:value @atom2)))))
        
        (finally
          (xts/stop-xtdb-node node))))))