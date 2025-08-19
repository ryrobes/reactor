#!/usr/bin/env clojure
;; Quick test script to verify XTDB 2.0 setup
;; Run with: lein with-profile +xtdb2 exec -p test_xtdb_v2.clj

(println "\n=== XTDB 2.0 Migration Test ===")
(println "Java version:" (System/getProperty "java.version"))

(try
  ;; Try to load XTDB 2.0
  (println "\n1. Loading XTDB 2.0 namespaces...")
  (require '[xtdb.node :as xtn])
  (require '[xtdb.api :as xt])
  (println "✓ XTDB 2.0 namespaces loaded successfully")
  
  ;; Try to start an embedded node
  (println "\n2. Starting embedded XTDB 2.0 node...")
  (with-open [node (xtn/start-node)]
    (println "✓ Node started successfully")
    
    ;; Check node status
    (println "\n3. Checking node status...")
    (let [status (xt/status node)]
      (println "✓ Node status:" status))
    
    ;; Try a simple SQL operation
    (println "\n4. Testing SQL operations...")
    
    ;; Create table
    (println "   Creating sales table...")
    (xt/execute-tx node
      [[:sql "CREATE TABLE sales (_id VARCHAR PRIMARY KEY, product VARCHAR, amount INTEGER)"]])
    (println "   ✓ Table created")
    
    ;; Insert data
    (println "   Inserting test data...")
    (xt/execute-tx node
      [[:sql "INSERT INTO sales (_id, product, amount) VALUES (?, ?, ?)"
        ["sale-1" "Widget" 100]]])
    (println "   ✓ Data inserted")
    
    ;; Query data
    (println "   Querying data...")
    (let [results (xt/q node "SELECT * FROM sales")]
      (println "   ✓ Query results:" results))
    
    ;; Test UPDATE
    (println "   Testing UPDATE...")
    (xt/execute-tx node
      [[:sql "UPDATE sales SET amount = ? WHERE _id = ?" [200 "sale-1"]]])
    (let [updated (xt/q node "SELECT * FROM sales WHERE _id = 'sale-1'")]
      (println "   ✓ Updated results:" updated))
    
    ;; Test DELETE
    (println "   Testing DELETE...")
    (xt/execute-tx node
      [[:sql "DELETE FROM sales WHERE _id = ?" ["sale-1"]]])
    (let [deleted (xt/q node "SELECT * FROM sales")]
      (println "   ✓ After delete:" deleted))
    
    (println "\n✅ All XTDB 2.0 tests passed!"))
  
  (catch Exception e
    (println "\n❌ Error during testing:")
    (println "   " (.getMessage e))
    (when (clojure.string/includes? (.getMessage e) "xtdb")
      (println "\n⚠️  Make sure to run with XTDB 2.0 profile:")
      (println "   lein with-profile +xtdb2 exec -p test_xtdb_v2.clj"))))

(println "\n=== Test Complete ===\n")