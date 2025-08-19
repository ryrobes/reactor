(ns reactor.xtdb-v2-test
  "Simple test to verify XTDB 2.0 works"
  (:require [xtdb.node :as xtn]
            [xtdb.api :as xt]))

(defn -main []
  (println "\n=== XTDB 2.0 Test ===")
  (println "Clojure version:" (clojure-version))
  (println "Java version:" (System/getProperty "java.version"))
  
  (try
    (println "\nStarting XTDB 2.0 node...")
    (with-open [node (xtn/start-node)]
      (println "✓ Node started!")
      
      ;; Get status
      (println "\nNode status:" (xt/status node))
      
      ;; Create a table and insert data
      (println "\nCreating table and inserting data...")
      (xt/execute-tx node 
        [[:sql "CREATE TABLE test_table (_id VARCHAR PRIMARY KEY, name VARCHAR, value INTEGER)"]])
      
      (xt/execute-tx node
        [[:sql "INSERT INTO test_table (_id, name, value) VALUES (?, ?, ?)"
          ["id1" "Test Item" 42]]])
      
      ;; Query the data
      (println "\nQuerying data...")
      (let [results (xt/q node "SELECT * FROM test_table")]
        (println "Results:" results))
      
      (println "\n✅ XTDB 2.0 is working!"))
    
    (catch Exception e
      (println "\n❌ Error:" (.getMessage e))
      (.printStackTrace e)))
  
  (System/exit 0))