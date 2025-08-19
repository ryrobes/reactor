(require '[reactor.xtdb-store :as xts])
(require '[next.jdbc :as jdbc])

(println "Testing remote XTDB 2.0 connection...")

;; Connect to remote server
(def node (xts/start-xtdb-node))
(println "Node spec:" node)

;; Test basic operations
(println "\n1. Testing put-entity...")
(try
  (xts/put-entity node "test_table" "test-1" {:name "Test Item" :value 42})
  (println "✓ Put entity successful")
  (catch Exception e
    (println "✗ Put entity failed:" (.getMessage e))))

(println "\n2. Testing get-entity...")
(try
  (let [result (xts/get-entity node "test_table" "test-1")]
    (println "✓ Get entity result:" result))
  (catch Exception e
    (println "✗ Get entity failed:" (.getMessage e))))

(println "\n3. Testing raw SQL query...")
(try
  (let [result (xts/execute-sql node "SELECT * FROM test_table")]
    (println "✓ SQL query result:" result))
  (catch Exception e
    (println "✗ SQL query failed:" (.getMessage e))))

(println "\n4. Testing update-entity...")
(try
  (xts/update-entity node "test_table" "test-1" {:value 100})
  (let [updated (xts/get-entity node "test_table" "test-1")]
    (println "✓ Updated entity:" updated))
  (catch Exception e
    (println "✗ Update failed:" (.getMessage e))))

(println "\n5. Testing delete-entity...")
(try
  (xts/delete-entity node "test_table" "test-1")
  (let [deleted (xts/get-entity node "test_table" "test-1")]
    (println "✓ After delete (should be nil):" deleted))
  (catch Exception e
    (println "✗ Delete failed:" (.getMessage e))))

(println "\n✅ Remote XTDB connection tests complete!")