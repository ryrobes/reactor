(require '[xtdb.node :as xtn])
(require '[xtdb.api :as xt])

(println "Starting XTDB 2.0 node...")
;; Try with explicit storage dir to avoid in-memory Arrow issues
(def node (xtn/start-node {:storage-dir (clojure.java.io/file "/tmp/xtdb-test")}))

(println "Executing XTQL put-docs (non-SQL)...")
(try
  ;; Try using XTQL instead of SQL
  (xt/execute-tx node [[:put-docs :test_table {:xt/id "test1" :name "Test Name"}]])
  (println "Put-docs successful")
  (Thread/sleep 100) ; Give it time to process
  (catch Exception e
    (println "Put-docs failed:" (.getMessage e))))

(println "Executing SQL query...")
(try
  (let [results (xt/q node "SELECT * FROM test_table")]
    (println "Query results:" results))
  (catch Exception e
    (println "Query failed:" (.getMessage e))))

(println "Executing XTQL query...")
(try
  (let [results (xt/q node '(from :test_table [*]))]
    (println "XTQL results:" results))
  (catch Exception e
    (println "XTQL failed:" (.getMessage e))))

(.close node)
(println "Done")