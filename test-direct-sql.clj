(require '[next.jdbc :as jdbc])
(require '[clojure.string :as str])

(def conn (jdbc/get-connection "jdbc:xtdb://10.174.1.144:5432/xtdb"))

;; Test data with special characters
(def test-state "{:todos {}}")

;; Escape the special characters
(def escaped-state 
  (-> test-state
      (str/replace "'" "''")  ; Escape single quotes
      (str/replace "{" "{{")  ; Escape opening braces
      (str/replace "}" "}}"))) ; Escape closing braces

(println "Original state:" test-state)
(println "Escaped state:" escaped-state)

;; Build the SQL
(def sql (str "INSERT INTO test_direct RECORDS "
             "{_id: 'test-direct-1', "
             "session_id: 'default', "
             "state: '" escaped-state "', "
             "timestamp: '2025-01-01'}"))

(println "\nSQL to execute:")
(println sql)

;; Execute it
(println "\nExecuting...")
(try
  (jdbc/execute! conn [sql])
  (println "✓ Insert successful!")
  
  ;; Query it back
  (let [results (jdbc/execute! conn ["SELECT * FROM test_direct WHERE _id = 'test-direct-1'"])]
    (println "✓ Retrieved:" (first results)))
  
  (catch Exception e
    (println "✗ Failed:" (.getMessage e))))

(.close conn)