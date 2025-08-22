(ns test-temporal-query
  "Test the correct XTDB temporal query syntax"
  (:require [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]))

(defn test-temporal-syntax []
  (println "\n=== Testing XTDB Temporal Query Syntax ===\n")
  
  ;; Initialize
  (session/init! :todo "todo_sessions")
  (let [node @session/default-node
        session-id "alice-knows"
        timestamp "2025-08-20T16:10:13.346Z"]
    
    (println "Testing temporal query syntax:")
    (println "Session ID:" session-id)
    (println "Timestamp:" timestamp)
    (println "App name:" (name @session/app-name))
    (println "Table:" @session/app-table)
    (println)
    
    ;; Test the correct syntax
    (let [query (str "SELECT * FROM " @session/app-table 
                    " FOR SYSTEM_TIME AS OF TIMESTAMP '" timestamp "'"
                    " WHERE session_id = ? AND app_name = ?")]
      (println "Query:" query)
      (println "Parameters:" [session-id (name @session/app-name)])
      (println)
      
      (try
        (let [result (xts/execute-sql node query session-id (name @session/app-name))]
          (println "Query executed successfully!")
          (println "Result count:" (count (:results result)))
          (when-let [row (first (:results result))]
            (println "Found session row:")
            (println "  session_id:" (:session_id row))
            (println "  app_name:" (:app_name row))
            (println "  created_at:" (:created_at row))
            (when-let [state (:state row)]
              (println "  state:" (pr-str (take 100 state))))))
        (catch Exception e
          (println "Query failed:" (.getMessage e))
          (println "This is expected if no data exists at that timestamp"))))
    
    (println "\n✓ Test complete!")))

;; Run the test
(test-temporal-syntax)