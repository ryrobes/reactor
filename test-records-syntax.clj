(require '[reactor.xtdb-store :as xts])

(println "Testing RECORDS syntax...")

;; Connect to remote server
(def node (xts/start-xtdb-node))

;; Test with session-like data
(println "\nTesting session insert with RECORDS syntax...")
(try
  (xts/put-entity node "sessions" "session-test-1" 
                  {:session_id "default"
                   :state "{:todos {}}"
                   :timestamp (str (java.time.Instant/now))})
  (println "✓ Session insert successful")
  (catch Exception e
    (println "✗ Session insert failed:" (.getMessage e))
    (.printStackTrace e)))

;; Verify it was saved
(println "\nVerifying session was saved...")
(try
  (let [result (xts/get-entity node "sessions" "session-test-1")]
    (println "✓ Retrieved session:" result))
  (catch Exception e
    (println "✗ Retrieval failed:" (.getMessage e))))