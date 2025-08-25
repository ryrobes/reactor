(ns test-temporal-scrubbing
  "Test that temporal queries (time scrubbing) use diffing"
  (:require [clojure.string :as str]
            [cheshire.core :as json]))

(defn test-temporal-queries []
  (println "\n=== Testing Temporal Query Diffing (Time Scrubbing) ===")
  
  ;; Simulate time scrubbing by sending multiple temporal queries
  (let [base-url "http://localhost:5000"
        session-id "test-temporal-scrub"
        base-sql "SELECT * FROM todo_sessions"
        ;; Simulate scrubbing through different timestamps
        timestamps ["2025-08-23T23:51:53.824Z"
                   "2025-08-23T23:51:55.443Z"
                   "2025-08-24T03:41:16.366Z"
                   "2025-08-24T03:55:42.300Z"
                   "2025-08-24T04:40:05.843Z"]]
    
    (println "\nSending temporal queries at different timestamps...")
    (println "Watch the server logs for [TEMPORAL-DIFF] and [DIFF-SEND] messages")
    
    (doseq [ts timestamps]
      (let [temporal-sql (str base-sql " FOR SYSTEM_TIME AS OF TIMESTAMP '" ts "'")
            response (try
                      (slurp (str base-url "/api/sql")
                            :method :post
                            :headers {"Content-Type" "application/json"}
                            :body (json/generate-string {:sql temporal-sql 
                                                        :as-of ts}))
                      (catch Exception e
                        (str "Error: " (.getMessage e))))]
        (println "\nTimestamp:" ts)
        (when (string? response)
          (let [result (json/parse-string response true)]
            (cond
              (:diff result) 
              (println "  ✅ DIFF received! Type:" (:type result))
              
              (:result result)
              (println "  ⚠️  FULL result received (no diff) - rows:" (count (:result result)))
              
              :else
              (println "  ❌ Unexpected response:" (take 100 (str result))))))))
    
    (println "\n=== Summary ===")
    (println "Check server logs for:")
    (println "  - [TEMPORAL-CACHE] messages showing cache hits")
    (println "  - [TEMPORAL-DIFF] messages when diffing happens")
    (println "  - [DIFF-SEND] messages showing compression ratios")
    (println "\nThe first query will be FULL (no cache)")
    (println "Subsequent queries should use DIFF if data is similar")))

;; Run the test
(test-temporal-queries)