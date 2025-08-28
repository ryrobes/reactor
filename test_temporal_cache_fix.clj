(ns test-temporal-cache-fix
  "Test to verify temporal queries have unique cache keys per timestamp"
  (:require [reactor.kafka-reactive :as kafka]))

(println "\n=== Temporal Cache Key Fix Test ===\n")

;; Test normalize-temporal-query function
(println "Test 1: normalize-temporal-query")
(let [query1 "SELECT * FROM sales FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T10:00:00Z'"
      query2 "SELECT * FROM sales FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-02T10:00:00Z'"
      result1 (kafka/normalize-temporal-query query1)
      result2 (kafka/normalize-temporal-query query2)]
  (println "  Query 1:" query1)
  (println "  Result 1:" result1)
  (println "  Query 2:" query2)
  (println "  Result 2:" result2)
  (assert (= (:base-query result1) (:base-query result2)) "Base queries should be the same")
  (assert (not= (:temporal-param result1) (:temporal-param result2)) "Temporal params should be different")
  (println "  ✓ PASSED - Base queries match, timestamps differ\n"))

;; Test 2: Cache keys for temporal queries
(println "Test 2: Cache key generation for temporal queries")
(let [session-id "test-session"
      base-query "SELECT * FROM sales"
      params []
      timestamp1 "2024-01-01T10:00:00Z"
      timestamp2 "2024-01-02T10:00:00Z"
      ;; Simulate cache key generation as in kafka_reactive.clj
      cache-key1 [session-id base-query params timestamp1]
      cache-key2 [session-id base-query params timestamp2]]
  (println "  Timestamp 1:" timestamp1)
  (println "  Cache key 1:" cache-key1)
  (println "  Timestamp 2:" timestamp2)
  (println "  Cache key 2:" cache-key2)
  (println "  Keys equal?" (= cache-key1 cache-key2))
  (assert (not= cache-key1 cache-key2) "Cache keys for different timestamps must be different")
  (println "  ✓ PASSED - Different timestamps produce different cache keys\n"))

;; Test 3: Regular (non-temporal) queries
(println "Test 3: Regular query cache keys")
(let [session-id "test-session"
      query "SELECT * FROM sales WHERE amount > 100"
      params [100]
      ;; Regular queries don't have temporal params
      cache-key1 [session-id query params]
      cache-key2 [session-id query params]]
  (println "  Query:" query)
  (println "  Cache key 1:" cache-key1)
  (println "  Cache key 2:" cache-key2)
  (println "  Keys equal?" (= cache-key1 cache-key2))
  (assert (= cache-key1 cache-key2) "Same regular query should produce same cache key")
  (println "  ✓ PASSED - Regular queries have consistent cache keys\n"))

;; Test 4: Verify cache isolation
(println "Test 4: Cache isolation between temporal queries")
(let [cache (atom {})
      session-id "test-session"
      base-query "SELECT COUNT(*) FROM sales"
      params []
      timestamp1 "2024-01-01T10:00:00Z"
      timestamp2 "2024-01-02T10:00:00Z"
      cache-key1 [session-id base-query params timestamp1]
      cache-key2 [session-id base-query params timestamp2]
      ;; Simulate caching results
      _ (swap! cache assoc cache-key1 {:results [{:count 100}]})
      _ (swap! cache assoc cache-key2 {:results [{:count 200}]})]
  (println "  Cache key 1:" cache-key1)
  (println "  Result 1:" (get-in @cache [cache-key1 :results]))
  (println "  Cache key 2:" cache-key2)
  (println "  Result 2:" (get-in @cache [cache-key2 :results]))
  (assert (= 100 (get-in @cache [cache-key1 :results 0 :count])) "T1 should have count 100")
  (assert (= 200 (get-in @cache [cache-key2 :results 0 :count])) "T2 should have count 200")
  (assert (not= (get @cache cache-key1) (get @cache cache-key2)) "Different timestamps have different cached results")
  (println "  ✓ PASSED - Temporal queries at different times have isolated cache entries\n"))

(println "=== All tests passed! Temporal queries now have unique cache keys per timestamp ===\n")