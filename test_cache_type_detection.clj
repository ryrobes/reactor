(ns test-cache-type-detection
  "Test cache entry type detection for clearer logging")

(println "\n=== Cache Entry Type Detection Test ===\n")

;; Test type detection logic
(defn is-temporal-entry?
  "Check if a cache key represents a temporal query"
  [k]
  (and (vector? k) 
       (= 4 (count k))
       (string? (last k))
       ;; Match various timestamp formats (ISO 8601, with or without time/timezone)
       (re-find #"^\d{4}-\d{2}-\d{2}" (str (last k)))))

;; Test cases
(let [test-cases [
      ;; Regular queries (3 elements)
      [["default" "SELECT * FROM sales" []] false "Regular query"]
      [["session-1" "SELECT * FROM sales where amount > 500" []] false "Regular query with WHERE"]
      [["test" "SELECT COUNT(*) FROM sales" [100]] false "Regular query with params"]
      
      ;; Temporal queries (4 elements with timestamp)
      [["default" "SELECT * FROM sales" [] "2025-08-28T10:00:00Z"] true "Temporal query"]
      [["session-1" "SELECT * FROM sales" [] "2024-01-01T00:00:00.000Z"] true "Temporal with full timestamp"]
      [["test" "SELECT COUNT(*) FROM sales" [] "2025-08-28"] true "Temporal with date only"]
      
      ;; Edge cases
      [["default" "SELECT * FROM sales" [] "not-a-date"] false "Fourth element not a date"]
      [["default" "SELECT * FROM sales" [] 12345] false "Fourth element not a string"]
      [[] false "Empty vector"]
      [["only-one-element"] false "Too few elements"]
      ]]
  
  (println "Testing cache entry type detection:")
  (doseq [[cache-key expected description] test-cases]
    (let [result (is-temporal-entry? cache-key)
          status (if (= result expected) "✓ PASS" "✗ FAIL")]
      (println (format "  %-60s => %-8s %s" 
                      (str cache-key)
                      (if result "TEMPORAL" "REGULAR")
                      status))
      (when (not= result expected)
        (println "    ERROR: Expected" (if expected "TEMPORAL" "REGULAR")))))
  
  ;; Summary
  (let [passed (count (filter #(= (is-temporal-entry? (first %)) (second %)) test-cases))
        total (count test-cases)]
    (println (format "\n  Results: %d/%d tests passed" passed total))
    (when (= passed total)
      (println "  ✓ All tests passed!"))))

(println "\n=== Example Cache with Mixed Types ===\n")

;; Simulate a mixed cache
(let [cache-entries {
      ["default" "SELECT * FROM sales" []] 
        {:results (range 100) :timestamp 1234567890}
      
      ["default" "SELECT * FROM sales" [] "2025-08-28T10:00:00Z"]
        {:results (range 50) :timestamp 1234567891}
      
      ["default" "SELECT * FROM sales where amount > 500" []]
        {:results (range 14) :timestamp 1234567892}
      
      ["default" "SELECT COUNT(*) FROM sales" [] "2025-08-28T11:00:00Z"]
        {:results [{:count 100}] :timestamp 1234567893}}]
  
  (println "Cache contents with type labels:")
  (doseq [[k v] cache-entries]
    (let [type-label (if (is-temporal-entry? k) "[TEMPORAL]" "[REGULAR] ")]
      (println (format "  %s %s -> %d results"
                      type-label
                      k
                      (count (:results v))))))
  
  (println "\nSummary:")
  (let [temporal-count (count (filter #(is-temporal-entry? (first %)) cache-entries))
        regular-count (count (filter #(not (is-temporal-entry? (first %))) cache-entries))]
    (println (format "  Temporal entries: %d" temporal-count))
    (println (format "  Regular entries:  %d" regular-count))
    (println (format "  Total entries:    %d" (count cache-entries)))))

(println "\n=== Test Complete ===\n")