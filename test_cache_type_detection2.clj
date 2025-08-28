(ns test-cache-type-detection2
  "Test cache entry type detection - fixed version")

(println "\n=== Cache Entry Type Detection Test (Fixed) ===\n")

;; Correct detection logic matching kafka_reactive.clj
(defn is-temporal-entry?
  "Check if a cache key represents a temporal query"
  [k]
  (and (vector? k) 
       (= 4 (count k))
       (string? (last k))
       ;; Match various timestamp formats
       (boolean (re-find #"^\d{4}-\d{2}-\d{2}" (str (last k))))))

;; Debug individual test
(let [test-key ["default" "SELECT * FROM sales" [] "2025-08-28T10:00:00Z"]]
  (println "Debug test:")
  (println "  Key:" test-key)
  (println "  Is vector?" (vector? test-key))
  (println "  Count:" (count test-key))
  (println "  Last element:" (last test-key))
  (println "  Last is string?" (string? (last test-key)))
  (println "  Regex match:" (boolean (re-find #"^\d{4}-\d{2}-\d{2}" (str (last test-key)))))
  (println "  Result:" (is-temporal-entry? test-key))
  (println))

;; Test cases with proper evaluation
(println "Testing cache entries:")
(doseq [[cache-key expected-temporal? description] 
        [;; Regular queries (3 elements)
         [["default" "SELECT * FROM sales" []] false "Regular query"]
         [["session-1" "SELECT * FROM sales where amount > 500" []] false "Regular with WHERE"]
         
         ;; Temporal queries (4 elements with timestamp)
         [["default" "SELECT * FROM sales" [] "2025-08-28T10:00:00Z"] true "Temporal query"]
         [["session-1" "SELECT * FROM sales" [] "2024-01-01T00:00:00.000Z"] true "Temporal with millis"]
         [["test" "SELECT COUNT(*) FROM sales" [] "2025-08-28"] true "Temporal date only"]
         
         ;; Edge cases
         [["default" "SELECT * FROM sales" [] "not-a-date"] false "Invalid timestamp"]
         [["default" "SELECT * FROM sales" [] 12345] false "Non-string fourth element"]]]
  (let [detected (is-temporal-entry? cache-key)
        passed? (= detected expected-temporal?)
        type-str (if detected "TEMPORAL" "REGULAR ")]
    (println (format "  %-70s => %-8s %s" 
                    (pr-str cache-key)
                    type-str
                    (if passed? "✓" "✗")))
    (when-not passed?
      (println (format "    ERROR: Expected %s but got %s"
                      (if expected-temporal? "TEMPORAL" "REGULAR")
                      type-str)))))

(println "\n=== Mixed Cache Example ===\n")

;; Simulate real cache entries
(let [entries [
      [["default" "SELECT * FROM sales" []] {:count 100}]
      [["default" "SELECT * FROM sales" [] "2025-08-28T10:00:00Z"] {:count 50}]
      [["default" "SELECT * FROM sales where amount > 500" []] {:count 14}]
      [["default" "SELECT COUNT(*) FROM sales" [] "2025-08-28T11:00:00Z"] {:count 1}]]]
  
  (println "Cache entries with type labels:")
  (doseq [[k v] entries]
    (println (format "  %s %s"
                    (if (is-temporal-entry? k) "[TEMPORAL]" "[REGULAR] ")
                    (pr-str k))))
  
  (let [temporal (count (filter #(is-temporal-entry? (first %)) entries))
        regular (count (filter #(not (is-temporal-entry? (first %))) entries))]
    (println (format "\n  Summary: %d temporal, %d regular, %d total" 
                    temporal regular (+ temporal regular)))))

(println "\n=== Test Complete ===\n")