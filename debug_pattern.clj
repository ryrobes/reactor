(println "\n=== Debug Pattern Matching ===\n")

(defn test-pattern [s]
  (println "Testing:" (pr-str s))
  (println "  Type:" (type s))
  (println "  Match ^\\d{4}-\\d{2}-\\d{2}:" (boolean (re-find #"^\d{4}-\d{2}-\d{2}" s)))
  (println "  Match \\d{4}-\\d{2}-\\d{2}:" (boolean (re-find #"\d{4}-\d{2}-\d{2}" s)))
  (println))

(test-pattern "2025-08-28T10:00:00Z")
(test-pattern "2024-01-01T00:00:00.000Z")
(test-pattern "2025-08-28")
(test-pattern "not-a-date")

;; Test with str conversion
(println "With str conversion:")
(let [s "2025-08-28T10:00:00Z"]
  (println "Original:" s)
  (println "After str:" (str s))
  (println "Match:" (boolean (re-find #"^\d{4}-\d{2}-\d{2}" (str s)))))