(ns test-count-extraction
  "Debug the COUNT wrapper extraction logic")

(println "\n=== COUNT Wrapper Extraction Debug ===\n")

;; Test the extraction logic
(defn extract-inner-query [query]
  (when (and (re-find #"(?i)SELECT\s+COUNT\(\*\)\s+as\s+cnt\s+FROM\s+\(" query)
             (re-find #"\)\s+as\s+subq" query))
    (let [;; Find the position after "FROM ("
          from-pattern "FROM ("
          from-idx (.indexOf (.toUpperCase query) from-pattern)
          start-idx (when (>= from-idx 0)
                     (+ from-idx (count from-pattern)))
          ;; Find matching closing paren for "as subq"
          before-subq (.lastIndexOf query ") as subq")
          inner-query (when (and start-idx (pos? start-idx) (pos? before-subq) (> before-subq start-idx))
                       (.substring query start-idx before-subq))]
      (println "  from-idx:" from-idx)
      (println "  start-idx:" start-idx)
      (println "  before-subq:" before-subq)
      inner-query)))

;; Test 1: Simple COUNT wrapper
(println "Test 1: Simple COUNT wrapper")
(let [query "SELECT COUNT(*) as cnt FROM (SELECT * FROM sales) as subq"
      inner (extract-inner-query query)]
  (println "  Query:" query)
  (println "  Inner:" inner)
  (assert (= inner "SELECT * FROM sales"))
  (println "  ✓ PASSED\n"))

;; Test 2: COUNT wrapper with temporal at end
(println "Test 2: COUNT wrapper with temporal clause")
(let [query "SELECT COUNT(*) as cnt FROM (SELECT * FROM sales WHERE amount > 100) as subq FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z'"
      inner (extract-inner-query query)]
  (println "  Query:" query)
  (println "  Inner:" inner)
  (assert (= inner "SELECT * FROM sales WHERE amount > 100"))
  (println "  ✓ PASSED\n"))

;; Test 3: Complex inner query (from your example)
(println "Test 3: Complex inner query with GROUP BY")
(let [query "SELECT COUNT(*) as cnt FROM (SELECT product, COUNT(*) AS count FROM ({{sales.sql}}) AS source_data GROUP BY product ORDER BY count DESC LIMIT 20) as subq FOR SYSTEM_TIME AS OF TIMESTAMP '2025-08-28T22:43:56.299Z'"
      inner (extract-inner-query query)]
  (println "  Query:" query)
  (println "  Inner:" inner)
  (println "  Expected: SELECT product, COUNT(*) AS count FROM ({{sales.sql}}) AS source_data GROUP BY product ORDER BY count DESC LIMIT 20")
  (assert (= inner "SELECT product, COUNT(*) AS count FROM ({{sales.sql}}) AS source_data GROUP BY product ORDER BY count DESC LIMIT 20"))
  (println "  ✓ PASSED\n"))

;; Test 4: Double COUNT (should not happen but let's test)
(println "Test 4: Double COUNT wrapper")
(let [query "SELECT COUNT(*) as cnt FROM (SELECT COUNT(*) as inner_cnt FROM sales) as subq"
      inner (extract-inner-query query)]
  (println "  Query:" query)
  (println "  Inner:" inner)
  (assert (= inner "SELECT COUNT(*) as inner_cnt FROM sales"))
  (println "  ✓ PASSED\n"))

(println "=== All extraction tests passed! ===\n")