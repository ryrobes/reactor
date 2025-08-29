(ns test-template-resolution-final
  "Final test for template resolution with and without temporal clauses"
  (:require [reactor.sql-template-temporal :as tpl]
            [clojure.string :as str]))

(println "\n=== Template Resolution Final Test ===\n")

;; Mock session state with template blocks
(def mock-session
  {:canvas
   {:blocks
    {:sales {:sql "SELECT * FROM sales"}
     :products {:sql "SELECT * FROM products WHERE active = true"}
     :customers {:sql "SELECT id, name FROM customers"}}}})

;; Test 1: Regular template resolution (no temporal)
(println "Test 1: Regular template resolution (no temporal)")
(let [query "SELECT * FROM ({{sales.sql}}) AS source_data WHERE amount > 100"
      result (tpl/resolve-sql-templates-with-deps-and-temporal query mock-session nil)]
  (println "  Input:" query)
  (println "  Output:" (:sql result))
  (println "  Expected: SELECT * FROM ((SELECT * FROM sales)) AS source_data WHERE amount > 100")
  (assert (= (:sql result) 
             "SELECT * FROM ((SELECT * FROM sales)) AS source_data WHERE amount > 100"))
  (println "  ✓ PASSED\n"))

;; Test 2: Template resolution with temporal
(println "Test 2: Template resolution with temporal")
(let [query "SELECT * FROM ({{sales.sql}}) AS source_data WHERE amount > 100"
      timestamp "2024-01-01T00:00:00Z"
      result (tpl/resolve-sql-templates-with-deps-and-temporal query mock-session timestamp)]
  (println "  Input:" query)
  (println "  Timestamp:" timestamp)
  (println "  Output:" (:sql result))
  (println "  Expected temporal on actual table: sales FOR SYSTEM_TIME AS OF...")
  (assert (str/includes? (:sql result) "FROM sales FOR SYSTEM_TIME AS OF"))
  (assert (not (str/includes? (:sql result) "source_data FOR SYSTEM_TIME")))
  (println "  ✓ PASSED\n"))

;; Test 3: COUNT wrapper with template and temporal
(println "Test 3: COUNT wrapper with template and temporal")
(let [query "SELECT COUNT(*) as cnt FROM (SELECT product FROM ({{sales.sql}}) AS source_data GROUP BY product) as subq"
      timestamp "2024-01-01T00:00:00Z"
      result (tpl/resolve-sql-templates-with-deps-and-temporal query mock-session timestamp)]
  (println "  Input:" query)
  (println "  Timestamp:" timestamp)
  (println "  Output:" (:sql result))
  (println "  Expected temporal on actual table: sales FOR SYSTEM_TIME AS OF...")
  (assert (str/includes? (:sql result) "FROM sales FOR SYSTEM_TIME AS OF"))
  (assert (not (str/includes? (:sql result) "source_data FOR SYSTEM_TIME")))
  (assert (not (str/includes? (:sql result) "subq FOR SYSTEM_TIME")))
  (println "  ✓ PASSED\n"))

;; Test 4: Multiple templates with temporal
(println "Test 4: Multiple templates with temporal")
(let [query "SELECT * FROM ({{sales.sql}}) AS s JOIN ({{products.sql}}) AS p ON s.product_id = p.id"
      timestamp "2024-01-01T00:00:00Z"
      result (tpl/resolve-sql-templates-with-deps-and-temporal query mock-session timestamp)]
  (println "  Input:" query)
  (println "  Timestamp:" timestamp)
  (println "  Output:" (:sql result))
  (println "  Expected temporal on both actual tables")
  (assert (str/includes? (:sql result) "FROM sales FOR SYSTEM_TIME AS OF"))
  (assert (str/includes? (:sql result) "FROM products FOR SYSTEM_TIME AS OF"))
  (assert (not (str/includes? (:sql result) ") AS s FOR SYSTEM_TIME")))
  (assert (not (str/includes? (:sql result) ") AS p FOR SYSTEM_TIME")))
  (println "  ✓ PASSED\n"))

;; Test 5: Nested templates with temporal
(println "Test 5: Nested template with temporal")
(let [mock-session-nested (assoc-in mock-session 
                                    [:canvas :blocks :summary] 
                                    {:sql "SELECT category, SUM(amount) FROM ({{sales.sql}}) GROUP BY category"})
      query "SELECT * FROM ({{summary.sql}}) AS summary_data"
      timestamp "2024-01-01T00:00:00Z"
      result (tpl/resolve-sql-templates-with-deps-and-temporal query mock-session-nested timestamp)]
  (println "  Input:" query)
  (println "  Timestamp:" timestamp)
  (println "  Output:" (:sql result))
  (println "  Expected temporal on innermost actual table: sales FOR SYSTEM_TIME AS OF...")
  (assert (str/includes? (:sql result) "FROM sales FOR SYSTEM_TIME AS OF"))
  (assert (not (str/includes? (:sql result) "summary_data FOR SYSTEM_TIME")))
  (println "  ✓ PASSED\n"))

;; Test 6: Direct table query with temporal (no templates)
(println "Test 6: Direct table query with temporal (no templates)")
(let [query "SELECT * FROM sales WHERE amount > 100"
      timestamp "2024-01-01T00:00:00Z"
      result (tpl/resolve-sql-templates-with-deps-and-temporal query mock-session timestamp)]
  (println "  Input:" query)
  (println "  Timestamp:" timestamp)
  (println "  Output:" (:sql result))
  (println "  Expected temporal on sales table")
  (assert (str/includes? (:sql result) "FROM sales FOR SYSTEM_TIME AS OF"))
  (println "  ✓ PASSED\n"))

(println "=== All Template Resolution Tests Passed! ===\n")
(println "Summary:")
(println "  - Regular templates work without temporal clauses")
(println "  - Temporal clauses are added only to actual table references")
(println "  - COUNT wrappers preserve structure while adding temporal to inner tables")
(println "  - Multiple and nested templates all get temporal clauses on actual tables")
(println "  - No temporal clauses appear after subquery aliases")