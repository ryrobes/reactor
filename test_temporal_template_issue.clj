(ns test-temporal-template-issue
  "Test to demonstrate the temporal clause placement issue with templated queries"
  (:require [reactor.sql-parser :as parser]
            [clojure.string :as str]))

(println "\n=== Temporal Template Query Issue Test ===\n")

;; Test Case 1: Simple temporal query (works)
(println "Test 1: Simple temporal query (no templates)")
(let [sql "SELECT * FROM sales"
      timestamp "2024-01-01T00:00:00Z"
      result (parser/add-as-of-clause sql timestamp)]
  (println "  Input:" sql)
  (println "  Output:" result)
  (println "  Expected: Temporal clause after 'sales'")
  (println "  ✓ Works correctly\n"))

;; Test Case 2: Count query without template (problematic)
(println "Test 2: Count query without template")
(let [sql "SELECT COUNT(*) as cnt FROM (SELECT * FROM sales) as subq"
      timestamp "2024-01-01T00:00:00Z"
      result (parser/add-as-of-clause sql timestamp)]
  (println "  Input:" sql)
  (println "  Output:" result)
  (println "  Problem: Temporal clause should be after inner 'sales', not after outer FROM")
  (println "  ✗ BROKEN - temporal clause in wrong place\n"))

;; Test Case 3: What it should produce
(println "Test 3: Correct placement (what we want)")
(let [correct "SELECT COUNT(*) as cnt FROM (SELECT * FROM sales FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z') as subq"]
  (println "  Correct SQL:" correct)
  (println "  Temporal clause is on the inner table access\n"))

;; Test Case 4: Templated count query (even more complex)
(println "Test 4: Templated count query")
(let [template-sql "SELECT COUNT(*) as cnt FROM ({{block-123.sql}}) as subq"
      ;; After template resolution
      resolved "SELECT COUNT(*) as cnt FROM ((SELECT * FROM sales WHERE amount > 100)) as subq"
      timestamp "2024-01-01T00:00:00Z"
      ;; Try to add temporal clause
      result (parser/add-as-of-clause resolved timestamp)]
  (println "  Template SQL:" template-sql)
  (println "  After resolution:" resolved)
  (println "  With temporal:" result)
  (println "  Problem: Temporal clause needs to be inside the resolved template")
  (println "  ✗ BROKEN - temporal clause not reaching inner query\n"))

;; Test Case 5: Multiple nested queries
(println "Test 5: Multiple levels of nesting")
(let [sql "SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM sales) t1) t2"
      timestamp "2024-01-01T00:00:00Z"
      result (parser/add-as-of-clause sql timestamp)]
  (println "  Input:" sql)
  (println "  Output:" result)
  (println "  Should be: SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM sales FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z') t1) t2")
  (println "  ✗ BROKEN - need deepest table access\n"))

(println "\n=== Analysis ===\n")
(println "The issue: add-as-of-clause uses a simple regex that adds the temporal clause")
(println "after the FIRST 'FROM' + table pattern it finds. This fails for:")
(println "1. Nested queries (subqueries)")
(println "2. Count queries with subqueries")
(println "3. Templated queries that expand to nested structures")
(println)
(println "For row count temporal queries with templates:")
(println "- Original: SELECT COUNT(*) as cnt FROM ({{block.sql}}) as subq")
(println "- Template resolves to: SELECT * FROM sales")
(println "- Current (WRONG): SELECT COUNT(*) as cnt FROM ((SELECT * FROM sales)) as subq FOR SYSTEM_TIME...")
(println "- Needed (RIGHT): SELECT COUNT(*) as cnt FROM ((SELECT * FROM sales FOR SYSTEM_TIME...)) as subq")
(println)
(println "The temporal clause must be placed on the actual table access,")
(println "not on the outer COUNT or subquery wrapper!")
(println "\n=== End Test ===\n")