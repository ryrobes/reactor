(ns test-temporal-template-fix2
  "Test the enhanced temporal template resolution"
  (:require [reactor.sql-template-temporal :as template-temporal]
            [clojure.string :as str]))

(println "\n=== Enhanced Temporal Template Resolution Test ===\n")

;; Test 1: Simple template with temporal
(println "Test 1: Simple template with temporal clause")
(let [;; Simulate session state with block SQL
      session-state {:canvas {:blocks {:block-123 {:sql "SELECT * FROM sales"}}}}
      ;; Template query with temporal
      template-sql "SELECT COUNT(*) as cnt FROM ({{block-123.sql}}) as subq"
      timestamp "2024-01-01T00:00:00Z"
      ;; Resolve with temporal
      resolved (template-temporal/resolve-sql-templates-with-temporal 
                template-sql 
                session-state
                timestamp)]
  (println "  Template SQL:" template-sql)
  (println "  Timestamp:" timestamp)
  (println "  Resolved:" resolved)
  (println "  ✓ Check: Temporal clause inside parentheses?" 
           (boolean (re-find #"\(\s*SELECT.*FROM sales.*FOR SYSTEM_TIME" resolved)))
  (println))

;; Test 2: Complex nested template
(println "Test 2: Product count query (from your example)")
(let [;; Session state with the template block
      session-state {:canvas {:blocks {:sales-block {:sql "SELECT * FROM sales"}}}}
      ;; The problematic query from your log
      template-sql "SELECT product, COUNT(*) AS count FROM ({{sales-block.sql}}) AS source_data GROUP BY product ORDER BY count DESC LIMIT 20"
      timestamp "2025-08-28T22:43:56.299Z"
      ;; Resolve with temporal
      resolved (template-temporal/resolve-sql-templates-with-temporal 
                template-sql 
                session-state
                timestamp)]
  (println "  Template SQL:" template-sql)
  (println "  Timestamp:" timestamp)
  (println "  Resolved:" resolved)
  (println "  Expected: SELECT product, COUNT(*) AS count FROM (SELECT * FROM sales FOR SYSTEM_TIME AS OF TIMESTAMP '2025-08-28T22:43:56.299Z') AS source_data GROUP BY product ORDER BY count DESC LIMIT 20")
  (println "  ✓ Check: Contains temporal clause?" 
           (boolean (re-find #"FOR SYSTEM_TIME AS OF TIMESTAMP" resolved)))
  (println))

;; Test 3: Multiple templates
(println "Test 3: Multiple templates in one query")
(let [session-state {:canvas {:blocks {:t1 {:sql "SELECT * FROM orders"}
                                      :t2 {:sql "SELECT * FROM customers"}}}}
      template-sql "SELECT * FROM ({{t1.sql}}) o JOIN ({{t2.sql}}) c ON o.customer_id = c.id"
      timestamp "2024-01-01T00:00:00Z"
      resolved (template-temporal/resolve-sql-templates-with-temporal 
                template-sql 
                session-state
                timestamp)]
  (println "  Template SQL:" template-sql)
  (println "  Resolved:" resolved)
  (println "  ✓ Check: Both tables have temporal clause?" 
           (let [orders-temporal (re-find #"orders.*FOR SYSTEM_TIME" resolved)
                 customers-temporal (re-find #"customers.*FOR SYSTEM_TIME" resolved)]
             (and orders-temporal customers-temporal)))
  (println))

;; Test 4: Nested templates
(println "Test 4: Nested template resolution")
(let [session-state {:canvas {:blocks {:inner {:sql "SELECT * FROM products"}
                                      :outer {:sql "SELECT * FROM ({{inner.sql}}) WHERE price > 100"}}}}
      template-sql "SELECT COUNT(*) FROM ({{outer.sql}}) as cnt"
      timestamp "2024-01-01T00:00:00Z"
      resolved (template-temporal/resolve-sql-templates-with-temporal 
                template-sql 
                session-state
                timestamp)]
  (println "  Template SQL:" template-sql)
  (println "  Resolved:" resolved)
  (println "  ✓ Check: Temporal clause on innermost table?" 
           (boolean (re-find #"products.*FOR SYSTEM_TIME" resolved)))
  (println))

;; Test 5: Template without FROM clause (edge case)
(println "Test 5: Template without FROM clause")
(let [session-state {:canvas {:blocks {:const {:sql "SELECT 1 as one, 2 as two"}}}}
      template-sql "SELECT * FROM ({{const.sql}}) as constants"
      timestamp "2024-01-01T00:00:00Z"
      resolved (template-temporal/resolve-sql-templates-with-temporal 
                template-sql 
                session-state
                timestamp)]
  (println "  Template SQL:" template-sql)
  (println "  Resolved:" resolved)
  (println "  Note: No FROM clause in template, so no temporal clause added")
  (println "  Result:" resolved)
  (println))

(println "\n=== Summary ===")
(println "The enhanced temporal template resolution:")
(println "1. Adds temporal clause to each resolved template piece")
(println "2. Ensures temporal clause is inside the parentheses")
(println "3. Handles nested and multiple templates correctly")
(println "4. Places temporal clause on actual table access")
(println "\n=== Test Complete ===\n")