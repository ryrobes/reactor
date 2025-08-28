(ns test-sql-resolver
  "Quick test to verify SQL resolver works correctly"
  (:require [reactor.sql-resolver :as resolver]
            [reactor.session_simple :as session]
            [clojure.string :as str]))

(println "\n=== SQL Resolver Test ===\n")

;; Test 1: SQL without templates
(let [sql "SELECT * FROM sales WHERE amount > 100"
      result (resolver/resolve-sql sql)]
  (println "Test 1: SQL without templates")
  (println "  Original:" sql)
  (println "  Resolved:" (:resolved-sql result))
  (println "  Has templates?" (:has-templates? result))
  (println "  Dependencies:" (:dependencies result))
  (assert (= sql (:resolved-sql result)))
  (assert (not (:has-templates? result)))
  (println "  ✓ PASSED\n"))

;; Test 2: SQL with templates (will fail without session state)
(let [sql "SELECT * FROM {{block-123.sql}} WHERE amount > 100"
      result (resolver/resolve-sql sql)]
  (println "Test 2: SQL with unresolvable templates (no session)")
  (println "  Original:" sql)
  (println "  Resolved:" (:resolved-sql result))
  (println "  Has templates?" (:has-templates? result))
  (println "  Dependencies:" (:dependencies result))
  (assert (:has-templates? result))
  (println "  ✓ PASSED (expected to not resolve without session)\n"))

;; Test 3: Cache key generation
(let [sql1 "SELECT * FROM sales"
      sql2 "  SELECT  *  FROM   sales  "  ; Different whitespace
      key1 (resolver/generate-cache-key sql1)
      key2 (resolver/generate-cache-key sql2)]
  (println "Test 3: Cache key normalization")
  (println "  SQL 1:" sql1)
  (println "  SQL 2:" sql2)
  (println "  Key 1:" key1)
  (println "  Key 2:" key2)
  (println "  Keys equal?" (= key1 key2))
  (assert (= key1 key2) "Cache keys should be equal for normalized SQL")
  (println "  ✓ PASSED\n"))

;; Test 4: Subscription ID generation
(let [sql "SELECT * FROM sales"
      temporal-sql "SELECT * FROM sales FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01'"
      id1 (resolver/generate-subscription-id sql nil nil)
      id2 (resolver/generate-subscription-id temporal-sql nil nil)]
  (println "Test 4: Subscription ID generation")
  (println "  Regular SQL:" sql)
  (println "  Temporal SQL:" temporal-sql)
  (println "  Regular ID:" id1)
  (println "  Temporal ID:" id2)
  (println "  IDs different?" (not= id1 id2))
  (assert (not= id1 id2) "Different SQL should generate different IDs")
  (assert (str/starts-with? id2 "temporal-") "Temporal queries should have temporal- prefix")
  (println "  ✓ PASSED\n"))

;; Test 5: Template detection
(let [test-cases [["SELECT * FROM sales" false]
                   ["SELECT * FROM {{block.sql}}" true]
                   ["SELECT {{a.sql}} UNION {{b.sql}}" true]
                   ["SELECT * FROM ({{nested.sql}})" true]
                   ["SELECT * FROM sales WHERE x = '{not-a-template}'" false]]]
  (println "Test 5: Template detection")
  (doseq [[sql expected] test-cases]
    (let [has-templates (resolver/has-templates? sql)]
      (println "  SQL:" sql)
      (println "    Has templates?" has-templates "Expected:" expected)
      (assert (= has-templates expected) (str "Failed for: " sql))))
  (println "  ✓ PASSED\n"))

(println "=== All tests passed! ===\n")