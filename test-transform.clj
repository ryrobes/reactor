(require '[reactor.sql-transform :as sql-transform])

;; Test GROUP BY for numeric column
(println "Testing numeric column transformation:")
(let [result (sql-transform/transform-sql
               {:type :group-by
                :source-sql "SELECT * FROM sales"
                :column-name "amount"
                :column-type :numeric})]
  (println result))

(println "\nTesting dimension column transformation:")
(let [result (sql-transform/transform-sql
               {:type :group-by
                :source-sql "SELECT * FROM sales"
                :column-name "product"
                :column-type :dimension})]
  (println result))

(println "\nTesting filter transformation:")
(let [result (sql-transform/transform-sql
               {:type :filter
                :source-sql "SELECT * FROM sales"
                :column-name "product"
                :cell-value "Widget"})]
  (println result))
