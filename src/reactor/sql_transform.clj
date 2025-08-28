(ns reactor.sql-transform
  "SQL transformation utilities for creating derived queries"
  (:require [cheshire.core :as json]
            [reactor.log :as log]
            [clojure.string :as str]))

(defn get-column-type
  "Detect if column is numeric based on common patterns and names"
  [column-name]
  (let [col-lower (str/lower-case (name column-name))]
    (cond
      ;; Common numeric column patterns
      (re-matches #".*(amount|price|cost|total|sum|count|quantity|qty|revenue|profit|loss|balance|score|rating|age|year|month|day|hour|minute|second|id|num|number|value|rate|percent|percentage|avg|average|min|max|median).*" col-lower) :numeric
      ;; Common date patterns
      (re-matches #".*(date|time|timestamp|created|updated|modified|deleted|expired|started|ended|_at|_on).*" col-lower) :date
      ;; Default to dimension
      :else :dimension)))

(defn remove-limit-clause
  "Remove LIMIT and OFFSET clauses from SQL string"
  [sql]
  ;; Simple regex-based removal of LIMIT and OFFSET
  (-> sql
      (str/replace #"(?i)\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?" "")
      (str/replace #"(?i)\s+OFFSET\s+\d+" "")
      str/trim))

(defn wrap-as-subquery
  "Wrap a SQL query as a subquery with alias"
  [sql alias]
  ;; Remove any trailing semicolon and limits
  (let [clean-sql (-> sql
                      (str/replace #";\s*$" "")
                      remove-limit-clause
                      str/trim)]
    (str "(" clean-sql ") AS " alias)))

(defn create-group-by-query
  "Create a GROUP BY query for a column from a source query"
  [{:keys [source-sql source-block-id column-name column-type]}]
  (let [;; If we have a source block ID, use template reference, otherwise embed SQL
        wrapped-sql (if source-block-id
                      (str "({{" source-block-id ".sql}}) AS source_data")
                      (wrap-as-subquery source-sql "source_data"))
        col-type (or column-type (get-column-type column-name))
        col-str (name column-name)]
    (case col-type
      :numeric
      ;; For numeric columns, calculate aggregates
      (str "SELECT "
           "MIN(" col-str ") AS min_" col-str ", "
           "MAX(" col-str ") AS max_" col-str ", "
           "AVG(" col-str ") AS avg_" col-str ", "
           "SUM(" col-str ") AS sum_" col-str ", "
           "COUNT(" col-str ") AS count_" col-str " "
           "FROM " wrapped-sql)
      
      :date
      ;; For date columns, treat as regular dimension (XTDB doesn't support DATE_TRUNC)
      (str "SELECT "
           col-str ", "
           "COUNT(*) AS count "
           "FROM " wrapped-sql " "
           "GROUP BY " col-str " "
           "ORDER BY " col-str " DESC "
           "LIMIT 20")
      
      ;; Default: dimension (text/categorical)
      (str "SELECT "
           col-str ", "
           "COUNT(*) AS count "
           "FROM " wrapped-sql " "
           "GROUP BY " col-str " "
           "ORDER BY count DESC "
           "LIMIT 20"))))

(defn create-filter-query
  "Create a filtered query for a specific cell value"
  [{:keys [source-sql column-name cell-value]}]
  (let [wrapped-sql (wrap-as-subquery source-sql "source_data")
        col-str (name column-name)
        ;; Escape single quotes in cell value
        escaped-value (str/replace (str cell-value) "'" "''")]
    (str "SELECT * FROM " wrapped-sql " "
         "WHERE " col-str " = '" escaped-value "'")))

(defn transform-sql
  "Main entry point for SQL transformations"
  [{:keys [type source-sql source-block-id column-name cell-value column-type] :as params}]
  (log/info {:message "Transforming SQL"
            :type type
            :column column-name
            :source-block-id source-block-id
            :has-source-sql? (boolean source-sql)})
  
  (case type
    :group-by (create-group-by-query params)
    :filter (create-filter-query params)
    (do
      (log/warn {:message "Unknown SQL transform type"
                :type type})
      nil)))