(ns reactor.time-travel-sql
  "Time travel functionality for SQL queries"
  (:require [reactor.xtdb-store :as xts]
            [reactor.sql-parser :as parser]
            [clojure.tools.logging :as log]))

(defn get-table-history-timestamps
  "Get recent history timestamps for a table"
  [node table-name limit]
  (try
    ;; Query XTDB's temporal metadata
    ;; This is a simplified version - XTDB 2.0 has different temporal query syntax
    (let [query (str "SELECT DISTINCT _valid_from FROM " table-name 
                    " ORDER BY _valid_from ASC"  ;; Order oldest first for consistent timeline
                    " LIMIT " limit)
          results (xts/execute-sql node query)]
      (map :_valid_from (:results results [])))
    (catch Exception e
      (log/error e "Failed to get history for table" table-name)
      [])))

(defn get-tables-from-sql
  "Extract table names from SQL query"
  [sql]
  (parser/extract-tables sql))

(defn execute-sql-with-time-travel
  "Execute SQL query at a specific point in time
   When as-of-timestamp is nil, executes the query without time travel (stays reactive)"
  [node sql params as-of-timestamp]
  (let [modified-sql (if as-of-timestamp
                       (parser/add-as-of-clause sql as-of-timestamp)
                       sql)
        result (xts/execute-sql node modified-sql params)]
    (log/info "[TIME-TRAVEL] Executing SQL:" modified-sql "with timestamp:" as-of-timestamp)
    ;; Only include executed-sql if it's different from the original (i.e., time travel is active)
    (if (and as-of-timestamp (not= sql modified-sql))
      (assoc result :executed-sql modified-sql)
      result)))

(defn get-query-history-range
  "Get the available time range for a SQL query based on its tables"
  [node sql limit]
  (let [tables (get-tables-from-sql sql)
        ;; Get history for all tables and find common range
        all-timestamps (mapcat #(get-table-history-timestamps node % limit) tables)
        ;; Clean timestamps - remove [UTC] suffix but keep the Z
        clean-timestamps (->> all-timestamps
                              (map (fn [ts] 
                                    (when ts
                                      (-> ts
                                          (clojure.string/replace #"\[.*\]$" "")))))  ; Only remove [UTC], keep the Z
                              (filter some?)  ; Remove nils
                              distinct
                              sort)  ; Sort chronologically (oldest first)
        ;; Take the requested limit minus 1 to leave room for NOW
        ;; Oldest timestamps first, NOW (nil) at the end for rightmost position
        timestamps-with-now (vec (concat (take (dec limit) clean-timestamps) [nil]))]
    {:tables tables
     :timestamps timestamps-with-now
     :count (count timestamps-with-now)}))