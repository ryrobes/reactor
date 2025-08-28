(ns reactor.time-travel-sql
  "Time travel functionality for SQL queries"
  (:require [reactor.xtdb-store :as xts]
            [reactor.sql-parser :as parser]
            [clojure.string :as cstr]
            [clojure.tools.logging :as log]))

(defn get-table-history-timestamps
  "Get recent history timestamps for a table, optionally filtered by WHERE clause"
  ([node table-name limit]
   (get-table-history-timestamps node table-name limit nil))
  ([node table-name limit where-clause]
   (try
     ;; Build query with optional WHERE clause for row-specific timestamps
     (let [query (str "SELECT DISTINCT _valid_from FROM " table-name 
                     " FOR VALID_TIME ALL "
                     (when where-clause 
                       (str " " where-clause " "))  ;; Add WHERE clause if provided
                     " ORDER BY _valid_from DESC"  ;; Get newest first
                     " LIMIT " limit)  ;; Use provided limit
           _ (log/debug "[TIME-TRAVEL] Query for timestamps:" query)
           results (xts/execute-sql node query)
           timestamps (map :_valid_from (:results results []))]
       ;; Log for debugging
      ;;  (when (seq timestamps)
      ;;    (log/info "[TIME-TRAVEL] Found" (count timestamps) "timestamps for" table-name 
      ;;             (when where-clause (str " with filter: " where-clause))))
       ;; Return in reverse order (oldest to newest) for consistent timeline display
       (reverse timestamps))
     (catch Exception e
       (log/error e "Failed to get history for table" table-name "with WHERE:" where-clause)
       []))))

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
    (when (not (cstr/starts-with? sql "SELECT COUNT(*) as cnt FROM (")) ;; dont log simple counts
      (log/info "[TIME-TRAVEL] Executing SQL:" modified-sql "with timestamp:" as-of-timestamp))
    ;; Only include executed-sql if it's different from the original (i.e., time travel is active)
    (if (and as-of-timestamp (not= sql modified-sql))
      (assoc result :executed-sql modified-sql)
      result)))

(defn get-query-history-range
  "Get the available time range for a SQL query based on its tables and WHERE clause"
  [node sql limit]
  (let [tables (get-tables-from-sql sql)
        ;; Extract WHERE clause from the original SQL to filter timestamps
        where-clause (parser/extract-where-clause sql)
        #_ (when where-clause
            (log/info "[TIME-TRAVEL] Using WHERE clause for timestamp filtering:" where-clause))
        ;; Get more timestamps from each table to ensure we capture recent changes
        ;; Request 2x the limit from each table to handle multiple tables
        per-table-limit (* 2 limit)
        ;; Pass WHERE clause to get row-specific timestamps
        all-timestamps (if where-clause
                         (mapcat #(get-table-history-timestamps node % per-table-limit where-clause) tables)
                         (mapcat #(get-table-history-timestamps node % per-table-limit) tables))
        ;; Clean timestamps - remove [UTC] suffix but keep the Z
        clean-timestamps (->> all-timestamps
                              (map (fn [ts] 
                                    (when ts
                                      (-> ts
                                          (clojure.string/replace #"\[.*\]$" "")))))  ; Only remove [UTC], keep the Z
                              (filter some?)  ; Remove nils
                              distinct
                              sort)  ; Sort chronologically (oldest first)
        ;; Take the most recent timestamps up to the limit
        ;; Since timestamps are sorted oldest-first, take from the end for most recent
        recent-timestamps (take-last (dec limit) clean-timestamps)
        ;; If we have too few timestamps, just use what we have
        ;; Interpolation would create misleading points since the data didn't actually change
        interpolated-timestamps recent-timestamps
        ;; Ensure we don't exceed the limit
        final-timestamps (take (dec limit) interpolated-timestamps)
        ;; Ensure chronological order (oldest to newest) with NOW (nil) at the end
        timestamps-with-now (vec (concat final-timestamps [nil]))]
    {:tables tables
     :timestamps timestamps-with-now
     :count (count timestamps-with-now)}))