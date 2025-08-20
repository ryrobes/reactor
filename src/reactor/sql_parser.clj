(ns reactor.sql-parser
  "SQL parsing and manipulation using JSqlParser"
  (:import [net.sf.jsqlparser.parser CCJSqlParserUtil]
           [net.sf.jsqlparser.statement.select Select PlainSelect]
           [net.sf.jsqlparser.expression.operators.relational EqualsTo]
           [net.sf.jsqlparser.expression LongValue StringValue]
           net.sf.jsqlparser.expression.Function
           [net.sf.jsqlparser.schema Column Table]
           [net.sf.jsqlparser.util.deparser ExpressionDeParser SelectDeParser StatementDeParser]
           [java.io StringReader]))

(defn parse-sql
  "Parse SQL string into AST"
  [sql]
  (try
    (CCJSqlParserUtil/parse sql)
    (catch Exception e
      (throw (ex-info "Failed to parse SQL" {:sql sql :error (.getMessage e)})))))

(defn add-as-of-clause
  "Add XTDB AS OF clause to SQL query for time travel"
  [sql as-of-timestamp]
  (if (or (nil? as-of-timestamp) 
          (empty? as-of-timestamp)
          ;; Don't add if already has FOR SYSTEM_TIME
          (re-find #"(?i)\s+FOR\s+SYSTEM_TIME\s+" sql))
    sql
    ;; For XTDB 2.0, we need to inject FOR SYSTEM_TIME AS OF after the table name
    ;; XTDB expects ISO-8601 format with Z suffix
    (let [clean-timestamp (if (string? as-of-timestamp)
                           (let [no-bracket (clojure.string/replace as-of-timestamp #"\[.*\]$" "")]
                             ;; Ensure it ends with Z
                             (if (clojure.string/ends-with? no-bracket "Z")
                               no-bracket
                               (str no-bracket "Z")))
                           as-of-timestamp)
          ;; Insert FOR SYSTEM_TIME AS OF after the table name in FROM clause with line breaks
          ;; Match: FROM table_name (with optional schema prefix and alias)
          final-sql (clojure.string/replace 
                     sql
                     #"(?i)(FROM\s+)([a-zA-Z_][a-zA-Z0-9_\.]*)"
                     (str "$1$2 \nFOR SYSTEM_TIME AS OF TIMESTAMP '" clean-timestamp "'\n"))]
      (println "[SQL-PARSER] Adding AS OF clause. Original SQL:" sql "Timestamp:" as-of-timestamp "Final SQL:" final-sql)
      final-sql)))

(defn extract-tables
  "Extract all table names from a SQL query"
  [sql]
  (try
    (let [stmt (parse-sql sql)]
      (cond
        ;; SELECT statement
        (instance? Select stmt)
        (let [select-body (.getSelectBody stmt)
              tables (atom #{})]
          (when (instance? PlainSelect select-body)
            ;; Get FROM table
            (when-let [from-item (.getFromItem select-body)]
              (when (instance? Table from-item)
                (swap! tables conj (str (.getName from-item)))))
            ;; Get JOIN tables
            (when-let [joins (.getJoins select-body)]
              (doseq [join joins]
                (when-let [right-item (.getRightItem join)]
                  (when (instance? Table right-item)
                    (swap! tables conj (str (.getName right-item))))))))
          @tables)
        
        ;; For other statement types, return empty set for now
        :else #{}))
    (catch Exception e
      ;; If parsing fails, fall back to regex extraction
      (set (map second (re-seq #"(?i)FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)" sql))))))

(defn modify-limit
  "Modify or add LIMIT clause to a SQL query"
  [sql new-limit]
  (try
    (let [stmt (parse-sql sql)]
      (when (instance? Select stmt)
        (let [select-body (.getSelectBody stmt)]
          (when (instance? PlainSelect select-body)
            ;; Set the limit
            (.setLimit select-body (net.sf.jsqlparser.statement.select.Limit.))
            (-> (.getLimit select-body)
                (.setRowCount (LongValue. (str new-limit)))))))
      (.toString stmt))
    (catch Exception e
      ;; Fallback: simple regex replacement
      (if (re-find #"(?i)\s+LIMIT\s+\d+" sql)
        (clojure.string/replace sql #"(?i)(\s+LIMIT\s+)\d+" (str "$1" new-limit))
        (str sql " LIMIT " new-limit)))))

(defn remove-where-clause
  "Remove WHERE clause from a SQL query"
  [sql]
  (try
    (let [stmt (parse-sql sql)]
      (when (instance? Select stmt)
        (let [select-body (.getSelectBody stmt)]
          (when (instance? PlainSelect select-body)
            (.setWhere select-body nil))))
      (.toString stmt))
    (catch Exception e
      ;; Fallback: regex removal
      (clojure.string/replace sql #"(?i)\s+WHERE\s+.*?(?=\s+ORDER\s+BY|\s+GROUP\s+BY|\s+LIMIT|$)" ""))))

(defn get-query-type
  "Determine the type of SQL query (SELECT, INSERT, UPDATE, DELETE, etc.)"
  [sql]
  (let [trimmed (clojure.string/trim sql)
        upper (clojure.string/upper-case trimmed)]
    (cond
      (clojure.string/starts-with? upper "SELECT") :select
      (clojure.string/starts-with? upper "INSERT") :insert
      (clojure.string/starts-with? upper "UPDATE") :update
      (clojure.string/starts-with? upper "DELETE") :delete
      (clojure.string/starts-with? upper "CREATE") :create
      (clojure.string/starts-with? upper "DROP") :drop
      (clojure.string/starts-with? upper "ALTER") :alter
      :else :unknown)))

(comment
  ;; Example usage
  (parse-sql "SELECT * FROM users WHERE id = 1")
  
  (extract-tables "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id")
  ;; => #{"users" "orders"}
  
  (add-as-of-clause "SELECT * FROM sales" "2024-01-01T00:00:00Z")
  ;; => "SELECT * FROM sales AS OF SYSTEM TIME '2024-01-01T00:00:00Z'"
  
  (modify-limit "SELECT * FROM sales" 20)
  ;; => "SELECT * FROM sales LIMIT 20"
  
  (remove-where-clause "SELECT * FROM users WHERE active = true ORDER BY name")
  ;; => "SELECT * FROM users ORDER BY name"
  )