(require '[clojure.string :as str])

;; Test the escaping function
(defn escape-sql-string [s]
  (if (string? s)
    ;; Escape single quotes by doubling them
    (str "'" (str/replace s "'" "''") "'")
    s))

(defn build-records-str [entity-id data]
  (let [record-map (merge {:_id entity-id} 
                         (into {} (map (fn [[k v]] [(keyword (name k)) v]) data)))
        record-str (str "{"
                       (str/join ", " 
                                (map (fn [[k v]] 
                                      (str (name k) ": " (escape-sql-string v)))
                                     record-map))
                       "}")]
    record-str))

;; Test with problematic data
(def test-data {:session_id "default"
                :state "{:todos {}}"
                :timestamp "2025-01-01"})

(def record-str (build-records-str "test-id" test-data))
(println "Generated RECORDS string:")
(println record-str)

(def sql (str "INSERT INTO sessions RECORDS " record-str))
(println "\nFull SQL:")
(println sql)