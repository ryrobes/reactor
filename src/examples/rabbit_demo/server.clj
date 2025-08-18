(ns examples.rabbit-demo.server
  "Rabbit Demo - SQL data browser with time travel"
  (:require [reactor.server :as r]
            [reactor.session_simple :as session]
            [xtdb.api :as xt]
            [cheshire.core :as json]
            [org.httpkit.server :as http]))

(defn seed-demo-data!
  "Seed the database with demo data if not already present"
  [node]
  (let [db (xt/db node)
        ;; Check if sales data exists
        existing-sales (xt/q db '{:find [?e]
                                  :where [[?e :table :sales]]
                                  :limit 1})]
    (when (empty? existing-sales)
      (println "Seeding demo sales data...")
      ;; Insert sales data
      (xt/submit-tx node
        [[::xt/put {:xt/id :sale-1
                    :table :sales
                    :product "Widget"
                    :amount 100
                    :quantity 5
                    :date "2024-01-01"}]
         [::xt/put {:xt/id :sale-2
                    :table :sales
                    :product "Gadget"
                    :amount 200
                    :quantity 3
                    :date "2024-01-02"}]
         [::xt/put {:xt/id :sale-3
                    :table :sales
                    :product "Doohickey"
                    :amount 150
                    :quantity 7
                    :date "2024-01-03"}]
         [::xt/put {:xt/id :sale-4
                    :table :sales
                    :product "Widget"
                    :amount 120
                    :quantity 4
                    :date "2024-01-04"}]
         [::xt/put {:xt/id :sale-5
                    :table :sales
                    :product "Gadget"
                    :amount 250
                    :quantity 6
                    :date "2024-01-05"}]
         [::xt/put {:xt/id :sale-6
                    :table :sales
                    :product "Doohickey"
                    :amount 75
                    :quantity 2
                    :date "2024-01-06"}]
         [::xt/put {:xt/id :sale-7
                    :table :sales
                    :product "Widget"
                    :amount 300
                    :quantity 10
                    :date "2024-01-07"}]])
      
      ;; Insert inventory data
      (xt/submit-tx node
        [[::xt/put {:xt/id :inv-widget
                    :table :inventory
                    :product "Widget"
                    :quantity 50
                    :reorder-point 20}]
         [::xt/put {:xt/id :inv-gadget
                    :table :inventory
                    :product "Gadget"
                    :quantity 30
                    :reorder-point 15}]
         [::xt/put {:xt/id :inv-doohickey
                    :table :inventory
                    :product "Doohickey"
                    :quantity 75
                    :reorder-point 25}]])
      
      ;; Wait for transactions to be indexed
      (xt/sync node)
      (println "Demo data seeded successfully!"))))

(defn parse-sql-to-datalog
  "Convert SQL to XTDB Datalog query"
  [sql]
  (try
    (cond
      ;; Handle SELECT * FROM sales with ORDER BY
      (re-find #"(?i)SELECT\s+\*\s+FROM\s+sales" sql)
      (let [order-by-match (re-find #"(?i)ORDER\s+BY\s+(\w+)(?:\s+(DESC|ASC))?" sql)
            order-field (when order-by-match (keyword (second order-by-match)))
            order-dir (when order-by-match 
                       (if (= "DESC" (.toUpperCase (or (nth order-by-match 2) "ASC")))
                         :desc
                         :asc))]
        '{:find [(pull ?e [*])]
          :where [[?e :table :sales]]})
      
      ;; Handle SELECT * FROM inventory
      (re-find #"(?i)SELECT\s+\*\s+FROM\s+inventory" sql)
      '{:find [(pull ?e [*])]
        :where [[?e :table :inventory]]}
      
      ;; Default - return all entities
      :else
      '{:find [(pull ?e [*])]
        :where [[?e :xt/id]]})
    (catch Exception e
      (println "Error parsing SQL:" (.getMessage e))
      '{:find [(pull ?e [*])]
        :where [[?e :xt/id]]})))

(defn execute-sql
  "Execute a SQL query on XTDB"
  [node sql & [params as-of]]
  (try
    (let [;; Use as-of time if provided for time travel
          db (if as-of
               (xt/db node (java.util.Date. (- (System/currentTimeMillis) 
                                              (* 1000 60 (Integer/parseInt (str as-of))))))
               (xt/db node))
          query (parse-sql-to-datalog sql)
          results (vec (map first (xt/q db query)))]
      
      ;; Handle ORDER BY in memory for now
      (if-let [order-match (re-find #"(?i)ORDER\s+BY\s+(\w+)(?:\s+(DESC|ASC))?" sql)]
        (let [order-field (keyword (second order-match))
              desc? (= "DESC" (.toUpperCase (or (nth order-match 2) "ASC")))]
          (sort-by order-field (if desc? > <) results))
        results))
    (catch Exception e
      (println "SQL execution error:" (.getMessage e))
      [])))

(defn -main []
  (r/start! 
    :port 5000
    :init-fn (fn []
              (println "Rabbit Demo server started on port 5000")
              ;; Seed demo data on startup
              (when-let [node @session/default-node]
                (seed-demo-data! node)))
    
    :handlers {;; Canvas management
               :add-block (fn [db [block]]
                           (println "ADD-BLOCK called with:" block)
                           (let [result (assoc-in db [:canvas :blocks (:id block)] block)]
                             (println "Result state:" result)
                             result))
               
               :update-block (fn [db [id updates]]
                              (update-in db [:canvas :blocks id] merge updates))
               
               :delete-block (fn [db [id]]
                              (update db [:canvas :blocks] dissoc id))
               
               :move-block (fn [db [id position]]
                            (assoc-in db [:canvas :blocks id :position] position))
               
               :resize-block (fn [db [id size]]
                              (assoc-in db [:canvas :blocks id :size] size))
               
               ;; SQL operations
               :execute-query (fn [db [block-id sql]]
                               ;; TODO: Execute SQL and store results
                               (assoc-in db [:canvas :blocks block-id :sql] sql))
               
               ;; Initialize session and seed data
               :init-rabbit (fn [db _]
                             {:canvas {:blocks {}}
                              :demo-data {:sales [{:id 1 :product "Widget" :amount 100 :date "2024-01-01"}
                                                 {:id 2 :product "Gadget" :amount 200 :date "2024-01-02"}
                                                 {:id 3 :product "Doohickey" :amount 150 :date "2024-01-03"}
                                                 {:id 4 :product "Widget" :amount 120 :date "2024-01-04"}
                                                 {:id 5 :product "Gadget" :amount 250 :date "2024-01-05"}]
                                         :inventory [{:id 1 :product "Widget" :quantity 50}
                                                    {:id 2 :product "Gadget" :quantity 30}
                                                    {:id 3 :product "Doohickey" :quantity 75}]}
                              :data-timeline-index 0
                              :canvas-timeline-index 0})}))