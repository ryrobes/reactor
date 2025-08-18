(ns examples.rabbit-demo.server
  "Rabbit Demo - SQL data browser with time travel"
  (:require [reactor.server :as r]
            [reactor.session_simple :as session]
            [xtdb.api :as xt]
            [xtdb.calcite]
            [cheshire.core :as json]
            [org.httpkit.server :as http]))

(defn create-sql-tables!
  "Create SQL table schemas for XTDB SQL queries"
  [node]
  ;; Define SQL table schema for sales
  (xt/submit-tx node
    [[::xt/put {:xt/id :xtdb.sql/sales-schema
                :xtdb.sql/table-name "sales"
                :xtdb.sql/table-query '{:find [?id ?product ?amount ?quantity ?sale_date]
                                        :where [[?id :table "sales"]
                                                [?id :product ?product]
                                                [?id :amount ?amount]
                                                [?id :quantity ?quantity]
                                                [?id :sale_date ?sale_date]]}
                :xtdb.sql/table-columns '{?id :varchar
                                          ?product :varchar
                                          ?amount :bigint
                                          ?quantity :bigint
                                          ?sale_date :varchar}}]
     ;; Define SQL table schema for inventory
     [::xt/put {:xt/id :xtdb.sql/inventory-schema
                :xtdb.sql/table-name "inventory"
                :xtdb.sql/table-query '{:find [?id ?product ?quantity ?reorder_point]
                                        :where [[?id :table "inventory"]
                                                [?id :product ?product]
                                                [?id :quantity ?quantity]
                                                [?id :reorder_point ?reorder_point]]}
                :xtdb.sql/table-columns '{?id :varchar
                                          ?product :varchar
                                          ?quantity :bigint
                                          ?reorder_point :bigint}}]])
  (xt/sync node)
  (println "SQL table schemas created"))

(defn seed-demo-data!
  "Seed the database with demo data if not already present"
  [node]
  (let [db (xt/db node)
        ;; Check if any data exists
        existing-data (xt/q db '{:find [?e]
                                :where [[?e :xt/id]]
                                :limit 1})]
    (when (empty? existing-data)
      (println "Seeding demo sales data...")
      ;; Create tables using XTDB SQL - documents will be queryable as tables
      ;; XTDB SQL works with documents - we use a :table field to group them
      (xt/submit-tx node
        [[::xt/put {:xt/id "sale-1"
                    :table "sales"
                    :product "Widget"
                    :amount 100
                    :quantity 5
                    :sale_date "2024-01-01"}]
         [::xt/put {:xt/id "sale-2"
                    :table "sales"
                    :product "Gadget"
                    :amount 200
                    :quantity 3
                    :sale_date "2024-01-02"}]
         [::xt/put {:xt/id "sale-3"
                    :table "sales"
                    :product "Doohickey"
                    :amount 150
                    :quantity 7
                    :sale_date "2024-01-03"}]
         [::xt/put {:xt/id "sale-4"
                    :table "sales"
                    :product "Widget"
                    :amount 120
                    :quantity 4
                    :sale_date "2024-01-04"}]
         [::xt/put {:xt/id "sale-5"
                    :table "sales"
                    :product "Gadget"
                    :amount 250
                    :quantity 6
                    :sale_date "2024-01-05"}]
         [::xt/put {:xt/id "sale-6"
                    :table "sales"
                    :product "Doohickey"
                    :amount 75
                    :quantity 2
                    :sale_date "2024-01-06"}]
         [::xt/put {:xt/id "sale-7"
                    :table "sales"
                    :product "Widget"
                    :amount 300
                    :quantity 10
                    :sale_date "2024-01-07"}]])
      
      ;; Insert inventory data
      (xt/submit-tx node
        [[::xt/put {:xt/id "inv-widget"
                    :table "inventory"
                    :product "Widget"
                    :quantity 50
                    :reorder_point 20}]
         [::xt/put {:xt/id "inv-gadget"
                    :table "inventory"
                    :product "Gadget"
                    :quantity 30
                    :reorder_point 15}]
         [::xt/put {:xt/id "inv-doohickey"
                    :table "inventory"
                    :product "Doohickey"
                    :quantity 75
                    :reorder_point 25}]])
      
      ;; Wait for transactions to be indexed
      (xt/sync node)
      (println "Demo data seeded successfully!")
      ;; Create SQL table schemas
      (create-sql-tables! node)
      (println "SQL tables configured"))))

;; SQL execution is now handled by session/execute-sql-query in the main server

(defn -main []
  (r/start! 
    :port 5000
    :init-fn (fn []
              (println "Rabbit Demo server started on port 5000")
              ;; Seed demo data and create SQL tables on startup
              (when-let [node @session/default-node]
                (seed-demo-data! node)
                ;; Give SQL server time to initialize
                (Thread/sleep 1000)))
    
    :handlers {;; Canvas management
               :add-block (fn [db [block]]
                           (println "ADD-BLOCK called with:" block)
                           (let [result (assoc-in db [:canvas :blocks (:id block)] block)]
                             (println "Result state:" result)
                             result))
               
               :update-block (fn [db [id updates]]
                              (println "UPDATE-BLOCK:" id "with" updates)
                              (let [result (update-in db [:canvas :blocks id] merge updates)]
                                (println "Block after update:" (get-in result [:canvas :blocks id]))
                                result))
               
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