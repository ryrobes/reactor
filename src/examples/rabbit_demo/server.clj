(ns examples.rabbit-demo.server
  "Rabbit Demo - SQL data browser with time travel"
  (:require [reactor.server :as r]
            [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [cheshire.core :as json]
            [org.httpkit.server :as http]))

(defn create-sql-tables!
  "Create SQL tables in XTDB 2.0"
  [node]
  ;; XTDB 2.0 doesn't need explicit table creation for documents
  ;; Tables are implicit when you insert data
  (println "Tables will be created implicitly on first insert in XTDB 2.0"))

(defn seed-demo-data!
  "Seed the database with demo data if not already present"
  [node]
  ;; Check if sales data already exists
  (let [existing-sales (try 
                         (xts/query node "SELECT COUNT(*) as cnt FROM sales")
                         (catch Exception _ []))
        sales-count (or (-> existing-sales first :cnt) 0)]
    (when (zero? sales-count)
      (println "Seeding demo sales data...")
      ;; Insert sales data using XTDB 2.0 SQL
      (xts/execute-sql node 
        "INSERT INTO sales RECORDS 
         {_id: 'sale-1', product: 'Widget', amount: 100, quantity: 5, sale_date: '2024-01-01'},
         {_id: 'sale-2', product: 'Gadget', amount: 200, quantity: 3, sale_date: '2024-01-02'},
         {_id: 'sale-3', product: 'Doohickey', amount: 150, quantity: 7, sale_date: '2024-01-03'},
         {_id: 'sale-4', product: 'Widget', amount: 120, quantity: 4, sale_date: '2024-01-04'},
         {_id: 'sale-5', product: 'Gadget', amount: 250, quantity: 6, sale_date: '2024-01-05'},
         {_id: 'sale-6', product: 'Doohickey', amount: 75, quantity: 2, sale_date: '2024-01-06'},
         {_id: 'sale-7', product: 'Widget', amount: 300, quantity: 10, sale_date: '2024-01-07'}")
      
      ;; Insert inventory data
      (xts/execute-sql node
        "INSERT INTO inventory RECORDS
         {_id: 'inv-widget', product: 'Widget', quantity: 50, reorder_point: 20},
         {_id: 'inv-gadget', product: 'Gadget', quantity: 30, reorder_point: 15},
         {_id: 'inv-doohickey', product: 'Doohickey', quantity: 75, reorder_point: 25}")
      
      (println "Demo data seeded successfully!"))))

;; SQL execution is now handled by session/execute-sql-query in the main server

(defn -main []
  (r/start! 
    :port 5000
    :init-fn (fn []
              ;; Initialize with app name and table for SQL queryability
              (session/init! :rabbit "rabbit_sessions")
              (println "Rabbit Demo server started on port 5000 using table: rabbit_sessions")
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
                              (println "DELETE-BLOCK called with id:" id)
                              (update-in db [:canvas :blocks] dissoc id))
               
               :move-block (fn [db [id position]]
                            (assoc-in db [:canvas :blocks id :position] position))
               
               :resize-block (fn [db [id size]]
                              (assoc-in db [:canvas :blocks id :size] size))
               
               ;; SQL operations
               :execute-query (fn [db [block-id sql]]
                               ;; TODO: Execute SQL and store results
                               (assoc-in db [:canvas :blocks block-id :sql] sql))
               
               ;; Initialize session - only if empty
               :init-rabbit (fn [db _]
                             ;; If db already has canvas data, keep it (loaded from persistence)
                             (if (:canvas db)
                               db  ;; Keep existing data completely
                               {:canvas {:blocks {}}}))}))