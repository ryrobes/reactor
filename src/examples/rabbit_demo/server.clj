(ns examples.rabbit-demo.server
  "Rabbit Demo - SQL data browser with time travel"
  (:require [reactor.reactive-server :as r]
            [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [reactor.tap :as t]
            [reactor.sql-rules :as rules]
            [reactor.kafka-reactive :as kafka]
            [reactor.log :as log]
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

(defn create-demo-rules!
  "Create example rules for the Rabbit Demo"
  [node]
  (println "Creating demo rules...")
  (try
    ;; Initialize rules engine
    (println "Initializing rules engine...")
    (rules/init-rules-engine! node)
    (println "Rules engine initialized")
    
    ;; Check if rules already exist
    (let [existing-rules (xts/execute-sql node "SELECT rule_id FROM reactor_rules")
          existing-rule-ids (set (map :rule_id existing-rules))]
      (println "Found" (count existing-rule-ids) "existing rules in database")
      
      ;; Rule using SQL stacks for sales metrics with unique IDs
      (when-not (contains? existing-rule-ids "sales-metrics-alert")
        (println "Creating sales metrics rule with SQL stacks...")
        (rules/create-rule! node
          {:rule_id "sales-metrics-alert"
           :description "Create alert with real sales metrics using SQL stacks"
           :watch_tables ["sales"]
           :condition_sql "SELECT 1"  ; Always true
           :action_type "sql-insert"
           :action_sql [{:name "metrics"
                         :sql "SELECT COUNT(*) as count, SUM(amount) as total, AVG(amount) as avg, MAX(amount) as max FROM sales"}
                        {:name "alert"
                         :sql "INSERT INTO alerts RECORDS {_id: 'alert-{{uuid}}', type: 'sales_summary', message: 'Sales update: {{metrics.0.count}} transactions, ${{metrics.0.total}} total (avg: ${{metrics.0.avg}})', resolved: false}"}]
           :enabled true
           :priority 100})
        (println "Sales metrics stack rule created"))
    
      ;; Rule 2: Create low stock alerts
      (when-not (contains? existing-rule-ids "low-stock-alert")
        (rules/create-rule! node
          {:rule_id "low-stock-alert"
           :description "Create alert when inventory falls below 10 units"
           :watch_tables ["inventory"]
           :condition_sql "SELECT EXISTS (
                             SELECT 1 FROM inventory 
                             WHERE quantity < 10
                             AND NOT EXISTS (
                               SELECT 1 FROM alerts
                               WHERE type = 'low_stock'
                               AND data->>'product' = inventory.product
                               AND resolved = false
                             )
                           )"
           :action_type "sql-insert"
           :action_sql [{:name "stock_alert"
                         :sql "INSERT INTO low_stock_alerts RECORDS {_id: 'low-stock-alert-{{uuid}}', type: 'low_stock', message: 'Low stock detected', created_at: CURRENT_TIMESTAMP, resolved: false}"}]
           :enabled true
           :priority 90}))
    
      ;; Rule 3: Calculate daily revenue using SQL stacks
      (when-not (contains? existing-rule-ids "calculate-daily-revenue")
        (rules/create-rule! node
          {:rule_id "calculate-daily-revenue"
           :description "Calculate and store daily revenue with unique IDs using SQL stacks"
           :watch_tables ["sales"]
           :condition_sql "SELECT 1"  ; Always execute
           :action_type "sql-execute"
           :action_sql [{:name "daily_stats"
                         :sql "SELECT COUNT(*) as transactions, SUM(amount) as revenue, COUNT(DISTINCT product) as products FROM sales WHERE sale_date = '2024-01-01'"}
                        {:name "insert_summary"
                         :sql "INSERT INTO revenue_summary RECORDS {_id: 'rev-{{timestamp_short}}-{{random}}', date: '2024-01-01', total_revenue: {{daily_stats.0.revenue}}, total_quantity: {{daily_stats.0.transactions}}, product_count: {{daily_stats.0.products}}}"}]
           :enabled true
           :priority 80}))
      
        ;; Rule 4: Auto-reorder products using SQL stacks
      (when-not (contains? existing-rule-ids "auto-reorder")
        (rules/create-rule! node
          {:rule_id "auto-reorder"
           :description "Create purchase orders with real inventory data using SQL stacks"
           :watch_tables ["inventory"]
           :condition_sql "SELECT EXISTS (SELECT 1 FROM inventory WHERE quantity < 5)"
           :action_type "sql-insert"
           :action_sql [{:name "low_stock"
                         :sql "SELECT product, quantity, reorder_point FROM inventory WHERE quantity < 5 LIMIT 1"}
                        {:name "create_po"
                         :sql "INSERT INTO purchase_orders RECORDS {_id: 'po-{{uuid}}', product: '{{low_stock.0.product}}', quantity: 50, current_stock: {{low_stock.0.quantity}}, status: 'pending'}"}]
           :enabled true
           :priority 70}))
      
        ;; Rule 5: Cascading rule example - notify on new purchase orders
      (when-not (contains? existing-rule-ids "notify-new-purchase-order")
        (rules/create-rule! node
                            {:rule_id "notify-new-purchase-order"
                             :description "Create notification when purchase order is created"
                             :watch_tables ["purchase_orders"]
                             :condition_sql "SELECT EXISTS (
                         SELECT 1 FROM purchase_orders
                         WHERE created_at >= NOW() - INTERVAL '1 minute'
                       )"
                             :action_type "sql-insert"
                             :action_sql [{:name "notify1"
                                           :sql "INSERT INTO notifications RECORDS {_id: 'notif-po-{{uuid}}', type: 'purchase_order', message: 'New purchase order created', created_at: CURRENT_TIMESTAMP}"}]
                             :enabled true
                             :priority 60}))
      
        ;; Rule 6: Resolve alerts when stock is replenished
      (when-not (contains? existing-rule-ids "resolve-stock-alerts")
        (rules/create-rule! node
                            {:rule_id "resolve-stock-alerts"
                             :description "Automatically resolve low stock alerts when inventory is replenished"
                             :watch_tables ["inventory"]
                             :condition_sql "SELECT EXISTS (
                         SELECT 1 FROM alerts a
                         JOIN inventory i ON a.data->>'product' = i.product
                         WHERE a.type = 'low_stock'
                         AND a.resolved = false
                         AND i.quantity >= 10
                       )"
                             :action_type "sql-execute"
                             :action_sql "UPDATE low_stock_alerts SET resolved = true WHERE resolved = false"
                             :enabled true
                             :priority 50}))
      
      (println "Successfully created/verified demo rules")
      
      ;; List all rules
      (let [all-rules (xts/execute-sql node "SELECT rule_id, description FROM reactor_rules ORDER BY priority DESC")]
        (println "Active rules in database:")
        (doseq [rule all-rules]
          (println "  -" (:rule_id rule) ":" (:description rule))))
      
      ;; Make sure rules are loaded into memory
      (println "Loading rules into memory...")
      (rules/load-active-rules! node)
      (println "Rules loaded successfully"))
    
    (catch Exception e
      (println "Error creating demo rules:" (.getMessage e)))))

;; SQL execution is now handled by session/execute-sql-query in the main server

(defn -main []
  ;; Initialize Kafka reactive system first
  (try
    (kafka/init! {"bootstrap.servers" "localhost:9092"
                  "group.id" "rabbit-demo"})
    (println "Kafka reactive system initialized")
    (catch Exception e
      (println "WARNING: Could not initialize Kafka:" (.getMessage e))
      (println "Reactive updates will not work")))
  
  (r/start-reactive! 
    :port 5000
    :init-fn (fn []
              ;; Initialize with app name and table for SQL queryability
              (session/init! :rabbit "rabbit_sessions")
              ;; Initialize the colored logging system
              (log/init!)
              (println "Rabbit Demo server started on port 5000 using table: rabbit_sessions")
              ;; Seed demo data and create SQL tables on startup
              (when-let [node @session/default-node]
                (seed-demo-data! node)
                ;; Create demo rules
                (create-demo-rules! node)
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
                              ;; No need to limit results anymore - they won't be persisted
                              (let [result (update-in db [:canvas :blocks id] merge updates)]
                                (println "Block after update:" (get-in result [:canvas :blocks id]))
                                result))
               
               :delete-block (fn [db [id]]
                              (println "DELETE-BLOCK called with id:" id)
                              ;; Note: Client should call rq/unsubscribe-block! before dispatching delete
                              (update-in db [:canvas :blocks] dissoc id))
               
               :move-block (fn [db [id position]]
                            (t/tap> [:test123 id position] ":move-block")
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