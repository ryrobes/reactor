(ns examples.rabbit-demo.server
  "Rabbit Demo - SQL data browser with time travel"
  (:require [reactor.reactive-server :as r]
            [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [reactor.tap :as t]
            [reactor.sql-rules :as rules]
            [reactor.kafka-reactive :as kafka]
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
    (rules/init-rules-engine! node)
    
    ;; Rule 1: Update inventory when sales are made
    (rules/create-rule! node
      {:rule_id "update-inventory-on-sale"
       :description "Decrease inventory quantity when a sale is recorded"
       :watch_tables ["sales"]
       :condition_sql "SELECT EXISTS (
                         SELECT 1 FROM sales s
                         WHERE s.sale_date >= CURRENT_DATE - INTERVAL '1 minute'
                       )"
       :action_type "sql-execute"
       :action_sql "UPDATE inventory 
                    SET quantity = quantity - (
                      SELECT COALESCE(SUM(s.quantity), 0)
                      FROM sales s
                      WHERE s.product = inventory.product
                      AND s.sale_date >= CURRENT_DATE - INTERVAL '1 minute'
                    )
                    WHERE product IN (
                      SELECT DISTINCT product FROM sales 
                      WHERE sale_date >= CURRENT_DATE - INTERVAL '1 minute'
                    )"
       :enabled true
       :priority 100})
    
    ;; Rule 2: Create low stock alerts
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
       :action_sql "INSERT INTO alerts (_id, type, message, data, created_at, resolved)
                    SELECT 
                      'alert-' || gen_random_uuid(),
                      'low_stock',
                      'Low stock alert: ' || product || ' has only ' || quantity || ' units',
                      jsonb_build_object('product', product, 'quantity', quantity),
                      NOW(),
                      false
                    FROM inventory
                    WHERE quantity < 10
                    AND NOT EXISTS (
                      SELECT 1 FROM alerts a
                      WHERE a.type = 'low_stock'
                      AND a.data->>'product' = inventory.product
                      AND a.resolved = false
                    )"
       :enabled true
       :priority 90})
    
    ;; Rule 3: Calculate daily revenue summary
    (rules/create-rule! node
      {:rule_id "calculate-daily-revenue"
       :description "Update daily revenue summary when sales change"
       :watch_tables ["sales"]
       :condition_sql "SELECT 1"  ; Always execute
       :action_type "sql-execute"
       :action_sql "INSERT INTO revenue_summary (_id, date, total_revenue, total_quantity, product_count)
                    SELECT 
                      'rev-' || sale_date,
                      sale_date,
                      SUM(amount),
                      SUM(quantity),
                      COUNT(DISTINCT product)
                    FROM sales
                    GROUP BY sale_date
                    ON CONFLICT (_id) DO UPDATE SET
                      total_revenue = EXCLUDED.total_revenue,
                      total_quantity = EXCLUDED.total_quantity,
                      product_count = EXCLUDED.product_count"
       :enabled true
       :priority 80})
    
    ;; Rule 4: Auto-reorder products
    (rules/create-rule! node
      {:rule_id "auto-reorder"
       :description "Create purchase orders when inventory is low"
       :watch_tables ["inventory"]
       :condition_sql "SELECT EXISTS (
                         SELECT 1 FROM inventory 
                         WHERE quantity < 5
                         AND NOT EXISTS (
                           SELECT 1 FROM purchase_orders po
                           WHERE po.product = inventory.product
                           AND po.status = 'pending'
                         )
                       )"
       :action_type "sql-insert"
       :action_sql "INSERT INTO purchase_orders (_id, product, quantity, status, created_at)
                    SELECT 
                      'po-' || gen_random_uuid(),
                      product,
                      50, -- Standard reorder quantity
                      'pending',
                      NOW()
                    FROM inventory
                    WHERE quantity < 5
                    AND NOT EXISTS (
                      SELECT 1 FROM purchase_orders po
                      WHERE po.product = inventory.product
                      AND po.status = 'pending'
                    )"
       :enabled true
       :priority 70})
    
    ;; Rule 5: Cascading rule example - notify on new purchase orders
    (rules/create-rule! node
      {:rule_id "notify-new-purchase-order"
       :description "Create notification when purchase order is created"
       :watch_tables ["purchase_orders"]
       :condition_sql "SELECT EXISTS (
                         SELECT 1 FROM purchase_orders
                         WHERE created_at >= NOW() - INTERVAL '1 minute'
                       )"
       :action_type "sql-insert"
       :action_sql "INSERT INTO notifications (_id, type, message, created_at)
                    SELECT 
                      'notif-' || gen_random_uuid(),
                      'purchase_order',
                      'New purchase order created for ' || product || ' (qty: ' || quantity || ')',
                      NOW()
                    FROM purchase_orders
                    WHERE created_at >= NOW() - INTERVAL '1 minute'"
       :enabled true
       :priority 60})
    
    ;; Rule 6: Resolve alerts when stock is replenished
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
       :action_sql "UPDATE alerts
                    SET resolved = true,
                        resolved_at = NOW()
                    WHERE type = 'low_stock'
                    AND resolved = false
                    AND EXISTS (
                      SELECT 1 FROM inventory i
                      WHERE i.product = alerts.data->>'product'
                      AND i.quantity >= 10
                    )"
       :enabled true
       :priority 50})
    
    (println "Successfully created 6 demo rules")
    
    ;; List all rules
    (let [all-rules (xts/execute-sql node "SELECT rule_id, description FROM reactor_rules ORDER BY priority DESC")]
      (println "Active rules:")
      (doseq [rule all-rules]
        (println "  -" (:rule_id rule) ":" (:description rule))))
    
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