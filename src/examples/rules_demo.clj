(ns examples.rules-demo
  "Demo of SQL-based reactive rules system"
  (:require [reactor.sql-rules :as rules]
            [reactor.xtdb-store :as xts]
            [reactor.session_simple :as session]
            [clojure.tools.logging :as log]))

(defn setup-demo-tables!
  "Create demo tables for testing rules"
  [node]
  ;; Insert sample data to create tables implicitly
  (xts/execute-sql node
    "INSERT INTO products (_id, name, base_price, stock_quantity, reorder_point)
     VALUES ('prod-1', 'Widget A', 10.00, 100, 20),
            ('prod-2', 'Widget B', 15.00, 5, 10),
            ('prod-3', 'Widget C', 20.00, 50, 15)")
  
  (xts/execute-sql node
    "INSERT INTO price_adjustments (_id, product_id, discount_percent, active)
     VALUES ('adj-1', 'prod-1', 10, true),
            ('adj-2', 'prod-2', 15, true)")
  
  (xts/execute-sql node
    "INSERT INTO alerts (_id, type, message, created_at, resolved)
     VALUES ('alert-1', 'test', 'System initialized', NOW(), false)")
  
  (log/info "Demo tables created"))

(defn create-demo-rules!
  "Create example rules demonstrating different patterns"
  [node]
  
  ;; Rule 1: Calculate final prices when base prices change
  (rules/create-rule! node
    {:rule_id "calculate-final-prices"
     :description "Update final prices when base prices or discounts change"
     :watch_tables ["products" "price_adjustments"]
     :condition_sql "SELECT EXISTS (
                       SELECT 1 FROM products p 
                       LEFT JOIN price_adjustments pa ON p._id = pa.product_id
                       WHERE pa.active = true
                     )"
     :action_type "sql-execute"
     :action_sql "UPDATE products 
                  SET final_price = base_price * (1 - COALESCE(
                    (SELECT discount_percent / 100.0 
                     FROM price_adjustments 
                     WHERE product_id = products._id 
                     AND active = true), 0))"
     :enabled true
     :priority 100})
  
  ;; Rule 2: Create low stock alerts
  (rules/create-rule! node
    {:rule_id "low-stock-alert"
     :description "Create alerts when stock falls below reorder point"
     :watch_tables ["products"]
     :condition_sql "SELECT EXISTS (
                       SELECT 1 FROM products 
                       WHERE stock_quantity < reorder_point
                       AND NOT EXISTS (
                         SELECT 1 FROM alerts 
                         WHERE type = 'low_stock' 
                         AND data->>'product_id' = products._id
                         AND resolved = false
                       )
                     )"
     :action_type "sql-insert"
     :action_sql "INSERT INTO alerts (_id, type, message, data, created_at, resolved)
                  SELECT 
                    'alert-' || gen_random_uuid(),
                    'low_stock',
                    'Low stock for ' || name || ': ' || stock_quantity || ' units',
                    jsonb_build_object('product_id', _id, 'quantity', stock_quantity),
                    NOW(),
                    false
                  FROM products
                  WHERE stock_quantity < reorder_point"
     :enabled true
     :priority 90})
  
  ;; Rule 3: Auto-resolve alerts when stock is replenished
  (rules/create-rule! node
    {:rule_id "resolve-stock-alerts"
     :description "Resolve low stock alerts when inventory is replenished"
     :watch_tables ["products"]
     :condition_sql "SELECT EXISTS (
                       SELECT 1 FROM alerts a
                       JOIN products p ON a.data->>'product_id' = p._id
                       WHERE a.type = 'low_stock'
                       AND a.resolved = false
                       AND p.stock_quantity >= p.reorder_point
                     )"
     :action_type "sql-execute"
     :action_sql "UPDATE alerts 
                  SET resolved = true, 
                      resolved_at = NOW()
                  WHERE type = 'low_stock'
                  AND resolved = false
                  AND EXISTS (
                    SELECT 1 FROM products p
                    WHERE p._id = alerts.data->>'product_id'
                    AND p.stock_quantity >= p.reorder_point
                  )"
     :enabled true
     :priority 80})
  
  ;; Rule 4: Cascade rule - Update order totals when products change
  (rules/create-rule! node
    {:rule_id "update-order-totals"
     :description "Recalculate order totals when product prices change"
     :watch_tables ["products"]
     :condition_sql "SELECT EXISTS (
                       SELECT 1 FROM order_items oi
                       JOIN products p ON oi.product_id = p._id
                     )"
     :action_type "sql-execute"
     :action_sql "UPDATE orders
                  SET total = (
                    SELECT SUM(oi.quantity * p.final_price)
                    FROM order_items oi
                    JOIN products p ON oi.product_id = p._id
                    WHERE oi.order_id = orders._id
                  )
                  WHERE EXISTS (
                    SELECT 1 FROM order_items WHERE order_id = orders._id
                  )"
     :enabled true
     :priority 70})
  
  (log/info "Demo rules created"))

(defn test-rule-execution!
  "Test rule execution with sample changes"
  [node]
  (log/info "=== Testing Rule Execution ===")
  
  ;; Initialize rules engine
  (rules/init-rules-engine! node)
  
  ;; Test 1: Update base price (should trigger price calculation)
  (log/info "Test 1: Updating base price...")
  (xts/execute-sql node
    "UPDATE products SET base_price = 12.00 WHERE _id = 'prod-1'")
  (Thread/sleep 1000)
  
  ;; Check final price was calculated
  (let [result (xts/execute-sql node 
                 "SELECT _id, base_price, final_price 
                  FROM products WHERE _id = 'prod-1'")]
    (log/info "Product after price rule:" result))
  
  ;; Test 2: Lower stock (should trigger alert)
  (log/info "Test 2: Lowering stock quantity...")
  (xts/execute-sql node
    "UPDATE products SET stock_quantity = 3 WHERE _id = 'prod-2'")
  (Thread/sleep 1000)
  
  ;; Check if alert was created
  (let [alerts (xts/execute-sql node
                 "SELECT type, message, data 
                  FROM alerts 
                  WHERE type = 'low_stock' 
                  ORDER BY created_at DESC 
                  LIMIT 5")]
    (log/info "Alerts after stock rule:" alerts))
  
  ;; Test 3: Replenish stock (should resolve alert)
  (log/info "Test 3: Replenishing stock...")
  (xts/execute-sql node
    "UPDATE products SET stock_quantity = 50 WHERE _id = 'prod-2'")
  (Thread/sleep 1000)
  
  ;; Check if alert was resolved
  (let [alerts (xts/execute-sql node
                 "SELECT type, resolved, resolved_at 
                  FROM alerts 
                  WHERE type = 'low_stock' 
                  AND data->>'product_id' = 'prod-2'")]
    (log/info "Alerts after replenishment:" alerts))
  
  ;; Test 4: Check rule execution history
  (log/info "Test 4: Checking rule execution history...")
  (let [executions (xts/execute-sql node
                     "SELECT rule_id, triggered_by, condition_result, 
                             action_executed, execution_time_ms
                      FROM reactor_rule_executions
                      ORDER BY executed_at DESC
                      LIMIT 10")]
    (log/info "Recent rule executions:" executions)))

(defn visualize-rule-flow
  "Query and display rule execution flow"
  [node correlation-id]
  (let [executions (xts/execute-sql node
                     "SELECT rule_id, triggered_by, trigger_source, 
                             action_executed, execution_time_ms
                      FROM reactor_rule_executions
                      WHERE correlation_id = ?
                      ORDER BY executed_at"
                     correlation-id)]
    (log/info "Rule flow for correlation" correlation-id ":")
    (doseq [exec executions]
      (log/info "  ->" (:rule_id exec) 
                (if (:action_executed exec) "✓" "✗")
                (str "(" (:execution_time_ms exec) "ms)")))))

(defn -main
  "Run the rules demo"
  [& args]
  (log/info "Starting SQL Rules Demo...")
  
  ;; Initialize XTDB
  (reset! session/default-node (xts/start-xtdb-node))
  
  ;; Setup demo data
  (setup-demo-tables! @session/default-node)
  
  ;; Create rules
  (create-demo-rules! @session/default-node)
  
  ;; Test execution
  (test-rule-execution! @session/default-node)
  
  (log/info "Rules demo complete!")
  (System/exit 0))