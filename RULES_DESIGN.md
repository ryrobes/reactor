# SQL-Based Reactive Rules System Design

## Core Concept
Rules are server-side subscriptions with conditional logic that execute actions when data changes match specific patterns. They form implicit flow graphs through their cascading executions.

## Rule Structure

```clojure
{:rule-id "calculate-order-total"
 :description "When order_items change, recalculate order total"
 
 ;; Trigger configuration
 :watch-tables ["order_items"]  ; Tables that trigger this rule
 :trigger-mode :immediate        ; :immediate, :debounced, :batched
 :debounce-ms 500               ; For debounced mode
 
 ;; Condition (SQL that returns truthy result)
 :condition-sql "SELECT EXISTS (
                   SELECT 1 FROM order_items 
                   WHERE order_id = $1 
                   AND updated_at > NOW() - INTERVAL '1 second'
                 )"
 :condition-params [:transaction/affected-ids]  ; Dynamic params from context
 
 ;; Action configuration
 :action-type :sql-execute       ; :sql-execute, :sql-insert, :function, :event
 :action-sql "UPDATE orders 
              SET total = (SELECT SUM(price * quantity) 
                          FROM order_items 
                          WHERE order_id = $1)
              WHERE id = $1"
 :action-params [:transaction/affected-ids]
 
 ;; Control flow
 :enabled true
 :priority 100                   ; Higher priority executes first
 :max-executions-per-minute 60   ; Rate limiting
 :timeout-ms 5000
 
 ;; Metadata
 :created-at #inst "2024-01-01"
 :created-by "system"
 :tags ["finance" "order-processing"]}
```

## Database Schema

```sql
-- Rules definition table
CREATE TABLE reactor_rules (
  _id TEXT PRIMARY KEY,
  rule_id TEXT UNIQUE NOT NULL,
  description TEXT,
  watch_tables TEXT[], -- Array of table names
  trigger_mode TEXT DEFAULT 'immediate',
  debounce_ms INTEGER,
  condition_sql TEXT NOT NULL,
  condition_params JSONB,
  action_type TEXT NOT NULL,
  action_sql TEXT,
  action_function TEXT,
  action_params JSONB,
  enabled BOOLEAN DEFAULT true,
  priority INTEGER DEFAULT 0,
  max_executions_per_minute INTEGER,
  timeout_ms INTEGER DEFAULT 5000,
  created_at TIMESTAMP DEFAULT NOW(),
  created_by TEXT,
  tags TEXT[]
);

-- Rule execution history (for flow graph generation)
CREATE TABLE reactor_rule_executions (
  _id TEXT PRIMARY KEY,
  rule_id TEXT REFERENCES reactor_rules(rule_id),
  triggered_by TEXT, -- 'table_change', 'rule_cascade', 'manual'
  trigger_source JSONB, -- {table: 'orders', transaction_id: '...', parent_rule_id: '...'}
  condition_result BOOLEAN,
  action_executed BOOLEAN,
  action_result JSONB,
  affected_tables TEXT[],
  affected_ids TEXT[],
  execution_time_ms INTEGER,
  executed_at TIMESTAMP DEFAULT NOW(),
  session_id TEXT,
  correlation_id TEXT -- Groups related rule executions
);

-- Rule flow tracking (for visualization)
CREATE TABLE reactor_rule_flows (
  _id TEXT PRIMARY KEY,
  correlation_id TEXT,
  flow_graph JSONB, -- Adjacency list or tree structure
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  total_rules_executed INTEGER,
  max_depth INTEGER,
  created_at TIMESTAMP DEFAULT NOW()
);
```

## Implementation Components

### 1. Rule Registry (in Kafka Consumer)

```clojure
(defn load-active-rules [node]
  (let [rules (xts/execute-sql node 
                "SELECT * FROM reactor_rules 
                 WHERE enabled = true 
                 ORDER BY priority DESC")]
    (group-by :watch-tables rules)))

(defn should-evaluate-rule? [rule change-context]
  (and (:enabled rule)
       (some #(contains? (:affected-tables change-context) %) 
             (:watch-tables rule))
       (check-rate-limit rule)))
```

### 2. Rule Evaluator

```clojure
(defn evaluate-rule-condition [node rule context]
  (let [params (resolve-params (:condition-params rule) context)
        result (xts/execute-sql node (:condition-sql rule) params)]
    (not (empty? result))))

(defn execute-rule-action [node rule context]
  (case (:action-type rule)
    :sql-execute 
    (xts/execute-sql node 
                     (:action-sql rule) 
                     (resolve-params (:action-params rule) context))
    
    :function
    (call-registered-function (:action-function rule) context)
    
    :event
    (dispatch-event (:action-params rule) context)))
```

### 3. Flow Graph Builder

```clojure
(defn track-rule-execution [node rule-id context result correlation-id]
  (let [execution-id (str "exec-" (UUID/randomUUID))]
    (xts/execute-sql node
      "INSERT INTO reactor_rule_executions (...) VALUES (...)"
      execution-id rule-id context result correlation-id)
    
    ;; Update flow graph
    (update-flow-graph node correlation-id rule-id context)))
```

## Example Rules

### 1. Inventory Management
```clojure
{:rule-id "low-stock-alert"
 :watch-tables ["inventory"]
 :condition-sql "SELECT * FROM inventory WHERE quantity < reorder_point"
 :action-type :sql-insert
 :action-sql "INSERT INTO alerts (type, message, data) 
              SELECT 'low_stock', 
                     'Product ' || name || ' is low on stock',
                     jsonb_build_object('product_id', id, 'quantity', quantity)
              FROM inventory WHERE quantity < reorder_point"}
```

### 2. Cascading Price Update
```clojure
{:rule-id "update-derived-prices"
 :watch-tables ["base_prices"]
 :condition-sql "SELECT 1 FROM base_prices WHERE updated_at > NOW() - INTERVAL '1 minute'"
 :action-type :sql-execute
 :action-sql "UPDATE product_prices 
              SET sale_price = bp.price * (1 - COALESCE(d.discount, 0))
              FROM base_prices bp
              LEFT JOIN discounts d ON bp.product_id = d.product_id
              WHERE product_prices.product_id = bp.product_id"}
```

### 3. Workflow Automation
```clojure
{:rule-id "order-fulfillment-workflow"
 :watch-tables ["orders"]
 :condition-sql "SELECT * FROM orders WHERE status = 'paid' AND fulfillment_status = 'pending'"
 :action-type :function
 :action-function "initiate-fulfillment-workflow"
 :action-params {:warehouse-selection "nearest"
                 :shipping-priority "standard"}}
```

## Advanced Features

### 1. Debouncing
Rules can wait for changes to stabilize before executing:
```clojure
{:trigger-mode :debounced
 :debounce-ms 1000  ; Wait 1 second after last change}
```

### 2. Batching
Accumulate multiple changes and process together:
```clojure
{:trigger-mode :batched
 :batch-size 100
 :batch-timeout-ms 5000}
```

### 3. Conditional Cascades
Rules can check if they're part of a cascade:
```clojure
{:condition-sql "SELECT 1 WHERE NOT EXISTS (
                  SELECT 1 FROM reactor_rule_executions 
                  WHERE correlation_id = $1 
                  AND rule_id = 'prevent-recursive-rule'
                 )"}
```

### 4. Transaction Boundaries
Ensure cascading rules execute atomically:
```clojure
(defn execute-rule-cascade [node initial-rule context]
  (xts/with-transaction [tx node]
    (let [correlation-id (UUID/randomUUID)]
      (execute-rules-recursively tx initial-rule context correlation-id))))
```

## Flow Graph Visualization
The execution data can be queried to generate:
- Dependency graphs showing which rules trigger others
- Execution timelines showing cascade patterns
- Hot path analysis showing most frequent rule chains
- Performance bottleneck identification

## Benefits
1. **Declarative**: Rules defined as data, not code
2. **Observable**: All executions tracked in database
3. **Debuggable**: Complete execution history with flow graphs
4. **Performant**: Leverages SQL and indexes
5. **Flexible**: Multiple action types, complex conditions
6. **Composable**: Rules can trigger other rules, creating workflows
7. **Versioned**: Rules stored in bitemporal XTDB

This system turns your database into a reactive workflow engine!