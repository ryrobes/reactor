(ns test-regular-subs
  "Test that regular (non-temporal) subscriptions still work"
  (:require [reactor.kafka-reactive :as kafka]))

(defn test-subscription-types []
  (println "\n=== Testing Subscription Type Detection ===")
  
  ;; Test regular query (no as-of)
  (let [reg-sub-id "test-regular"
        reg-sql "SELECT * FROM sales"]
    (kafka/register-query-subscription! 
     reg-sub-id reg-sql nil 
     (fn [msg] (println "Callback called"))
     "test-session"
     nil  ;; client-id
     false)  ;; NOT temporal
    
    (let [sub-info (get @kafka/active-subscriptions reg-sub-id)]
      (println "\nRegular subscription:")
      (println "  Temporal?:" (:temporal? sub-info))
      (println "  Inert?:" (:inert? sub-info))
      (println "  In table-to-subs?:" (contains? (get @kafka/table-to-subs "sales") reg-sub-id))
      (assert (not (:temporal? sub-info)) "Regular sub should NOT be temporal")
      (assert (not (:inert? sub-info)) "Regular sub should NOT be inert")))
  
  ;; Test temporal query (with as-of passed as param)
  (let [temp-sub-id "test-temporal"
        temp-sql "SELECT * FROM sales"]  ;; Same SQL, but will be marked temporal
    (kafka/register-query-subscription! 
     temp-sub-id temp-sql nil 
     (fn [msg] (println "Callback called"))
     "test-session"
     nil  ;; client-id
     true)  ;; IS temporal (as-of passed)
    
    (let [sub-info (get @kafka/active-subscriptions temp-sub-id)]
      (println "\nTemporal subscription (via param):")
      (println "  Temporal?:" (:temporal? sub-info))
      (println "  Inert?:" (:inert? sub-info))
      (println "  In table-to-subs?:" (contains? (get @kafka/table-to-subs "sales") temp-sub-id))
      (assert (:temporal? sub-info) "Temporal sub SHOULD be temporal")
      (assert (:inert? sub-info) "Temporal sub SHOULD be inert")))
  
  ;; Test temporal query with SQL clause
  (let [sql-temp-id "test-sql-temporal"
        sql-temp "SELECT * FROM sales FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z'"]
    (kafka/register-query-subscription! 
     sql-temp-id sql-temp nil 
     (fn [msg] (println "Callback called"))
     "test-session")
    
    (let [sub-info (get @kafka/active-subscriptions sql-temp-id)]
      (println "\nTemporal subscription (via SQL):")
      (println "  Temporal?:" (:temporal? sub-info))
      (println "  Inert?:" (:inert? sub-info))
      (println "  In table-to-subs?:" (contains? (get @kafka/table-to-subs "sales") sql-temp-id))
      (assert (:temporal? sub-info) "SQL temporal sub SHOULD be temporal")
      (assert (:inert? sub-info) "SQL temporal sub SHOULD be inert")))
  
  (println "\n=== Table-to-subs mapping ===")
  (println "Sales table subscriptions:" (get @kafka/table-to-subs "sales"))
  (println "Should only contain 'test-regular', not the temporal ones")
  
  ;; Clean up
  (kafka/unregister-query-subscription! "test-regular")
  (kafka/unregister-query-subscription! "test-temporal")
  (kafka/unregister-query-subscription! "test-sql-temporal")
  
  (println "\n✅ All tests passed!"))

(test-subscription-types)