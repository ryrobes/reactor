(ns reactor.kafka-reactive-test
  "Test multi-client real-time updates via Kafka."
  (:require [reactor.kafka-reactive :as kafka]
            [reactor.session-simple :as session]
            [reactor.xtdb-store :as xts]
            [clojure.pprint :as pp]))

(defn simulate-client
  "Simulate a client with SQL subscriptions."
  [client-id session-id queries]
  (println (str "\n=== Client " client-id " (" session-id ") ==="))
  (let [subscriptions (atom {})
        results (atom {})]
    
    ;; Create callback to collect results
    (let [callback (fn [{:keys [subscription-id query result]}]
                    (swap! results assoc subscription-id result)
                    (println (str "[Client " client-id "] Query update:"))
                    (println "  Query:" query)
                    (println "  Results:" (take 2 (:results result))))]
      
      ;; Subscribe to queries
      (doseq [[query-name sql] queries]
        (let [sub-id (kafka/register-query-subscription!
                      (str client-id "-" (name query-name))
                      sql
                      nil
                      callback
                      session-id)]
          (swap! subscriptions assoc query-name sub-id)
          (println (str "  Subscribed to " query-name ": " sql))))
      
      {:client-id client-id
       :session-id session-id
       :subscriptions @subscriptions
       :results results})))

(defn -main []
  (println "=== Kafka Reactive Multi-Client Test ===\n")
  
  ;; Initialize systems
  (println "1. Initializing systems...")
  (session/init! :test "test_reactive")
  
  ;; Start Kafka consumer (with mock config for testing)
  (println "2. Starting Kafka consumer...")
  (try
    (kafka/init! {"bootstrap.servers" "10.174.1.144:9092"
                  "group.id" "test-reactor-watcher"})
    (println "   Kafka consumer started successfully")
    (catch Exception e
      (println "   WARNING: Could not connect to Kafka:" (.getMessage e))
      (println "   Continuing with simulation...")))
  
  (Thread/sleep 1000)
  
  ;; Create test data
  (println "\n3. Creating test data...")
  (when-let [node @session/default-node]
    (xts/execute-sql node 
      "INSERT INTO test_reactive (_id, name, status, priority) 
       VALUES ('item-1', 'First Item', 'pending', 1),
              ('item-2', 'Second Item', 'active', 2),
              ('item-3', 'Third Item', 'completed', 3)"))
  
  ;; Simulate multiple clients with different queries
  (println "\n4. Simulating multiple clients...")
  
  (let [;; Client 1: Watches all items
        client1 (simulate-client "A" "session-A" 
                                {:all-items "SELECT * FROM test_reactive ORDER BY priority"
                                 :count "SELECT COUNT(*) as total FROM test_reactive"})
        
        ;; Client 2: Watches only active items
        client2 (simulate-client "B" "session-B"
                                {:active "SELECT * FROM test_reactive WHERE status = 'active'"
                                 :pending "SELECT * FROM test_reactive WHERE status = 'pending'"})
        
        ;; Client 3: Watches high priority items
        client3 (simulate-client "C" "session-C"
                                {:high-priority "SELECT * FROM test_reactive WHERE priority <= 2"})]
    
    ;; Give subscriptions time to execute initial queries
    (Thread/sleep 1000)
    
    ;; Now make changes and observe propagation
    (println "\n5. Making changes to trigger updates...")
    
    ;; Change 1: Update status
    (println "\n   Change 1: Updating item-1 status to 'active'")
    (when-let [node @session/default-node]
      (xts/execute-sql node 
        "UPDATE test_reactive SET status = 'active' WHERE _id = 'item-1'"))
    
    (Thread/sleep 500)
    
    ;; Change 2: Add new item
    (println "\n   Change 2: Adding new high-priority item")
    (when-let [node @session/default-node]
      (xts/execute-sql node
        "INSERT INTO test_reactive (_id, name, status, priority)
         VALUES ('item-4', 'Urgent Item', 'pending', 0)"))
    
    (Thread/sleep 500)
    
    ;; Change 3: Delete item
    (println "\n   Change 3: Deleting completed item")
    (when-let [node @session/default-node]
      (xts/execute-sql node
        "DELETE FROM test_reactive WHERE _id = 'item-3'"))
    
    (Thread/sleep 1000)
    
    ;; Show final state
    (println "\n6. Final subscription states:")
    (println "\nClient A results:")
    (pp/pprint @(:results client1))
    (println "\nClient B results:")
    (pp/pprint @(:results client2))
    (println "\nClient C results:")
    (pp/pprint @(:results client3))
    
    ;; Cleanup
    (println "\n7. Cleaning up...")
    (doseq [[_ sub-id] (:subscriptions client1)]
      (kafka/unregister-query-subscription! sub-id))
    (doseq [[_ sub-id] (:subscriptions client2)]
      (kafka/unregister-query-subscription! sub-id))
    (doseq [[_ sub-id] (:subscriptions client3)]
      (kafka/unregister-query-subscription! sub-id)))
  
  (kafka/shutdown!)
  (println "\n=== Test Completed ===")
  (System/exit 0))

(-main)