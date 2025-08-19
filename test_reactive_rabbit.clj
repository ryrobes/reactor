#!/usr/bin/env clojure

(require '[reactor.session_simple :as session])
(require '[reactor.xtdb-store :as xts])
(require '[reactor.kafka-reactive :as kafka])
(require '[reactor.sql-reactive-bridge :as bridge])

(println "=== Testing Reactive SQL Updates ===\n")

;; Initialize
(session/init! :test "test_reactive")

;; Set up a test callback to track updates
(def updates (atom []))

(defn test-callback [{:keys [subscription-id result]}]
  (println "Received update for subscription:" subscription-id)
  (println "Result count:" (count (:results result)))
  (swap! updates conj result))

;; Register a subscription for a COUNT query
(println "1. Registering subscription for COUNT query...")
(def sub-id (kafka/register-query-subscription!
             "count-test"
             "SELECT COUNT(*) as total FROM test_items"
             nil
             test-callback
             "test-session"))

;; Insert initial data
(println "\n2. Inserting initial data...")
(when-let [node @session/default-node]
  (xts/execute-sql node 
    "INSERT INTO test_items (_id, name, value) VALUES ('item-1', 'First', 100)"))

;; Execute the query initially
(println "\n3. Executing initial query...")
(kafka/re-execute-subscription sub-id)
(Thread/sleep 100)
(println "Initial count:" (-> @updates last :results first :total))

;; Now insert more data and trigger update
(println "\n4. Inserting more data and triggering update...")
(when-let [node @session/default-node]
  (bridge/execute-sql-reactive node 
    "INSERT INTO test_items (_id, name, value) VALUES ('item-2', 'Second', 200)"))

;; Wait for reactive update
(Thread/sleep 500)

(println "\n5. Checking if count updated...")
(if (> (count @updates) 1)
  (do
    (println "SUCCESS! Received" (count @updates) "updates")
    (println "Final count:" (-> @updates last :results first :total)))
  (do
    (println "No automatic update received.")
    (println "Manually triggering update...")
    (bridge/notify-table-change! "test_items")
    (Thread/sleep 500)
    (if (> (count @updates) 1)
      (println "Update received after manual trigger")
      (println "Still no update - check Kafka connection"))))

;; Clean up
(kafka/unregister-query-subscription! sub-id)
(when-let [node @session/default-node]
  (xts/execute-sql node "DELETE FROM test_items"))

(println "\n=== Test Complete ===")
(System/exit 0)