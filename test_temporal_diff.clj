(ns test-temporal-diff
  "Test script to verify temporal queries work with diffing"
  (:require [reactor.kafka-reactive :as kafka]
            [reactor.reactive-server :as server]
            [reactor.xtdb-store :as xts]
            [reactor.session_simple :as session]
            [clojure.tools.logging :as log]))

(defn test-temporal-subscription []
  (println "\n=== Testing Temporal Query Subscriptions ===")
  
  ;; Check configuration
  (println "\nDiff Configuration:")
  (println "  Field-based diffing:" (:field-based? @kafka/diff-config))
  (println "  Temporal always diff:" (:temporal-always-diff @kafka/diff-config))
  
  ;; Create a test temporal query
  (let [temporal-sql "SELECT * FROM sales 
                      FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z'"
        regular-sql "SELECT * FROM sales"
        session-id "test-session"]
    
    ;; Register temporal subscription
    (println "\nRegistering temporal subscription...")
    (let [temp-sub-id (kafka/subscribe-query! session-id temporal-sql nil)]
      (println "  Temporal subscription ID:" temp-sub-id)
      (when-let [sub-info (get @kafka/active-subscriptions temp-sub-id)]
        (println "  Is temporal?:" (:temporal? sub-info))
        (println "  Is inert?:" (:inert? sub-info))
        (println "  Tables:" (:tables sub-info))))
    
    ;; Register regular subscription
    (println "\nRegistering regular subscription...")
    (let [reg-sub-id (kafka/subscribe-query! session-id regular-sql nil)]
      (println "  Regular subscription ID:" reg-sub-id)
      (when-let [sub-info (get @kafka/active-subscriptions reg-sub-id)]
        (println "  Is temporal?:" (:temporal? sub-info))
        (println "  Is inert?:" (:inert? sub-info))
        (println "  Tables:" (:tables sub-info))))
    
    ;; Check table-to-subs mapping
    (println "\nTable-to-subscription mapping:")
    (doseq [[table subs] @kafka/table-to-subs]
      (println "  Table" table "has" (count subs) "subscriptions:" subs))
    
    ;; Test that temporal queries don't trigger on changes
    (println "\nTesting that temporal queries are inert...")
    (let [temp-sub (first (filter #(:temporal? (val %)) @kafka/active-subscriptions))
          reg-sub (first (filter #(not (:temporal? (val %))) @kafka/active-subscriptions))]
      (when temp-sub
        (println "  Requesting re-execution for temporal sub" (key temp-sub))
        (kafka/request-re-execution! (key temp-sub))
        (println "    Should see 'Skipping re-execution' log above"))
      (when reg-sub
        (println "  Requesting re-execution for regular sub" (key reg-sub))
        (kafka/request-re-execution! (key reg-sub))
        (println "    Should see 'Debounced re-execution requested' log above")))
    
    ;; Check diff stats
    (println "\nDiff Statistics:")
    (println (kafka/get-diff-stats))))

(defn test-temporal-diff-caching []
  (println "\n=== Testing Temporal Query Diff Caching ===")
  
  ;; Simulate multiple temporal queries at same timestamp
  (let [timestamp "2024-01-01T00:00:00Z"
        query1 (str "SELECT id, name, amount FROM sales "
                   "FOR SYSTEM_TIME AS OF TIMESTAMP '" timestamp "'")
        query2 (str "SELECT id, name, amount, category FROM sales "
                   "FOR SYSTEM_TIME AS OF TIMESTAMP '" timestamp "'")
        session-id "test-session"]
    
    (println "\nCreating temporal subscriptions at same timestamp...")
    (println "  Query 1: SELECT id, name, amount...")
    (println "  Query 2: SELECT id, name, amount, category...")
    
    ;; These should both benefit from diffing if they share cache
    (let [sub1 (kafka/subscribe-query! session-id query1 nil)
          sub2 (kafka/subscribe-query! session-id query2 nil)]
      (println "\nSubscriptions created:")
      (println "  Sub 1:" sub1)
      (println "  Sub 2:" sub2)
      
      ;; Check cache entries
      (println "\nCache entries:" (count @kafka/client-result-cache))
      (doseq [[k v] (take 2 @kafka/client-result-cache)]
        (println "  Key:" k)
        (println "    Last diff type:" (:last-diff-type v))
        (println "    Timestamp:" (:timestamp v))))))

(defn run-tests []
  ;; Ensure diff mode is set to field-based
  (kafka/set-diff-mode! :field)
  
  (test-temporal-subscription)
  (test-temporal-diff-caching)
  
  (println "\n=== Test Complete ===")
  (println "Temporal queries are now:" 
           (if (pos? (count (filter #(:temporal? (val %)) @kafka/active-subscriptions)))
             "ENABLED for subscriptions (can be diffed)"
             "NOT creating subscriptions"))
  (println "\nTo verify in production:")
  (println "1. Run a temporal query in rabbit-demo")
  (println "2. Check server logs for 'TEMPORAL subscription' messages")
  (println "3. Run the same query again - should see diff messages")
  (println "4. Check that data changes don't trigger temporal re-execution"))

;; Run the tests
(run-tests)