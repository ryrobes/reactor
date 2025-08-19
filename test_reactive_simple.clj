#!/usr/bin/env clojure

(require '[reactor.session_simple :as session])
(require '[reactor.xtdb-store :as xts])

(println "=== Testing Simple Reactive Bridge ===\n")

;; Initialize
(session/init! :test "test_reactive")

;; Create a simple subscription tracking system
(def subscriptions (atom {}))
(def results (atom {}))

(defn register-sub [id sql callback]
  (swap! subscriptions assoc id {:sql sql :callback callback})
  ;; Execute initially
  (when-let [node @session/default-node]
    (let [result (xts/execute-sql node sql)]
      (callback id result))))

(defn notify-change [table]
  (println "Table changed:" table)
  ;; Re-execute all subscriptions that use this table
  (doseq [[id {:keys [sql callback]}] @subscriptions]
    (when (.contains sql table)
      (println "Re-executing subscription:" id)
      (when-let [node @session/default-node]
        (let [result (xts/execute-sql node sql)]
          (callback id result))))))

;; Test callback
(defn track-results [id result]
  (swap! results update id (fnil conj []) result)
  (println (str "  Subscription " id " got result: " 
               (or (-> result :results first) "empty"))))

;; Set up test
(println "1. Setting up subscriptions...")
(register-sub "count-sub" 
             "SELECT COUNT(*) as total FROM test_items"
             track-results)

(register-sub "list-sub"
             "SELECT * FROM test_items ORDER BY _id"
             track-results)

;; Insert initial data
(println "\n2. Inserting initial data...")
(when-let [node @session/default-node]
  (xts/execute-sql node 
    "INSERT INTO test_items (_id, name, value) VALUES ('item-1', 'First', 100)"))

;; Manually trigger update
(println "\n3. Triggering reactive update...")
(notify-change "test_items")

;; Check results
(println "\n4. Results after first update:")
(println "  Count subscription received" (count (get @results "count-sub")) "updates")
(println "  List subscription received" (count (get @results "list-sub")) "updates")

;; Insert more data
(println "\n5. Inserting more data...")
(when-let [node @session/default-node]
  (xts/execute-sql node 
    "INSERT INTO test_items (_id, name, value) VALUES ('item-2', 'Second', 200)"))

;; Trigger again
(notify-change "test_items")

(println "\n6. Final results:")
(println "  Count:" (-> @results (get "count-sub") last :results first :total))
(println "  Items:" (count (-> @results (get "list-sub") last :results)))

;; Clean up
(when-let [node @session/default-node]
  (xts/execute-sql node "DELETE FROM test_items"))

(println "\n=== Test Complete ===")
(System/exit 0)