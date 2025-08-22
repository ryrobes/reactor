(ns test-time-travel
  "Test script for session time-travel functionality"
  (:require [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [clojure.pprint :refer [pprint]]))

(defn test-session-time-travel []
  (println "\n=== Testing Session Time-Travel ===\n")
  
  ;; Initialize the system
  (session/init! :test-app "test_sessions")
  (println "✓ Initialized with app: test-app, table: test_sessions")
  
  (let [session-id "time-travel-test"
        s (session/get-session session-id)]
    
    ;; Create some state changes with timestamps
    (println "\nCreating session history...")
    
    ;; State 1
    (Thread/sleep 1000)
    (session/set-state! s {:counter 1 :message "Initial state"})
    (let [timestamp-1 (java.time.Instant/now)]
      (println (str "  State 1 at " timestamp-1 ": {:counter 1, :message \"Initial state\"}"))
      
      ;; State 2
      (Thread/sleep 1000)
      (session/set-state! s {:counter 2 :message "Updated state"})
      (let [timestamp-2 (java.time.Instant/now)]
        (println (str "  State 2 at " timestamp-2 ": {:counter 2, :message \"Updated state\"}"))
        
        ;; State 3
        (Thread/sleep 1000)
        (session/set-state! s {:counter 3 :message "Final state" :todos ["Buy milk" "Walk dog"]})
        (let [timestamp-3 (java.time.Instant/now)]
          (println (str "  State 3 at " timestamp-3 ": {:counter 3, :message \"Final state\", :todos [...]}\n"))
          
          ;; Now query the session at different timestamps
          (println "Testing temporal queries:\n")
          
          ;; Current state (no AS OF)
          (let [current-query "SELECT * FROM test_sessions WHERE session_id = ?"
                current-result (xts/execute-sql @session/default-node current-query session-id)
                current-row (first (:results current-result))]
            (println "Current state (no timestamp):")
            (println "  Counter:" (get current-row :app_counter))
            (println "  Message:" (get current-row :app_message))
            (println "  State:" (when-let [s (:state current-row)]
                                 (pr-str (take 50 (str s)))))
            (println))
          
          ;; State at timestamp 1
          (let [temporal-query-1 (str "SELECT * FROM test_sessions WHERE session_id = ? "
                                      "AS OF SYSTEM TIME '" timestamp-1 "'")
                result-1 (xts/execute-sql @session/default-node temporal-query-1 session-id)
                row-1 (first (:results result-1))]
            (println (str "State at " timestamp-1 " (should be state 1):"))
            (println "  Counter:" (get row-1 :app_counter))
            (println "  Message:" (get row-1 :app_message))
            (println))
          
          ;; State at timestamp 2
          (let [temporal-query-2 (str "SELECT * FROM test_sessions WHERE session_id = ? "
                                      "AS OF SYSTEM TIME '" timestamp-2 "'")
                result-2 (xts/execute-sql @session/default-node temporal-query-2 session-id)
                row-2 (first (:results result-2))]
            (println (str "State at " timestamp-2 " (should be state 2):"))
            (println "  Counter:" (get row-2 :app_counter))
            (println "  Message:" (get row-2 :app_message))
            (println))
          
          ;; Generate URLs for testing
          (println "\n=== URLs for Testing ===\n")
          (println "To test in browser, use these URLs:")
          (println (str "  1. Normal load: http://localhost:5000"))
          (println (str "  2. Session at time 1: http://localhost:5000?session_id=" session-id "&at=" (java.net.URLEncoder/encode (str timestamp-1) "UTF-8")))
          (println (str "  3. Session at time 2: http://localhost:5000?session_id=" session-id "&at=" (java.net.URLEncoder/encode (str timestamp-2) "UTF-8")))
          (println (str "  4. Session at time 3: http://localhost:5000?session_id=" session-id "&at=" (java.net.URLEncoder/encode (str timestamp-3) "UTF-8")))
          
          ;; Also test snapshot creation for comparison
          (println "\nCreating a snapshot for comparison...")
          (let [snapshot-id "test-snapshot-123"
                snapshot-result (xts/execute-sql @session/default-node
                                  "INSERT INTO reactor_snapshots 
                                   (_id, snapshot_id, app_name, session_id, state, description, created_at)
                                   VALUES (?, ?, ?, ?, ?, ?, ?)"
                                  snapshot-id snapshot-id "test-app" session-id 
                                  (pr-str {:counter 99 :message "Snapshot state"})
                                  "Test snapshot" (java.time.Instant/now))]
            (println (str "  Created snapshot: " snapshot-id))
            (println (str "  5. Snapshot load: http://localhost:5000?snapshot=" snapshot-id)))
          
          (println "\n✓ Test complete!")
          (println "\nPriority order:")
          (println "  1. ?snapshot=xxx (highest priority)")
          (println "  2. ?session_id=xxx&at=yyy") 
          (println "  3. Normal session load (default)")))))))

;; Run the test
(test-session-time-travel)
