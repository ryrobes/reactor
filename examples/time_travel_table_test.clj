(ns reactor.time-travel-table-test
  (:require [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [clojure.pprint :as pp]))

(defn -main []
  (println "=== Time Travel Table Test ===\n")
  
  ;; Initialize TODO app with its specific table
  (println "1. Initializing TODO app with todo_sessions table...")
  (session/init! :todo "todo_sessions")
  (session/reg-event-db :add-todo (fn [db [todo]] 
                                    (assoc-in db [:todos (:id todo)] todo)))
  (session/reg-event-db :toggle-todo (fn [db [id]]
                                       (update-in db [:todos id :completed] not)))
  
  ;; Create a session and add some todos
  (println "\n2. Creating session and adding todos...")
  (def sess (session/get-session "test-time-travel"))
  
  ;; Add first todo
  (session/dispatch "test-time-travel" [:add-todo {:id "t1" :text "First todo" :completed false}])
  (Thread/sleep 100)
  
  ;; Add second todo
  (session/dispatch "test-time-travel" [:add-todo {:id "t2" :text "Second todo" :completed false}])
  (Thread/sleep 100)
  
  ;; Toggle first todo
  (session/dispatch "test-time-travel" [:toggle-todo "t1"])
  (Thread/sleep 100)
  
  ;; Check history
  (println "\n3. Checking history info:")
  (let [history-info (session/get-history-info "test-time-travel")]
    (println "Total states:" (:total-states history-info))
    (println "Current index:" (:current-index history-info))
    (println "Can undo:" (:can-undo history-info))
    (println "Can redo:" (:can-redo history-info)))
  
  ;; Current state
  (println "\n4. Current state:")
  (pp/pprint @sess)
  
  ;; Undo once
  (println "\n5. After undo (should have t1 not completed):")
  (session/undo! "test-time-travel")
  (pp/pprint @sess)
  
  ;; Undo again
  (println "\n6. After second undo (should have only t1):")
  (session/undo! "test-time-travel")
  (pp/pprint @sess)
  
  ;; Redo
  (println "\n7. After redo (should have t1 and t2):")
  (session/redo! "test-time-travel")
  (pp/pprint @sess)
  
  ;; Verify we're using the right table
  (println "\n8. Verifying data is in todo_sessions table:")
  (when-let [node @session/default-node]
    (let [result (xts/execute-sql node "SELECT session_id, app_name FROM todo_sessions WHERE session_id = 'test-time-travel'")]
      (if (:error result)
        (println "Error:" (:error result))
        (pp/pprint (:results result))))
    
    ;; Show that sessions table doesn't have this data
    (println "\n9. Confirming 'sessions' table doesn't have this data:")
    (let [result (xts/execute-sql node "SELECT session_id FROM sessions WHERE session_id = 'test-time-travel'")]
      (if (:error result)
        (println "Error:" (:error result))
        (if (empty? (:results result))
          (println "Correct: No data found in 'sessions' table")
          (println "ERROR: Found data in wrong table!" (:results result))))))
  
  (println "\n=== Test Completed ===")
  (System/exit 0))

(-main)