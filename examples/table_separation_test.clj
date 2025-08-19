(ns reactor.table-separation-test
  (:require [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [clojure.pprint :as pp]))

(defn -main []
  (println "=== Table Separation Test ===\n")
  
  ;; Initialize TODO app with custom table
  (println "1. Initializing TODO app with todo_sessions table...")
  (session/init! :todo "todo_sessions")
  (session/reg-event-db :set-todo-data (fn [db [data]] data))
  
  ;; Create a TODO session
  (session/dispatch "alice-todo" [:set-todo-data 
    {:todos {:t1 {:id :t1 :text "TODO task" :completed false}}
     :user "alice"}])
  
  (Thread/sleep 500)
  
  ;; Check what table it went to
  (when-let [node @session/default-node]
    (println "\n2. Checking todo_sessions table:")
    (let [result (xts/execute-sql node "SELECT session_id, app_name FROM todo_sessions")]
      (if (:error result)
        (println "Error:" (:error result))
        (pp/pprint (:results result)))))
  
  ;; Now switch to Rabbit app with its own table
  (println "\n3. Switching to Rabbit app with rabbit_sessions table...")
  (session/init! :rabbit "rabbit_sessions")
  (session/reg-event-db :set-rabbit-data (fn [db [data]] data))
  
  ;; Create a Rabbit session
  (session/dispatch "designer-rabbit" [:set-rabbit-data 
    {:canvas {:blocks {:b1 {:id :b1 :type "query"}}}
     :user "designer"}])
  
  (Thread/sleep 500)
  
  ;; Check rabbit table
  (when-let [node @session/default-node]
    (println "\n4. Checking rabbit_sessions table:")
    (let [result (xts/execute-sql node "SELECT session_id, app_name FROM rabbit_sessions")]
      (if (:error result)
        (println "Error:" (:error result))
        (pp/pprint (:results result))))
    
    ;; Show that data is in separate tables
    (println "\n5. Verifying separation - todo_sessions still has TODO data:")
    (let [result (xts/execute-sql node "SELECT session_id, app_name FROM todo_sessions")]
      (if (:error result)
        (println "Error:" (:error result))
        (pp/pprint (:results result))))
    
    ;; List all tables to show they exist
    (println "\n6. All tables in the database:")
    (let [tables (xts/list-tables node)]
      (println "Public tables:" (:public tables))))
  
  (println "\n=== Test Completed ===")
  (System/exit 0))

(-main)