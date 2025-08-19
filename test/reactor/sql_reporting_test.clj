(ns reactor.sql-reporting-test
  (:require [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [clojure.pprint :as pp]))

(defn -main []
  (println "=== SQL Reporting Test ===\n")
  
  ;; Initialize TODO app
  (println "1. Initializing TODO app...")
  (session/init! :todo)
  (session/reg-event-db :set-todo-data (fn [db [data]] data))
  
  ;; Create multiple TODO sessions with different data
  (println "2. Creating TODO sessions with sample data...")
  
  ;; Session 1: Active user with many todos
  (session/dispatch "alice" [:set-todo-data 
    {:todos {:t1 {:id :t1 :text "Review PR" :completed true}
             :t2 {:id :t2 :text "Fix bug #123" :completed false}
             :t3 {:id :t3 :text "Write tests" :completed false}
             :t4 {:id :t4 :text "Deploy to staging" :completed false}}
     :filter :active
     :user "alice"
     :count 4
     :active true}])
  
  ;; Session 2: Less active user
  (session/dispatch "bob" [:set-todo-data 
    {:todos {:t1 {:id :t1 :text "Read docs" :completed true}
             :t2 {:id :t2 :text "Team meeting" :completed true}}
     :filter :completed
     :user "bob"
     :count 2
     :active false}])
  
  ;; Session 3: New user
  (session/dispatch "charlie" [:set-todo-data 
    {:todos {:t1 {:id :t1 :text "Setup dev env" :completed false}}
     :filter :all
     :user "charlie"
     :count 1
     :active true}])
  
  ;; Now reinitialize as Rabbit app
  (println "\n3. Switching to Rabbit app...")
  (reset! session/app-name :rabbit)
  (session/reg-event-db :set-rabbit-data (fn [db [data]] data))
  
  ;; Create Rabbit sessions
  (session/dispatch "designer" [:set-rabbit-data 
    {:canvas {:blocks {:b1 {:id :b1 :type "query" :x 100 :y 100}
                       :b2 {:id :b2 :type "chart" :x 300 :y 100}
                       :b3 {:id :b3 :type "table" :x 500 :y 100}}}
     :user "designer"
     :active true}])
  
  (Thread/sleep 1000)
  
  ;; Run reporting queries
  (when-let [node @session/default-node]
    (println "\n=== SQL Reporting Queries ===\n")
    
    ;; Query 1: Summary by app
    (println "1. Sessions by application:")
    (let [result (xts/execute-sql node 
                   "SELECT app_name, COUNT(*) as session_count, 
                           SUM(CASE WHEN app_active = true THEN 1 ELSE 0 END) as active_sessions
                    FROM sessions 
                    WHERE app_name IS NOT NULL
                    GROUP BY app_name")]
      (if (:error result)
        (println "Error:" (:error result))
        (pp/pprint (:results result))))
    
    ;; Query 2: TODO app metrics
    (println "\n2. TODO app user activity:")
    (let [result (xts/execute-sql node 
                   "SELECT session_id as user, app_todos_count as total_todos, 
                           app_filter as current_filter, app_active as is_active
                    FROM sessions 
                    WHERE app_name = 'todo'
                    ORDER BY app_todos_count DESC")]
      (if (:error result)
        (println "Error:" (:error result))
        (pp/pprint (:results result))))
    
    ;; Query 3: Active sessions across all apps
    (println "\n3. All active sessions:")
    (let [result (xts/execute-sql node 
                   "SELECT session_id, app_name, app_user, 
                           COALESCE(app_todos_count, 0) as todos,
                           COALESCE(app_blocks_count, 0) as blocks
                    FROM sessions 
                    WHERE app_active = true")]
      (if (:error result)
        (println "Error:" (:error result))
        (pp/pprint (:results result))))
    
    ;; Query 4: Cross-app user analysis
    (println "\n4. Users with specific names across apps:")
    (let [result (xts/execute-sql node 
                   "SELECT app_name, app_user, session_id
                    FROM sessions 
                    WHERE app_user IS NOT NULL
                    ORDER BY app_user, app_name")]
      (if (:error result)
        (println "Error:" (:error result))
        (pp/pprint (:results result))))
    
    ;; Query 5: Time-based analysis
    (println "\n5. Recent session activity (created in last hour):")
    (let [result (xts/execute-sql node 
                   "SELECT session_id, app_name, created_at
                    FROM sessions 
                    ORDER BY created_at DESC
                    LIMIT 5")]
      (if (:error result)
        (println "Error:" (:error result))
        (pp/pprint (:results result)))))
  
  (println "\n=== Test Completed ===")
  (System/exit 0))

(-main)