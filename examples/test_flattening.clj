(ns reactor.test-flattening
  (:require [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]
            [clojure.pprint :as pp]))

(defn -main []
  ;; Initialize with app name
  (println "Initializing session system with app name 'test'...")
  (session/init! :test)

  ;; Register a simple handler
  (session/reg-event-db :set-data (fn [db [data]] data))

  ;; Create a session and set some data
  (println "Creating session and setting data...")
  (def sess (session/get-session "test-flatten"))

  ;; Set some app state with various fields
  (session/dispatch "test-flatten" [:set-data 
    {:todos {:t1 {:id :t1 :text "Buy milk" :completed false}
             :t2 {:id :t2 :text "Walk dog" :completed true}}
     :filter :active
     :user "john"
     :count 42
     :active true}])

  (println "State set successfully!")
  
  ;; Give XTDB time to persist
  (Thread/sleep 1000)

  ;; Now query the sessions table to see the flattened columns
  (when-let [node @session/default-node]
    (println "\nQuerying sessions table to see flattened columns:")
    (let [result (xts/execute-sql node "SELECT * FROM sessions WHERE session_id = 'test-flatten' LIMIT 1")]
      (if (:error result)
        (println "Error:" (:error result))
        (do
          (println "Column names in sessions table:")
          (when-let [row (first (:results result))]
            (doseq [col (sort (keys row))]
              (println (str "  - " (name col) ": " 
                           (let [v (get row col)]
                             (cond
                               (nil? v) "null"
                               (string? v) (if (> (count v) 50)
                                            (str (subs v 0 50) "...")
                                            v)
                               :else (str v))))))))))
    
    ;; Also test querying by flattened columns
    (println "\nTesting SQL query on flattened columns:")
    (let [result (xts/execute-sql node "SELECT session_id, app_name, app_filter, app_user, app_count, app_active, app_todos_count FROM sessions WHERE session_id = 'test-flatten'")]
      (if (:error result)
        (println "Error:" (:error result))
        (do
          (println "Results:")
          (pp/pprint (:results result))))))

  (println "\nTest completed!")
  (System/exit 0))

(-main)