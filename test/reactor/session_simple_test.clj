(ns reactor.session-simple-test
  (:require [clojure.test :refer :all]
            [reactor.session_simple :as session]
            [reactor.xtdb-store :as xts]))

(defn test-fixture
  "Clean up state between tests"
  [f]
  ;; Clear in-memory state before test
  (reset! session/sessions {})
  (reset! session/session-history-index {})
  (reset! session/event-handlers {})
  
  ;; Clean up test data from XTDB before test
  (when-let [node @session/default-node]
    (doseq [table ["test_sessions" "app1_sessions" "app2_sessions" "flatten_sessions"]]
      (try
        ;; Delete all test data from tables
        (xts/execute-sql node (str "DELETE FROM " table))
        (catch Exception _ nil))))
  
  ;; Run test
  (f)
  
  ;; Clean up after test
  (reset! session/sessions {})
  (reset! session/session-history-index {})
  (reset! session/event-handlers {}))

(use-fixtures :each test-fixture)

(deftest session-basic-operations
  (testing "Session creation and state management"
    ;; Initialize with test table
    (session/init! :test "test_sessions")
    
    ;; Use unique session ID for this test
    (let [sess (session/create-session! "test-basic-ops" {})]
      ;; Test initial state
      (is (= {} @sess))
      
      ;; Test state update
      (reset! sess {:count 1})
      (is (= {:count 1} @sess))
      
      ;; Test swap operations
      (swap! sess update :count inc)
      (is (= {:count 2} @sess)))))

(deftest session-event-handlers
  (testing "Event handler registration and dispatch"
    (session/init! :test "test_sessions")
    
    ;; Register handler
    (session/reg-event-db :increment
                         (fn [db [amount]]
                           (update db :count (fnil + 0) amount)))
    
    ;; Use unique session ID and create fresh
    (let [sess (session/create-session! "test-handlers-unique" {})]
      (session/dispatch "test-handlers-unique" [:increment 5])
      (is (= {:count 5} @sess))
      
      (session/dispatch "test-handlers-unique" [:increment 3])
      (is (= {:count 8} @sess)))))

(deftest session-persistence
  (testing "Session persistence across restarts"
    (session/init! :test "test_sessions")
    
    ;; Use unique session ID
    (let [test-id (str "persist-" (System/currentTimeMillis))
          sess1 (session/create-session! test-id {})]
      (reset! sess1 {:data "important"})
      (Thread/sleep 100) ;; Give time to persist
      
      ;; Clear in-memory cache
      (reset! session/sessions {})
      
      ;; Get session again - should load from XTDB
      (let [sess2 (session/get-session test-id)]
        (is (= {:data "important"} @sess2))))))

(deftest session-time-travel
  (testing "Time travel functionality"
    (session/init! :test "test_sessions")
    
    (session/reg-event-db :set-value
                         (fn [db [value]]
                           (assoc db :value value)))
    
    ;; Use unique session ID
    (let [test-id (str "time-travel-" (System/currentTimeMillis))
          sess (session/create-session! test-id {})]
      ;; Create history
      (session/dispatch test-id [:set-value "first"])
      (Thread/sleep 100)
      (session/dispatch test-id [:set-value "second"])
      (Thread/sleep 100)
      (session/dispatch test-id [:set-value "third"])
      (Thread/sleep 100)
      
      (is (= {:value "third"} @sess))
      
      ;; Test undo
      (session/undo! test-id)
      (is (= {:value "second"} @sess))
      
      ;; Test redo
      (session/redo! test-id)
      (is (= {:value "third"} @sess)))))

(deftest table-separation
  (testing "Different apps use different tables"
    ;; Use unique session IDs
    (let [sess1-id (str "app1-sess-" (System/currentTimeMillis))
          sess2-id (str "app2-sess-" (System/currentTimeMillis))]
      ;; App 1
      (session/init! :app1 "app1_sessions")
      (session/reg-event-db :set-app1 (fn [db [v]] (assoc db :app1 v)))
      (session/create-session! sess1-id {})
      (session/dispatch sess1-id [:set-app1 "data1"])
      (Thread/sleep 100)
      
      ;; App 2
      (session/init! :app2 "app2_sessions")
      (session/reg-event-db :set-app2 (fn [db [v]] (assoc db :app2 v)))
      (session/create-session! sess2-id {})
      (session/dispatch sess2-id [:set-app2 "data2"])
      (Thread/sleep 100)
      
      ;; Verify data is in correct tables
      (when-let [node @session/default-node]
        (let [app1-data (xts/execute-sql node (str "SELECT * FROM app1_sessions WHERE session_id = '" sess1-id "'"))
              app2-data (xts/execute-sql node (str "SELECT * FROM app2_sessions WHERE session_id = '" sess2-id "'"))]
          (is (seq (:results app1-data)))
          (is (seq (:results app2-data))))))))

(deftest field-flattening
  (testing "Automatic field flattening for SQL queries"
    (session/init! :flatten-test "flatten_sessions")
    
    (session/reg-event-db :set-data (fn [db [data]] data))
    
    ;; Use unique session ID
    (let [test-id (str "flatten-" (System/currentTimeMillis))]
      (session/create-session! test-id {})
      (session/dispatch test-id [:set-data {:user "alice"
                                           :count 42
                                           :active true
                                           :nested {:ignored "value"}}])
      (Thread/sleep 200)
      
      ;; Query flattened fields
      (when-let [node @session/default-node]
        (let [result (xts/execute-sql node 
                                      (str "SELECT app_user, app_count, app_active 
                                            FROM flatten_sessions 
                                            WHERE session_id = '" test-id "'"))]
          (when-let [row (first (:results result))]
            (is (= "alice" (:app_user row)))
            (is (= 42 (:app_count row)))
            (is (= true (:app_active row)))))))))