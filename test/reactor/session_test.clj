(ns reactor.session-test
  "Tests for session-scoped state management"
  (:require [clojure.test :refer :all]
            [reactor.session :as session]))

(deftest test-session-creation
  (testing "Sessions can be created and accessed"
    (session/init!)
    (let [session-id "test-session-1"
          s (session/create-session! session-id {:counter 0})]
      (is (not (nil? s)))
      (is (= {:counter 0} @s))
      (session/destroy-session! session-id))))

(deftest test-session-isolation
  (testing "Sessions are isolated from each other"
    (session/init!)
    (let [s1 (session/create-session! "user-1" {:todos []})
          s2 (session/create-session! "user-2" {:todos []})]
      
      ;; Modify session 1
      (swap! s1 update :todos conj {:text "User 1 todo"})
      
      ;; Verify session 2 is unchanged
      (is (= [] (:todos @s2)))
      (is (= [{:text "User 1 todo"}] (:todos @s1)))
      
      ;; Modify session 2
      (swap! s2 update :todos conj {:text "User 2 todo"})
      
      ;; Verify both sessions have their own data
      (is (= [{:text "User 1 todo"}] (:todos @s1)))
      (is (= [{:text "User 2 todo"}] (:todos @s2)))
      
      (session/destroy-session! "user-1")
      (session/destroy-session! "user-2"))))

(deftest test-session-atom-interface
  (testing "Sessions work like atoms"
    (session/init!)
    (let [s (session/create-session! "atom-test" {:value 0})]
      
      ;; Deref
      (is (= {:value 0} @s))
      
      ;; Reset
      (reset! s {:value 10})
      (is (= {:value 10} @s))
      
      ;; Swap
      (swap! s update :value inc)
      (is (= {:value 11} @s))
      
      ;; Swap with args
      (swap! s assoc :name "test")
      (is (= {:value 11 :name "test"} @s))
      
      (session/destroy-session! "atom-test"))))

(deftest test-session-watches
  (testing "Sessions support watches"
    (session/init!)
    (let [s (session/create-session! "watch-test" {:value 0})
          watch-calls (atom [])]
      
      ;; Add watch
      (add-watch s :test-watch
        (fn [key ref old new]
          (swap! watch-calls conj {:key key :old old :new new})))
      
      ;; Trigger watch
      (swap! s assoc :value 1)
      
      ;; Verify watch was called
      (Thread/sleep 100) ; Give watch time to fire
      (is (= 1 (count @watch-calls)))
      (is (= {:key :test-watch
              :old {:value 0}
              :new {:value 1}}
             (first @watch-calls)))
      
      ;; Remove watch
      (remove-watch s :test-watch)
      (swap! s assoc :value 2)
      
      ;; Verify watch not called again
      (Thread/sleep 100)
      (is (= 1 (count @watch-calls)))
      
      (session/destroy-session! "watch-test"))))

(deftest test-event-handlers
  (testing "Event handlers work with sessions"
    (session/init!)
    
    ;; Register handler
    (session/reg-event-db :test-increment
      (fn [db [amount]]
        (update db :counter (fnil + 0) amount)))
    
    (let [s (session/create-session! "event-test" {:counter 0})]
      
      ;; Dispatch event
      (session/dispatch "event-test" [:test-increment 5])
      (Thread/sleep 100) ; Give event time to process
      
      (is (= {:counter 5} @s))
      
      ;; Dispatch again
      (session/dispatch "event-test" [:test-increment 3])
      (Thread/sleep 100)
      
      (is (= {:counter 8} @s))
      
      (session/destroy-session! "event-test"))))

(deftest test-session-time-travel
  (testing "Sessions support time travel"
    (session/init!)
    (let [s (session/create-session! "time-test" {:value 0})]
      
      ;; Make some changes
      (swap! s assoc :value 1)
      (Thread/sleep 10)
      (swap! s assoc :value 2)
      (Thread/sleep 10)
      (swap! s assoc :value 3)
      (Thread/sleep 10)
      
      ;; Get history
      (let [history (session/get-history s)]
        (is (>= (count history) 3)))
      
      ;; Undo
      (session/undo! "time-test")
      (Thread/sleep 100)
      
      ;; Should be back to value 2
      (is (= 2 (:value @s)))
      
      (session/destroy-session! "time-test"))))

(deftest test-global-state
  (testing "Global state works across sessions"
    (session/init!)
    
    ;; Set global value
    (session/set-global! [:app-version] "1.0.0")
    
    ;; Access from different sessions
    (let [s1 (session/create-session! "global-1" {})
          s2 (session/create-session! "global-2" {})]
      
      (is (= "1.0.0" (session/get-global :app-version)))
      
      ;; Update global
      (session/set-global! [:app-version] "1.0.1")
      
      (is (= "1.0.1" (session/get-global :app-version)))
      
      (session/destroy-session! "global-1")
      (session/destroy-session! "global-2"))))

(deftest test-session-persistence
  (testing "Sessions persist across get-session calls"
    (session/init!)
    
    ;; Create session with data
    (let [s1 (session/create-session! "persist-test" {:value 42})]
      (is (= {:value 42} @s1)))
    
    ;; Get same session again
    (let [s2 (session/get-session "persist-test")]
      (is (= {:value 42} @s2)))
    
    ;; Modify through second reference
    (let [s2 (session/get-session "persist-test")]
      (swap! s2 assoc :value 99))
    
    ;; Verify change visible through first reference
    (let [s1 (session/get-session "persist-test")]
      (is (= {:value 99} @s1)))
    
    (session/destroy-session! "persist-test")))