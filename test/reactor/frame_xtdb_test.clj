(ns reactor.frame-xtdb-test
  (:require [clojure.test :refer :all]
            [reactor.frame-xtdb :as rfx]
            [reactor.xtdb-store :as xts]))

(deftest test-basic-frame-xtdb
  (testing "Basic XTDB-backed frame app"
    (let [app (rfx/create-xtdb-frame-app
               {:counter 0
                :name "test"}
               :app-id "test-app")]
      
      (try
        ;; Test initial state
        (let [db @(rfx/get-app-db app)]
          (is (= 0 (:counter db)))
          (is (= "test" (:name db))))
        
        ;; Register and test event handler
        (rfx/reg-event-db app :increment
          (fn [db _]
            (update db :counter inc)))
        
        ;; Dispatch event
        (rfx/dispatch app [:increment])
        
        ;; Check updated state
        (Thread/sleep 100) ; Wait for XTDB
        (let [db @(rfx/get-app-db app)]
          (is (= 1 (:counter db))))
        
        ;; Test subscription
        (rfx/reg-sub app :counter
          (fn [db _]
            (:counter db)))
        
        (let [counter-sub (rfx/subscribe app [:counter])]
          (is (= 1 @counter-sub)))
        
        (finally
          (rfx/stop-app! app))))))

(deftest test-time-travel
  (testing "Time travel with XTDB"
    (let [app (rfx/create-xtdb-frame-app
               {:todos []}
               :app-id "time-travel-test")]
      
      (try
        ;; Register event
        (rfx/reg-event-db app :add-todo
          (fn [db [text]]
            (update db :todos conj text)))
        
        ;; Add some todos
        (rfx/dispatch app [:add-todo "First"])
        (Thread/sleep 100)
        (rfx/dispatch app [:add-todo "Second"])
        (Thread/sleep 100)
        (rfx/dispatch app [:add-todo "Third"])
        (Thread/sleep 100)
        
        ;; Check current state
        (let [db @(rfx/get-app-db app)]
          (is (= ["First" "Second" "Third"] (:todos db))))
        
        ;; Get history
        (let [history (rfx/get-history app :limit 10)]
          (is (>= (count history) 3)))
        
        ;; Undo
        (rfx/undo! app)
        (Thread/sleep 100)
        
        ;; Check state after undo
        (let [db @(rfx/get-app-db app)]
          (is (= ["First" "Second"] (:todos db))))
        
        (finally
          (rfx/stop-app! app))))))