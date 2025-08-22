(ns reactor.visual-testing-test
  "Visual regression tests for reactor apps
   
   REQUIREMENTS: Server and UI must be running before tests:
   
   For rabbit app:
     Terminal 1: lein run -m examples.rabbit-demo.server/-main
     Terminal 2: shadow-cljs watch rabbit
   
   For todo app:
     Terminal 1: lein run -m examples.todo-app.server/-main
     Terminal 2: shadow-cljs watch todo
   
   Then run tests:
     lein test :only reactor.visual-testing-test"
  (:require [clojure.test :refer [deftest is testing]]
            [reactor.visual-testing :as vt]
            [reactor.xtdb-store :as xts]))

;; Initialize tables on load
(vt/create-visual-test-tables!)

(deftest test-rabbit-app-specific-snapshot
  "Visual test for specific snapshot snapshot-1755848155579"
  (testing "Snapshot renders consistently"
    (let [snapshot-id "snapshot-1755848155579"
          
          ;; Run visual test (will create baseline on first run, compare on subsequent runs)
          result (vt/run-visual-test! "rabbit" "snapshot-1755848155579" snapshot-id
                                     :base-url "http://localhost:8081/rabbit.html"
                                     :threshold 95.0)]
      
      (cond
        (= "BASELINE_CREATED" (:status result))
        (do 
          (println "First run - baseline created")
          (is true "Baseline created successfully"))
        
        (= "PASS" (:status result))
        (is true "Visual test passed")
        
        (= "FAIL" (:status result))
        (is false (str "Visual differences detected:\n"
                      (for [r (:results result)
                            :when (= "FAIL" (:status r))]
                        {:step (:step r)
                         :similarity (:similarity r)
                         :dom-changes (take 3 (:dom-differences r))})))
        
        :else
        (is false (str "Test error: " (:message result)))))))

(deftest test-rabbit-app-homepage
  "Visual test for rabbit app homepage"
  (testing "Homepage renders correctly"
    ;; First, create a snapshot of current state
    (let [node (xts/start-xtdb-node)
          snapshot-id (str "test-snapshot-" (System/currentTimeMillis))
          
          ;; Save current app-db as snapshot
          _ (xts/execute-sql node
              "INSERT INTO reactor_snapshots (_id, app_db, description, created_at)
               VALUES (?, ?, ?, ?)"
              snapshot-id
              "{:test-data \"visual test baseline\"}"
              "Visual test baseline"
              (java.time.Instant/now))
          
          ;; Run visual test (auto baseline on first run)
          result (vt/run-visual-test! "rabbit" "homepage" snapshot-id
                                     :base-url "http://localhost:8081/rabbit.html"
                                     :threshold 95.0)]
      
      (cond
        (= "BASELINE_CREATED" (:status result))
        (is true "Baseline created for first run")
        
        (= "PASS" (:status result))
        (is true "Visual test passed")
        
        :else
        (is false (str "Visual regression detected:\n"
                      (pr-str (:results result))))))))

(deftest test-todo-app-with-items
  "Visual test for todo app with items"
  (testing "Todo list displays correctly"
    (let [node (xts/start-xtdb-node)
          snapshot-id (str "todo-snapshot-" (System/currentTimeMillis))
          
          ;; Create snapshot with todo items
          _ (xts/execute-sql node
              "INSERT INTO reactor_snapshots (_id, app_db, description, created_at)
               VALUES (?, ?, ?, ?)"
              snapshot-id
              "{:todos [{:id 1 :text \"Test item 1\" :done false}
                        {:id 2 :text \"Test item 2\" :done true}]}"
              "Todo app with test items"
              (java.time.Instant/now))
          
          ;; Run test (using correct todo URL)
          result (vt/run-visual-test! "todo" "with-items" snapshot-id
                                     :base-url "http://localhost:8083/todo.html"
                                     :threshold 98.0)]
      
      (is (= "PASS" (:status result))
          (str "Visual test failed. Differences found:\n"
               (when-let [failures (filter #(= "FAIL" (:status %)) (:results result))]
                 (for [f failures]
                   {:step (:step f)
                    :similarity (:similarity f)
                    :dom-changes (take 5 (:dom-differences f))})))))))

;; Helper function to update baselines
(defn update-baseline!
  "Helper to update visual baselines when UI intentionally changes"
  [app-name test-name]
  (println "Updating baseline for" app-name "/" test-name)
  (let [node (xts/start-xtdb-node)
        ;; Delete old baseline
        _ (xts/execute-sql node
            "DELETE FROM reactor_visual_baselines 
             WHERE app_name = ? AND test_name = ?"
            app-name test-name)
        
        ;; Create new baseline
        snapshot-id (str "baseline-" (System/currentTimeMillis))]
    
    ;; Save snapshot
    (xts/execute-sql node
      "INSERT INTO reactor_snapshots (_id, app_db, description, created_at)
       VALUES (?, ?, ?, ?)"
      snapshot-id
      "{:baseline true}"
      (str "Baseline for " app-name "/" test-name)
      (java.time.Instant/now))
    
    ;; Capture new baseline
    (vt/capture-baseline! app-name test-name snapshot-id)
    
    (println "Baseline updated successfully")))

;; Run with: lein test :only reactor.visual-testing-test/test-rabbit-app-homepage