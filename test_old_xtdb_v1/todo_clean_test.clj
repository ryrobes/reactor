(ns reactor.todo-clean-test
  "Test the clean XTDB TODO server"
  (:require [clojure.test :refer :all]
            [examples.todo-app.server-xtdb-clean :as server]
            [org.httpkit.client :as http]
            [clojure.edn :as edn]))

(deftest test-clean-server
  (testing "Clean server starts fresh each time"
    ;; Start first server
    (server/start-server 8888)
    (server/seed-todos!)
    (Thread/sleep 200)
    
    (try
      ;; Check initial state has 3 todos
      (let [resp @(http/get "http://localhost:8888/api/state" {:as :text})
            state (edn/read-string (:body resp))]
        (is (= 3 (count (:todos state))))
        (println "First server todos:" (map :text (vals (:todos state)))))
      
      ;; Stop and restart
      (server/stop-server)
      (Thread/sleep 200)
      
      (server/start-server 8888)
      (Thread/sleep 200)
      
      ;; Should have empty todos now
      (let [resp @(http/get "http://localhost:8888/api/state" {:as :text})
            state (edn/read-string (:body resp))]
        (is (= 0 (count (:todos state))))
        (println "After restart todos:" (:todos state)))
      
      ;; Add one todo
      @(http/post "http://localhost:8888/api/dispatch"
                 {:body (pr-str [:add-todo "New todo"])
                  :headers {"Content-Type" "application/edn"}
                  :as :text})
      
      (Thread/sleep 200)
      
      ;; Should have exactly 1 todo
      (let [resp @(http/get "http://localhost:8888/api/state" {:as :text})
            state (edn/read-string (:body resp))]
        (is (= 1 (count (:todos state))))
        (is (= "New todo" (:text (first (vals (:todos state)))))))
      
      (finally
        (server/stop-server)))))

(deftest test-time-travel-clean
  (testing "Time travel works correctly"
    (server/start-server 8889)
    
    (try
      (Thread/sleep 200)
      
      ;; Add todos
      (doseq [text ["First" "Second" "Third"]]
        @(http/post "http://localhost:8889/api/dispatch"
                   {:body (pr-str [:add-todo text])
                    :headers {"Content-Type" "application/edn"}
                    :as :text})
        (Thread/sleep 100))
      
      ;; Check we have 3 todos
      (let [resp @(http/get "http://localhost:8889/api/state" {:as :text})
            state (edn/read-string (:body resp))]
        (is (= 3 (count (:todos state)))))
      
      ;; Undo
      @(http/post "http://localhost:8889/api/dispatch"
                 {:body (pr-str [:time-travel/undo])
                  :headers {"Content-Type" "application/edn"}
                  :as :text})
      
      (Thread/sleep 200)
      
      ;; Should have 2 todos
      (let [resp @(http/get "http://localhost:8889/api/state" {:as :text})
            state (edn/read-string (:body resp))]
        (is (= 2 (count (:todos state))))
        (let [todo-texts (set (map :text (vals (:todos state))))]
          (is (contains? todo-texts "First"))
          (is (contains? todo-texts "Second"))
          (is (not (contains? todo-texts "Third")))))
      
      (finally
        (server/stop-server)))))