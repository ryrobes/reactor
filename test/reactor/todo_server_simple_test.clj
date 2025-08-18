(ns reactor.todo-server-simple-test
  "Simple tests for XTDB TODO server without EDN parsing"
  (:require [clojure.test :refer :all]
            [examples.todo-app.server-xtdb-clean :as server]
            [org.httpkit.client :as http]))

(deftest test-server-endpoints
  (testing "Server endpoints respond correctly"
    (server/start-server 7777)
    
    (try
      (Thread/sleep 200)
      
      ;; Test state endpoint
      (let [resp @(http/get "http://localhost:7777/api/state" {:as :text})]
        (is (= 200 (:status resp)))
        (is (re-find #":todos" (:body resp))))
      
      ;; Test dispatch endpoint
      (let [resp @(http/post "http://localhost:7777/api/dispatch"
                             {:body (pr-str [:add-todo "Test"])
                              :headers {"Content-Type" "application/edn"}
                              :as :text})]
        (is (= 200 (:status resp))))
      
      ;; Test history endpoint
      (let [resp @(http/get "http://localhost:7777/api/history" {:as :text})]
        (is (= 200 (:status resp))))
      
      (finally
        (server/stop-server)))))