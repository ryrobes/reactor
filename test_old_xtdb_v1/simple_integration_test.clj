(ns reactor.simple-integration-test
  "Simplified integration test to debug issues"
  (:require [clojure.test :refer :all]
            [reactor.xtdb-store :as xts]
            [reactor.xtdb-query :as xtq]
            [reactor.sse-xtdb :as sse]
            [org.httpkit.client :as http]
            [clojure.edn :as edn]))

(deftest test-basic-server-setup
  (testing "Basic server functionality"
    (let [node (xts/start-xtdb-node)
          port 8766
          server (sse/start-xtdb-sse-server node port)
          base-url (str "http://localhost:" port)]
      
      (try
        ;; Wait for server to start
        (Thread/sleep 500)
        
        ;; Check server health
        (println "Testing health endpoint...")
        (let [health-resp @(http/get (str base-url "/health") 
                                     {:as :text :timeout 2000})]
          (println "Health response:" health-resp)
          (is (= 200 (:status health-resp)))
          (is (= "OK" (:body health-resp))))
        
        ;; Test simple update
        (println "Testing update endpoint...")
        (let [resp @(http/post (str base-url "/update")
                               {:body (pr-str {:entity-id "test-1"
                                             :data {:value "test"}})
                                :headers {"Content-Type" "application/edn"}
                                :as :text
                                :timeout 2000})]
          (println "Update response:" resp)
          (is (= 200 (:status resp))))
        
        ;; Test simple query
        (println "Testing query endpoint...")
        (let [resp @(http/post (str base-url "/query")
                               {:body (pr-str {:query ["test-1"]})
                                :headers {"Content-Type" "application/edn"}
                                :as :text
                                :timeout 2000})]
          (println "Query response:" resp)
          (is (= 200 (:status resp))))
        
        (finally
          (println "Stopping server...")
          (server)
          (xts/stop-xtdb-node node))))))