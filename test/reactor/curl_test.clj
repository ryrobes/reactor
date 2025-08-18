(ns reactor.curl-test
  "Test SSE with curl instead of http-kit client"
  (:require [clojure.test :refer :all]
            [reactor.xtdb-store :as xts]
            [reactor.sse-xtdb :as sse]
            [clojure.java.shell :as shell]))

(deftest test-sse-with-curl
  (testing "SSE endpoint with curl"
    (let [node (xts/start-xtdb-node)
          port 8768
          server (sse/start-xtdb-sse-server node port)
          base-url (str "http://localhost:" port)]
      
      (try
        ;; Wait for server to start
        (Thread/sleep 500)
        
        ;; Test health endpoint with curl
        (println "Testing health with curl...")
        (let [result (shell/sh "curl" "-s" (str base-url "/health"))]
          (println "Health result:" (:out result))
          (is (= 0 (:exit result)))
          (is (= "OK" (:out result))))
        
        ;; Test SSE endpoint with curl
        (println "Testing SSE with curl...")
        (let [sse-url (str base-url "/subscribe?query=%5B%22test%22%5D&query-format=keypath&format=edn")
              result (shell/sh "curl" "-s" "-N" "--max-time" "2" 
                             "-H" "Accept: text/event-stream"
                             sse-url)]
          (println "SSE result exit code:" (:exit result))
          (println "SSE result output:" (:out result))
          (println "SSE result error:" (:err result))
          
          ;; curl will timeout but should receive some data
          (is (or (= 0 (:exit result))    ; Success
                  (= 28 (:exit result))))  ; Timeout (expected for SSE)
          (is (re-find #"data:" (:out result))))
        
        (finally
          (server)
          (xts/stop-xtdb-node node))))))