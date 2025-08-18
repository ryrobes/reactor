(ns reactor.sse-test
  (:require [clojure.test :refer :all]
            [reactor.core :as r]
            [reactor.sse :as sse]
            [org.httpkit.client :as http-client]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]))

(deftest sse-handler-tests
  (testing "SSE handler creation"
    (let [ra (r/ratom {:test "data"})
          handler (sse/create-app ra)]
      (is (fn? handler))))
  
  (testing "Format data - EDN"
    (is (= "{:a 1, :b 2}" 
           (#'sse/format-data "edn" {:a 1 :b 2}))))
  
  (testing "Format data - JSON"
    (is (= "{\"a\":1,\"b\":2}"
           (#'sse/format-data "json" {:a 1 :b 2}))))
  
  (testing "Parse path"
    (is (= [:users :alice :age]
           (#'sse/parse-path "users.alice.age")))
    (is (= [:single]
           (#'sse/parse-path "single")))
    (is (nil? (#'sse/parse-path nil)))))

(deftest broadcast-tests
  (testing "Broadcast to all channels"
    (let [ra (r/ratom {:value 0})
          channels-state (atom {})
          data {:update "test"}]
      (with-redefs [sse/channels channels-state]
        (swap! channels-state assoc 
               :channel1 {:format "edn"}
               :channel2 {:format "json"})
        (let [sent-data (atom [])]
          (with-redefs [sse/send-sse (fn [channel data]
                                       (swap! sent-data conj {:channel channel :data data}))]
            (sse/broadcast-to-all ra data)
            (is (= 2 (count @sent-data)))
            (is (some #(= :channel1 (:channel %)) @sent-data))
            (is (some #(= :channel2 (:channel %)) @sent-data)))))))
  
  (testing "Broadcast to specific path"
    (let [ra (r/ratom {:users {:alice {:age 30}}})
          channels-state (atom {})
          data {:age 31}]
      (with-redefs [sse/channels channels-state]
        (swap! channels-state assoc
               :channel1 {:path [:users :alice] :format "edn"}
               :channel2 {:path [:users :bob] :format "edn"}
               :channel3 {:path [:users :alice] :format "json"})
        (let [sent-data (atom [])]
          (with-redefs [sse/send-sse (fn [channel data]
                                       (swap! sent-data conj {:channel channel :data data}))]
            (sse/broadcast-to-path ra [:users :alice] data)
            (is (= 2 (count @sent-data)))
            (is (every? #(#{:channel1 :channel3} (:channel %)) @sent-data))
            (is (not (some #(= :channel2 (:channel %)) @sent-data)))))))))

(deftest update-endpoint-tests
  (testing "Handle update with assoc-in operation"
    (let [ra (r/ratom {:users {:alice {:age 30}}})
          request {:body (java.io.ByteArrayInputStream. 
                          (.getBytes (pr-str {:op :assoc-in
                                             :path [:users :alice :age]
                                             :value 31})))
                   :headers {"content-type" "application/edn"}}
          response (#'sse/handle-update request ra)]
      (is (= 200 (:status response)))
      (is (= 31 (get-in @ra [:users :alice :age])))))
  
  (testing "Handle update with dissoc-in operation"
    (let [ra (r/ratom {:users {:alice {:age 30 :name "Alice"}}})
          request {:body (java.io.ByteArrayInputStream.
                          (.getBytes (pr-str {:op :dissoc-in
                                             :path [:users :alice :age]
                                             :value nil})))
                   :headers {"content-type" "application/edn"}}
          response (#'sse/handle-update request ra)]
      (is (= 200 (:status response)))
      (is (= {:name "Alice"} (get-in @ra [:users :alice])))))
  
  (testing "Handle update with reset operation"
    (let [ra (r/ratom {:old "data"})
          request {:body (java.io.ByteArrayInputStream.
                          (.getBytes (pr-str {:op :reset
                                             :value {:new "data"}})))
                   :headers {"content-type" "application/edn"}}
          response (#'sse/handle-update request ra)]
      (is (= 200 (:status response)))
      (is (= {:new "data"} @ra))))
  
  (testing "Handle update with invalid operation"
    (let [ra (r/ratom {:data "test"})
          request {:body (java.io.ByteArrayInputStream.
                          (.getBytes (pr-str {:op :invalid-op
                                             :value "test"})))
                   :headers {"content-type" "application/edn"}}
          response (#'sse/handle-update request ra)]
      (is (= 400 (:status response)))
      (is (str/includes? (:body response) "error"))))
  
  (testing "Handle JSON update"
    (let [ra (r/ratom {:users {:count 0}})
          json-body (json/generate-string {:op "assoc-in"
                                          :path ["users" "count"]
                                          :value 5})
          request {:body (java.io.ByteArrayInputStream. (.getBytes json-body))
                   :headers {"content-type" "application/json"}}
          response (#'sse/handle-update request ra)]
      (is (= 200 (:status response)))
      (is (= 5 (get-in @ra [:users :count]))))))

(deftest cors-middleware-test
  (testing "CORS headers are added"
    (let [handler (fn [_] {:status 200 :body "test"})
          wrapped (sse/wrap-cors handler)
          response (wrapped {})]
      (is (= "*" (get-in response [:headers "Access-Control-Allow-Origin"])))
      (is (= "GET, POST, OPTIONS" (get-in response [:headers "Access-Control-Allow-Methods"])))
      (is (= "Content-Type" (get-in response [:headers "Access-Control-Allow-Headers"]))))))

(deftest integration-test
  (testing "SSE server can be started"
    (let [ra (r/ratom {:test "data"})
          server (atom nil)]
      (try
        (reset! server (sse/start-sse-server ra 8081))
        (Thread/sleep 100)
        (is (not (nil? @server)))
        (finally
          (when @server
            (@server)))))))