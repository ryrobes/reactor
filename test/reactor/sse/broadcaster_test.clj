(ns reactor.sse.broadcaster-test
  (:require [clojure.test :refer :all]
            [reactor.sse.broadcaster :as broadcaster]
            [org.httpkit.server :as http]))

(defn mock-channel 
  "Create a mock HTTP channel for testing"
  [& {:keys [open? send-fn] :or {open? true send-fn (constantly true)}}]
  (let [state (atom {:open? open?})]
    (reify
      org.httpkit.server.Channel
      (send! [_ data close?] 
        (if (:open? @state)
          (send-fn data)
          false))
      (close [_] 
        (swap! state assoc :open? false)
        true)
      (websocket? [_] false)
      (open? [_] (:open? @state)))))

(deftest test-channel-registration
  (testing "Register and unregister channels"
    ;; Clear existing channels
    (broadcaster/disconnect-all!)
    
    (let [chan1 (mock-channel)
          chan2 (mock-channel)
          session-id "test-session"]
      
      ;; Register channels
      (broadcaster/register-channel! session-id chan1)
      (is (= 1 (broadcaster/channel-count session-id)))
      
      ;; Register another channel for same session
      (broadcaster/register-channel! session-id chan2 {:metadata "test"})
      (is (= 2 (broadcaster/channel-count session-id)))
      
      ;; Unregister one channel
      (broadcaster/unregister-channel! session-id chan1)
      (is (= 1 (broadcaster/channel-count session-id)))
      
      ;; Unregister remaining channel
      (broadcaster/unregister-channel! session-id chan2)
      (is (= 0 (broadcaster/channel-count session-id))))))

(deftest test-get-channels
  (testing "Getting channels for sessions"
    (broadcaster/disconnect-all!)
    
    (let [chan1 (mock-channel)
          chan2 (mock-channel)
          session1 "session-1"
          session2 "session-2"]
      
      (broadcaster/register-channel! session1 chan1)
      (broadcaster/register-channel! session2 chan2)
      
      ;; Get channels for specific session
      (is (= 1 (count (broadcaster/get-channels session1))))
      (is (= 1 (count (broadcaster/get-channels session2))))
      
      ;; Get all sessions
      (is (= 2 (count (broadcaster/get-all-sessions))))
      (is (contains? (set (broadcaster/get-all-sessions)) session1))
      (is (contains? (set (broadcaster/get-all-sessions)) session2))
      
      ;; Total channel count
      (is (= 2 (broadcaster/total-channel-count)))
      
      ;; Clean up
      (broadcaster/disconnect-all!)
      (is (= 0 (broadcaster/total-channel-count))))))

(deftest test-broadcast-to-session
  (testing "Broadcasting messages to session channels"
    (broadcaster/disconnect-all!)
    
    (let [received (atom [])
          chan1 (mock-channel :send-fn #(swap! received conj %))
          chan2 (mock-channel :send-fn #(swap! received conj %))
          session-id "broadcast-test"
          test-data {:type :update :value 123}]
      
      (broadcaster/register-channel! session-id chan1)
      (broadcaster/register-channel! session-id chan2)
      
      ;; Broadcast message
      (let [sent-count (broadcaster/broadcast-to-session! session-id test-data)]
        (is (= 2 sent-count))
        (is (= 2 (count @received)))
        
        ;; Check message format
        (doseq [msg @received]
          (is (string? msg))
          (is (re-find #"data:" msg))
          (is (re-find #"\"type\":\"update\"" msg)))))))

(deftest test-broadcast-to-all
  (testing "Broadcasting to all sessions"
    (broadcaster/disconnect-all!)
    
    (let [received (atom [])
          chan1 (mock-channel :send-fn #(swap! received conj {:session "s1" :msg %}))
          chan2 (mock-channel :send-fn #(swap! received conj {:session "s2" :msg %}))
          chan3 (mock-channel :send-fn #(swap! received conj {:session "s3" :msg %}))]
      
      (broadcaster/register-channel! "session1" chan1)
      (broadcaster/register-channel! "session2" chan2)
      (broadcaster/register-channel! "session3" chan3)
      
      ;; Broadcast to all
      (broadcaster/broadcast-to-all! {:type :global :data "announcement"})
      
      ;; All channels should receive message
      (is (= 3 (count @received)))
      (is (every? #(string? (:msg %)) @received)))))

(deftest test-failed-channel-cleanup
  (testing "Failed channels are automatically cleaned up"
    (broadcaster/disconnect-all!)
    
    (let [good-chan (mock-channel)
          bad-chan (mock-channel :send-fn (fn [_] (throw (Exception. "Channel error"))))
          session-id "cleanup-test"]
      
      (broadcaster/register-channel! session-id good-chan)
      (broadcaster/register-channel! session-id bad-chan)
      
      (is (= 2 (broadcaster/channel-count session-id)))
      
      ;; Broadcast - should cleanup bad channel that throws
      (broadcaster/broadcast-to-session! session-id {:data "test"})
      
      ;; Bad channel should be removed
      (is (= 1 (broadcaster/channel-count session-id))))))

(deftest test-sse-formatting
  (testing "SSE message formatting"
    (let [data {:type :test :value 123}
          formatted (broadcaster/format-sse-message data)]
      (is (string? formatted))
      (is (re-find #"^data:" formatted))
      (is (re-find #"\n\n$" formatted))
      (is (re-find #"\"type\":\"test\"" formatted))
      (is (re-find #"\"value\":123" formatted)))))

(deftest test-disconnect-session
  (testing "Disconnecting all channels for a session"
    (broadcaster/disconnect-all!)
    
    (let [chan1 (mock-channel)
          chan2 (mock-channel)
          chan3 (mock-channel)
          session1 "session-1"
          session2 "session-2"]
      
      (broadcaster/register-channel! session1 chan1)
      (broadcaster/register-channel! session1 chan2)
      (broadcaster/register-channel! session2 chan3)
      
      ;; Disconnect session1
      (let [disconnected (broadcaster/disconnect-session! session1)]
        (is (= 2 disconnected))
        (is (= 0 (broadcaster/channel-count session1)))
        (is (= 1 (broadcaster/channel-count session2)))))))

(deftest test-stats
  (testing "Statistics gathering"
    (broadcaster/disconnect-all!)
    
    (let [chan1 (mock-channel)
          chan2 (mock-channel)
          chan3 (mock-channel)]
      
      (broadcaster/register-channel! "session1" chan1)
      (broadcaster/register-channel! "session1" chan2)
      (broadcaster/register-channel! "session2" chan3)
      
      (let [stats (broadcaster/stats)]
        (is (= 2 (:total-sessions stats)))
        (is (= 3 (:total-channels stats)))
        (is (= 2 (get-in stats [:sessions "session1"])))
        (is (= 1 (get-in stats [:sessions "session2"])))))))

(deftest test-heartbeat
  (testing "Heartbeat functionality"
    (broadcaster/disconnect-all!)
    
    (let [received (atom [])
          chan (mock-channel :send-fn #(swap! received conj %))
          session-id "heartbeat-test"]
      
      (broadcaster/register-channel! session-id chan)
      
      ;; Send heartbeat
      (broadcaster/send-heartbeat!)
      
      ;; Should receive heartbeat message
      (is (= 1 (count @received)))
      (let [msg (first @received)]
        (is (string? msg))
        (is (re-find #"\"type\":\"heartbeat\"" msg))))))

(deftest test-broadcast-to-sessions
  (testing "Broadcasting to specific sessions"
    (broadcaster/disconnect-all!)
    
    (let [received (atom {})
          chan1 (mock-channel :send-fn #(swap! received update "s1" (fnil conj []) %))
          chan2 (mock-channel :send-fn #(swap! received update "s2" (fnil conj []) %))
          chan3 (mock-channel :send-fn #(swap! received update "s3" (fnil conj []) %))]
      
      (broadcaster/register-channel! "session1" chan1)
      (broadcaster/register-channel! "session2" chan2)
      (broadcaster/register-channel! "session3" chan3)
      
      ;; Broadcast to specific sessions only
      (let [sent (broadcaster/broadcast-to-sessions! 
                   ["session1" "session3"] 
                   {:type :selective})]
        (is (= 2 sent))
        (is (= 1 (count (get @received "s1"))))
        (is (= 0 (count (get @received "s2" []))))
        (is (= 1 (count (get @received "s3"))))))))

(defn run-all-tests []
  (run-tests 'reactor.sse.broadcaster-test))