(ns reactor.time-travel-simple-test
  (:require [clojure.test :refer :all]
            [reactor.time-travel-simple :as tt]
            [reactor.core :as r]))

(deftest basic-undo-redo-test
  (testing "Simple undo/redo with time travel"
    (let [tt-state (tt/create-time-travel {:counter 0})]
      ;; Make changes
      (reset! (:state tt-state) {:counter 1})
      (tt/record-change! tt-state {:counter 0} {:counter 1} [:inc])
      
      (reset! (:state tt-state) {:counter 2})
      (tt/record-change! tt-state {:counter 1} {:counter 2} [:inc])
      
      (reset! (:state tt-state) {:counter 3})
      (tt/record-change! tt-state {:counter 2} {:counter 3} [:inc])
      
      (is (= 3 (:counter @(:state tt-state))))
      
      ;; Undo
      (tt/undo! tt-state)
      (is (= 2 (:counter @(:state tt-state))))
      
      (tt/undo! tt-state)
      (is (= 1 (:counter @(:state tt-state))))
      
      ;; Redo
      (tt/redo! tt-state)
      (is (= 2 (:counter @(:state tt-state))))
      
      (tt/redo! tt-state)
      (is (= 3 (:counter @(:state tt-state)))))))

(deftest checkpoint-test
  (testing "Checkpoints work correctly"
    (let [tt-state (tt/create-time-travel {:value "initial"})]
      (tt/checkpoint! tt-state :start)
      
      (reset! (:state tt-state) {:value "modified"})
      (tt/record-change! tt-state {:value "initial"} {:value "modified"} [:change])
      (tt/checkpoint! tt-state :middle)
      
      (reset! (:state tt-state) {:value "final"})
      (tt/record-change! tt-state {:value "modified"} {:value "final"} [:change])
      
      ;; Jump to checkpoints
      (tt/jump-to! tt-state :middle)
      (is (= "modified" (:value @(:state tt-state))))
      
      (tt/jump-to! tt-state :start)
      (is (= "initial" (:value @(:state tt-state)))))))

(deftest ratom-with-time-travel-test
  (testing "RAtom with time travel enabled"
    (let [atom (r/ratom {:x 0} {:history true})]
      (swap! atom assoc :x 1)
      (swap! atom assoc :x 2)
      (swap! atom assoc :x 3)
      
      (is (= 3 (:x @atom)))
      
      ;; Get history
      (when-let [history (r/get-history atom)]
        (is (pos? (count history)))))))

(deftest history-limit-test
  (testing "History respects max limit"
    (let [tt-state (tt/create-time-travel {:n 0} :max-history 5)]
      ;; Make more changes than max-history
      (dotimes [i 10]
        (reset! (:state tt-state) {:n i})
        (tt/record-change! tt-state {:n (dec i)} {:n i} [:set i]))
      
      ;; History should be limited
      (is (<= (count (tt/get-history tt-state)) 6))))) ; +1 for initial state