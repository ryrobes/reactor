(ns examples.basic-usage
  (:require [reactor.core :as r]
            [reactor.sse :as sse]))

(defn basic-ratom-example []
  (println "=== Basic RAtom Usage ===")
  (let [ra (r/ratom {:users {:alice {:age 30 :status :active}
                             :bob {:age 25 :status :inactive}}})]
    
    (println "Initial state:" @ra)
    
    (swap! ra assoc-in [:users :alice :age] 31)
    (println "After update:" @ra)
    
    (println "Direct path access:" (ra [:users :alice]))
    (println)))

(defn subscription-example []
  (println "=== Subscription Example ===")
  (let [ra (r/ratom {:counter 0})
        updates (atom [])]
    
    (r/subscribe! ra [:counter]
                  (fn [old new]
                    (swap! updates conj {:old old :new new})
                    (println (str "Counter changed from " old " to " new))))
    
    (swap! ra update :counter inc)
    (swap! ra update :counter inc)
    (swap! ra update :counter #(+ % 10))
    
    (println "All updates:" @updates)
    (println)))

(defn cursor-example []
  (println "=== Cursor Example ===")
  (let [ra (r/ratom {:users {:alice {:age 30 :preferences {:theme :dark}}
                             :bob {:age 25 :preferences {:theme :light}}}})
        alice-prefs (r/cursor ra [:users :alice :preferences])]
    
    (println "Alice preferences via cursor:" @alice-prefs)
    
    (add-watch alice-prefs :pref-watch
               (fn [_ _ old new]
                 (println "Alice's preferences changed from" old "to" new)))
    
    (swap! ra assoc-in [:users :alice :preferences :theme] :light)
    (println "After theme change:" @alice-prefs)
    (println)))

(defn rule-example []
  (println "=== Rule System Example ===")
  (let [ra (r/ratom {:temperature 20 :heater :off :cooler :off})]
    
    (r/def-rule ra :heater-control [:temperature]
                (fn [temp] (< temp 18))
                (fn [_ _]
                  (println "Temperature too low, turning on heater")
                  (swap! ra assoc :heater :on)))
    
    (r/def-rule ra :cooler-control [:temperature]
                (fn [temp] (> temp 25))
                (fn [_ _]
                  (println "Temperature too high, turning on cooler")
                  (swap! ra assoc :cooler :on)))
    
    (r/def-rule ra :heater-off [:temperature]
                (fn [temp] (>= temp 20))
                (fn [_ _]
                  (when (= :on (:heater @ra))
                    (println "Temperature normal, turning off heater")
                    (swap! ra assoc :heater :off))))
    
    (println "Initial state:" @ra)
    (swap! ra assoc :temperature 15)
    (println "After cooling to 15°C:" @ra)
    (swap! ra assoc :temperature 28)
    (println "After heating to 28°C:" @ra)
    (swap! ra assoc :temperature 22)
    (println "After normalizing to 22°C:" @ra)
    (println)))

(defn cascading-rules-example []
  (println "=== Cascading Rules Example ===")
  (let [ra (r/ratom {:order-count 0 :revenue 0 :tier :bronze})]
    
    (r/def-rule ra :update-revenue [:order-count]
                (fn [_ count]
                  (swap! ra assoc :revenue (* count 50))))
    
    (r/def-rule ra :update-tier [:revenue]
                (fn [revenue] (>= revenue 0))
                (fn [_ revenue]
                  (cond
                    (>= revenue 1000) (swap! ra assoc :tier :gold)
                    (>= revenue 500) (swap! ra assoc :tier :silver)
                    :else (swap! ra assoc :tier :bronze))))
    
    (println "Initial:" @ra)
    (swap! ra assoc :order-count 5)
    (Thread/sleep 10)
    (println "After 5 orders:" @ra)
    (swap! ra assoc :order-count 12)
    (Thread/sleep 10)
    (println "After 12 orders:" @ra)
    (swap! ra assoc :order-count 25)
    (Thread/sleep 10)
    (println "After 25 orders:" @ra)
    (println)))

(defn derived-subscription-example []
  (println "=== Derived Subscription Example ===")
  (let [ra (r/ratom {:items [{:name "Apple" :price 1.5 :quantity 10}
                             {:name "Banana" :price 0.5 :quantity 20}
                             {:name "Orange" :price 2.0 :quantity 5}]})]
    
    (r/subscribe! ra
                  (fn []
                    (reduce + (map #(* (:price %) (:quantity %))
                                   (:items @ra))))
                  (fn [old new]
                    (println (str "Total inventory value changed from $" old " to $" new))))
    
    (swap! ra update-in [:items 0 :quantity] + 5)
    (swap! ra update :items conj {:name "Grape" :price 3.0 :quantity 8})
    (println)))

(defn persistence-example []
  (println "=== Persistence Example ===")
  (let [file-path "/tmp/ratom-state.edn"
        ra1 (r/ratom {:saved-data "important" :timestamp (System/currentTimeMillis)})]
    
    (println "Original state:" @ra1)
    (r/persist! ra1 file-path)
    (println "State persisted to" file-path)
    
    (let [ra2 (r/ratom {})]
      (r/rehydrate! ra2 file-path)
      (println "Rehydrated state:" @ra2))
    (println)))

(defn time-atom-example []
  (println "=== Time Atom Example (3 seconds) ===")
  (let [tr (r/time-ratom {:interval :second})
        tick-count (atom 0)]
    
    (r/subscribe! tr [:second]
                  (fn [_ new-second]
                    (swap! tick-count inc)
                    (println "Tick" @tick-count "- Second:" new-second)))
    
    (Thread/sleep 3100)
    (.close tr)
    (println "Time atom closed")
    (println)))

(defn reaction-example []
  (println "=== Reaction Example ===")
  (let [ra1 (r/ratom {:x 10})
        ra2 (r/ratom {:y 20})
        sum-reaction (r/reaction #(+ (:x @ra1) (:y @ra2)))]
    
    (println "Initial sum:" @sum-reaction)
    
    (add-watch sum-reaction :sum-watch
               (fn [_ _ old new]
                 (println "Sum changed from" old "to" new)))
    
    (swap! ra1 assoc :x 15)
    (Thread/sleep 10)
    (swap! ra2 assoc :y 25)
    (Thread/sleep 10)
    (println "Final sum:" @sum-reaction)
    (println)))

(defn run-all-examples []
  (basic-ratom-example)
  (subscription-example)
  (cursor-example)
  (rule-example)
  (cascading-rules-example)
  (derived-subscription-example)
  (persistence-example)
  (time-atom-example)
  (reaction-example)
  (println "=== All examples completed ==="))

(defn -main [& args]
  (run-all-examples)
  (System/exit 0))