(ns examples.magic-counter.server
  "Magic Counter server in 10 lines!"
  (:require [reactor.server :as reactor]))

(defn -main []
  (reactor/start! 
    :port 4000
    :handlers {:inc (fn [db _] (update db :count (fnil inc 0)))
               :dec (fn [db _] (update db :count (fnil dec 0)))
               :set (fn [db [n]] (assoc db :count n))}))