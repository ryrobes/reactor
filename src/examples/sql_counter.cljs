(ns examples.sql-counter
  "Counter with SQL queries over history!"
  (:require [reactor.core :as r]
            [reagent.core :as reagent]
            [reagent.dom :as rdom]))

;; Custom subscription that calculates average
(r/reg-sub :average
  (fn [db _]
    (let [history (:history db [])]
      (if (empty? history)
        0
        (/ (reduce + (map :count history)) (count history))))))

(defn counter-app []
  (let [count (r/subscribe [:get [:count]])
        history (reagent/atom [])]
    
    ;; Query history on mount
    (reagent/create-class
      {:component-did-mount
       (fn []
         (-> (r/q '{:find [?time ?count]
                    :where [[?e :state ?s ?time]
                            [(get ?s :count) ?count]]})
             (.then #(reset! history %))))
       
       :reagent-render
       (fn []
         [:div
          [:h1 "SQL-Powered Counter"]
          [:div {:style {:font-size "48px"}} @count]
          
          [:div
           [:button {:on-click #(r/dispatch! [:inc])} "+"]
           [:button {:on-click #(r/dispatch! [:dec])} "-"]]
          
          [:div
           [:h3 "History (via SQL!)"]
           [:ul
            (for [[time cnt] @history]
              [:li {:key time} 
               (str cnt " at " (.toLocaleTimeString time))])]]])})))

(defn init! []
  (r/init!)
  (rdom/render [counter-app] (.getElementById js/document "app")))