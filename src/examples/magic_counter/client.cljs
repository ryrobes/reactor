(ns examples.magic-counter.client
  "Magic Counter client in 15 lines - just like re-frame!"
  (:require [reactor.core :as r]
            [reagent.dom :as rdom]))

;; Your entire app!
(defn counter-app []
  (let [count (r/subscribe [:get [:count]])]  ;; Subscribe to :count key
    [:div {:style {:text-align "center"}}
     [:h1 "Magic Counter"]
     [:div {:style {:font-size "48px"}} @count]
     [:div
      [:button {:on-click #(r/dispatch! [:dec])} "−"]
      [:button {:on-click #(r/dispatch! [:inc])} "+"]
      [:button {:on-click #(r/dispatch! [:set 0])} "Reset"]]
     [:div
      [:button {:on-click #(r/undo!)} "↶ Undo"]
      [:button {:on-click #(r/redo!)} "↷ Redo"]]]))

(defn ^:export init! []
  (r/init! {:server-url "http://localhost:4000"})
  (rdom/render [counter-app] (.getElementById js/document "app")))