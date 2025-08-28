(ns examples.rabbit-demo.cache-debug-panel
  "Debug panel for monitoring temporal cache performance"
  (:require [reagent.core :as reagent]
            [reagent.dom]
            [examples.rabbit-demo.temporal-cache-utils :as cache-utils]
            [examples.rabbit-demo.themes :as themes]))

(defn format-bytes
  "Format bytes into human readable format"
  [bytes]
  (cond
    (< bytes 1024) (str bytes " B")
    (< bytes (* 1024 1024)) (str (.toFixed (/ bytes 1024) 1) " KB")
    :else (str (.toFixed (/ bytes (* 1024 1024)) 1) " MB")))

(defn cache-debug-panel
  "Component that shows cache statistics"
  []
  (let [stats (reagent/atom (cache-utils/get-cache-stats))
        visible? (reagent/atom false)]
    (fn []
      [:div {:style {:position "fixed"
                     :bottom 10
                     :right 10
                     :z-index 10000
                     :font-family "monospace"
                     :font-size "12px"}}
       ;; Toggle button
       [:button {:style {:padding "5px 10px"
                        :background (themes/get-primary-color)
                        :color "white"
                        :border "none"
                        :border-radius "4px"
                        :cursor "pointer"}
                :on-click #(do
                            (swap! visible? not)
                            (when @visible?
                              (reset! stats (cache-utils/get-cache-stats))))}
        (if @visible? "Hide Cache Stats" "Show Cache Stats")]
       
       ;; Stats panel
       (when @visible?
         [:div {:style {:margin-top "5px"
                       :padding "10px"
                       :background "rgba(0,0,0,0.9)"
                       :color "#00ff9f"
                       :border "1px solid #00ff9f"
                       :border-radius "4px"
                       :min-width "250px"}}
          [:div {:style {:margin-bottom "10px"
                        :font-weight "bold"
                        :border-bottom "1px solid #00ff9f"
                        :padding-bottom "5px"}}
           "Temporal Cache Stats"]
          
          [:div {:style {:margin "5px 0"}}
           "Total entries: " [:span {:style {:color "white"}} (:total @stats)]]
          
          [:div {:style {:margin "5px 0"}}
           "Valid entries: " [:span {:style {:color "#00ff00"}} (:valid @stats)]]
          
          [:div {:style {:margin "5px 0"}}
           "Invalid entries: " [:span {:style {:color "#ff6666"}} (:invalid @stats)]]
          
          [:div {:style {:margin "5px 0"}}
           "Est. size: " [:span {:style {:color "white"}} 
                               (format-bytes (:size-estimate @stats))]]
          
          [:div {:style {:margin "5px 0"}}
           "Hit rate: " [:span {:style {:color "#ffff00"}} 
                             (if (> (:total @stats) 0)
                               (str (.toFixed (* 100 (/ (:valid @stats) (:total @stats))) 1) "%")
                               "N/A")]]
          
          [:div {:style {:margin-top "10px"
                        :padding-top "10px"
                        :border-top "1px solid #00ff9f"}}
           [:button {:style {:padding "5px 10px"
                            :margin-right "5px"
                            :background "#ff6666"
                            :color "white"
                            :border "none"
                            :border-radius "4px"
                            :cursor "pointer"}
                    :on-click #(do
                                (cache-utils/clear-temporal-cache!)
                                (reset! stats (cache-utils/get-cache-stats))
                                (js/console.log "[CACHE] Cleared temporal cache"))}
            "Clear Cache"]
           
           [:button {:style {:padding "5px 10px"
                            :background "#0066ff"
                            :color "white"
                            :border "none"
                            :border-radius "4px"
                            :cursor "pointer"}
                    :on-click #(do
                                (reset! stats (cache-utils/get-cache-stats))
                                (js/console.log "[CACHE] Refreshed stats" (clj->js @stats)))}
            "Refresh"]]])])))

(defn init-debug-panel!
  "Initialize the debug panel in the DOM"
  []
  (when-let [container (.getElementById js/document "cache-debug-panel")]
    (reagent.dom/render [cache-debug-panel] container)))