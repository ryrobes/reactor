(ns examples.rabbit-demo.tap-block
  "TAP block component for displaying tap> entries"
  (:require [reagent.core :as reagent]
            [examples.rabbit-demo.tap-handler :as tap]
            [examples.rabbit-demo.edn-tree :as tree]
            [clojure.string :as str]
            [clojure.pprint :as pprint]
            [goog.string :as gstr]
            [goog.string.format]))

(defn format-timestamp [date]
  (let [hours (.getHours date)
        mins (.getMinutes date)
        secs (.getSeconds date)
        ms (.getMilliseconds date)]
    (str (str/join ":" [(gstr/format "%02d" hours)
                         (gstr/format "%02d" mins)
                         (gstr/format "%02d" secs)])
         "." (gstr/format "%03d" ms))))

(defn tap-entry-component [entry]
  (let [expanded? (:expanded? entry)
        value (:value entry)
        timestamp (:timestamp entry)
        caller (:caller entry)
        platform (:platform entry)]
    [:div {:style {:border-bottom "1px solid rgba(0,255,212,0.1)"
                   :padding "8px"
                   :cursor "pointer"
                   :transition "background 0.2s"
                   :background (when expanded? "rgba(0,255,212,0.05)")}
           :on-click #(tap/toggle-entry-expansion! (:id entry))}
     ;; Header with timestamp and metadata
     [:div {:style {:display "flex"
                    :justify-content "space-between"
                    :align-items "center"
                    :margin-bottom (if expanded? "8px" "0")}}
      [:div {:style {:display "flex"
                     :align-items "center"
                     :gap "10px"}}
       [:span {:style {:color "#00ffd4"
                       :font-family "monospace"
                       :font-size "10px"
                       :opacity 0.7}}
        (format-timestamp timestamp)]
       ;; Platform badge
       [:span {:style {:padding "2px 4px"
                       :background (if (= platform "CLJ")
                                    "rgba(255,183,0,0.2)"
                                    "rgba(0,255,159,0.2)")
                       :color (if (= platform "CLJ")
                               "#ffb700"
                               "#00ff9f")
                       :border-radius "2px"
                       :font-family "monospace"
                       :font-size "9px"
                       :font-weight "bold"}}
        platform]
       ;; Caller name
       (when (and caller (not= caller "anonymous"))
         [:span {:style {:color "#ff4f99"
                        :font-family "monospace"
                        :font-size "10px"
                        :opacity 0.8}}
          caller])]
      [:span {:style {:color "#00ff9f"
                      :font-family "monospace"
                      :font-size "10px"}}
       (cond
         (map? value) (str "map [" (count value) " keys]")
         (vector? value) (str "vec [" (count value) " items]")
         (set? value) (str "set [" (count value) " items]")
         (list? value) (str "list [" (count value) " items]")
         (string? value) (str "\"" (if (> (count value) 30)
                                     (str (subs value 0 30) "...")
                                     value) "\"")
         :else (let [s (str value)]
                (if (> (count s) 50)
                  (str (subs s 0 50) "...")
                  s)))]]
     ;; Expanded content
     (when expanded?
       [:div {:style {:margin-top "8px"}}
        (if (or (map? value) (vector? value) (set? value) (list? value))
          ;; Use EDN tree viewer for complex data structures
          [:div {:style {:background "rgba(0,0,0,0.3)"
                        :border-radius "4px"
                        :padding "8px"
                        :max-height "300px"
                        :overflow "auto"}}
           [tree/edn-tree-view 
            {:data value
             :initial-depth 2}]]
          ;; Use simple pre for primitive values
          [:pre {:style {:margin "0"
                        :padding "8px"
                        :background "rgba(0,0,0,0.3)"
                        :border-radius "4px"
                        :color "#8ff0a4"
                        :font-family "monospace"
                        :font-size "11px"
                        :overflow-x "auto"
                        :white-space "pre-wrap"
                        :word-wrap "break-word"}}
           (tap/format-tap-value value)])])]))

(defn tap-block-content [{:keys [id position size]}]
  (let [search-term (reagent/atom "")
        auto-scroll? (reagent/atom true)
        container-ref (atom nil)]
    (reagent/create-class
     {:component-did-update
      (fn []
        ;; Auto-scroll to bottom when new entries arrive
        (when (and @auto-scroll? @container-ref)
          (set! (.-scrollTop @container-ref) (.-scrollHeight @container-ref))))
      
      :reagent-render
      (fn [{:keys [id position size]}]
        (let [entries (tap/filter-entries @search-term)]
          [:div {:style {:height "100%"
                        :display "flex"
                        :flex-direction "column"}}
           ;; Header with controls
           [:div {:style {:padding "10px"
                         :background "rgba(0,0,0,0.3)"
                         :border-bottom "1px solid rgba(0,255,212,0.2)"}}
            [:div {:style {:display "flex"
                          :justify-content "space-between"
                          :align-items "center"
                          :margin-bottom "10px"}}
             [:span {:style {:color "#00ffd4"
                            :font-family "monospace"
                            :font-size "12px"
                            :text-transform "uppercase"
                            :letter-spacing "1px"}}
              "TAP ENTRIES"]
             [:span {:style {:color "#00ff9f"
                            :font-family "monospace"
                            :font-size "10px"
                            :opacity 0.7}}
              (str (count entries) " entries")]]
            ;; Control buttons
            [:div {:style {:display "flex"
                          :gap "5px"
                          :margin-bottom "10px"}}
             [:button {:style {:padding "4px 8px"
                              :background "rgba(0,255,212,0.1)"
                              :color "#00ffd4"
                              :border "1px solid rgba(0,255,212,0.3)"
                              :border-radius "2px"
                              :cursor "pointer"
                              :font-family "monospace"
                              :font-size "10px"}
                       :on-click #(tap/clear-tap-entries!)}
              "CLEAR"]
             [:button {:style {:padding "4px 8px"
                              :background (if @auto-scroll?
                                           "rgba(0,255,159,0.3)"
                                           "rgba(0,255,159,0.1)")
                              :color "#00ff9f"
                              :border "1px solid rgba(0,255,159,0.3)"
                              :border-radius "2px"
                              :cursor "pointer"
                              :font-family "monospace"
                              :font-size "10px"}
                       :on-click #(swap! auto-scroll? not)}
              (if @auto-scroll? "AUTO-SCROLL ON" "AUTO-SCROLL OFF")]]
            ;; Search input
            [:input {:type "text"
                    :placeholder "Search tap entries..."
                    :value @search-term
                    :on-change #(reset! search-term (.. % -target -value))
                    :style {:width "100%"
                           :padding "4px 8px"
                           :background "rgba(0,255,212,0.05)"
                           :color "#00ffd4"
                           :border "1px solid rgba(0,255,212,0.2)"
                           :border-radius "2px"
                           :font-family "monospace"
                           :font-size "11px"
                           :outline "none"}}]]
           ;; Entries list
           [:div {:ref #(reset! container-ref %)
                 :style {:flex 1
                        :overflow-y "auto"
                        :overflow-x "hidden"
                        :background "rgba(0,0,0,0.2)"
                        :scrollbar-width "thin"
                        :scrollbar-color "rgba(0,255,212,0.3) transparent"}}
            (if (empty? entries)
              [:div {:style {:padding "20px"
                            :text-align "center"
                            :color "#00ffd4"
                            :opacity 0.5
                            :font-family "monospace"
                            :font-size "11px"}}
               "No tap entries yet. Use (tap> value) in your code."]
              (for [entry entries]
                ^{:key (:id entry)}
                [tap-entry-component entry]))]]))})))