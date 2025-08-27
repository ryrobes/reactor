(ns examples.rabbit-demo.tap-block
  "TAP block component for displaying tap> entries"
  (:require [reagent.core :as reagent]
            [examples.rabbit-demo.tap-handler :as tap]
            [examples.rabbit-demo.edn-tree :as tree]
            [clojure.string :as str]
            [clojure.pprint :as pprint]
            [goog.string :as gstr]
            [goog.string.format]
            [examples.rabbit-demo.themes :as themes]))

(defn format-timestamp [date]
  (let [hours (.getHours date)
        mins (.getMinutes date)
        secs (.getSeconds date)
        ms (.getMilliseconds date)]
    (str (str/join ":" [(gstr/format "%02d" hours)
                         (gstr/format "%02d" mins)
                         (gstr/format "%02d" secs)])
         "." (gstr/format "%03d" ms))))

(defn tap-entry-component [entry block-width]
  (let [expanded? (:expanded? entry)
        value (:value entry)
        timestamp (:timestamp entry)
        caller (:caller entry)
        platform (:platform entry)
        ;; Calculate max width for preview based on block width
        max-preview-chars (max 20 (int (/ (- block-width 280) 8)))]
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
                       :font-family (themes/get-font-family :monospace)
                       :font-size "10px"
                       :opacity 0.7}}
        (format-timestamp timestamp)]
       ;; Platform badge
       [:span {:style {:padding "2px 4px"
                       :background (cond
                                    (= platform "CLJ") "rgba(255,183,0,0.2)"
                                    (= platform "JS") "rgba(255,255,0,0.2)"
                                    :else "rgba(0,255,159,0.2)")
                       :color (cond
                               (= platform "CLJ") "#ffb700"
                               (= platform "JS") "#ffff00"
                               :else "#00ff9f")
                       :border-radius "2px"
                       :font-family (themes/get-font-family :monospace)
                       :font-size "9px"
                       :font-weight "bold"}}
        platform]
       ;; Caller name
       (when (and caller (not= caller "anonymous"))
         [:span {:style {:color "#ff4f99"
                        :font-family (themes/get-font-family :monospace)
                        :font-size "10px"
                        :opacity 0.8}}
          caller])]
      ;; Value preview - show as much as block width allows
      [:span {:style {:color "#00ff9f"
                      :font-family (themes/get-font-family :monospace)
                      :font-size "10px"
                      :flex 1
                      :overflow "hidden"
                      :text-overflow "ellipsis"
                      :white-space "nowrap"
                      :margin-left "10px"}}
       (cond
         ;; For strings, show as-is
         (string? value) 
         (if (> (count value) max-preview-chars)
           (str (subs value 0 max-preview-chars) "...")
           value)
         
         ;; For vectors from JS console (platform = "JS"), show first element if it's a string
         (and (vector? value) (= platform "JS") (string? (first value)))
         (let [preview (first value)]
           (if (> (count preview) max-preview-chars)
             (str (subs preview 0 max-preview-chars) "...")
             preview))
         
         ;; For other vectors, show as data structure
         (vector? value)
         (let [preview (pr-str value)]
           (if (> (count preview) max-preview-chars)
             (str (subs preview 0 max-preview-chars) "...")
             preview))
         
         ;; For maps, show as data structure  
         (map? value) 
         (let [preview (pr-str value)]
           (if (> (count preview) max-preview-chars)
             (str (subs preview 0 max-preview-chars) "...")
             preview))
         
         :else 
         (let [s (str value)]
           (if (> (count s) max-preview-chars)
             (str (subs s 0 max-preview-chars) "...")
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
                        :font-family (themes/get-font-family :monospace)
                        :font-size "11px"
                        :overflow-x "auto"
                        :white-space "pre-wrap"
                        :word-wrap "break-word"}}
           (tap/format-tap-value value)])])]))

(defn tap-block-content [{:keys [id position size]}]
  (let [search-term (reagent/atom "")
        auto-scroll? (reagent/atom true)
        container-ref (atom nil)
        ;; Platform filters
        show-clj? (reagent/atom true)
        show-cljs? (reagent/atom true)
        show-js? (reagent/atom true)]
    (reagent/create-class
     {:component-did-update
      (fn []
        ;; Auto-scroll to bottom when new entries arrive
        (when (and @auto-scroll? @container-ref)
          (set! (.-scrollTop @container-ref) (.-scrollHeight @container-ref))))
      
      :reagent-render
      (fn [{:keys [id position size]}]
        (let [all-entries (tap/filter-entries @search-term)
              ;; Apply platform filters
              entries (filter (fn [entry]
                               (let [platform (:platform entry)]
                                 (or (and (= platform "CLJ") @show-clj?)
                                     (and (= platform "CLJS") @show-cljs?)
                                     (and (= platform "JS") @show-js?)
                                     (and (nil? platform) @show-cljs?)))) ; Default old entries to CLJS
                             all-entries)]
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
                            :font-family (themes/get-font-family :monospace)
                            :font-size "12px"
                            :text-transform "uppercase"
                            :letter-spacing "1px"}}
              "TAP ENTRIES"]
             [:span {:style {:color "#00ff9f"
                            :font-family (themes/get-font-family :monospace)
                            :font-size "10px"
                            :opacity 0.7}}
              (str (count entries) " entries")]]
            ;; Control buttons
            [:div {:style {:display "flex"
                          :gap "5px"
                          :margin-bottom "10px"
                          :flex-wrap "wrap"}}
             [:button {:style {:padding "4px 8px"
                              :background "rgba(0,255,212,0.1)"
                              :color "#00ffd4"
                              :border "1px solid rgba(0,255,212,0.3)"
                              :border-radius "2px"
                              :cursor "pointer"
                              :font-family (themes/get-font-family :monospace)
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
                              :font-family (themes/get-font-family :monospace)
                              :font-size "10px"}
                       :on-click #(swap! auto-scroll? not)}
              (if @auto-scroll? "AUTO-SCROLL ON" "AUTO-SCROLL OFF")]
             ;; Platform filter toggles
             [:div {:style {:display "flex"
                           :gap "3px"
                           :margin-left "auto"}}
              [:button {:style {:padding "3px 6px"
                               :background (if @show-clj?
                                            "rgba(255,183,0,0.3)"
                                            "rgba(255,183,0,0.1)")
                               :color "#ffb700"
                               :border "1px solid rgba(255,183,0,0.5)"
                               :border-radius "2px"
                               :cursor "pointer"
                               :font-family (themes/get-font-family :monospace)
                               :font-size "9px"
                               :font-weight "bold"}
                        :title "Toggle CLJ entries"
                        :on-click #(swap! show-clj? not)}
               "CLJ"]
              [:button {:style {:padding "3px 6px"
                               :background (if @show-cljs?
                                            "rgba(0,255,159,0.3)"
                                            "rgba(0,255,159,0.1)")
                               :color "#00ff9f"
                               :border "1px solid rgba(0,255,159,0.5)"
                               :border-radius "2px"
                               :cursor "pointer"
                               :font-family (themes/get-font-family :monospace)
                               :font-size "9px"
                               :font-weight "bold"}
                        :title "Toggle CLJS entries"
                        :on-click #(swap! show-cljs? not)}
               "CLJS"]
              [:button {:style {:padding "3px 6px"
                               :background (if @show-js?
                                            "rgba(255,255,0,0.3)"
                                            "rgba(255,255,0,0.1)")
                               :color "#ffff00"
                               :border "1px solid rgba(255,255,0,0.5)"
                               :border-radius "2px"
                               :cursor "pointer"
                               :font-family (themes/get-font-family :monospace)
                               :font-size "9px"
                               :font-weight "bold"}
                        :title "Toggle JS entries (console)"
                        :on-click #(swap! show-js? not)}
               "JS"]]]
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
                           :font-family (themes/get-font-family :monospace)
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
                            :font-family (themes/get-font-family :monospace)
                            :font-size "11px"}}
               "No tap entries yet. Use (tap> value) in your code."]
              (for [entry entries]
                ^{:key (:id entry)}
                [tap-entry-component entry (:width size)]))]]))})))