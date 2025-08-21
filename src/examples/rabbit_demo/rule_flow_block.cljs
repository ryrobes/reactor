(ns examples.rabbit-demo.rule-flow-block
  "Visualization block for rule execution flow graphs"
  (:require [reagent.core :as reagent]
            [reactor.core :as r]
            [clojure.string :as str]))

(defn format-timestamp [timestamp-str]
  (if-let [date (js/Date. timestamp-str)]
    (let [hours (.getHours date)
          mins (.getMinutes date)
          secs (.getSeconds date)]
      (str hours ":" (if (< mins 10) "0" "") mins ":" (if (< secs 10) "0" "") secs))
    timestamp-str))

(defn rule-execution-row [{:keys [_id rule_id triggered_by condition_result action_executed 
                                  execution_time_ms executed_at correlation_id]}]
  [:div {:style {:border-bottom "1px solid rgba(0,255,212,0.1)"
                 :padding "8px"
                 :display "flex"
                 :justify-content "space-between"
                 :align-items "center"
                 :transition "background 0.2s"
                 :cursor "pointer"}
         :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(0,255,212,0.05)")
         :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")}
   [:div {:style {:display "flex"
                  :align-items "center"
                  :gap "10px"}}
    ;; Execution status icon
    [:span {:style {:color (if action_executed "#00ff9f" "#ff4f99")
                    :font-size "14px"}}
     (if action_executed "✓" "✗")]
    ;; Rule ID
    [:span {:style {:color "#00ffd4"
                    :font-family "monospace"
                    :font-size "11px"
                    :min-width "150px"}}
     rule_id]
    ;; Trigger type badge
    [:span {:style {:padding "2px 6px"
                    :background (case triggered_by
                                 "table_change" "rgba(0,255,159,0.2)"
                                 "rule_cascade" "rgba(255,183,0,0.2)"
                                 "manual" "rgba(255,79,153,0.2)"
                                 "rgba(128,128,128,0.2)")
                    :color (case triggered_by
                            "table_change" "#00ff9f"
                            "rule_cascade" "#ffb700"
                            "manual" "#ff4f99"
                            "#808080")
                    :border-radius "2px"
                    :font-family "monospace"
                    :font-size "9px"
                    :text-transform "uppercase"}}
     triggered_by]]
   [:div {:style {:display "flex"
                  :align-items "center"
                  :gap "15px"}}
    ;; Execution time
    [:span {:style {:color "#8ff0a4"
                    :font-family "monospace"
                    :font-size "10px"}}
     (str execution_time_ms "ms")]
    ;; Timestamp
    [:span {:style {:color "#00ffd4"
                    :opacity 0.6
                    :font-family "monospace"
                    :font-size "10px"}}
     (format-timestamp executed_at)]]])

(defn correlation-group [{:keys [correlation_id executions]}]
  (let [expanded? (reagent/atom false)]
    (fn []
      [:div {:style {:margin-bottom "10px"
                     :border "1px solid rgba(0,255,212,0.2)"
                     :border-radius "4px"
                     :overflow "hidden"}}
       ;; Header
       [:div {:style {:padding "10px"
                      :background "rgba(0,255,212,0.05)"
                      :cursor "pointer"
                      :display "flex"
                      :justify-content "space-between"
                      :align-items "center"}
              :on-click #(swap! expanded? not)}
        [:div {:style {:display "flex"
                       :align-items "center"
                       :gap "10px"}}
         [:span {:style {:color "#00ffd4"
                         :font-family "monospace"
                         :font-size "10px"
                         :opacity 0.7}}
          "FLOW"]
         [:span {:style {:color "#00ff9f"
                         :font-family "monospace"
                         :font-size "11px"}}
          (str (count executions) " rules")]
         [:span {:style {:color "#8ff0a4"
                         :font-family "monospace"
                         :font-size "10px"}}
          (str "(" (reduce + (map :execution_time_ms executions)) "ms total)")]]
        [:span {:style {:color "#00ffd4"
                        :font-size "12px"
                        :transform (if @expanded? "rotate(90deg)" "rotate(0deg)")
                        :transition "transform 0.2s"}}
         "▶"]]
       ;; Expanded content
       (when @expanded?
         [:div {:style {:padding "10px"
                        :background "rgba(0,0,0,0.3)"}}
          ;; Flow visualization
          [:div {:style {:margin-bottom "10px"
                         :padding "10px"
                         :background "rgba(0,255,212,0.02)"
                         :border-radius "2px"}}
           (let [by-trigger (group-by :triggered_by executions)
                 root-rules (get by-trigger "table_change")
                 cascade-rules (get by-trigger "rule_cascade")]
             [:div
              ;; Root rules
              (when root-rules
                [:div {:style {:margin-bottom "10px"}}
                 [:span {:style {:color "#00ff9f"
                                 :font-family "monospace"
                                 :font-size "10px"
                                 :text-transform "uppercase"
                                 :letter-spacing "1px"}}
                  "TRIGGERED BY TABLE CHANGE:"]
                 (for [rule root-rules]
                   ^{:key (:_id rule)}
                   [:div {:style {:margin-left "20px"
                                  :margin-top "5px"}}
                    [:span {:style {:color "#00ffd4"
                                    :font-family "monospace"
                                    :font-size "11px"}}
                     "→ " (:rule_id rule)]])])
              ;; Cascaded rules
              (when cascade-rules
                [:div
                 [:span {:style {:color "#ffb700"
                                 :font-family "monospace"
                                 :font-size "10px"
                                 :text-transform "uppercase"
                                 :letter-spacing "1px"}}
                  "CASCADED RULES:"]
                 (for [rule cascade-rules]
                   ^{:key (:_id rule)}
                   [:div {:style {:margin-left "40px"
                                  :margin-top "5px"}}
                    [:span {:style {:color "#ffb700"
                                    :font-family "monospace"
                                    :font-size "11px"}}
                     "→→ " (:rule_id rule)]])])])]
          ;; Individual executions
          (for [exec executions]
            ^{:key (:_id exec)}
            [rule-execution-row exec])])])))

(defn rule-flow-block [{:keys [id position size]}]
  (let [executions (reagent/atom nil)
        rules (reagent/atom nil)
        loading? (reagent/atom false)
        view-mode (reagent/atom :executions) ; :executions, :rules, :flows
        auto-refresh? (reagent/atom true)
        refresh-timer (atom nil)]
    
    (letfn [(refresh-data []
              (reset! loading? true)
              ;; Fetch rule executions
              (-> (js/fetch "http://localhost:5000/api/sql"
                           #js {:method "POST"
                                :headers #js {"Content-Type" "application/json"}
                                :body (js/JSON.stringify 
                                       #js {:sql "SELECT * FROM reactor_rule_executions 
                                                  ORDER BY executed_at DESC 
                                                  LIMIT 100"})})
                  (.then #(.json %))
                  (.then (fn [data]
                          (let [result (js->clj data :keywordize-keys true)]
                            (reset! executions (or (:results result) result))
                            (reset! loading? false))))
                  (.catch (fn [err]
                           (js/console.error "Failed to fetch executions:" err)
                           (reset! loading? false))))
              
              ;; Fetch rules
              (-> (js/fetch "http://localhost:5000/api/sql"
                           #js {:method "POST"
                                :headers #js {"Content-Type" "application/json"}
                                :body (js/JSON.stringify 
                                       #js {:sql "SELECT * FROM reactor_rules 
                                                  ORDER BY priority DESC"})})
                  (.then #(.json %))
                  (.then (fn [data]
                          (let [result (js->clj data :keywordize-keys true)]
                            (reset! rules (or (:results result) result)))))
                  (.catch (fn [err]
                           (js/console.error "Failed to fetch rules:" err)))))
            
            (start-auto-refresh []
              (when @auto-refresh?
                (when @refresh-timer (js/clearInterval @refresh-timer))
                (reset! refresh-timer (js/setInterval refresh-data 3000))))
            
            (stop-auto-refresh []
              (when @refresh-timer 
                (js/clearInterval @refresh-timer)
                (reset! refresh-timer nil)))]
      
      (reagent/create-class
       {:component-did-mount
        (fn []
          (refresh-data)
          (start-auto-refresh))
        
        :component-will-unmount
        (fn []
          (stop-auto-refresh))
        
        :reagent-render
        (fn [{:keys [id position size]}]
          [:div {:style {:width "100%"
                        :height "100%"
                        :display "flex"
                        :flex-direction "column"
                        :background "#0a0a0a"}}
           ;; Header
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
              "RULE SYSTEM"]
             [:div {:style {:display "flex"
                           :gap "10px"}}
              [:button {:style {:padding "4px 8px"
                               :background (if @auto-refresh?
                                            "rgba(0,255,159,0.3)"
                                            "rgba(0,255,159,0.1)")
                               :color "#00ff9f"
                               :border "1px solid rgba(0,255,159,0.3)"
                               :border-radius "2px"
                               :cursor "pointer"
                               :font-family "monospace"
                               :font-size "10px"}
                        :on-click (fn []
                                   (swap! auto-refresh? not)
                                   (if @auto-refresh?
                                     (stop-auto-refresh)
                                     (start-auto-refresh)))}
               (if @auto-refresh? "AUTO" "MANUAL")]
              [:button {:style {:padding "4px 8px"
                               :background "rgba(0,255,212,0.1)"
                               :color "#00ffd4"
                               :border "1px solid rgba(0,255,212,0.3)"
                               :border-radius "2px"
                               :cursor "pointer"
                               :font-family "monospace"
                               :font-size "10px"}
                        :on-click refresh-data}
               "REFRESH"]]]
            
            ;; View mode tabs
            [:div {:style {:display "flex"
                          :gap "5px"}}
             [:button {:style {:padding "6px 12px"
                              :background (if (= @view-mode :executions)
                                           "rgba(0,255,212,0.2)"
                                           "transparent")
                              :color "#00ffd4"
                              :border "1px solid rgba(0,255,212,0.3)"
                              :border-radius "2px 0 0 2px"
                              :cursor "pointer"
                              :font-family "monospace"
                              :font-size "10px"}
                       :on-click #(reset! view-mode :executions)}
              "EXECUTIONS"]
             [:button {:style {:padding "6px 12px"
                              :background (if (= @view-mode :flows)
                                           "rgba(0,255,212,0.2)"
                                           "transparent")
                              :color "#00ffd4"
                              :border "1px solid rgba(0,255,212,0.3)"
                              :border-left "none"
                              :cursor "pointer"
                              :font-family "monospace"
                              :font-size "10px"}
                       :on-click #(reset! view-mode :flows)}
              "FLOWS"]
             [:button {:style {:padding "6px 12px"
                              :background (if (= @view-mode :rules)
                                           "rgba(0,255,212,0.2)"
                                           "transparent")
                              :color "#00ffd4"
                              :border "1px solid rgba(0,255,212,0.3)"
                              :border-left "none"
                              :border-radius "0 2px 2px 0"
                              :cursor "pointer"
                              :font-family "monospace"
                              :font-size "10px"}
                       :on-click #(reset! view-mode :rules)}
              "RULES"]]]
           
           ;; Content area
           [:div {:style {:flex 1
                         :overflow-y "auto"
                         :overflow-x "hidden"
                         :background "rgba(0,0,0,0.2)"
                         :scrollbar-width "thin"
                         :scrollbar-color "rgba(0,255,212,0.3) transparent"}}
            (cond
              @loading?
              [:div {:style {:padding "20px"
                            :text-align "center"
                            :color "#00ffd4"
                            :opacity 0.7
                            :font-family "monospace"
                            :font-size "11px"}}
               "Loading rule data..."]
              
              (= @view-mode :executions)
              (if (empty? @executions)
                [:div {:style {:padding "20px"
                              :text-align "center"
                              :color "#00ffd4"
                              :opacity 0.5
                              :font-family "monospace"
                              :font-size "11px"}}
                 "No rule executions yet. Modify some data to trigger rules!"]
                (for [exec @executions]
                  ^{:key (:_id exec)}
                  [rule-execution-row exec]))
              
              (= @view-mode :flows)
              (let [grouped (group-by :correlation_id (or @executions []))]
                (if (empty? grouped)
                  [:div {:style {:padding "20px"
                                :text-align "center"
                                :color "#00ffd4"
                                :opacity 0.5
                                :font-family "monospace"
                                :font-size "11px"}}
                   "No flow graphs yet. Rules will create flows when triggered."]
                  (for [[corr-id execs] grouped]
                    ^{:key corr-id}
                    [correlation-group {:correlation_id corr-id
                                       :executions execs}])))
              
              (= @view-mode :rules)
              (if (empty? @rules)
                [:div {:style {:padding "20px"
                              :text-align "center"
                              :color "#00ffd4"
                              :opacity 0.5
                              :font-family "monospace"
                              :font-size "11px"}}
                 "No rules defined yet."]
                (for [rule @rules]
                  ^{:key (:_id rule)}
                  [:div {:style {:border-bottom "1px solid rgba(0,255,212,0.1)"
                                :padding "10px"
                                :cursor "pointer"
                                :transition "background 0.2s"}
                         :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(0,255,212,0.05)")
                         :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")}
                   [:div {:style {:display "flex"
                                 :justify-content "space-between"
                                 :align-items "center"}}
                    [:div
                     [:div {:style {:color "#00ffd4"
                                   :font-family "monospace"
                                   :font-size "11px"
                                   :margin-bottom "4px"}}
                      (:rule_id rule)]
                     [:div {:style {:color "#8ff0a4"
                                   :font-family "monospace"
                                   :font-size "10px"
                                   :opacity 0.7}}
                      (:description rule)]]
                    [:div {:style {:display "flex"
                                  :align-items "center"
                                  :gap "10px"}}
                     [:span {:style {:padding "2px 6px"
                                    :background (if (:enabled rule)
                                                 "rgba(0,255,159,0.2)"
                                                 "rgba(255,79,153,0.2)")
                                    :color (if (:enabled rule) "#00ff9f" "#ff4f99")
                                    :border-radius "2px"
                                    :font-family "monospace"
                                    :font-size "9px"}}
                      (if (:enabled rule) "ON" "OFF")]
                     [:span {:style {:color "#ffb700"
                                    :font-family "monospace"
                                    :font-size "10px"}}
                      (str "P:" (:priority rule))]]]])))]])}))))