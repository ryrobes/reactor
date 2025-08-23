(ns examples.rabbit-demo.sql-tap-block
  "SQL-based TAP block component that queries reactor_taps table"
  (:require [reagent.core :as reagent]
            [reactor.core :as r]
            [examples.rabbit-demo.edn-tree :as tree]
            [examples.rabbit-demo.reactive-queries :as rq]
            [cljs.reader :as reader]
            [clojure.string :as str]
            [goog.string :as gstr]
            [goog.string.format]))

(defn format-timestamp [timestamp-str]
  (if-let [date (js/Date. timestamp-str)]
    (let [hours (.getHours date)
          mins (.getMinutes date)
          secs (.getSeconds date)
          ms (.getMilliseconds date)]
      (str (str/join ":" [(gstr/format "%02d" hours)
                          (gstr/format "%02d" mins)
                          (gstr/format "%02d" secs)])
           "." (gstr/format "%03d" ms)))
    timestamp-str))

(defn tap-entry-row [{:keys [_id value_edn caller platform created_at value_type]} expanded-ids block-width]
  (let [expanded? (contains? @expanded-ids _id)
        value (try 
                (reader/read-string value_edn)
                (catch :default _ value_edn))
        ;; For JS console messages, simplify unwrapping
        value (if (= platform "JS")
                (cond
                  ;; If it's a string, use it
                  (string? value) value
                  ;; If it's a single-element sequence, unwrap it
                  (and (sequential? value) (= (count value) 1))
                  (first value)
                  ;; Otherwise keep as is
                  :else value)
                value)
        ;; Calculate max width for preview based on block width
        max-preview-chars (max 20 (int (/ (- block-width 280) 8)))]
    [:div {:style {:border-bottom "1px solid rgba(0,255,212,0.1)"
                   :padding "8px"
                   :cursor "pointer"
                   :transition "background 0.2s"
                   :background (when expanded? "rgba(0,255,212,0.05)")}
           :on-click #(swap! expanded-ids (fn [ids]
                                           (if (contains? ids _id)
                                             (disj ids _id)
                                             (conj ids _id))))}
     ;; Header
     [:div {:style {:display "flex"
                    :justify-content "space-between"
                    :align-items "center"
                    :margin-bottom (if expanded? "8px" "0")}}
      [:div {:style {:display "flex"
                     :align-items "center"
                     :gap "10px"}}
       [:span {:style {:color "#00ffd4"
                       :font-family "'JetBrains Mono', monospace"
                       :font-size "10px"
                       :opacity 0.7}}
        (format-timestamp created_at)]
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
                       :font-family "'JetBrains Mono', monospace"
                       :font-size "9px"
                       :font-weight "bold"}}
        platform]
       ;; Caller name
       (when (and caller (not= caller "anonymous"))
         [:span {:style {:color "#ff4f99"
                        :font-family "'JetBrains Mono', monospace"
                        :font-size "10px"
                        :opacity 0.8}}
          caller])]
      ;; Value preview - show as much as block width allows
      [:span {:style {:color "#00ff9f"
                      :font-family "'JetBrains Mono', monospace"
                      :font-size "10px"
                      :flex 1
                      :overflow "hidden"
                      :text-overflow "ellipsis"
                      :white-space "nowrap"
                      :margin-left "10px"}}
       (cond
         ;; For JS console messages - always show as plain text
         (= platform "JS")
         (let [display-str (if (string? value) 
                             value
                             (str value))]
           (if (> (count display-str) max-preview-chars)
             (str (subs display-str 0 max-preview-chars) "...")
             display-str))
         
         ;; For other strings, show as-is
         (= value_type "string")
         (if (> (count value) max-preview-chars)
           (str (subs value 0 max-preview-chars) "...")
           value)
         
         ;; For vectors from JS console (platform = "JS"), show first element if it's a string
         (and (= value_type "vector") (= platform "JS") (vector? value) (string? (first value)))
         (let [preview (first value)]
           (if (> (count preview) max-preview-chars)
             (str (subs preview 0 max-preview-chars) "...")
             preview))
         
         ;; For other vectors, show as data structure
         (= value_type "vector")
         (let [preview (pr-str value)]
           (if (> (count preview) max-preview-chars)
             (str (subs preview 0 max-preview-chars) "...")
             preview))
         
         ;; For maps, show as data structure
         (= value_type "map")
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
        (cond
          ;; For JS platform, always show as plain text
          (= platform "JS")
          [:pre {:style {:margin "0"
                        :padding "8px"
                        :background "rgba(0,0,0,0.3)"
                        :border-radius "4px"
                        :color "#8ff0a4"
                        :font-family "'JetBrains Mono', monospace"
                        :font-size "11px"
                        :overflow-x "auto"
                        :white-space "pre-wrap"
                        :word-wrap "break-word"}}
           (if (string? value) value (str value))]
          
          ;; For complex data structures
          (contains? #{"map" "vector" "set" "list"} value_type)
          [:div {:style {:background "rgba(0,0,0,0.3)"
                        :border-radius "4px"
                        :padding "8px"
                        :max-height "300px"
                        :overflow "auto"}}
           [tree/edn-tree-view 
            {:data value
             :initial-depth 2}]]
          
          ;; For simple values
          :else
          [:pre {:style {:margin "0"
                        :padding "8px"
                        :background "rgba(0,0,0,0.3)"
                        :border-radius "4px"
                        :color "#8ff0a4"
                        :font-family "'JetBrains Mono', monospace"
                        :font-size "11px"
                        :overflow-x "auto"
                        :white-space "pre-wrap"
                        :word-wrap "break-word"}}
           (str value)])])]))

(defn sql-tap-block [{:keys [id position size source-id connection-mode] :as block}]
  (let [expanded-ids (reagent/atom #{})
        search-term (reagent/atom "")
        auto-refresh? (reagent/atom false)
        limit (reagent/atom 100)
        query-result (reagent/atom nil)
        loading? (reagent/atom false)
        refresh-timer (atom nil)
        ;; Platform filters
        show-clj? (reagent/atom true)
        show-cljs? (reagent/atom true)
        show-js? (reagent/atom true)
        ;; Connection to query block for time travel
        connected-query-id (reagent/atom source-id)
        ;; Use provided connection-mode or create a dummy one
        connection-mode (or connection-mode (reagent/atom nil))]
    
    ;; Function to refresh data
    (letfn [(refresh-data []
              (js/console.log "Refreshing TAP data...")
              (reset! loading? true)
              (let [;; Get timestamp from connected query block if any
                    timestamp (when @connected-query-id
                               (let [block-id-str (cond
                                                   (string? @connected-query-id) @connected-query-id
                                                   (keyword? @connected-query-id) (name @connected-query-id)
                                                   :else (str @connected-query-id))]
                                 (get-in @rq/block-results [:*timestamp block-id-str])))
                    ;; Build platform filter
                    platforms-to-show (cond-> []
                                       @show-clj? (conj "'CLJ'")
                                       @show-cljs? (conj "'CLJS'")
                                       @show-js? (conj "'JS'"))
                    ;; Build WHERE conditions
                    where-conditions (cond-> []
                                       ;; Platform filter - if none selected, show none
                                       (empty? platforms-to-show)
                                       (conj "1=0") ; This ensures no results if no platforms selected
                                       
                                       (seq platforms-to-show)
                                       (conj (str "platform IN (" (str/join ", " platforms-to-show) ")"))
                                       ;; Search filter
                                       (not (str/blank? @search-term))
                                       (conj (str "(value_edn LIKE '%" @search-term "%'"
                                                 " OR caller LIKE '%" @search-term "%')")))
                    ;; Combine conditions
                    where-clause (when (seq where-conditions)
                                  (str " WHERE " (str/join " AND " where-conditions)))
                    ;; Add temporal clause if we have a timestamp - proper XTDB syntax
                    temporal-clause (when timestamp
                                     (str " FOR SYSTEM_TIME AS OF TIMESTAMP '" timestamp "'"))
                    ;; Build full query - temporal clause goes after FROM table
                    full-query (str "SELECT * FROM reactor_taps"
                                  temporal-clause
                                  where-clause
                                  " ORDER BY created_at DESC"
                                  " LIMIT " @limit)]
                (js/console.log "TAP SQL Query:" full-query)
                (when timestamp
                  (js/console.log "Using timestamp from connected block:" timestamp))
                ;; Use fetch directly to query the SQL endpoint
                (-> (js/fetch "http://localhost:5000/api/sql"
                             #js {:method "POST"
                                  :headers #js {"Content-Type" "application/json"}
                                  :body (js/JSON.stringify #js {:sql full-query})})
                    (.then #(.json %))
                    (.then (fn [data]
                            (let [result (js->clj data :keywordize-keys true)
                                  tap-data (or (:results result) result)]
                              (reset! query-result tap-data)
                              ;; Store results for chart blocks to access
                              (when id
                                (let [block-data {:results tap-data
                                                 :loading false
                                                 :error nil}]
                                  (js/console.log "[SQL-TAP]" id "storing results for charts:" 
                                                 "count:" (count tap-data)
                                                 "sample:" (clj->js (take 2 tap-data)))
                                  (swap! rq/block-results assoc id block-data)))
                              (reset! loading? false))))
                    (.catch (fn [err]
                             (js/console.error "Failed to query taps:" err)
                             (reset! loading? false))))))
            
            (start-auto-refresh []
              (when @auto-refresh?
                (when @refresh-timer (js/clearInterval @refresh-timer))
                (reset! refresh-timer (js/setInterval refresh-data 5000))))
            
            (stop-auto-refresh []
              (when @refresh-timer 
                (js/clearInterval @refresh-timer)
                (reset! refresh-timer nil)))]
      
      (reagent/create-class
       {:component-did-mount
        (fn []
          (refresh-data)
          (start-auto-refresh)
          ;; Watch for timestamp changes on connected query block
          (when @connected-query-id
            (let [watch-key (keyword (str "timestamp-watch-" id))]
              (add-watch rq/block-results watch-key
                        (fn [_ _ old-state new-state]
                          (when @connected-query-id
                            (let [block-id-str (cond
                                               (string? @connected-query-id) @connected-query-id
                                               (keyword? @connected-query-id) (name @connected-query-id)
                                               :else (str @connected-query-id))
                                  old-timestamp (get-in old-state [:*timestamp block-id-str])
                                  new-timestamp (get-in new-state [:*timestamp block-id-str])]
                              ;; Only refresh if the timestamp actually changed
                              (when (not= old-timestamp new-timestamp)
                                (js/console.log "[SQL-TAP] Timestamp changed for connected block" block-id-str 
                                              "from" old-timestamp "to" new-timestamp)
                                (refresh-data)))))))))
        
        :component-did-update
        (fn [_ [_ old-props]]
          ;; Update connected-query-id if source-id prop changed
          (when (not= (:source-id old-props) source-id)
            (reset! connected-query-id source-id)
            ;; Remove old watch and add new one if needed
            (let [watch-key (keyword (str "timestamp-watch-" id))]
              (remove-watch rq/block-results watch-key)
              (when source-id
                (add-watch rq/block-results watch-key
                          (fn [_ _ old-state new-state]
                            (when @connected-query-id
                              (let [block-id-str (cond
                                                 (string? @connected-query-id) @connected-query-id
                                                 (keyword? @connected-query-id) (name @connected-query-id)
                                                 :else (str @connected-query-id))
                                    old-timestamp (get-in old-state [:*timestamp block-id-str])
                                    new-timestamp (get-in new-state [:*timestamp block-id-str])]
                                ;; Only refresh if the timestamp actually changed
                                (when (not= old-timestamp new-timestamp)
                                  (js/console.log "[SQL-TAP] Timestamp changed for connected block" block-id-str 
                                                "from" old-timestamp "to" new-timestamp)
                                  (refresh-data))))))))
            (refresh-data)))
        
        :component-will-unmount
        (fn []
          (stop-auto-refresh)
          ;; Clean up block results when unmounting
          (when id
            (swap! rq/block-results dissoc id))
          (let [watch-key (keyword (str "timestamp-watch-" id))]
            (remove-watch rq/block-results watch-key)))
        
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
           [:div {:style {:display "flex"
                         :align-items "center"
                         :gap "10px"}}
            [:span {:style {:color "#00ffd4"
                           :font-family "'JetBrains Mono', monospace"
                           :font-size "12px"
                           :text-transform "uppercase"
                           :letter-spacing "1px"}}
             "TAP ENTRIES (SQL)"]
            ;; Show connected query block info
            (when @connected-query-id
              (let [block-id-str (cond
                                 (string? @connected-query-id) @connected-query-id
                                 (keyword? @connected-query-id) (name @connected-query-id)
                                 :else (str @connected-query-id))
                    timestamp (get-in @rq/block-results [:*timestamp block-id-str])]
                [:span {:style {:color (if timestamp "#ffff00" "#00ff9f")
                               :font-family "'JetBrains Mono', monospace"
                               :font-size "10px"
                               :padding "2px 6px"
                               :background (if timestamp 
                                            "rgba(255,255,0,0.1)"
                                            "rgba(0,255,159,0.1)")
                               :border (str "1px solid " (if timestamp
                                                           "rgba(255,255,0,0.3)"
                                                           "rgba(0,255,159,0.3)"))
                               :border-radius "2px"}}
                 (if timestamp
                   (str "⏱ " (.toLocaleTimeString (js/Date. timestamp)))
                   "→ NOW")]))]
           [:span {:style {:color "#00ff9f"
                          :font-family "'JetBrains Mono', monospace"
                          :font-size "10px"
                          :opacity 0.7}}
            (str (count @query-result) " entries")]]
          
          ;; Controls
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
                            :font-family "'JetBrains Mono', monospace"
                            :font-size "10px"}
                     :on-click refresh-data}
            "REFRESH"]
           [:button {:style {:padding "4px 8px"
                            :background (if @auto-refresh?
                                         "rgba(0,255,159,0.3)"
                                         "rgba(0,255,159,0.1)")
                            :color "#00ff9f"
                            :border "1px solid rgba(0,255,159,0.3)"
                            :border-radius "2px"
                            :cursor "pointer"
                            :font-family "'JetBrains Mono', monospace"
                            :font-size "10px"}
                     :on-click (fn []
                                (swap! auto-refresh? not)
                                (if @auto-refresh?
                                  (stop-auto-refresh)
                                  (start-auto-refresh)))}
            (if @auto-refresh? "AUTO ON" "AUTO OFF")]
           [:button {:style {:padding "4px 8px"
                            :background "rgba(255,79,153,0.1)"
                            :color "#ff4f99"
                            :border "1px solid rgba(255,79,153,0.3)"
                            :border-radius "2px"
                            :cursor "pointer"
                            :font-family "'JetBrains Mono', monospace"
                            :font-size "10px"}
                     :on-click (fn []
                                ;; Use SQL exec endpoint directly  
                                (-> (js/fetch "http://localhost:5000/api/sql-exec"
                                             #js {:method "POST"
                                                  :headers #js {"Content-Type" "application/json"}
                                                  :body (js/JSON.stringify #js {:sql "DELETE FROM reactor_taps"})})
                                    (.then (fn [_] (refresh-data)))
                                    (.catch (fn [err]
                                             (js/console.error "Failed to clear taps:" err)))))}
            "CLEAR ALL"]
           ;; Connection management button
           [:button {:style {:padding "4px 8px"
                            :background (cond
                                         @connection-mode "linear-gradient(90deg, #ff006e 0%, #ff4f99 100%)"
                                         @connected-query-id "rgba(0,255,212,0.2)"
                                         :else "transparent")
                            :color (cond
                                   @connection-mode "#ffffff"
                                   @connected-query-id "#00ffd4"
                                   :else "#00ffd4")
                            :border (str "1px solid " (if @connection-mode
                                                        "#ff006e"
                                                        "rgba(0,255,212,0.3)"))
                            :border-radius "2px"
                            :cursor "pointer"
                            :font-family "'JetBrains Mono', monospace"
                            :font-size "10px"}
                     :on-click (fn []
                                (cond
                                  ;; If in connection mode, cancel it
                                  @connection-mode
                                  (reset! connection-mode nil)
                                  
                                  ;; If connected, disconnect
                                  @connected-query-id
                                  (do (reset! connected-query-id nil)
                                      ;; Remove the watch when disconnecting
                                      (let [watch-key (keyword (str "timestamp-watch-" id))]
                                        (remove-watch rq/block-results watch-key))
                                      (r/dispatch! [:update-block id (dissoc block :source-id)])
                                      (refresh-data))
                                  
                                  ;; Otherwise, start connection mode
                                  :else
                                  (reset! connection-mode {:source-id id})))}
            (cond
              @connection-mode "CANCEL CONNECTION"
              @connected-query-id "DISCONNECT"
              :else "CONNECT TO QUERY")]
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
                             :font-family "'JetBrains Mono', monospace"
                             :font-size "9px"
                             :font-weight "bold"}
                      :title "Toggle CLJ entries"
                      :on-click #(do (swap! show-clj? not)
                                    (refresh-data))}
             "CLJ"]
            [:button {:style {:padding "3px 6px"
                             :background (if @show-cljs?
                                          "rgba(0,255,159,0.3)"
                                          "rgba(0,255,159,0.1)")
                             :color "#00ff9f"
                             :border "1px solid rgba(0,255,159,0.5)"
                             :border-radius "2px"
                             :cursor "pointer"
                             :font-family "'JetBrains Mono', monospace"
                             :font-size "9px"
                             :font-weight "bold"}
                      :title "Toggle CLJS entries"
                      :on-click #(do (swap! show-cljs? not)
                                    (refresh-data))}
             "CLJS"]
            [:button {:style {:padding "3px 6px"
                             :background (if @show-js?
                                          "rgba(255,255,0,0.3)"
                                          "rgba(255,255,0,0.1)")
                             :color "#ffff00"
                             :border "1px solid rgba(255,255,0,0.5)"
                             :border-radius "2px"
                             :cursor "pointer"
                             :font-family "'JetBrains Mono', monospace"
                             :font-size "9px"
                             :font-weight "bold"}
                      :title "Toggle JS entries (console)"
                      :on-click #(do (swap! show-js? not)
                                    (refresh-data))}
             "JS"]]]
          
          ;; Search and limit
          [:div {:style {:display "flex"
                        :gap "5px"}}
           [:input {:type "text"
                   :placeholder "Search tap entries..."
                   :value @search-term
                   :on-change #(reset! search-term (.. % -target -value))
                   :on-key-press #(when (= (.-key %) "Enter") (refresh-data))
                   :style {:flex 1
                          :padding "4px 8px"
                          :background "rgba(0,255,212,0.05)"
                          :color "#00ffd4"
                          :border "1px solid rgba(0,255,212,0.2)"
                          :border-radius "2px"
                          :font-family "'JetBrains Mono', monospace"
                          :font-size "11px"
                          :outline "none"}}]
           [:select {:value @limit
                    :on-change #(do (reset! limit (js/parseInt (.. % -target -value)))
                                   (refresh-data))
                    :style {:padding "4px 8px"
                           :background "rgba(0,255,212,0.05)"
                           :color "#00ffd4"
                           :border "1px solid rgba(0,255,212,0.2)"
                           :border-radius "2px"
                           :font-family "'JetBrains Mono', monospace"
                           :font-size "11px"
                           :cursor "pointer"}}
            [:option {:value 50} "50"]
            [:option {:value 100} "100"]
            [:option {:value 200} "200"]
            [:option {:value 500} "500"]]]]
         
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
                          :font-family "'JetBrains Mono', monospace"
                          :font-size "11px"}}
             "Loading tap entries..."]
            
            (empty? @query-result)
            [:div {:style {:padding "20px"
                          :text-align "center"
                          :color "#00ffd4"
                          :opacity 0.5
                          :font-family "'JetBrains Mono', monospace"
                          :font-size "11px"}}
             "No tap entries yet. Use (r/tap> value) in your code."]
            
            :else
            (for [entry @query-result]
              ^{:key (:_id entry)}
              [tap-entry-row entry expanded-ids (:width size)]))]])}))))