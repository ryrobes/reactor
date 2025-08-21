(ns examples.rabbit-demo.client
  "Rabbit Demo - Interactive SQL data browser with time travel"
  (:require [reactor.core :as r]
            [reagent.core :as reagent]
            [reactor.tap :as t]
            [reagent.dom :as rdom]
            [clojure.string :as str]
            [clojure.pprint :as pprint]
            [cljs.reader :as reader]
            [examples.rabbit-demo.monaco :as monaco]
            [examples.rabbit-demo.reactive-queries :as rq]
            [examples.rabbit-demo.auto-refresh :as auto-refresh]
            [examples.rabbit-demo.time-travel-ui :as tt]
            [examples.rabbit-demo.vega-chart :as vega]
            [examples.rabbit-demo.edn-tree :as tree]
            [examples.rabbit-demo.sql-tap-block :as sql-tap-block]
            [examples.rabbit-demo.rule-flow-block :as rule-flow-block]))

;; ============= Subscriptions =============

(r/reg-sub :blocks
  (fn [db _]
    (get-in db [:canvas :blocks] {})))

(r/reg-sub :block
  (fn [db [_ id]]
    (get-in db [:canvas :blocks id])))

(r/reg-sub :all-blocks
  (fn [db _]
    (get-in db [:canvas :blocks] {})))

;; ============= Dragging & Resizing State =============

(defonce drag-state (reagent/atom nil))
(defonce resize-state (reagent/atom nil))
(defonce connection-mode (reagent/atom nil))  ;; Track connection mode {:source-id block-id}
(defonce local-positions (reagent/atom {}))  ;; Track positions locally for smooth dragging
(defonce local-sizes (reagent/atom {}))  ;; Track sizes locally for smooth resizing
(defonce pending-update (atom nil))  ;; Debounce timer

;; Clear local positions when blocks change from time travel
(defonce blocks-watcher 
  (let [blocks-sub (r/subscribe [:blocks])]
    (add-watch blocks-sub ::position-sync
               (fn [_ _ old-blocks new-blocks]
                 ;; If blocks changed significantly (time travel), sync positions
                 (when (not= (set (keys old-blocks)) (set (keys new-blocks)))
                   ;; Clear local overrides to use state positions
                   (reset! local-positions {})
                   (reset! local-sizes {}))
                 ;; When a block's position in server state matches its local position,
                 ;; clear the local override (server has caught up)
                 (doseq [[id block] new-blocks]
                   (when-let [pos (:position block)]
                     (when (= pos (get @local-positions id))
                       ;; Server position matches local - clear local override
                       (swap! local-positions dissoc id)))
                   (when-let [size (:size block)]
                     (when (= size (get @local-sizes id))
                       ;; Server size matches local - clear local override
                       (swap! local-sizes dissoc id))))))))

(defn start-drag! [block-id event]
  (let [rect (.. event -currentTarget getBoundingClientRect)
        offset-x (- (.-clientX event) (.-left rect))
        offset-y (- (.-clientY event) (.-top rect))]
    (reset! drag-state {:block-id block-id
                        :offset-x offset-x
                        :offset-y offset-y})
    (.preventDefault event)))

(defn handle-drag! [event]
  (when-let [{:keys [block-id offset-x offset-y]} @drag-state]
    (let [canvas-rect (.. (.getElementById js/document "canvas") getBoundingClientRect)
          x (- (.-clientX event) (.-left canvas-rect) offset-x)
          y (- (.-clientY event) (.-top canvas-rect) offset-y)]
      ;; Update local position immediately for smooth dragging
      (swap! local-positions assoc block-id {:x x :y y})
      
      ;; Debounce the server update (increased to 300ms since we guarantee final update)
      (when @pending-update
        (js/clearTimeout @pending-update))
      (reset! pending-update
              (js/setTimeout
               #(r/dispatch! [:move-block block-id {:x x :y y}])
               300)))))

(defn stop-drag! []
  ;; Cancel any pending update FIRST to prevent race conditions
  (when @pending-update
    (js/clearTimeout @pending-update)
    (reset! pending-update nil))
  
  ;; Send final position on drag end
  (when-let [{:keys [block-id]} @drag-state]
    (when-let [pos (get @local-positions block-id)]
      ;; Immediately dispatch the final position
      (t/tap> [:stop-drag block-id] "web dragger")
      (r/dispatch! [:move-block block-id pos])
      ;; Don't clear local position immediately - let it persist until server updates
      ;; This prevents jitter from using stale server position
      ;; The position will be cleared when blocks change (via blocks-watcher)
      ))
  (reset! drag-state nil))

(defn start-resize! [block-id event]
  (let [rect (.. ^js event -currentTarget -parentElement getBoundingClientRect)
        start-x (.-clientX ^js event)
        start-y (.-clientY ^js event)
        start-width (.-width rect)
        start-height (.-height rect)]
    (reset! resize-state {:block-id block-id
                         :start-x start-x
                         :start-y start-y
                         :start-width start-width
                         :start-height start-height})
    (.preventDefault ^js event)
    (.stopPropagation ^js event)))

(defn handle-resize! [event]
  (when-let [{:keys [block-id start-x start-y start-width start-height]} @resize-state]
    (let [dx (- (.-clientX ^js event) start-x)
          dy (- (.-clientY ^js event) start-y)
          new-width (max 200 (+ start-width dx))
          new-height (max 150 (+ start-height dy))]
      ;; Update local size immediately for smooth resizing
      (swap! local-sizes assoc block-id {:width new-width :height new-height})
      
      ;; Debounce the server update (increased to 300ms since we guarantee final update)
      (when @pending-update
        (js/clearTimeout @pending-update))
      (reset! pending-update
              (js/setTimeout
               #(r/dispatch! [:resize-block block-id {:width new-width :height new-height}])
               300)))))

(defn stop-resize! []
  ;; Cancel any pending update FIRST to prevent race conditions
  (when @pending-update
    (js/clearTimeout @pending-update)
    (reset! pending-update nil))
  
  ;; Send final size on resize end
  (when-let [{:keys [block-id]} @resize-state]
    (when-let [size (get @local-sizes block-id)]
      ;; Immediately dispatch the final size
      (r/dispatch! [:resize-block block-id size])
      ;; Don't clear local size immediately - let it persist until server updates
      ;; This prevents jitter from using stale server size
      ;; The size will be cleared when blocks change (via blocks-watcher)
      ))
  (reset! resize-state nil))

;; ============= Block Components =============

(defn query-block [{:keys [id position size sql as-of] :as block}]
  (let [;; Local state for SQL editing - prevents re-renders while typing
        local-sql (reagent/atom sql)]
    (reagent/create-class
     {:component-did-mount
      (fn []
        (when sql
          ;; Initialize subscription on mount
          (js/console.log "[QUERY-BLOCK] Initializing subscription for block" id "with SQL:" sql)
          (rq/execute-block-query! id sql nil nil)))
      
      :component-did-update
      (fn [this [_ old-props]]
        (let [new-props (reagent/props this)]
          ;; Update local SQL if props change (e.g., from undo/redo or external update)
          (when (and (:sql new-props)
                    (not= (:sql old-props) (:sql new-props)))
            (reset! local-sql (:sql new-props))
            (js/console.log "[QUERY-BLOCK] SQL changed externally for block" (:id new-props) "re-subscribing")
            (rq/execute-block-query! (:id new-props) (:sql new-props) nil nil))))
      
      :component-will-unmount
      (fn []
        (rq/unsubscribe-block! id))
      
      :reagent-render
      (fn [{:keys [id position size sql as-of] :as block}]
        (let [;; Get results from reactive-queries module - including executed-sql
              {:keys [results error loading executed-sql]} (rq/get-block-results id)
            ;; Use local position only while dragging, otherwise use state position
            is-dragging? (= id (:block-id @drag-state))
            is-resizing? (= id (:block-id @resize-state))
            actual-pos (if (or is-dragging? (get @local-positions id))
                         (get @local-positions id position)
                         position)
            actual-size (if (or is-resizing? (get @local-sizes id))
                          (get @local-sizes id size)
                          size)]
    [:div.block.query-block
     {:style {:position "absolute"
              :left (:x actual-pos)
              :top (:y actual-pos)
              :width (:width actual-size)
              :height (:height actual-size)
              :background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
              :border (if (and @connection-mode (= (:source-id @connection-mode) id))
                       "2px solid #ff006e"
                       "1px solid #00ff9f")
              :border-radius "4px"
              :padding "10px"
              :z-index 10
              :box-shadow (if @connection-mode
                           "0 0 30px rgba(255,0,110,0.5), inset 0 0 20px rgba(255,0,110,0.1)"
                           "0 0 20px rgba(0,255,159,0.3), inset 0 0 20px rgba(0,255,159,0.05)")
              :display "flex"
              :flex-direction "column"}
      :draggable false}
     ;; Resize handle
     [:div.resize-handle
      {:style {:position "absolute"
               :bottom 0
               :right 0
               :width "15px"
               :height "15px"
               :cursor "nwse-resize"
               :background "linear-gradient(135deg, transparent 50%, #00ff9f 50%)"
               :opacity 0.5}
       :on-mouse-down #(start-resize! id %)}]
     ;; Fixed header and controls container
     [:div.fixed-content
      {:style {:flex-shrink 0}}
     [:div.block-header
      {:style {:display "flex"
               :justify-content "space-between"
               :margin-bottom "10px"
               :padding-bottom "5px"
               :border-bottom "1px solid rgba(0,255,159,0.2)"
               :cursor (if @connection-mode "pointer" "move")}
       :on-mouse-down (fn [e]
                        (when-not @connection-mode
                          (start-drag! id e)))
       :on-click (fn [e]
                   (when-let [conn @connection-mode]
                     (.stopPropagation ^js e)
                     ;; Update the chart block (conn's source-id) to link to this query block (id)
                     (r/dispatch! [:update-block (:source-id conn) {:source-id id}])
                     (reset! connection-mode nil)))}
      [:div {:style {:display "flex" :align-items "center" :gap "10px"}}
       [:span {:style {:font-weight "bold" 
                       :color "#00ff9f"
                       :font-family "monospace"
                       :text-transform "uppercase"
                       :font-size "11px"
                       :letter-spacing "1px"}} "SQL QUERY"]
       [:span {:style {:color "#8ff0a4"
                       :font-family "monospace"
                       :font-size "9px"
                       :opacity 0.7}} 
        (str "#" id)]]
      [:button {:on-click (fn [e]
                           (.stopPropagation ^js e)
                           (rq/unsubscribe-block! id)  ;; Clean up subscription
                           (r/dispatch! [:delete-block id]))
                :style {:background "none"
                        :border "none"
                        :color "#00ff9f"
                        :cursor "pointer"
                        :font-size "20px"
                        :line-height "20px"}}
       "×"]]
     ;; SQL Editor with Execute button to the right
     [:div {:style {:margin "10px 0"
                    :display "flex"
                    :gap "10px"}}
      ;; Editor column
      [:div {:style {:flex 1}}
       (when (and executed-sql (not= executed-sql sql))
         [:div {:style {:background "rgba(0,255,159,0.1)"
                        :border "1px solid rgba(0,255,159,0.3)"
                        :border-radius "4px 4px 0 0"
                        :padding "4px 8px"
                        :font-size "10px"
                        :font-family "monospace"
                        :color "#00ff9f"
                        :display "flex"
                        :align-items "center"
                        :gap "5px"
                        :cursor "pointer"
                        :transition "all 0.2s"}
                :on-mouse-over #(set! (.. % -target -style -background) "rgba(0,255,159,0.2)")
                :on-mouse-out #(set! (.. % -target -style -background) "rgba(0,255,159,0.1)")
                :on-click (fn [e]
                           (.stopPropagation e)
                           ;; Reset to NOW - re-execute query without time travel
                           (let [current-sql @local-sql]
                             (rq/execute-block-query! id current-sql nil nil)
                             ;; Reset time travel slider to NOW position
                             (tt/reset-time-travel! id current-sql)))}
          [:span "⏰"]
          [:span "TIME TRAVEL MODE - Click to return to NOW"]])
       [:div {:style {:border (if (and executed-sql (not= executed-sql sql))
                               "1px solid rgba(0,255,159,0.5)"
                               "1px solid rgba(0,255,159,0.3)")
                      :border-radius (if (and executed-sql (not= executed-sql sql))
                                       "0 0 4px 4px"
                                       "4px")
                      :overflow "hidden"
                      :background (when (and executed-sql (not= executed-sql sql))
                                    "rgba(0,255,159,0.05)")}}
        [monaco/sql-editor 
         {:value (or executed-sql @local-sql "SELECT * FROM sales")
          :on-change #(reset! local-sql %)  ;; Only update local state while typing
          :height "100px"
          :theme "vs-dark"
          :options (when executed-sql
                     {:readOnly false  ;; Keep editable but show visual indicator
                      :lineDecorationsWidth 10
                      :minimap {:enabled false}})}]]]
      ;; Execute button column
      [:div {:style {:display "flex"
                     :flex-direction "column"
                     :justify-content "flex-end"}}
       [:button
        {:style {:padding "8px 12px"
                 :background "linear-gradient(90deg, #00ff9f 0%, #00cc7f 100%)"
                 :color "#0a0a0a"
                 :border "none"
                 :border-radius "4px"
                 :cursor "pointer"
                 :font-weight "bold"
                 :text-transform "uppercase"
                 :font-size "10px"
                 :letter-spacing "0.5px"
                 :writing-mode "vertical-rl"
                 :text-orientation "mixed"
                 :height "100px"
                 :width "32px"
                 :display "flex"
                 :align-items "center"
                 :justify-content "center"}
         :on-click (fn []
                     ;; Sync local SQL to global state and execute
                     (let [current-sql @local-sql]
                       (r/dispatch! [:update-block id {:sql current-sql}])
                       ;; Use reactive query that auto-updates
                       (rq/execute-block-query! id (or current-sql "SELECT * FROM sales") nil as-of)))}
        "EXECUTE"]]]
     ;; Time scrubber for this query
     [:div {:style {:margin "10px 0"
                    :padding "10px"
                    :background "rgba(0,0,0,0.3)"
                    :border-radius "4px"}}
      ;; Time travel controls
      [tt/time-travel-controls {:block-id id :sql sql}]]
     (when error
       [:div {:style {:margin-top "10px"
                     :padding "10px"
                     :background "rgba(255,0,0,0.1)"
                     :border "1px solid rgba(255,0,0,0.3)"
                     :border-radius "4px"
                     :color "#ff6b6b"
                     :font-family "monospace"
                     :font-size "11px"}}
        error])]  ;; Close fixed-content div
     (when results
       (let [;; Check if this is a single value result (1 row, 1 column)
             is-single-value? (and (= 1 (count results))
                                  (= 1 (count (keys (first results)))))
             single-value (when is-single-value?
                           (let [row (first results)
                                 col-key (first (keys row))
                                 value (get row col-key)]
                             value))
             ;; Format number with commas
             format-number (fn [n]
                            (if (number? n)
                              ;; Use JS Intl.NumberFormat for reliable formatting
                              (.format (js/Intl.NumberFormat. "en-US") n)
                              (str n)))
             ;; Calculate font size based on block dimensions
             ;; Aim for about 1/3 of height, max 120px
             font-size (min 120 (int (/ (:height actual-size) 3)))]
         (if is-single-value?
           ;; Render as large callout text
           [:div.single-value-result
            {:style {:flex 1
                     :display "flex"
                     :align-items "center"
                     :justify-content "center"
                     :margin-top "10px"
                     :background "rgba(0,0,0,0.3)"
                     :border "1px solid rgba(0,255,159,0.2)"
                     :padding "20px"
                     :overflow "hidden"}}
            [:div {:style {:color "#00ff9f"
                          :font-family "'JetBrains Mono', 'Courier New', monospace"
                          :font-weight "bold"
                          :font-size (str font-size "px")
                          :text-align "center"
                          :word-break "break-word"
                          :line-height "1.1"
                          :max-width "100%"}}
             (if (number? single-value)
               (format-number single-value)
               (str single-value))]]
           ;; Render as table for multiple rows/columns
           [:div.results
            {:style {:flex 1
                     :margin-top "10px"
                     :min-height 0  ;; Important for flex children to shrink properly
                     :overflow "auto"
                     :background "rgba(0,0,0,0.3)"
                     :border "1px solid rgba(0,255,159,0.2)"
                     :padding "5px"
                     :display "flex"
                     :flex-direction "column"}}
            [:table {:style {:width "100%" 
                            :font-size "11px"
                            :table-layout "fixed"}}
             [:thead {:style {:position "sticky"
                             :top 0
                             :background "rgba(26,26,46,0.95)"
                             :z-index 1}}
              [:tr
               (for [col (keys (first results))]
                 ^{:key col}
                 [:th {:style {:text-align "left" 
                              :padding "4px"
                              :color "#00ff9f"
                              :border-bottom "1px solid rgba(0,255,159,0.2)"
                              :font-family "monospace"
                              :text-transform "uppercase"
                              :font-size "10px"}} (name col)])]]
             [:tbody
              (for [row results]
                ^{:key (or (:ID row) (:id row) (:xt/id row) (str (hash row)))}
                [:tr
                 (for [col (keys (first results))]
                   ^{:key col}
                   [:td {:style {:padding "4px"
                                :color "#8ff0a4"
                                :font-family "monospace"
                                :font-size "10px"
                                :overflow "hidden"
                                :text-overflow "ellipsis"
                                :white-space "nowrap"}} (str (get row col))])])]]])))]))})))

(defn chart-block [{:keys [id position size source-id chart-config]}]
  (reagent/create-class
   {:component-did-update
    (fn [this [_ old-props]]
      (let [new-props (reagent/props this)
            old-source (:source-id old-props)
            new-source (:source-id new-props)]
        ;; When source changes, trigger query execution if needed
        (when (and new-source (not= old-source new-source))
          (let [source-block @(r/subscribe [:block new-source])]
            (when-let [sql (:sql source-block)]
              (js/console.log "[CHART-BLOCK] New source connected, executing query")
              (rq/execute-block-query! new-source sql nil nil))))))
    
    :reagent-render
    (fn [{:keys [id position size source-id chart-config]}]
      (let [;; Use local position only while dragging, otherwise use state position
            is-dragging? (= id (:block-id @drag-state))
            is-resizing? (= id (:block-id @resize-state))
            actual-pos (if (or is-dragging? (get @local-positions id))
                         (get @local-positions id position)
                         position)
            actual-size (if (or is-resizing? (get @local-sizes id))
                          (get @local-sizes id size)
                          size)
            source-block (when source-id @(r/subscribe [:block source-id]))
            _ (println "SOURCE BLOCK" source-block source-id (rq/get-block-results source-id) (keys @rq/block-results))
            ;; Get results from reactive-queries for the source block - exactly like query block does it!
            source-results (when source-id (rq/get-block-results source-id))
            ;; Extract the data - this should be the same as what the query block shows
            chart-data (:results source-results)
            _ (when source-id
                (js/console.log "[CHART-BLOCK]" id "connected to" source-id 
                               "source-results:" (clj->js source-results)
                               "chart-data:" (clj->js chart-data)))]
    [:div.block.chart-block
     {:style {:position "absolute"
              :left (:x actual-pos)
              :top (:y actual-pos)
              :width (:width actual-size)
              :height (:height actual-size)
              :background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
              :border "1px solid #ff006e"
              :border-radius "4px"
              :padding "10px"
              :z-index 10
              :box-shadow "0 0 20px rgba(255,0,110,0.3), inset 0 0 20px rgba(255,0,110,0.05)"
              :display "flex"
              :flex-direction "column"}
      :draggable false}  ;; Remove the on-mouse-down from here
     ;; Resize handle
     [:div.resize-handle
      {:style {:position "absolute"
               :bottom 0
               :right 0
               :width "15px"
               :height "15px"
               :cursor "nwse-resize"
               :background "linear-gradient(135deg, transparent 50%, #ff006e 50%)"
               :opacity 0.5}
       :on-mouse-down #(start-resize! id %)}]
     [:div.block-header
      {:style {:display "flex"
               :justify-content "space-between"
               :margin-bottom "10px"
               :padding-bottom "5px"
               :border-bottom "1px solid rgba(255,0,110,0.2)"
               :cursor "move"}  ;; Add cursor: move to the header
       :on-mouse-down #(start-drag! id %)}
      [:span {:style {:font-weight "bold" 
                      :color "#ff006e"
                      :font-family "monospace"
                      :text-transform "uppercase"
                      :font-size "11px"
                      :letter-spacing "1px"}} "CHART"]
      [:button {:on-click #(do (rq/unsubscribe-block! id)
                              (r/dispatch! [:delete-block id]))
                :style {:background "none"
                        :border "none"
                        :color "#ff006e"
                        :cursor "pointer"
                        :font-size "20px"
                        :line-height "20px"}} "×"]]
     ;; Source selector
     [:div {:style {:margin "10px 0"}}
      [:div {:style {:display "flex" :align-items "center" :gap "10px"}}
       [:label {:style {:color "#ff006e"
                        :font-family "monospace"
                        :font-size "10px"
                        :text-transform "uppercase"}}
        "Source:"]
       (if source-id
         [:span {:style {:color "#ff4f99"
                        :font-family "monospace"
                        :font-size "11px"}} 
          (str "#" source-id)]
         [:span {:style {:color "#ff4f99"
                        :font-family "monospace"
                        :font-size "11px"
                        :opacity 0.5}} 
          "Not connected"])]
      [:button {:style {:width "100%"
                       :margin-top "5px"
                       :padding "5px"
                       :background (if @connection-mode 
                                    "linear-gradient(90deg, #ff006e 0%, #ff4f99 100%)"
                                    "transparent")
                       :color (if @connection-mode "#0a0a0a" "#ff006e")
                       :border "1px solid #ff006e"
                       :border-radius "2px"
                       :cursor "pointer"
                       :font-family "monospace"
                       :font-size "10px"
                       :text-transform "uppercase"
                       :letter-spacing "1px"}
               :on-click (fn [e]
                          (.stopPropagation ^js e)
                          (if @connection-mode
                            (reset! connection-mode nil)
                            (reset! connection-mode {:source-id id})))}
       (if @connection-mode
         "Cancel Connection"
         "Connect to Query")]]
     ;; Chart visualization using Vega-Lite
     [:div {:style {:height "calc(100% - 100px)"
                    :overflow "auto"}}
      [vega/vega-chart-component 
       {:id id
        :data chart-data
        :config chart-config
        :block-size actual-size
        :on-config-change (fn [new-config]
                            (r/dispatch! [:update-block id {:chart-config new-config}]))}]]]))}))

(defn sql-exec-block [{:keys [id position size sql error result] :as block}]
  (let [;; Local state for SQL (only syncs to app state on execute)
        local-sql (reagent/atom (or sql "INSERT INTO sales (product, amount, quantity, sale_date) VALUES ('TestProduct', 500, 2, '2024-01-10')"))]
    (reagent/create-class
     {:component-did-update
      (fn [this [_ old-props]]
        (let [new-props (reagent/props this)]
          ;; Update local SQL if props change from external source
          (when (and (:sql new-props)
                    (not= (:sql old-props) (:sql new-props)))
            (reset! local-sql (:sql new-props)))))
      
      :reagent-render
      (fn [{:keys [id position size sql error result] :as block}]
        (let [;; Use local position only while dragging, otherwise use state position
              is-dragging? (= id (:block-id @drag-state))
              is-resizing? (= id (:block-id @resize-state))
              actual-pos (if (or is-dragging? (get @local-positions id))
                           (get @local-positions id position)
                           position)
              actual-size (if (or is-resizing? (get @local-sizes id))
                            (get @local-sizes id size)
                            size)]
          [:div.block.sql-exec-block
           {:style {:position "absolute"
                    :left (:x actual-pos)
                    :top (:y actual-pos)
                    :width (:width actual-size)
                    :height (:height actual-size)
                    :background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
                    :border "1px solid #ffb700"
                    :border-radius "4px"
                    :padding "10px"
                    :z-index 10
                    :box-shadow "0 0 20px rgba(255,183,0,0.3), inset 0 0 20px rgba(255,183,0,0.05)"
                    :display "flex"
                    :flex-direction "column"}
            :draggable false}
           ;; Resize handle
           [:div.resize-handle
            {:style {:position "absolute"
                     :bottom 0
                     :right 0
                     :width "15px"
                     :height "15px"
                     :cursor "nwse-resize"
                     :background "linear-gradient(135deg, transparent 50%, #ffb700 50%)"
                     :opacity 0.5}
             :on-mouse-down #(start-resize! id %)}]
           [:div.block-header
            {:style {:display "flex"
                     :justify-content "space-between"
                     :margin-bottom "10px"
                     :padding-bottom "5px"
                     :border-bottom "1px solid rgba(255,183,0,0.2)"
                     :cursor "move"}
             :on-mouse-down #(start-drag! id %)}
            [:span {:style {:font-weight "bold" 
                            :color "#ffb700"
                            :font-family "monospace"
                            :text-transform "uppercase"
                            :font-size "11px"
                            :letter-spacing "1px"}} "SQL EXECUTE"]
            [:button {:on-click #(do (rq/unsubscribe-block! id)
                                    (r/dispatch! [:delete-block id]))
                      :style {:background "none"
                              :border "none"
                              :color "#ffb700"
                              :cursor "pointer"
                              :font-size "20px"
                              :line-height "20px"}} "×"]]
           ;; SQL Editor - now using local state
           [:div {:style {:margin "10px 0"
                          :border "1px solid rgba(255,183,0,0.3)"
                          :border-radius "4px"
                          :overflow "hidden"}}
            [monaco/sql-editor
             {:value @local-sql
              :on-change #(reset! local-sql %)  ; Only update local state on typing
              :height "120px"
              :theme "vs-dark"}]]
           [:button
            {:style {:width "100%"
                     :margin-top "5px"
                     :padding "5px 10px"
                     :background "linear-gradient(90deg, #ffb700 0%, #ff8c00 100%)"
                     :color "#0a0a0a"
                     :border "none"
                     :border-radius "2px"
                     :cursor "pointer"
                     :font-weight "bold"
                     :text-transform "uppercase"
                     :font-size "11px"
                     :letter-spacing "1px"}
             :on-click (fn []
                        ;; Sync local SQL to global state on execute
                        (let [current-sql @local-sql]
                          (r/dispatch! [:update-block id {:sql current-sql}])
                          (-> (r/sql-exec! current-sql)
                              (.then (fn [response]
                                      (if (:error response)
                                        (r/dispatch! [:update-block id {:error (:error response) :result nil}])
                                        (r/dispatch! [:update-block id {:result (:result response) :error nil}])))))))}
            "Execute"]
           ;; Error display
           (when error
             [:div {:style {:margin-top "10px"
                           :padding "10px"
                           :background "rgba(255,0,0,0.1)"
                           :border "1px solid rgba(255,0,0,0.3)"
                           :border-radius "4px"
                           :color "#ff6b6b"
                           :font-family "monospace"
                           :font-size "11px"}}
              error])
           ;; Success result display
           (when result
             [:div {:style {:margin-top "10px"
                           :padding "10px"
                           :background "rgba(255,183,0,0.1)"
                           :border "1px solid rgba(255,183,0,0.3)"
                           :border-radius "4px"
                           :color "#ffb700"
                           :font-family "monospace"
                           :font-size "11px"}}
              (str "Success: " result)])]))})))  ; Close reagent-render and create-class

(defn debug-block [{:keys [id position size debug-mode] :as block}]
  (let [;; Local state for the debug block
        local-mode (reagent/atom (or debug-mode :subscriptions))
        ;; Use local position only while dragging, otherwise use state position
        is-dragging? (= id (:block-id @drag-state))
        is-resizing? (= id (:block-id @resize-state))
        actual-pos (if (or is-dragging? (get @local-positions id))
                     (get @local-positions id position)
                     position)
        actual-size (if (or is-resizing? (get @local-sizes id))
                      (get @local-sizes id size)
                      size)
        ;; Get debug data reactively
        debug-results (rq/get-block-results id)]
    (reagent/create-class
     {:component-did-mount
      (fn []
        ;; Execute initial query based on mode
        (case (or debug-mode :subscriptions)
          :subscriptions 
          (rq/execute-block-query! id 
            "SELECT sub_id, session_id, status, update_count, 
                    last_updated, total_execution_time
             FROM reactor_subscriptions 
             WHERE status = 'active'
             LIMIT 100" nil nil)
          :events
          (rq/execute-block-query! id
            "SELECT category, event_type, session_id, created_at
             FROM reactor_events
             LIMIT 100" nil nil)
          :reactions
          (rq/execute-block-query! id
            "SELECT table_name, change_type, affected_subscriptions, triggered_at
             FROM reactor_reactions
             LIMIT 50" nil nil)
          :performance
          (rq/execute-block-query! id
            "SELECT metric_type, query, execution_time_ms, result_count, measured_at
             FROM reactor_performance
             LIMIT 100" nil nil)
          nil))
      
      :component-will-unmount
      (fn []
        (rq/unsubscribe-block! id))
      
      :reagent-render
      (fn [{:keys [id position size debug-mode] :as block}]
        (let [is-dragging? (= id (:block-id @drag-state))
              is-resizing? (= id (:block-id @resize-state))
              actual-pos (if (or is-dragging? (get @local-positions id))
                           (get @local-positions id position)
                           position)
              actual-size (if (or is-resizing? (get @local-sizes id))
                            (get @local-sizes id size)
                            size)
              debug-results (rq/get-block-results id)
              {:keys [results error loading]} debug-results]
          [:div.block.debug-block
           {:style {:position "absolute"
                    :left (:x actual-pos)
                    :top (:y actual-pos)
                    :width (:width actual-size)
                    :height (:height actual-size)
                    :background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
                    :border "1px solid #9b59b6"
                    :border-radius "4px"
                    :padding "10px"
                    :z-index 10
                    :box-shadow "0 0 20px rgba(155,89,182,0.3), inset 0 0 20px rgba(155,89,182,0.05)"
                    :display "flex"
                    :flex-direction "column"}
            :draggable false}
           ;; Resize handle
           [:div.resize-handle
            {:style {:position "absolute"
                     :bottom 0
                     :right 0
                     :width "15px"
                     :height "15px"
                     :cursor "nwse-resize"
                     :background "linear-gradient(135deg, transparent 50%, #9b59b6 50%)"
                     :opacity 0.5}
             :on-mouse-down #(start-resize! id %)}]
           ;; Header
           [:div.block-header
            {:style {:display "flex"
                     :justify-content "space-between"
                     :margin-bottom "10px"
                     :padding-bottom "5px"
                     :border-bottom "1px solid rgba(155,89,182,0.2)"
                     :cursor "move"}
             :on-mouse-down #(start-drag! id %)}
            [:div {:style {:display "flex" :align-items "center" :gap "10px"}}
             [:span {:style {:font-weight "bold" 
                             :color "#9b59b6"
                             :font-family "monospace"
                             :text-transform "uppercase"
                             :font-size "11px"
                             :letter-spacing "1px"}} "REACTOR DEBUG"]
             [:select {:style {:background "rgba(0,0,0,0.3)"
                               :color "#9b59b6"
                               :border "1px solid rgba(155,89,182,0.3)"
                               :border-radius "2px"
                               :padding "2px 5px"
                               :font-family "monospace"
                               :font-size "10px"
                               :cursor "pointer"}
                       :value (name @local-mode)
                       :on-mouse-down (fn [e] (.stopPropagation e))
                       :on-change (fn [e]
                                   (let [new-mode (keyword (.. e -target -value))]
                                     (reset! local-mode new-mode)
                                     (case new-mode
                                       :subscriptions 
                                       (rq/execute-block-query! id 
                                         "SELECT sub_id, session_id, status, update_count, 
                                                 last_updated, total_execution_time
                                          FROM reactor_subscriptions 
                                          WHERE status = 'active'
                                          LIMIT 100" nil nil)
                                       :events
                                       (rq/execute-block-query! id
                                         "SELECT category, event_type, session_id, created_at
                                          FROM reactor_events
                                          LIMIT 100" nil nil)
                                       :reactions
                                       (rq/execute-block-query! id
                                         "SELECT table_name, change_type, affected_subscriptions, triggered_at
                                          FROM reactor_reactions
                                          LIMIT 50" nil nil)
                                       :performance
                                       (rq/execute-block-query! id
                                         "SELECT metric_type, query, execution_time_ms, result_count, measured_at
                                          FROM reactor_performance
                                          LIMIT 100" nil nil)
                                       nil)))}
              [:option {:value "subscriptions"} "Subscriptions"]
              [:option {:value "events"} "Events"]
              [:option {:value "reactions"} "Reactions"]
              [:option {:value "performance"} "Performance"]]]
            [:button {:on-click #(do (rq/unsubscribe-block! id)
                                    (r/dispatch! [:delete-block id]))
                      :style {:background "none"
                              :border "none"
                              :color "#9b59b6"
                              :cursor "pointer"
                              :font-size "20px"
                              :line-height "20px"}} "×"]]
           ;; Results
           (cond
             loading
             [:div {:style {:flex 1
                           :display "flex"
                           :align-items "center"
                           :justify-content "center"
                           :color "#9b59b6"
                           :font-family "monospace"}}
              "Loading..."]
             
             error
             [:div {:style {:flex 1
                           :padding "10px"
                           :background "rgba(255,0,0,0.1)"
                           :border "1px solid rgba(255,0,0,0.3)"
                           :border-radius "4px"
                           :color "#ff6b6b"
                           :font-family "monospace"
                           :font-size "11px"}}
              error]
             
             (and results (seq results))
             [:div.results
              {:style {:flex 1
                       :min-height 0
                       :overflow "auto"
                       :background "rgba(0,0,0,0.3)"
                       :border "1px solid rgba(155,89,182,0.2)"
                       :padding "5px"}}
              [:table {:style {:width "100%" 
                              :font-size "10px"
                              :table-layout "fixed"}}
               [:thead {:style {:position "sticky"
                               :top 0
                               :background "rgba(26,26,46,0.95)"
                               :z-index 1}}
                [:tr
                 (for [col (keys (first results))]
                   ^{:key col}
                   [:th {:style {:text-align "left" 
                                :padding "4px"
                                :color "#9b59b6"
                                :border-bottom "1px solid rgba(155,89,182,0.2)"
                                :font-family "monospace"
                                :text-transform "uppercase"
                                :font-size "9px"}} (name col)])]]
               [:tbody
                (for [row results]
                  ^{:key (or (:_id row) (str (hash row)))}
                  [:tr
                   (for [col (keys (first results))]
                     ^{:key col}
                     [:td {:style {:padding "4px"
                                  :color "#d8b4fe"
                                  :font-family "monospace"
                                  :font-size "9px"
                                  :overflow "hidden"
                                  :text-overflow "ellipsis"
                                  :white-space "nowrap"}} 
                      (str (get row col))])])]]]
             
             results  ;; Empty results array
             [:div {:style {:flex 1
                           :display "flex"
                           :align-items "center"
                           :justify-content "center"
                           :color "#9b59b6"
                           :font-family "monospace"
                           :opacity 0.5
                           :flex-direction "column"
                           :gap "10px"}}
              [:div {:style {:font-size "12px"}} "No data yet"]
              [:div {:style {:font-size "10px"
                            :text-align "center"
                            :max-width "300px"
                            :line-height "1.4"}} 
               (case @local-mode
                 :subscriptions "No active subscriptions being tracked"
                 :events "No events recorded yet" 
                 :reactions "No table reactions captured"
                 :performance "No performance metrics available"
                 "No data available")]]
             
             :else
             [:div {:style {:flex 1
                           :display "flex"
                           :align-items "center"
                           :justify-content "center"
                           :color "#9b59b6"
                           :font-family "monospace"
                           :opacity 0.5}}
              "Loading..."])]))})))

(defn edn-browser-block [{:keys [id position size source-id field-index row-index view-mode] :as block}]
  (let [;; Local state for view mode and search
        local-view-mode (reagent/atom (or view-mode :monaco))
        search-term (reagent/atom "")]
    (fn [{:keys [id position size source-id field-index row-index view-mode] :as block}]
      (let [;; Use local position only while dragging, otherwise use state position
            is-dragging? (= id (:block-id @drag-state))
            is-resizing? (= id (:block-id @resize-state))
            actual-pos (if (or is-dragging? (get @local-positions id))
                         (get @local-positions id position)
                         position)
            actual-size (if (or is-resizing? (get @local-sizes id))
                          (get @local-sizes id size)
                          size)
            ;; Use the values from the block state, defaulting to 0
            current-field-index (or field-index 0)
            current-row-index (or row-index 0)
            ;; Calculate available height for tree view
            ;; Block height - header(~35px) - controls(~35px) - view toggle(~35px) - padding(20px) - border(2px)
            tree-height (- (:height actual-size) 127)]
        [:div.block.edn-browser-block
     {:style {:position "absolute"
              :left (:x actual-pos)
              :top (:y actual-pos)
              :width (:width actual-size)
              :height (:height actual-size)
              :background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
              :border (if (and @connection-mode (= (:source-id @connection-mode) id))
                       "2px solid #00ffd4"
                       "1px solid #00ffd4")
              :border-radius "4px"
              :padding "10px"
              :z-index 10
              :box-shadow (if @connection-mode
                           "0 0 30px rgba(0,255,212,0.5), inset 0 0 20px rgba(0,255,212,0.1)"
                           "0 0 20px rgba(0,255,212,0.3), inset 0 0 20px rgba(0,255,212,0.05)")
              :display "flex"
              :flex-direction "column"}
      :draggable false}
     ;; Resize handle
     [:div.resize-handle
      {:style {:position "absolute"
               :bottom 0
               :right 0
               :width "15px"
               :height "15px"
               :cursor "nwse-resize"
               :background "linear-gradient(135deg, transparent 50%, #00ffd4 50%)"
               :opacity 0.5}
       :on-mouse-down #(start-resize! id %)}]
     ;; Header
     [:div.block-header
      {:style {:display "flex"
               :justify-content "space-between"
               :margin-bottom "10px"
               :padding-bottom "5px"
               :border-bottom "1px solid rgba(0,255,212,0.2)"
               :cursor (if @connection-mode "pointer" "move")}
       :on-mouse-down (fn [e]
                        (when-not @connection-mode
                          (start-drag! id e)))
       :on-click (fn [e]
                   (when-let [conn @connection-mode]
                     (.stopPropagation ^js e)
                     ;; Update the EDN browser block to link to this query block
                     (r/dispatch! [:update-block (:source-id conn) {:source-id id}])
                     (reset! connection-mode nil)))}
      [:span {:style {:color "#00ffd4"
                      :font-family "monospace"
                      :font-size "12px"
                      :text-transform "uppercase"
                      :letter-spacing "1px"}}
       "EDN BROWSER"]
      [:button {:style {:padding "2px 6px"
                        :background "transparent"
                        :color "#00ffd4"
                        :border "1px solid #00ffd4"
                        :border-radius "2px"
                        :cursor "pointer"
                        :font-size "10px"
                        :font-family "monospace"}
                :on-click #(r/dispatch! [:remove-block id])}
       "×"]]
     ;; Connection status
     (if source-id
       (let [source-results (when source-id (rq/get-block-results source-id))
             data (:results source-results)
             fields (when (seq data) (keys (first data)))
             selected-field (when fields (nth (vec fields) current-field-index nil))
             selected-row (when data (nth data current-row-index nil))
             raw-value (when selected-row (get selected-row selected-field))
             ;; Try to parse EDN if it's a string
             selected-value (if (string? raw-value)
                             (try
                               ;; Attempt to read the string as EDN
                               (let [parsed (reader/read-string raw-value)]
                                 ;; Check if the raw value looks like it was meant to be EDN
                                 ;; by checking for common EDN patterns
                                 (if (or (map? parsed) 
                                        (vector? parsed) 
                                        (list? parsed) 
                                        (set? parsed)
                                        ;; Check for EDN-specific syntax in the original string
                                        (re-find #"^[\[\{\(:#]" raw-value)
                                        ;; Also parse if it's a quoted string that became a string
                                        (and (string? parsed)
                                             (str/starts-with? raw-value "\"")))
                                   parsed
                                   raw-value))
                               (catch :default _
                                 ;; If parsing fails, just use the raw string
                                 raw-value))
                             ;; Non-strings pass through as-is
                             raw-value)]
         [:div {:style {:flex 1
                        :display "flex"
                        :flex-direction "column"
                        :gap "10px"}}
          ;; Controls
          [:div {:style {:display "flex"
                         :gap "10px"
                         :align-items "center"
                         :margin-bottom "5px"}}
           ;; Field selector
           [:div {:style {:display "flex"
                          :align-items "center"
                          :gap "5px"}}
            [:span {:style {:color "#00ffd4"
                            :font-family "monospace"
                            :font-size "10px"
                            :opacity 0.7}}
             "FIELD:"]
            [:select {:style {:background "rgba(0,255,212,0.1)"
                              :color "#00ffd4"
                              :border "1px solid rgba(0,255,212,0.3)"
                              :border-radius "2px"
                              :padding "2px 5px"
                              :font-family "monospace"
                              :font-size "10px"
                              :cursor "pointer"}
                      :value current-field-index
                      :on-change (fn [e]
                                   (let [new-index (js/parseInt (.. e -target -value))]
                                     (r/dispatch! [:update-block id {:field-index new-index}])))}
             (map-indexed (fn [idx field]
                           ^{:key field}
                           [:option {:value idx} (name field)])
                         fields)]]
           ;; Row selector
           [:div {:style {:display "flex"
                          :align-items "center"
                          :gap "5px"}}
            [:span {:style {:color "#00ffd4"
                            :font-family "monospace"
                            :font-size "10px"
                            :opacity 0.7}}
             "ROW:"]
            [:select {:style {:background "rgba(0,255,212,0.1)"
                              :color "#00ffd4"
                              :border "1px solid rgba(0,255,212,0.3)"
                              :border-radius "2px"
                              :padding "2px 5px"
                              :font-family "monospace"
                              :font-size "10px"
                              :cursor "pointer"}
                      :value current-row-index
                      :on-change (fn [e]
                                   (let [new-index (js/parseInt (.. e -target -value))]
                                     (r/dispatch! [:update-block id {:row-index new-index}])))}
             (map-indexed (fn [idx _]
                           ^{:key idx}
                           [:option {:value idx} (str "Row " idx)])
                         data)]]]
          ;; View mode toggle
          [:div {:style {:display "flex"
                         :gap "5px"
                         :margin-bottom "5px"}}
           [:button {:style {:padding "3px 8px"
                             :background (if (= @local-view-mode :monaco)
                                          "rgba(0,255,212,0.2)"
                                          "rgba(0,255,212,0.05)")
                             :color "#00ffd4"
                             :border "1px solid rgba(0,255,212,0.3)"
                             :border-radius "2px 0 0 2px"
                             :cursor "pointer"
                             :font-size "9px"
                             :font-family "monospace"}
                     :on-click #(reset! local-view-mode :monaco)}
            "CODE"]
           [:button {:style {:padding "3px 8px"
                             :background (if (= @local-view-mode :tree)
                                          "rgba(0,255,212,0.2)"
                                          "rgba(0,255,212,0.05)")
                             :color "#00ffd4"
                             :border "1px solid rgba(0,255,212,0.3)"
                             :border-radius "0 2px 2px 0"
                             :cursor "pointer"
                             :font-size "9px"
                             :font-family "monospace"
                             :margin-left "-1px"}
                     :on-click #(reset! local-view-mode :tree)}
            "TREE"]
           (when (= @local-view-mode :tree)
             [:input {:type "text"
                      :placeholder "Search..."
                      :value @search-term
                      :style {:flex 1
                              :margin-left "10px"
                              :background "rgba(0,255,212,0.05)"
                              :color "#00ffd4"
                              :border "1px solid rgba(0,255,212,0.2)"
                              :border-radius "2px"
                              :padding "2px 5px"
                              :font-family "monospace"
                              :font-size "9px"
                              :outline "none"}
                      :on-change #(reset! search-term (.. % -target -value))}])]
          ;; EDN Display
          [:div {:style {:height (str tree-height "px")
                         :overflow "hidden"
                         :border "1px solid rgba(0,255,212,0.2)"
                         :border-radius "2px"
                         :position "relative"}}
           [:style 
            ".edn-tree-container::-webkit-scrollbar { width: 8px; height: 8px; }
             .edn-tree-container::-webkit-scrollbar-track { background: transparent; }
             .edn-tree-container::-webkit-scrollbar-thumb { background: rgba(0,255,212,0.3); border-radius: 4px; }
             .edn-tree-container::-webkit-scrollbar-thumb:hover { background: rgba(0,255,212,0.5); }"]
           (cond
             ;; No data
             (nil? selected-value)
             [:div {:style {:display "flex"
                            :align-items "center"
                            :justify-content "center"
                            :height "100%"
                            :color "#00ffd4"
                            :font-family "monospace"
                            :opacity 0.5}}
              "No data"]
             
             ;; Tree view mode
             (= @local-view-mode :tree)
             [:div {:style {:height "100%"
                            :overflow "hidden"}}
              [tree/edn-tree-view 
               {:data selected-value
                :search-term @search-term
                :initial-depth 3
                :on-select (fn [path value]
                            (js/console.log "Selected path:" (clj->js path) "value:" (clj->js value)))}]]
             
             ;; Monaco view mode (default)
             :else
             [monaco/edn-editor 
              {:value (with-out-str (pprint/pprint selected-value))
               :height (str tree-height "px")
               :read-only? true
               :theme "rabbit-theme"}])]])
       ;; Not connected
       [:div {:style {:flex 1
                      :display "flex"
                      :flex-direction "column"
                      :align-items "center"
                      :justify-content "center"
                      :gap "10px"}}
        [:div {:style {:color "#00ffd4"
                       :font-family "monospace"
                       :font-size "11px"
                       :opacity 0.7}}
         "Not connected to a query block"]
        [:button {:style {:padding "6px 12px"
                          :background "transparent"
                          :color "#00ffd4"
                          :border "1px solid #00ffd4"
                          :border-radius "2px"
                          :cursor "pointer"
                          :font-family "monospace"
                          :font-size "10px"}
                  :on-click (fn []
                              (reset! connection-mode {:source-id id}))}
         "CONNECT TO QUERY"]])]))))

(defn render-block [block]
  (js/console.log "Rendering block:" (clj->js block) "Type:" (:type block))
  (let [block-type (if (string? (:type block))
                     (keyword (:type block))
                     (:type block))]
    (case block-type
      :query [query-block block]
      :chart [chart-block block]
      :sql-exec [sql-exec-block block]
      :debug [debug-block block]
      :edn-browser [edn-browser-block block]
      :tap (let [;; Use local position only while dragging
                 is-dragging? (= (:id block) (:block-id @drag-state))
                 is-resizing? (= (:id block) (:block-id @resize-state))
                 actual-pos (if (or is-dragging? (get @local-positions (:id block)))
                              (get @local-positions (:id block) (:position block))
                              (:position block))
                 actual-size (if (or is-resizing? (get @local-sizes (:id block)))
                               (get @local-sizes (:id block) (:size block))
                               (:size block))]
             [:div.block
              {:style {:position "absolute"
                       :left (:x actual-pos)
                       :top (:y actual-pos)
                       :width (:width actual-size)
                       :height (:height actual-size)
                       :background "linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 100%)"
                       :border "1px solid #ff4f99"
                       :border-radius "4px"
                       :box-shadow "0 4px 20px rgba(255,79,153,0.3)"
                       :display "flex"
                       :flex-direction "column"
                       :transition (when-not (or is-dragging? is-resizing?)
                                    "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)")}}
              ;; Header
              [:div.block-header
               {:style {:padding "10px"
                        :background "rgba(255,79,153,0.1)"
                        :border-bottom "1px solid rgba(255,79,153,0.3)"
                        :cursor "move"
                        :display "flex"
                        :justify-content "space-between"
                        :align-items "center"}
                :on-mouse-down #(start-drag! (:id block) %)}
               [:span {:style {:color "#ff4f99"
                               :font-family "monospace"
                               :text-transform "uppercase"
                               :font-size "11px"
                               :letter-spacing "1px"}} "TAP"]
               [:button {:on-click #(r/dispatch! [:delete-block (:id block)])
                         :style {:background "transparent"
                                 :border "none"
                                 :color "#ff4f99"
                                 :cursor "pointer"
                                 :font-size "16px"
                                 :padding "0 5px"}}
                "×"]]
              ;; Content
              [:div {:style {:flex 1
                             :overflow "hidden"
                             :display "flex"}}
               [sql-tap-block/sql-tap-block block]]
              ;; Resize handle
              [:div.resize-handle
               {:style {:position "absolute"
                        :bottom 0
                        :right 0
                        :width "15px"
                        :height "15px"
                        :cursor "nwse-resize"
                        :background "radial-gradient(circle at center, rgba(255,79,153,0.5) 0%, transparent 70%)"}
                :on-mouse-down #(start-resize! (:id block) %)}]])
      :rule-flow (let [;; Use local position only while dragging
                       is-dragging? (= (:id block) (:block-id @drag-state))
                       is-resizing? (= (:id block) (:block-id @resize-state))
                       actual-pos (if (or is-dragging? (get @local-positions (:id block)))
                                    (get @local-positions (:id block) (:position block))
                                    (:position block))
                       actual-size (if (or is-resizing? (get @local-sizes (:id block)))
                                     (get @local-sizes (:id block) (:size block))
                                     (:size block))]
                   [:div.block
                    {:style {:position "absolute"
                             :left (:x actual-pos)
                             :top (:y actual-pos)
                             :width (:width actual-size)
                             :height (:height actual-size)
                             :background "linear-gradient(135deg, #0a0a0a 0%, #1a2e1a 100%)"
                             :border "1px solid #00ff9f"
                             :border-radius "4px"
                             :box-shadow "0 4px 20px rgba(0,255,159,0.3)"
                             :display "flex"
                             :flex-direction "column"
                             :transition (when-not (or is-dragging? is-resizing?)
                                          "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)")}}
                    ;; Header
                    [:div.block-header
                     {:style {:padding "10px"
                              :background "rgba(0,255,159,0.1)"
                              :border-bottom "1px solid rgba(0,255,159,0.3)"
                              :cursor "move"
                              :display "flex"
                              :justify-content "space-between"
                              :align-items "center"}
                      :on-mouse-down #(start-drag! (:id block) %)}
                     [:span {:style {:color "#00ff9f"
                                     :font-family "monospace"
                                     :text-transform "uppercase"
                                     :font-size "11px"
                                     :letter-spacing "1px"}} "RULES"]
                     [:button {:on-click #(r/dispatch! [:delete-block (:id block)])
                               :style {:background "transparent"
                                       :border "none"
                                       :color "#00ff9f"
                                       :cursor "pointer"
                                       :font-size "16px"
                                       :padding "0 5px"}}
                      "×"]]
                    ;; Content
                    [:div {:style {:flex 1
                                   :overflow "hidden"
                                   :display "flex"}}
                     [rule-flow-block/rule-flow-block block]]
                    ;; Resize handle
                    [:div.resize-handle
                     {:style {:position "absolute"
                              :bottom 0
                              :right 0
                              :width "15px"
                              :height "15px"
                              :cursor "nwse-resize"
                              :background "radial-gradient(circle at center, rgba(0,255,159,0.5) 0%, transparent 70%)"}
                      :on-mouse-down #(start-resize! (:id block) %)}]])
      (do
        (js/console.warn "Unknown block type:" block-type "original:" (:type block))
        nil))))

;; ============= Canvas Component =============

(defonce table-dropdown-open (reagent/atom false))
(defonce table-list (reagent/atom {:public [] :system [] :reactor []}))

(defn canvas []
  (let [blocks (r/subscribe [:blocks])]
    (fn []
      (js/console.log "Canvas rendering, blocks:" (clj->js @blocks))
      [:div#canvas
       {:style {:position "relative"
                :width "100%"
                :height "calc(100vh - 120px)"
                :background "radial-gradient(circle at 20% 50%, #1a1a2e 0%, #0a0a0a 100%)"
                :overflow "auto"
                :box-shadow "inset 0 0 100px rgba(0,0,0,0.5)"
                :cursor (when @connection-mode "crosshair")}
        :on-click (fn []
                   ;; Close dropdown when clicking on canvas
                   (reset! table-dropdown-open false))
        :on-mouse-move (fn [e]
                        (handle-drag! e)
                        (handle-resize! e))
        :on-mouse-up (fn []
                      (stop-drag!)
                      (stop-resize!))
        :on-mouse-leave (fn []
                         (stop-drag!)
                         (stop-resize!))}
       ;; Grid overlay effect
       [:div {:style {:position "absolute"
                     :width "100%"
                     :height "100%"
                     :background-image "linear-gradient(rgba(0,255,159,0.03) 1px, transparent 1px), linear-gradient(90deg, rgba(0,255,159,0.03) 1px, transparent 1px)"
                     :background-size "50px 50px"
                     :pointer-events "none"}}]
       ;; Connection lines SVG
       (let [local-pos @local-positions
             local-sz @local-sizes
             all-blocks @blocks
             ;; Build lines outside the SVG element - filter out nils immediately
             lines (->> (for [[block-id block] all-blocks
                              :when (:source-id block)]
                          (let [source-id (:source-id block)
                                ;; Try both string and keyword lookup
                                source-block (or (get all-blocks source-id)
                                               (get all-blocks (keyword source-id))
                                               (get all-blocks (name source-id)))
                                ;; Chart block (block) connects TO query block (source-block)
                                ;; So line goes FROM query block TO chart block
                                source-pos (or (get local-pos source-id) 
                                             (get local-pos (keyword source-id))
                                             (get local-pos (name source-id))
                                             (:position source-block))
                                target-pos (or (get local-pos block-id)
                                             (get local-pos (keyword block-id))
                                             (get local-pos (name block-id))
                                             (:position block))
                                source-size (or (get local-sz source-id) 
                                              (get local-sz (keyword source-id))
                                              (get local-sz (name source-id))
                                              (:size source-block) 
                                              {:width 400 :height 300})
                                target-size (or (get local-sz block-id)
                                              (get local-sz (keyword block-id))
                                              (get local-sz (name block-id))
                                              (:size block) 
                                              {:width 400 :height 300})]
                            (when (and source-pos target-pos)
                              [:line {:key (str "line-" block-id)
                                     :x1 (+ (:x source-pos) (/ (:width source-size) 2))
                                     :y1 (+ (:y source-pos) (/ (:height source-size) 2))
                                     :x2 (+ (:x target-pos) (/ (:width target-size) 2))
                                     :y2 (+ (:y target-pos) (/ (:height target-size) 2))
                                     :stroke "#00ff9f"
                                     :stroke-width "3"
                                     :opacity 1}])))
                        (remove nil?)
                        vec)]
         [:svg {:style {:position "absolute"
                       :top 0
                       :left 0
                       :width "100%"
                       :height "100%"
                       :pointer-events "none"
                       :z-index 5}
                :id "connection-svg"}
          ;; Add the connection lines
          (for [line lines]
            line)])
       ;; Connection mode indicator
       (when @connection-mode
         [:div {:style {:position "fixed"
                       :top "80px"
                       :left "50%"
                       :transform "translateX(-50%)"
                       :background "linear-gradient(90deg, #ff006e 0%, #ff4f99 100%)"
                       :color "#0a0a0a"
                       :padding "10px 20px"
                       :border-radius "4px"
                       :font-family "monospace"
                       :font-size "12px"
                       :font-weight "bold"
                       :text-transform "uppercase"
                       :letter-spacing "1px"
                       :z-index 1000
                       :box-shadow "0 0 30px rgba(255,0,110,0.5)"}}
          "Click on a Query Block header to connect"])
       (if (empty? @blocks)
         [:div {:style {:color "#00ff9f"
                       :font-family "monospace"
                       :position "absolute"
                       :top "50%"
                       :left "50%"
                       :transform "translate(-50%, -50%)"
                       :opacity 0.3}}
          "Click buttons above to add blocks"]
         (for [[id block] @blocks]
           ^{:key id}
           [render-block (assoc block :id id)]))
       ])))

;; ============= Toolbar Component =============



(defn toolbar []
  [:div.toolbar
   {:style {:height "60px"
            :background "linear-gradient(90deg, #0a0a0a 0%, #1a1a2e 100%)"
            :border-bottom "1px solid rgba(0,255,159,0.2)"
            :display "flex"
            :align-items "center"
            :padding "0 20px"
            :gap "10px"
            :box-shadow "0 2px 20px rgba(0,0,0,0.5)"}}
   [:button
    {:style {:padding "8px 16px"
             :background "transparent"
             :color "#00ff9f"
             :border "1px solid #00ff9f"
             :border-radius "2px"
             :cursor "pointer"
             :font-family "monospace"
             :font-size "12px"
             :text-transform "uppercase"
             :letter-spacing "1px"
             :transition "all 0.3s"}
     :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(0,255,159,0.1)")
     :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
     :on-click (fn []
                 (let [block-data {:id (str (random-uuid))
                                   :type :query
                                   :position {:x 100 :y 100}
                                   :size {:width 400 :height 300}
                                   :sql "SELECT * FROM sales"}]
                   (js/console.log "Adding query block:" (clj->js block-data))
                   (r/dispatch! [:add-block block-data])))}
    "+ QUERY"]
   ;; Table dropdown button
   [:div {:style {:position "relative"}}
    [:button
     {:style {:padding "8px 16px"
              :background "transparent"
              :color "#00ff9f"
              :border "1px solid #00ff9f"
              :border-radius "2px"
              :cursor "pointer"
              :font-family "monospace"
              :font-size "12px"
              :text-transform "uppercase"
              :letter-spacing "1px"
              :transition "all 0.3s"
              :display "flex"
              :align-items "center"
              :gap "5px"}
      :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(0,255,159,0.1)")
      :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
      :on-click (fn []
                 (swap! table-dropdown-open not)
                 ;; Fetch tables when opening dropdown (after toggle, so check the new value)
                 (when @table-dropdown-open
                   (-> (js/fetch "http://localhost:5000/api/tables")
                       (.then #(.json %))
                       (.then (fn [data]
                               (let [tables-data (js->clj data :keywordize-keys true)
                                     ;; Separate reactor tables from regular public tables
                                     reactor-tables (filter #(str/starts-with? % "reactor_") (:public tables-data))
                                     other-tables (remove #(str/starts-with? % "reactor_") (:public tables-data))]
                                 (reset! table-list {:public other-tables
                                                    :reactor reactor-tables
                                                    :system (get tables-data :system [])})))))))}
     "+ TABLE"
     [:span {:style {:font-size "10px"}} "▼"]]
    ;; Dropdown menu
    (when @table-dropdown-open
      [:div {:style {:position "absolute"
                     :top "100%"
                     :left 0
                     :margin-top "5px"
                     :background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
                     :border "1px solid #00ff9f"
                     :border-radius "4px"
                     :min-width "200px"
                     :max-height "400px"
                     :overflow-y "auto"
                     :z-index 1000
                     :box-shadow "0 4px 20px rgba(0,255,159,0.3)"}}
       ;; User tables (dynamically loaded)
       [:div {:style {:padding "5px 10px"
                      :color "#00ff9f"
                      :font-family "monospace"
                      :font-size "10px"
                      :text-transform "uppercase"
                      :border-bottom "1px solid rgba(0,255,159,0.2)"
                      :opacity 0.7}}
        "Data Tables"]
       (for [table (filter #(not (str/starts-with? % "test_")) (:public @table-list))]
         ^{:key table}
         [:div {:style {:padding "8px 15px"
                        :color "#8ff0a4"
                        :font-family "monospace"
                        :font-size "11px"
                        :cursor "pointer"
                        :transition "all 0.2s"}
                :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(0,255,159,0.1)")
                :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
                :on-click (fn []
                           (reset! table-dropdown-open false)
                           (let [block-data {:id (str (random-uuid))
                                           :type :query
                                           :position {:x (+ 100 (rand-int 200)) :y (+ 100 (rand-int 200))}
                                           :size {:width 400 :height 300}
                                           :sql (str "SELECT * FROM " table " LIMIT 10")}]
                             (r/dispatch! [:add-block block-data])))}
          table])
       ;; Reactor tables
       (when (seq (:reactor @table-list))
         [:div
          [:div {:style {:padding "5px 10px"
                         :color "#9b59b6"
                         :font-family "monospace"
                         :font-size "10px"
                         :text-transform "uppercase"
                         :border-bottom "1px solid rgba(155,89,182,0.2)"
                         :margin-top "5px"
                         :opacity 0.7}}
           "Reactor Debug Tables"]
          (for [table (:reactor @table-list)]
            ^{:key table}
            [:div {:style {:padding "8px 15px"
                           :color "#d8b4fe"
                           :font-family "monospace"
                           :font-size "11px"
                           :cursor "pointer"
                           :transition "all 0.2s"}
                   :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(155,89,182,0.1)")
                   :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
                   :on-click (fn []
                              (reset! table-dropdown-open false)
                              (let [block-data {:id (str (random-uuid))
                                              :type :query
                                              :position {:x (+ 100 (rand-int 200)) :y (+ 100 (rand-int 200))}
                                              :size {:width 400 :height 300}
                                              :sql (str "SELECT * FROM " table " LIMIT 10")}]
                                (r/dispatch! [:add-block block-data])))}
             table])])
       ;; System tables
       (when (seq (:system @table-list))
         [:div
          [:div {:style {:padding "5px 10px"
                         :color "#ff006e"
                         :font-family "monospace"
                         :font-size "10px"
                         :text-transform "uppercase"
                         :border-bottom "1px solid rgba(255,0,110,0.2)"
                         :margin-top "5px"
                         :opacity 0.7}}
           "System Tables"]
          (for [table (:system @table-list)]
            ^{:key table}
            [:div {:style {:padding "8px 15px"
                           :color "#ff4f99"
                           :font-family "monospace"
                           :font-size "11px"
                           :cursor "pointer"
                           :transition "all 0.2s"}
                   :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(255,0,110,0.1)")
                   :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
                   :on-click (fn []
                              (reset! table-dropdown-open false)
                              (let [block-data {:id (str (random-uuid))
                                              :type :query
                                              :position {:x (+ 100 (rand-int 200)) :y (+ 100 (rand-int 200))}
                                              :size {:width 400 :height 300}
                                              :sql (str "SELECT * FROM " table " LIMIT 10")}]
                                (r/dispatch! [:add-block block-data])))}
             table])])])]
   [:button
    {:style {:padding "8px 16px"
             :background "transparent"
             :color "#ff006e"
             :border "1px solid #ff006e"
             :border-radius "2px"
             :cursor "pointer"
             :font-family "monospace"
             :font-size "12px"
             :text-transform "uppercase"
             :letter-spacing "1px"
             :transition "all 0.3s"}
     :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(255,0,110,0.1)")
     :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
     :on-click (fn []
                 (let [block-data {:id (str (random-uuid))
                                   :type :chart
                                   :position {:x 200 :y 100}
                                   :size {:width 400 :height 300}}]
                   (js/console.log "Adding chart block:" (clj->js block-data))
                   (r/dispatch! [:add-block block-data])))}
    "+ CHART"]
   [:button
    {:style {:padding "8px 16px"
             :background "transparent"
             :color "#ffb700"
             :border "1px solid #ffb700"
             :border-radius "2px"
             :cursor "pointer"
             :font-family "monospace"
             :font-size "12px"
             :text-transform "uppercase"
             :letter-spacing "1px"
             :transition "all 0.3s"}
     :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(255,183,0,0.1)")
     :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
     :on-click (fn []
                 (let [block-data {:id (str (random-uuid))
                                   :type :sql-exec
                                   :position {:x 300 :y 100}
                                   :size {:width 350 :height 200}}]
                   (js/console.log "Adding SQL exec block:" (clj->js block-data))
                   (r/dispatch! [:add-block block-data])))}
    "+ EXECUTE"]
   [:button
    {:style {:padding "8px 16px"
             :background "transparent"
             :color "#9b59b6"
             :border "1px solid #9b59b6"
             :border-radius "2px"
             :cursor "pointer"
             :font-family "monospace"
             :font-size "12px"
             :font-weight "bold"
             :text-transform "uppercase"
             :letter-spacing "1px"}
     :on-click (fn []
                 (let [block-data {:id (str "debug-" (random-uuid))
                                   :type :debug
                                   :position {:x (+ 50 (rand-int 200))
                                              :y (+ 50 (rand-int 200))}
                                   :size {:width 500 :height 400}
                                   :debug-mode :subscriptions}]
                   (js/console.log "Adding debug block:" (clj->js block-data))
                   (r/dispatch! [:add-block block-data])))}
    "+ DEBUG"]
   [:button
    {:style {:padding "8px 16px"
             :background "transparent"
             :color "#00ffd4"
             :border "1px solid #00ffd4"
             :border-radius "2px"
             :cursor "pointer"
             :font-family "monospace"
             :font-size "12px"
             :text-transform "uppercase"
             :letter-spacing "1px"
             :transition "all 0.3s"}
     :on-click (fn []
                 (let [block-data {:id (str (random-uuid))
                                   :type :edn-browser
                                   :position {:x (+ 100 (rand-int 200))
                                              :y (+ 100 (rand-int 200))}
                                   :size {:width 450 :height 350}}]
                   (js/console.log "Adding EDN browser block:" (clj->js block-data))
                   (r/dispatch! [:add-block block-data])))}
    "+ EDN BROWSER"]
   [:button
    {:style {:padding "8px 16px"
             :background "transparent"
             :color "#ff4f99"
             :border "1px solid #ff4f99"
             :border-radius "2px"
             :cursor "pointer"
             :font-family "monospace"
             :font-size "12px"
             :text-transform "uppercase"
             :letter-spacing "1px"
             :transition "all 0.3s"}
     :on-click (fn []
                 (let [block-data {:id (keyword (str "tap-" (random-uuid)))
                                   :type :tap
                                   :position {:x (+ 100 (rand-int 200))
                                              :y (+ 100 (rand-int 200))}
                                   :size {:width 450 :height 400}}]
                   (js/console.log "Adding TAP block:" (clj->js block-data))
                   (r/dispatch! [:add-block block-data])))}
    "+ TAP"]
   [:div {:style {:flex 1}}]
   [:span {:style {:color "#00ff9f"
                   :font-family "monospace"
                   :font-size "14px"
                   :text-transform "uppercase"
                   :letter-spacing "2px"}} "RABBIT//SQL_BROWSER"]])

;; ============= Session Management =============

(defonce session-dropdown-open (reagent/atom false))
(defonce sessions-list (reagent/atom []))
(defonce current-session (reagent/atom "default"))
(defonce new-session-name (reagent/atom ""))

(defn load-sessions! []
  (-> (r/get-sessions!)
      (.then (fn [sessions]
               (reset! sessions-list sessions)))))

(defn session-selector []
  (let [blocks (r/subscribe [:blocks])
        _ (reagent/create-class
           {:component-did-mount
            (fn []
              (load-sessions!)
              ;; Poll for session updates
              (js/setInterval load-sessions! 5000))
            
            :reagent-render
            (fn [] [:div])})
        ;; Reload sessions when blocks change
        _ (add-watch blocks ::session-refresh
                    (fn [_ _ _ _]
                      (load-sessions!)))]
    (fn []
      [:div {:style {:position "relative"}}
       ;; Current session button
       [:button
        {:style {:padding "6px 12px"
                 :background "transparent"
                 :color "#00ff9f"
                 :border "1px solid rgba(0,255,159,0.5)"
                 :border-radius "2px"
                 :cursor "pointer"
                 :font-family "monospace"
                 :font-size "11px"
                 :text-transform "uppercase"
                 :display "flex"
                 :align-items "center"
                 :gap "5px"}
         :on-click #(swap! session-dropdown-open not)}
        [:span {:style {:color "#00ff9f"
                        :opacity 0.7
                        :font-size "10px"}} "SESSION:"]
        [:span @current-session]
        [:span {:style {:font-size "10px"}} (if @session-dropdown-open "▲" "▼")]]
       
       ;; Dropdown menu (drops upward)
       (when @session-dropdown-open
         [:div {:style {:position "absolute"
                        :bottom "100%"
                        :right 0
                        :margin-bottom "5px"
                        :background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
                        :border "1px solid #00ff9f"
                        :border-radius "4px"
                        :min-width "250px"
                        :max-height "300px"
                        :overflow-y "auto"
                        :z-index 1000
                        :box-shadow "0 -4px 20px rgba(0,255,159,0.3)"}}
          ;; New session input
          [:div {:style {:padding "10px"
                         :border-bottom "1px solid rgba(0,255,159,0.2)"}}
           [:div {:style {:display "flex" :gap "5px"}}
            [:input {:type "text"
                     :placeholder "New session name"
                     :value @new-session-name
                     :on-change #(reset! new-session-name (-> % .-target .-value))
                     :style {:flex 1
                             :padding "4px 8px"
                             :background "rgba(0,0,0,0.3)"
                             :border "1px solid rgba(0,255,159,0.3)"
                             :border-radius "2px"
                             :color "#00ff9f"
                             :font-family "monospace"
                             :font-size "11px"}
                     :on-key-down #(when (= (.-which %) 13)
                                    (when (seq @new-session-name)
                                      (r/create-session! @new-session-name)
                                      (r/switch-session! @new-session-name)
                                      (reset! current-session @new-session-name)
                                      (reset! new-session-name "")
                                      (load-sessions!)))}]
            [:button {:style {:padding "4px 10px"
                              :background "linear-gradient(90deg, #00ff9f 0%, #00cc7f 100%)"
                              :color "#0a0a0a"
                              :border "none"
                              :border-radius "2px"
                              :cursor "pointer"
                              :font-family "monospace"
                              :font-size "10px"
                              :font-weight "bold"}
                      :on-click (fn []
                                 (when (seq @new-session-name)
                                   (r/create-session! @new-session-name)
                                   (r/switch-session! @new-session-name)
                                   (reset! current-session @new-session-name)
                                   (reset! new-session-name "")
                                   (load-sessions!)))}
             "CREATE"]]]
          
          ;; Session list
          [:div {:style {:max-height "200px" :overflow-y "auto"}}
           (for [session @sessions-list]
             ^{:key (:session-id session)}
             [:div {:style {:display "flex"
                            :align-items "center"
                            :padding "8px 10px"
                            :cursor "pointer"
                            :transition "all 0.2s"
                            :background (when (= (:session-id session) @current-session)
                                         "rgba(0,255,159,0.1)")}
                    :on-mouse-over #(when (not= (:session-id session) @current-session)
                                     (set! (.-style.background ^js (.-currentTarget %)) "rgba(0,255,159,0.05)"))
                    :on-mouse-out #(when (not= (:session-id session) @current-session)
                                    (set! (.-style.background ^js (.-currentTarget %)) "transparent"))
                    :on-click (fn []
                               (r/switch-session! (:session-id session))
                               (reset! current-session (:session-id session))
                               (reset! session-dropdown-open false))}
              [:div {:style {:flex 1}}
               [:div {:style {:color "#00ff9f"
                              :font-family "monospace"
                              :font-size "11px"}} 
                (:session-id session)]
               [:div {:style {:color "#8ff0a4"
                              :font-family "monospace"
                              :font-size "9px"
                              :opacity 0.7}} 
                (str (count (get-in session [:canvas :blocks] {})) " blocks")]]
              ;; Delete button (not for default session)
              (when (not= (:session-id session) "default")
                [:button {:style {:padding "2px 6px"
                                  :background "none"
                                  :border "1px solid rgba(255,0,0,0.5)"
                                  :border-radius "2px"
                                  :color "#ff6b6b"
                                  :cursor "pointer"
                                  :font-family "monospace"
                                  :font-size "9px"}
                          :on-click (fn [e]
                                     (.stopPropagation ^js e)
                                     (when (js/confirm (str "Delete session '" (:session-id session) "'?"))
                                       (-> (js/fetch "http://localhost:5000/api/delete-session"
                                                    #js {:method "POST"
                                                         :headers #js {"Content-Type" "application/json"}
                                                         :body (js/JSON.stringify #js {:session-id (:session-id session)})})
                                           (.then #(.json %))
                                           (.then (fn [result]
                                                   (when (= (:session-id session) @current-session)
                                                     (reset! current-session "default")
                                                     (r/switch-session! "default"))
                                                   (load-sessions!))))))}
                 "DELETE"])])]])])))

;; ============= Timeline Component =============

(defn timeline-controls []
  (let [history-info (r/subscribe [:history-info])]
    (fn []
      [:div.timeline
       {:style {:position "fixed"
                :bottom 0
                :left 0
                :right 0
                :height "60px"
                :background "linear-gradient(90deg, #0a0a0a 0%, #1a1a2e 100%)"
                :border-top "1px solid rgba(0,255,159,0.2)"
                :display "flex"
                :align-items "center"
                :padding "0 20px"
                :gap "20px"
                :box-shadow "0 -2px 20px rgba(0,0,0,0.5)"}}
       [:button {:style {:padding "6px 12px"
                         :background "transparent"
                         :color "#00ff9f"
                         :border "1px solid rgba(0,255,159,0.5)"
                         :border-radius "2px"
                         :cursor "pointer"
                         :font-family "monospace"
                         :font-size "11px"
                         :text-transform "uppercase"
                         :opacity (if (:can-undo @history-info) 1 0.5)}
                 :disabled (not (:can-undo @history-info))
                 :on-click #(r/undo!)} "← UNDO"]
       [:button {:style {:padding "6px 12px"
                         :background "transparent"
                         :color "#00ff9f"
                         :border "1px solid rgba(0,255,159,0.5)"
                         :border-radius "2px"
                         :cursor "pointer"
                         :font-family "monospace"
                         :font-size "11px"
                         :text-transform "uppercase"
                         :opacity (if (:can-redo @history-info) 1 0.5)}
                 :disabled (not (:can-redo @history-info))
                 :on-click #(r/redo!)} "REDO →"]
       [:div {:style {:flex 1 :display "flex" :align-items "center" :gap "15px"}}
        [:span {:style {:color "#00ff9f" :font-family "monospace" :font-size "11px" :text-transform "uppercase"}} 
         "TIMELINE:"]
        [:span {:style {:color "#8ff0a4" :font-family "monospace" :font-size "10px"}}
         (str "State " (- (:total-states @history-info 0) (:current-index @history-info 0))
              " of " (:total-states @history-info 0))]
        [:input {:type "range"
                 :min 0
                 :max (max 0 (dec (:total-states @history-info 1)))
                 :value (- (max 0 (dec (:total-states @history-info 1))) 
                          (:current-index @history-info 0))
                 :style {:flex 1 
                        :-webkit-appearance "none"
                        :height "2px"
                        :background "rgba(0,255,159,0.2)"
                        :outline "none"}
                 :on-change #(let [val (js/parseInt (.. % -target -value))
                                  max-idx (max 0 (dec (:total-states @history-info 1)))
                                  idx (- max-idx val)]
                              (r/jump-to-history! idx))}]]
       ;; Session selector on the right
       [session-selector]])))

;; ============= Main App Component =============

(defn rabbit-app []
  [:div.rabbit-demo
   {:style {:height "100vh"
            :display "flex"
            :flex-direction "column"
            :font-family "sans-serif"
            :background "#0a0a0a"
            :overflow "hidden"}}
   [toolbar]
   [canvas]
   [timeline-controls]])

;; ============= Initialize =============

(defn ^:export init! []
  (r/init! {:server-url "http://localhost:5000"})
  ;; Initialize with default session or get from query params
  (let [params (js/URLSearchParams. js/window.location.search)
        session-id (or (.get params "session") "default")]
    (reset! current-session session-id)
    (r/switch-session! session-id))
  ;; Auto-refresh queries for blocks loaded from persistence
  (auto-refresh/init-auto-refresh!)
  ;; Wait for state to load, then check if we need to initialize
  (js/setTimeout
    (fn []
      ;; Only dispatch init-rabbit for truly new/empty sessions
      (let [current-state @(r/subscribe [:get []])]
        (when-not (:canvas current-state)
          (js/console.log "No canvas found, initializing...")
          (r/dispatch! [:init-rabbit])))
      ;; Always get history info for time travel
      (r/get-history-info!))
    500)  ;; Give more time for state to load
  (load-sessions!)
  (rdom/render [rabbit-app] (.getElementById js/document "app")))