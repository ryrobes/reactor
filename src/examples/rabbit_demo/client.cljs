(ns examples.rabbit-demo.client
  "Rabbit Demo - Interactive SQL data browser with time travel"
  (:require [reactor.core :as r]
            [reagent.core :as reagent]
            [reagent.dom :as rdom]
            [clojure.string :as str]
            [examples.rabbit-demo.monaco :as monaco]))

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
      
      ;; Debounce the server update
      (when @pending-update
        (js/clearTimeout @pending-update))
      (reset! pending-update
              (js/setTimeout
               #(r/dispatch! [:move-block block-id {:x x :y y}])
               100)))))

(defn stop-drag! []
  ;; Send final position on drag end
  (when-let [{:keys [block-id]} @drag-state]
    (when-let [pos (get @local-positions block-id)]
      (r/dispatch! [:move-block block-id pos])
      ;; Keep the local position until server confirms
      ;; (removed the reset of local-positions here)
      ))
  (reset! drag-state nil)
  (when @pending-update
    (js/clearTimeout @pending-update)
    (reset! pending-update nil)))

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
      
      ;; Debounce the server update
      (when @pending-update
        (js/clearTimeout @pending-update))
      (reset! pending-update
              (js/setTimeout
               #(r/dispatch! [:resize-block block-id {:width new-width :height new-height}])
               100)))))

(defn stop-resize! []
  ;; Send final size on resize end
  (when-let [{:keys [block-id]} @resize-state]
    (when-let [size (get @local-sizes block-id)]
      (r/dispatch! [:resize-block block-id size])))
  (reset! resize-state nil)
  (when @pending-update
    (js/clearTimeout @pending-update)
    (reset! pending-update nil)))

;; ============= Block Components =============

(defn query-block [{:keys [id position size sql results as-of]}]
  (let [;; Initialize local position if not set
        _ (when (and position (not (get @local-positions id)))
            (swap! local-positions assoc id position))
        local-pos (get @local-positions id)
        actual-pos (or local-pos position)
        ;; Initialize local size if not set
        _ (when (and size (not (get @local-sizes id)))
            (swap! local-sizes assoc id size))
        local-size (get @local-sizes id)
        actual-size (or local-size size)]
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
              :cursor "move"
              :z-index 10
              :box-shadow (if @connection-mode
                           "0 0 30px rgba(255,0,110,0.5), inset 0 0 20px rgba(255,0,110,0.1)"
                           "0 0 20px rgba(0,255,159,0.3), inset 0 0 20px rgba(0,255,159,0.05)")}
      :on-mouse-down #(start-drag! id %)
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
     [:div.block-header
      {:style {:display "flex"
               :justify-content "space-between"
               :margin-bottom "10px"
               :padding-bottom "5px"
               :border-bottom "1px solid rgba(0,255,159,0.2)"
               :cursor (when @connection-mode "pointer")}
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
                           (r/dispatch! [:delete-block id]))
                :style {:background "none"
                        :border "none"
                        :color "#ff006e"
                        :cursor "pointer"
                        :font-size "20px"
                        :line-height "20px"}}
       "×"]]
     ;; SQL Editor
     [:div {:style {:margin "10px 0"
                    :border "1px solid rgba(0,255,159,0.3)"
                    :border-radius "4px"
                    :overflow "hidden"}}
      [monaco/sql-editor 
       {:value (or sql "SELECT * FROM sales")
        :on-change #(r/dispatch! [:update-block id {:sql %}])
        :height "100px"
        :theme "vs-dark"}]]
     ;; Time scrubber for this query
     [:div {:style {:margin "10px 0"
                    :padding "10px"
                    :background "rgba(0,0,0,0.3)"
                    :border-radius "4px"}}
      [:div {:style {:display "flex"
                     :align-items "center"
                     :gap "10px"
                     :margin-bottom "5px"}}
       [:span {:style {:color "#00ff9f"
                       :font-family "monospace"
                       :font-size "10px"
                       :text-transform "uppercase"}} "Time:"]
       [:input {:type "range"
                :min 0
                :max 10
                :value (or as-of 0)
                :style {:flex 1
                        :-webkit-appearance "none"
                        :height "2px"
                        :background "rgba(0,255,159,0.2)"
                        :outline "none"}
                :on-change #(r/dispatch! [:update-block id {:as-of (.. % -target -value)}])}]
       [:span {:style {:color "#8ff0a4"
                       :font-family "monospace"
                       :font-size "10px"}} 
        (str "T-" (or as-of 0))]]
      [:button
       {:style {:width "100%"
                :padding "5px 10px"
                :background "linear-gradient(90deg, #00ff9f 0%, #00cc7f 100%)"
                :color "#0a0a0a"
                :border "none"
                :border-radius "2px"
                :cursor "pointer"
                :font-weight "bold"
                :text-transform "uppercase"
                :font-size "11px"
                :letter-spacing "1px"}
        :on-click (fn []
                    (-> (r/sql-query! (or sql "SELECT * FROM sales") nil as-of)
                        (.then (fn [response]
                                 (r/dispatch! [:update-block id {:results (:results response)}])))))}
       "Execute Query"]]
     (when results
       [:div.results
        {:style {:margin-top "10px"
                 :max-height "200px"
                 :overflow "auto"
                 :background "rgba(0,0,0,0.3)"
                 :border "1px solid rgba(0,255,159,0.2)"
                 :padding "5px"}}
        [:table {:style {:width "100%" :font-size "11px"}}
         [:thead
          [:tr
           (for [col (keys (first results))]
             ^{:key col}
             [:th {:style {:text-align "left" 
                          :padding "4px"
                          :color "#00ff9f"
                          :border-bottom "1px solid rgba(0,255,159,0.2)"
                          :font-family "monospace"
                          :text-transform "uppercase"}} (name col)])]]
         [:tbody
          (for [row results]
            ^{:key (or (:ID row) (:id row) (:xt/id row) (str (hash row)))}
            [:tr
             (for [col (keys (first results))]
               ^{:key col}
               [:td {:style {:padding "4px"
                            :color "#8ff0a4"
                            :font-family "monospace"}} (str (get row col))])])]]])]))

(defn chart-block [{:keys [id position size source-id chart-type]}]
  (let [;; Initialize local position if not set
        _ (when (and position (not (get @local-positions id)))
            (swap! local-positions assoc id position))
        local-pos (get @local-positions id)
        actual-pos (or local-pos position)
        ;; Initialize local size if not set
        _ (when (and size (not (get @local-sizes id)))
            (swap! local-sizes assoc id size))
        local-size (get @local-sizes id)
        actual-size (or local-size size)
        source-block (when source-id @(r/subscribe [:block source-id]))
        chart-data (:results source-block [])]
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
              :cursor "move"
              :z-index 10
              :box-shadow "0 0 20px rgba(255,0,110,0.3), inset 0 0 20px rgba(255,0,110,0.05)"}
      :on-mouse-down #(start-drag! id %)}
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
               :border-bottom "1px solid rgba(255,0,110,0.2)"}}
      [:span {:style {:font-weight "bold" 
                      :color "#ff006e"
                      :font-family "monospace"
                      :text-transform "uppercase"
                      :font-size "11px"
                      :letter-spacing "1px"}} "CHART"]
      [:button {:on-click #(r/dispatch! [:delete-block id])
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
     ;; Chart visualization
     [:div {:style {:height "calc(100% - 100px)"
                    :overflow "auto"}}
      (if (seq chart-data)
        [:div {:style {:color "#ff4f99" :font-family "monospace" :font-size "10px"}}
         [:pre (pr-str (take 3 chart-data))]]
        [:div {:style {:color "#ff4f99" 
                       :font-family "monospace" 
                       :opacity 0.5
                       :text-align "center"
                       :margin-top "20px"}} 
         "Link to a query block to see data"])]]))

(defn sql-exec-block [{:keys [id position size sql]}]
  (let [;; Initialize local position if not set
        _ (when (and position (not (get @local-positions id)))
            (swap! local-positions assoc id position))
        local-pos (get @local-positions id)
        actual-pos (or local-pos position)
        ;; Initialize local size if not set
        _ (when (and size (not (get @local-sizes id)))
            (swap! local-sizes assoc id size))
        local-size (get @local-sizes id)
        actual-size (or local-size size)]
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
              :cursor "move"
              :z-index 10
              :box-shadow "0 0 20px rgba(255,183,0,0.3), inset 0 0 20px rgba(255,183,0,0.05)"}
      :on-mouse-down #(start-drag! id %)}
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
               :border-bottom "1px solid rgba(255,183,0,0.2)"}}
      [:span {:style {:font-weight "bold" 
                      :color "#ffb700"
                      :font-family "monospace"
                      :text-transform "uppercase"
                      :font-size "11px"
                      :letter-spacing "1px"}} "SQL EXECUTE"]
      [:button {:on-click #(r/dispatch! [:delete-block id])
                :style {:background "none"
                        :border "none"
                        :color "#ff006e"
                        :cursor "pointer"
                        :font-size "20px"
                        :line-height "20px"}} "×"]]
     ;; SQL Editor
     [:div {:style {:margin "10px 0"
                    :border "1px solid rgba(255,183,0,0.3)"
                    :border-radius "4px"
                    :overflow "hidden"}}
      [monaco/sql-editor
       {:value (or sql "UPDATE sales SET amount = 300 WHERE id = 1")
        :on-change #(r/dispatch! [:update-block id {:sql %}])
        :height "120px"
        :theme "vs-dark"}]]
     [:button
      {:style {:margin-top "5px"
               :padding "5px 10px"
               :background "linear-gradient(90deg, #ffb700 0%, #ff8c00 100%)"
               :color "#0a0a0a"
               :border "none"
               :border-radius "2px"
               :cursor "pointer"
               :font-weight "bold"
               :text-transform "uppercase"
               :font-size "11px"
               :letter-spacing "1px"}}
      "Execute"]]))

(defn render-block [block]
  (js/console.log "Rendering block:" (clj->js block) "Type:" (:type block))
  (let [block-type (if (string? (:type block))
                     (keyword (:type block))
                     (:type block))]
    (case block-type
      :query [query-block block]
      :chart [chart-block block]
      :sql-exec [sql-exec-block block]
      (do
        (js/console.warn "Unknown block type:" block-type "original:" (:type block))
        nil))))

;; ============= Canvas Component =============

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
   [:div {:style {:flex 1}}]
   [:span {:style {:color "#00ff9f"
                   :font-family "monospace"
                   :font-size "14px"
                   :text-transform "uppercase"
                   :letter-spacing "2px"}} "RABBIT//SQL_BROWSER"]])

;; ============= Timeline Component =============

(defn timeline-controls []
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
                     :text-transform "uppercase"}
             :on-click #(r/undo!)} "← UNDO"]
   [:button {:style {:padding "6px 12px"
                     :background "transparent"
                     :color "#00ff9f"
                     :border "1px solid rgba(0,255,159,0.5)"
                     :border-radius "2px"
                     :cursor "pointer"
                     :font-family "monospace"
                     :font-size "11px"
                     :text-transform "uppercase"}
             :on-click #(r/redo!)} "REDO →"]
   [:div {:style {:flex 1 :display "flex" :align-items "center" :gap "15px"}}
    [:span {:style {:color "#00ff9f" :font-family "monospace" :font-size "11px" :text-transform "uppercase"}} "Canvas:"]
    [:input {:type "range" 
             :style {:flex 1 
                    :-webkit-appearance "none"
                    :height "2px"
                    :background "rgba(0,255,159,0.2)"
                    :outline "none"}}]
    [:span {:style {:color "#ff006e" :font-family "monospace" :font-size "11px" :text-transform "uppercase"}} "Data:"]
    [:input {:type "range" 
             :style {:flex 1
                    :-webkit-appearance "none"
                    :height "2px"
                    :background "rgba(255,0,110,0.2)"
                    :outline "none"}}]]])

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
  (r/dispatch! [:init-rabbit])
  (rdom/render [rabbit-app] (.getElementById js/document "app")))