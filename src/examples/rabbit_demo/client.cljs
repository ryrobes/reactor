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
                 ;; Update local positions for blocks that moved in state
                 (doseq [[id block] new-blocks]
                   (when-let [pos (:position block)]
                     (when (not= pos (get-in old-blocks [id :position]))
                       (swap! local-positions assoc id pos)))
                   (when-let [size (:size block)]
                     (when (not= size (get-in old-blocks [id :size]))
                       (swap! local-sizes assoc id size))))))))

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
      ;; Clear local position after dispatch
      (swap! local-positions dissoc block-id)))
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
      (r/dispatch! [:resize-block block-id size])
      ;; Clear local size after dispatch
      (swap! local-sizes dissoc block-id)))
  (reset! resize-state nil)
  (when @pending-update
    (js/clearTimeout @pending-update)
    (reset! pending-update nil)))

;; ============= Block Components =============

(defn query-block [{:keys [id position size sql results as-of error] :as block}]
  (let [;; Use local position only while dragging, otherwise use state position
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
                        :color "#00ff9f"
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
                                 (if (:error response)
                                   (r/dispatch! [:update-block id {:results nil :error (:error response)}])
                                   (r/dispatch! [:update-block id {:results (:results response) :error nil}]))))))}
       "Execute Query"]]
     (when (:error block)
       [:div {:style {:margin-top "10px"
                     :padding "10px"
                     :background "rgba(255,0,0,0.1)"
                     :border "1px solid rgba(255,0,0,0.3)"
                     :border-radius "4px"
                     :color "#ff6b6b"
                     :font-family "monospace"
                     :font-size "11px"}}
        (:error block)])
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

(defn sql-exec-block [{:keys [id position size sql error result] :as block}]
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
                        :color "#ffb700"
                        :cursor "pointer"
                        :font-size "20px"
                        :line-height "20px"}} "×"]]
     ;; SQL Editor
     [:div {:style {:margin "10px 0"
                    :border "1px solid rgba(255,183,0,0.3)"
                    :border-radius "4px"
                    :overflow "hidden"}}
      [monaco/sql-editor
       {:value (or sql "INSERT INTO sales (product, amount, quantity, sale_date) VALUES ('TestProduct', 500, 2, '2024-01-10')")
        :on-change #(r/dispatch! [:update-block id {:sql %}])
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
                  (-> (r/sql-exec! (or sql ""))
                      (.then (fn [response]
                              (if (:error response)
                                (r/dispatch! [:update-block id {:error (:error response) :result nil}])
                                (r/dispatch! [:update-block id {:result (:result response) :error nil}]))))))}
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
        (str "Success: " result)])]))

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

(defonce table-dropdown-open (reagent/atom false))
(defonce table-list (reagent/atom {:public [] :system []}))

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
                 ;; Fetch tables when opening dropdown
                 (when-not @table-dropdown-open
                   (-> (js/fetch "http://localhost:5000/api/tables")
                       (.then #(.json %))
                       (.then (fn [data]
                               (let [tables-data (js->clj data :keywordize-keys true)]
                                 (reset! table-list tables-data)))))))}
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