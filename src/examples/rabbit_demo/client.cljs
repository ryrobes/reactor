(ns examples.rabbit-demo.client
  "Rabbit Demo - Interactive SQL data browser with time travel"
  (:require [reactor.core :as r]
            [reagent.core :as reagent]
            [reactor.tap :as t]
            [reactor.console-tap :as console-tap]
            [reagent.dom :as rdom]
            [reagent.dom.client :as rdom-client]
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
            [examples.rabbit-demo.rule-flow-block :as rule-flow-block]
            [examples.rabbit-demo.iframe-block :as iframe-block]
            [examples.rabbit-demo.template-resolver :as resolver]
            [examples.rabbit-demo.themes :as themes]
            [examples.rabbit-demo.draggable-toolbar :as dtoolbar]
            [examples.rabbit-demo.virtual-grid :as vgrid]
            [examples.rabbit-demo.cache-debug-panel :as cache-debug]))

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

;; Subscribe to UI settings separately from canvas to avoid triggering block updates
(r/reg-sub :ui-settings-db
  (fn [db _]
    (get db :ui-settings {:monaco-font-size 12})))

;; Simple client-side atom for UI settings - will be synced with server
;; Initialize with saved value from localStorage if available
(defonce ui-settings 
  (reagent/atom 
    (let [saved-size (js/localStorage.getItem "rabbit-monaco-font-size")]
      {:monaco-font-size (if saved-size 
                           (js/parseInt saved-size 10) 
                           12)})))

;; Watch UI settings changes from server and sync to local atom
(defonce ui-settings-watcher
  (let [ui-settings-sub (r/subscribe [:ui-settings-db])]
    (add-watch ui-settings-sub ::ui-sync
      (fn [_ _ _ new-ui]
        (when new-ui
          (reset! ui-settings new-ui))))))

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

;; ============= Query Management - Completely separate from rendering =============

;; Track if we've done initial load
(defonce initial-queries-loaded (atom false))

;; Watch for blocks that need queries and manage them separately
(defonce blocks-query-watcher
  (let [blocks-sub (r/subscribe [:blocks])
        ;; Helper to initialize all queries
        initialize-all-queries! (fn [blocks]
                                  (js/console.log "[QUERY-MANAGER] Initializing all query blocks...")
                                  (doseq [[id block] blocks]
                                    (when (and (= (:type block) :query) (:sql block))
                                      (js/console.log "[QUERY-MANAGER] Initial load - starting query for block" id)
                                      (rq/execute-block-query! id (:sql block) nil nil)))
                                  (reset! initial-queries-loaded true))]
    
    ;; Initial load - wait for app to stabilize then run all queries
    (js/setTimeout
     (fn []
       (when-not @initial-queries-loaded
         (let [current-blocks @blocks-sub]
           (when (seq current-blocks)
             (js/console.log "[QUERY-MANAGER] Initial page load - starting queries after delay")
             (initialize-all-queries! current-blocks)))))
     500) ; 500ms delay to let everything settle
    
    ;; Watch for changes
    (add-watch blocks-sub ::query-manager
               (fn [_ _ old-blocks new-blocks]
                 ;; If this is the first time we have blocks and we haven't initialized
                 (when (and (empty? old-blocks) 
                           (not-empty new-blocks)
                           (not @initial-queries-loaded))
                   (js/console.log "[QUERY-MANAGER] First blocks loaded - initializing after delay")
                   (js/setTimeout 
                    #(initialize-all-queries! new-blocks)
                    300))
                 
                 ;; Normal change detection
                 (when @initial-queries-loaded
                   ;; Check each block to see if it needs a query
                   (doseq [[id block] new-blocks]
                     (when (= (:type block) :query)
                       (let [sql (:sql block)
                             old-block (get old-blocks id)
                             old-sql (:sql old-block)]
                         (cond
                           ;; New block with SQL - initialize query
                           (and sql (nil? old-block))
                           (do
                             (js/console.log "[QUERY-MANAGER] New query block" id "- initializing")
                             (rq/execute-block-query! id sql nil nil))
                           
                           ;; SQL changed - update query
                           (and sql old-sql (not= sql old-sql))
                           (do
                             (js/console.log "[QUERY-MANAGER] SQL changed for block" id)
                             (rq/execute-block-query! id sql nil nil))
                           
                           ;; Block exists, query should already be running
                           (and sql old-sql (= sql old-sql))
                           nil)))) ; Do nothing - query already running
                   
                   ;; Clean up removed blocks
                   (doseq [[id old-block] old-blocks]
                     (when (and (= (:type old-block) :query)
                                (nil? (get new-blocks id)))
                       (js/console.log "[QUERY-MANAGER] Block removed" id "- unsubscribing")
                       (rq/unsubscribe-block! id))))))))

(defn query-block [{:keys [id position size sql as-of] :as block}]
  (let [;; Local state for SQL editing - prevents re-renders while typing
        local-sql (reagent/atom sql)]
    
    ;; NO QUERY EXECUTION HERE - just rendering!
    ;; Queries are managed by the blocks-query-watcher above
    
    (reagent/create-class
     {:reagent-render
      (fn [{:keys [id position size sql as-of] :as block}]
        (let [;; Get results from reactive-queries module - including executed-sql
              {:keys [results error loading executed-sql]} (rq/get-block-results id)
            ;; Get UI settings - deref the atom directly for reactivity
            ui-settings-val @ui-settings
            font-size (or (:monaco-font-size ui-settings-val) 12)
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
     {:style (themes/apply-block-style
              {:position "absolute"
               :left (:x actual-pos)
               :top (:y actual-pos)
               :width (:width actual-size)
               :height (:height actual-size)
               :background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
               :border (if (and @connection-mode (= (:source-id @connection-mode) id))
                        "2px solid #ff006e"
                        (str "1px solid " (themes/get-primary-color)))
               :border-radius "4px"
               :padding "10px"
               :z-index (or (:z-index block) 10)
               :box-shadow (if @connection-mode
                            "0 0 30px rgba(255,0,110,0.5), inset 0 0 20px rgba(255,0,110,0.1)"
                            (str "0 0 20px " (themes/get-primary-color) "4C, inset 0 0 20px " (themes/get-primary-color) "0D"))
               :display "flex"
               :flex-direction "column"}
              :sql-block)
      :draggable false}
     ;; Resize handle
     [:div.resize-handle
      {:style {:position "absolute"
               :bottom 0
               :right 0
               :width "15px"
               :height "15px"
               :cursor "nwse-resize"
               :background (str "linear-gradient(135deg, transparent 50%, " (themes/get-primary-color) " 50%)")
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
                :border-bottom (str "1px solid " (themes/get-primary-color) "33")
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
                        :color (themes/get-font-color :block-title)
                        :font-family (themes/get-font-family :monospace)
                        :text-transform "uppercase"
                        :font-size "11px"
                        :letter-spacing "1px"}} "SQL QUERY"]
        [:span {:style {:color (themes/get-font-color :block-title-secondary)
                        :font-family (themes/get-font-family :monospace)
                        :font-size "9px"
                        :opacity 0.7}}
         (str "#" id)]]
       [:button {:on-click (fn [e]
                             (.stopPropagation ^js e)
                             (rq/unsubscribe-block! id)  ;; Clean up subscription
                             (r/dispatch! [:delete-block id]))
                 :style {:background "none"
                         :border "none"
                         :color (themes/get-primary-color)
                         :cursor "pointer"
                         :font-size "20px"
                         :line-height "20px"}}
        "×"]]
      ;; Placeholder area with font size controls
      [:div {:style {:display "flex"
                     :justify-content "space-between"
                     :align-items "center"
                     :margin-bottom "5px"
                     :min-height "20px"}}
       ;; Time travel indicator (left side)
       (when (and executed-sql (not= executed-sql sql))
         [:div {:style {:display "flex"
                        :align-items "center"
                        :gap "5px"
                        :padding "2px 8px"
                        :background (str (themes/get-primary-color) "1A")
                        :border (str "1px solid " (themes/get-primary-color) "33")
                        :border-radius "2px"
                        :font-size "10px"
                        :font-family (themes/get-font-family :monospace)
                        :color (themes/get-primary-color)
                        :cursor "pointer"}
                :on-click (when (and executed-sql (not= executed-sql sql))
                            (fn [e]
                              (.stopPropagation e)
                              ;; Reset to NOW - re-execute query without time travel
                              (let [current-sql @local-sql]
                                (rq/execute-block-query! id current-sql nil nil)
                                ;; Reset time travel slider to NOW position
                                (tt/reset-time-travel! id current-sql))))}
          [:span "⏰"]
          [:span "TIME TRAVEL MODE - Click to return to NOW"]])
       ;; Font size controls (right side)
       [:div {:style {:display "flex"
                      :align-items "center"
                      :gap "2px"}}
        [:button {:style {:background "transparent"
                          :border (str "1px solid " (themes/get-primary-color) "33")
                          :color (themes/get-primary-color)
                          :padding "0 6px"
                          :font-size "12px"
                          :line-height "16px"
                          :border-radius "2px"
                          :cursor "pointer"}
                  :on-click (fn [e]
                              (.stopPropagation e)
                              (let [new-size (max 8 (dec font-size))
                                    new-ui (assoc @ui-settings :monaco-font-size new-size)]
                                (swap! ui-settings assoc :monaco-font-size new-size)
                                (js/localStorage.setItem "rabbit-monaco-font-size" (str new-size))
                                (r/dispatch! [:update-canvas-ui new-ui])))}
         "−"]
        [:span {:style {:padding "0 4px"
                        :font-size "10px"
                        :color (themes/get-primary-color)
                        :font-family (themes/get-font-family :monospace)}}
         (str font-size "px")]
        [:button {:style {:background "transparent"
                          :border (str "1px solid " (themes/get-primary-color) "33")
                          :color (themes/get-primary-color)
                          :padding "0 6px"
                          :font-size "12px"
                          :line-height "16px"
                          :border-radius "2px"
                          :cursor "pointer"}
                  :on-click (fn [e]
                              (.stopPropagation e)
                              (let [new-size (min 24 (inc font-size))
                                    new-ui (assoc @ui-settings :monaco-font-size new-size)]
                                (swap! ui-settings assoc :monaco-font-size new-size)
                                (js/localStorage.setItem "rabbit-monaco-font-size" (str new-size))
                                (r/dispatch! [:update-canvas-ui new-ui])))}
         "+"]]]
      ;; SQL Editor with Execute button to the right
      [:div {:style {:margin "10px 0"
                     :display "flex"
                     :gap "10px"}}
       ;; Editor column
       [:div {:style {:flex 1}}
        ;;;Always render the container to prevent layout shift
        ;;  [:div {:style {:background (if (and executed-sql (not= executed-sql sql))
        ;;                               (str (themes/get-primary-color) "1A")
        ;;                               "transparent")
        ;;                 :border (if (and executed-sql (not= executed-sql sql))
        ;;                          (str "1px solid " (themes/get-primary-color) "4C")
        ;;                          "1px solid transparent")
        ;;                 :border-radius "4px 4px 0 0"
        ;;                 :padding "4px 8px"
        ;;                 :font-size "10px"
        ;;                 :font-family (themes/get-font-family :monospace)
        ;;                 :color (themes/get-primary-color)
        ;;                 :display "flex"
        ;;                 :align-items "center"
        ;;                 :gap "5px"
        ;;                 :cursor (if (and executed-sql (not= executed-sql sql)) "pointer" "default")
        ;;                 :transition "all 0.2s"
        ;;                 :min-height "24px"  ;; Ensure consistent height
        ;;                 :visibility (if (and executed-sql (not= executed-sql sql)) "visible" "hidden")}
        ;;         :on-mouse-over (when (and executed-sql (not= executed-sql sql))
        ;;                         #(set! (.. % -target -style -background) (str (themes/get-primary-color) "33")))
        ;;         :on-mouse-out (when (and executed-sql (not= executed-sql sql))
        ;;                        #(set! (.. % -target -style -background) (str (themes/get-primary-color) "1A")))
        ;;         :on-click (when (and executed-sql (not= executed-sql sql))
        ;;                    (fn [e]
        ;;                      (.stopPropagation e)
        ;;                      ;; Reset to NOW - re-execute query without time travel
        ;;                      (let [current-sql @local-sql]
        ;;                        (rq/execute-block-query! id current-sql nil nil)
        ;;                        ;; Reset time travel slider to NOW position
        ;;                        (tt/reset-time-travel! id current-sql))))}
        ;;   [:span "⏰"]
        ;;   [:span "TIME TRAVEL MODE - Click to return to NOW?"]]
        [:div {:style {:border (if (and executed-sql (not= executed-sql sql))
                                 (str "1px solid " (themes/get-primary-color) "80")
                                 (str "1px solid " (themes/get-primary-color) "4C"))
                       :border-radius (if (and executed-sql (not= executed-sql sql))
                                        "0 0 4px 4px"
                                        "4px")
                       ;:margin-top "-15px"
                       :overflow "hidden"
                       :background (when (and executed-sql (not= executed-sql sql))
                                     (str (themes/get-primary-color) "0D"))}}
         [monaco/sql-editor
          {:value (or executed-sql @local-sql "SELECT * FROM sales")
           :on-change #(reset! local-sql %)  ;; Only update local state while typing
           :height "100px"
           :width "100%"  ;; Explicitly set width
           :theme "vs-dark"
           :font-size font-size
           :editor-key (str "monaco-query-" id "-" (:width actual-size) "-" font-size)  ;; Include font-size in key
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
                  :background (str "linear-gradient(90deg, " (themes/get-primary-color) " 0%, " (themes/get-secondary-color) " 100%)")
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
                     :background "rgba(0,0,0,0.01)"
                     :border-radius "4px"}}
       ;; Time travel controls
       [tt/time-travel-controls {:block-id id :sql sql}]
       (when (> (count results) 1)
         [:div {:style {:font-size "11px"
                        :opacity 0.8
                        ;:margin-left "10px"
                        :position "absolute"
                        :right 30
                        ;:margin-bottom "-20px"
                        :margin-top "-12px"}}
          (str (.format (js/Intl.NumberFormat. "en-US") (or (count results) 0)) " rows")])]
      (when error
        [:div {:style {:margin-top "10px"
                       :padding "10px"
                       :background "rgba(255,0,0,0.1)"
                       :border "1px solid rgba(255,0,0,0.3)"
                       :border-radius "4px"
                       :color "#ff6b6b"
                       :font-family (themes/get-font-family :monospace)
                       :font-size "11px"}}
         error])
      ]  ;; Close fixed-content div
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
             font-size (min 100 (int (/ (:height actual-size) 4)))]
         (if is-single-value?
           ;; Render as large callout text
           [:div.single-value-result
            {:style {:flex 1
                     :display "flex"
                     ;:height "120%"
                     :align-items "center"
                     :justify-content "center"
                     ;:margin-top "10px"
                     :margin-top "-26px"
                     :background "rgba(0,0,0,0.01)"
                     ;:border (str "1px solid " (themes/get-primary-color) "33")
                     :padding "20px"
                     :overflow "hidden"}}
            [:div {:style {:color (themes/get-primary-color)
                           :font-family (if (themes/get-font-family :monospace)
                                          (str (themes/get-font-family :monospace) ", 'JetBrains Mono', 'Courier New', monospace")
                                          "'JetBrains Mono', 'Courier New', monospace")
                           :font-weight "bold"
                           :font-size (str font-size "px")
                           :text-align "center"
                           :word-break "break-word"
                           :line-height "1.1"
                           :max-width "100%"}}
             (if (number? single-value)
               (format-number single-value)
               (str single-value))]]
           ;; Render as virtual grid for multiple rows/columns
           [:div.results
            {:style {:flex 1
                     ;:margin-top "10px"
                     :min-height 0  ;; Important for flex children to shrink properly
                     :display "flex"
                     :flex-direction "column"}}
            [vgrid/virtual-grid
             {:results results
              :width (- (:width actual-size) 20)  ; Account for padding
              :height (- (:height actual-size)      ; Total block height
                        200                          ; Approximate header/controls height
                        (if error 80 0))            ; Error message height if present
              :block-id id
              :sql sql  ; Pass the current SQL for generating sub-queries
              :on-cell-drag (fn [row col value event]
                             ;; Create a filter block when dragging a cell
                             (let [filter-sql (str "SELECT * FROM (" sql ") WHERE " (name col) " = '" value "'")
                                   new-block {:id (str (random-uuid))
                                            :type :query
                                            :position {:x (+ (:x actual-pos) 50) 
                                                      :y (+ (:y actual-pos) 50)}
                                            :size {:width 400 :height 300}
                                            :sql filter-sql}]
                               (js/console.log "Creating filter block from cell drag" (clj->js new-block))
                               ;; We'll dispatch this when drag ends on canvas
                               ))
              :on-column-drag (fn [col event]
                               ;; Create a GROUP BY block when dragging a column
                               (let [group-sql (str "SELECT " (name col) ", COUNT(*) as count FROM (" sql ") GROUP BY " (name col))
                                     new-block {:id (str (random-uuid))
                                               :type :query
                                               :position {:x (+ (:x actual-pos) 50) 
                                                         :y (+ (:y actual-pos) 50)}
                                               :size {:width 400 :height 300}
                                               :sql group-sql}]
                                 (js/console.log "Creating group by block from column drag" (clj->js new-block))
                                 ;; We'll dispatch this when drag ends on canvas
                                 ))
              :on-cell-click (fn [row col value]
                             (js/console.log "Cell clicked:" (str col) "=" value))}]])))]))})))

(defn chart-block [{:keys [id position size source-id chart-config z-index] :as block}]
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
              ;; (js/console.log "[CHART-BLOCK] New source connected, executing query")
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
            ;source-block (when source-id @(r/subscribe [:block source-id]))
            #_ (println "SOURCE BLOCK" source-block source-id (rq/get-block-results source-id) (keys @rq/block-results))
            ;; Get results from reactive-queries for the source block - exactly like query block does it!
            source-results (when source-id (rq/get-block-results source-id))
            ;; Extract the data - this should be the same as what the query block shows
            chart-data (:results source-results)
            #_ (when (and source-id source-results)
                (js/console.log "[CHART-BLOCK]" id "connected to" source-id 
                               ;"source-results:" (clj->js source-results)
                               ;"chart-data:" (clj->js chart-data)
                               "chart-data count:" (count chart-data)))]
    [:div.block.chart-block
     {:style (themes/apply-block-style
              {:position "absolute"
               :left (:x actual-pos)
               :top (:y actual-pos)
               :width (:width actual-size)
               :height (:height actual-size)
               :padding "10px"
               :z-index (or (:z-index block) 10)
               :display "flex"
               :flex-direction "column"}
              :chart-block)
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
                      :color (themes/get-secondary-color)
                      :font-family (themes/get-font-family :monospace)
                      :text-transform "uppercase"
                      :font-size "11px"
                      :letter-spacing "1px"}} "CHART"]
      [:button {:on-click #(do (rq/unsubscribe-block! id)
                              (r/dispatch! [:delete-block id]))
                :style {:background "none"
                        :border "none"
                        :color (themes/get-secondary-color)
                        :cursor "pointer"
                        :font-size "20px"
                        :line-height "20px"}} "×"]]
     ;; Source selector
     [:div {:style {:margin "10px 0"}}
      [:div {:style {:display "flex" :align-items "center" :gap "10px"}}
       [:label {:style {:color "#ff006e"
                        :font-family (themes/get-font-family :monospace)
                        :font-size "10px"
                        :text-transform "uppercase"}}
        "Source:"]
       (if source-id
         [:span {:style {:color "#ff4f99"
                        :font-family (themes/get-font-family :monospace)
                        :font-size "11px"}} 
          (str "#" source-id)]
         [:span {:style {:color "#ff4f99"
                        :font-family (themes/get-font-family :monospace)
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
                       :border (str "1px solid " (themes/get-secondary-color))
                       :border-radius "2px"
                       :cursor "pointer"
                       :font-family (themes/get-font-family :monospace)
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
        (let [;; Get UI settings - deref the atom directly for reactivity
              ui-settings-val @ui-settings
              font-size (or (:monaco-font-size ui-settings-val) 12)
              ;; Use local position only while dragging, otherwise use state position
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
                    :border (str "1px solid " (themes/get-secondary-color))
                    :border-radius "4px"
                    :padding "10px"
                    :z-index (or (:z-index block) 10)
                    :box-shadow (str "0 0 20px " (themes/get-secondary-color) "4C, inset 0 0 20px " (themes/get-secondary-color) "0D")
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
                     :background (str "linear-gradient(135deg, transparent 50%, " (themes/get-secondary-color) " 50%)")
                     :opacity 0.5}
             :on-mouse-down #(start-resize! id %)}]
           [:div.block-header
            {:style {:display "flex"
                     :justify-content "space-between"
                     :margin-bottom "10px"
                     :padding-bottom "5px"
                     :border-bottom (str "1px solid " (themes/get-secondary-color) "33")
                     :cursor "move"}
             :on-mouse-down #(start-drag! id %)}
            [:span {:style {:font-weight "bold" 
                            :color (themes/get-secondary-color)
                            :font-family (themes/get-font-family :monospace)
                            :text-transform "uppercase"
                            :font-size "11px"
                            :letter-spacing "1px"}} "SQL EXECUTE"]
            [:button {:on-click #(do (rq/unsubscribe-block! id)
                                    (r/dispatch! [:delete-block id]))
                      :style {:background "none"
                              :border "none"
                              :color (themes/get-secondary-color)
                              :cursor "pointer"
                              :font-size "20px"
                              :line-height "20px"}} "×"]]
           ;; SQL Editor - now using local state, expands to fill space
           [:div {:style {:flex 1
                          :display "flex"
                          :flex-direction "column"
                          :margin "10px 0"
                          :border (str "1px solid " (themes/get-secondary-color) "4C")
                          :border-radius "4px"
                          :overflow "hidden"}}
            [monaco/sql-editor
             {:value @local-sql
              :on-change #(reset! local-sql %)  ; Only update local state on typing
              :height "100%"  ; Fill available space
              :width "100%"   ; Explicitly set width
              :theme "vs-dark"
              :font-size font-size  ; Pass font-size from ui-settings
              :editor-key (str "monaco-exec-" id "-" (:width actual-size) "-" font-size)}]]  ;; Include font-size in key
           ;; Bottom controls - Execute button and output in columns
           [:div {:style {:display "flex"
                          :gap "10px"
                          :margin-top "10px"
                          :min-height "40px"}}
            ;; Output panel (left side)
            [:div {:style {:flex 1
                           :display "flex"
                           :align-items "center"}}
             (cond
               ;; Error display
               error
               [:div {:style {:padding "8px"
                             :background "rgba(255,0,0,0.1)"
                             :border "1px solid rgba(255,0,0,0.3)"
                             :border-radius "4px"
                             :color "#ff6b6b"
                             :font-family (themes/get-font-family :monospace)
                             :font-size "10px"
                             :width "100%"}}
                error]
               
               ;; Success result display
               result
               [:div {:style {:padding "8px"
                             :background (str (themes/get-primary-color) "1A")
                             :border (str "1px solid " (themes/get-primary-color) "4C")
                             :border-radius "4px"
                             :color (themes/get-primary-color)
                             :font-family (themes/get-font-family :monospace)
                             :font-size "10px"
                             :width "100%"}}
                (str "Success: " result)]
               
               ;; Default empty state
               :else
               [:div {:style {:padding "8px"
                             :color (str (themes/get-primary-color) "66")
                             :font-family (themes/get-font-family :monospace)
                             :font-size "10px"}}
                "Ready to execute..."])]
            ;; Execute button (right side)
            [:button
             {:style {:padding "8px 20px"
                      :background (str "linear-gradient(0deg, " (themes/get-primary-color) " 0%, " (themes/get-secondary-color) " 100%)")
                      :color "#0a0a0a"
                      :border "none"
                      :border-radius "4px"
                      :cursor "pointer"
                      :font-weight "bold"
                      :text-transform "uppercase"
                      :font-size "11px"
                      :letter-spacing "1px"
                      :white-space "nowrap"
                      :box-shadow (str "0 2px 10px " (themes/get-primary-color) "33")
                      :transition "all 0.3s"}
              :on-mouse-over #(set! (.. % -currentTarget -style -boxShadow) 
                                   (str "0 4px 20px " (themes/get-primary-color) "66"))
              :on-mouse-out #(set! (.. % -currentTarget -style -boxShadow) 
                                  (str "0 2px 10px " (themes/get-primary-color) "33"))
              :on-click (fn []
                         ;; Sync local SQL to global state on execute
                         (let [current-sql @local-sql]
                           (r/dispatch! [:update-block id {:sql current-sql}])
                           (-> (r/sql-exec! current-sql)
                               (.then (fn [response]
                                       (if (:error response)
                                         (r/dispatch! [:update-block id {:error (:error response) :result nil}])
                                         (r/dispatch! [:update-block id {:result (:result response) :error nil}])))))))}
             "EXECUTE"]]]))})))  ; Close reagent-render and create-class

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
                    :z-index (or (:z-index block) 10)
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
                             :color (or (themes/get-theme-property :pop-4) "#9b59b6")
                             :font-family (themes/get-font-family :monospace)
                             :text-transform "uppercase"
                             :font-size "11px"
                             :letter-spacing "1px"}} "REACTOR DEBUG"]
             [:select {:style {:background "rgba(0,0,0,0.03)"
                               :color (or (themes/get-theme-property :pop-4) "#9b59b6")
                               :border "1px solid rgba(155,89,182,0.3)"
                               :border-radius "2px"
                               :padding "2px 5px"
                               :font-family (themes/get-font-family :monospace)
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
                              :color (or (themes/get-theme-property :pop-4) "#9b59b6")
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
                           :font-family (themes/get-font-family :monospace)}}
              "Loading..."]
             
             error
             [:div {:style {:flex 1
                           :padding "10px"
                           :background "rgba(255,0,0,0.1)"
                           :border "1px solid rgba(255,0,0,0.3)"
                           :border-radius "4px"
                           :color "#ff6b6b"
                           :font-family (themes/get-font-family :monospace)
                           :font-size "11px"}}
              error]
             
             (and results (seq results))
             [:div.results
              {:style {:flex 1
                       :min-height 0
                       :overflow "auto"
                       :background "rgba(0,0,0,0.03)"
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
                 (doall
                  (for [col (keys (first results))]
                    ^{:key col}
                    [:th {:style {:text-align "left"
                                  :padding "4px"
                                  :color (or (themes/get-theme-property :pop-4) "#9b59b6")
                                  :border-bottom "1px solid rgba(155,89,182,0.2)"
                                  :font-family (themes/get-font-family :monospace)
                                  :text-transform "uppercase"
                                  :font-size "9px"}} (name col)]))]]
               [:tbody
                (doall
                 (for [row results]
                   ^{:key (or (:_id row) (str (hash row)))}
                   [:tr
                    (doall
                     (for [col (keys (first results))]
                       ^{:key col}
                       [:td {:style {:padding "4px"
                                     :color "#d8b4fe"
                                     :font-family (themes/get-font-family :monospace)
                                     :font-size "9px"
                                     :overflow "hidden"
                                     :text-overflow "ellipsis"
                                     :white-space "nowrap"}}
                        (str (get row col))]))]))]]]
             
             results  ;; Empty results array
             [:div {:style {:flex 1
                           :display "flex"
                           :align-items "center"
                           :justify-content "center"
                           :color "#9b59b6"
                           :font-family (themes/get-font-family :monospace)
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
                           :font-family (themes/get-font-family :monospace)
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
     {:style (themes/apply-block-style
              {:position "absolute"
               :left (:x actual-pos)
               :top (:y actual-pos)
               :width (:width actual-size)
               :height (:height actual-size)
               :padding "10px"
               :z-index (or (:z-index block) 10)
               :display "flex"
               :flex-direction "column"}
              :edn-block)
      :draggable false}
     ;; Resize handle
     [:div.resize-handle
      {:style {:position "absolute"
               :bottom 0
               :right 0
               :width "15px"
               :height "15px"
               :cursor "nwse-resize"
               :background (str "linear-gradient(135deg, transparent 50%, " (themes/get-primary-color) " 50%)")
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
      [:span {:style {:color (themes/get-primary-color)
                      :font-family (themes/get-font-family :monospace)
                      :font-size "12px"
                      :text-transform "uppercase"
                      :letter-spacing "1px"}}
       "EDN BROWSER"]
      [:button {:style {:background "none"
                        :border "none"
                        :color (themes/get-tertiary-color)
                        :cursor "pointer"
                        :font-size "20px"
                        :line-height "20px"
                        :padding "0 5px"}
                :on-click (fn [e]
                           (.stopPropagation ^js e)
                           (r/dispatch! [:delete-block id]))}
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
            [:span {:style {:color (themes/get-primary-color)
                            :font-family (themes/get-font-family :monospace)
                            :font-size "10px"
                            :opacity 0.7}}
             "FIELD:"]
            [:select {:style {:background "rgba(0,255,212,0.1)"
                              :color (themes/get-tertiary-color)
                              :border "1px solid rgba(0,255,212,0.3)"
                              :border-radius "2px"
                              :padding "2px 5px"
                              :font-family (themes/get-font-family :monospace)
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
            [:span {:style {:color (themes/get-primary-color)
                            :font-family (themes/get-font-family :monospace)
                            :font-size "10px"
                            :opacity 0.7}}
             "ROW:"]
            [:select {:style {:background "rgba(0,255,212,0.1)"
                              :color (themes/get-tertiary-color)
                              :border "1px solid rgba(0,255,212,0.3)"
                              :border-radius "2px"
                              :padding "2px 5px"
                              :font-family (themes/get-font-family :monospace)
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
                             :color (themes/get-tertiary-color)
                             :border "1px solid rgba(0,255,212,0.3)"
                             :border-radius "2px 0 0 2px"
                             :cursor "pointer"
                             :font-size "9px"
                             :font-family (themes/get-font-family :monospace)}
                     :on-click #(reset! local-view-mode :monaco)}
            "CODE"]
           [:button {:style {:padding "3px 8px"
                             :background (if (= @local-view-mode :tree)
                                          "rgba(0,255,212,0.2)"
                                          "rgba(0,255,212,0.05)")
                             :color (themes/get-tertiary-color)
                             :border "1px solid rgba(0,255,212,0.3)"
                             :border-radius "0 2px 2px 0"
                             :cursor "pointer"
                             :font-size "9px"
                             :font-family (themes/get-font-family :monospace)
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
                              :color (themes/get-tertiary-color)
                              :border "1px solid rgba(0,255,212,0.2)"
                              :border-radius "2px"
                              :padding "2px 5px"
                              :font-family (themes/get-font-family :monospace)
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
                            :color (themes/get-tertiary-color)
                            :font-family (themes/get-font-family :monospace)
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
        [:div {:style {:color (themes/get-primary-color)
                       :font-family (themes/get-font-family :monospace)
                       :font-size "11px"
                       :opacity 0.7}}
         "Not connected to a query block"]
        [:button {:style {:padding "6px 12px"
                          :background "transparent"
                          :color (themes/get-tertiary-color)
                          :border (str "1px solid " (themes/get-tertiary-color))
                          :border-radius "2px"
                          :cursor "pointer"
                          :font-family (themes/get-font-family :monospace)
                          :font-size "10px"}
                  :on-click (fn []
                              (reset! connection-mode {:source-id id}))}
         "CONNECT TO QUERY"]])]))))

(defn rules-block [{:keys [id position size] :as block}]
  (let [;; State for rules data and stats
        rules-data (reagent/atom nil)
        exec-stats (reagent/atom nil)
        cascade-data (reagent/atom nil)
        selected-rule (reagent/atom nil)
        view-mode (reagent/atom :overview) ;; :overview, :cascade, :details
        
        ;; Fetch rules and stats
        fetch-rules! (fn []
                       (js/console.log "[RULES-BLOCK] Fetching rules...")
                       (-> (r/sql-query! 
                            "SELECT * FROM reactor_rules WHERE enabled = true ORDER BY rule_id")
                           (.then (fn [result]
                                   (js/console.log "[RULES-BLOCK] Rules result:" (clj->js result))
                                   (reset! rules-data (:results result))
                                   (js/console.log "[RULES-BLOCK] Rules data atom:" (clj->js @rules-data))))
                           (.catch (fn [err]
                                    (js/console.error "[RULES-BLOCK] Error fetching rules:" err)))))
        
        fetch-stats! (fn []
                      (-> (r/sql-query!
                           "SELECT rule_id, 
                                   COUNT(*) as execution_count,
                                   SUM(CASE WHEN action_result::text NOT LIKE '%error%' THEN 1 ELSE 0 END) as success_count,
                                   SUM(CASE WHEN action_result::text LIKE '%error%' THEN 1 ELSE 0 END) as error_count,
                                   AVG(CAST(execution_time_ms AS DOUBLE)) as avg_time_ms,
                                   MAX(executed_at) as last_executed
                            FROM reactor_rule_executions
                            GROUP BY rule_id")
                          (.then (fn [result]
                                  (reset! exec-stats (reduce (fn [m row]
                                                             (assoc m (:rule_id row) row))
                                                           {}
                                                           (:results result)))))))
        
        fetch-cascade! (fn []
                        (-> (r/sql-query!
                             "SELECT r1.rule_id as source_rule, 
                                     r2.rule_id as target_rule,
                                     COUNT(*) as execution_count
                              FROM reactor_rule_executions r1 
                              JOIN reactor_rule_executions r2 
                                ON r1.correlation_id = r2.correlation_id 
                                AND r1.triggered_by = 'table_change' 
                                AND r2.triggered_by = 'rule_cascade'
                              GROUP BY r1.rule_id, r2.rule_id")
                            (.then (fn [result]
                                    (js/console.log "[RULES-BLOCK] Cascade result with counts:" (clj->js result))
                                    (reset! cascade-data (:results result))))))]
    
    ;; Initial fetch
    (reagent/create-class
     {:component-did-mount
      (fn []
        (fetch-rules!)
        (fetch-stats!)
        (fetch-cascade!)
        ;; Refresh every 5 seconds
        (js/setInterval (fn []
                         (fetch-stats!)
                         (fetch-cascade!))
                       5000))
      
      :reagent-render
      (fn [{:keys [id position size] :as block}]
        (let [is-dragging? (= id (:block-id @drag-state))
              is-resizing? (= id (:block-id @resize-state))
              actual-pos (if (or is-dragging? (get @local-positions id))
                          (get @local-positions id position)
                          position)
              actual-size (if (or is-resizing? (get @local-sizes id))
                           (get @local-sizes id size)
                           size)]
          [:div.block
           {:style {:position "absolute"
                   :left (:x actual-pos)
                   :top (:y actual-pos)
                   :width (:width actual-size)
                   :height (:height actual-size)
                   :background "linear-gradient(135deg, #1a1a2e 0%, #2d2d44 100%)"
                   :border "1px solid #9933ff"
                   :border-radius "8px"
                   :padding "10px"
                   :overflow "hidden"
                   :display "flex"
                   :flex-direction "column"}}
           
           ;; Header
           [:div {:style {:display "flex"
                         :justify-content "space-between"
                         :align-items "center"
                         :margin-bottom "10px"
                         :border-bottom "1px solid #9933ff"
                         :padding-bottom "5px"
                         :cursor "move"}
                  :on-mouse-down #(start-drag! id %)}
            [:div {:style {:display "flex"
                          :align-items "center"
                          :gap "10px"}}
             [:span {:style {:color "#9933ff"
                            :font-weight "bold"
                            :font-size "14px"}}
              "⚙ RULES ENGINE"]
             [:span {:style {:color "#666"
                            :font-size "12px"}}
              (str (count @rules-data) " active rules")]]
            
            ;; View mode toggle
            [:div {:style {:display "flex"
                          :gap "5px"}}
             [:button {:style {:padding "2px 8px"
                              :background (if (= @view-mode :overview) "#9933ff" "#333")
                              :color "white"
                              :border "none"
                              :border-radius "3px"
                              :cursor "pointer"
                              :font-size "11px"}
                      :on-click #(reset! view-mode :overview)}
              "Overview"]
             [:button {:style {:padding "2px 8px"
                              :background (if (= @view-mode :cascade) "#9933ff" "#333")
                              :color "white"
                              :border "none"
                              :border-radius "3px"
                              :cursor "pointer"
                              :font-size "11px"}
                      :on-click #(reset! view-mode :cascade)}
              "Cascades"]
             [:button {:style {:padding "2px 8px"
                              :background (if (= @view-mode :details) "#9933ff" "#333")
                              :color "white"
                              :border "none"
                              :border-radius "3px"
                              :cursor "pointer"
                              :font-size "11px"}
                      :on-click #(reset! view-mode :details)}
              "Details"]]
            
            ;; Close button
            [:button {:style {:background "transparent"
                             :border "none"
                             :color "#ff4f99"
                             :cursor "pointer"
                             :font-size "16px"}
                     :on-click #(r/dispatch! [:delete-block id])}
             "×"]]
           
           ;; Content area
           [:div {:style {:flex 1
                         :overflow-y "auto"
                         :padding-right "5px"}}
            (case @view-mode
              :overview
              [:div
               ;; Rules list with stats
               (doall 
                (for [rule @rules-data]
                 (let [rule-id (:rule_id rule)
                       stats (get @exec-stats rule-id)
                       success-rate (if (and stats (> (:execution_count stats) 0))
                                     (* 100 (/ (:success_count stats) 
                                             (:execution_count stats)))
                                     0)]
                   ^{:key rule-id}
                   [:div {:style {:background "rgba(0,0,0,0.03)"
                                 :border "1px solid #444"
                                 :border-radius "4px"
                                 :padding "8px"
                                 :margin-bottom "8px"
                                 :cursor "pointer"}
                          :on-click #(do (reset! selected-rule rule)
                                       (reset! view-mode :details))}
                    [:div {:style {:display "flex"
                                  :justify-content "space-between"
                                  :align-items "center"}}
                     [:div
                      [:div {:style {:color "#9933ff"
                                    :font-weight "bold"
                                    :font-size "12px"}}
                       rule-id]
                      [:div {:style {:color "#888"
                                    :font-size "10px"}}
                       (str (:trigger_table rule) " → " (:action_table rule))]]
                     
                     (when stats
                       [:div {:style {:text-align "right"}}
                        [:div {:style {:font-size "10px"
                                      :color (if (>= success-rate 90) (themes/get-primary-color) 
                                               (if (>= success-rate 50) "#ffaa00" "#ff4444"))}}
                         (str (.toFixed success-rate 1) "% success")]
                        [:div {:style {:font-size "9px"
                                      :color "#666"}}
                         (str (:execution_count stats) " runs")]])]
                    
                    [:div {:style {:margin-top "5px"
                                  :font-size "10px"
                                  :color "#aaa"}}
                     (:description rule)]])))]
              
              :cascade
              [:div {:style {:padding "10px"}}
                (if (empty? @cascade-data)
                  [:div {:style {:color "#666"
                                :text-align "center"
                                :padding "20px"
                                :background "rgba(0,0,0,0.03)"
                                :border-radius "4px"}}
                   "No cascade relationships detected yet. Rules will cascade when one rule's action writes to a table that another rule watches."]
                  (let [;; Build adjacency map with counts
                        edge-counts (reduce (fn [m {:keys [source_rule target_rule execution_count]}]
                                            (assoc m [source_rule target_rule] execution_count))
                                          {}
                                          @cascade-data)
                        ;; Create Mermaid syntax with execution counts on edges
                        mermaid-id (str "mermaid-" (gensym))
                        mermaid-text (str "graph LR\n"
                                         (clojure.string/join "\n"
                                           (for [{:keys [source_rule target_rule execution_count]} @cascade-data]
                                             (str "    " source_rule " -->|" execution_count "x| " target_rule))))]
                    [:div
                     ;; Mermaid Diagram
                     [:div {:style {:background "rgba(0,0,0,0.04)"
                                   :padding "20px"
                                   :border-radius "8px"
                                   :margin-bottom "15px"}}
                      [:div {:style {:color "#9933ff"
                                    :font-size "14px"
                                    :font-weight "bold"
                                    :margin-bottom "20px"}}
                       "Rule Cascade Flow"]
                      
                      ;; Mermaid diagram container
                      [:div {:style {:background "#1a1a1a"
                                    :padding "20px"
                                    :border-radius "6px"
                                    :border "1px solid #333"
                                    :min-height "200px"}
                             :ref (fn [el]
                                   (when el
                                     ;; Render Mermaid diagram when element is mounted
                                     (js/setTimeout
                                      #(try
                                        (if (and js/window js/window.mermaid)
                                          (do
                                            ;; Use mermaid.run for newer versions
                                            (set! (.-innerHTML el) 
                                                 (str "<div class='mermaid'>" mermaid-text "</div>"))
                                            (if (.-run js/window.mermaid)
                                              ;; Mermaid v10+ uses .run()
                                              (.run js/window.mermaid)
                                              ;; Older versions use .init()
                                              (.init js/window.mermaid)))
                                          ;; Fallback if mermaid not loaded
                                          (set! (.-innerHTML el) 
                                               (str "<pre style='color: #00ffd4; font-size: 11px;'>" 
                                                   mermaid-text "</pre>")))
                                        (catch :default e
                                          (js/console.error "Mermaid render error:" e)
                                          ;; Fallback to showing the text
                                          (set! (.-innerHTML el) 
                                               (str "<pre style='color: #00ffd4; font-size: 11px;'>" 
                                                   mermaid-text "</pre>"))))
                                      200)))}
                       ;; Initial loading text
                       [:div {:style {:color "#666"
                                     :font-size "12px"}}
                        "Loading diagram..."]]
                      
                      ;; Legend
                      [:div {:style {:margin-top "15px"
                                    :padding "10px"
                                    :background "rgba(0,0,0,0.03)"
                                    :border-radius "4px"
                                    :font-size "10px"
                                    :color "#888"}}
                       [:div {:style {:margin-bottom "5px"}}
                        [:span {:style {:color (themes/get-primary-color)}} "→"] 
                        " Arrows show cascade triggers"]
                       [:div {:style {:margin-bottom "5px"}}
                        [:span {:style {:color "#9933ff"
                                      :background "rgba(153, 51, 255, 0.2)"
                                      :padding "1px 4px"
                                      :border-radius "3px"}}
                         "Nx"] 
                        " Numbers show execution count"]
                       [:div
                        "Higher counts indicate more frequently triggered cascades"]]]
                     
                     ;; Raw Mermaid text (collapsible)
                     [:details {:style {:margin-top "10px"}}
                      [:summary {:style {:color "#666"
                                        :font-size "10px"
                                        :cursor "pointer"}}
                       "View Mermaid source"]
                      [:pre {:style {:margin "10px 0 0 0"
                                    :padding "10px"
                                    :background "rgba(0,0,0,0.05)"
                                    :border-radius "4px"
                                    :color (themes/get-tertiary-color)
                                    :font-family (themes/get-font-family :monospace)
                                    :font-size "10px"
                                    :white-space "pre-wrap"
                                    :overflow-x "auto"}}
                       mermaid-text]]]))]
              
              :details
              (if @selected-rule
                [:div
                 [:button {:style {:background "#333"
                                  :color "#9933ff"
                                  :border "1px solid #9933ff"
                                  :padding "4px 8px"
                                  :border-radius "4px"
                                  :cursor "pointer"
                                  :margin-bottom "10px"}
                          :on-click #(reset! view-mode :overview)}
                  "← Back to Overview"]
                 
                 ;; Rule details with EDN viewer
                 [:div {:style {:background "rgba(0,0,0,0.03)"
                               :padding "10px"
                               :border-radius "4px"}}
                  [:div {:style {:color "#9933ff"
                                :font-weight "bold"
                                :margin-bottom "10px"}}
                   (:rule_id @selected-rule)]
                  
                  ;; Parse and display condition and action
                  (let [condition-sql (:condition_sql @selected-rule)
                        action-sql (:action_sql @selected-rule)
                        ;; Try to parse as EDN, fall back to string
                        condition (if (string? condition-sql)
                                   condition-sql
                                   (try (reader/read-string condition-sql)
                                        (catch :default _ condition-sql)))
                        action (if (string? action-sql)
                                (try (reader/read-string action-sql)
                                     (catch :default _ action-sql))
                                action-sql)]
                    [:div
                     [:div {:style {:margin-bottom "10px"}}
                      [:div {:style {:color "#888"
                                    :font-size "10px"
                                    :margin-bottom "5px"}}
                       "CONDITION SQL:"]
                      (if (string? condition)
                        [:div {:style {:background "rgba(0,0,0,0.04)"
                                      :padding "8px"
                                      :border-radius "4px"
                                      :max-height "300px"
                                      :overflow-y "auto"}}
                         [:pre {:style {:margin 0
                                       :color (themes/get-tertiary-color)
                                       :font-family (themes/get-font-family :monospace)
                                       :font-size "11px"
                                       :white-space "pre"
                                       :overflow-x "auto"}}
                          condition]]
                        [tree/edn-tree-view {:data condition
                                            :initial-depth 2}])]
                     
                     [:div {:style {:margin-bottom "10px"}}
                      [:div {:style {:color "#888"
                                    :font-size "10px"
                                    :margin-bottom "5px"}}
                       "ACTION SQL:"]
                      (if (and (string? action) (not (coll? action)))
                        [:div {:style {:background "rgba(0,0,0,0.04)"
                                      :padding "8px"
                                      :border-radius "4px"
                                      :max-height "300px"
                                      :overflow-y "auto"}}
                         [:pre {:style {:margin 0
                                       :color (themes/get-tertiary-color)
                                       :font-family (themes/get-font-family :monospace)
                                       :font-size "11px"
                                       :white-space "pre"
                                       :overflow-x "auto"}}
                          action]]
                        [tree/edn-tree-view {:data action
                                            :initial-depth 2}])]
                     
                     ;; Stats for this rule
                     (when-let [stats (get @exec-stats (:rule_id @selected-rule))]
                       [:div {:style {:margin-top "10px"
                                     :padding-top "10px"
                                     :border-top "1px solid #444"}}
                        [:div {:style {:color "#888"
                                      :font-size "10px"
                                      :margin-bottom "5px"}}
                         "EXECUTION STATS:"]
                        [:div {:style {:display "grid"
                                      :grid-template-columns "1fr 1fr"
                                      :gap "5px"
                                      :font-size "10px"}}
                         [:div [:span {:style {:color "#666"}} "Runs: "] 
                          [:span {:style {:color "#9933ff"}} (:execution_count stats)]]
                         [:div [:span {:style {:color "#666"}} "Success: "] 
                          [:span {:style {:color (themes/get-primary-color)}} (:success_count stats)]]
                         [:div [:span {:style {:color "#666"}} "Errors: "] 
                          [:span {:style {:color "#ff4444"}} (:error_count stats)]]
                         [:div [:span {:style {:color "#666"}} "Avg time: "] 
                          [:span {:style {:color "#ffaa00"}} 
                           (str (.toFixed (or (:avg_time_ms stats) 0) 2) " ms")]]]])])]]
                
                [:div {:style {:color "#666"
                              :text-align "center"
                              :margin-top "20px"}}
                 "Select a rule from Overview to see details"]))
           
           ;; Resize handle
           [:div {:style {:position "absolute"
                         :bottom 0
                         :right 0
                         :width "10px"
                         :height "10px"
                         :background "#9933ff"
                         :cursor "se-resize"}
                  :on-mouse-down #(start-resize! id %)}]]]))})))


(defn render-block [block]
  ;(js/console.log "Rendering block:" (clj->js block) "Type:" (:type block))
  (let [block-type (if (string? (:type block))
                     (keyword (:type block))
                     (:type block))]
    (case block-type
      :query [query-block block]
      :chart [chart-block block]
      :sql-exec [sql-exec-block block]
      :debug [debug-block block]
      :edn-browser [edn-browser-block block]
      :rules [rules-block block]
      :iframe (let [;; Use local position only while dragging
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
                           :background "linear-gradient(135deg, #1a1a2e 0%, #2a1a3e 100%)"
                           :border "1px solid #8a2be2"
                           :border-radius "2px"
                           :box-shadow "0 4px 20px rgba(138,43,226,0.3)"
                           :z-index (or (:z-index block) 10)
                           :display "flex"
                           :flex-direction "column"
                           :transition (when-not (or is-dragging? is-resizing?)
                                        "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)")}}
                  ;; Header
                  [:div.block-header
                   {:style {:padding "10px"
                            :background "rgba(138,43,226,0.1)"
                            :border-bottom "1px solid rgba(138,43,226,0.3)"
                            :cursor "move"
                            :display "flex"
                            :justify-content "space-between"
                            :align-items "center"}
                    :on-mouse-down #(start-drag! (:id block) %)}
                   [:span {:style {:color "#8a2be2"
                                   :font-family (themes/get-font-family :monospace)
                                   :text-transform "uppercase"
                                   :font-size "11px"
                                   :letter-spacing "1px"}} "IFRAME"]
                   [:button {:on-click #(r/dispatch! [:delete-block (:id block)])
                             :style {:background "transparent"
                                     :border "none"
                                     :color "#8a2be2"
                                     :cursor "pointer"
                                     :font-size "16px"
                                     :padding "0 5px"}}
                    "×"]]
                  ;; Content
                  [:div {:style {:flex 1
                                 :overflow "hidden"
                                 :display "flex"}}
                   [iframe-block/iframe-block (assoc block :blocks @(r/subscribe [:blocks]))]]
                  ;; Resize handle
                  [:div.resize-handle
                   {:style {:position "absolute"
                            :bottom 0
                            :right 0
                            :width "15px"
                            :height "15px"
                            :cursor "nwse-resize"
                            :background "radial-gradient(circle at center, rgba(138,43,226,0.5) 0%, transparent 70%)"}
                    :on-mouse-down #(start-resize! (:id block) %)}]])
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
                       :z-index (or (:z-index block) 10)
                       :display "flex"
                       :flex-direction "column"
                       :transition (when-not (or is-dragging? is-resizing?)
                                    "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)")}}
              ;; Header
              [:div.block-header
               {:style {:padding "10px"
                        :background "rgba(255,79,153,0.1)"
                        :border-bottom "1px solid rgba(255,79,153,0.3)"
                        :cursor (if @connection-mode "pointer" "move")
                        :display "flex"
                        :justify-content "space-between"
                        :align-items "center"}
                :on-mouse-down (fn [e]
                                (when-not @connection-mode
                                  (start-drag! (:id block) e)))
                :on-click (fn [e]
                           (when-let [conn @connection-mode]
                             (.stopPropagation ^js e)
                             ;; Update the block that initiated connection to link to this TAP block
                             (r/dispatch! [:update-block (:source-id conn) {:source-id (:id block)}])
                             (reset! connection-mode nil)))}
               [:span {:style {:color "#ff4f99"
                               :font-family (themes/get-font-family :monospace)
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
               [sql-tap-block/sql-tap-block (assoc block :connection-mode connection-mode)]]
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
                             :border (str "1px solid " (themes/get-primary-color))
                             :border-radius "4px"
                             :box-shadow (str "0 4px 20px " (themes/get-primary-color) "4C")
                             :z-index (or (:z-index block) 10)
                             :display "flex"
                             :flex-direction "column"
                             :transition (when-not (or is-dragging? is-resizing?)
                                          "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)")}}
                    ;; Header
                    [:div.block-header
                     {:style {:padding "10px"
                              :background (str (themes/get-primary-color) "1A")
                              :border-bottom (str "1px solid " (themes/get-primary-color) "4C")
                              :cursor "move"
                              :display "flex"
                              :justify-content "space-between"
                              :align-items "center"}
                      :on-mouse-down #(start-drag! (:id block) %)}
                     [:span {:style {:color (themes/get-primary-color)
                                     :font-family (themes/get-font-family :monospace)
                                     :text-transform "uppercase"
                                     :font-size "11px"
                                     :letter-spacing "1px"}} "RULES"]
                     [:button {:on-click #(r/dispatch! [:delete-block (:id block)])
                               :style {:background "transparent"
                                       :border "none"
                                       :color (themes/get-primary-color)
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
                              :background (str "radial-gradient(circle at center, " (themes/get-primary-color) "80 0%, transparent 70%)")}
                      :on-mouse-down #(start-resize! (:id block) %)}]])
      (do
        (js/console.warn "Unknown block type:" block-type "original:" (:type block))
        nil))))

;; ============= Canvas Component =============

(defonce table-dropdown-open (reagent/atom false))
(defonce table-list (reagent/atom {:public [] :system [] :reactor []}))
(defonce theme-dropdown-open (reagent/atom false))

(defn canvas []
  (let [blocks (r/subscribe [:blocks])]
    (fn []
      [:div#canvas
       {:style (themes/apply-canvas-style
                {:position "relative"
                 :width "100%"
                 :height "calc(100vh - 120px)"
                 :background "radial-gradient(circle at 20% 50%, #1a1a2e 0%, #0a0a0a 100%)"
                 :overflow "auto"
                 :box-shadow "inset 0 0 100px rgba(0,0,0,0.5)"
                 :cursor (when @connection-mode "crosshair")})
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
                         (stop-resize!))
        :on-drag-enter (fn [e]
                        (.preventDefault e)
                        (js/console.log "Drag entered canvas")
                        (set! (.-dropEffect (.-dataTransfer e)) "copy"))
        :on-drag-leave (fn [e]
                        (js/console.log "Drag left canvas"))
        :on-drag-over (fn [e]
                       (.preventDefault e)  ; Allow drop
                       (.stopPropagation e) ; Stop event bubbling
                       (set! (.-dropEffect (.-dataTransfer e)) "copy")
                       ;; Log occasionally to avoid spam
                       (when (= 0 (mod (.-timeStamp e) 100))
                         (js/console.log "Drag over canvas")))
        :on-drop (fn [e]
                  (.preventDefault e)
                  (js/console.log "DROP event fired")
                  (let [dt (.-dataTransfer e)
                        text-data (.getData dt "text/plain")
                        has-cell? (= (.getData dt "reactor/grid-cell") "true")
                        has-column? (= (.getData dt "reactor/grid-column") "true")]
                    (js/console.log "Data transfer - text:" text-data "cell:" has-cell? "column:" has-column?)
                    ;; Check if this is a grid drag (check text/plain as fallback)
                    (when (or has-cell? has-column? (= text-data "column-drag"))
                      (js/console.log "Grid drag detected")
                      (let [canvas-rect (.getBoundingClientRect (.-currentTarget e))
                            drop-x (- (.-clientX e) (.-left canvas-rect))
                            drop-y (- (.-clientY e) (.-top canvas-rect))]
                        (js/console.log "Drop position:" drop-x drop-y)
                        ;; Call with callback for async handling
                        (if-let [sync-block (vgrid/create-block-from-drag! 
                                              drop-x drop-y
                                              (fn [new-block]
                                                (js/console.log "Async block created:" new-block)
                                                (when new-block
                                                  (r/dispatch! [:add-block new-block])
                                                  (vgrid/handle-drag-end! e))))]
                          ;; If synchronous block returned, handle it
                          (do
                            (js/console.log "Sync block created:" sync-block)
                            (r/dispatch! [:add-block sync-block])
                            (vgrid/handle-drag-end! e))
                          ;; For async, don't clear drag state yet - callback will do it
                          (js/console.log "Waiting for async block creation..."))))))}
       ;; Grid overlay effect
       [:div {:style {:position "absolute"
                     :width "100%"
                     :height "100%"
                     :background-image (str "linear-gradient(" (themes/get-primary-color) "08 1px, transparent 1px), linear-gradient(90deg, " (themes/get-primary-color) "08 1px, transparent 1px)")
                     :background-size "50px 50px"
                     :pointer-events "none"}}]
       ;; Connection lines SVG
       (let [local-pos @local-positions
             local-sz @local-sizes
             ;; Only deref blocks here for connection lines
             all-blocks @blocks
             ;; Get implicit template connections
             implicit-connections (resolver/get-implicit-connections)
             ;; Build explicit connection lines
             explicit-lines (->> (for [[block-id block] all-blocks
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
                                     :stroke (themes/get-primary-color)
                                     :stroke-width 4
                                     :opacity 1}])))
                                 (remove nil?)
                                 vec)
             ;; Build implicit connection lines (dashed for template dependencies)
             implicit-lines (->> (for [{:keys [from to]} implicit-connections]
                                  (let [source-id (keyword from)
                                        target-id (keyword to)
                                        source-block (get all-blocks source-id)
                                        target-block (get all-blocks target-id)
                                        source-pos (or (get local-pos source-id) 
                                                      (:position source-block))
                                        target-pos (or (get local-pos target-id)
                                                      (:position target-block))
                                        source-size (or (get local-sz source-id) 
                                                       (:size source-block) 
                                                       {:width 400 :height 300})
                                        target-size (or (get local-sz target-id)
                                                       (:size target-block) 
                                                       {:width 400 :height 300})]
                                    (when (and source-pos target-pos)
                                      [:line {:key (str "implicit-line-" from "-" to)
                                             :x1 (+ (:x source-pos) (/ (:width source-size) 2))
                                             :y1 (+ (:y source-pos) (/ (:height source-size) 2))
                                             :x2 (+ (:x target-pos) (/ (:width target-size) 2))
                                             :y2 (+ (:y target-pos) (/ (:height target-size) 2))
                                             :stroke "#ff4f99"
                                             :stroke-width 4
                                             :stroke-dasharray "5,5"
                                             :opacity 0.7}])))
                                (remove nil?)
                                vec)]
         [:svg {:style {:position "absolute"
                       :top 0
                       :left 0
                       :width "100%"
                       :height "100%"
                       :pointer-events "none"
                       :z-index 1}
                :id "connection-svg"}
          ;; Add implicit template connection lines first (so they render behind)
          (for [line implicit-lines]
            line)
          ;; Add explicit connection lines
          (for [line explicit-lines]
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
                       :font-family (themes/get-font-family :monospace)
                       :font-size "12px"
                       :font-weight "bold"
                       :text-transform "uppercase"
                       :letter-spacing "1px"
                       :z-index 1000
                       :box-shadow "0 0 30px rgba(255,0,110,0.5)"}}
          "Click on a Query Block header to connect"])
       (if (empty? @blocks)
         [:div {:style {:color (themes/get-primary-color)
                       :font-family (themes/get-font-family :monospace)
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
            :background (or (themes/get-theme-property :toolbar-background)
                          (themes/get-theme-property :base-block-color)
                          "linear-gradient(90deg, #0a0a0a 0%, #1a1a2e 100%)")
            :border-bottom (str "1px solid " (themes/get-primary-color) "33")
            :display "flex"
            :align-items "center"
            :padding "0 20px"
            :gap "10px"
            :box-shadow "0 2px 20px rgba(0,0,0,0.5)"}}
   [:button
    {:style {:padding "8px 16px"
             :background "transparent"
             :color (themes/get-primary-color)
             :border (str "1px solid " (themes/get-primary-color))
             :border-radius "2px"
             :cursor "pointer"
             :font-family (themes/get-font-family :monospace)
             :font-size "12px"
             :text-transform "uppercase"
             :letter-spacing "1px"
             :transition "all 0.3s"}
     :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) (str (themes/get-primary-color) "1A"))
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
              :color (themes/get-primary-color)
              :border (str "1px solid " (themes/get-primary-color))
              :border-radius "2px"
              :cursor "pointer"
              :font-family (themes/get-font-family :monospace)
              :font-size "12px"
              :text-transform "uppercase"
              :letter-spacing "1px"
              :transition "all 0.3s"
              :display "flex"
              :align-items "center"
              :gap "5px"}
      :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) (str (themes/get-primary-color) "1A"))
      :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
      :on-click (fn []
                 ;; Check if we're about to open the dropdown
                 (let [will-open (not @table-dropdown-open)]
                   (swap! table-dropdown-open not)
                   ;; Fetch tables when opening dropdown
                   (when will-open
                     (-> (js/fetch "http://localhost:5000/api/tables")
                         (.then #(.json %))
                         (.then (fn [data]
                                 (let [tables-data (js->clj data :keywordize-keys true)]
                                   ;; Server already separates public, reactor, and system tables
                                   (reset! table-list {:public (get tables-data :public [])
                                                      :reactor (get tables-data :reactor [])
                                                      :system (get tables-data :system [])}))))))))}
     "+ TABLE"
     [:span {:style {:font-size "10px"}} "▼"]]
    ;; Dropdown menu
    (when @table-dropdown-open
      [:div {:style {:position "absolute"
                     :top "100%"
                     :left 0
                     :margin-top "5px"
                     :background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
                     :border (str "1px solid " (themes/get-primary-color))
                     :border-radius "4px"
                     :min-width "200px"
                     :max-height "400px"
                     :overflow-y "auto"
                     :z-index 1000
                     :box-shadow (str "0 4px 20px " (themes/get-primary-color) "4C")}}
       ;; User tables (dynamically loaded)
       [:div {:style {:padding "5px 10px"
                      :color (themes/get-primary-color)
                      :font-family (themes/get-font-family :monospace)
                      :font-size "10px"
                      :text-transform "uppercase"
                      :border-bottom (str "1px solid " (themes/get-primary-color) "33")
                      :opacity 0.7}}
        "Data Tables"]
       (doall
        (for [table (filter #(not (str/starts-with? % "test_")) (:public @table-list))]
          ^{:key table}
          [:div {:style {:padding "8px 15px"
                         :color (themes/get-secondary-color)
                         :font-family (themes/get-font-family :monospace)
                         :font-size "11px"
                         :cursor "grab"
                         :transition "all 0.2s"
                         :user-select "none"}
                 :draggable true
                 :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) (str (themes/get-primary-color) "1A"))
                 :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
                 :on-drag-start (fn [e]
                                  (.setData (.-dataTransfer e) "text/plain" "")
                                  (dtoolbar/start-pill-drag! {:type :query
                                                               :sql (str "SELECT * FROM " table " LIMIT 10")
                                                               :table table} e))
                 :on-drag (fn [e] (dtoolbar/handle-pill-drag! e))
                 :on-drag-end (fn [e]
                                (dtoolbar/stop-pill-drag! e)
                                (reset! table-dropdown-open false))
                 :on-click (fn []
                             (reset! table-dropdown-open false)
                             (let [block-data {:id (str (random-uuid))
                                               :type :query
                                               :position {:x (+ 100 (rand-int 200)) :y (+ 100 (rand-int 200))}
                                               :size {:width 400 :height 300}
                                               :sql (str "SELECT * FROM " table " LIMIT 10")}]
                               (r/dispatch! [:add-block block-data])))}
           table]))
       ;; Reactor tables
       (when (seq (:reactor @table-list))
         [:div
          [:div {:style {:padding "5px 10px"
                         :color "#9b59b6"
                         :font-family (themes/get-font-family :monospace)
                         :font-size "10px"
                         :text-transform "uppercase"
                         :border-bottom "1px solid rgba(155,89,182,0.2)"
                         :margin-top "5px"
                         :opacity 0.7}}
           "Reactor Debug Tables"]
          (doall
           (for [table (:reactor @table-list)]
             ^{:key table}
             [:div {:style {:padding "8px 15px"
                            :color "#d8b4fe"
                            :font-family (themes/get-font-family :monospace)
                            :font-size "11px"
                            :cursor "grab"
                            :transition "all 0.2s"
                            :user-select "none"}
                    :draggable true
                    :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(155,89,182,0.1)")
                    :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
                    :on-drag-start (fn [e]
                                     (.setData (.-dataTransfer e) "text/plain" "")
                                     (dtoolbar/start-pill-drag! {:type :query
                                                                  :sql (str "SELECT * FROM " table " LIMIT 10")
                                                                  :table table} e))
                    :on-drag (fn [e] (dtoolbar/handle-pill-drag! e))
                    :on-drag-end (fn [e]
                                   (dtoolbar/stop-pill-drag! e)
                                   (reset! table-dropdown-open false))
                    :on-click (fn []
                                (reset! table-dropdown-open false)
                                (let [block-data {:id (str (random-uuid))
                                                  :type :query
                                                  :position {:x (+ 100 (rand-int 200)) :y (+ 100 (rand-int 200))}
                                                  :size {:width 400 :height 300}
                                                  :sql (str "SELECT * FROM " table " LIMIT 10")}]
                                  (r/dispatch! [:add-block block-data])))}
              table]))])
       ;; System tables
       (when (seq (:system @table-list))
         [:div
          [:div {:style {:padding "5px 10px"
                         :color (themes/get-secondary-color)
                         :font-family (themes/get-font-family :monospace)
                         :font-size "10px"
                         :text-transform "uppercase"
                         :border-bottom "1px solid rgba(255,0,110,0.2)"
                         :margin-top "5px"
                         :opacity 0.7}}
           "System Tables"]
          (doall
           (for [table (:system @table-list)]
             ^{:key table}
             [:div {:style {:padding "8px 15px"
                            :color "#ff4f99"
                            :font-family (themes/get-font-family :monospace)
                            :font-size "11px"
                            :cursor "grab"
                            :transition "all 0.2s"
                            :user-select "none"}
                    :draggable true
                    :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(255,0,110,0.1)")
                    :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
                    :on-drag-start (fn [e]
                                     (.setData (.-dataTransfer e) "text/plain" "")
                                     (dtoolbar/start-pill-drag! {:type :query
                                                                  :sql (str "SELECT * FROM " table " LIMIT 10")
                                                                  :table table} e))
                    :on-drag (fn [e] (dtoolbar/handle-pill-drag! e))
                    :on-drag-end (fn [e]
                                   (dtoolbar/stop-pill-drag! e)
                                   (reset! table-dropdown-open false))
                    :on-click (fn []
                                (reset! table-dropdown-open false)
                                (let [block-data {:id (str (random-uuid))
                                                  :type :query
                                                  :position {:x (+ 100 (rand-int 200)) :y (+ 100 (rand-int 200))}
                                                  :size {:width 400 :height 300}
                                                  :sql (str "SELECT * FROM " table " LIMIT 10")}]
                                  (r/dispatch! [:add-block block-data])))}
              table]))])])]
   [:button
    {:style {:padding "8px 16px"
             :background "transparent"
             :color (themes/get-secondary-color)
             :border (str "1px solid " (themes/get-secondary-color))
             :border-radius "2px"
             :cursor "pointer"
             :font-family (themes/get-font-family :monospace)
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
             :font-family (themes/get-font-family :monospace)
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
             :font-family (themes/get-font-family :monospace)
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
             :color (themes/get-tertiary-color)
             :border (str "1px solid " (themes/get-primary-color))
             :border-radius "2px"
             :cursor "pointer"
             :font-family (themes/get-font-family :monospace)
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
             :color "#9933ff"
             :border "1px solid #9933ff"
             :border-radius "2px"
             :cursor "pointer"
             :font-family (themes/get-font-family :monospace)
             :font-size "12px"
             :text-transform "uppercase"
             :letter-spacing "1px"
             :transition "all 0.3s"}
     :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(153,51,255,0.1)")
     :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
     :on-click (fn []
                 (let [block-data {:id (str (random-uuid))
                                   :type :rules
                                   :position {:x (+ 150 (rand-int 200))
                                              :y (+ 150 (rand-int 200))}
                                   :size {:width 500 :height 400}}]
                   (js/console.log "Adding rules block:" (clj->js block-data))
                   (r/dispatch! [:add-block block-data])))}
    "+ RULES"]
   [:button
    {:style {:padding "8px 16px"
             :background "transparent"
             :color "#ff4f99"
             :border "1px solid #ff4f99"
             :border-radius "2px"
             :cursor "pointer"
             :font-family (themes/get-font-family :monospace)
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
   [:button
    {:style {:padding "8px 16px"
             :background "transparent"
             :color "#8a2be2"
             :border "1px solid #8a2be2"
             :border-radius "2px"
             :cursor "pointer"
             :font-family (themes/get-font-family :monospace)
             :font-size "12px"
             :text-transform "uppercase"
             :letter-spacing "1px"
             :transition "all 0.3s"
             :margin-left "10px"}
     :on-click (fn []
                 (let [block-data {:id (keyword (str "iframe-" (random-uuid)))
                                   :type :iframe
                                   :position {:x (+ 150 (rand-int 200))
                                              :y (+ 150 (rand-int 200))}
                                   :size {:width 600 :height 450}
                                   :url "http://localhost:8080"
                                   :zoom 100}]
                   (js/console.log "Adding iframe block:" (clj->js block-data))
                   (r/dispatch! [:add-block block-data])))}
    "+ IFRAME"]
   
   ;; Theme selector dropdown
   [:div {:style {:position "relative"}}
    [:button
     {:style {:padding "8px 16px"
              :background "transparent"
              :color "#ffd700"
              :border "1px solid #ffd700"
              :border-radius "2px"
              :cursor "pointer"
              :font-family (themes/get-font-family :monospace)
              :font-size "12px"
              :text-transform "uppercase"
              :letter-spacing "1px"
              :transition "all 0.3s"
              :display "flex"
              :align-items "center"
              :gap "5px"
              :margin-left "10px"}
      :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(255,215,0,0.1)")
      :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) "transparent")
      :on-click (fn []
                 (swap! theme-dropdown-open not)
                 ;; Fetch available themes when opening
                 (when @theme-dropdown-open
                   (themes/fetch-available-themes!)))}
     "THEME"
     ;; Show current theme name
     (let [saved-theme (js/localStorage.getItem "rabbit-demo-theme")]
       (when saved-theme
         [:span {:style {:font-size "10px"
                         :opacity 0.7
                         :margin-left "5px"
                         :font-weight "normal"
                         :text-transform "none"}}
          (str ": " (-> saved-theme
                       (str/replace #"\.edn$" "")
                       (str/replace #"[-_]" " ")
                       (str/split #" ")
                       (->> (take 2)
                            (str/join " "))))]))
     [:span {:style {:font-size "10px"
                     :margin-left "5px"}} "▼"]]
    ;; Theme dropdown menu
    (when @theme-dropdown-open
      [:div {:style {:position "absolute"
                     :top "100%"
                     :left 0
                     :margin-top "5px"
                     :background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
                     :border "1px solid #ffd700"
                     :border-radius "4px"
                     :min-width "250px"
                     :max-height "400px"
                     :overflow-y "auto"
                     :z-index 1000
                     :box-shadow "0 4px 20px rgba(255,215,0,0.3)"}}
       ;; Default theme option
       (let [is-default? (nil? (js/localStorage.getItem "rabbit-demo-theme"))]
         [:div {:style {:padding "8px 15px"
                        :color (if is-default? "#00ff9f" "#ffd700")
                        :font-family (themes/get-font-family :monospace)
                        :font-size "11px"
                        :cursor "pointer"
                        :transition "all 0.2s"
                        :border-bottom "1px solid rgba(255,215,0,0.2)"
                        :background (when is-default? "rgba(0,255,159,0.05)")
                        :position "relative"}
                :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(255,215,0,0.1)")
                :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) (if is-default? "rgba(0,255,159,0.05)" "transparent"))
                :on-click (fn []
                           (reset! theme-dropdown-open false)
                           (themes/set-theme! nil)
                           (reset! themes/current-theme themes/default-theme))}
          [:span "Default Theme"]
          (when is-default?
            [:span {:style {:position "absolute"
                           :right "15px"
                           :color "#00ff9f"
                           :font-size "10px"}} "✓"])])
       ;; Available theme files
       (when @themes/available-themes
         (doall
          (for [theme-file @themes/available-themes]
            (let [is-active? (= theme-file (js/localStorage.getItem "rabbit-demo-theme"))]
              ^{:key theme-file}
              [:div {:style {:padding "8px 15px"
                             :color (if is-active? "#00ff9f" "#f0e68c")
                             :font-family (themes/get-font-family :monospace)
                             :font-size "11px"
                             :cursor "pointer"
                             :transition "all 0.2s"
                             :background (when is-active? "rgba(0,255,159,0.05)")
                             :position "relative"}
                     :on-mouse-over #(set! (.-style.background ^js (.-currentTarget ^js %)) "rgba(255,215,0,0.1)")
                     :on-mouse-out #(set! (.-style.background ^js (.-currentTarget ^js %)) (if is-active? "rgba(0,255,159,0.05)" "transparent"))
                     :on-click (fn []
                                 (reset! theme-dropdown-open false)
                                 (themes/set-theme! theme-file))}
               [:span (-> theme-file
                         (str/replace #"\.edn$" "")
                         (str/replace #"-" " ")
                         (str/replace #"_" " "))]
               (when is-active?
                 [:span {:style {:position "absolute"
                                :right "15px"
                                :color "#00ff9f"
                                :font-size "10px"}} "✓"])]))))])]
   
   [:div {:style {:flex 1}}]
   ;; Console tap toggle
   [:button
    {:style {:padding "6px 12px"
             :background (if @console-tap/hijacked? 
                          "rgba(0,255,212,0.1)" 
                          "transparent")
             :color (themes/get-tertiary-color)
             :border (str "1px solid " (if @console-tap/hijacked? (themes/get-primary-color) (str (themes/get-primary-color) "80")))
             :border-radius "2px"
             :cursor "pointer"
             :font-family (themes/get-font-family :monospace)
             :font-size "10px"
             :margin-right "10px"}
     :title (if @console-tap/hijacked? 
             "Console is being tapped - click to disable" 
             "Click to send console.log to tap>")
     :on-click #(console-tap/toggle-console-tap!)}
    (if @console-tap/hijacked? "CONSOLE→TAP ✓" "CONSOLE→TAP")]
   
   ;; Test buttons for console hijacking
   (when @console-tap/hijacked?
     [:div {:style {:display "flex" :gap "5px" :margin-right "10px"}}
      [:button
       {:style {:padding "4px 8px"
                :background "transparent"
                :color "#ff9f00"
                :border "1px solid #ff9f00"
                :border-radius "2px"
                :cursor "pointer"
                :font-family (themes/get-font-family :monospace)
                :font-size "9px"}
        :title "Test console.log"
        :on-click #(js/console.log "Test message from console.log" {:data "test" :timestamp (js/Date.now)})}
       "TEST LOG"]
      [:button
       {:style {:padding "4px 8px"
                :background "transparent"
                :color "#ff4444"
                :border "1px solid #ff4444"
                :border-radius "2px"
                :cursor "pointer"
                :font-family (themes/get-font-family :monospace)
                :font-size "9px"}
        :title "Test console.error"
        :on-click #(js/console.error "Test error from console.error" (js/Error. "Test error"))}
       "TEST ERROR"]
      [:button
       {:style {:padding "4px 8px"
                :background "transparent"
                :color "#ffff00"
                :border "1px solid #ffff00"
                :border-radius "2px"
                :cursor "pointer"
                :font-family (themes/get-font-family :monospace)
                :font-size "9px"}
        :title "Test console.warn"
        :on-click #(js/console.warn "Test warning from console.warn" "Warning details")}
       "TEST WARN"]])
   
   [:span {:style {:color (themes/get-primary-color)
                   :font-family (themes/get-font-family :monospace)
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
                 :color (themes/get-primary-color)
                 :border (str "1px solid " (themes/get-primary-color) "80")
                 :border-radius "2px"
                 :cursor "pointer"
                 :font-family (themes/get-font-family :monospace)
                 :font-size "11px"
                 :text-transform "uppercase"
                 :display "flex"
                 :align-items "center"
                 :gap "5px"}
         :on-click #(swap! session-dropdown-open not)}
        [:span {:style {:color (themes/get-primary-color)
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
                        :border (str "1px solid " (themes/get-primary-color))
                        :border-radius "4px"
                        :min-width "250px"
                        :max-height "300px"
                        :overflow-y "auto"
                        :z-index 1000
                        :box-shadow (str "0 -4px 20px " (themes/get-primary-color) "4C")}}
          ;; New session input
          [:div {:style {:padding "10px"
                         :border-bottom (str "1px solid " (themes/get-primary-color) "33")}}
           [:div {:style {:display "flex" :gap "5px"}}
            [:input {:type "text"
                     :placeholder "New session name"
                     :value @new-session-name
                     :on-change #(reset! new-session-name (-> % .-target .-value))
                     :style {:flex 1
                             :padding "4px 8px"
                             :background "rgba(0,0,0,0.3)"
                             :border (str "1px solid " (themes/get-primary-color) "4C")
                             :border-radius "2px"
                             :color (themes/get-primary-color)
                             :font-family (themes/get-font-family :monospace)
                             :font-size "11px"}
                     :on-key-down #(when (= (.-which %) 13)
                                    (when (seq @new-session-name)
                                      (r/create-session! @new-session-name)
                                      (r/switch-session! @new-session-name)
                                      (reset! current-session @new-session-name)
                                      (reset! new-session-name "")
                                      (load-sessions!)))}]
            [:button {:style {:padding "4px 10px"
                              :background (str "linear-gradient(90deg, " (themes/get-primary-color) " 0%, " (themes/get-secondary-color) " 100%)")
                              :color "#0a0a0a"
                              :border "none"
                              :border-radius "2px"
                              :cursor "pointer"
                              :font-family (themes/get-font-family :monospace)
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
           (doall 
            (for [session @sessions-list]
             ^{:key (:session-id session)}
             [:div {:style {:display "flex"
                            :align-items "center"
                            :padding "8px 10px"
                            :cursor "pointer"
                            :transition "all 0.2s"
                            :background (when (= (:session-id session) @current-session)
                                         (str (themes/get-primary-color) "1A"))}
                    :on-mouse-over #(when (not= (:session-id session) @current-session)
                                     (set! (.-style.background ^js (.-currentTarget %)) (str (themes/get-primary-color) "0D")))
                    :on-mouse-out #(when (not= (:session-id session) @current-session)
                                    (set! (.-style.background ^js (.-currentTarget %)) "transparent"))
                    :on-click (fn []
                               (r/switch-session! (:session-id session))
                               (reset! current-session (:session-id session))
                               (reset! session-dropdown-open false))}
              [:div {:style {:flex 1}}
               [:div {:style {:color (themes/get-primary-color)
                              :font-family (themes/get-font-family :monospace)
                              :font-size "11px"}} 
                (:session-id session)]
               [:div {:style {:color (themes/get-secondary-color)
                              :font-family (themes/get-font-family :monospace)
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
                                  :font-family (themes/get-font-family :monospace)
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
                 "DELETE"])]))]])])))

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
                :border-top (str "1px solid " (themes/get-primary-color) "33")
                :display "flex"
                :align-items "center"
                :padding "0 20px"
                :gap "20px"
                :box-shadow "0 -2px 20px rgba(0,0,0,0.05)"}}
       [:button {:style {:padding "6px 12px"
                         :background "transparent"
                         :color (themes/get-primary-color)
                         :border (str "1px solid " (themes/get-primary-color) "80")
                         :border-radius "2px"
                         :cursor "pointer"
                         :font-family (themes/get-font-family :monospace)
                         :font-size "11px"
                         :text-transform "uppercase"
                         :opacity (if (:can-undo @history-info) 1 0.5)}
                 :disabled (not (:can-undo @history-info))
                 :on-click #(r/undo!)} "← UNDO"]
       [:button {:style {:padding "6px 12px"
                         :background "transparent"
                         :color (themes/get-primary-color)
                         :border (str "1px solid " (themes/get-primary-color) "80")
                         :border-radius "2px"
                         :cursor "pointer"
                         :font-family (themes/get-font-family :monospace)
                         :font-size "11px"
                         :text-transform "uppercase"
                         :opacity (if (:can-redo @history-info) 1 0.5)}
                 :disabled (not (:can-redo @history-info))
                 :on-click #(r/redo!)} "REDO →"]
       [:div {:style {:flex 1 :display "flex" :align-items "center" :gap "15px"}}
        [:span {:style {:color (themes/get-primary-color) :font-family (themes/get-font-family :monospace) :font-size "11px" :text-transform "uppercase"}} 
         "TIMELINE:"]
        [:span {:style {:color (themes/get-secondary-color) :font-family (themes/get-font-family :monospace) :font-size "10px"}}
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
                        :background (str (themes/get-primary-color) "33")
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
   ;; Use the new draggable toolbar
   [dtoolbar/draggable-toolbar]
   ;; Add the drag preview overlay
   [dtoolbar/drag-preview-overlay]
   ;; Add virtual grid drag preview
   [vgrid/drag-preview]
   ;; Canvas with ID for drop detection
   [:div#canvas {:style {:flex 1 :position "relative"}}
    [canvas]]
   [cache-debug/cache-debug-panel]
   [timeline-controls]
   ;; Add cache debug panel
   
   ])

;; ============= Initialize =============

;; Store the React root for hot reload support
(defonce react-root (atom nil))

(defn ^:export init! []
  ;; Set app name for snapshot tracking
  (set! js/window.REACTOR_APP_NAME "rabbit")
  ;; Get session ID from query params FIRST
  (let [params (js/URLSearchParams. js/window.location.search)
        session-id (or (.get params "session") "default")]
    ;; Initialize reactor with proper session ID
    (r/init! {:server-url "http://localhost:5000"
              :session-id session-id})
    ;; Ensure SSE connection is established early for SQL subscriptions
    (r/ensure-sql-sse-connection!)
    ;; Initialize theming system
    (themes/init!)
    ;; Initialize drag-and-drop handlers for the toolbar
    (dtoolbar/init-drag-handlers!)
    ;; Hijack console by default to send to tap>
    (console-tap/hijack-console!)
    ;; Set current session and switch to it
    (reset! current-session session-id)
    (r/switch-session! session-id))
  ;; Auto-refresh queries for blocks loaded from persistence
  (auto-refresh/init-auto-refresh!)
  ;; Initialize cache debug panel if element exists
  (when-let [debug-container (.getElementById js/document "cache-debug-panel")]
    (cache-debug/init-debug-panel!))
  ;; Wait for state to load, then check if we need to initialize
  (js/setTimeout
    (fn []
      ;; Only dispatch init-rabbit for truly new/empty sessions
      (let [current-state @(r/subscribe [:get []])]
        (when-not (or (:canvas current-state) (:ui-settings current-state))
          (js/console.log "No canvas or UI settings found, initializing...")
          (r/dispatch! [:init-rabbit])))
      ;; Always get history info for time travel
      (r/get-history-info!))
    500)  ;; Give more time for state to load
  (load-sessions!)
  ;; Use React 18 createRoot API
  (let [root-element (.getElementById js/document "app")]
    (when root-element
      (if-let [existing-root @react-root]
        ;; Re-render on existing root (for hot reload)
        (rdom-client/render existing-root [rabbit-app])
        ;; Create new root on first load
        (let [root (rdom-client/create-root root-element)]
          (reset! react-root root)
          (rdom-client/render root [rabbit-app]))))))