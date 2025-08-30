(ns examples.rabbit-demo.draggable-toolbar
  "Draggable pill-based toolbar for creating blocks"
  (:require [reactor.core :as r]
            [reagent.core :as reagent]
            [examples.rabbit-demo.themes :as themes]
            [clojure.string :as str]))

;; ============= Drag State Management =============

(defonce drag-pill-state (reagent/atom nil))
(defonce drag-preview (reagent/atom nil))
(defonce table-panel-open (reagent/atom false))
(defonce table-list (reagent/atom {:user [] :reactor [] :system []}))

(defn get-highest-z-index
  "Get the highest z-index from all blocks"
  [blocks]
  (reduce (fn [max-z [_ block]]
            (max max-z (or (:z-index block) 10)))
          10
          blocks))

(defn start-pill-drag!
  "Start dragging a pill from the toolbar"
  [pill-type event]
  (let [rect (.. event -currentTarget getBoundingClientRect)
        ;; Normalize pill-type for preview (extract type if it's a map)
        preview-type (if (map? pill-type)
                       (:type pill-type)
                       pill-type)]
    (reset! drag-pill-state
            {:type pill-type
             :start-x (.-clientX event)
             :start-y (.-clientY event)
             :offset-x (- (.-clientX event) (.-left rect))
             :offset-y (- (.-clientY event) (.-top rect))})
    (reset! drag-preview
            {:x (.-clientX event)
             :y (.-clientY event)
             :type preview-type})
    (.preventDefault event)))

(defn handle-pill-drag!
  "Handle dragging motion"
  [event]
  (when @drag-pill-state
    (let [pill-info (:type @drag-pill-state)
          preview-type (if (map? pill-info)
                         (:type pill-info)
                         pill-info)]
      (reset! drag-preview
              {:x (.-clientX event)
               :y (.-clientY event)
               :type preview-type}))
    (.preventDefault event)))

(defn stop-pill-drag!
  "Stop dragging and create block at drop position"
  [event]
  (when @drag-pill-state
    (let [canvas-el (.getElementById js/document "canvas")]
      (when canvas-el
        (let [canvas-rect (.getBoundingClientRect canvas-el)
              mouse-x (.-clientX event)
              mouse-y (.-clientY event)
              ;; Check if mouse is within canvas bounds
              in-canvas? (and (>= mouse-x (.-left canvas-rect))
                            (<= mouse-x (.-right canvas-rect))
                            (>= mouse-y (.-top canvas-rect))
                            (<= mouse-y (.-bottom canvas-rect)))
              drop-x (- mouse-x (.-left canvas-rect))
              drop-y (- mouse-y (.-top canvas-rect))
              ;; The type stored in drag-pill-state can be either:
              ;; - A keyword (from toolbar pills)
              ;; - A map with :type, :sql, :table (from table dropdown)
              pill-info (:type @drag-pill-state)
              pill-type (if (map? pill-info)
                          (:type pill-info)  ;; Extract :type from the map
                          pill-info)         ;; Use the keyword directly
              custom-sql (when (map? pill-info)
                           (:sql pill-info))
              blocks @(r/subscribe [:blocks])
              new-z-index (+ 1 (get-highest-z-index blocks))
          
          ;; Create block data based on type
          block-data (case pill-type
                      :query {:id (str (random-uuid))
                             :type :query
                             :position {:x drop-x :y drop-y}
                             :size {:width 400 :height 300}
                             :sql (or custom-sql "SELECT * FROM sales LIMIT 10")
                             :z-index new-z-index}
                      
                      :chart {:id (str (random-uuid))
                             :type :chart
                             :position {:x drop-x :y drop-y}
                             :size {:width 400 :height 300}
                             :z-index new-z-index}
                      
                      :sql-exec {:id (str (random-uuid))
                                :type :sql-exec
                                :position {:x drop-x :y drop-y}
                                :size {:width 350 :height 200}
                                :z-index new-z-index}
                      
                      :debug {:id (str "debug-" (random-uuid))
                             :type :debug
                             :position {:x drop-x :y drop-y}
                             :size {:width 500 :height 400}
                             :debug-mode :subscriptions
                             :z-index new-z-index}
                      
                      :edn-browser {:id (str (random-uuid))
                                   :type :edn-browser
                                   :position {:x drop-x :y drop-y}
                                   :size {:width 450 :height 350}
                                   :z-index new-z-index}
                      
                      :rules {:id (str (random-uuid))
                             :type :rules
                             :position {:x drop-x :y drop-y}
                             :size {:width 500 :height 400}
                             :z-index new-z-index}
                      
                      :tap {:id (keyword (str "tap-" (random-uuid)))
                           :type :tap
                           :position {:x drop-x :y drop-y}
                           :size {:width 450 :height 400}
                           :z-index new-z-index}
                      
                      :iframe {:id (keyword (str "iframe-" (random-uuid)))
                              :type :iframe
                              :position {:x drop-x :y drop-y}
                              :size {:width 600 :height 450}
                              :url "http://localhost:8080"
                              :zoom 100
                              :z-index new-z-index}
                      
                      nil)]  ;; Default case returns nil
          
          ;; Only create block if dropped on canvas
          (when (and block-data in-canvas?)
            (js/console.log "Creating block from pill drop at:" drop-x drop-y (clj->js block-data))
            (r/dispatch! [:add-block block-data]))
          
          ;; Log if not in canvas for debugging
          (when (and block-data (not in-canvas?))
            (js/console.log "Pill dropped outside canvas bounds")))))
    
    ;; Always clean up drag state when mouse is released
    (reset! drag-pill-state nil)
    (reset! drag-preview nil)
    (.preventDefault event)))

;; ============= Pill Component =============

(defn draggable-pill
  "A draggable pill button"
  [{:keys [type label color hover-color icon]}]
  (let [hovering? (reagent/atom false)]
    (fn []
      [:div.pill
       {:style {:display "inline-flex"
                :align-items "center"
                :gap "5px"
                :padding "8px 16px"
                :background (if @hovering?
                             (or hover-color (str color "1A"))
                             "transparent")
                :color color
                :border (str "1px solid " color)
                :border-radius "10px"
                :cursor "grab"
                :font-family (themes/get-font-family :monospace)
                :font-size "12px"
                :text-transform "uppercase"
                :letter-spacing "1px"
                :transition "all 0.3s"
                :user-select "none"}
        :on-mouse-enter #(reset! hovering? true)
        :on-mouse-leave #(reset! hovering? false)
        :on-mouse-down #(start-pill-drag! type %)
        :draggable false}
       label])))

;; ============= Table Selector Component =============

(defonce table-metadata-cache (reagent/atom {}))

(defn fetch-table-metadata!
  "Fetch metadata for a specific table"
  [table-name]
  (when-not (get @table-metadata-cache table-name)
    (-> (js/fetch "http://localhost:5000/api/table-info"
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"}
                       :body (js/JSON.stringify #js {:table table-name})})
        (.then #(.json %))
        (.then (fn [data]
                 (let [metadata (js->clj data :keywordize-keys true)]
                   (swap! table-metadata-cache assoc table-name metadata))))
        (.catch (fn [error]
                  (js/console.error "Failed to fetch table metadata:" error))))))

(defn fetch-tables!
  "Fetch tables from the server and update the state"
  []
  (-> (js/fetch "http://localhost:5000/api/tables")
      (.then #(.json %))
      (.then (fn [data]
               (let [tables-data (js->clj data :keywordize-keys true)
                     public-tables (get tables-data :public [])
                     system-tables (get tables-data :system [])
                     ;; Separate reactor tables from public tables
                     reactor-prefixes ["reactor_" "rx_" "rxt_"]
                     is-reactor-table? (fn [table]
                                        (some #(str/starts-with? table %) reactor-prefixes))
                     user-tables (vec (remove is-reactor-table? public-tables))
                     reactor-tables (vec (filter is-reactor-table? public-tables))]
                 ;; Update the local atom with categorized tables
                 (reset! table-list {:user user-tables
                                    :reactor reactor-tables
                                    :system system-tables}))))
      (.catch (fn [error]
                (js/console.error "Failed to fetch tables:" error)))))

(defn table-pill
  "A draggable table pill with metadata tooltip"
  [{:keys [table color hover-color]}]
  (let [hovering? (reagent/atom false)
        metadata (reagent/atom nil)]
    (fn []
      [:div {:style {:position "relative"}}
       [:div {:style {:padding "8px 15px"
                      :color color
                      :font-family (themes/get-font-family :monospace)
                      :font-size "11px"
                      :cursor "grab"
                      :transition "all 0.2s"
                      :border-radius "4px"
                      :background (when @hovering?
                                   hover-color)}
               :on-mouse-enter (fn []
                               (reset! hovering? true)
                               (fetch-table-metadata! table)
                               (reset! metadata (get @table-metadata-cache table)))
               :on-mouse-leave #(reset! hovering? false)
               :on-mouse-down #(do
                              (start-pill-drag! {:type :query
                                               :sql (str "SELECT * FROM " table " LIMIT 10")
                                               :table table} %)
                              (reset! table-panel-open false))
               :draggable false}
        table
        (when-let [row-count (:row-count @metadata)]
          [:span {:style {:opacity 0.5
                         :margin-left "8px"
                         :font-size "10px"}}
           (str "(" row-count " rows)")])]
       ;; Tooltip with metadata
       (when (and @hovering? @metadata)
         [:div {:style {:position "absolute"
                       :left "100%"
                       :top 0
                       :margin-left "10px"
                       :background "rgba(0,0,0,0.9)"
                       :border (str "1px solid " color "4C")
                       :border-radius "4px"
                       :padding "8px"
                       :min-width "200px"
                       :z-index 1002
                       :white-space "nowrap"}}
          [:div {:style {:font-family (themes/get-font-family :monospace)
                        :font-size "10px"
                        :color color
                        :margin-bottom "5px"}}
           (str (:schema @metadata) "." table)]
          (when (:row-count @metadata)
            [:div {:style {:font-size "9px"
                          :color (str color "CC")
                          :margin-bottom "3px"}}
             (str (:row-count @metadata) " rows")])
          (when (seq (:columns @metadata))
            [:div {:style {:font-size "9px"
                          :color (str color "99")}}
             (str (count (:columns @metadata)) " columns")])])])))

(defn table-selector-panel
  "Expandable panel for table selection"
  []
  (let [panel-hovering? (reagent/atom false)
        table-hovering? (reagent/atom nil)]
    (fn []
      [:div {:style {:position "relative"
                     :display "inline-block"}}
       ;; Main pill button
       [:div.pill
        {:style {:display "inline-flex"
                 :align-items "center"
                 :gap "5px"
                 :padding "8px 16px"
                 :background (if (or @table-panel-open @panel-hovering?)
                              (str (themes/get-primary-color) "1A")
                              "transparent")
                 :color (themes/get-primary-color)
                 :border (str "1px solid " (themes/get-primary-color))
                 :border-radius "10px"
                 :cursor "pointer"
                 :font-family (themes/get-font-family :monospace)
                 :font-size "12px"
                 :text-transform "uppercase"
                 :letter-spacing "1px"
                 :transition "all 0.3s"
                 :user-select "none"}
         :on-mouse-enter #(reset! panel-hovering? true)
         :on-mouse-leave #(reset! panel-hovering? false)
         :on-click (fn []
                    (let [will-open (not @table-panel-open)]
                      (swap! table-panel-open not)
                      ;; Fetch tables when opening
                      (when will-open
                        (fetch-tables!))))}
        "TABLES"
        [:span {:style {:font-size "10px"
                        :transform (if @table-panel-open "rotate(180deg)" "rotate(0deg)")
                        :transition "transform 0.3s"}}
         "▼"]]
       
       ;; Expandable table panel
       (when @table-panel-open
         [:div {:style {:position "absolute"
                        :top "45px"
                        :left 0
                        :background (or (themes/get-theme-property :toolbar-background)
                                      "linear-gradient(180deg, #1a1a2e 0%, #0a0a0a 100%)")
                        :border (str "1px solid " (themes/get-primary-color) "4C")
                        :border-radius "8px"
                        :padding "10px"
                        :min-width "200px"
                        :max-height "400px"
                        :overflow-y "auto"
                        :z-index 1001
                        :box-shadow (str "0 4px 20px " (themes/get-primary-color) "4C")}}
          
          ;; User tables section
          (when (seq (:user @table-list))
            [:div
             [:div {:style {:padding "5px 10px"
                           :color (themes/get-primary-color)
                           :font-family (themes/get-font-family :monospace)
                           :font-size "10px"
                           :text-transform "uppercase"
                           :border-bottom (str "1px solid " (themes/get-primary-color) "33")
                           :opacity 0.7}}
              "Data Tables"]
             (doall
              (for [table (:user @table-list)]
                ^{:key table}
                [table-pill {:table table
                            :color (themes/get-tertiary-color)
                            :hover-color (str (themes/get-primary-color) "1A")}]))])
          
          ;; Reactor tables section
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
                [table-pill {:table table
                            :color "#d8b4fe"
                            :hover-color "rgba(155,89,182,0.1)"}]))])
          
          ;; System tables section
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
                [table-pill {:table table
                            :color "#ff4f99"
                            :hover-color "rgba(255,0,110,0.1)"}]))])])])))

;; ============= Drag Preview Component =============

(defn drag-preview-overlay
  "Shows a preview of the pill being dragged"
  []
  (when @drag-preview
    (let [{:keys [x y type]} @drag-preview
          label (case type
                  :query "QUERY"
                  :chart "CHART"
                  :sql-exec "EXECUTE"
                  :debug "DEBUG"
                  :edn-browser "EDN"
                  :rules "RULES"
                  :tap "TAP"
                  :iframe "IFRAME"
                  "BLOCK")]
      [:div {:style {:position "fixed"
                     :left (str x "px")
                     :top (str y "px")
                     :transform "translate(-50%, -50%)"
                     :padding "8px 16px"
                     :background "rgba(0,255,212,0.2)"
                     :color (themes/get-primary-color)
                     :border (str "2px solid " (themes/get-primary-color))
                     :border-radius "10px"
                     :font-family (themes/get-font-family :monospace)
                     :font-size "12px"
                     :text-transform "uppercase"
                     :letter-spacing "1px"
                     :pointer-events "none"
                     :z-index 9999
                     :box-shadow "0 4px 20px rgba(0,255,212,0.5)"}}
       label])))

;; ============= Theme Selector Component =============

(defonce theme-dropdown-open (reagent/atom false))

(defn theme-selector
  "Theme selector dropdown"
  []
  [:div {:style {:position "relative"}}
   [:button
    {:style {:padding "8px 16px"
             :background "transparent"
             :color "#ffd700"
             :border "1px solid #ffd700"
             :border-radius "10px"
             :cursor "pointer"
             :font-family (themes/get-font-family :monospace)
             :font-size "12px"
             :text-transform "uppercase"
             :letter-spacing "1px"
             :transition "all 0.3s"
             :display "flex"
             :align-items "center"
             :gap "5px"}
     :on-mouse-enter #(set! (.. % -currentTarget -style -background) "rgba(255,215,0,0.1)")
     :on-mouse-leave #(set! (.. % -currentTarget -style -background) "transparent")
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
                    :transform (if @theme-dropdown-open "rotate(180deg)" "rotate(0deg)")
                    :transition "transform 0.3s"}} "▼"]]
   ;; Theme dropdown menu
   (when @theme-dropdown-open
     [:div {:style {:position "absolute"
                    :top "45px"
                    :right 0
                    :background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
                    :border "1px solid #ffd700"
                    :border-radius "8px"
                    :min-width "250px"
                    :max-height "400px"
                    :overflow-y "auto"
                    :z-index 1001
                    :box-shadow "0 4px 10px rgba(255,215,0,0.3)"}}
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
               :on-mouse-enter #(set! (.. % -currentTarget -style -background) "rgba(255,215,0,0.1)")
               :on-mouse-leave #(set! (.. % -currentTarget -style -background) 
                                     (if is-default? "rgba(0,255,159,0.05)" "transparent"))
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
                    :on-mouse-enter #(set! (.. % -currentTarget -style -background) "rgba(255,215,0,0.1)")
                    :on-mouse-leave #(set! (.. % -currentTarget -style -background) 
                                          (if is-active? "rgba(0,255,159,0.05)" "transparent"))
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
                               :font-size "10px"}} "✓"])]))))])])

;; ============= Main Toolbar Component =============

(defn draggable-toolbar
  "New pill-based draggable toolbar"
  []
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
   
   ;; Query pill
   [draggable-pill {:type :query
                    :label "QUERY"
                    :color (themes/get-primary-color)}]
   
   ;; Table selector (special expandable pill)
   [table-selector-panel]
   
   ;; Chart pill
   [draggable-pill {:type :chart
                    :label "CHART"
                    :color (themes/get-secondary-color)}]
   
   ;; Execute pill
   [draggable-pill {:type :sql-exec
                    :label "EXECUTE"
                    :color "#ffb700"}]
   
   ;; Debug pill
   [draggable-pill {:type :debug
                    :label "DEBUG"
                    :color "#9b59b6"}]
   
   ;; EDN Browser pill
   [draggable-pill {:type :edn-browser
                    :label "EDN"
                    :color (themes/get-tertiary-color)}]
   
   ;; Rules pill
   [draggable-pill {:type :rules
                    :label "RULES"
                    :color "#9933ff"}]
   
   ;; TAP pill
   [draggable-pill {:type :tap
                    :label "TAP"
                    :color "#ff4f99"}]
   
   ;; iFrame pill
   [draggable-pill {:type :iframe
                    :label "IFRAME"
                    :color "#8a2be2"}]
   
   ;; Spacer to push theme selector to the right
   [:div {:style {:flex 1}}]
   
   ;; Theme selector on far right
   [theme-selector]])

;; ============= Global Event Handlers =============

(defn init-drag-handlers!
  "Initialize global drag event handlers"
  []
  ;; Remove any existing handlers first to avoid duplicates
  (.removeEventListener js/document "mousemove" handle-pill-drag!)
  (.removeEventListener js/document "mouseup" stop-pill-drag!)
  
  ;; Handle global mouse move
  (.addEventListener js/document "mousemove" handle-pill-drag!)
  
  ;; Handle global mouse up - capture phase to ensure we get it
  (.addEventListener js/document "mouseup" stop-pill-drag! true)
  
  ;; Prevent default drag behavior
  (.addEventListener js/document "dragstart" 
                     (fn [e] 
                       (when @drag-pill-state
                         (.preventDefault e))))
  
  (js/console.log "Drag handlers initialized for toolbar pills"))