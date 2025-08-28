(ns examples.rabbit-demo.virtual-grid
  "Virtual grid component for efficient rendering of large datasets with selection and drag capabilities"
  (:require [reagent.core :as r]
            [reagent.dom :as rdom]
            [examples.rabbit-demo.themes :as themes]
            ["react-window" :refer [VariableSizeGrid]]
            [clojure.string :as str]))

;; ============= Selection State =============

(defonce grid-selection 
  (r/atom {:selected-cells #{}      ; Set of [row-idx col-idx] tuples
           :selected-rows #{}       ; Set of row indices
           :selected-columns #{}    ; Set of column indices
           :selection-mode :cell    ; :cell, :row, :column
           :drag-start nil          ; Starting cell for drag selection
           :drag-end nil}))         ; Ending cell for drag selection

(defonce grid-hover
  (r/atom {:row nil
           :column nil
           :cell nil}))

;; ============= Drag State =============

(defonce drag-state
  (r/atom {:dragging? false
           :type nil           ; :cell, :column, :row
           :data nil           ; The actual data being dragged
           :preview nil        ; Preview element position
           :handlers nil}))    ; Event handlers for cleanup

;; ============= Selection Helpers =============

(defn clear-selection! []
  (swap! grid-selection assoc
         :selected-cells #{}
         :selected-rows #{}
         :selected-columns #{}
         :drag-start nil
         :drag-end nil))

(defn toggle-cell-selection! [row-idx col-idx]
  (swap! grid-selection update :selected-cells
         (fn [cells]
           (if (cells [row-idx col-idx])
             (disj cells [row-idx col-idx])
             (conj cells [row-idx col-idx])))))

(defn select-range! [start-row start-col end-row end-col]
  (let [min-row (min start-row end-row)
        max-row (max start-row end-row)
        min-col (min start-col end-col)
        max-col (max start-col end-col)]
    (swap! grid-selection assoc :selected-cells
           (into #{}
                 (for [r (range min-row (inc max-row))
                       c (range min-col (inc max-col))]
                   [r c])))))

(defn select-column! [col-idx row-count]
  (swap! grid-selection
         (fn [sel]
           (-> sel
               (update :selected-columns conj col-idx)
               (assoc :selected-cells
                      (into (:selected-cells sel)
                            (for [r (range row-count)]
                              [r col-idx])))))))

(defn select-row! [row-idx col-count]
  (swap! grid-selection
         (fn [sel]
           (-> sel
               (update :selected-rows conj row-idx)
               (assoc :selected-cells
                      (into (:selected-cells sel)
                            (for [c (range col-count)]
                              [row-idx c])))))))

;; ============= Column Type Detection =============

(defn detect-column-type
  "Detect if column is numeric based on sample values and column name"
  [column-name sample-values]
  (let [col-lower (str/lower-case (name column-name))
        ;; Check column name patterns
        numeric-pattern? (re-matches #".*(amount|price|cost|total|sum|count|quantity|qty|revenue|profit|loss|balance|score|rating|age|year|month|day|hour|minute|second|id|num|number|value|rate|percent|percentage|avg|average|min|max|median).*" col-lower)
        date-pattern? (re-matches #".*(date|time|timestamp|created|updated|modified|deleted|expired|started|ended|_at|_on).*" col-lower)
        ;; Check actual values if we have samples
        values-numeric? (when (seq sample-values)
                         (every? #(or (number? %)
                                     (and (string? %)
                                          (re-matches #"^-?\d+\.?\d*$" %)))
                                sample-values))]
    (cond
      ;; If values are actually numeric, it's numeric
      values-numeric? :numeric
      ;; Otherwise check name patterns
      numeric-pattern? :numeric
      date-pattern? :date
      ;; Default to dimension
      :else :dimension)))

;; ============= Drag Handlers =============

(defn prevent-default-dragover 
  "Global handler to prevent default and allow drop"
  [e]
  (.preventDefault e)
  (when (.-dataTransfer e)
    (set! (.-dropEffect (.-dataTransfer e)) "copy"))
  ;; Also update preview position during dragover
  (when (:dragging? @drag-state)
    (swap! drag-state assoc :preview
           {:x (.-clientX e)
            :y (.-clientY e)})))

(defn start-cell-drag! [row-data col-key cell-value sql event]
  (.preventDefault event)
  (let [filter-sql (str "SELECT * FROM (" sql ") WHERE " (name col-key) " = '" cell-value "'")]
    (reset! drag-state
            {:dragging? true
             :type :cell
             :data {:row row-data
                    :column col-key
                    :value cell-value}
             :pending-block {:type :query
                           :sql filter-sql}
             :preview {:x (.-clientX event)
                       :y (.-clientY event)}}))
  
  ;; Set drag data for compatibility
  (when-let [dt (.-dataTransfer event)]
    (.setData dt "text/plain" "")
    (.setData dt "reactor/grid-cell" "true")
    (set! (.-effectAllowed dt) "copy")
    ;; Hide the native drag image
    (let [img (js/document.createElement "img")]
      (set! (.-src img) "data:image/gif;base64,R0lGODlhAQABAIAAAAUEBAAAACwAAAAAAQABAAACAkQBADs=")
      (.setDragImage dt img 0 0))))

(defn start-column-drag! [col-key sql source-block-id results event]
  (js/console.log "start-column-drag! called for column:" (name col-key) "from block:" source-block-id
                  "type:" (type source-block-id) "keyword?" (keyword? source-block-id))
  ;; Don't prevent default on dragstart - let the browser handle it
  ;; Get sample values for type detection
  (let [sample-values (take 10 (map col-key results))
        col-type (detect-column-type col-key sample-values)
        drag-counter (atom 0)
        ;; Use mouse move for smooth preview tracking
        handle-mouse-move (fn handle-mouse-move [e]
                           (when (:dragging? @drag-state)
                             (let [new-x (.-clientX e)
                                   new-y (.-clientY e)]
                               ;; Log first movement to verify it's working
                               (when (= 1 (.-timeStamp e))
                                 (js/console.log "Mouse moving, updating preview to:" new-x new-y))
                               (swap! drag-state assoc :preview
                                      {:x new-x
                                       :y new-y}))))
        ;; Track drag events
        handle-drag (fn handle-drag [e]
                      (swap! drag-counter inc)
                      (when (= 1 @drag-counter)
                        (js/console.log "Drag event is firing")))
        ;; Clean up listeners on dragend
        handle-drag-end (fn handle-drag-end [e]
                         (js/console.log "Global dragend event fired")
                         (js/document.removeEventListener "mousemove" handle-mouse-move)
                         (js/document.removeEventListener "drag" handle-drag)
                         (js/document.removeEventListener "dragend" handle-drag-end)
                         ;; Only clear state if drop didn't happen (e.g., cancelled)
                         (js/setTimeout 
                           (fn []
                             (when (:dragging? @drag-state)
                               (js/console.log "No drop occurred, clearing drag state")
                               (reset! drag-state
                                       {:dragging? false
                                        :type nil
                                        :data nil
                                        :preview nil
                                        :handlers nil})))
                           100))]
    
    ;; Store handlers and data in state
    (reset! drag-state
            {:dragging? true
             :type :column
             :data {:column col-key
                    :column-type col-type
                    :source-sql sql
                    :source-block-id (if (keyword? source-block-id)
                                       (name source-block-id)
                                       (str source-block-id))}
             :pending-block {:type :query
                           :sql sql  ;; Will be transformed on drop
                           :column col-key
                           :column-type col-type
                           :source-block-id (if (keyword? source-block-id)
                                             (name source-block-id)
                                             (str source-block-id))}
             :preview {:x (.-clientX event)
                       :y (.-clientY event)}
             :handlers {:move handle-mouse-move
                       :end handle-drag-end}})
    
    ;; Add listeners - mousemove for preview, drag for tracking, dragend for cleanup
    (js/document.addEventListener "mousemove" handle-mouse-move)
    (js/document.addEventListener "drag" handle-drag)
    (js/document.addEventListener "dragend" handle-drag-end)
    
    ;; Add global dragover to ensure drag continues
    (js/document.addEventListener "dragover" prevent-default-dragover)
    (js/console.log "Event listeners added"))
  
  (when-let [dt (.-dataTransfer event)]
    ;; Set multiple data formats for compatibility
    (.setData dt "text/plain" "column-drag")
    (.setData dt "reactor/grid-column" "true")
    (set! (.-effectAllowed dt) "all")  ;; Allow all effects
    ;; Hide the native drag image by setting it to a transparent 1x1 image
    (let [img (js/document.createElement "img")]
      (set! (.-src img) "data:image/gif;base64,R0lGODlhAQABAIAAAAUEBAAAACwAAAAAAQABAAACAkQBADs=")
      (.setDragImage dt img 0 0))
    (js/console.log "Drag data set, effectAllowed: all")))

(defn handle-drag-end! [event]
  ;; Clean up ALL event listeners
  (when-let [handlers (:handlers @drag-state)]
    (when (:move handlers)
      (js/document.removeEventListener "mousemove" (:move handlers)))
    (when (:end handlers)
      (js/document.removeEventListener "dragend" (:end handlers))))
  
  ;; Also remove the global dragover listener
  (js/document.removeEventListener "dragover" prevent-default-dragover)
  
  (reset! drag-state
          {:dragging? false
           :type nil
           :data nil
           :preview nil
           :handlers nil}))

(defn get-pending-block
  "Returns the pending block from the current drag state if any"
  []
  (let [state @drag-state
        pending (:pending-block state)]
    (js/console.log "Getting pending block. Drag state keys:" (clj->js (keys state)))
    (js/console.log "Pending block:" (clj->js pending))
    pending))

(defn create-block-from-drag!
  "Creates a block from the current drag state at the given position"
  [x y on-complete]
  (js/console.log "create-block-from-drag! called, x:" x "y:" y)
  (when-let [pending-block (get-pending-block)]
    (js/console.log "Pending block found:" (clj->js pending-block))
    (let [state @drag-state]
      (js/console.log "Drag state type:" (:type state) "has-sql:" (boolean (get-in state [:data :source-sql])))
      (if (and (= (:type state) :column)
               (get-in state [:data :source-sql]))
        ;; For column drags, call the transform API
        (let [column (get-in state [:data :column])
              column-type (get-in state [:data :column-type])
              source-sql (get-in state [:data :source-sql])
              source-block-id (get-in state [:data :source-block-id])]
          (js/console.log "Calling transform API for column:" (name column) "type:" (name column-type) 
                         "from block:" source-block-id "is-string?" (string? source-block-id))
          (let [api-url (str js/window.location.protocol "//" js/window.location.hostname ":5000/api/sql-transform")
                request-body #js {:type "group-by"
                                  :source_sql source-sql
                                  :source_block_id source-block-id
                                  :column_name (name column)
                                  :column_type (name column-type)}]
            (js/console.log "API URL:" api-url)
            (js/console.log "Request body:" request-body)
            (-> (js/fetch api-url
                         #js {:method "POST"
                              :headers #js {"Content-Type" "application/json"}
                              :body (js/JSON.stringify request-body)})
              (.then (fn [resp] 
                       (js/console.log "API response status:" (.-status resp))
                       (.json resp)))
              (.then (fn [response]
                       (js/console.log "API response data:" response)
                       (let [transformed-sql (.-sql ^js response)
                             block-data {:id (str (random-uuid))
                                       :type :query
                                       :sql transformed-sql
                                       :position {:x x :y y}
                                       :size {:width 400 :height 300}
                                       :title (str (name column) " - " (name column-type))}]
                         (when on-complete
                           (on-complete block-data)))))
              (.catch (fn [error]
                       (js/console.error "Failed to transform SQL:" error)
                       ;; Fallback to simple GROUP BY
                       (let [fallback-sql (str "SELECT " (name column) ", COUNT(*) as count "
                                              "FROM (" source-sql ") AS source_data "
                                              "GROUP BY " (name column) " "
                                              "ORDER BY count DESC LIMIT 20")
                             block-data {:id (str (random-uuid))
                                       :type :query
                                       :sql fallback-sql
                                       :position {:x x :y y}
                                       :size {:width 400 :height 300}
                                       :title (str (name column) " - fallback")}]
                         (when on-complete
                           (on-complete block-data)))))))
          nil)  ;; Return nil since we're handling async
        ;; For cell drags or other types, create immediately
        (merge {:id (str (random-uuid))
                :position {:x x :y y}
                :size {:width 400 :height 300}}
               pending-block)))))

;; ============= Cell Component =============

(defn grid-cell [^js props]
  ;; react-window passes props as a JS object
  (let [column-index (.-columnIndex props)
        row-index (.-rowIndex props)
        style (.-style props)
        ^js data (.-data props)
        ;; Access JS properties directly
        results (.-results data)
        columns (.-columns data)
        on-cell-click (.-on-cell-click data)
        on-cell-drag (.-on-cell-drag data)
        block-id (.-block-id data)
        sql (.-sql data)
        col-count (.-col-count data)
        ;; Check if this is the row number column
        is-row-num-col? (= column-index 0)
        ;; Get row and column data from JS arrays
        row (when (and results (< row-index (.-length results)))
              (aget results row-index))
        col (when (and columns (< column-index (.-length columns)))
              (aget columns column-index))
        cell-value (cond
                    ;; Row number column - display 1-indexed row number
                    is-row-num-col? (inc row-index)
                    ;; Regular data column
                    (and row col) (aget row col)
                    :else nil)
        ;; Check selection state
        selection @grid-selection
        ;; Adjust column index for selection check (subtract 1 for row number column)
        selection-col-idx (if is-row-num-col? column-index (dec column-index))
        selected-cell? (contains? (:selected-cells selection) [row-index selection-col-idx])
        selected-row? (contains? (:selected-rows selection) row-index)
        selected-column? (and (not is-row-num-col?) 
                             (contains? (:selected-columns selection) selection-col-idx))
        selected? (or selected-cell? selected-row? selected-column?)
        hover-state @grid-hover
        hovering? (and (= (:row hover-state) row-index)
                      (= (:column hover-state) column-index))]
    
    ;; Return a React element using r/as-element
    (r/as-element
     [:div {:on-mouse-enter #(reset! grid-hover {:row row-index :column column-index})
            :on-mouse-leave #(reset! grid-hover {:row nil :column nil})
            :on-mouse-down (fn [e]
                            (.preventDefault e)
                            (let [;; Use adjusted column index for selection
                                  click-col-idx (if is-row-num-col? column-index (dec column-index))]
                              (cond
                                ;; Shift+click for range selection
                                (.-shiftKey e)
                                (when-let [start (:drag-start selection)]
                                  (select-range! (first start) (second start)
                                               row-index click-col-idx))
                                
                                ;; Ctrl/Cmd+click for multi-select
                                (or (.-ctrlKey e) (.-metaKey e))
                                (toggle-cell-selection! row-index click-col-idx)
                                
                                ;; Regular click
                                :else
                                (do
                                  (clear-selection!)
                                  (toggle-cell-selection! row-index click-col-idx)
                                  (swap! grid-selection assoc :drag-start [row-index click-col-idx])))
                              
                              (when (and on-cell-click (not is-row-num-col?))
                                (on-cell-click row col cell-value))
                              
                              ;; Special handling for row number column - select entire row
                              (when is-row-num-col?
                                (do
                                  (clear-selection!)
                                  (select-row! row-index (dec col-count))))))
            :draggable (boolean (and row col (not is-row-num-col?)))
            :on-drag-start (when (and row col (not is-row-num-col?))
                            #(start-cell-drag! (js->clj row :keywordize-keys true) col cell-value sql %))
            :on-drag-end handle-drag-end!
            :title (str cell-value)
            :style (js/Object.assign 
                    #js {} 
                    style  ;; First apply react-window's positioning styles
                    #js {:padding "4px 8px"
                         :borderRight (str "1px solid " (themes/get-primary-color) "1A")
                         :borderBottom (str "1px solid " (themes/get-primary-color) "1A")
                         :color (if selected?
                                 (themes/get-primary-color)
                                 "#ffffff")
                         :background (cond
                                      selected? (str (themes/get-primary-color) "1A")
                                      hovering? (str (themes/get-primary-color) "0A")
                                      is-row-num-col? "rgba(0,0,0,0.2)"
                                      :else "transparent")
                         :fontFamily (themes/get-font-family :monospace)
                         :fontSize "11px"
                         :fontWeight (if is-row-num-col? "bold" "normal")
                         :textAlign (if is-row-num-col? "center" "left")
                         :overflow "hidden"
                         :textOverflow "ellipsis"
                         :whiteSpace "nowrap"
                         :cursor (if is-row-num-col? "pointer" "cell")
                         :userSelect "none"
                         :transition "background 0.1s"
                         :display "flex"
                         :alignItems "center"
                         :justifyContent (if is-row-num-col? "center" "flex-start")})}
      ;; Display cell value in a span
      [:span {:style {:color "inherit"}}
       (let [display-value (if (nil? cell-value)
                             ""
                             (str cell-value))]
         display-value)]])))

;; ============= Header Component =============

(defn grid-header [{:keys [columns width on-column-drag sql block-id row-count results]}]
  (let [;; Add row number header
        columns-with-row-num (vec (cons "#" columns))
        row-num-width 60
        data-col-width (/ (- width row-num-width) (count columns))
        selection @grid-selection]
    [:div {:style {:display "flex"
                   :position "sticky"
                   :top 0
                   :background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
                   :border-bottom (str "2px solid " (themes/get-primary-color) "4C")
                   :z-index 10}}
     (doall
      (map-indexed
       (fn [idx col]
         (let [is-row-num? (= idx 0)
               col-width (if is-row-num? row-num-width data-col-width)
               actual-col-idx (when (not is-row-num?) (dec idx))]
           ^{:key (str "header-" idx)}
           [:div {:style {:width col-width
                          :padding "8px"
                          :color (if (and (not is-row-num?)
                                         (contains? (:selected-columns selection) actual-col-idx))
                                  (themes/get-primary-color)
                                  (str (themes/get-primary-color) "CC"))
                          :background (cond
                                       is-row-num? "rgba(0,0,0,0.2)"
                                       (contains? (:selected-columns selection) actual-col-idx)
                                       (str (themes/get-primary-color) "1A")
                                       :else "transparent")
                          :font-family (themes/get-font-family :monospace)
                          :font-weight "bold"
                          :font-size "10px"
                          :text-transform (if is-row-num? "none" "uppercase")
                          :letter-spacing "0.5px"
                          :text-align (if is-row-num? "center" "left")
                          :overflow "hidden"
                          :text-overflow "ellipsis"
                          :white-space "nowrap"
                          :cursor (if is-row-num? "default" "grab")
                          :border-right (str "1px solid " (themes/get-primary-color) "33")
                          :user-select "none"
                          :transition "all 0.2s"}
                  :draggable (not is-row-num?)
                  :on-drag-start (when (not is-row-num?)
                                  (fn [e]
                                    ;; Don't stop propagation - let it bubble
                                    (start-column-drag! (nth columns actual-col-idx) sql block-id results e)))
                  ;; Don't handle drag-end here - let the drop or global dragend handle it
                  :on-drag-end (when (not is-row-num?) 
                                (fn [e] 
                                  (.stopPropagation e)
                                  (js/console.log "Header drag-end - not clearing state yet")))
                  :on-mouse-enter (when (not is-row-num?)
                                   #(set! (.. % -currentTarget -style -background) 
                                         (str (themes/get-primary-color) "1A")))
                  :on-mouse-leave (when (not is-row-num?)
                                   #(set! (.. % -currentTarget -style -background) 
                                         (if (contains? (:selected-columns selection) actual-col-idx)
                                           (str (themes/get-primary-color) "1A")
                                           "transparent")))
                  :on-click (when (not is-row-num?)
                             (fn [e]
                               (.preventDefault e)
                               (clear-selection!)
                               (select-column! actual-col-idx row-count)))}
            (if (string? col) col (name col))]))
       columns-with-row-num))]))

;; ============= Main Virtual Grid Component =============

(defn virtual-grid [{:keys [results width height block-id sql on-cell-drag on-column-drag on-cell-click]}]
  (if (or (empty? results) (not (seq results)))
    [:div {:style {:display "flex"
                   :align-items "center"
                   :justify-content "center"
                   :height height
                   :color (str (themes/get-primary-color) "66")
                   :font-family (themes/get-font-family :monospace)
                   :font-size "12px"}}
     "No results"]
    
    (let [;; Ensure results is a vector for indexed access
          results-vec (if (vector? results) results (vec results))
          columns (keys (first results-vec))
          ;; Add row number as first column internally
          columns-with-row-num (vec (cons :__row-num__ columns))
          col-count (count columns-with-row-num)
          row-count (count results-vec)
          ;; Row num column is narrower (60px), others share remaining space
          row-num-width 60
          data-col-width (max 120 (/ (- width row-num-width 20) (dec col-count)))
          row-height 28
          header-height 36
          grid-height (- height header-height)
          ;; Include selection state to force re-renders
          selection @grid-selection
          ;; Create unique key based on selection to force re-render
          grid-key (str "grid-" (hash selection))
          
          ;; Create the data prop for cells - convert to JS for react-window
          cell-data (clj->js {:results results-vec
                              :columns columns-with-row-num
                              :on-cell-click on-cell-click
                              :on-cell-drag on-cell-drag
                              :block-id block-id
                              :sql sql
                              :col-count col-count
                              :selection selection})]
      
      [:div {:style {:height height
                     :display "flex"
                     :flex-direction "column"
                     :background "rgba(0,0,0,0.03)"
                     :border (str "1px solid " (themes/get-primary-color) "33")
                     :border-radius "4px"
                     :overflow "hidden"}}
       
       ;; Header
       [grid-header {:columns columns
                     :width width
                     :on-column-drag on-column-drag
                     :sql sql
                     :block-id block-id
                     :row-count row-count
                     :results results-vec}]
       
       ;; Virtual Grid Body
       [:> VariableSizeGrid
        {:key grid-key  ;; Force re-render when selection changes
         :height grid-height
         :width width
         :columnCount col-count
         :columnWidth (fn [index] 
                       (if (= index 0) row-num-width data-col-width))
         :rowCount row-count
         :rowHeight (fn [index] row-height)
         :itemData cell-data
         :style {:outline "none"}}
        grid-cell]])))

;; ============= Drag Preview Overlay =============

(defn drag-preview []
  (let [state @drag-state]
    (when (:dragging? state)
      (let [x (get-in state [:preview :x] 100)
            y (get-in state [:preview :y] 100)]
        ;; Log occasionally to debug
        (when (= 0 (mod (.now js/Date) 1000))
          (js/console.log "Drag preview rendering at:" x y))
        [:div {:style {:position "fixed"
                       :left (str (+ 10 x) "px")
                       :top (str (+ 10 y) "px")
                       :padding "8px 12px"
                       :background (str "linear-gradient(135deg, " 
                                      (themes/get-primary-color) " 0%, " 
                                      (themes/get-secondary-color) " 100%)")
                       :color "#ffffff"
                       :border-radius "4px"
                       :font-family (themes/get-font-family :monospace)
                       :font-size "12px"
                       :font-weight "bold"
                       :pointer-events "none"
                       :z-index 999999
                       :box-shadow "0 4px 20px rgba(0,0,0,0.8)"}}
         (case (:type state)
           :cell (str "Cell: " (get-in state [:data :value]))
           :column (let [col-type (get-in state [:data :column-type])]
                     (str "Column: " (name (get-in state [:data :column]))
                          " (" (name col-type) ")"))
           :row "Row Data"
           "Dragging...")]))))