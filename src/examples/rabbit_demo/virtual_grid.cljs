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

(defn start-column-drag! [col-key sql results event]
  (.preventDefault event)
  ;; Get sample values for type detection
  (let [sample-values (take 10 (map col-key results))
        col-type (detect-column-type col-key sample-values)
        ;; Create handlers that we can reference for removal
        handle-mouse-move (fn handle-mouse-move [e]
                           (when (:dragging? @drag-state)
                             (swap! drag-state assoc :preview
                                    {:x (.-clientX e)
                                     :y (.-clientY e)})))
        handle-mouse-up (fn handle-mouse-up [e]
                         (when-let [handlers (:handlers @drag-state)]
                           (js/document.removeEventListener "mousemove" (:move handlers))
                           (js/document.removeEventListener "mouseup" (:up handlers)))
                         (swap! drag-state assoc :dragging? false))]
    
    ;; Store handlers in state for cleanup
    (reset! drag-state
            {:dragging? true
             :type :column
             :data {:column col-key
                    :column-type col-type
                    :source-sql sql}
             :pending-block {:type :query
                           :sql sql  ;; Will be transformed on drop
                           :column col-key
                           :column-type col-type}
             :preview {:x (.-clientX event)
                       :y (.-clientY event)}
             :handlers {:move handle-mouse-move
                       :up handle-mouse-up}})
    
    ;; Add global mouse listeners
    (js/document.addEventListener "mousemove" handle-mouse-move)
    (js/document.addEventListener "mouseup" handle-mouse-up))
  
  (when-let [dt (.-dataTransfer event)]
    (.setData dt "text/plain" "")
    (.setData dt "reactor/grid-column" "true")
    (set! (.-effectAllowed dt) "copy")
    ;; Hide the native drag image by setting it to a transparent 1x1 image
    (let [img (js/document.createElement "img")]
      (set! (.-src img) "data:image/gif;base64,R0lGODlhAQABAIAAAAUEBAAAACwAAAAAAQABAAACAkQBADs=")
      (.setDragImage dt img 0 0))))

(defn handle-drag-end! [event]
  ;; Clean up event listeners if they exist
  (when-let [handlers (:handlers @drag-state)]
    (js/document.removeEventListener "mousemove" (:move handlers))
    (js/document.removeEventListener "mouseup" (:up handlers)))
  
  (reset! drag-state
          {:dragging? false
           :type nil
           :data nil
           :preview nil
           :handlers nil}))

(defn get-pending-block
  "Returns the pending block from the current drag state if any"
  []
  (:pending-block @drag-state))

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
              source-sql (get-in state [:data :source-sql])]
          (js/console.log "Calling transform API for column:" (name column) "type:" (name column-type))
          (-> (js/fetch "/api/sql-transform"
                       #js {:method "POST"
                            :headers #js {"Content-Type" "application/json"}
                            :body (js/JSON.stringify
                                    #js {:type "group-by"
                                         :source_sql source-sql
                                         :column_name (name column)
                                         :column_type (name column-type)})})
              (.then #(.json %))
              (.then (fn [response]
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
                           (on-complete block-data))))))
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

(defn grid-header [{:keys [columns width on-column-drag sql row-count results]}]
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
                                  #(start-column-drag! (nth columns actual-col-idx) sql results %))
                  :on-drag-end (when (not is-row-num?) handle-drag-end!)
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
  (when-let [state @drag-state]
    (when (:dragging? state)
      [:div {:style {:position "fixed"
                     :left (+ 10 (get-in state [:preview :x] 0))
                     :top (+ 10 (get-in state [:preview :y] 0))
                     :padding "8px 12px"
                     :background (str "linear-gradient(135deg, " 
                                    (themes/get-primary-color) " 0%, " 
                                    (themes/get-secondary-color) " 100%)")
                     :color "#0a0a0a"
                     :border-radius "4px"
                     :font-family (themes/get-font-family :monospace)
                     :font-size "11px"
                     :font-weight "bold"
                     :pointer-events "none"
                     :z-index 10000
                     :box-shadow "0 4px 20px rgba(0,0,0,0.5)"
                     :transition "left 0.05s, top 0.05s"}}
       (case (:type state)
         :cell (str "Cell: " (get-in state [:data :value]))
         :column (let [col-type (get-in state [:data :column-type])]
                   (str "Column: " (name (get-in state [:data :column]))
                        " (" (name col-type) ")"))
         :row "Row Data"
         "Dragging...")])))