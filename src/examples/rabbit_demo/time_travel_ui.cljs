(ns examples.rabbit-demo.time-travel-ui
  "Time travel UI components for query blocks"
  (:require [reagent.core :as reagent]
            [reactor.core :as r]
            [clojure.string :as cstr]
            [examples.rabbit-demo.reactive-queries :as rq]
            [examples.rabbit-demo.row-count-viz :as rcv]))

(defonce block-history (reagent/atom {}))  ;; block-id -> {:timestamps [...] :current-index N :hover-index N}
(defonce debounce-timers (atom {}))  ;; block-id -> timer-id for debouncing
(defonce chart-mode (reagent/atom {}))  ;; block-id -> :differential | :cumulative | :data-size
(defonce linked-blocks (reagent/atom {}))  ;; block-id -> boolean indicating if this block is linked
(defonce block-tables (reagent/atom {}))  ;; block-id -> table-name extracted from SQL

(defn normalize-block-id
  "Normalize block ID to string for consistent storage"
  [id]
  (cond
    (string? id) id
    (keyword? id) (name id)
    :else (str id)))

(defn extract-table-from-sql
  "Extract table name from SQL query using naive string parsing"
  [sql]
  (when sql
    (let [sql-upper (cstr/upper-case sql)
          ;; Match patterns like "FROM table_name" or "FROM schema.table_name"
          from-match (re-find #"FROM\s+([\w\.]+)" sql-upper)]
      (when from-match
        ;; Extract just the table name (without schema if present)
        (let [full-table (second from-match)
              table-parts (cstr/split full-table #"\.")]
          (cstr/lower-case (last table-parts)))))))

(defn get-linked-blocks-for-table
  "Get all linked blocks that query the same table"
  [table-name]
  (when table-name
    (let [all-linked @linked-blocks
          all-tables @block-tables]
      (reduce (fn [acc [block-id linked?]]
                (if (and linked? 
                         (= (get all-tables block-id) table-name))
                  (conj acc block-id)
                  acc))
              #{}
              all-linked))))

;; Forward declaration for mutual recursion
(declare debounced-time-change!)

(defn sync-linked-blocks!
  "Sync time travel position across all linked blocks for the same table"
  [source-block-id on-time-change timestamp index]
  (let [source-block-str (normalize-block-id source-block-id)
        table-name (get @block-tables source-block-str)
        linked-block-ids (get-linked-blocks-for-table table-name)]
    ;; Update all linked blocks except the source
    (doseq [block-id linked-block-ids
            :when (not= block-id source-block-str)]
      ;; Update the current index for UI
      (swap! block-history update block-id assoc :current-index index)
      ;; Get the stored on-time-change callback for this block
      (let [block-history-entry (get @block-history block-id)
            sql (:sql block-history-entry)
            stored-callback (:on-time-change block-history-entry)]
        ;; Use the stored callback if available, otherwise manually update
        (if stored-callback
          (do 
            (js/console.log "[LINKED-SYNC] Using stored callback for block" block-id "with timestamp" timestamp)
            ;; Use the block's own on-time-change callback with debouncing
            (debounced-time-change! block-id stored-callback timestamp 100))
          ;; Fallback: manually update timestamp and execute query
          (do
            (js/console.log "[LINKED-SYNC] No stored callback for block" block-id ", using fallback")
            (swap! rq/block-results assoc-in [:*timestamp block-id] timestamp)
            (when sql
              (rq/execute-block-query! block-id sql nil timestamp))))))))

(defn format-relative-time
  "Format timestamp as relative time (e.g., '2 hours ago', 'yesterday')"
  [timestamp]
  (when timestamp
    (let [now (js/Date.)
          date (js/Date. timestamp)
          diff-ms (- now date)
          diff-secs (/ diff-ms 1000)
          diff-mins (/ diff-secs 60)
          diff-hours (/ diff-mins 60)
          diff-days (/ diff-hours 24)]
      (cond
        (< diff-secs 60) "just now"
        (< diff-mins 2) "1 minute ago"
        (< diff-mins 60) (str (js/Math.floor diff-mins) " minutes ago")
        (< diff-hours 2) "1 hour ago"
        (< diff-hours 24) (str (js/Math.floor diff-hours) " hours ago")
        (< diff-days 2) "yesterday"
        (< diff-days 7) (str (js/Math.floor diff-days) " days ago")
        (< diff-days 30) (str (js/Math.floor (/ diff-days 7)) " weeks ago")
        :else (.toLocaleDateString date)))))

(defn get-time-markers
  "Get important time markers for visualization"
  [timestamps]
  (when (seq timestamps)
    (let [now (js/Date.)
          ;; Filter out nil (NOW marker)
          valid-timestamps (filter some? timestamps)
          sorted-ts (sort valid-timestamps)]
      (when (seq sorted-ts)
        (let [oldest (first sorted-ts)
              newest (last sorted-ts)
              ;; Calculate key time boundaries
              one-hour-ago (- now (* 60 60 1000))
              one-day-ago (- now (* 24 60 60 1000))
              one-week-ago (- now (* 7 24 60 60 1000))]
          {:oldest oldest
           :newest newest
           :markers (cond-> []
                     (some #(and % (>= (js/Date. %) one-hour-ago)) sorted-ts)
                     (conj {:label "1h" :position (/ one-hour-ago (- newest oldest))})
                     (some #(and % (>= (js/Date. %) one-day-ago)) sorted-ts)
                     (conj {:label "1d" :position (/ one-day-ago (- newest oldest))})
                     (some #(and % (>= (js/Date. %) one-week-ago)) sorted-ts)
                     (conj {:label "1w" :position (/ one-week-ago (- newest oldest))}))})))))

(defn debounced-time-change!
  "Execute time change with debouncing to avoid excessive queries"
  [block-id on-time-change timestamp delay-ms]
  (let [block-id-str (normalize-block-id block-id)]
    ;; Cancel any existing timer for this block
    (when-let [existing-timer (get @debounce-timers block-id-str)]
      (js/clearTimeout existing-timer))
    ;; Set new timer
    (let [timer-id (js/setTimeout 
                    (fn []
                      (swap! debounce-timers dissoc block-id-str)
                      (when on-time-change
                        (on-time-change timestamp)))
                    delay-ms)]
      (swap! debounce-timers assoc block-id-str timer-id))))

;; Forward declaration for mutual recursion
(declare fetch-query-history!)

(defn invalidate-latest-row-count!
  "Invalidate the cached row count for the latest timestamp (NOW) when table changes"
  [sql]
  (let [query-key (rcv/query-hash sql)
        cached-counts (get @rcv/row-count-cache query-key {})
        ;; Find the latest timestamp (usually NOW)
        latest-ts (when (seq cached-counts)
                   (apply max (keys cached-counts)))]
    (when latest-ts
      ;; Remove the latest timestamp from cache so it gets recalculated
      (swap! rcv/row-count-cache update query-key dissoc latest-ts)
      ;(js/console.log "[TIME-TRAVEL] Invalidated cached row count for latest timestamp:" latest-ts)
      )))

(defn handle-table-mutation!
  "Handle when a table mutation is detected for our query"
  [block-id sql]
  ;(js/console.log "[TIME-TRAVEL] Table mutation detected for block" block-id)
  ;; 1. Invalidate the latest row count cache
  (invalidate-latest-row-count! sql)
  ;; 2. Re-fetch the query history to get new time ticks
  (fetch-query-history! block-id sql))

(defn fetch-query-history!
  "Fetch available history timestamps for a query"
  [block-id sql]
  ;; Ensure block-id is a string (handle keywords properly)
  (let [block-id-str (normalize-block-id block-id)]
    ;(js/console.log "[TIME-TRAVEL] Fetching history for block" block-id-str "SQL:" sql)
    (-> (js/fetch (str (:server-url @r/config) "/api/query-history")
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"}
                       :body (js/JSON.stringify 
                              (clj->js {:sql sql
                                       :limit 50}))})  ;; Increased from 20 to get more timestamps
        (.then #(.json %))
        (.then (fn [data]
                 (let [history-data (js->clj data :keywordize-keys true)
                       ;; Preserve existing current-index if available
                       existing-history (get @block-history block-id-str)
                       existing-index (:current-index existing-history)]
                  ;;  (js/console.log "[TIME-TRAVEL] History received:" (clj->js history-data))
                  ;;  (js/console.log "[TIME-TRAVEL] Storing under key:" block-id-str)
                  ;;  (js/console.log "[TIME-TRAVEL] Existing index:" existing-index)
                   (let [timestamps (:timestamps history-data [])
                         table-name (extract-table-from-sql sql)]
                     ;; Store the table name for this block
                     (when table-name
                       (swap! block-tables assoc block-id-str table-name))
                     (swap! block-history assoc block-id-str 
                            {:timestamps timestamps
                             ;; Preserve existing index if valid, otherwise start at NOW
                             :current-index (if (and existing-index 
                                                    (>= existing-index 0)
                                                    (< existing-index (count timestamps)))
                                             existing-index
                                             (dec (count timestamps)))
                             :tables (:tables history-data [])
                             :sql sql}))
                   ;(js/console.log "[TIME-TRAVEL] block-history now:" (clj->js @block-history))
                   )))
        (.catch (fn [err]
                  (js/console.error "[TIME-TRAVEL] Failed to fetch history:" err)
                  )))))

(defn time-travel-slider
  "Time travel slider component for a query block"
  [{:keys [block-id sql on-time-change]}]
  (let [container-ref (atom nil)
        container-width (reagent/atom 500)]  ; Default width
    (reagent/create-class
     {:component-did-mount
      (fn []
        (when @container-ref
          ;; Measure and set initial width
          (reset! container-width (.-offsetWidth @container-ref))
          ;; Add resize observer
          (when (exists? js/ResizeObserver)
            (let [observer (js/ResizeObserver. 
                           (fn [entries]
                             (when-let [entry (aget entries 0)]
                               (let [width (.. entry -contentRect -width)]
                                 (reset! container-width width)))))]
              (.observe observer @container-ref)))))
      
      :reagent-render
      (fn [{:keys [block-id sql on-time-change]}]
        (let [block-id-str (normalize-block-id block-id)
              ;; Store the on-time-change callback for linked blocks to use
              _ (when on-time-change
                  (swap! block-history update block-id-str assoc :on-time-change on-time-change))
              history (get @block-history block-id-str)
              timestamps (:timestamps history [])
              current-index (:current-index history (dec (count timestamps)))
              hover-index (:hover-index history nil)]
          (when (seq timestamps)
            [:div {:ref #(reset! container-ref %)
                   :style {:margin "10px 0"
                          :padding "10px"
                          :background "rgba(0,0,0,0.3)"
                          :border-radius "4px"}}
       ;; Header with TIME TRAVEL label and link button
       [:div {:style {:display "flex"
                      :align-items "center"
                      :gap "10px"
                      :margin-bottom "8px"}}
        ;; Link toggle button
        (let [table-name (get @block-tables block-id-str)
              is-linked (get @linked-blocks block-id-str false)
              linked-count (count (get-linked-blocks-for-table table-name))]
          [:button {:style {:padding "2px 4px"
                           :background (if is-linked 
                                        "rgba(0,255,159,0.3)" 
                                        "rgba(0,255,159,0.1)")
                           :border (if is-linked
                                    "1px solid #00ff9f"
                                    "1px solid rgba(0,255,159,0.3)")
                           :border-radius "3px"
                           :color "#00ff9f"
                           :font-size "9px"
                           :font-family "'JetBrains Mono', monospace"
                           :cursor "pointer"
                           :margin-right "5px"
                           :display "flex"
                           :align-items "center"
                           :gap "3px"}
                    :title (if is-linked
                            (str "Linked with " (dec linked-count) " other block(s) querying " table-name)
                            (str "Click to link with blocks querying " table-name))
                    :on-click (fn []
                               (swap! linked-blocks update block-id-str not))}
           ;; Link icon - different style for linked vs unlinked
           [:span {:style {:font-size "11px"
                          :opacity (if is-linked 1.0 0.5)}} 
            "🔗"]
           (when (and is-linked (> linked-count 1))
             [:span {:style {:font-size "8px"}} 
              (dec linked-count)])])
        [:span {:style {:color "#00ff9f"
                        :font-size "11px"
                        :font-family "'JetBrains Mono', monospace"}}
         "TIME TRAVEL"]
        ;; Show current or hovered time with row count diff
        [:span {:style {:color (if hover-index "#ffd700" "#8ff0a4")
                        :font-size "10px"
                        :font-family "'JetBrains Mono', monospace"
                        :flex 1}}
         (let [display-index (or hover-index current-index)
               is-now? (= display-index (dec (count timestamps)))
               timestamp (when-not is-now? (nth timestamps display-index nil))
               mode (get @chart-mode block-id-str :differential)
               cumulative? (= mode :cumulative)
               data-size-mode? (= mode :data-size)
               ;; Get row counts for diff calculation
               row-count (when (and sql timestamp) 
                          (rcv/get-cached-count sql timestamp))
               prev-timestamp (when (and (> display-index 0) (not is-now?))
                              (nth timestamps (dec display-index) nil))
               prev-count (when (and sql prev-timestamp)
                           (rcv/get-cached-count sql prev-timestamp))
               ;; Calculate the difference or show absolute for first point
               row-diff (when (and row-count prev-count)
                         (- row-count prev-count))
               ;; Format the diff display based on mode
               diff-str (cond
                         ;; Cumulative mode - always show total count
                         (and cumulative? row-count)
                         (str " • " row-count " total rows")
                         ;; First data point in differential mode - show absolute count
                         (and (= display-index 0) row-count)
                         (str " • " row-count " rows")
                         ;; Show the difference in differential mode
                         row-diff
                         (cond
                           (pos? row-diff) (str " • +" row-diff " rows")
                           (neg? row-diff) (str " • " row-diff " rows")
                           :else " • no change")
                         ;; No data available
                         :else nil)]
           (if is-now?
             "NOW"
             (if timestamp
               (str (format-relative-time timestamp) 
                    " • " (.toLocaleString (js/Date. timestamp))
                    (or diff-str ""))
               "NO HISTORY")))]
        ;; Toggle for cumulative mode
        [:button {:style {:padding "2px 6px"
                          :background (case (get @chart-mode block-id-str :differential)
                                       :cumulative "rgba(255,165,0,0.3)"
                                       :data-size "rgba(138,43,226,0.3)"
                                       "rgba(0,255,159,0.1)")
                          :border "1px solid rgba(0,255,159,0.5)"
                          :border-radius "3px"
                          :color "#00ff9f"
                          :font-size "9px"
                          :font-family "'JetBrains Mono', monospace"
                          :cursor "pointer"}
                  :on-click (fn []
                             (swap! chart-mode update block-id-str
                                   (fn [mode]
                                     (case mode
                                       :cumulative :data-size
                                       :data-size :differential
                                       :differential :cumulative
                                       :cumulative))))}
         (case (get @chart-mode block-id-str :differential)
           :cumulative "CUMULATIVE"
           :data-size "DATA-SIZE"
           :differential "DIFFERENTIAL")]]
       
       ;; Compact timeline visualization
       [:div {:style {:position "relative"
                      :height "40px"
                      :margin-bottom "5px"
                      :background "rgba(0,0,0,0.2)"
                      :border-radius "3px"
                      :overflow "hidden"}}
        ;; Background gradient showing time density
        [:div {:style {:position "absolute"
                       :width "100%"
                       :height "100%"
                       :background "linear-gradient(to right, rgba(0,255,159,0.05), rgba(0,255,159,0.15))"
                       :pointer-events "none"}}]
        
        ;; Time markers (1h, 1d, 1w ago)
        (let [valid-ts (filter some? timestamps)]
          (when (seq valid-ts)
            (let [oldest (apply min (map #(js/Date. %) valid-ts))
                  newest (js/Date.)
                  range-ms (- newest oldest)]
              [:div {:style {:position "absolute"
                            :width "100%"
                            :height "100%"}}
               ;; Hour marker
               (when (> range-ms (* 60 60 1000))
                 (let [hour-ago (- newest (* 60 60 1000))
                       position (if (> hour-ago oldest)
                                 (* 100 (/ (- hour-ago oldest) range-ms))
                                 0)]
                   [:div {:style {:position "absolute"
                                 :left (str position "%")
                                 :height "100%"
                                 :border-left "1px dashed rgba(0,255,159,0.3)"}}
                    [:span {:style {:position "absolute"
                                   :top "2px"
                                   :left "2px"
                                   :font-size "8px"
                                   :color "rgba(0,255,159,0.6)"
                                   :font-family "'JetBrains Mono', monospace"}}
                     "1h"]]))
               ;; Day marker
               (when (> range-ms (* 24 60 60 1000))
                 (let [day-ago (- newest (* 24 60 60 1000))
                       position (if (> day-ago oldest)
                                 (* 100 (/ (- day-ago oldest) range-ms))
                                 0)]
                   [:div {:style {:position "absolute"
                                 :left (str position "%")
                                 :height "100%"
                                 :border-left "1px dashed rgba(0,255,159,0.4)"}}
                    [:span {:style {:position "absolute"
                                   :top "12px"
                                   :left "2px"
                                   :font-size "8px"
                                   :color "rgba(0,255,159,0.7)"
                                   :font-family "'JetBrains Mono', monospace"}}
                     "1d"]]))
               ;; Week marker
               (when (> range-ms (* 7 24 60 60 1000))
                 (let [week-ago (- newest (* 7 24 60 60 1000))
                       position (if (> week-ago oldest)
                                 (* 100 (/ (- week-ago oldest) range-ms))
                                 0)]
                   [:div {:style {:position "absolute"
                                 :left (str position "%")
                                 :height "100%"
                                 :border-left "1px dashed rgba(0,255,159,0.5)"}}
                    [:span {:style {:position "absolute"
                                   :top "22px"
                                   :left "2px"
                                   :font-size "8px"
                                   :color "rgba(0,255,159,0.8)"
                                   :font-family "'JetBrains Mono', monospace"}}
                     "1w"]]))])))
        
             ;; Row count area chart overlay
             (when sql
               [:div {:style {:position "absolute"
                             :width "100%"
                             :height "100%"
                             :pointer-events "none"}}
                [rcv/row-count-overlay 
                 {:sql sql
                  :timestamps (filter some? timestamps)  ; Filter out nil (NOW) markers
                  :width @container-width  ; Use measured container width
                  :height 40
                  :mode (get @chart-mode block-id-str :differential)
                   :cumulative? (= (get @chart-mode block-id-str :differential) :cumulative)}]])
        
        ;; Data points visualization
        [:div {:style {:position "absolute"
                       :width "100%"
                       :height "100%"
                       :display "flex"
                       :align-items "center"}}
         (map-indexed 
          (fn [idx ts]
            (when ts ;; Skip NOW (nil) marker
              [:div {:key idx
                     :style {:position "absolute"
                            :left (str (* 100 (/ idx (dec (count timestamps)))) "%")
                            :width "2px"
                            :height "20px"
                            :background (if (= idx current-index)
                                       "rgba(0,255,159,1)"
                                       "rgba(0,255,159,0.3)")
                            :transition "all 0.2s"}}]))
          timestamps)]]
       
       ;; Slider control
       [:div {:style {:position "relative"
                      :display "flex"
                      :align-items "center"}}
        ;; Visual track line
        [:div {:style {:position "absolute"
                       :width "100%"
                       :height "2px"
                       :background "rgba(0,255,159,0.2)"
                       :border-radius "1px"
                       :pointer-events "none"}}]
        ;; Progress line
        [:div {:style {:position "absolute"
                       :width (str (* 100 (/ current-index (max 1 (dec (count timestamps))))) "%")
                       :height "2px"
                       :background "rgba(0,255,159,0.6)"
                       :border-radius "1px"
                       :pointer-events "none"
                       :transition "width 0.2s"}}]
        [:input {:type "range"
                 :min 0
                 :max (dec (count timestamps))
                 :value current-index
                 :style {:flex 1
                         :width "100%"
                         :background "transparent"
                         :position "relative"
                         :z-index 1
                         :-webkit-appearance "none"
                         :appearance "none"
                         :height "20px"
                         :cursor "pointer"
                         :outline "none"}
                 :on-mouse-move (fn [e]
                                 ;; Calculate hover index from mouse position
                                 (let [rect (.getBoundingClientRect (.-target e))
                                       x (- (.-clientX e) (.-left rect))
                                       width (.-width rect)
                                       hover-idx (js/Math.round (* (dec (count timestamps)) (/ x width)))]
                                   (when (and (>= hover-idx 0) (< hover-idx (count timestamps)))
                                     (swap! block-history update block-id-str assoc :hover-index hover-idx))))
                 :on-mouse-leave (fn [_]
                                  (swap! block-history update block-id-str dissoc :hover-index))
                 :on-change (fn [e]
                             (let [new-index (js/parseInt (.. e -target -value))
                                   is-now? (= new-index (dec (count timestamps)))
                                   timestamp (when-not is-now? (nth timestamps new-index))
                                   is-linked (get @linked-blocks block-id-str false)]
                               ;; Update index immediately for UI responsiveness
                               (swap! block-history update block-id-str assoc :current-index new-index)
                               ;; Clear hover when selecting
                               (swap! block-history update block-id-str dissoc :hover-index)
                               ;; Debounce the actual query execution
                               (debounced-time-change! block-id on-time-change timestamp 300)
                               ;; If this block is linked, sync all other linked blocks
                               (when is-linked
                                 (sync-linked-blocks! block-id on-time-change timestamp new-index))))}]]])))})))

(defn time-travel-controls
  "Complete time travel controls for a query block"
  [{:keys [block-id sql]}]
  (let [refresh-interval (atom nil)]
    (reagent/create-class
     {:component-did-mount
      (fn []
        (when sql
          ;; Auto-fetch history when component mounts
          (fetch-query-history! block-id sql)
          ;; Set up periodic refresh to catch new time ticks
          ;; Every 5 seconds, check for new history (reduced from 30s for responsiveness)
          (reset! refresh-interval
                  (js/setInterval 
                   #(fetch-query-history! block-id sql)
                   5000))  ; 5 seconds
          
          ;; Register hook for reactive query updates
          (rq/register-query-hook! block-id handle-table-mutation!)))
    
    :component-did-update
    (fn [this [_ old-props]]
      (let [new-props (reagent/props this)
            old-sql (:sql old-props)
            new-sql (:sql new-props)]
        (when (and new-sql (not= old-sql new-sql))
          ;; Re-fetch history when SQL changes
          (fetch-query-history! (:block-id new-props) new-sql)
          ;; Reset refresh interval for new SQL
          (when @refresh-interval
            (js/clearInterval @refresh-interval))
          (reset! refresh-interval
                  (js/setInterval 
                   #(fetch-query-history! (:block-id new-props) new-sql)
                   5000)))))  ; 5 seconds
    
    :component-will-unmount
    (fn []
      ;; Clean up the refresh interval when component unmounts
      (when @refresh-interval
        (js/clearInterval @refresh-interval)
        (reset! refresh-interval nil))
      ;; Unregister query hook
      (rq/unregister-query-hook! block-id))
    
    :reagent-render
    (fn [{:keys [block-id sql]}]
      (let [block-id-str (normalize-block-id block-id)
            #_ (js/console.log "[TIME-TRAVEL-UI] Rendering for block-id:" block-id "-> str:" block-id-str)
            #_ (js/console.log "[TIME-TRAVEL-UI] block-history keys:" (clj->js (keys @block-history)))
            history (get @block-history block-id-str)
            #_ (js/console.log "[TIME-TRAVEL-UI] History for" block-id-str ":" (clj->js history))]
        [:div
         (when (:timestamps history)
           [time-travel-slider 
            {:block-id block-id
             :sql sql
             :on-time-change (fn [timestamp]
                               ;; Re-execute query with time travel
                               ;; nil timestamp means "NOW" - query stays reactive
                               ;; Use string block-id for consistent storage
                               (let [block-id-str (normalize-block-id block-id)]
                                 (swap! rq/block-results assoc-in [:*timestamp block-id-str] timestamp)
                                 ;(js/console.log "[TIME-TRAVEL] Setting timestamp for" block-id-str "to" timestamp)
                                 (rq/execute-block-query! block-id sql nil timestamp)))}])
         (when-not (:timestamps history)
           [:button {:style {:padding "4px 8px"
                            :background "rgba(0,255,159,0.1)"
                            :color "#00ff9f"
                            :border "1px solid #00ff9f"
                            :border-radius "3px"
                            :cursor "pointer"
                            :font-size "10px"
                            :font-family "'JetBrains Mono', monospace"}
                    :on-click #(fetch-query-history! block-id sql)}
            "LOAD HISTORY"])]))})))

(defn reset-time-travel!
  "Reset time travel to current time (NOW)"
  [block-id sql]
  (let [block-id-str (normalize-block-id block-id)
        is-linked (get @linked-blocks block-id-str false)
        table-name (when is-linked (get @block-tables block-id-str))
        linked-block-ids (when is-linked (get-linked-blocks-for-table table-name))
        history (get @block-history block-id-str)
        timestamps (:timestamps history [])]
    ;; Set to the last index which represents NOW
    (when history
      (swap! block-history update block-id-str assoc 
             :current-index (dec (count timestamps))))
    ;; Clear the timestamp in block-results (nil means NOW) - use string ID
    (swap! rq/block-results assoc-in [:*timestamp block-id-str] nil)
    ;(js/console.log "[TIME-TRAVEL] Resetting to NOW for" block-id-str)
    ;; Execute query without time travel (nil as-of means NOW)
    (rq/execute-block-query! block-id sql nil nil)
    
    ;; If linked, reset all linked blocks too
    (when is-linked
      (doseq [linked-id linked-block-ids
              :when (not= linked-id block-id-str)]
        (let [linked-history (get @block-history linked-id)
              linked-timestamps (:timestamps linked-history [])
              linked-sql (:sql linked-history)]
          ;; Set to the last index which represents NOW
          (when linked-history
            (swap! block-history update linked-id assoc 
                   :current-index (dec (count linked-timestamps))))
          ;; Clear the timestamp
          (swap! rq/block-results assoc-in [:*timestamp linked-id] nil)
          ;; Execute query without time travel
          (when linked-sql
            (rq/execute-block-query! linked-id linked-sql nil nil)))))))