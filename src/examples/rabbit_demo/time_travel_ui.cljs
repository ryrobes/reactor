(ns examples.rabbit-demo.time-travel-ui
  "Time travel UI components for query blocks"
  (:require [reagent.core :as reagent]
            [reactor.core :as r]
            [examples.rabbit-demo.reactive-queries :as rq]))

(defonce block-history (reagent/atom {}))  ;; block-id -> {:timestamps [...] :current-index N :hover-index N}
(defonce debounce-timers (atom {}))  ;; block-id -> timer-id for debouncing

(defn normalize-block-id
  "Normalize block ID to string for consistent storage"
  [id]
  (cond
    (string? id) id
    (keyword? id) (name id)
    :else (str id)))

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

(defn fetch-query-history!
  "Fetch available history timestamps for a query"
  [block-id sql]
  ;; Ensure block-id is a string (handle keywords properly)
  (let [block-id-str (normalize-block-id block-id)]
    (js/console.log "[TIME-TRAVEL] Fetching history for block" block-id-str "SQL:" sql)
    (-> (js/fetch (str (:server-url @r/config) "/api/query-history")
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"}
                       :body (js/JSON.stringify 
                              (clj->js {:sql sql
                                       :limit 20}))})
        (.then #(.json %))
        (.then (fn [data]
                 (let [history-data (js->clj data :keywordize-keys true)]
                   (js/console.log "[TIME-TRAVEL] History received:" (clj->js history-data))
                   (js/console.log "[TIME-TRAVEL] Storing under key:" block-id-str)
                   (let [timestamps (:timestamps history-data [])]
                     (swap! block-history assoc block-id-str 
                            {:timestamps timestamps
                             ;; Start at NOW (last index) not the oldest (0)
                             :current-index (dec (count timestamps))
                             :tables (:tables history-data [])}))
                   (js/console.log "[TIME-TRAVEL] block-history now:" (clj->js @block-history)))))
        (.catch (fn [err]
                  (js/console.error "[TIME-TRAVEL] Failed to fetch history:" err))))))

(defn time-travel-slider
  "Time travel slider component for a query block"
  [{:keys [block-id sql on-time-change]}]
  (let [block-id-str (normalize-block-id block-id)
        history (get @block-history block-id-str)
        timestamps (:timestamps history [])
        current-index (:current-index history (dec (count timestamps)))
        hover-index (:hover-index history nil)]
    (when (seq timestamps)
      [:div {:style {:margin "10px 0"
                     :padding "10px"
                     :background "rgba(0,0,0,0.3)"
                     :border-radius "4px"}}
       ;; Header with TIME TRAVEL label
       [:div {:style {:display "flex"
                      :align-items "center"
                      :gap "10px"
                      :margin-bottom "8px"}}
        [:span {:style {:color "#00ff9f"
                        :font-size "11px"
                        :font-family "monospace"}}
         "TIME TRAVEL"]
        ;; Show current or hovered time
        [:span {:style {:color (if hover-index "#ffd700" "#8ff0a4")
                        :font-size "10px"
                        :font-family "monospace"
                        :flex 1}}
         (let [display-index (or hover-index current-index)
               is-now? (= display-index (dec (count timestamps)))
               timestamp (when-not is-now? (nth timestamps display-index nil))]
           (if is-now?
             "NOW"
             (if timestamp
               (str (format-relative-time timestamp) " • " (.toLocaleString (js/Date. timestamp)))
               "NO HISTORY")))]]
       
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
                                   :font-family "monospace"}}
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
                                   :font-family "monospace"}}
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
                                   :font-family "monospace"}}
                     "1w"]]))])))
        
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
                                   timestamp (when-not is-now? (nth timestamps new-index))]
                               ;; Update index immediately for UI responsiveness
                               (swap! block-history update block-id-str assoc :current-index new-index)
                               ;; Clear hover when selecting
                               (swap! block-history update block-id-str dissoc :hover-index)
                               ;; Debounce the actual query execution
                               (debounced-time-change! block-id on-time-change timestamp 300)))}]]])))

(defn time-travel-controls
  "Complete time travel controls for a query block"
  [{:keys [block-id sql]}]
  (reagent/create-class
   {:component-did-mount
    (fn []
      (when sql
        ;; Auto-fetch history when component mounts
        (fetch-query-history! block-id sql)))
    
    :component-did-update
    (fn [this [_ old-props]]
      (let [new-props (reagent/props this)
            old-sql (:sql old-props)
            new-sql (:sql new-props)]
        (when (and new-sql (not= old-sql new-sql))
          ;; Re-fetch history when SQL changes
          (fetch-query-history! (:block-id new-props) new-sql))))
    
    :reagent-render
    (fn [{:keys [block-id sql]}]
      (let [block-id-str (normalize-block-id block-id)
            _ (js/console.log "[TIME-TRAVEL-UI] Rendering for block-id:" block-id "-> str:" block-id-str)
            _ (js/console.log "[TIME-TRAVEL-UI] block-history keys:" (clj->js (keys @block-history)))
            history (get @block-history block-id-str)
            _ (js/console.log "[TIME-TRAVEL-UI] History for" block-id-str ":" (clj->js history))]
        [:div
         (when (:timestamps history)
           [time-travel-slider 
            {:block-id block-id
             :sql sql
             :on-time-change (fn [timestamp]
                              ;; Re-execute query with time travel
                              ;; nil timestamp means "NOW" - query stays reactive
                              (rq/execute-block-query! block-id sql nil timestamp))}])
         (when-not (:timestamps history)
           [:button {:style {:padding "4px 8px"
                            :background "rgba(0,255,159,0.1)"
                            :color "#00ff9f"
                            :border "1px solid #00ff9f"
                            :border-radius "3px"
                            :cursor "pointer"
                            :font-size "10px"
                            :font-family "monospace"}
                    :on-click #(fetch-query-history! block-id sql)}
            "LOAD HISTORY"])]))}))

(defn reset-time-travel!
  "Reset time travel to current time (NOW)"
  [block-id sql]
  (let [block-id-str (normalize-block-id block-id)
        history (get @block-history block-id-str)
        timestamps (:timestamps history [])]
    ;; Set to the last index which represents NOW
    (when history
      (swap! block-history update block-id-str assoc 
             :current-index (dec (count timestamps))))
    ;; Execute query without time travel (nil as-of means NOW)
    (rq/execute-block-query! block-id sql nil nil)))