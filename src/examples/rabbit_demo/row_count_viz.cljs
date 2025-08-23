(ns examples.rabbit-demo.row-count-viz
  "Row count visualization for time travel scrubber"
  (:require [reagent.core :as reagent]
            [reactor.core :as r]
            [clojure.string :as str]))

;; ============= Row Count Cache =============

(defonce row-count-cache 
  ;; {query-hash -> {timestamp -> count}}
  (reagent/atom {}))

(defonce pending-calculations
  ;; Track which calculations are in flight to avoid duplicates
  (atom #{}))

(defonce data-size-cache
  ;; {query-hash -> {timestamp -> size}}
  (reagent/atom {}))

(defn query-hash
  "Create a stable hash for a query to use as cache key"
  [sql]
  (str (hash sql)))

(defn get-cached-count
  "Get cached row count for a query at a specific timestamp"
  [sql timestamp]
  (get-in @row-count-cache [(query-hash sql) timestamp]))

(defn cache-count!
  "Cache a row count for a query at a specific timestamp"
  [sql timestamp count]
  (swap! row-count-cache assoc-in [(query-hash sql) timestamp] count))

(defn get-cached-size
  "Get cached data size for a query at a specific timestamp"
  [sql timestamp]
  (get-in @data-size-cache [(query-hash sql) timestamp]))

(defn cache-size!
  "Cache a data size for a query at a specific timestamp"
  [sql timestamp size]
  (swap! data-size-cache assoc-in [(query-hash sql) timestamp] size))

(defn calculate-row-count!
  "Calculate and cache row count for a query at a specific time"
  [sql timestamp callback]
  (let [cache-key [(query-hash sql) timestamp]]
    ;; Check if already cached
    (if-let [cached (get-cached-count sql timestamp)]
      (callback cached)
      ;; Check if calculation already in flight
      (when-not (contains? @pending-calculations cache-key)
        (swap! pending-calculations conj cache-key)
        ;; Build count query with time travel
        (let [count-sql (str "SELECT COUNT(*) as cnt FROM (" sql ") as subq")
              as-of-val (when timestamp 
                         (.toISOString (js/Date. timestamp)))]
          ;; Execute the count query
          (-> (r/sql-query! count-sql nil as-of-val)
              (.then (fn [response]
                       (let [;; Handle various response formats and empty results
                             count (or (get-in response [:results 0 :cnt])
                                      (get-in response [:results 0 :CNT])
                                      (get-in response [:results 0 "cnt"])
                                      (get-in response [:results 0 "CNT"])
                                      0)]  ; Default to 0 if no count found
                         ;; Cache the result
                         (cache-count! sql timestamp count)
                         ;; Clear pending flag
                         (swap! pending-calculations disj cache-key)
                         ;; Invoke callback
                         (callback count))))
              (.catch (fn [error]
                        (js/console.warn "Error calculating row count for timestamp" timestamp ":" error)
                        ;; Cache 0 for failed queries so we don't retry
                        (cache-count! sql timestamp 0)
                        (swap! pending-calculations disj cache-key)
                        (callback 0)))))))))

(defn calculate-data-size!
  "Calculate and cache data size for a query at a specific time (from metrics)"
  [sql timestamp callback]
  (let [cache-key [(query-hash sql) timestamp]]
    ;; Check if already cached
    (if-let [cached (get-cached-size sql timestamp)]
      (callback cached)
      ;; Check if calculation already in flight
      (when-not (contains? @pending-calculations cache-key)
        (swap! pending-calculations conj cache-key)
        ;; Execute the full query to get metrics
        (let [as-of-val (when timestamp 
                         (.toISOString (js/Date. timestamp)))]
          ;; Execute the query
          (-> (r/sql-query! sql nil as-of-val)
              (.then (fn [response]
                       (let [;; Get data-size from metrics
                             size (or (get-in response [:metrics :data-size])
                                     ;; Fallback: estimate from result count * average row size
                                     (let [results (:results response [])]
                                       (if (empty? results)
                                         0
                                         ;; Simple estimation: count * 100 chars per row average
                                         (* (count results) 100))))]
                         ;; Cache the result
                         (cache-size! sql timestamp size)
                         ;; Clear pending flag
                         (swap! pending-calculations disj cache-key)
                         ;; Invoke callback
                         (callback size))))
              (.catch (fn [error]
                        (js/console.warn "Error calculating data size for timestamp" timestamp ":" error)
                        ;; Cache 0 for failed queries so we don't retry
                        (cache-size! sql timestamp 0)
                        (swap! pending-calculations disj cache-key)
                        (callback 0)))))))))

(defn calculate-missing-counts!
  "Calculate row counts only for timestamps not already cached"
  [sql timestamps on-complete]
  (let [;; Check which timestamps are already cached
        query-key (query-hash sql)
        cached-counts (get @row-count-cache query-key {})
        missing-timestamps (filter #(not (contains? cached-counts %)) timestamps)
        ;; If all are cached, return immediately
        _ (when (empty? missing-timestamps)
            (on-complete cached-counts))
        ;; Calculate only missing ones
        total (count missing-timestamps)
        results (atom cached-counts)  ; Start with cached values
        completed (atom 0)]
    (when (seq missing-timestamps)
      (js/console.log "Calculating row counts for" (count missing-timestamps) "new timestamps out of" (count timestamps))
      (doseq [ts missing-timestamps]
        (calculate-row-count! sql ts
          (fn [count]
            (swap! results assoc ts count)
            (swap! completed inc)
            (when (= @completed total)
              (on-complete @results))))))))

(defn calculate-missing-sizes!
  "Calculate data sizes only for timestamps not already cached"
  [sql timestamps on-complete]
  (let [;; Check which timestamps are already cached
        query-key (query-hash sql)
        cached-sizes (get @data-size-cache query-key {})
        missing-timestamps (filter #(not (contains? cached-sizes %)) timestamps)
        ;; If all are cached, return immediately
        _ (when (empty? missing-timestamps)
            (on-complete cached-sizes))
        ;; Calculate only missing ones
        total (count missing-timestamps)
        results (atom cached-sizes)  ; Start with cached values
        completed (atom 0)]
    (when (seq missing-timestamps)
      (js/console.log "Calculating data sizes for" (count missing-timestamps) "new timestamps out of" (count timestamps))
      (doseq [ts missing-timestamps]
        (calculate-data-size! sql ts
          (fn [size]
            (swap! results assoc ts size)
            (swap! completed inc)
            (when (= @completed total)
              (on-complete @results))))))))

;; ============= Visualization =============

(defn calculate-chart-data
  "Calculate differential or cumulative chart data from row counts"
  [counts timestamps cumulative?]
  (when (and (seq counts) (seq timestamps))
    (let [sorted-ts (sort timestamps)
          ;; Get counts in order
          values (map #(get counts % 0) sorted-ts)]
      (if cumulative?
        ;; Cumulative mode - just use the counts as-is
        values
        ;; Differential mode - calculate changes between ticks
        (let [diffs (map (fn [[prev curr]]
                          (Math/abs (- curr prev)))
                        (partition 2 1 values))]
          ;; First value is 0 (no previous to compare)
          (cons 0 diffs))))))

(defn normalize-values
  "Normalize values to 0-1 range for chart rendering"
  [values]
  (when (seq values)
    (let [max-val (apply max values)
          min-val 0]  ; Always start from 0
      (if (= max-val min-val)
        (repeat (count values) 0.5)  ; If all same, show at 50% height
        (map #(/ (- % min-val) (- max-val min-val)) values)))))

(defn render-area-chart
  "Render the area chart as SVG"
  [values width height & [mode]]
  (when (seq values)
    (let [normalized (normalize-values values)
          points-count (count normalized)
          x-step (/ width (max 1 (dec points-count)))
          ;; Create path points
          points (map-indexed
                  (fn [idx val]
                    {:x (* idx x-step)
                     :y (* height (- 1 val))})  ; Invert Y axis
                  normalized)
          ;; Build SVG path
          path-data (str
                     ;; Move to first point at bottom
                     "M 0," height " "
                     ;; Line to first data point
                     "L " (:x (first points)) "," (:y (first points)) " "
                     ;; Draw lines through all points
                     (str/join " " (map #(str "L " (:x %) "," (:y %)) (rest points)))
                     ;; Line down to bottom at last X
                     " L " (:x (last points)) "," height
                     ;; Close path back to start
                     " Z")]
      [:svg {:width width
             :height height
             :style {:position "absolute"
                     :top 0
                     :left 0
                     :pointer-events "none"}}
       ;; Area fill
       [:path {:d path-data
               :fill (case mode
                      :data-size "rgba(138,43,226,0.15)"  ; Purple for data-size
                      "rgba(0,255,159,0.15)")              ; Green for row counts
               :stroke "none"}]
       ;; Top line
       [:polyline {:points (str/join " " (map #(str (:x %) "," (:y %)) points))
                   :fill "none"
                   :stroke (case mode
                           :data-size "rgba(138,43,226,0.4)"  ; Purple for data-size
                           "rgba(0,255,159,0.4)")              ; Green for row counts
                   :stroke-width "1"}]])))

(defn row-count-overlay
  "Main component for row count visualization overlay"
  [{:keys [sql timestamps width height mode cumulative?]}]
  (let [counts (reagent/atom {})
        data-sizes (reagent/atom {})
        loading? (reagent/atom true)]
    (reagent/create-class
     {:component-did-mount
      (fn []
        (when (and sql (seq timestamps))
          (reset! loading? true)
          (if (= mode :data-size)
            ;; Fetch data sizes
            (calculate-missing-sizes! sql timestamps
              (fn [results]
                (reset! data-sizes results)
                (reset! loading? false)))
            ;; Fetch row counts
            (calculate-missing-counts! sql timestamps
              (fn [results]
                (reset! counts results)
                (reset! loading? false))))))
      
      :component-did-update
      (fn [this [_ old-props]]
        (let [new-props (reagent/props this)]
          ;; Recalculate if SQL, timestamps, or mode changed
          (when (or (not= (:sql old-props) (:sql new-props))
                   (not= (:timestamps old-props) (:timestamps new-props))
                   (not= (:mode old-props) (:mode new-props)))
            (reset! loading? true)
            (if (= (:mode new-props) :data-size)
              ;; Fetch data sizes
              (calculate-missing-sizes! (:sql new-props) (:timestamps new-props)
                (fn [results]
                  (reset! data-sizes results)
                  (reset! loading? false)))
              ;; Fetch row counts
              (calculate-missing-counts! (:sql new-props) (:timestamps new-props)
                (fn [results]
                  (reset! counts results)
                  (reset! loading? false)))))))
      
      :reagent-render
      (fn [{:keys [sql timestamps width height mode cumulative?]}]
        (when-not @loading?
          (let [;; Choose data source based on mode
                data-source (if (= mode :data-size) @data-sizes @counts)
                ;; If we have timestamps but no data yet, use zeros
                filled-data (if (empty? data-source)
                              (zipmap timestamps (repeat 0))
                              data-source)
                chart-data (calculate-chart-data filled-data timestamps cumulative?)]
            ;; Always render if we have timestamps
            (when (seq timestamps)
              [render-area-chart (or chart-data (repeat (count timestamps) 0)) width height mode]))))})))