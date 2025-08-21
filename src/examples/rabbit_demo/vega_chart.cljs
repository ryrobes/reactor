(ns examples.rabbit-demo.vega-chart
  "Vega-Lite chart component with EDN configuration"
  (:require [reagent.core :as reagent]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.pprint :as pprint]
            [examples.rabbit-demo.monaco :as monaco]))

(def base-font "Ubuntu")
(def base-single-color "magenta")

(defn format-edn
  "Format EDN data structure to a pretty-printed string"
  [data]
  (with-out-str (pprint/pprint data)))

;; Default Vega-Lite spec template
(def default-vega-spec
  {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
   :description "A simple bar chart"
   :mark {:type "bar"
          :color "#ff4f99"}  ;; Pink color to match the chart block theme
   :encoding {:x {:field "x-field"
                  :type "nominal"
                  :title "X Axis"
                  :axis {:labelColor "#ff4f99"
                         :titleColor "#ff006e"
                         :gridColor "rgba(255, 0, 110, 0.1)"
                         :domainColor "#ff006e"
                         :tickColor "#ff006e"}}
              ;:color {:value 1}
              :y {:field "y-field"
                  :type "quantitative"
                  :aggregate "sum"
                  :title "Y Axis"
                  :axis {:labelColor "#ff4f99"
                         :titleColor "#ff006e"
                         :gridColor "rgba(255, 0, 110, 0.1)"
                         :domainColor "#ff006e"
                         :tickColor "#ff006e"}}}
   :width "container"
   :height "container"
   :autosize {:type "fit" :contains "padding"}
   :background "transparent"  ;; Transparent background
   :config {:view   {:stroke "transparent"}
            :axis   {:domainColor "#ffffff22"
                     :grid        true
                     :labelColor  "#ffffff88"
                     :titleFont   base-font
                     :axisFont    base-font
                     :font        base-font
                     :titleColor  "#ffffff99"
                     :labelFont   base-font
                     :domain      false
                     :gridColor   "#ffffff22"
                     :tickColor   "#ffffff22"
                     :labelFontSize 10
                     :titleFontSize 11}
            :legend {:labelFont  base-font
                     :legendFont base-font
                     :labelColor "#ffffff99"
                     :titleColor "#ffffff99"
                     :stroke     "#ffffff99"
                     :titleFont  base-font}
            :header {:labelFont base-font
                     :titleFont base-font
                     :titleColor "#ffffff99"}
            :mark {:color base-single-color
                   :font base-font}
            :point {:color base-single-color}
            :area {:fill "#f0f0f0"
                   :stroke "#ffffff"
                   :strokeWidth 1}
            :title  {:font         base-font
                     :subtitleFont base-font
                     :labelFont    base-font
                     :titleFont    base-font
                     :titleColor   "#ffffff99"}}})

(defn render-vega-chart!
  "Render a Vega-Lite chart into a DOM element"
  ([element-id spec data] (render-vega-chart! element-id spec data nil))
  ([element-id spec data height]
   (js/console.log "[VEGA] Rendering chart with spec:" (clj->js spec) "and data:" (clj->js data) "height:" height "element-id:" element-id)
   (when-let [elem (.getElementById js/document element-id)]
     (try
       ;; Clear any existing chart content first to avoid duplicates
       (set! (.-innerHTML elem) "")
       ;; Convert EDN spec to JS object, ensuring data is included
       ;; Override height with calculated pixel value if provided
       (let [js-spec (clj->js (cond-> (assoc spec :data {:values (or data [])})
                                height (assoc :height height)
                                ;; Keep width as container for responsiveness
                                true (assoc :width "container")))]
         (js/console.log "[VEGA] Final spec for vegaEmbed:" js-spec)
         ;; Use vegaEmbed for interactive charts
         (if (exists? js/vegaEmbed)
           (-> (js/vegaEmbed (str "#" element-id) js-spec
                            #js {:actions {:export true
                                          :source false
                                          :compiled false
                                          :editor false}
                                 :hover true  ; Enable hover interactions
                                 :renderer "canvas"  ; Canvas is better for interactions
                                 :theme nil  ; Don't use the dark theme which adds background
                                 :tooltip true  ; Enable tooltips
                                 :width "container"  ; Responsive width
                                 :height (if height height "container")}) ; Use calculated height or container
               (.then (fn [result]
                        (js/console.log "Vega chart rendered successfully in" element-id)))
               (.catch (fn [error]
                         (js/console.error "Error embedding Vega chart:" error))))
           (js/console.error "vegaEmbed not found. Please include Vega-Lite libraries in HTML")))
       (catch js/Error e
         (js/console.error "Error rendering Vega chart:" e))))))

(defn field-selector
  "Simple dropdown for selecting a field from the data"
  [{:keys [value on-change data label allow-none?]}]
  (let [fields (when (seq data)
                 (keys (first data)))]
    [:div {:style {:display "flex"
                   :align-items "center"
                   :gap "10px"
                   :margin "5px 0"}}
     [:label {:style {:color "#ff006e"
                      :font-family "monospace"
                      :font-size "10px"
                      :text-transform "uppercase"
                      :min-width "80px"}}
      label]
     [:select {:style {:flex 1
                       :background "rgba(0,0,0,0.3)"
                       :color "#ff4f99"
                       :border "1px solid rgba(255,0,110,0.3)"
                       :border-radius "2px"
                       :padding "4px 8px"
                       :font-family "monospace"
                       :font-size "11px"
                       :cursor "pointer"}
               :value (or value "")
               :on-change #(on-change (-> % .-target .-value))}
      [:option {:value ""} (if allow-none? "(none)" "Select field...")]
      (for [field fields]
        ^{:key field}
        [:option {:value (name field)} (name field)])]]))

(defn chart-type-selector
  "Dropdown for selecting chart type"
  [{:keys [value on-change]}]
  [:div {:style {:display "flex"
                 :align-items "center"
                 :gap "10px"
                 :margin "5px 0"}}
   [:label {:style {:color "#ff006e"
                    :font-family "monospace"
                    :font-size "10px"
                    :text-transform "uppercase"
                    :min-width "80px"}}
    "Chart Type:"]
   [:select {:style {:flex 1
                     :background "rgba(0,0,0,0.3)"
                     :color "#ff4f99"
                     :border "1px solid rgba(255,0,110,0.3)"
                     :border-radius "2px"
                     :padding "4px 8px"
                     :font-family "monospace"
                     :font-size "11px"
                     :cursor "pointer"}
             :value value
             :on-change #(on-change (-> % .-target .-value))}
    [:option {:value "bar"} "Bar Chart"]
    [:option {:value "line"} "Line Chart"]
    [:option {:value "point"} "Scatter Plot"]
    [:option {:value "area"} "Area Chart"]
    [:option {:value "rect"} "Heatmap"]
    [:option {:value "circle"} "Bubble Chart"]]])


(declare render-vega-chart!)


(defn vega-chart-component
  "Main Vega-Lite chart component with simple and edit modes"
  [{:keys [id data on-config-change config block-size]}]
  (let [;; Local state
        edit-mode? (reagent/atom (boolean (:edit-mode config)))
        collapsed? (reagent/atom (boolean (:collapsed config)))
        ;; Parse existing config or use defaults
        current-config (reagent/atom (merge default-vega-spec (:vega-spec config)))
        edn-text (reagent/atom (format-edn @current-config))
        ;; Simple mode selections
        x-field (reagent/atom (get-in @current-config [:encoding :x :field] ""))
        y-field (reagent/atom (get-in @current-config [:encoding :y :field] ""))
        color-field (reagent/atom (get-in @current-config [:encoding :color :field] ""))
        chart-type (reagent/atom (or (if (map? (:mark @current-config))
                                       (:type (:mark @current-config))
                                       (:mark @current-config))
                                     "bar"))
        ;; Track if we've initialized
        initialized? (reagent/atom false)
        ;; Unique element ID for this chart - ensure it's a valid CSS selector
        ;; Replace any invalid CSS selector characters with dashes
        ;; Use different IDs for collapsed and expanded states to avoid conflicts
        chart-elem-id-base (str "vega-chart-" (str/replace (str id) #"[^a-zA-Z0-9\-_]" "-"))
        ;; For backward compatibility, keep chart-elem-id as the base
        chart-elem-id chart-elem-id-base
        ;; Calculate chart height based on block size and UI elements
        calculate-chart-height (fn []
                                 (if block-size
                                   (let [block-height (:height block-size) ;(or (:height block-size) 400)
                                         ;; Subtract UI elements more precisely:
                                         ;; - Mode toggle bar: 50px
                                         ;; - Controls (when not collapsed): 
                                         ;;   - 4 dropdowns @ 35px each = 140px
                                         ;;   - Margins/padding = 20px
                                         ;; - Container padding: 20px (10px top/bottom)
                                         ;; - Scrollbar/safety buffer: 25px to prevent any scrolling
                                         ui-height (cond
                                                     @collapsed? 120  ; Toggle bar + padding + buffer (50 + 20 + 25)
                                                     @edit-mode? 165  ; Toggle bar + apply button + padding + buffer
                                                     :else 100)  ; Toggle bar + all controls + padding + buffer
                                        ;;  ui-height (if
                                        ;;              (or @collapsed? @edit-mode?) 130  ; Toggle bar + padding + buffer (50 + 20 + 25)
                                        ;;              ;@edit-mode? 165  ; Toggle bar + apply button + padding + buffer
                                        ;;              170)  ; Toggle bar + all controls + padding + buffer
                                         chart-height (- block-height ui-height 55) ;(max 150 (- block-height ui-height))
                                         ;;_ (println "!!!!!!CHART_SIZES!!!!!!" block-size block-height ui-height @collapsed? @edit-mode?)
                                         ]
                                     (js/console.log "[VEGA-CHART] Block height:" block-height "UI height:" ui-height "Chart height:" chart-height)
                                     chart-height)
                                   ;; Default height if no block-size
                                   300))
        ;; Get the current chart element ID based on collapsed state
        get-chart-elem-id (fn [] 
                           (if @collapsed?
                             (str chart-elem-id-base "-collapsed")
                             (str chart-elem-id-base "-expanded")))]
    
    (reagent/create-class
     {:component-did-mount
      (fn [this]
        ;; Render initial chart
        (let [props (reagent/props this)
              ;; Props come from the reagent-render function, which passes the original props
              {:keys [data]} props]
          (js/console.log "[VEGA-CHART] component-did-mount with data:" (clj->js data))
          (when (and data (seq data) (not @edit-mode?))
            ;; Auto-select fields if not set
            (when (and (empty? @x-field) (empty? @y-field))
              (let [fields (keys (first data))
                    first-field (name (first fields))
                    second-field (name (second fields))]
                (when first-field (reset! x-field first-field))
                (when second-field (reset! y-field second-field))
                (let [spec (-> @current-config
                              (assoc-in [:encoding :x :field] first-field)
                              (assoc-in [:encoding :y :field] (or second-field first-field))
                              (assoc-in [:encoding :x :type] "nominal")
                              (assoc-in [:encoding :y :type] "quantitative")
                              (assoc-in [:encoding :y :aggregate] "sum"))
                      _ (println "SPEC" spec)]
                  (reset! current-config spec))))
            ;; Render chart
            (js/setTimeout #(render-vega-chart! (get-chart-elem-id) @current-config data) 100))))
      
      :component-did-update
      (fn [this [_ old-props]]
        (let [new-props (reagent/props this)
              new-data (:data new-props)
              old-data (:data old-props)]
          (js/console.log "[VEGA-CHART] component-did-update - old data:" (clj->js old-data) "new data:" (clj->js new-data))
          ;; Re-render when data changes or collapsed state changes
          (when (and (not= new-data old-data) 
                     (seq new-data)
                     (not @edit-mode?))
            ;; Auto-select fields if not set and data structure changed
            (when (and (empty? @x-field) (empty? @y-field))
              (let [fields (keys (first new-data))
                    first-field (name (first fields))
                    second-field (name (second fields))]
                (when first-field (reset! x-field first-field))
                (when second-field (reset! y-field second-field))
                (let [spec (-> @current-config
                              (assoc-in [:encoding :x :field] first-field)
                              (assoc-in [:encoding :y :field] (or second-field first-field))
                              (assoc-in [:encoding :x :type] "nominal")
                              (assoc-in [:encoding :y :type] "quantitative")
                              (assoc-in [:encoding :y :aggregate] "sum"))]
                  (reset! current-config spec))))
            (render-vega-chart! (get-chart-elem-id) @current-config new-data))
          ;; Also re-render when collapsed state changes  
          (when (and (seq new-data) @collapsed? (not @edit-mode?))
            (js/setTimeout #(render-vega-chart! (get-chart-elem-id) @current-config new-data) 100))))
      
      :reagent-render
      (fn [{:keys [id data on-config-change config block-size]}]
        (js/console.log "[VEGA-CHART]" id "rendering with data:" (clj->js data) "edit-mode?" @edit-mode? "x-field:" @x-field "y-field:" @y-field "initialized?" @initialized?)
        ;; Auto-initialize and render when we have data and fields aren't set
        (when (and data (seq data) (empty? @x-field) (empty? @y-field) (not @edit-mode?))
          (let [fields (keys (first data))
                ;;_ (or @collapsed? @edit-mode?)  ;;force rerender
                first-field (if (keyword? (first fields)) 
                              (name (first fields))
                              (str (first fields)))
                second-field (if (keyword? (second fields))
                               (name (second fields)) 
                               (str (second fields)))]
            (js/console.log "[VEGA-CHART] Auto-initializing with fields:" first-field "and" second-field "from fields:" (clj->js fields))
            (when first-field 
              (reset! x-field first-field)
              (reset! initialized? true))
            (when second-field 
              (reset! y-field second-field))
            (let [spec (-> @current-config
                          (assoc-in [:encoding :x :field] first-field)
                          (assoc-in [:encoding :y :field] (or second-field first-field))
                          (assoc-in [:encoding :x :type] "nominal")
                          (assoc-in [:encoding :y :type] "quantitative")
                          (assoc-in [:encoding :y :aggregate] "sum"))]
              (reset! current-config spec)
              ;; Render chart immediately after a delay to ensure DOM is ready
              (js/setTimeout #(do 
                               (js/console.log "[VEGA-CHART] Rendering chart now with spec:" (clj->js spec) "and data:" (clj->js data))
                               (render-vega-chart! (get-chart-elem-id) spec data)) 
                            100))))
        ;; Also render if we have data and fields are already set
        (when (and data (seq data) (not (empty? @x-field)) (not (empty? @y-field)) (not @edit-mode?) @initialized?)
          (js/setTimeout #(do 
                           (js/console.log "[VEGA-CHART] Re-rendering with existing fields")
                           (render-vega-chart! (get-chart-elem-id) @current-config data)) 
                        50))
        [:div {:style {:height "100%"
                       :display "flex"
                       :flex-direction "column"}}
         ;; Mode toggle with collapse button
         [:div {:style {:display "flex"
                        :justify-content "space-between"
                        :margin-bottom "10px"
                        :padding "5px"
                        :background "rgba(0,0,0,0.3)"
                        :border-radius "4px"}}
          [:div {:style {:display "flex"
                         :gap "5px"
                         :align-items "center"}}
           ;; Collapse/expand button
           [:button {:style {:padding "4px 6px"
                            :background "transparent"
                            :color "#ff006e"
                            :border "1px solid #ff006e"
                            :border-radius "2px"
                            :cursor "pointer"
                            :font-family "monospace"
                            :font-size "12px"
                            :line-height "1"
                            :display "flex"
                            :align-items "center"
                            :justify-content "center"}
                     :on-click (fn []
                                (swap! collapsed? not)
                                (when on-config-change
                                  (on-config-change {:vega-spec @current-config
                                                    :edit-mode @edit-mode?
                                                    :collapsed @collapsed?}))
                                ;; Re-render chart with new height after collapse state changes
                                (when (and (seq data) (not @edit-mode?))
                                  (js/setTimeout 
                                   #(render-vega-chart! (get-chart-elem-id) @current-config data (calculate-chart-height))
                                   50)))}
            (if @collapsed? "▶" "▼")]
           [:button {:style {:padding "4px 8px"
                            :background (if @edit-mode? 
                                         "transparent"
                                         "linear-gradient(90deg, #ff006e 0%, #ff4f99 100%)")
                            :color (if @edit-mode? "#ff006e" "#0a0a0a")
                            :border "1px solid #ff006e"
                            :border-radius "2px 0 0 2px"
                            :cursor "pointer"
                            :font-family "monospace"
                            :font-size "10px"
                            :font-weight "bold"}
                     :on-click (fn []
                                (reset! edit-mode? false)
                                ;; Apply simple mode changes
                                (let [spec (-> @current-config
                                             (assoc :mark @chart-type)
                                             (assoc-in [:encoding :x :field] @x-field)
                                             (assoc-in [:encoding :y :field] @y-field))]
                                  (reset! current-config spec)
                                  (when on-config-change
                                    (on-config-change {:vega-spec spec 
                                                      :edit-mode false}))
                                  ;; Re-render chart
                                  (js/setTimeout 
                                   #(render-vega-chart! (get-chart-elem-id) spec data) 
                                   100)))}
            "SIMPLE"]
           [:button {:style {:padding "4px 8px"
                            :background (if @edit-mode?
                                         "linear-gradient(90deg, #ff006e 0%, #ff4f99 100%)"
                                         "transparent")
                            :color (if @edit-mode? "#0a0a0a" "#ff006e")
                            :border "1px solid #ff006e"
                            :border-radius "0 2px 2px 0"
                            :cursor "pointer"
                            :font-family "monospace"
                            :font-size "10px"
                            :font-weight "bold"}
                     :on-click (fn []
                                (reset! edit-mode? true)
                                (reset! edn-text (format-edn @current-config))
                                (when on-config-change
                                  (on-config-change {:vega-spec @current-config
                                                    :edit-mode true})))}
            "EDIT"]]
          (when (seq data)
            [:span {:style {:color "#ff4f99"
                           :font-family "monospace"
                           :font-size "10px"
                           :opacity 0.7}}
             (str (count data) " rows")])]
         
         ;; Content area - use conditional rendering to avoid duplicate divs
         (if @collapsed?
           ;; When collapsed, show the chart full-size
           [:div {:key "collapsed-chart"
                  :id (str chart-elem-id-base "-collapsed")
                  :style {:flex 1
                         :background "rgba(0,0,0,0.3)"
                         :borderRadius "4px"
                         :padding "10px"
                         :height (str (or (calculate-chart-height) 300) "px")
                         :position "relative"}}]
           ;; When not collapsed, show controls and chart
           [:div {:style {:flex 1
                          :min-height 0
                          :overflow "auto"}}
            (if @edit-mode?
              ;; Edit mode branch
              [:div {:style {:height "100%"
                            :display "flex"
                            :flex-direction "column"}}
               [:div {:style {:flex 1
                             :border "1px solid rgba(255,0,110,0.3)"
                             :border-radius "4px"
                             :overflow "hidden"}}
                [monaco/edn-editor
                 {:value @edn-text
                  :on-change #(reset! edn-text %)
                  :height "100%"}]]  ;; Will use rabbit-theme by default
               [:button {:style {:margin-top "5px"
                                :padding "5px 10px"
                                :background "linear-gradient(90deg, #ff006e 0%, #ff4f99 100%)"
                                :color "#0a0a0a"
                                :border "none"
                                :border-radius "2px"
                                :cursor "pointer"
                                :font-weight "bold"
                                :font-family "monospace"
                                :font-size "11px"
                                :text-transform "uppercase"}
                         :on-click (fn []
                                    (try
                                      ;; Parse EDN and update config
                                      (let [new-spec (edn/read-string @edn-text)]
                                        (reset! current-config new-spec)
                                        (when on-config-change
                                          (on-config-change {:vega-spec new-spec
                                                            :edit-mode true}))
                                        ;; Re-render chart
                                        (render-vega-chart! (get-chart-elem-id) new-spec data (calculate-chart-height)))
                                      (catch js/Error e
                                        (js/console.error "Invalid EDN:" e))))}
                "Apply Changes"]]
              ;; Simple mode branch
              [:div {:style {:height "100%"
                            :display "flex"
                            :flex-direction "column"}}
               ;; Show controls if we have data
               (if (and data (seq data))
                 [:div {:style {:padding "10px"
                               :background "rgba(0,0,0,0.3)"
                               :border-radius "4px"
                               :margin-bottom "10px"}}
                  [chart-type-selector 
                   {:value @chart-type
                    :on-change (fn [val]
                                (reset! chart-type val)
                                (let [spec (-> @current-config
                                             (assoc :mark {:type val :color "#ff4f99"})
                                             ;; Add/remove aggregate based on chart type
                                             (cond-> 
                                               (contains? #{"bar" "area"} val)
                                               (assoc-in [:encoding :y :aggregate] "sum")
                                               (contains? #{"line" "point" "circle"} val)
                                               (update-in [:encoding :y] dissoc :aggregate)))]
                                  (reset! current-config spec)
                                  (when on-config-change
                                    (on-config-change {:vega-spec spec :edit-mode false}))
                                  (render-vega-chart! (get-chart-elem-id) spec data (calculate-chart-height))))}]
                  [field-selector 
                   {:label "X Axis:"
                    :value @x-field
                    :data data
                    :on-change (fn [val]
                                (reset! x-field val)
                                (let [spec (-> @current-config
                                              (assoc-in [:encoding :x :field] val)
                                              (assoc-in [:encoding :x :type] "nominal"))]
                                  (reset! current-config spec)
                                  (when on-config-change
                                    (on-config-change {:vega-spec spec :edit-mode false}))
                                  (render-vega-chart! (get-chart-elem-id) spec data (calculate-chart-height))))}]
                  [field-selector 
                   {:label "Y Axis:"
                    :value @y-field
                    :data data
                    :on-change (fn [val]
                                (reset! y-field val)
                                (let [spec (-> @current-config
                                              (assoc-in [:encoding :y :field] val)
                                              (assoc-in [:encoding :y :type] "quantitative")
                                              (assoc-in [:encoding :y :aggregate] "sum"))]
                                  (reset! current-config spec)
                                  (when on-config-change
                                    (on-config-change {:vega-spec spec :edit-mode false}))
                                  (render-vega-chart! (get-chart-elem-id) spec data (calculate-chart-height))))}]
                  [field-selector 
                   {:label "Color:"
                    :value @color-field
                    :data data
                    :allow-none? true
                    :on-change (fn [val]
                                (reset! color-field val)
                                (let [spec (if (empty? val)
                                            ;; Remove color encoding if "(none)" selected
                                            (update @current-config :encoding dissoc :color)
                                            ;; Add color encoding
                                            (-> @current-config
                                                (assoc-in [:encoding :color :field] val)
                                                (assoc-in [:encoding :color :type] "nominal")))]
                                  (reset! current-config spec)
                                  (when on-config-change
                                    (on-config-change {:vega-spec spec :edit-mode false}))
                                  (render-vega-chart! (get-chart-elem-id) spec data (calculate-chart-height))))}]]
                 ;; No data yet but connected
                 [:div {:style {:padding "10px"
                               :background "rgba(0,0,0,0.3)"
                               :border-radius "4px"
                               :margin-bottom "10px"
                               :text-align "center"
                               :color "#ff4f99"
                               :font-family "monospace"
                               :opacity 0.7}}
                  "Waiting for query results..."])
               ;; Chart container 
               [:div {:key "expanded-chart"
                      :id (str chart-elem-id-base "-expanded")
                      :style {:background "rgba(0,0,0,0.3)"
                             :borderRadius "4px"
                             :padding "10px"
                             :height (str (calculate-chart-height) "px")
                             :position "relative"}}]])])])})))