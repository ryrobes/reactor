(ns examples.todo-app.app
  "Todo app demonstrating Reactor as a re-frame alternative.
   Clean, simple API with server-side reactivity using re-com UI."
  (:require [reagent.core :as r]
            [reagent.dom :as rdom]
            [reactor.client :as reactor]
            [re-com.core :as rc]
            [clojure.string :as str]))

;; ===== Reactor Connection =====

(defonce connection 
  (reactor/connect! "http://localhost:9000"
                    {:format :edn
                     :initial-state {:todos {}
                                    :filter :all}}))

;; ===== Subscriptions (direct from Reactor) =====

(defn todos []
  @(reactor/subscribe connection [:todos]))

(defn current-filter []
  @(reactor/subscribe connection [:filter]))

(defn visible-todos []
  (let [all-todos @(reactor/subscribe connection [:todos])
        filter-type @(reactor/subscribe connection [:filter])]
    (case filter-type
      :active (into {} (filter #(not (:completed (val %))) all-todos))
      :completed (into {} (filter #(:completed (val %)) all-todos))
      :all all-todos)))

(defn todo-count []
  (count (filter #(not (:completed (val %))) (todos))))

(defn completed-count []
  (count (filter #(:completed (val %)) (todos))))

(defn all-completed? []
  (let [todos (todos)]
    (and (seq todos)
         (every? :completed (vals todos)))))

;; ===== Dispatches (to server) =====

(defn dispatch!
  "Direct server dispatch - no local handlers needed!"
  [event]
  (reactor/dispatch! connection event))

;; ===== Todo Item Component =====

(defn todo-item [{:keys [id text completed]}]
  [:div {:style {:display "flex"
                 :flex-direction "row"
                 :align-items "center"
                 :padding "15px 20px"
                 :background-color (if completed "rgba(0, 255, 204, 0.02)" "rgba(0, 0, 0, 0.3)")
                 :border-bottom "1px solid rgba(0, 255, 204, 0.15)"
                 :transition "all 0.3s ease"}
         :on-mouse-enter #(set! (.. % -currentTarget -style -backgroundColor) "rgba(255, 0, 128, 0.05)")
         :on-mouse-leave #(set! (.. % -currentTarget -style -backgroundColor) 
                               (if completed "rgba(0, 255, 204, 0.02)" "rgba(0, 0, 0, 0.3)"))}
   [:input {:type "checkbox"
            :checked completed
            :style {:width "20px"
                    :height "20px"
                    :margin-right "15px"
                    :cursor "pointer"}
            :on-change #(dispatch! [:toggle-todo id])}]
   
   [:span {:style {:flex "1"
                   :font-size "18px"
                   :font-family "'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif"
                   :color (if completed "rgba(0, 255, 204, 0.5)" "#00ffcc")
                   :text-decoration (if completed "line-through" "none")
                   :cursor "pointer"
                   :user-select "none"}
           :on-click #(dispatch! [:toggle-todo id])}
    text]
   
   [:button {:style {:background "transparent"
                     :border "1px solid #ff0080"
                     :color "#ff0080"
                     :font-size "20px"
                     :padding "4px 10px"
                     :cursor "pointer"
                     :border-radius "4px"
                     :line-height "1"
                     :opacity "0.7"
                     :transition "all 0.2s"}
             :on-mouse-enter #(set! (.. % -currentTarget -style -opacity) "1")
             :on-mouse-leave #(set! (.. % -currentTarget -style -opacity) "0.7")
             :on-click #(dispatch! [:delete-todo id])}
    "×"]])

;; ===== Todo Input Component =====

(defn todo-input []
  (let [value (r/atom "")]
    (fn []
      [rc/v-box
       :gap "25px"
       :style {:padding "30px 20px"
               :background "linear-gradient(135deg, rgba(255, 0, 128, 0.1) 0%, rgba(0, 255, 204, 0.1) 100%)"
               :border-bottom "2px solid rgba(255, 0, 128, 0.3)"}
       :children [[rc/title
                   :level :level1
                   :label "REACTOR://TODOS"
                   :style {:color "#00ffcc"
                           :text-align "center"
                           :font-family "'Courier New', monospace"
                           :font-weight "bold"
                           :letter-spacing "6px"
                           :text-shadow "0 0 30px rgba(0, 255, 204, 0.6), 0 0 60px rgba(255, 0, 128, 0.4)"
                           :margin "0"}]
                  
                  [rc/input-text
                   :model value
                   :placeholder "What needs to be done?"
                   :change-on-blur? false
                   :width "100%"
                   :on-change #(reset! value %)
                   :style {:font-size "16px"
                           :padding "12px 16px"
                           :background-color "rgba(0, 0, 0, 0.6)"
                           :border "2px solid rgba(0, 255, 204, 0.3)"
                           :border-radius "4px"
                           :color "#00ffcc"
                           :font-family "'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif"}
                   :attr {:on-key-down 
                          (fn [e]
                            (when (= (.-key e) "Enter")
                              (let [text (str/trim @value)]
                                (when (seq text)
                                  (dispatch! [:add-todo text])
                                  (reset! value "")))))}]]])))

;; ===== Filter Component =====

(defn todo-filters []
  (let [filter (current-filter)]
    [:div {:style {:display "flex"
                   :gap "15px"
                   :align-items "center"}}
     [:a {:style {:color (if (= filter :all) "#ff0080" "#00ffcc")
                  :text-decoration "none"
                  :cursor "pointer"
                  :font-size "14px"
                  :font-weight "500"}
          :on-click #(dispatch! [:set-filter :all])}
      "All"]
     
     [:a {:style {:color (if (= filter :active) "#ff0080" "#00ffcc")
                  :text-decoration "none"
                  :cursor "pointer"
                  :font-size "14px"
                  :font-weight "500"}
          :on-click #(dispatch! [:set-filter :active])}
      "Active"]
     
     [:a {:style {:color (if (= filter :completed) "#ff0080" "#00ffcc")
                  :text-decoration "none"
                  :cursor "pointer"
                  :font-size "14px"
                  :font-weight "500"}
          :on-click #(dispatch! [:set-filter :completed])}
      "Completed"]]))

;; ===== Stats Component =====

(defn todo-stats []
  (let [active-count (todo-count)
        completed-count (completed-count)]
    [:div {:style {:display "flex"
                   :justify-content "space-between"
                   :align-items "center"
                   :padding "15px 20px"
                   :background "linear-gradient(90deg, rgba(0, 0, 0, 0.6) 0%, rgba(255, 0, 128, 0.1) 100%)"
                   :border-top "2px solid rgba(255, 0, 128, 0.3)"}}
     [:span {:style {:color "#00ffcc"
                     :font-weight "500"
                     :font-size "14px"}}
      (str active-count " " (if (= active-count 1) "item" "items") " left")]
     
     [todo-filters]
     
     (if (pos? completed-count)
       [:button {:style {:background-color "rgba(255, 0, 128, 0.2)"
                         :border "1px solid #ff0080"
                         :border-radius "4px"
                         :color "#ff0080"
                         :padding "6px 12px"
                         :font-size "14px"
                         :cursor "pointer"
                         :transition "all 0.3s"}
                 :on-mouse-enter #(set! (.. % -currentTarget -style -backgroundColor) "rgba(255, 0, 128, 0.4)")
                 :on-mouse-leave #(set! (.. % -currentTarget -style -backgroundColor) "rgba(255, 0, 128, 0.2)")
                 :on-click #(dispatch! [:clear-completed])}
        "Clear completed"]
       [:div])]))

;; ===== Main Todo List =====

(defn todo-list []
  (let [todos (visible-todos)
        all-completed? (all-completed?)]
    [rc/v-box
     :children [(when (seq todos)
                  [:div {:style {:display "flex"
                                 :align-items "center"
                                 :padding "15px 20px"
                                 :background-color "rgba(0, 0, 0, 0.2)"
                                 :border-bottom "1px solid rgba(255, 0, 128, 0.2)"}}
                   [:input {:type "checkbox"
                            :checked all-completed?
                            :style {:width "20px"
                                    :height "20px"
                                    :margin-right "15px"
                                    :cursor "pointer"}
                            :on-change #(dispatch! [:toggle-all])}]
                   [:label {:style {:color "rgba(0, 255, 204, 0.7)"
                                    :cursor "pointer"
                                    :user-select "none"
                                    :font-size "16px"}
                            :on-click #(dispatch! [:toggle-all])}
                    "Mark all as complete"]])
                
                [rc/v-box
                 :children (for [[id todo] todos]
                             ^{:key id} [todo-item todo])]]]))

;; ===== Time Travel UI =====

(defn time-travel-panel []
  (let [expanded? (r/atom true)]  ;; Start expanded to show scrubber
    (fn []
      (let [time-travel @(reactor/subscribe connection [:time-travel])
            history-count (or (get time-travel :history-count) 0)
            current-index (or (get time-travel :current-index) 0)
            future-count (or (get time-travel :future-count) 0)
            max-index (or (get time-travel :max-index) 0)
            can-undo? (boolean (get time-travel :can-undo))
            can-redo? (boolean (get time-travel :can-redo))]
        [rc/v-box
         :style {:position "fixed"
                 :bottom "20px"
                 :right "20px"
                 :background-color "rgba(10, 10, 30, 0.95)"
                 :border "1px solid #ff0080"
                 :border-radius "4px"
                 :padding "15px"
                 :min-width "300px"
                 :z-index 1000}
         :gap "12px"
         :children [;; Header
                    [rc/h-box
                     :justify :between
                     :align :center
                     :children [[rc/label
                                 :label "⏰ TIME TRAVEL"
                                 :style {:color "#ff0080"
                                         :font-weight "bold"
                                         :font-size "14px"}]
                                [rc/md-icon-button
                                 :md-icon-name (if @expanded? "zmdi-chevron-down" "zmdi-chevron-up")
                                 :size :smaller
                                 :style {:color "#ff0080"}
                                 :on-click #(swap! expanded? not)]]]
                    
                    ;; Controls
                    [rc/h-box
                     :gap "10px"
                     :align :center
                     :children [[rc/button
                                 :label "◄ UNDO"
                                 :disabled? (not can-undo?)
                                 :style {:background-color (if can-undo? "#ff0080" "#333")
                                         :color (if can-undo? "#000" "#666")
                                         :border "none"
                                         :font-weight "bold"}
                                 :on-click #(dispatch! [:time-travel/undo])]
                                
                                [rc/label
                                 :label (str current-index "/" max-index)
                                 :style {:color "#00ffcc"
                                         :font-size "12px"
                                         :min-width "60px"
                                         :text-align "center"}]
                                
                                [rc/button
                                 :label "REDO ►"
                                 :disabled? (not can-redo?)
                                 :style {:background-color (if can-redo? "#00ffcc" "#333")
                                         :color (if can-redo? "#000" "#666")
                                         :border "none"
                                         :font-weight "bold"}
                                 :on-click #(dispatch! [:time-travel/redo])]]]
                    
                    ;; Scrubber (when expanded)
                    (when @expanded?
                      [rc/v-box
                       :gap "10px"
                       :children [[rc/label
                                   :label "Time Travel Scrubber"
                                   :style {:color "#00ffcc"
                                           :font-size "13px"
                                           :font-weight "bold"}]
                                  
                                  [rc/label
                                   :label (str "History: " history-count " states | "
                                              "Future: " future-count " states")
                                   :style {:color "#ff0080"
                                           :font-size "11px"}]
                                  
                                  [rc/slider
                                   :model current-index
                                   :min 0
                                   :max max-index
                                   :step 1
                                   :width "100%"
                                   :on-change #(do
                                              (js/console.log "Jumping to index:" %)
                                              (dispatch! [:time-travel/jump-to %]))]
                                  
                                  [rc/label
                                   :label "Drag slider to jump to any point in time"
                                   :style {:color "#888"
                                           :font-size "10px"
                                           :font-style "italic"}]]])]]))))

;; ===== App Container =====

(defn todo-app []
  [rc/v-box
   :height "100vh"
   :style {:background "linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%)"
           :padding "60px 20px"
           :overflow "auto"}
   :children [[rc/v-box
               :class "todoapp"
               :style {:max-width "700px"
                       :margin "0 auto"
                       :background "linear-gradient(135deg, rgba(15, 12, 41, 0.9) 0%, rgba(48, 43, 99, 0.9) 50%, rgba(36, 36, 62, 0.9) 100%)"
                       :border "2px solid rgba(255, 0, 128, 0.6)"
                       :border-radius "8px"
                       :box-shadow "0 20px 60px rgba(0, 0, 0, 0.5), 0 0 100px rgba(255, 0, 128, 0.3), inset 0 0 30px rgba(0, 255, 204, 0.1)"
                       :overflow "hidden"}
               :children [[todo-input]
                          [todo-list]
                          [todo-stats]]]
              
              [time-travel-panel]]])

;; ===== Mount App =====

(defn mount-root []
  (rdom/render [todo-app]
               (.getElementById js/document "app")))

(defn ^:export init! []
  (println "Initializing Reactor Todo App with re-com...")
  (mount-root))

;; Support hot reload in development
(defn ^:dev/after-load reload! []
  (mount-root))