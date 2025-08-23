(ns examples.todo-app.client
  "TODO app with the new clean Reactor API"
  (:require [reactor.core :as r]
            [reagent.core :as reagent]
            [reagent.dom :as rdom]
            [clojure.string :as str]))

;; Custom subscriptions
(r/reg-sub :filtered-todos
  (fn [db _]
    ;; Use server-computed filtered todos if available, otherwise fallback to client computation
    (if-let [filtered (:filtered-todos db)]
      (do (js/console.log "Using server filtered todos:" (clj->js filtered))
          filtered)
      (let [todos (vals (:todos db {}))
            filter-type (:filter db :all)]
        (js/console.log "Client filtering - filter:" filter-type "todos:" (clj->js todos))
        (case filter-type
          :active (filter (complement :completed) todos)
          :completed (filter :completed todos)
          todos)))))

(r/reg-sub :todo-stats
  (fn [db _]
    (let [todos (vals (:todos db {}))]
      {:total (count todos)
       :active (count (filter (complement :completed) todos))
       :completed (count (filter :completed todos))})))

(r/reg-sub :all-completed?
  (fn [db _]
    (let [todos (vals (:todos db {}))]
      (and (seq todos)
           (every? :completed todos)))))

;; Components
(defn todo-input []
  (let [value (reagent/atom "")]
    (fn []
      [:header.header
       [:h1 {:style {:font-family "Homemade Apple" :font-size "76px"  :font-weight 700 :margin-bottom "-25px"}} "todos"]
       
       [:input.new-todo
        {:placeholder "What needs to be done?"
         :value @value
         :margin-bottom "10px"
         :on-change #(reset! value (-> % .-target .-value))
         :on-key-down #(when (= (.-which %) 13)
                        (let [text (str/trim @value)]
                          (when (seq text)
                            (r/dispatch! [:add-todo {:id (random-uuid)
                                                    :text text
                                                    :completed false}])
                            (reset! value ""))))}]
       
       ])))

(defn todo-item [{:keys [id text completed]}]
  (let [editing (reagent/atom false)
        edit-value (reagent/atom text)]
    (fn [{:keys [id text completed]}]
      [:li {:class (str (when completed "completed ")
                          (when @editing "editing"))
            :style {:position "relative"}}
       [:div.view
        [:input.toggle
         {:type "checkbox"
          :checked completed
          :on-change #(r/dispatch! [:toggle-todo id])}]
        [:label
         {:on-double-click #(do (reset! editing true)
                               (reset! edit-value text))}
         text]
        [:button.destroy
         {:on-click #(r/dispatch! [:delete-todo id])
          :style {:position "absolute"
                  :right "10px"
                  :top "50%"
                  :transform "translateY(-50%)"
                  :width "40px"
                  :height "40px"
                  :font-size "32px"
                  :font-weight "bold"
                  :color "#ffffff"
                  :background "none"
                  :border "none"
                  :cursor "pointer"
                  :transition "all 0.2s ease"
                  :display "flex"
                  :align-items "center"
                  :justify-content "center"
                  :padding "0"
                  :margin "0"
                  :box-shadow "none"}
          :on-mouse-enter #(set! (.-style.transform ^js (.-currentTarget %)) "translateY(-50%) scale(1.2)")
          :on-mouse-leave #(set! (.-style.transform ^js (.-currentTarget %)) "translateY(-50%) scale(1.0)")}
         "×"]]
       (when @editing
         [:input.edit
          {:value @edit-value
           :on-change #(reset! edit-value (-> % .-target .-value))
           :on-blur #(do (r/dispatch! [:edit-todo id @edit-value])
                        (reset! editing false))
           :on-key-down #(case (.-which %)
                          13 (do (r/dispatch! [:edit-todo id @edit-value])
                                (reset! editing false))
                          27 (reset! editing false)
                          nil)}])])))

(defn todo-list []
  (let [todos (r/subscribe [:filtered-todos])
        all-completed? (r/subscribe [:all-completed?])]
    (fn []
      [:section.main
       [:br]
       [:input#toggle-all.toggle-all
        {:type "checkbox"
          :style {:font-size "23px"}
         :checked @all-completed?
         :on-change #(r/dispatch! [:toggle-all (not @all-completed?)])}]
       [:label {:for "toggle-all" :style {:font-size "23px" :color "#00000066"}} " Mark all as complete"]
       [:ul.todo-list
        (doall
         (for [todo @todos]
           ^{:key (:id todo)}
           [todo-item todo]))]])))

(defn footer []
  (let [stats (r/subscribe [:todo-stats])
        filter-type (r/subscribe [:get [:filter]])]
    (fn []
      (when (pos? (:total @stats))
        [:footer.footer
         [:span.todo-count
          [:strong (:active @stats)]
          (str " " (if (= (:active @stats) 1) "item" "items") " left")]
         [:ul.filters
          [:li [:a {:href "#"
                   :class (when (= @filter-type :all) "selected")
                   :on-click #(r/dispatch! [:set-filter :all])} "All"]]
          [:li [:a {:href "#"
                   :class (when (= @filter-type :active) "selected")
                   :on-click #(r/dispatch! [:set-filter :active])} "Active"]]
          [:li [:a {:href "#"
                   :class (when (= @filter-type :completed) "selected")
                   :on-click #(r/dispatch! [:set-filter :completed])} "Completed"]]]
         (when (pos? (:completed @stats))
           [:button.clear-completed
            {:on-click #(r/dispatch! [:clear-completed])}
            "Clear completed"])]))))

(defn time-travel-controls []
  (let [history-info (r/subscribe [:history-info])
        current-session (r/subscribe [:session-id])
        preview-state (reagent/atom nil)]
    (fn []
      [:div.time-travel-controls
       {:style {:position "fixed"
                :top "10px"
                :right "10px"
                :background "white"
                :border "1px solid #ddd"
                :padding "15px"
                :border-radius "5px"
                :box-shadow "0 2px 4px rgba(0,0,0,0.1)"
                :max-width "350px"}}
       [:h3 {:style {:margin-top 0 :font-family "Homemade Apple" :font-size "30px" :color "#00000077" :font-weight 700}} "time travel"]
       [:div {:style {:margin-bottom "10px"}}
        [:button {:on-click #(r/undo!)
                  :disabled (not (:can-undo @history-info))
                  :style {:margin-right "5px"}}
         "↶ Undo"]
        [:button {:on-click #(r/redo!)
                  :disabled (not (:can-redo @history-info))
                  :style {:margin-right "5px"}}
         "↷ Redo"]]
       
       ;; Visual timeline with clickable ticks
       (when (> (:total-states @history-info 0) 0)
         [:div {:style {:margin "15px 0"}}
          [:label {:style {:display "block" :font-size "12px" :color "#666" :margin-bottom "8px"}}
           (str "State " (- (:total-states @history-info) (:current-index @history-info 0))
                " of " (:total-states @history-info))]
          ;; Clickable timeline ticks
          [:div {:style {:display "flex" 
                        :gap "2px"
                        :height "24px"
                        :align-items "center"}}
           (doall
            (for [i (range 20)]
              (let [;; i goes from 0 (leftmost) to 19 (rightmost)
                    ;; We want: leftmost = oldest state, rightmost = newest state (index 0)
                    total-states (:total-states @history-info)
                    ;; How many ticks to actually show
                    states-to-show (min 20 total-states)
                    ;; Where do valid ticks start (for left-padding)
                    start-offset (- 20 states-to-show)
                    ;; Is this tick position valid?
                    valid? (>= i start-offset)
                    ;; Calculate the history index for this tick
                    ;; The rightmost valid tick should be index 0 (newest)
                    ;; As we go left, index increases (older states)
                    ;; adjusted-pos is 0 for leftmost valid tick, states-to-show-1 for rightmost
                    adjusted-pos (when valid? (- i start-offset))
                    ;; Rightmost tick (highest adjusted-pos) = index 0
                    ;; Leftmost tick (adjusted-pos 0) = index states-to-show-1
                    history-index (when valid? (- (dec states-to-show) adjusted-pos))
                    is-current? (and valid? (= history-index (:current-index @history-info)))
                    ;; States with lower index than current are newer (can redo to them)
                    is-future? (and valid? (< history-index (:current-index @history-info)))]
                ^{:key i}
                [:div {:style {:width "12px"
                              :height (if is-current? "24px" "16px")
                              :background (cond
                                           (not valid?) "#ccc"
                                           is-current? "#2196f3"
                                           is-future? "#e0e0e0"  ;; Future states (can redo) are grey
                                           :else "#90caf9")       ;; Past states (can undo) are light blue
                              :border-radius "2px"
                              :cursor (if valid? "pointer" "not-allowed")
                              :opacity (if valid? 1 0.3)
                              :transition "all 0.2s ease"}
                       :title (when valid? (str "Jump to state " (- total-states history-index)))
                       :on-click (when valid? 
                                   #(do 
                                      (js/console.log "Timeline tick clicked - jumping to index:" history-index)
                                      (r/jump-to-history! history-index)))
                       :on-mouse-enter (when valid? 
                                        #(set! (.-style.height ^js (.-currentTarget %)) "20px"))
                       :on-mouse-leave (when (and valid? (not is-current?))
                                        #(set! (.-style.height ^js (.-currentTarget %)) "16px"))}])))]])
       
       ;; History list with preview
       (when-let [history (:history @history-info)]
         [:div {:style {:max-height "200px" 
                       :overflow-y "auto"
                       :border "1px solid #eee"
                       :border-radius "3px"
                       :padding "5px"
                       :margin-top "10px"}}
          [:div {:style {:font-size "11px" :color "#999" :margin-bottom "5px"}}
           "Click to jump to state:"]
          (doall
           (for [{:keys [index tx-time state]} (take 20 history)]
             ^{:key index}
             [:div {:style {:padding "3px 5px"
                           :cursor "pointer"
                           :font-size "11px"
                           :background (if (= index (:current-index @history-info))
                                        "#e3f2fd"
                                        (if (= @preview-state index)
                                          "#f5f5f5"
                                          "white"))
                           :border-bottom "1px solid #f0f0f0"}
                    :on-mouse-enter #(reset! preview-state index)
                    :on-mouse-leave #(reset! preview-state nil)
                    :on-click #(r/jump-to-history! index)}
              [:div {:style {:font-weight (when (= index (:current-index @history-info)) "bold")}}
               (str "State " (- (:total-states @history-info) index))]
              [:div {:style {:color "#666" :font-size "10px"}}
               (str (count (:todos state {})) " todos, "
                    "filter: " (:filter state :all))]
              (when tx-time
                [:div {:style {:color "#999" :font-size "9px"}}
                 (if (string? tx-time)
                   tx-time
                   (try (.toLocaleTimeString tx-time)
                        (catch js/Error _ (str tx-time))))])]))])])))

(defn session-selector []
  (let [sessions (r/subscribe [:sessions])
        current-session (r/subscribe [:session-id])
        connected? (r/subscribe [:connected?])
        new-session-name (reagent/atom "")]
    (reagent/create-class
     {:component-did-mount
      (fn [] (r/get-sessions!))
      
      :reagent-render
      (fn []
        [:div.session-selector
         {:style {:position "fixed"
                  :top "10px"
                  :left "10px"
                  :background "white"
                  :border "1px solid #ddd"
                  :padding "15px"
                  :border-radius "5px"
                  :box-shadow "0 2px 4px rgba(0,0,0,0.1)"}}
         [:div {:style {:display "flex" :align-items "center" 
                        ;:margin-bottom "10px"
                        :font-size "34px"}}
          [:h3 {:style {:margin 0 :flex 1 :font-family "Homemade Apple" :font-size "30px" :color "#00000077" :font-weight 700}}
           "sessions"]
          [:div {:style {:width "8px" 
                        :height "8px" 
                        :border-radius "50%"
                         
                        :background (if @connected? "#4caf50" "#f44336")
                        :margin-left "10px"
                        :title (if @connected? "Connected" "Disconnected")}}]]
         [:div {:style {;:margin-bottom "10px" 
                        :font-size "24px"}}
          [:select {:value (or @current-session "default")
                    :on-change #(r/switch-session! (-> % .-target .-value))
                    :style {:width "100%" :padding "4px" :font-size "24px"}}
           (doall
            (for [session @sessions]
              ^{:key (:session-id session)}
              [:option {:value (:session-id session) :style {:font-size "24px"}}
               (str (:session-id session) 
                    " (" (:todo-count session 0) " todos)")]))]
          ]
         [:div
          [:input {:type "text"
                   :placeholder "New session name"
                   :value @new-session-name
                   :on-change #(reset! new-session-name (-> % .-target .-value))
                   :style {:width "120px" :margin-right "5px"}}]
          [:button {:on-click #(when (seq @new-session-name)
                                (r/create-session! @new-session-name)
                                (reset! new-session-name ""))}
           "Create"]]])})))

(defn todo-app []
  [:div
   [session-selector]
   [time-travel-controls]
   [:section.todoapp
    [todo-input]
    [todo-list]
    [footer]]
   [:footer.info
    [:p "Double-click to edit a todo"]
    [:p "Created with " [:a {:href "https://github.com/ryrobes/reactor"} "(Rabbit) Reactor"]]]])

(defn ^:export init! []
  (r/init! {:server-url "http://localhost:4000"})
  ;; Get initial history info and sessions once
  (r/get-history-info!)
  (r/get-sessions!)
  (rdom/render [todo-app] (.getElementById js/document "app")))