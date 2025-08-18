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
       [:h1 "todos"]
       [:input.new-todo
        {:placeholder "What needs to be done?"
         :value @value
         :on-change #(reset! value (-> % .-target .-value))
         :on-key-down #(when (= (.-which %) 13)
                        (let [text (str/trim @value)]
                          (when (seq text)
                            (r/dispatch! [:add-todo {:id (random-uuid)
                                                    :text text
                                                    :completed false}])
                            (reset! value ""))))}]])))

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
         {:on-click #(r/dispatch! [:delete-todo id])}
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
       [:input#toggle-all.toggle-all
        {:type "checkbox"
         :checked @all-completed?
         :on-change #(r/dispatch! [:toggle-all (not @all-completed?)])}]
       [:label {:for "toggle-all"} "Mark all as complete"]
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
       [:h3 {:style {:margin-top 0}} "Time Travel"]
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
            (for [i (range (min 20 (:total-states @history-info)))]
              ^{:key i}
              [:div {:style {:width "12px"
                            :height (if (= i (:current-index @history-info)) "24px" "16px")
                            :background (cond
                                         (= i (:current-index @history-info)) "#2196f3"
                                         (< i (:current-index @history-info)) "#90caf9"
                                         :else "#e0e0e0")
                            :border-radius "2px"
                            :cursor "pointer"
                            :transition "all 0.2s ease"}
                     :title (str "Jump to state " (- (:total-states @history-info) i))
                     :on-click #(r/jump-to-history! i)
                     :on-mouse-enter #(set! (.-style.height ^js (.-currentTarget %)) "20px")
                     :on-mouse-leave #(when-not (= i (:current-index @history-info))
                                      (set! (.-style.height ^js (.-currentTarget %)) "16px"))}]))]])
       
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
         [:div {:style {:display "flex" :align-items "center" :margin-bottom "10px"}}
          [:h3 {:style {:margin 0 :flex 1}} "Sessions"]
          [:div {:style {:width "8px" 
                        :height "8px" 
                        :border-radius "50%"
                        :background (if @connected? "#4caf50" "#f44336")
                        :margin-left "10px"
                        :title (if @connected? "Connected" "Disconnected")}}]]
         [:div {:style {:margin-bottom "10px"}}
          [:select {:value (or @current-session "default")
                    :on-change #(r/switch-session! (-> % .-target .-value))
                    :style {:width "100%" :padding "4px"}}
           (doall
            (for [session @sessions]
              ^{:key (:session-id session)}
              [:option {:value (:session-id session)}
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
    [:p "Created with " [:a {:href "https://github.com/ryrobes/reactor"} "Reactor"]]]])

(defn ^:export init! []
  (r/init! {:server-url "http://localhost:4000"})
  ;; Periodically update history info and sessions
  (js/setInterval r/get-history-info! 2000)
  (js/setInterval r/get-sessions! 3000)
  ;; Use React 17 render for now as Reagent 1.2.0 doesn't fully support React 18
  (rdom/render [todo-app] (.getElementById js/document "app")))