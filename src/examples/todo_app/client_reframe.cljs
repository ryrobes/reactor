(ns examples.todo-app.client-reframe
  "TODO app using enhanced re-frame-like Reactor API with SQL as first-class citizen"
  (:require 
   [reactor.core :as r]
   [reagent.core :as reagent]
   [reagent.dom :as rdom]
   [clojure.string :as str]))

;; ============================================================================
;; SQL Subscription Registration
;; ============================================================================

;; Register the main todo state as a SQL-backed key-value store
(r/reg-sql-store :todo-state
  {:table "todo_sessions"
   :key-field "_id"
   :value-field "app_state"
   :default {:todos {} :filter :all}})

;; Register sessions list subscription
(r/reg-sql-sub :todo-sessions
  (fn [_]
    {:sql "SELECT DISTINCT _id, app_state FROM todo_sessions ORDER BY _id"
     :transform (fn [rows]
                 (map (fn [row]
                       (let [session-id (str/replace (:_id row) "todo-" "")
                             app-state (:app_state row)]
                         {:session-id session-id
                          :todo-count (count (:todos app-state))}))
                     rows))}))

;; Register history subscription
(r/reg-sql-sub :todo-history
  (fn [[_ session-id]]
    {:sql "SELECT _id, app_state, _version FROM todo_sessions 
           WHERE _id = ? 
           FOR SYSTEM_TIME ALL
           ORDER BY _version"
     :params [(str "todo-" session-id)]
     :transform (fn [rows]
                 {:history (vec rows)
                  :current-index (dec (count rows))
                  :total (count rows)})}))

;; ============================================================================
;; SQL Event Registration
;; ============================================================================

;; Register event for updating todo state
(r/reg-event-sql :update-todos
  (fn [[session-id new-state]]
    {:sql "INSERT INTO todo_sessions (_id, app_state) 
           VALUES (?, ?) 
           ON CONFLICT (_id) 
           DO UPDATE SET app_state = EXCLUDED.app_state"
     :params [(str "todo-" session-id) (pr-str new-state)]}))

;; Register event for creating a new session
(r/reg-event-sql :create-todo-session
  (fn [[session-id initial-state]]
    {:sql "INSERT INTO todo_sessions (_id, app_state) 
           VALUES (?, ?) 
           ON CONFLICT (_id) DO NOTHING"
     :params [(str "todo-" session-id) (pr-str initial-state)]}))

;; ============================================================================
;; Business Logic Helpers
;; ============================================================================

(defn add-todo [todos text]
  (let [id (str (random-uuid))
        new-todo {:id id :text text :completed false}]
    (assoc todos id new-todo)))

(defn toggle-todo [todos id]
  (update-in todos [id :completed] not))

(defn delete-todo [todos id]
  (dissoc todos id))

(defn clear-completed [todos]
  (into {} (remove (fn [[_ todo]] (:completed todo)) todos)))

(defn toggle-all [todos]
  (let [all-completed? (every? :completed (vals todos))]
    (into {} (map (fn [[id todo]] 
                   [id (assoc todo :completed (not all-completed?))]) 
                 todos))))

(defn filter-todos [todos filter-type]
  (case filter-type
    :active (remove :completed (vals todos))
    :completed (filter :completed (vals todos))
    :all (vals todos)))

;; ============================================================================
;; Components
;; ============================================================================

(defn todo-input [session-id]
  (let [value (reagent/atom "")]
    (fn []
      [:header.header
       [:h1 "todos"]
       [:input.new-todo
        {:placeholder "What needs to be done?"
         :value @value
         :on-change #(reset! value (.. % -target -value))
         :on-key-press (fn [e]
                        (when (and (= 13 (.-charCode e))
                                  (not (str/blank? @value)))
                          (let [state @(r/subscribe [:todo-state session-id])
                                new-todos (add-todo (:todos state) @value)
                                new-state (assoc state :todos new-todos)]
                            (r/dispatch-sql! [:update-todos session-id new-state])
                            (reset! value ""))))}]])))

(defn todo-item [session-id todo]
  (let [editing (reagent/atom false)
        edit-value (reagent/atom (:text todo))]
    (fn [session-id todo]
      [:li {:class (str (when (:completed todo) "completed ")
                       (when @editing "editing"))}
       [:div.view
        [:input.toggle
         {:type "checkbox"
          :checked (:completed todo)
          :on-change #(let [state @(r/subscribe [:todo-state session-id])
                           new-todos (toggle-todo (:todos state) (:id todo))
                           new-state (assoc state :todos new-todos)]
                       (r/dispatch-sql! [:update-todos session-id new-state]))}]
        [:label
         {:on-double-click #(do (reset! editing true)
                               (reset! edit-value (:text todo)))}
         (:text todo)]
        [:button.destroy
         {:on-click #(let [state @(r/subscribe [:todo-state session-id])
                          new-todos (delete-todo (:todos state) (:id todo))
                          new-state (assoc state :todos new-todos)]
                      (r/dispatch-sql! [:update-todos session-id new-state]))}]]
       (when @editing
         [:input.edit
          {:value @edit-value
           :auto-focus true
           :on-change #(reset! edit-value (.. % -target -value))
           :on-blur #(do (reset! editing false)
                        (when-not (str/blank? @edit-value)
                          (let [state @(r/subscribe [:todo-state session-id])
                                new-todos (assoc-in (:todos state) [(:id todo) :text] @edit-value)
                                new-state (assoc state :todos new-todos)]
                            (r/dispatch-sql! [:update-todos session-id new-state]))))
           :on-key-press (fn [e]
                          (when (= 13 (.-charCode e))
                            (.blur (.-target e))))}])])))

(defn todo-list [session-id]
  (let [state (r/subscribe [:todo-state session-id])]
    (fn []
      (let [{:keys [todos filter]} @state
            visible-todos (filter-todos todos filter)
            all-completed? (and (seq todos) 
                              (every? :completed (vals todos)))]
        [:section.main
         [:input#toggle-all.toggle-all
          {:type "checkbox"
           :checked all-completed?
           :on-change #(let [new-todos (toggle-all todos)
                            new-state (assoc @state :todos new-todos)]
                        (r/dispatch-sql! [:update-todos session-id new-state]))}]
         [:label {:for "toggle-all"} "Mark all as complete"]
         [:ul.todo-list
          (for [todo visible-todos]
            ^{:key (:id todo)} [todo-item session-id todo])]]))))

(defn todo-footer [session-id]
  (let [state (r/subscribe [:todo-state session-id])]
    (fn []
      (let [{:keys [todos filter]} @state
            active-count (count (filter #(not (:completed %)) (vals todos)))
            completed-count (count (filter :completed (vals todos)))]
        [:footer.footer
         [:span.todo-count
          [:strong active-count] " " (if (= 1 active-count) "item" "items") " left"]
         [:ul.filters
          [:li [:a {:class (when (= filter :all) "selected")
                   :on-click #(r/dispatch-sql! [:update-todos session-id (assoc @state :filter :all)])}
                "All"]]
          [:li [:a {:class (when (= filter :active) "selected")
                   :on-click #(r/dispatch-sql! [:update-todos session-id (assoc @state :filter :active)])}
                "Active"]]
          [:li [:a {:class (when (= filter :completed) "selected")
                   :on-click #(r/dispatch-sql! [:update-todos session-id (assoc @state :filter :completed)])}
                "Completed"]]]
         (when (pos? completed-count)
           [:button.clear-completed
            {:on-click #(let [new-todos (clear-completed todos)
                             new-state (assoc @state :todos new-todos)]
                         (r/dispatch-sql! [:update-todos session-id new-state]))}
            "Clear completed"])]))))

(defn session-selector []
  (let [session-id (r/subscribe [:session-id])
        sessions (r/subscribe [:todo-sessions])
        new-session-name (reagent/atom "")]
    (fn []
      [:div.session-selector
       [:div.current-session
        [:span "Session: "]
        [:select
         {:value @session-id
          :on-change #(r/switch-session! (.. % -target -value))}
         (for [{:keys [session-id todo-count]} @sessions]
           ^{:key session-id}
           [:option {:value session-id} 
            (str session-id " (" todo-count " todos)")])]]
       [:div.new-session
        [:input
         {:placeholder "New session name"
          :value @new-session-name
          :on-key-press (fn [e]
                         (when (and (= 13 (.-charCode e))
                                   (not (str/blank? @new-session-name)))
                           (r/dispatch-sql! [:create-todo-session @new-session-name 
                                           {:todos {} :filter :all}])
                           (r/switch-session! @new-session-name)
                           (reset! new-session-name "")))}]
        [:button
         {:on-click #(when (not (str/blank? @new-session-name))
                      (r/dispatch-sql! [:create-todo-session @new-session-name 
                                       {:todos {} :filter :all}])
                      (r/switch-session! @new-session-name)
                      (reset! new-session-name ""))}
         "Create Session"]]])))

(defn time-travel-controls []
  (let [session-id (r/subscribe [:session-id])
        history (r/subscribe [:todo-history @session-id])]
    (fn []
      (when @history
        (let [{:keys [current-index total]} @history]
          [:div.time-travel-controls
           [:h3 "Time Travel"]
           [:div.slider-container
            [:input
             {:type "range"
              :min 0
              :max (dec total)
              :value current-index
              :on-change #(let [index (js/parseInt (.. % -target -value))]
                           (r/jump-to-history! index))}]
            [:div.slider-info
             [:span (str "Version " (inc current-index) " of " total)]]]
           [:div.time-travel-buttons
            [:button 
             {:disabled (= current-index 0)
              :on-click #(r/undo!)}
             "← Undo"]
            [:button
             {:disabled (= current-index (dec total))
              :on-click #(r/redo!)}
             "Redo →"]]])))))

(defn connection-status []
  (let [connected? (r/subscribe [:connected?])]
    (fn []
      [:div.connection-status
       {:class (if @connected? "connected" "disconnected")}
       [:span (if @connected? "● Connected" "○ Disconnected")]])))

(defn todo-app []
  (let [session-id (r/subscribe [:session-id])
        state (r/subscribe [:todo-state @session-id])]
    (fn []
      ;; Show loading state while subscription is loading
      (if (:loading @state)
        [:div.loading "Loading todos..."]
        [:div.todo-app-container
         [connection-status]
         [session-selector]
         [time-travel-controls]
         [:section.todoapp
          [todo-input @session-id]
          (when (seq (:todos @state))
            [:<>
             [todo-list @session-id]
             [todo-footer @session-id]])]]))))

;; ============================================================================
;; Initialization
;; ============================================================================

(defn mount-root []
  (rdom/render [todo-app] (.getElementById js/document "app")))

(defn init []
  (println "Initializing TODO app with re-frame-like API...")
  ;; Initialize reactor
  (r/init! {:session-id "default"})
  ;; Create default session if needed
  (r/dispatch-sql! [:create-todo-session "default" {:todos {} :filter :all}])
  ;; Mount the app
  (mount-root))

;; Re-render on code reload
(defn ^:dev/after-load reload []
  (mount-root))