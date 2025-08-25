(ns examples.todo-app.client-enhanced
  "Enhanced TODO app client that uses SQL subscriptions for automatic diff benefits"
  (:require [reactor.sql-client :as sql]
            [reagent.core :as reagent]
            [reagent.dom :as rdom]
            [clojure.string :as str]
            [cljs.reader :as reader]))

;; ============================================================================
;; State Management with SQL Subscriptions
;; ============================================================================

(defonce app-state (reagent/atom {:todos {} :filter :all}))
(defonce session-id (str "todo-" (random-uuid)))
(defonce subscription-id (atom nil))

;; Configure SQL client
(sql/set-config! {:server-url "http://localhost:4000"
                  :session-id session-id
                  :debug? true})

;; ============================================================================
;; SQL Subscription Setup
;; ============================================================================

(defn setup-subscription!
  "Set up SQL subscription to todo_sessions table"
  []
  ;; First ensure our session exists
  (sql/execute-sql! 
   (str "INSERT INTO todo_sessions (_id, app_state) "
        "VALUES ('" session-id "', '{:todos {} :filter :all}') "
        "ON CONFLICT (_id) DO NOTHING"))
  
  ;; Subscribe to our session's state
  (let [sub-id (sql/subscribe-sql!
                (str "SELECT * FROM todo_sessions WHERE _id = '" session-id "'")
                {:subscription-id "todo-state-sub"
                 :callback (fn [data]
                            (when-let [row (first (:results data))]
                              (when-let [app-state-str (:app_state row)]
                                ;; Parse the EDN string
                                (let [new-state (reader/read-string app-state-str)]
                                  (js/console.log "Received state update (via diff!):" (clj->js new-state))
                                  ;; Update our local atom
                                  (reset! app-state new-state)))))})]
    (reset! subscription-id sub-id)
    (js/console.log "Subscribed to todo state with ID:" sub-id)))

;; ============================================================================
;; Event Handlers - Update DB which triggers subscription updates
;; ============================================================================

(defn update-state!
  "Update state in database, which will trigger subscription update"
  [update-fn & args]
  (let [new-state (apply update-fn @app-state args)
        state-str (pr-str new-state)]
    ;; Optimistically update local state
    (reset! app-state new-state)
    ;; Persist to database (will trigger diff-based update via subscription)
    (sql/execute-sql!
     (str "UPDATE todo_sessions SET app_state = '" state-str "' "
          "WHERE _id = '" session-id "'")
     {:callback (fn [result]
                 (js/console.log "State persisted to DB"))})))

(defn add-todo! [todo]
  (update-state! #(assoc-in % [:todos (:id todo)] todo) ))

(defn toggle-todo! [id]
  (update-state! #(update-in % [:todos id :completed] not)))

(defn delete-todo! [id]
  (update-state! #(update % :todos dissoc id)))

(defn edit-todo! [id text]
  (update-state! #(assoc-in % [:todos id :text] text)))

(defn set-filter! [filter-type]
  (update-state! #(assoc % :filter filter-type)))

(defn toggle-all! [completed?]
  (update-state! 
   #(update % :todos
           (fn [todos]
             (into {} (map (fn [[id todo]]
                            [id (assoc todo :completed completed?)])
                          todos))))))

(defn clear-completed! []
  (update-state!
   #(update % :todos
           (fn [todos]
             (into {} (remove (fn [[_ todo]] (:completed todo)) todos))))))

;; ============================================================================
;; Computed Values
;; ============================================================================

(defn filtered-todos []
  (let [{:keys [todos filter]} @app-state
        todo-list (vals todos)]
    (case filter
      :active (filter (complement :completed) todo-list)
      :completed (filter :completed todo-list)
      todo-list)))

(defn todo-stats []
  (let [todos (vals (:todos @app-state {}))]
    {:total (count todos)
     :active (count (filter (complement :completed) todos))
     :completed (count (filter :completed todos))}))

(defn all-completed? []
  (let [todos (vals (:todos @app-state {}))]
    (and (seq todos)
         (every? :completed todos))))

;; ============================================================================
;; Components
;; ============================================================================

(defn todo-input []
  (let [value (reagent/atom "")]
    (fn []
      [:header.header
       [:h1 {:style {:font-family "Homemade Apple" :font-size "76px"  :font-weight 700 :margin-bottom "-25px"}} 
        "todos"]
       [:div {:style {:font-size "12px" :color "#4CAF50" :margin-bottom "10px"}}
        "✨ Now with automatic diff optimization!"]
       [:input.new-todo
        {:placeholder "What needs to be done?"
         :value @value
         :on-change #(reset! value (-> % .-target .-value))
         :on-key-down #(when (= (.-which %) 13)
                        (let [text (str/trim @value)]
                          (when (seq text)
                            (add-todo! {:id (str (random-uuid))
                                       :text text
                                       :completed false})
                            (reset! value ""))))}]])))

(defn todo-item [{:keys [id text completed]}]
  (let [editing (reagent/atom false)
        edit-value (reagent/atom text)]
    (fn [{:keys [id text completed]}]
      [:li {:class (str (when completed "completed ")
                       (when @editing "editing"))}
       [:div.view
        [:input.toggle
         {:type "checkbox"
          :checked completed
          :on-change #(toggle-todo! id)}]
        [:label
         {:on-double-click #(do (reset! editing true)
                              (reset! edit-value text))}
         text]
        [:button.destroy
         {:on-click #(delete-todo! id)
          :style {:position "absolute" :right "10px" :top "50%" 
                  :transform "translateY(-50%)" :width "40px" :height "40px"
                  :font-size "32px" :color "#ffffff" :background "none"
                  :border "none" :cursor "pointer"}}
         "×"]]
       (when @editing
         [:input.edit
          {:value @edit-value
           :on-change #(reset! edit-value (-> % .-target .-value))
           :on-blur #(do (edit-todo! id @edit-value)
                        (reset! editing false))
           :on-key-down #(case (.-which %)
                          13 (do (edit-todo! id @edit-value)
                                (reset! editing false))
                          27 (reset! editing false)
                          nil)}])])))

(defn todo-list []
  [:section.main
   [:br]
   [:input#toggle-all.toggle-all
    {:type "checkbox"
     :checked (all-completed?)
     :on-change #(toggle-all! (not (all-completed?)))}]
   [:label {:for "toggle-all"} " Mark all as complete"]
   [:ul.todo-list
    (doall
     (for [todo (filtered-todos)]
       ^{:key (:id todo)}
       [todo-item todo]))]])

(defn footer []
  (let [stats (todo-stats)
        filter-type (:filter @app-state)]
    (when (pos? (:total stats))
      [:footer.footer
       [:span.todo-count
        [:strong (:active stats)]
        (str " " (if (= (:active stats) 1) "item" "items") " left")]
       [:ul.filters
        [:li [:a {:href "#"
                 :class (when (= filter-type :all) "selected")
                 :on-click #(set-filter! :all)} "All"]]
        [:li [:a {:href "#"
                 :class (when (= filter-type :active) "selected")
                 :on-click #(set-filter! :active)} "Active"]]
        [:li [:a {:href "#"
                 :class (when (= filter-type :completed) "selected")
                 :on-click #(set-filter! :completed)} "Completed"]]]
       (when (pos? (:completed stats))
         [:button.clear-completed
          {:on-click #(clear-completed!)}
          "Clear completed"])])))

(defn todo-app []
  [:section.todoapp
   [todo-input]
   (when (seq (vals (:todos @app-state)))
     [:<>
      [todo-list]
      [footer]])])

(defn debug-panel []
  [:div {:style {:position "fixed" :bottom "10px" :right "10px" 
                 :background "rgba(0,0,0,0.8)" :color "white" 
                 :padding "10px" :border-radius "5px"
                 :font-family "monospace" :font-size "11px"}}
   [:div "Session: " session-id]
   [:div "Subscription: " @subscription-id]
   [:div "State size: " (count (pr-str @app-state)) " bytes"]
   [:button {:on-click #(sql/enable-debug!)
            :style {:margin-top "5px"}}
    "Enable Debug Logs"]])

;; ============================================================================
;; Initialization
;; ============================================================================

(defn init []
  (js/console.log "Enhanced TODO app starting...")
  (setup-subscription!)
  (rdom/render [:<>
                [todo-app]
                [debug-panel]]
               (.getElementById js/document "app")))

;; Call init when the page loads
(defonce start (init))