(ns examples.todo-app.client-enhanced
  "Enhanced TODO app client that uses SQL subscriptions for automatic diff benefits"
  (:require [reactor.sql-client :as sql]
            [reactor.core :as r]
            [reagent.core :as reagent]
            [reagent.dom :as rdom]
            [clojure.string :as str]
            [cljs.reader :as reader]))

;; ============================================================================
;; State Management with SQL Subscriptions
;; ============================================================================

(defonce app-state (reagent/atom {:todos {} :filter :all}))
(defonce session-id (reagent/atom "default"))
(defonce subscription-id (atom nil))

;; Configure SQL client
(sql/set-config! {:server-url "http://localhost:4000"
                  :session-id @session-id
                  :debug? true})

;; Initialize reactor core for time travel and session management
(r/init! {:server-url "http://localhost:4000"})

;; ============================================================================
;; SQL Subscription Setup
;; ============================================================================

(defn setup-subscription!
  "Set up SQL subscription to todo_sessions table"
  []
  ;; Unsubscribe from previous subscription if exists
  (when @subscription-id
    (sql/unsubscribe! @subscription-id))
  
  ;; Update SQL client config with new session ID
  (sql/set-config! {:server-url "http://localhost:4000"
                    :session-id @session-id
                    :debug? true})
  
  ;; First ensure our session exists
  ;; Use session_id as both _id and session_id for simplicity
  (sql/execute-sql! 
   (str "INSERT INTO todo_sessions (_id, session_id, app_state) "
        "VALUES ('" @session-id "', '" @session-id "', '{:todos {} :filter :all}') "
        "ON CONFLICT (_id) DO NOTHING"))
  
  ;; Immediately fetch current state for the session
  (sql/execute-sql!
   (str "SELECT * FROM todo_sessions WHERE session_id = '" @session-id "'")
   {:callback (fn [data]
                (js/console.log "Loading session data response:" (clj->js data))
                (if-let [row (first (:results data))]
                  (if-let [app-state-str (:app_state row)]
                    (let [new-state (reader/read-string app-state-str)]
                      (js/console.log "Loaded session state:" (clj->js new-state))
                      (reset! app-state new-state))
                    (js/console.warn "No app_state in row:" (clj->js row)))
                  (js/console.warn "No rows returned for session:" @session-id)))})
  
  ;; Subscribe to our session's state
  (let [sub-id (sql/subscribe-sql!
                (str "SELECT * FROM todo_sessions WHERE session_id = '" @session-id "'")
                {:subscription-id "todo-state-sub"
                 :callback (fn [data]
                            (js/console.log "Subscription callback received:" (clj->js data))
                            (if-let [row (first (:results data))]
                              (if-let [app-state-str (:app_state row)]
                                ;; Parse the EDN string
                                (let [new-state (reader/read-string app-state-str)]
                                  (js/console.log "Received state update (via diff!):" (clj->js new-state))
                                  ;; Update our local atom
                                  (reset! app-state new-state))
                                (js/console.warn "No app_state in subscription row:" (clj->js row)))
                              (js/console.warn "No rows in subscription results:" (clj->js data))))})]
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
          "WHERE session_id = '" @session-id "'")
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
    (if (pos? (:total stats))
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
          "Clear completed"])]
      [:div])))  ;; Return empty div instead of nil

(defn time-travel-controls []
  (let [history-info (r/subscribe [:history-info])
        preview-state (reagent/atom nil)]
    ;(fn []
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
              (let [total-states (:total-states @history-info)
                    states-to-show (min 20 total-states)
                    start-offset (- 20 states-to-show)
                    valid? (>= i start-offset)
                    adjusted-pos (when valid? (- i start-offset))
                    history-index (when valid? (- (dec states-to-show) adjusted-pos))
                    is-current? (and valid? (= history-index (:current-index @history-info)))
                    is-future? (and valid? (< history-index (:current-index @history-info)))]
                ^{:key i}
                [:div {:style {:width "12px"
                              :height (if is-current? "24px" "16px")
                              :background (cond
                                           (not valid?) "#ccc"
                                           is-current? "#2196f3"
                                           is-future? "#e0e0e0"
                                           :else "#90caf9")
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
                                        #(set! (.-style.height ^js (.-currentTarget %)) "16px"))}])))]
       
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
                        (catch js/Error _ (str tx-time))))])]))])])])

(defn simple-session-selector []
  (let [sessions (r/subscribe [:sessions])
        connected? (r/subscribe [:connected?])]
    ;; Get sessions on first render
    (r/get-sessions!)
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
                      :font-size "34px"}}
        [:h3 {:style {:margin 0 :flex 1 :font-family "Homemade Apple" :font-size "30px" :color "#00000077" :font-weight 700}}
         "sessions"]
        [:div {:style {:width "8px" 
                      :height "8px" 
                      :border-radius "50%"
                      :background (if @connected? "#4caf50" "#f44336")
                      :margin-left "10px"
                      :title (if @connected? "Connected" "Disconnected")}}]]
       [:div {:style {:font-size "24px"}}
        [:select {:value @session-id
                  :on-change #(let [new-session (-> % .-target .-value)]
                               (reset! session-id new-session)
                               (r/switch-session! new-session)
                               ;; Re-setup subscription with new session
                               (setup-subscription!))
                  :style {:width "100%" :padding "4px" :font-size "24px"}}
         (doall
          (for [session @sessions]
            ^{:key (:session-id session)}
            [:option {:value (:session-id session) :style {:font-size "24px"}}
             (str (:session-id session) 
                  " (" (:todo-count session 0) " todos)")]))]]])))

(defn session-selector []
  (let [sessions (r/subscribe [:sessions])
        current-session (r/subscribe [:session-id])
        connected? (r/subscribe [:connected?])
        new-session-name (reagent/atom "")]
    ;; Get sessions on mount
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
                        :font-size "34px"}}
          [:h3 {:style {:margin 0 :flex 1 :font-family "Homemade Apple" :font-size "30px" :color "#00000077" :font-weight 700}}
           "sessions"]
          [:div {:style {:width "8px" 
                        :height "8px" 
                        :border-radius "50%"
                        :background (if @connected? "#4caf50" "#f44336")
                        :margin-left "10px"
                        :title (if @connected? "Connected" "Disconnected")}}]]
         [:div {:style {:font-size "24px"}}
          [:select {:value (or @current-session "default")
                    :on-change #(do
                                 (reset! session-id (-> % .-target .-value))
                                 (r/switch-session! (-> % .-target .-value))
                                 ;; Re-setup subscription with new session
                                 (setup-subscription!))
                    :style {:width "100%" :padding "4px" :font-size "24px"}}
           (doall
            (for [session @sessions]
              ^{:key (:session-id session)}
              [:option {:value (:session-id session) :style {:font-size "24px"}}
               (str (:session-id session) 
                    " (" (:todo-count session 0) " todos)")]))]]
         [:div
          [:input {:type "text"
                   :placeholder "New session name"
                   :value @new-session-name
                   :on-change #(reset! new-session-name (-> % .-target .-value))
                   :style {:width "120px" :margin-right "5px"}}]
          [:button {:on-click #(when (seq @new-session-name)
                                (r/create-session! @new-session-name)
                                (reset! new-session-name "")
                                ;; Refresh sessions after creating
                                (js/setTimeout r/get-sessions! 500))}
           "Create"]]])}))))

(defn todo-app []
  [:div
   ;; Simple session switcher buttons for testing
   [:div {:style {:position "fixed" :top "10px" :left "10px" :background "white" 
                  :padding "10px" :border "1px solid #ddd" :border-radius "5px"}}
    [:h4 "Switch Session:"]
    [:button {:on-click #(do (reset! session-id "default")
                            (setup-subscription!))
              :style {:margin "2px"}} "Default"]
    [:button {:on-click #(do (reset! session-id "alice_knows")  
                            (setup-subscription!))
              :style {:margin "2px"}} "Alice"]
    [:button {:on-click #(do (reset! session-id "test-session")
                            (setup-subscription!))
              :style {:margin "2px"}} "Test"]
    [:div {:style {:margin-top "5px" :font-size "12px"}}
     "Current: " @session-id]
    [:button {:on-click #(do 
                          (js/console.log "Forcing reload for session:" @session-id)
                          ;; Just resubscribe which should get initial data
                          (setup-subscription!))
              :style {:margin "2px" :background "#4CAF50" :color "white"}}
     "Force Reload"]]
   [time-travel-controls]
   [:section.todoapp
    [todo-input]
    [todo-list]
    [footer]]
   [:footer.info
    [:p "Double-click to edit a todo"]
    [:p "Created with " [:a {:href "https://github.com/ryrobes/reactor"} "(Rabbit) Reactor"]]]])

(defn debug-panel []
  [:div.debug-panel
   [:h3 "🎯 Diff Optimization Active"]
   [:div "Session: " (subs @session-id 0 8) "..."]
   [:div "Subscription: " (if @subscription-id 
                            [:span.diff-indicator "✓ Active"]
                            "Connecting...")]
   [:div "State size: " (count (pr-str @app-state)) " bytes"]
   [:div {:style {:margin-top "10px" :padding-top "10px" 
                  :border-top "1px solid #4CAF50"}}
    [:div {:style {:font-size "10px" :margin-bottom "5px"}}
     "When todos change, only diffs are sent!"]
    [:div {:style {:font-size "10px" :color "#FF9800"}}
     "⚠️ Note: Currently server→client only"]
    [:div {:style {:font-size "10px" :color "#FF9800"}}
     "Bi-directional diffing coming soon!"]]
   [:button {:on-click #(do 
                         (sql/enable-debug!)
                         (js/alert "Check browser console for diff stats!"))}
    "Enable Debug Logs"]])

;; ============================================================================
;; Initialization
;; ============================================================================

(defn init []
  (js/console.log "Enhanced TODO app starting...")
  ;; Get initial history info and sessions
  (r/get-history-info!)
  (r/get-sessions!)
  (setup-subscription!)
  ;; Give subscription time to establish, then re-trigger
  (js/setTimeout #(do
                   (js/console.log "Re-triggering subscription after delay...")
                   (setup-subscription!))
                1000)
  (rdom/render [:div
                [todo-app]
                [debug-panel]]
               (.getElementById js/document "app")))

;; Call init when the page loads
(defonce start (init))