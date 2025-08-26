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
(defonce current-valid-time (reagent/atom nil))  ;; nil means "current time"
(defonce history-timestamps (reagent/atom []))    ;; Track all valid_time values

;; Configure SQL client
(sql/set-config! {:server-url "http://localhost:4000"
                  :session-id @session-id
                  :debug? true})

;; Initialize reactor core for time travel and session management
(r/init! {:server-url "http://localhost:4000"})

;; ============================================================================
;; SQL Subscription Setup
;; ============================================================================

(declare fetch-history!)

(defn setup-subscription!
  "Set up SQL subscription to todo_sessions table"
  []
  (js/console.log "=============================================")
  (js/console.log "setup-subscription! called for session:" @session-id)
  
  ;; Capture the current session-id value to avoid stale closures
  (let [current-session @session-id
        current-time @current-valid-time]
    
    ;; Unsubscribe from previous subscription if exists
    (when @subscription-id
      (js/console.log "Unsubscribing from previous subscription:" @subscription-id)
      (sql/unsubscribe! @subscription-id)
      (reset! subscription-id nil))
    
    ;; Clear current state and history when switching sessions
    (reset! app-state {:todos {} :filter :all})
    (reset! history-timestamps [])
    (when-not current-time
      (reset! current-valid-time nil))  ; Only reset if not time traveling
    
    ;; Add a small delay to ensure clean EventSource closure
    (js/setTimeout 
     (fn []
       ;; CRITICAL: Update SQL client config BEFORE creating subscription
       ;; This ensures the SSE connection URL has the correct session-id
       (js/console.log "Updating SQL config with session:" current-session "(captured from:" @session-id ")")
       (sql/set-config! {:server-url "http://localhost:4000"
                         :session-id current-session
                         :debug? true})
       (js/console.log "Config updated, about to create subscription...")
     
     ;; The subscription below will provide the data for the session
     ;; No need to INSERT - sessions should already exist
     
     ;; Fetch history for the new session (unless we're time traveling)
     (when-not current-time
       (js/setTimeout #(fetch-history!) 500))  ; Small delay to let subscription establish
     
     ;; Subscribe to our session's state with optional time travel
     ;; Use a unique subscription ID each time to force new SSE connection
     (let [query (if current-time
                    (str "SELECT * FROM todo_sessions "
                         "AS OF TIMESTAMP '" current-time "' "
                         "WHERE session_id = '" current-session "'")
                    (str "SELECT * FROM todo_sessions WHERE session_id = '" current-session "'"))
           _ (js/console.log "Subscribing with query:" query "for session:" current-session)
           ;; Generate unique ID to force new SSE connection
           ;; Include session-id in the subscription ID for clarity
           unique-sub-id (str "todo-sub-" current-session "-" (random-uuid))
           _ (js/console.log "Creating subscription with ID:" unique-sub-id)
           sub-id (sql/subscribe-sql!
                   query
                   {:subscription-id unique-sub-id
                    :callback (fn [data]
                               (js/console.log "Subscription callback received for session" current-session ":" (clj->js data))
                               (if-let [results (:results data)]
                                 (if (empty? results)
                                   ;; No data for this session yet - initialize it
                                   (do 
                                     (js/console.log "No data for session" current-session "- using empty state")
                                     (reset! app-state {:todos {} :filter :all}))
                                   ;; We have results
                                   (if-let [row (first results)]
                                     ;; Try both app_state and state fields
                                     (if-let [state-str (or (:app_state row) (:state row))]
                                       ;; Parse the EDN string
                                       (let [new-state (if (= state-str "{}")
                                                        {:todos {} :filter :all}  ; Default empty state
                                                        (reader/read-string state-str))]
                                         (js/console.log "Received state update:" (clj->js new-state))
                                         ;; Update our local atom
                                         (reset! app-state new-state))
                                       (js/console.warn "No app_state or state field in row:" (clj->js row)))
                                     (js/console.warn "Unexpected empty first row")))
                                 (js/console.warn "No results field in data:" (clj->js data))))})]
       (reset! subscription-id sub-id)
       (js/console.log "Subscribed to todo state with ID:" sub-id " for session:" current-session)))
   50)))

;; ============================================================================
;; Time Travel Functions
;; ============================================================================

(defn fetch-history! []
  "Fetch all historical timestamps for this session using temporal query"
  ;; We need to use subscribe-sql to get query results
  (let [query (str "SELECT DISTINCT _valid_from FROM todo_sessions "
                   "FOR VALID_TIME ALL "
                   "WHERE session_id = '" @session-id "' "
                   "ORDER BY _valid_from DESC "
                   "LIMIT 40")
        sub-id (str "history-query-" (random-uuid))]
    (js/console.log "Fetching history with query:" query)
    ;; Use a one-time subscription to get the results
    (sql/subscribe-sql!
     query
     {:subscription-id sub-id
      :callback (fn [data]
                  (js/console.log "History timestamps received:" (clj->js data))
                  (when-let [results (:results data)]
                    (let [timestamps (map :_valid_from results)]
                      (reset! history-timestamps timestamps)
                      ;; Immediately unsubscribe since this is a one-time query
                      (sql/unsubscribe! sub-id))))})))

(defn time-travel-to! [timestamp]
  "Travel to a specific point in time"
  ;; Clean up timestamp format - remove [UTC] or any timezone suffix
  (let [clean-timestamp (-> timestamp
                           str
                           (clojure.string/replace #"\[.*?\]" "") ; Remove [UTC] or similar
                           clojure.string/trim)]
    (js/console.log "Time traveling to:" clean-timestamp "(from:" timestamp ")")
    (reset! current-valid-time clean-timestamp)
    (setup-subscription!)))

(defn time-travel-back! []
  "Go to present time"
  (reset! current-valid-time nil)
  (setup-subscription!))

;; ============================================================================
;; Event Handlers - Update DB which triggers subscription updates
;; ============================================================================

(defn update-state!
  "Update state in database, which will trigger subscription update"
  [update-fn & args]
  (let [new-state (apply update-fn @app-state args)
        state-str (pr-str new-state)
        ;; Create a unique ID for each session's data
        row-id (str "todo-" @session-id)]
    ;; Optimistically update local state
    (reset! app-state new-state)
    ;; Use PUT for XTDB which acts as an upsert (insert or update)
    ;; The _id must be unique per row, session_id is just a field
    (sql/execute-sql!
     (str "PUT todo_sessions {_id: '" row-id "', "
          "session_id: '" @session-id "', "
          "app_state: '" state-str "'}")
     {:callback (fn [result]
                 (js/console.log "State persisted to DB for session:" @session-id " with id:" row-id)
                 ;; Fetch updated history after each change
                 (fetch-history!))
      :error-callback (fn [error]
                       (js/console.error "Failed to persist state:" error))})))

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

(defn simple-time-travel-controls []
  ;; Fetch history on mount
  (reagent/create-class
   {:component-did-mount
    (fn [] (fetch-history!))
    
    :reagent-render
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
              :max-width "300px"}}
     [:h4 {:style {:margin-top 0}} "Time Travel"]
     
     ;; Current time status
     [:div {:style {:margin-bottom "10px" :padding "5px" 
                    :background (if @current-valid-time "#fff3cd" "#d4edda")
                    :border-radius "3px"}}
      (if @current-valid-time
        [:div
         [:div {:style {:font-size "12px" :font-weight "bold"}} "Viewing Past State"]
         [:div {:style {:font-size "11px"}} (str "Time: " @current-valid-time)]
         [:button {:on-click #(time-travel-back!)
                   :style {:margin-top "5px" :background "#28a745" :color "white"
                          :border "none" :padding "3px 8px" :border-radius "3px"}}
          "← Back to Present"]]
        [:div {:style {:font-size "12px"}} "Viewing Current State"])]
     
     ;; History timestamps
     (when (seq @history-timestamps)
       [:div
        [:div {:style {:font-size "12px" :margin-top "10px" :font-weight "bold"}}
         "History Points:"]
        [:div {:style {:max-height "150px" :overflow-y "auto" :border "1px solid #ddd" 
                       :border-radius "3px" :padding "5px" :margin-top "5px"}}
         (doall
          (for [timestamp (reverse @history-timestamps)]
            ^{:key timestamp}
            [:div {:style {:font-size "11px" :padding "2px 5px" :cursor "pointer"
                          :background (if (= timestamp @current-valid-time) "#e3f2fd" "white")
                          :border-bottom "1px solid #f0f0f0"}
                   :on-click #(time-travel-to! timestamp)}
             timestamp]))]])])}))  ;; Close reagent/create-class

(defn time-travel-controls-old []
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
     "Current: " @session-id]]
   [simple-time-travel-controls]
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
  ;; Set up the initial subscription
  (setup-subscription!)
  ;; Render the UI
  (rdom/render [:div
                [todo-app]
                [debug-panel]]
               (.getElementById js/document "app")))

;; Call init when the page loads
(defonce start (init))