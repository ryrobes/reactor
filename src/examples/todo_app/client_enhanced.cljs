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

(defn subscribe-sql-with-temporal!
  "Subscribe to SQL with optional as-of parameter for time travel"
  [sql as-of subscription-id callback]
  (let [{:keys [server-url session-id]} @sql/config
        connection-id (str (random-uuid))
        sse-url (str server-url "/api/subscribe-sql?session=" session-id "&connection=" connection-id)
        event-source (js/EventSource. sse-url)]
    
    ;; Set up SSE handlers
    (set! (.-onopen event-source)
          (fn [_]
            (js/console.log "[TODO-TEMPORAL]" subscription-id "SSE connected")))
    
    (set! (.-onmessage event-source)
          (fn [e]
            ;; Process the message directly here since handle-sse-message is private
            (try
              (let [data (js/JSON.parse (.-data e))
                    data-clj (js->clj data :keywordize-keys true)]
                (when (#{:query-update :full-update :diff-update :field-diff-update} (:type data-clj))
                  (swap! sql/subscription-results assoc subscription-id (:results data-clj))
                  (when callback
                    (callback {:results (:results data-clj)
                              :subscription-id subscription-id}))))
              (catch js/Error e
                (js/console.error "[TODO-TEMPORAL] Error processing message:" e)))))
    
    (set! (.-onerror event-source)
          (fn [e]
            (js/console.error "[TODO-TEMPORAL]" subscription-id "SSE error:" e)))
    
    ;; Store subscription info
    (swap! sql/active-subscriptions assoc subscription-id
           {:sql sql
            :as-of as-of
            :callback callback
            :event-source event-source})
    
    ;; Store SSE connection
    (swap! sql/sse-connections assoc subscription-id event-source)
    
    ;; Send subscription request with as-of parameter
    (-> (js/fetch (str server-url "/api/sql")
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"
                                    "X-Session-ID" session-id}
                       :body (js/JSON.stringify 
                              (clj->js {:sql sql
                                       :as-of as-of  ; Include as-of for server-side temporal handling
                                       :subscription-id subscription-id}))})
        (.then (fn [response]
                 (if (.-ok response)
                   (.json response)
                   (throw (js/Error. (str "HTTP " (.-status response)))))))
        (.then (fn [data]
                 ;; Initial results
                 (let [result-clj (js->clj data :keywordize-keys true)]
                   (when (:results result-clj)
                     (swap! sql/subscription-results assoc subscription-id (:results result-clj))
                     (when callback
                       (callback result-clj))))))
        (.catch (fn [error]
                  (js/console.error "[TODO-TEMPORAL]" subscription-id "subscription failed:" error))))
    
    ;; Return subscription ID
    subscription-id))

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
    
    ;; CRITICAL: Update SQL client config immediately
    ;; This ensures the SSE connection URL has the correct session-id
    (js/console.log "Updating SQL config with session:" current-session)
    (sql/set-config! {:server-url "http://localhost:4000"
                      :session-id current-session
                      :debug? true})
    
    ;; The subscription below will provide the data for the session
    ;; No need to INSERT - sessions should already exist
    
    ;; Fetch history for the new session (unless we're time traveling)
    (when-not current-time
      (js/setTimeout #(fetch-history!) 500))  ; Small delay to let subscription establish
    
    ;; Subscribe to our session's state with optional time travel
    ;; IMPORTANT: Query by _id to avoid conflicts with session system's rows
    (let [row-id (str "todo-" current-session)
          ;; Base query without temporal clause - server will add it if as-of is provided
          base-query (str "SELECT * FROM todo_sessions WHERE _id = '" row-id "'")
          _ (js/console.log "Subscribing with query:" base-query 
                           "for session:" current-session
                           (when current-time (str " at time: " current-time)))
          ;; Use a stable subscription ID based on session and time
          ;; This allows proper tracking but still unique per session/time combo
          unique-sub-id (str "todo-" current-session "-" (or current-time "now"))
          _ (js/console.log "Creating subscription with ID:" unique-sub-id)
          ;; Create subscription with as-of parameter
          ;; We need to send as-of in the request body for the server to handle
          sub-id (subscribe-sql-with-temporal!
                  base-query
                  current-time
                  unique-sub-id
                  (fn [data]
                               (js/console.log "Subscription callback received for session" current-session ":" (clj->js data))
                               (if-let [results (:results data)]
                                 (if (empty? results)
                                   ;; No data for this session yet - initialize it
                                   (do 
                                     (js/console.log "No data for session" current-session "- initializing with empty state")
                                     (reset! app-state {:todos {} :filter :all})
                                     ;; Insert initial empty state for this session
                                     (let [row-id (str "todo-" current-session)
                                           init-state (pr-str {:todos {} :filter :all})
                                           init-sql (str "INSERT INTO todo_sessions RECORDS "
                                                        "{_id: '" row-id "', "
                                                        "session_id: '" current-session "', "
                                                        "app_state: '" init-state "'}")]
                                       (js/console.log "Creating initial state with SQL:" init-sql)
                                       (sql/execute-sql!
                                        init-sql
                                        {:callback (fn [_] 
                                                    (js/console.log "Initial state created for session:" current-session))
                                         :error-callback (fn [error]
                                                          (js/console.warn "Could not create initial state:" error))})))
                                   ;; We have results
                                   (if-let [row (first results)]
                                     ;; Try both app_state and state fields
                                     (if-let [state-str (or (:app_state row) (:state row))]
                                       ;; Parse the EDN string
                                       (let [new-state (if (or (= state-str "{}") (= state-str ""))
                                                        {:todos {} :filter :all}  ; Default empty state
                                                        (reader/read-string state-str))]
                                         (js/console.log "Received state update:" (clj->js new-state))
                                         ;; Update our local atom
                                         (reset! app-state new-state))
                                       (js/console.warn "No app_state or state field in row:" (clj->js row)))
                                     (js/console.warn "Unexpected empty first row")))
                                 (js/console.warn "No results field in data:" (clj->js data)))))]
      (reset! subscription-id sub-id)
      (js/console.log "Subscribed to todo state with ID:" sub-id " for session:" current-session))))

;; ============================================================================
;; Time Travel Functions
;; ============================================================================

(defn fetch-history! []
  "Fetch all historical timestamps for this session using the query-history API"
  ;; Use the proper API endpoint that handles temporal queries server-side
  (let [row-id (str "todo-" @session-id)
        ;; Base query - server will determine the tables and get their history
        base-query (str "SELECT * FROM todo_sessions WHERE _id = '" row-id "'")]
    (js/console.log "Fetching history for query:" base-query)
    ;; Use the query-history endpoint which handles XTDB temporal queries properly
    (-> (js/fetch (str "http://localhost:4000/api/query-history")
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"}
                       :body (js/JSON.stringify 
                              (clj->js {:sql base-query
                                       :limit 40}))})
        (.then #(.json %))
        (.then (fn [data]
                 (js/console.log "History data received:" (clj->js data))
                 (let [data-clj (js->clj data :keywordize-keys true)
                       ;; The API returns {:timestamps [...], :tables [...]}
                       timestamps (:timestamps data-clj)]
                   (js/console.log "Parsed timestamps:" (clj->js timestamps))
                   (reset! history-timestamps (vec timestamps)))))
        (.catch (fn [error]
                  (js/console.error "Failed to fetch history:" error))))))

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
        row-id (str "todo-" @session-id)
        ;; Escape single quotes in the state string for SQL
        escaped-state (clojure.string/replace state-str "'" "''")
        ;; XTDB uses INSERT ... RECORDS syntax
        ;; Since XTDB doesn't have ON CONFLICT, we'll delete then insert for updates
        delete-sql (str "DELETE FROM todo_sessions WHERE _id = '" row-id "'")
        insert-sql (str "INSERT INTO todo_sessions RECORDS "
                       "{_id: '" row-id "', "
                       "session_id: '" @session-id "', "
                       "app_state: '" escaped-state "'}")]
    ;; Log the SQL for debugging
    (js/console.log "Updating state with DELETE + INSERT:")
    (js/console.log "  Delete SQL:" delete-sql)
    (js/console.log "  Insert SQL:" insert-sql)
    ;; Optimistically update local state
    (reset! app-state new-state)
    ;; First delete, then insert (upsert pattern for XTDB)
    (sql/execute-sql!
     delete-sql
     {:callback (fn [_]
                 ;; After delete, do the insert
                 (sql/execute-sql!
                  insert-sql
                  {:callback (fn [result]
                             (js/console.log "State persisted to DB for session:" @session-id)
                             ;; Fetch updated history after each change
                             (fetch-history!))
                   :error-callback (fn [error]
                                    (js/console.error "Failed to insert state:" error))}))  
      :error-callback (fn [error]
                       ;; If delete fails, try insert anyway (might be first time)
                       (js/console.warn "Delete failed (might be first insert), trying insert:" error)
                       (sql/execute-sql!
                        insert-sql
                        {:callback (fn [result]
                                    (js/console.log "State persisted to DB for session:" @session-id)
                                    (fetch-history!))
                         :error-callback (fn [error]
                                          (js/console.error "Failed to persist state:" error))}))})))

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
            ^{:key (str "tts" timestamp)}
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