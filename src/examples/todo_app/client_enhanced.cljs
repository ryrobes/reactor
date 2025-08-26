(ns examples.todo-app.client-enhanced
  "Enhanced TODO app client with fancy UI and SQL subscriptions"
  (:require [reactor.sql-client :as sql]
            [reactor.core :as r]
            [reagent.core :as reagent]
            [reagent.dom :as rdom]
            [clojure.string :as str]
            [cljs.reader :as reader]))

;; ============================================================================
;; State Management
;; ============================================================================

(defonce app-state (reagent/atom {:todos {} :filter :all}))
(defonce session-id (reagent/atom "default"))
(defonce subscription-id (atom nil))
(defonce current-valid-time (reagent/atom nil))
(defonce history-timestamps (reagent/atom []))
(defonce available-sessions (reagent/atom ["default"]))
(defonce new-session-name (reagent/atom ""))
(defonce session-sub-id (atom nil))

;; Configure SQL client
(sql/set-config! {:server-url "http://localhost:4000"
                  :session-id @session-id
                  :debug? true})

;; ============================================================================
;; Core Functions
;; ============================================================================

(declare fetch-history!)
(declare fetch-sessions!)

(defn setup-subscription! []
  (js/console.log "Setting up subscription for session:" @session-id "at time:" @current-valid-time)
  
  ;; Clean up old subscription
  (when @subscription-id
    (js/console.log "Cleaning up old subscription:" @subscription-id)
    ;; TODO: Need proper cleanup for r/sql-subscribe-with-id! subscriptions
    (reset! subscription-id nil))
  
  ;; Configure Reactor core for SQL subscriptions
  (reset! r/config {:server-url "http://localhost:4000"
                    :session-id @session-id})
  
  ;; Subscribe to this session's data using Reactor core
  (let [row-id (str "todo-" @session-id)
        base-query (str "SELECT * FROM todo_sessions WHERE _id = '" row-id "'")
        _ (js/console.log "Subscribing with query:" base-query "as-of:" @current-valid-time)
        ;; Use a unique subscription ID that includes the timestamp
        sub-id-base (str "todo-" @session-id)
        sub-id (if @current-valid-time
                 (str sub-id-base "-" (.getTime (js/Date. @current-valid-time)))
                 (str sub-id-base "-now"))
        ;; Use r/sql-subscribe-with-id! which properly handles as-of parameter
        result-atom (r/sql-subscribe-with-id! sub-id base-query nil @current-valid-time)]
    
    ;; Watch the result atom for changes
    (add-watch result-atom :todo-update
               (fn [_ _ _ new-val]
                 (js/console.log "Subscription update:" (clj->js new-val))
                 (when-not (:loading new-val)
                   (if (:error new-val)
                     (js/console.error "Subscription error:" (:error new-val))
                     (when-let [results (:data new-val)]
                       (if (empty? results)
                         (do
                           (js/console.log "No data for session at this time, resetting to empty")
                           (reset! app-state {:todos {} :filter :all}))
                         (when-let [row (first results)]
                           (when-let [state-str (or (:app_state row) (:state row))]
                             (js/console.log "Got historical state:" state-str)
                             (let [new-state (if (or (= state-str "{}") (empty? state-str))
                                              {:todos {} :filter :all}
                                              (reader/read-string state-str))]
                               (reset! app-state new-state))))))))))
    
    (js/console.log "Created subscription with ID:" sub-id)
    (reset! subscription-id sub-id)
    (fetch-history!)))

(defn update-state! [update-fn & args]
  (let [new-state (apply update-fn @app-state args)
        state-str (pr-str new-state)
        row-id (str "todo-" @session-id)
        escaped-state (clojure.string/replace state-str "'" "''")
        delete-sql (str "DELETE FROM todo_sessions WHERE _id = '" row-id "'")
        insert-sql (str "INSERT INTO todo_sessions RECORDS "
                       "{_id: '" row-id "', "
                       "session_id: '" @session-id "', "
                       "app_state: '" escaped-state "'}")]
    (reset! app-state new-state)
    (sql/execute-sql!
     delete-sql
     {:callback (fn [_]
                 (sql/execute-sql!
                  insert-sql
                  {:callback (fn [_] 
                             (js/console.log "State persisted"))
                   :error-callback (fn [e] 
                                    (js/console.error "Insert failed:" e))}))
      :error-callback (fn [_]
                       (sql/execute-sql!
                        insert-sql
                        {:callback (fn [_]
                                    (js/console.log "State persisted (first insert)"))
                         :error-callback (fn [e]
                                          (js/console.error "Failed to persist:" e))}))})))

(defn fetch-history! []
  (let [row-id (str "todo-" @session-id)
        base-query (str "SELECT * FROM todo_sessions WHERE _id = '" row-id "'")]
    (js/console.log "Fetching history for query:" base-query)
    (-> (js/fetch "http://localhost:4000/api/query-history"
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"}
                       :body (js/JSON.stringify 
                              (clj->js {:sql base-query :limit 30}))})
        (.then #(.json %))
        (.then (fn [data]
                 (let [timestamps (-> data (js->clj :keywordize-keys true) :timestamps)]
                   (js/console.log "Got history timestamps for" @session-id ":" (count timestamps) "timestamps")
                   (js/console.log "Timestamps:" timestamps)
                   (reset! history-timestamps (vec timestamps)))))
        (.catch (fn [e] (js/console.error "Failed to fetch history:" e))))))

(defn fetch-sessions! []
  (sql/execute-sql!
   "SELECT DISTINCT session_id FROM todo_sessions"
   {:callback (fn [result]
               (when-let [results (:results result)]
                 (let [sessions (vec (distinct (map :session_id results)))]
                   (js/console.log "Available sessions:" sessions)
                   (reset! available-sessions 
                           (if (empty? sessions) 
                             ["default"] 
                             sessions)))))
    :error-callback (fn [e]
                     (js/console.error "Failed to fetch sessions:" e)
                     (reset! available-sessions ["default"]))}))

(defn create-session! [name]
  (when (and (not (empty? name))
             (not (contains? (set @available-sessions) name)))
    (reset! session-id name)
    (reset! app-state {:todos {} :filter :all})
    (setup-subscription!)
    (swap! available-sessions conj name)
    (reset! new-session-name "")))

;; ============================================================================
;; Event Handlers
;; ============================================================================

(defn add-todo! [todo]
  (update-state! #(assoc-in % [:todos (:id todo)] todo)))

(defn toggle-todo! [id]
  (update-state! #(update-in % [:todos id :completed] not)))

(defn delete-todo! [id]
  (update-state! #(update % :todos dissoc id)))

(defn set-filter! [f]
  (update-state! #(assoc % :filter f)))

(defn clear-completed! []
  (update-state! #(update % :todos 
                         (fn [todos]
                           (into {} (remove (fn [[_ todo]] (:completed todo)) todos))))))

(defn toggle-all! [completed?]
  (update-state! #(update % :todos
                         (fn [todos]
                           (into {} (map (fn [[id todo]]
                                          [id (assoc todo :completed completed?)])
                                        todos))))))

;; ============================================================================
;; Components
;; ============================================================================

(defn enhanced-session-selector []
  [:div {:style {:position "fixed" :top "10px" :left "10px" 
                 :background "white" :padding "15px" 
                 :border "1px solid #ddd" :border-radius "5px"
                 :box-shadow "0 2px 4px rgba(0,0,0,0.1)"}}
   [:h4 {:style {:margin "0 0 10px 0"}} "Session"]
   [:select {:value @session-id
             :on-change #(do (reset! session-id (-> % .-target .-value))
                            (reset! current-valid-time nil)
                            (setup-subscription!))
             :style {:width "150px" :margin-bottom "10px"}}
    (for [sess @available-sessions]
      ^{:key sess}
      [:option {:value sess} sess])]
   [:div {:style {:display "flex" :gap "5px"}}
    [:input {:type "text"
             :placeholder "New session..."
             :value @new-session-name
             :on-change #(reset! new-session-name (-> % .-target .-value))
             :on-key-down #(when (= (.-which %) 13)
                            (create-session! @new-session-name))
             :style {:width "100px" :padding "2px 5px"}}]
    [:button {:on-click #(create-session! @new-session-name)
              :style {:padding "2px 10px"}} 
     "+"]]])

(defn time-travel-slider []
  [:div {:style {:position "fixed" :top "10px" :right "10px"
                 :background "white" :padding "15px"
                 :border "1px solid #ddd" :border-radius "5px"
                 :box-shadow "0 2px 4px rgba(0,0,0,0.1)"
                 :width "300px"}}
   [:h4 {:style {:margin "0 0 10px 0"}} "Time Travel"]
   [:button {:on-click #(fetch-history!)
             :style {:margin-bottom "10px"}} 
    "Load History"]
   
   (when (seq @history-timestamps)
     [:div
      [:div {:style {:margin-bottom "5px" :font-size "12px"}}
       (if @current-valid-time
         (str "Time: " (subs (str @current-valid-time) 11 19))
         "Time: NOW")]
      [:input {:type "range"
               :min 0
               :max (count @history-timestamps)
               :value (if @current-valid-time
                       (inc (.indexOf @history-timestamps @current-valid-time))
                       0)
               :on-change #(let [idx (js/parseInt (-> % .-target .-value))]
                            (if (= idx 0)
                              (do (reset! current-valid-time nil)
                                  (setup-subscription!))
                              (let [ts (nth @history-timestamps (dec idx))]
                                (reset! current-valid-time ts)
                                (setup-subscription!))))
               :style {:width "100%"}}]
      [:div {:style {:display "flex" :justify-content "space-between" 
                     :font-size "10px" :color "#666"}}
       [:span "NOW"]
       [:span (str (count @history-timestamps) " versions")]]])])

(defn todo-input []
  [:header.header
   [:h1 "todos"]
   [:input.new-todo
    {:placeholder "What needs to be done?"
     :auto-focus true
     :on-key-down
     (fn [e]
       (when (= (.-which e) 13)
         (let [text (str/trim (-> e .-target .-value))]
           (when (seq text)
             (add-todo! {:id (str (random-uuid))
                        :text text
                        :completed false})
             (set! (.-value (.-target e)) "")))))}]])

(defn todo-item [{:keys [id text completed]}]
  [:li {:class (when completed "completed")}
   [:div.view
    [:input.toggle {:type "checkbox"
                    :checked completed
                    :on-change #(toggle-todo! id)}]
    [:label text]
    [:button.destroy {:on-click #(delete-todo! id)}]]])

(defn todo-list []
  (let [filter-val (:filter @app-state)
        todos (:todos @app-state)
        filtered-todos (case filter-val
                        :active (remove #(:completed (second %)) todos)
                        :completed (filter #(:completed (second %)) todos)
                        todos)]
    [:section.main
     [:input#toggle-all.toggle-all
      {:type "checkbox"
       :checked (and (seq todos)
                    (every? :completed (vals todos)))
       :on-change #(toggle-all! (-> % .-target .-checked))}]
     [:label {:for "toggle-all"} "Mark all as complete"]
     [:ul.todo-list
      (for [[id todo] filtered-todos]
        ^{:key id}
        [todo-item todo])]]))

(defn todo-footer []
  (let [todos (:todos @app-state)
        active-count (count (remove :completed (vals todos)))
        completed-count (count (filter :completed (vals todos)))
        filter-val (:filter @app-state)]
    [:footer.footer
     [:span.todo-count
      [:strong active-count]
      (str " " (if (= active-count 1) "item" "items") " left")]
     [:ul.filters
      [:li [:a {:class (when (= filter-val :all) "selected")
                :on-click #(set-filter! :all)} "All"]]
      [:li [:a {:class (when (= filter-val :active) "selected")
                :on-click #(set-filter! :active)} "Active"]]
      [:li [:a {:class (when (= filter-val :completed) "selected")
                :on-click #(set-filter! :completed)} "Completed"]]]
     (when (pos? completed-count)
       [:button.clear-completed
        {:on-click #(clear-completed!)}
        "Clear completed"])]))

(defn todo-app []
  [:div
   [enhanced-session-selector]
   [time-travel-slider]
   [:section.todoapp {:style {:margin-top "80px"}}
    [todo-input]
    (when (seq (:todos @app-state))
      [:<>
       [todo-list]
       [todo-footer]])]])

;; ============================================================================
;; Initialization
;; ============================================================================

(defn init []
  (fetch-sessions!)
  (setup-subscription!)
  (rdom/render [todo-app] (.getElementById js/document "app")))

(defonce start (init))