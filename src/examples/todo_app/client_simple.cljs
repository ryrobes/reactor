(ns examples.todo-app.client-simple
  "Simple TODO app client with working enhanced features"
  (:require [reactor.sql-client :as sql]
            [reactor.core :as r]
            [reagent.core :as reagent]
            [reagent.dom :as rdom]
            [clojure.string :as str]
            [cljs.reader :as reader]))

;; State
(defonce app-state (reagent/atom {:todos {} :filter :all}))
(defonce session-id (reagent/atom "default"))
(defonce subscription-id (atom nil))
(defonce current-valid-time (reagent/atom nil))
(defonce history-timestamps (reagent/atom []))

;; Configure SQL client
(sql/set-config! {:server-url "http://localhost:4000"
                  :session-id @session-id
                  :debug? true})

;; Core functions
(declare fetch-history!)

(defn setup-subscription! []
  (js/console.log "Setting up subscription for session:" @session-id)
  (when @subscription-id
    (sql/unsubscribe! @subscription-id)
    (reset! subscription-id nil))
  
  (sql/set-config! {:server-url "http://localhost:4000"
                    :session-id @session-id
                    :debug? true})
  
  (let [row-id (str "todo-" @session-id)
        base-query (str "SELECT * FROM todo_sessions WHERE _id = '" row-id "'")
        sub-id (sql/subscribe-sql!
                base-query
                {:subscription-id (str "todo-" @session-id "-" (random-uuid))
                 :callback (fn [data]
                            (when-let [results (:results data)]
                              (if (empty? results)
                                (reset! app-state {:todos {} :filter :all})
                                (when-let [row (first results)]
                                  (when-let [state-str (or (:app_state row) (:state row))]
                                    (let [new-state (if (= state-str "{}")
                                                     {:todos {} :filter :all}
                                                     (reader/read-string state-str))]
                                      (reset! app-state new-state)))))))})]
    (reset! subscription-id sub-id)))

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
                                    (js/console.log "State persisted"))
                         :error-callback (fn [e]
                                          (js/console.error "Failed to persist:" e))}))})))

(defn fetch-history! []
  (let [row-id (str "todo-" @session-id)
        base-query (str "SELECT * FROM todo_sessions WHERE _id = '" row-id "'")]
    (-> (js/fetch "http://localhost:4000/api/query-history"
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"}
                       :body (js/JSON.stringify 
                              (clj->js {:sql base-query :limit 30}))})
        (.then #(.json %))
        (.then (fn [data]
                 (let [timestamps (-> data (js->clj :keywordize-keys true) :timestamps)]
                   (reset! history-timestamps (vec timestamps)))))
        (.catch (fn [e] (js/console.error "Failed to fetch history:" e))))))

;; Event handlers
(defn add-todo! [todo]
  (update-state! #(assoc-in % [:todos (:id todo)] todo)))

(defn toggle-todo! [id]
  (update-state! #(update-in % [:todos id :completed] not)))

(defn delete-todo! [id]
  (update-state! #(update % :todos dissoc id)))

(defn set-filter! [f]
  (update-state! #(assoc % :filter f)))

;; Components
(defn session-selector []
  [:div {:style {:position "fixed" :top "10px" :left "10px" 
                 :background "white" :padding "10px" 
                 :border "1px solid #ddd" :border-radius "5px"}}
   [:h4 "Session"]
   [:select {:value @session-id
             :on-change #(do (reset! session-id (-> % .-target .-value))
                            (setup-subscription!))
             :style {:width "150px"}}
    [:option {:value "default"} "Default"]
    [:option {:value "alice"} "Alice"]  
    [:option {:value "bob"} "Bob"]
    [:option {:value "test"} "Test"]]])

(defn time-travel-ui []
  [:div {:style {:position "fixed" :top "10px" :right "10px"
                 :background "white" :padding "10px"
                 :border "1px solid #ddd" :border-radius "5px"}}
   [:h4 "Time Travel"]
   [:button {:on-click #(fetch-history!)} "Load History"]
   (when (seq @history-timestamps)
     [:select {:on-change #(let [ts (-> % .-target .-value)]
                             (if (= ts "NOW")
                               (do (reset! current-valid-time nil)
                                   (setup-subscription!))
                               (do (reset! current-valid-time ts)
                                   (setup-subscription!))))}
      [:option {:value "NOW"} "NOW"]
      (for [ts @history-timestamps]
        ^{:key ts}
        [:option {:value ts} (subs (str ts) 11 19)])])])

(defn todo-input []
  [:input {:placeholder "What needs to be done?"
           :on-key-down #(when (= (.-which %) 13)
                          (let [text (-> % .-target .-value str/trim)]
                            (when (seq text)
                              (add-todo! {:id (str (random-uuid))
                                         :text text
                                         :completed false})
                              (set! (.-value (.-target %)) ""))))}])

(defn todo-item [{:keys [id text completed]}]
  [:li
   [:input {:type "checkbox" :checked completed
            :on-change #(toggle-todo! id)}]
   [:span {:style {:text-decoration (when completed "line-through")}}
    text]
   [:button {:on-click #(delete-todo! id)} "×"]])

(defn todo-list []
  [:ul
   (for [[id todo] (:todos @app-state)]
     ^{:key id}
     [todo-item todo])])

(defn todo-app []
  [:div
   [session-selector]
   [time-travel-ui]
   [:div {:style {:margin "80px 20px"}}
    [:h1 "todos"]
    [todo-input]
    [todo-list]]])

(defn init []
  (setup-subscription!)
  (rdom/render [todo-app] (.getElementById js/document "app")))

(defonce start (init))