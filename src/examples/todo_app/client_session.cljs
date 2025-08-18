(ns examples.todo-app.client-session
  "ClojureScript client for session-aware TODO app.
   Provides re-frame-like API but backed by server state."
  (:require [reagent.core :as r]
            [reagent.dom :as rdom]
            [re-com.core :as rc]
            [cljs.core.async :as async :refer [<!]]
            [cljs-http.client :as http]
            [clojure.string :as str])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))

;; Configuration
;; =============

(def server-url "http://localhost:9000")
(defonce session-id (r/atom nil))
(defonce app-state (r/atom {:todos {}
                            :filter :all
                            :loading true
                            :connected false}))

;; Server Communication
;; ====================

(defn api-call
  "Make API call to server"
  [endpoint & {:keys [method body params]
              :or {method :get}}]
  (go
    (let [url (str server-url endpoint)
          request (cond-> {:method method
                          :headers {"Content-Type" "application/edn"}}
                   params (assoc :query-params (assoc params :session-id @session-id))
                   body (assoc :body (pr-str body)))
          response (<! (http/request url request))]
      (if (= 200 (:status response))
        (cljs.reader/read-string (:body response))
        (do
          (js/console.error "API call failed:" response)
          nil)))))

;; Session Management
;; ==================

(defn init-session!
  "Initialize or restore a session"
  []
  (go
    (let [stored-session (js/localStorage.getItem "reactor-session-id")
          session (if stored-session
                   ;; Try to restore existing session
                   (let [state (<! (api-call "/api/state" 
                                           :params {:session-id stored-session}))]
                     (if state
                       (do
                         (reset! session-id stored-session)
                         (reset! app-state (merge @app-state state))
                         stored-session)
                       ;; Session expired, create new one
                       (let [response (<! (api-call "/api/init" :method :post))]
                         (reset! session-id (:session-id response))
                         (reset! app-state (merge @app-state (:state response)))
                         (:session-id response))))
                   ;; Create new session
                   (let [response (<! (api-call "/api/init" :method :post))]
                     (reset! session-id (:session-id response))
                     (reset! app-state (merge @app-state (:state response)))
                     (:session-id response)))]
      (js/localStorage.setItem "reactor-session-id" session)
      (swap! app-state assoc :loading false)
      session)))

;; Event System
;; ============

(defn dispatch!
  "Dispatch event to server and update local state"
  [event]
  (go
    (when @session-id
      (let [response (<! (api-call "/api/dispatch" 
                                  :method :post
                                  :body event))]
        (when response
          (reset! app-state (merge @app-state (:state response))))))))

(defn dispatch-sync!
  "Dispatch and wait for response"
  [event]
  (go
    (when @session-id
      (<! (dispatch! event)))))

;; Real-time Updates via SSE
;; ==========================

(defonce event-source (atom nil))

(defn connect-sse!
  "Establish SSE connection for real-time updates"
  []
  (when @session-id
    (when @event-source
      (.close @event-source))
    (let [url (str server-url "/api/subscribe?session-id=" @session-id)
          es (js/EventSource. url)]
      (set! (.-onmessage es)
            (fn [e]
              (let [data (js->clj (js/JSON.parse (.-data e)) :keywordize-keys true)]
                (case (:type data)
                  :connected (do
                              (swap! app-state assoc :connected true)
                              (js/console.log "Connected to server" (:session-id data)))
                  :state-update (reset! app-state (merge @app-state (:state data)))
                  (js/console.log "Unknown SSE message:" data)))))
      (set! (.-onerror es)
            (fn [e]
              (swap! app-state assoc :connected false)
              (js/console.error "SSE error:" e)))
      (reset! event-source es))))

;; Time Travel
;; ===========

(defonce history (r/atom []))
(defonce history-index (r/atom 0))

(defn load-history!
  "Load session history from server"
  []
  (go
    (when @session-id
      (let [response (<! (api-call "/api/history"))]
        (reset! history response)))))

(defn undo!
  "Undo last action"
  []
  (go
    (when @session-id
      (let [response (<! (api-call "/api/undo" :method :post))]
        (when response
          (reset! app-state (merge @app-state (:state response))))))))

(defn time-travel!
  "Jump to specific point in history"
  [tx-time]
  (go
    (when @session-id
      (let [response (<! (api-call "/api/time-travel" 
                                  :method :post
                                  :body {:tx-time tx-time}))]
        (when response
          (reset! app-state (merge @app-state (:state response))))))))

;; UI Components
;; =============

(defn todo-input []
  (let [value (r/atom "")]
    (fn []
      [rc/h-box
       :gap "10px"
       :children
       [[rc/input-text
         :model value
         :placeholder "What needs to be done?"
         :width "400px"
         :on-submit (fn [v]
                     (when (not (str/blank? v))
                       (dispatch! [:add-todo v])
                       (reset! value "")))
         :change-on-blur? false]
        [rc/button
         :label "Add"
         :class "btn-primary"
         :on-click (fn []
                    (when (not (str/blank? @value))
                      (dispatch! [:add-todo @value])
                      (reset! value "")))]]])))

(defn todo-item [{:keys [id text completed]}]
  [rc/h-box
   :gap "10px"
   :align :center
   :style {:padding "5px"}
   :children
   [[rc/checkbox
     :model completed
     :on-change (fn [_] (dispatch! [:toggle-todo id]))]
    [rc/label
     :label text
     :style (when completed
             {:text-decoration "line-through"
              :color "#999"})]
    [rc/md-icon-button
     :md-icon-name "zmdi-delete"
     :size :smaller
     :on-click (fn [] (dispatch! [:delete-todo id]))]]])

(defn todo-list []
  (let [todos (vals (:todos @app-state))
        filter (:filter @app-state)
        visible-todos (case filter
                       :active (remove :completed todos)
                       :completed (filter :completed todos)
                       todos)]
    [rc/v-box
     :children
     (if (empty? visible-todos)
       [[rc/label :label "No todos to show"]]
       (for [todo visible-todos]
         ^{:key (:id todo)}
         [todo-item todo]))]))

(defn todo-filters []
  (let [todos (vals (:todos @app-state))
        active-count (count (remove :completed todos))
        completed-count (count (filter :completed todos))]
    [rc/h-box
     :gap "20px"
     :align :center
     :children
     [[rc/label :label (str active-count " active")]
      [rc/horizontal-tabs
       :model (:filter @app-state)
       :tabs [{:id :all :label "All"}
              {:id :active :label "Active"}
              {:id :completed :label "Completed"}]
       :on-change (fn [id] (dispatch! [:set-filter id]))]
      (when (> completed-count 0)
        [rc/button
         :label "Clear completed"
         :on-click (fn [] (dispatch! [:clear-completed]))])]]))

(defn time-travel-controls []
  [rc/v-box
   :gap "10px"
   :children
   [[rc/title :level :level3 :label "Time Travel"]
    [rc/h-box
     :gap "10px"
     :children
     [[rc/button
       :label "Undo"
       :on-click undo!]
      [rc/button
       :label "Load History"
       :on-click load-history!]]]
    (when (seq @history)
      [rc/v-box
       :children
       (for [[idx [_ path value tx-time]] (map-indexed vector (take 10 @history))]
         ^{:key idx}
         [rc/h-box
          :gap "5px"
          :style {:font-size "12px"
                 :padding "2px"}
          :children
          [[rc/label :label (str tx-time)]
           [rc/hyperlink
            :label "Jump here"
            :on-click (fn [] (time-travel! tx-time))]]])])]])

(defn connection-status []
  [rc/h-box
   :gap "5px"
   :align :center
   :children
   [[rc/md-icon-button
     :md-icon-name (if (:connected @app-state)
                    "zmdi-wifi"
                    "zmdi-wifi-off")
     :size :smaller
     :style {:color (if (:connected @app-state) "green" "red")}]
    [rc/label
     :label (if (:connected @app-state)
             (str "Connected (Session: " @session-id ")")
             "Disconnected")]]])

(defn todo-app []
  [rc/v-box
   :height "100vh"
   :padding "20px"
   :children
   [[rc/h-box
     :justify :between
     :align :center
     :children
     [[rc/title :level :level1 :label "Reactor TODOs (Session Edition)"]
      [connection-status]]]
    
    (if (:loading @app-state)
      [rc/throbber :size :large]
      [rc/v-box
       :gap "20px"
       :children
       [[todo-input]
        [todo-list]
        [todo-filters]
        [rc/line]
        [time-travel-controls]]])]])

;; Application Entry Point
;; =======================

(defn mount-root []
  (rdom/render [todo-app] (.getElementById js/document "app")))

(defn init! []
  (enable-console-print!)
  (go
    (<! (init-session!))
    (connect-sse!)
    (mount-root)))

(defn ^:dev/after-load reload! []
  (mount-root))

;; Initialize on load
(init!)