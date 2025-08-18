(ns examples.todo-app.app-session
  "Todo app with session support and time travel - ClojureScript version"
  (:require [reagent.core :as r]
            [reagent.dom :as rdom]
            [clojure.string :as str]))

;; ===== Configuration =====

(def server-url "http://localhost:9000")
(defonce session-id (r/atom nil))
(defonce app-state (r/atom {:todos {}
                            :filter :all
                            :loading true}))
(defonce history-info (r/atom nil))
(defonce sessions-list (r/atom []))
(defonce sse-connection (atom nil))

;; ===== Server Communication =====

(defn init-session! []
  (-> (js/fetch (str server-url "/api/init")
                #js {:method "POST"})
      (.then #(.json %))
      (.then (fn [data]
               (reset! session-id (aget data "session-id"))
               (reset! app-state (js->clj (aget data "state") :keywordize-keys true))
               (js/console.log "Session initialized:" @session-id)))))

(defn dispatch! [event]
  (when @session-id
    (-> (js/fetch (str server-url "/api/dispatch?session-id=" @session-id)
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"}
                       :body (js/JSON.stringify (clj->js event))})
        (.then #(.json %))
        (.then (fn [data]
                 (reset! app-state (js->clj (aget data "state") :keywordize-keys true))
                 (update-history!)))
        (.catch #(js/console.error "Dispatch failed:" %)))))

(defn undo! []
  (when @session-id
    (-> (js/fetch (str server-url "/api/undo?session-id=" @session-id)
                  #js {:method "POST"})
        (.then #(.json %))
        (.then (fn [data]
                 (reset! app-state (js->clj (aget data "state") :keywordize-keys true))
                 (update-history!))))))

(defn redo! []
  (when @session-id
    (-> (js/fetch (str server-url "/api/redo?session-id=" @session-id)
                  #js {:method "POST"})
        (.then #(.json %))
        (.then (fn [data]
                 (reset! app-state (js->clj (aget data "state") :keywordize-keys true))
                 (update-history!))))))

(defn jump-to-history! [index]
  (when @session-id
    (-> (js/fetch (str server-url "/api/jump-history?session-id=" @session-id)
                  #js {:method "POST"
                       :headers #js {"Content-Type" "application/json"}
                       :body (js/JSON.stringify #js {:index index})})
        (.then #(.json %))
        (.then (fn [data]
                 (reset! app-state (js->clj (aget data "state") :keywordize-keys true))
                 (update-history!))))))

(defn update-history! []
  (when @session-id
    (-> (js/fetch (str server-url "/api/history?session-id=" @session-id))
        (.then #(.json %))
        (.then (fn [data]
                 (reset! history-info (js->clj data :keywordize-keys true)))))))

(defn load-sessions! []
  (-> (js/fetch (str server-url "/api/sessions"))
      (.then #(.json %))
      (.then (fn [data]
               (reset! sessions-list (js->clj data :keywordize-keys true))))))

(defn switch-session! [new-session-id]
  (reset! session-id new-session-id)
  (setup-sse!)
  (-> (js/fetch (str server-url "/api/state?session-id=" new-session-id))
      (.then #(.json %))
      (.then (fn [data]
               (reset! app-state (js->clj data :keywordize-keys true))
               (update-history!)
               (load-sessions!)))))

;; ===== SSE Connection =====

(defn setup-sse! []
  ;; Close existing connection if any
  (when-let [conn @sse-connection]
    (.close conn))
  
  (when @session-id
    (let [event-source (js/EventSource. (str server-url "/api/subscribe?session-id=" @session-id))]
      (set! (.-onmessage event-source)
            (fn [event]
              (let [data (js/JSON.parse (.-data event))]
                (reset! app-state (js->clj data :keywordize-keys true)))))
      (set! (.-onerror event-source)
            (fn [event]
              (js/console.error "SSE error:" event)))
      (reset! sse-connection event-source))))

;; ===== Styles =====

(def styles
  {:app {:min-height "100vh"
         :background "linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%)"
         :padding "40px 20px"
         :font-family "'Inter', -apple-system, BlinkMacSystemFont, sans-serif"}
   
   :container {:max-width "800px"
               :margin "0 auto"
               :padding-left "220px"}  ;; Make room for sidebar
   
   :main-panel {:background "rgba(15, 12, 41, 0.95)"
                :border "1px solid rgba(0, 255, 204, 0.2)"
                :border-radius "12px"
                :padding "30px"
                :box-shadow "0 20px 60px rgba(0, 0, 0, 0.3), 0 0 100px rgba(255, 0, 128, 0.1)"}
   
   :title {:color "#00ffcc"
           :font-size "28px"
           :font-weight "700"
           :margin-bottom "8px"
           :text-shadow "0 0 20px rgba(0, 255, 204, 0.5)"
           :letter-spacing "2px"}
   
   :session-badge {:display "inline-block"
                   :padding "4px 12px"
                   :background "rgba(255, 0, 128, 0.2)"
                   :border "1px solid #ff0080"
                   :border-radius "20px"
                   :color "#ff0080"
                   :font-size "12px"
                   :margin-left "15px"
                   :font-family "'Courier New', monospace"}
   
   :input {:width "100%"
           :padding "14px 18px"
           :background "rgba(0, 0, 0, 0.4)"
           :border "1px solid rgba(0, 255, 204, 0.3)"
           :border-radius "8px"
           :color "#00ffcc"
           :font-size "16px"
           :outline "none"
           :margin "20px 0"
           :transition "all 0.3s"}
   
   :todo-item {:display "flex"
               :align-items "center"
               :padding "16px"
               :margin-bottom "8px"
               :background "rgba(0, 0, 0, 0.3)"
               :border "1px solid rgba(0, 255, 204, 0.1)"
               :border-radius "8px"
               :transition "all 0.3s"}
   
   :todo-item-hover {:background "rgba(0, 255, 204, 0.05)"
                     :border-color "rgba(0, 255, 204, 0.3)"}
   
   :checkbox {:width "20px"
              :height "20px"
              :margin-right "15px"
              :cursor "pointer"
              :accent-color "#00ffcc"}
   
   :todo-text {:flex "1"
               :color "#00ffcc"
               :font-size "16px"}
   
   :todo-text-completed {:color "rgba(0, 255, 204, 0.4)"
                         :text-decoration "line-through"}
   
   :delete-btn {:padding "6px 12px"
                :background "rgba(255, 0, 128, 0.2)"
                :border "1px solid #ff0080"
                :border-radius "6px"
                :color "#ff0080"
                :cursor "pointer"
                :font-size "14px"
                :transition "all 0.2s"}
   
   :filter-bar {:display "flex"
                :gap "12px"
                :justify-content "center"
                :margin "20px 0"}
   
   :filter-btn {:padding "8px 16px"
                :background "rgba(0, 0, 0, 0.3)"
                :border "1px solid rgba(0, 255, 204, 0.2)"
                :border-radius "6px"
                :color "rgba(0, 255, 204, 0.6)"
                :cursor "pointer"
                :transition "all 0.3s"
                :font-size "14px"}
   
   :filter-btn-active {:background "rgba(0, 255, 204, 0.2)"
                       :border-color "#00ffcc"
                       :color "#00ffcc"}
   
   :stats-bar {:display "flex"
               :justify-content "space-between"
               :align-items "center"
               :padding "15px 20px"
               :margin-top "20px"
               :background "rgba(0, 0, 0, 0.3)"
               :border "1px solid rgba(255, 0, 128, 0.2)"
               :border-radius "8px"}
   
   :time-travel {:margin-top "30px"
                 :padding "20px"
                 :background "rgba(0, 0, 0, 0.3)"
                 :border "1px solid rgba(255, 0, 128, 0.2)"
                 :border-radius "8px"}
   
   :time-controls {:display "flex"
                   :gap "15px"
                   :align-items "center"
                   :justify-content "center"
                   :margin-bottom "20px"}
   
   :btn {:padding "10px 20px"
         :background "rgba(0, 255, 204, 0.2)"
         :border "1px solid #00ffcc"
         :border-radius "6px"
         :color "#00ffcc"
         :cursor "pointer"
         :font-size "14px"
         :font-weight "500"
         :transition "all 0.3s"}
   
   :btn-disabled {:opacity "0.3"
                  :cursor "not-allowed"}
   
   :sessions-panel {:position "fixed"
                    :left "20px"
                    :top "20px"
                    :bottom "20px"
                    :width "180px"
                    :background "rgba(15, 12, 41, 0.95)"
                    :border "1px solid rgba(0, 255, 204, 0.2)"
                    :border-radius "12px"
                    :padding "20px"
                    :overflow-y "auto"
                    :z-index "1000"}
   
   :session-item {:padding "10px"
                  :margin-bottom "8px"
                  :background "rgba(0, 0, 0, 0.3)"
                  :border "1px solid rgba(0, 255, 204, 0.1)"
                  :border-radius "6px"
                  :cursor "pointer"
                  :transition "all 0.3s"
                  :word-wrap "break-word"
                  :font-size "12px"}
   
   :session-item-active {:background "rgba(0, 255, 204, 0.2)"
                         :border "1px solid #00ffcc"}
   
   :session-name {:color "#00ffcc"
                  :font-weight "600"
                  :display "block"
                  :margin-bottom "4px"
                  :overflow "hidden"
                  :text-overflow "ellipsis"
                  :white-space "nowrap"}
   
   :session-count {:color "rgba(0, 255, 204, 0.5)"
                   :font-size "11px"}})

;; ===== Components =====

(defn todo-item [{:keys [id text completed]}]
  [:div {:style (merge (:todo-item styles)
                       (when completed {:opacity "0.7"}))
         :on-mouse-enter #(set! (.. % -currentTarget -style -background) 
                               "rgba(0, 255, 204, 0.05)")
         :on-mouse-leave #(set! (.. % -currentTarget -style -background) 
                               "rgba(0, 0, 0, 0.3)")}
   [:input {:type "checkbox"
            :checked completed
            :style (:checkbox styles)
            :on-change #(dispatch! ["toggle-todo" id])}]
   [:span {:style (if completed
                    (merge (:todo-text styles) (:todo-text-completed styles))
                    (:todo-text styles))
           :on-click #(dispatch! ["toggle-todo" id])}
    text]
   [:button {:style (:delete-btn styles)
             :on-mouse-enter #(set! (.. % -currentTarget -style -background) 
                                   "rgba(255, 0, 128, 0.4)")
             :on-mouse-leave #(set! (.. % -currentTarget -style -background) 
                                   "rgba(255, 0, 128, 0.2)")
             :on-click #(dispatch! ["delete-todo" id])}
    "×"]])

(defn todo-input []
  (let [text (r/atom "")]
    (fn []
      [:input {:type "text"
               :placeholder "What needs to be done?"
               :value @text
               :style (:input styles)
               :on-focus #(set! (.. % -currentTarget -style -border) "1px solid #00ffcc")
               :on-blur #(set! (.. % -currentTarget -style -border) "1px solid rgba(0, 255, 204, 0.3)")
               :on-change #(reset! text (.. % -target -value))
               :on-key-down (fn [e]
                             (when (= (.-key e) "Enter")
                               (when (not (str/blank? @text))
                                 (dispatch! ["add-todo" @text])
                                 (reset! text ""))))}])))

(defn filter-bar []
  (let [current-filter (:filter @app-state)]
    [:div {:style (:filter-bar styles)}
     [:button {:style (if (= current-filter :all)
                       (merge (:filter-btn styles) (:filter-btn-active styles))
                       (:filter-btn styles))
               :on-click #(dispatch! ["set-filter" "all"])}
      "All"]
     [:button {:style (if (= current-filter :active)
                       (merge (:filter-btn styles) (:filter-btn-active styles))
                       (:filter-btn styles))
               :on-click #(dispatch! ["set-filter" "active"])}
      "Active"]
     [:button {:style (if (= current-filter :completed)
                       (merge (:filter-btn styles) (:filter-btn-active styles))
                       (:filter-btn styles))
               :on-click #(dispatch! ["set-filter" "completed"])}
      "Completed"]]))

(defn time-travel-controls []
  (when @history-info
    (let [{:keys [current-index total-states can-undo can-redo]} @history-info
          display-value (- (dec total-states) current-index)]
      [:div {:style (:time-travel styles)}
       [:h3 {:style {:color "#ff0080"
                    :margin-top "0"
                    :margin-bottom "20px"
                    :font-size "14px"
                    :text-transform "uppercase"
                    :letter-spacing "2px"}}
        "⏰ Time Travel"]
       [:div {:style (:time-controls styles)}
        [:button {:style (if can-undo
                          (:btn styles)
                          (merge (:btn styles) (:btn-disabled styles)))
                  :disabled (not can-undo)
                  :on-click undo!}
         "← Undo"]
        [:span {:style {:color "#00ffcc"
                       :font-size "14px"
                       :min-width "100px"
                       :text-align "center"}}
         (str "State " (- total-states current-index) " of " total-states)]
        [:button {:style (if can-redo
                          (:btn styles)
                          (merge (:btn styles) (:btn-disabled styles)))
                  :disabled (not can-redo)
                  :on-click redo!}
         "Redo →"]]
       [:div {:style {:margin-top "20px"}}
        [:input {:type "range"
                 :min 0
                 :max (dec total-states)
                 :value display-value
                 :style {:width "100%"
                        :accent-color "#00ffcc"}
                 :on-change #(jump-to-history! (- (dec total-states) 
                                                 (js/parseInt (.. % -target -value))))}]
        [:div {:style {:display "flex"
                      :justify-content "space-between"
                      :margin-top "10px"
                      :color "rgba(0, 255, 204, 0.5)"
                      :font-size "12px"}}
         [:span "Oldest"]
         [:span "Newest"]]]])))

(defn sessions-panel []
  [:div {:style (:sessions-panel styles)}
   [:h3 {:style {:color "#00ffcc"
                :margin-top "0"
                :margin-bottom "20px"
                :font-size "14px"
                :text-transform "uppercase"
                :letter-spacing "2px"}}
    "Sessions"]
   [:div
    (doall
      (for [s @sessions-list]
        ^{:key (:session-id s)}
        [:div {:style (if (= (:session-id s) @session-id)
                       (merge (:session-item styles) (:session-item-active styles))
                       (:session-item styles))
               :on-click #(switch-session! (:session-id s))}
         [:span {:style (:session-name styles)
                 :title (:session-id s)}  ;; Show full name on hover
          (:session-id s)]
         [:span {:style (:session-count styles)}
          (str (:todo-count s) " todos")]]))]
   [:button {:style (merge (:btn styles) 
                          {:width "100%"
                           :margin-top "20px"})
             :on-click #(init-session!)}
    "New Session"]])

(defn todo-app []
  (let [todos (vals (:todos @app-state))
        ;; Use server-computed filtered todos with underscore key
        filtered-todos (or (:filtered_todos @app-state) [])
        active-count (count (filter #(not (:completed %)) todos))]
    [:div {:style (:app styles)}
     [sessions-panel]
     [:div {:style (:container styles)}
      [:div {:style (:main-panel styles)}
       [:div
        [:h1 {:style (:title styles)}
         "REACTOR://TODOS"
         [:span {:style (:session-badge styles)}
          @session-id]]
        [todo-input]
        [:div
         (doall
           (for [todo filtered-todos]
             ^{:key (:id todo)}
             [todo-item todo]))]
        (when (seq todos)
          [:div {:style (:stats-bar styles)}
           [:span {:style {:color "#00ffcc"
                          :font-size "14px"}}
            (str active-count " " (if (= active-count 1) "item" "items") " left")]
           [filter-bar]
           (when (some :completed todos)
             [:button {:style (merge (:delete-btn styles)
                                   {:padding "8px 16px"})
                      :on-click #(dispatch! ["clear-completed"])}
              "Clear completed"])])
        [time-travel-controls]]]]]))

;; ===== Mount =====

(defn mount-root []
  ;; For now, keep using the classic render until Reagent fully supports React 18
  (rdom/render [todo-app]
               (.getElementById js/document "app")))

(defn ^:export init! []
  (js/console.log "Initializing Reactor Session Todo App...")
  (.then (init-session!)
         (fn []
           (setup-sse!)
           (update-history!)
           (load-sessions!)
           (mount-root)
           ;; Refresh sessions periodically
           (js/setInterval load-sessions! 5000))))

(defn ^:dev/after-load reload! []
  (mount-root))