(ns examples.todo-app.client
  (:require [reagent.core :as r]
            [reagent.dom :as rdom]
            [clojure.string :as str]
            [cljs.reader :as reader]))

;; App state
(defonce app-state (r/atom {:todos {}
                             :filter :all
                             :loading true}))

;; Connect to server via SSE
(defonce event-source 
  (when (exists? js/EventSource)
    (let [es (js/EventSource. "/subscribe?format=edn")]
      (set! (.-onmessage es)
            (fn [event]
              (try
                (let [data (reader/read-string (.-data event))
                      old-state @app-state]
                  (js/console.log "SSE data received:" (pr-str data))
                  (js/console.log "Old state:" (pr-str old-state))
                  (reset! app-state (assoc data :loading false))
                  (js/console.log "New state after reset!:" (pr-str @app-state))
                  ;; Force re-render
                  (r/flush))
                (catch :default e
                  (js/console.error "Failed to parse SSE data:" e)))))
      (set! (.-onerror es)
            (fn [_]
              (js/console.error "SSE connection error")))
      (set! (.-onopen es)
            (fn [_]
              (js/console.log "Connected to server via SSE")))
      es)))

;; Forward declare
(declare load-state!)

;; Dispatch to server
(defn dispatch! [event]
  (js/console.log "Dispatching event:" (pr-str event))
  (-> (js/fetch "/api/dispatch"
                #js {:method "POST"
                     :headers #js {"Content-Type" "application/edn"}
                     :body (pr-str event)})
      (.then (fn [response]
               (if (.-ok response)
                 (do
                   (js/console.log "Event dispatched successfully:" (pr-str event))
                   ;; Manually reload state after dispatch for now
                   (js/console.log "Loading state after dispatch...")
                   (load-state!))
                 (js/console.error "Dispatch failed with status:" (.-status response)))))
      (.catch (fn [error]
                (js/console.error "Network error during dispatch:" error)))))

;; Load initial state
(defn load-state! []
  (js/console.log "Loading state from server...")
  (-> (js/fetch "/api/state")
      (.then (fn [response] 
               (if (.-ok response)
                 (.text response)
                 (throw (js/Error. (str "Failed to load state: " (.-status response)))))))
      (.then (fn [text]
               (js/console.log "Raw state text:" text)
               (let [data (reader/read-string text)]
                 (js/console.log "Parsed state data:" (pr-str data))
                 (reset! app-state (assoc data :loading false))
                 (js/console.log "App state after load:" (pr-str @app-state))
                 (r/flush))))
      (.catch (fn [error]
                (js/console.error "Failed to load initial state:" error)
                (swap! app-state assoc :loading false :error true)))))

;; Components
(defn todo-input []
  (let [val (r/atom "")]
    (fn []
      [:div.todo-input
       [:input.new-todo
        {:type "text"
         :placeholder "What needs to be done?"
         :value @val
         :on-change #(reset! val (-> % .-target .-value))
         :on-key-down
         (fn [e]
           (when (= (.-key e) "Enter")
             (when-not (str/blank? @val)
               (dispatch! [:add-todo @val])
               (reset! val ""))))}]])))

(defn todo-item [{:keys [id text completed]}]
  [:li {:class (when completed "completed")}
   [:div.view
    [:input.toggle
     {:type "checkbox"
      :checked completed
      :on-change #(dispatch! [:toggle-todo id])}]
    [:label text]
    [:button.destroy
     {:on-click #(dispatch! [:delete-todo id])}]]])

(defn todo-list []
  (let [{:keys [todos]} @app-state
        current-filter (:filter @app-state)
        visible-todos (case current-filter
                        :active (into {} (filter #(not (:completed (val %))) todos))
                        :completed (into {} (filter #(:completed (val %)) todos))
                        :all todos)]
    [:ul.todo-list
     (for [[id todo] visible-todos]
       ^{:key id}
       [todo-item todo])]))

(defn footer-controls []
  (let [{:keys [todos]} @app-state
        current-filter (:filter @app-state)
        active-count (count (filter #(not (:completed (val %))) todos))
        completed-count (count (filter #(:completed (val %)) todos))]
    [:footer.footer
     [:span.todo-count
      [:strong active-count] " " (if (= active-count 1) "item" "items") " left"]
     [:ul.filters
      [:li [:a {:href "#"
                :class (when (= current-filter :all) "selected")
                :on-click #(dispatch! [:set-filter :all])} "All"]]
      [:li [:a {:href "#"
                :class (when (= current-filter :active) "selected")
                :on-click #(dispatch! [:set-filter :active])} "Active"]]
      [:li [:a {:href "#"
                :class (when (= current-filter :completed) "selected")
                :on-click #(dispatch! [:set-filter :completed])} "Completed"]]]
     (when (pos? completed-count)
       [:button.clear-completed
        {:on-click #(dispatch! [:clear-completed])}
        "Clear completed"])]))

(defn todo-app []
  (let [{:keys [loading error]} @app-state]
    [:div
     [:section.todoapp
      [:header.header
       [:h1 "todos"]
       [todo-input]]
      (cond
        loading [:div {:style {:text-align "center" :padding "20px"}} 
                 "Loading..."]
        error [:div {:style {:text-align "center" :padding "20px" :color "red"}} 
               "Failed to connect to server"]
        :else
        [:<>
         [:section.main
          [:input#toggle-all.toggle-all
           {:type "checkbox"
            :on-change #(dispatch! [:toggle-all])}]
          [:label {:for "toggle-all"} "Mark all as complete"]
          [todo-list]]
         [footer-controls]])]
     [:footer.info
      [:p "Double-click to edit a todo"]
      [:p "Created with Reactor - Server-side Re-frame"]
      [:p "Part of " [:a {:href "http://todomvc.com"} "TodoMVC"]]]]))

(defn init! []
  (js/console.log "Initializing Todo app...")
  (load-state!)
  (rdom/render [todo-app]
               (js/document.getElementById "app"))
  (js/console.log "Todo app initialized!"))