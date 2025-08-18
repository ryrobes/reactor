(ns examples.todo-app.server
  (:require [reactor.core :as r]
            [reactor.frame :as rf]
            [reactor.sse :as sse]
            [org.httpkit.server :as http]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.middleware.file :refer [wrap-file]]
            [ring.middleware.content-type :refer [wrap-content-type]]
            [ring.middleware.not-modified :refer [wrap-not-modified]]
            [compojure.core :refer [defroutes GET POST routes]]
            [compojure.route :as route]
            [clojure.java.io :as io]))

;; Initialize the app with time-travel enabled
(def app (rf/create-frame-app
          {:todos {}
           :filter :all
           :next-id 1
           :users {}
           :time-travel {:history-count 0
                        :current-index 0
                        :can-undo false
                        :can-redo false}}
          {:history true
           :max-history 100}))


;; Helper to update time travel metadata
(defn update-time-travel-state [db]
  (let [history ((:get-history app))
        history-count (if history (count history) 0)
        future-count ((:get-future-count app))
        ;; Current index is based on history position
        current-index (max 0 (dec history-count))]
    (assoc db :time-travel
           {:history-count history-count
            :current-index current-index
            :can-undo (and history (> history-count 1))
            :can-redo (pos? future-count)
            :future-count future-count
            :max-index (+ current-index future-count)})))

;; Subscriptions
(rf/reg-sub :todos
  (fn [db _]
    (:todos db)))

(rf/reg-sub :visible-todos
  (fn [db _]
    (let [todos (:todos db)
          filter (:filter db)]
      (case filter
        :active (into {} (filter #(not (:completed (val %))) todos))
        :completed (into {} (filter #(:completed (val %)) todos))
        :all todos))))

(rf/reg-sub :todo-count
  (fn [db _]
    (count (filter #(not (:completed (val %))) (:todos db)))))

(rf/reg-sub :completed-count
  (fn [db _]
    (count (filter #(:completed (val %)) (:todos db)))))

(rf/reg-sub :filter
  (fn [db _]
    (:filter db)))

(rf/reg-sub :time-travel/history-count
  (fn [db _]
    (get-in db [:time-travel :history-count] 0)))

(rf/reg-sub :time-travel/current-index
  (fn [db _]
    (get-in db [:time-travel :current-index] 0)))

(rf/reg-sub :time-travel/can-undo
  (fn [db _]
    (get-in db [:time-travel :can-undo] false)))

(rf/reg-sub :time-travel/can-redo
  (fn [db _]
    (get-in db [:time-travel :can-redo] false)))

;; Events
(rf/reg-event-db :add-todo
  (fn [db [text]]
    (let [id (:next-id db)
          new-todo {:id id
                    :text text
                    :completed false
                    :created-at (System/currentTimeMillis)}]
      (-> db
          (assoc-in [:todos id] new-todo)
          (update :next-id inc)
          update-time-travel-state))))

(rf/reg-event-db :toggle-todo
  (fn [db [id]]
    (-> db
        (update-in [:todos id :completed] not)
        update-time-travel-state)))

(rf/reg-event-db :delete-todo
  (fn [db [id]]
    (-> db
        (update :todos dissoc id)
        update-time-travel-state)))

(rf/reg-event-db :update-todo-text
  (fn [db [id new-text]]
    (assoc-in db [:todos id :text] new-text)))

(rf/reg-event-db :set-filter
  (fn [db [filter]]
    (-> db
        (assoc :filter filter)
        update-time-travel-state)))

(rf/reg-event-db :clear-completed
  (fn [db _]
    (-> db
        (update :todos
                (fn [todos]
                  (into {} (remove #(:completed (val %)) todos))))
        update-time-travel-state)))

(rf/reg-event-fx :toggle-all
  (fn [{:keys [db]} _]
    (let [todos (:todos db)
          all-completed? (every? #(:completed (val %)) todos)]
      {:db (-> db
               (update :todos
                       (fn [todos]
                         (into {}
                               (map (fn [[id todo]]
                                      [id (assoc todo :completed (not all-completed?))])
                                    todos))))
               update-time-travel-state)})))

;; Time travel event handlers
(rf/reg-event-fx :time-travel/undo
  (fn [{:keys [db]} _]
    (if-let [new-db ((:undo! app))]
      {:db (update-time-travel-state new-db)}
      (do
        (println "Cannot undo - at beginning of history")
        {:db db}))))

(rf/reg-event-fx :time-travel/redo
  (fn [{:keys [db]} _]
    (if-let [new-db ((:redo! app))]
      {:db (update-time-travel-state new-db)}
      (do
        (println "Cannot redo - at end of history")
        {:db db}))))

(rf/reg-event-fx :time-travel/jump-to
  (fn [{:keys [db]} [index]]
    (println "Jump to index:" index)
    (if-let [new-db ((:jump-to! app) index)]
      {:db (update-time-travel-state new-db)}
      {:db db})))

;; Rules for business logic
(r/def-rule (:app-db app) :log-todo-changes [:todos]
            (fn [_ todos]
              (println "Todos changed. Count:" (count todos))))

(r/def-rule (:app-db app) :achievement-checker [:completed-count]
            (fn [count] (>= count 10))
            (fn [_ count]
              (println "🎉 Achievement unlocked! You've completed" count "todos!")))

;; Server routes
(defroutes api-routes
  (GET "/api/state" []
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (pr-str @(:app-db app))})
  
  (POST "/api/dispatch" req
    (let [event (read-string (slurp (:body req)))]
      ((:dispatch app) event)
      {:status 200
       :headers {"Content-Type" "application/edn"}
       :body (pr-str {:status :ok})})))

(defroutes static-routes
  (GET "/" [] 
    {:status 200
     :headers {"Content-Type" "text/html"}
     :body (slurp "resources/public/todo.html")})
  
  (GET "/old" []
    {:status 200
     :headers {"Content-Type" "text/html"}
     :body (slurp "resources/public/index.html")})
  (route/resources "/")
  (route/not-found "Not Found"))

;; Combine all routes
(defn create-handler []
  (-> (routes
        api-routes
        (sse/sse-routes (:app-db app))
        static-routes)
      (wrap-resource "public")
      (wrap-content-type)
      (wrap-not-modified)))

;; Development helpers
(defn seed-todos! []
  ((:dispatch app) [:add-todo "Learn Reactor"])
  ((:dispatch app) [:add-todo "Build reactive apps"])
  ((:dispatch app) [:add-todo "Master server-side re-frame"])
  ((:dispatch app) [:toggle-todo 1]))

(defn print-state []
  (println "Current state:")
  ((requiring-resolve 'clojure.pprint/pprint) @(:app-db app)))

(defn subscribe-to-todos []
  (let [todos-sub ((:subscribe app) [:visible-todos])]
    (add-watch todos-sub :printer
               (fn [_ _ _ new-todos]
                 (println "Visible todos updated:")
                 ((requiring-resolve 'clojure.pprint/pprint) new-todos)))))

;; Server startup
(defonce server (atom nil))

(defn start-server [port]
  (reset! server (http/run-server (create-handler) {:port port}))
  (println "Server started on port" port)
  (seed-todos!)
  (println "Sample todos added"))

(defn stop-server []
  (when @server
    (@server)
    (reset! server nil)
    (println "Server stopped")))

(defn restart-server [port]
  (stop-server)
  (start-server port))

(defn -main [& args]
  (let [port (Integer/parseInt (or (first args) "3000"))]
    (start-server port)
    (println "Todo app server running at http://localhost:" port)
    (println "SSE endpoint at http://localhost:" port "/subscribe")
    (println "Dispatch endpoint at http://localhost:" port "/api/dispatch")))