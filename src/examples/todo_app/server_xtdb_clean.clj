(ns examples.todo-app.server-xtdb-clean
  "Clean XTDB-backed TODO server implementation"
  (:require [reactor.frame-xtdb :as rfx]
            [reactor.sse-xtdb :as sse]
            [org.httpkit.server :as http]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.middleware.content-type :refer [wrap-content-type]]
            [ring.middleware.not-modified :refer [wrap-not-modified]]
            [compojure.core :refer [defroutes GET POST routes]]
            [compojure.route :as route]
            [clojure.edn :as edn]))

;; Global state
(defonce app (atom nil))
(defonce server (atom nil))

(defn update-time-travel-state [db]
  "Helper to update time travel metadata"
  (let [history (if @app (rfx/get-history @app :limit 50) [])
        history-count (count history)]
    (assoc db :time-travel
           {:history-count history-count
            :current-index (max 0 (dec history-count))
            :can-undo (> history-count 1)
            :can-redo false
            :future-count 0
            :max-index (dec history-count)})))

(defn register-handlers! []
  "Register all subscriptions and event handlers"
  ;; Subscriptions
  (rfx/reg-sub @app :todos
    (fn [db _] (:todos db)))
  
  (rfx/reg-sub @app :filter
    (fn [db _] (:filter db)))
  
  ;; Event handlers
  (rfx/reg-event-db @app :add-todo
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
  
  (rfx/reg-event-db @app :toggle-todo
    (fn [db [id]]
      (-> db
          (update-in [:todos id :completed] not)
          update-time-travel-state)))
  
  (rfx/reg-event-db @app :delete-todo
    (fn [db [id]]
      (-> db
          (update :todos dissoc id)
          update-time-travel-state)))
  
  (rfx/reg-event-fx @app :time-travel/undo
    (fn [{:keys [db]} _]
      (if-let [new-db (rfx/undo! @app)]
        {:db (update-time-travel-state new-db)}
        {:db db}))))

(defn init-app! []
  "Initialize a fresh XTDB-backed app"
  (when @app
    (rfx/stop-app! @app))
  
  (reset! app (rfx/create-xtdb-frame-app
               {:todos {}
                :filter :all
                :next-id 1
                :time-travel {:history-count 0
                            :current-index 0
                            :can-undo false
                            :can-redo false}}
               :app-id (str "todo-" (System/currentTimeMillis))
               :history true))
  
  (register-handlers!))

;; Routes
(defroutes api-routes
  (GET "/api/state" []
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (pr-str @(rfx/get-app-db @app))})
  
  (POST "/api/dispatch" req
    (let [event (edn/read-string (slurp (:body req)))]
      (rfx/dispatch @app event)
      {:status 200
       :headers {"Content-Type" "application/edn"}
       :body (pr-str {:status :ok})}))
  
  (GET "/api/history" []
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (pr-str (rfx/get-history @app :limit 50))}))

(defroutes static-routes
  (GET "/" [] 
    {:status 200
     :headers {"Content-Type" "text/html"}
     :body (if (.exists (clojure.java.io/file "resources/public/todo.html"))
             (slurp "resources/public/todo.html")
             "<h1>TODO App</h1><p>Frontend not found</p>")})
  (route/resources "/")
  (route/not-found "Not Found"))

(defn create-sse-routes []
  (when @app
    (let [node (:node @app)]
      (sse/create-xtdb-sse-handler node))))

(defn create-handler []
  (-> (routes
        api-routes
        (when @app (create-sse-routes))
        static-routes)
      (wrap-resource "public")
      (wrap-content-type)
      (wrap-not-modified)))

(defn seed-todos! []
  "Add some sample todos"
  (when @app
    (rfx/dispatch @app [:add-todo "Learn Reactor with XTDB"])
    (rfx/dispatch @app [:add-todo "Build persistent apps"])
    (rfx/dispatch @app [:add-todo "Master time travel"])))

(declare stop-server)

(defn start-server [port]
  (stop-server)
  (init-app!)
  (reset! server (http/run-server (create-handler) {:port port}))
  (println "Clean XTDB TODO server started on port" port))

(defn stop-server []
  (when @server
    (@server)
    (reset! server nil))
  (when @app
    (rfx/stop-app! @app)
    (reset! app nil)))

(defn -main [& args]
  (let [port (Integer/parseInt (or (first args) "3001"))]
    (start-server port)
    (seed-todos!)
    (println "Server ready at http://localhost:" port)))