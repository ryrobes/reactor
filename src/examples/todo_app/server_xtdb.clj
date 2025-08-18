(ns examples.todo-app.server-xtdb
  "TODO app server using XTDB-backed frame for persistence"
  (:require [reactor.frame-xtdb :as rfx]
            [reactor.sse-xtdb :as sse]
            [org.httpkit.server :as http]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.middleware.content-type :refer [wrap-content-type]]
            [ring.middleware.not-modified :refer [wrap-not-modified]]
            [compojure.core :refer [defroutes GET POST routes]]
            [compojure.route :as route]
            [clojure.edn :as edn]))

;; Global app instance (created on server start)
(defonce app (atom nil))

;; Helper to update time travel metadata
(defn update-time-travel-state [db]
  (let [history (if @app (rfx/get-history @app :limit 50) [])
        history-count (count history)]
    (assoc db :time-travel
           {:history-count history-count
            :current-index (max 0 (dec history-count))
            :can-undo (> history-count 1)
            :can-redo false  ; XTDB doesn't track future
            :future-count 0
            :max-index (dec history-count)})))

;; ===== Subscriptions =====

(rfx/reg-sub app :todos
  (fn [db _]
    (:todos db)))

(rfx/reg-sub app :visible-todos
  (fn [db _]
    (let [todos (:todos db)
          filter (:filter db)]
      (case filter
        :active (into {} (filter #(not (:completed (val %))) todos))
        :completed (into {} (filter #(:completed (val %)) todos))
        :all todos))))

(rfx/reg-sub app :todo-count
  (fn [db _]
    (count (filter #(not (:completed (val %))) (:todos db)))))

(rfx/reg-sub app :completed-count
  (fn [db _]
    (count (filter #(:completed (val %)) (:todos db)))))

(rfx/reg-sub app :filter
  (fn [db _]
    (:filter db)))

(rfx/reg-sub app :time-travel/history-count
  (fn [db _]
    (get-in db [:time-travel :history-count] 0)))

(rfx/reg-sub app :time-travel/can-undo
  (fn [db _]
    (get-in db [:time-travel :can-undo] false)))

(rfx/reg-sub app :time-travel/can-redo
  (fn [db _]
    (get-in db [:time-travel :can-redo] false)))

;; ===== Event Handlers =====

(rfx/reg-event-db app :add-todo
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

(rfx/reg-event-db app :toggle-todo
  (fn [db [id]]
    (-> db
        (update-in [:todos id :completed] not)
        update-time-travel-state)))

(rfx/reg-event-db app :delete-todo
  (fn [db [id]]
    (-> db
        (update :todos dissoc id)
        update-time-travel-state)))

(rfx/reg-event-db app :update-todo-text
  (fn [db [id new-text]]
    (-> db
        (assoc-in [:todos id :text] new-text)
        update-time-travel-state)))

(rfx/reg-event-db app :set-filter
  (fn [db [filter]]
    (-> db
        (assoc :filter filter)
        update-time-travel-state)))

(rfx/reg-event-db app :clear-completed
  (fn [db _]
    (-> db
        (update :todos
                (fn [todos]
                  (into {} (remove #(:completed (val %)) todos))))
        update-time-travel-state)))

(rfx/reg-event-fx app :toggle-all
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

;; Time travel event handlers using XTDB
(rfx/reg-event-fx app :time-travel/undo
  (fn [{:keys [db]} _]
    (if-let [new-db (rfx/undo! app)]
      {:db (update-time-travel-state new-db)}
      (do
        (println "Cannot undo - at beginning of history")
        {:db db}))))

(rfx/reg-event-fx app :time-travel/redo
  (fn [{:keys [db]} _]
    ;; XTDB doesn't support redo natively
    (println "Redo not supported with XTDB backend")
    {:db db}))

(rfx/reg-event-fx app :time-travel/jump-to
  (fn [{:keys [db]} [tx-time]]
    (println "Jump to time:" tx-time)
    (if-let [new-db (rfx/jump-to-time! app tx-time)]
      {:db (update-time-travel-state new-db)}
      {:db db})))

;; ===== Server Routes =====

(defroutes api-routes
  (GET "/api/state" []
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (pr-str @(rfx/get-app-db app))})
  
  (POST "/api/dispatch" req
    (let [event (edn/read-string (slurp (:body req)))]
      (rfx/dispatch app event)
      {:status 200
       :headers {"Content-Type" "application/edn"}
       :body (pr-str {:status :ok})}))
  
  (GET "/api/history" []
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (pr-str (rfx/get-history app :limit 50))})
  
  ;; SQL query endpoint
  (POST "/api/query" req
    (let [query-data (edn/read-string (slurp (:body req)))
          result (rfx/query app (:query query-data) 
                           :format (:format query-data :keypath))]
      {:status 200
       :headers {"Content-Type" "application/edn"}
       :body (pr-str {:status :ok :result result})})))

(defroutes static-routes
  (GET "/" [] 
    {:status 200
     :headers {"Content-Type" "text/html"}
     :body (slurp "resources/public/todo.html")})
  
  (route/resources "/")
  (route/not-found "Not Found"))

;; SSE support using XTDB SSE handler
(defn create-sse-routes []
  (let [node (:node app)]
    (sse/create-xtdb-sse-handler node)))

;; Combine all routes
(defn create-handler []
  (-> (routes
        api-routes
        (create-sse-routes)
        static-routes)
      (wrap-resource "public")
      (wrap-content-type)
      (wrap-not-modified)))

;; ===== Development Helpers =====

(defn seed-todos! []
  (println "Seeding todos...")
  (rfx/dispatch app [:add-todo "Learn Reactor with XTDB"])
  (rfx/dispatch app [:add-todo "Build persistent reactive apps"])
  (rfx/dispatch app [:add-todo "Master temporal queries"])
  (rfx/dispatch app [:toggle-todo 1])
  (println "Todos seeded"))

(defn print-state []
  (println "Current state:")
  (clojure.pprint/pprint @(rfx/get-app-db app)))

(defn print-history []
  (println "Transaction history:")
  (doseq [tx (rfx/get-history app :limit 10)]
    (println " -" (:xtdb.api/tx-time tx) (:xtdb.api/tx-id tx))))

;; ===== Server Management =====

(defonce server (atom nil))

(defn init-app! []
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
               :app-id (str "todo-app-" (System/currentTimeMillis))
               :history true))
  ;; Register all handlers
  (rfx/reg-sub @app :todos
    (fn [db _]
      (:todos db)))
  
  (rfx/reg-sub @app :visible-todos
    (fn [db _]
      (let [todos (:todos db)
            filter (:filter db)]
        (case filter
          :active (into {} (filter #(not (:completed (val %))) todos))
          :completed (into {} (filter #(:completed (val %)) todos))
          :all todos))))
  
  (rfx/reg-sub @app :todo-count
    (fn [db _]
      (count (filter #(not (:completed (val %))) (:todos db)))))
  
  (rfx/reg-sub @app :completed-count
    (fn [db _]
      (count (filter #(:completed (val %)) (:todos db)))))
  
  (rfx/reg-sub @app :filter
    (fn [db _]
      (:filter db)))
  
  (rfx/reg-sub @app :time-travel/history-count
    (fn [db _]
      (get-in db [:time-travel :history-count] 0)))
  
  (rfx/reg-sub @app :time-travel/can-undo
    (fn [db _]
      (get-in db [:time-travel :can-undo] false)))
  
  (rfx/reg-sub @app :time-travel/can-redo
    (fn [db _]
      (get-in db [:time-travel :can-redo] false)))
  
  ;; Register event handlers
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
  
  (rfx/reg-event-db @app :update-todo-text
    (fn [db [id new-text]]
      (-> db
          (assoc-in [:todos id :text] new-text)
          update-time-travel-state)))
  
  (rfx/reg-event-db @app :set-filter
    (fn [db [filter]]
      (-> db
          (assoc :filter filter)
          update-time-travel-state)))
  
  (rfx/reg-event-db @app :clear-completed
    (fn [db _]
      (-> db
          (update :todos
                  (fn [todos]
                    (into {} (remove #(:completed (val %)) todos))))
          update-time-travel-state)))
  
  (rfx/reg-event-fx @app :toggle-all
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
  
  ;; Time travel event handlers using XTDB
  (rfx/reg-event-fx @app :time-travel/undo
    (fn [{:keys [db]} _]
      (if-let [new-db (rfx/undo! @app)]
        {:db (update-time-travel-state new-db)}
        (do
          (println "Cannot undo - at beginning of history")
          {:db db}))))
  
  (rfx/reg-event-fx @app :time-travel/redo
    (fn [{:keys [db]} _]
      ;; XTDB doesn't support redo natively
      (println "Redo not supported with XTDB backend")
      {:db db}))
  
  (rfx/reg-event-fx @app :time-travel/jump-to
    (fn [{:keys [db]} [tx-time]]
      (println "Jump to time:" tx-time)
      (if-let [new-db (rfx/jump-to-time! @app tx-time)]
        {:db (update-time-travel-state new-db)}
        {:db db}))))

(defn start-server [port]
  (init-app!)
  (reset! server (http/run-server (create-handler) {:port port}))
  (println "XTDB-backed TODO server started on port" port)
  (seed-todos!)
  (println "Sample todos added"))

(defn stop-server []
  (when @server
    (@server)
    (reset! server nil)
    (rfx/stop-app! app)
    (println "Server stopped")))

(defn restart-server [port]
  (stop-server)
  (start-server port))

(defn -main [& args]
  (let [port (Integer/parseInt (or (first args) "3001"))]
    (start-server port)
    (println "XTDB TODO app server running at http://localhost:" port)
    (println "SSE endpoint at http://localhost:" port "/subscribe")
    (println "Query endpoint at http://localhost:" port "/api/query")
    (println "History endpoint at http://localhost:" port "/api/history")
    (println "\nFeatures:")
    (println "  ✓ Full persistence with XTDB")
    (println "  ✓ Time-travel debugging")
    (println "  ✓ SQL/HoneySQL query support")
    (println "  ✓ Temporal queries")
    (println "  ✓ Real-time subscriptions")))