(ns examples.todo-app.server-simple
  "Simplified session-aware TODO app server with XTDB backing."
  (:require [reactor.session_simple :as session]
            [org.httpkit.server :as http]
            [compojure.core :refer [defroutes GET POST OPTIONS]]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]))

;; Initialize the session system
(session/init!)

;; Configuration
(def server-url "http://localhost:9000")

;; Event Handlers
;; ==============

(session/reg-event-db :initialize
  (fn [db _]
    {:todos {}
     :filter :all
     :next-id 1}))

(session/reg-event-db :add-todo
  (fn [db [text]]
    (let [db (if (nil? (:next-id db))
               (assoc db :todos {} :filter :all :next-id 1)
               db)
          id (:next-id db)]
      (-> db
          (assoc-in [:todos id] {:id id
                                 :text text
                                 :completed false})
          (update :next-id inc)))))

(session/reg-event-db :toggle-todo
  (fn [db [id]]
    (if (get-in db [:todos id])
      (update-in db [:todos id :completed] not)
      db)))

(session/reg-event-db :delete-todo
  (fn [db [id]]
    (if (:todos db)
      (update db :todos dissoc id)
      db)))

(session/reg-event-db :set-filter
  (fn [db [filter]]
    (assoc db :filter (keyword filter))))

(session/reg-event-db :clear-completed
  (fn [db _]
    (if (:todos db)
      (update db :todos
              (fn [todos]
                (reduce-kv (fn [m k v]
                            (if (:completed v)
                              m
                              (assoc m k v)))
                          {}
                          todos)))
      db)))

;; HTTP Handlers
;; =============

(defn compute-filtered-todos [state]
  (let [todos (vals (:todos state {}))
        filter-type (:filter state :all)]
    (case filter-type
      :active (filter #(not (:completed %)) todos)
      :completed (filter :completed todos)
      todos)))

(defn wrap-cors
  "Add CORS headers"
  [handler]
  (fn [request]
    (let [response (handler request)]
      (-> response
          (assoc-in [:headers "Access-Control-Allow-Origin"] "*")
          (assoc-in [:headers "Access-Control-Allow-Methods"] "GET, POST, OPTIONS")
          (assoc-in [:headers "Access-Control-Allow-Headers"] "Content-Type")))))

(defn handle-init [request]
  (let [session-id (str "session-" (System/currentTimeMillis))
        session (session/create-session! session-id)]
    (session/dispatch session-id [:initialize])
    (let [state @session
          state-with-filtered (assoc state :filtered_todos (compute-filtered-todos state))]
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:session-id session-id
                                    :state state-with-filtered})})))

(defn handle-state [request]
  (let [session-id (get-in request [:params :session-id] "default")
        session (session/get-session session-id)
        state @session]
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body (json/generate-string (assoc state :filtered_todos (compute-filtered-todos state)))}))

(defn handle-dispatch [request]
  (try
    (let [session-id (get-in request [:params :session-id] "default")
          body (slurp (:body request))
          content-type (get-in request [:headers "content-type"] "")
          event (if (str/includes? content-type "edn")
                  (edn/read-string body)
                  (json/parse-string body true))
          event-kw (if (keyword? (first event))
                     (first event)
                     (keyword (first event)))
          event-vec (vec (cons event-kw (rest event)))]
      (session/dispatch session-id event-vec)
      (let [response-state @(session/get-session session-id)
            state-with-filtered (assoc response-state :filtered_todos (compute-filtered-todos response-state))]
        {:status 200
         :headers {"Content-Type" (if (str/includes? content-type "edn")
                                    "application/edn"
                                    "application/json")}
         :body (if (str/includes? content-type "edn")
                 (pr-str {:status "ok" :state state-with-filtered})
                 (json/generate-string {:status "ok" :state state-with-filtered}))}))
    (catch Exception e
      (println "Error in handle-dispatch:" (.getMessage e))
      (.printStackTrace e)
      {:status 500
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:error (.getMessage e)})})))

(defn handle-redo [request]
  (let [session-id (get-in request [:params :session-id] "default")
        session (session/get-session session-id)]
    (session/redo! session-id)
    (let [state @session
          state-with-filtered (assoc state :filtered_todos (compute-filtered-todos state))]
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:status "ok"
                                    :state state-with-filtered})})))

(defn handle-jump-to-history [request]
  (let [session-id (get-in request [:params :session-id] "default")
        body (slurp (:body request))
        {:keys [index]} (json/parse-string body true)]
    (if-let [new-state (session/jump-to-history! session-id index)]
      (let [state-with-filtered (assoc new-state :filtered_todos (compute-filtered-todos new-state))]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string {:status "ok"
                                      :state state-with-filtered})})
      {:status 400
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:error "Invalid history index"})})))

(defn handle-sessions [request]
  (let [sessions (session/get-all-sessions)]
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body (json/generate-string sessions)}))

(defn handle-undo [request]
  (let [session-id (get-in request [:params :session-id] "default")
        session (session/get-session session-id)]
    (session/undo! session-id)
    (let [state @session
          state-with-filtered (assoc state :filtered_todos (compute-filtered-todos state))]
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string {:status "ok"
                                    :state state-with-filtered})})))

(defn handle-history [request]
  (let [session-id (get-in request [:params :session-id] "default")
        history-info (session/get-history-info session-id)]
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body (json/generate-string history-info)}))

;; SSE Support for real-time updates
;; ==================================

(defonce sse-channels (atom {}))

(defn handle-sse [request]
  (let [session-id (get-in request [:params :session-id] "default")]
    (http/with-channel request channel
      (do
        ;; Store channel
        (swap! sse-channels assoc session-id channel)
        
        ;; Watch session for changes
        (let [session (session/get-session session-id)
              watch-key (keyword "sse" session-id)]
          (add-watch session watch-key
            (fn [_ _ old-state new-state]
              (when-let [ch (get @sse-channels session-id)]
                (try
                  (let [state-with-filtered (assoc new-state :filtered_todos (compute-filtered-todos new-state))]
                    (http/send! ch (str "data: " (json/generate-string state-with-filtered) "\n\n") false))
                  (catch Exception e
                    (println "Error sending SSE update:" (.getMessage e))))))))
        
        ;; Send initial state
        (http/send! channel {:status 200
                           :headers {"Content-Type" "text/event-stream"
                                    "Cache-Control" "no-cache"
                                    "Access-Control-Allow-Origin" "*"}}
                   false)
        
        ;; Send the actual initial data
        (let [initial-state @(session/get-session session-id)
              state-with-filtered (assoc initial-state :filtered_todos (compute-filtered-todos initial-state))]
          (http/send! channel (str "data: " (json/generate-string state-with-filtered) "\n\n") false))
        
        ;; Handle disconnect
        (http/on-close channel
          (fn [_]
            (when-let [session (get @session/sessions session-id)]
              (remove-watch session (keyword "sse" session-id)))
            (swap! sse-channels dissoc session-id)))))))

;; Routes
;; ======

(defn handle-index [_]
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body (slurp "resources/public/index.html")})

(defn handle-demo [_]
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body (slurp "resources/public/demo.html")})

(defroutes app-routes
  (GET "/" [] handle-index)
  (GET "/demo.html" [] handle-demo)
  (POST "/api/init" [] handle-init)
  (GET "/api/state" [] handle-state)
  (POST "/api/dispatch" [] handle-dispatch)
  (POST "/api/undo" [] handle-undo)
  (POST "/api/redo" [] handle-redo)
  (POST "/api/jump-history" [] handle-jump-to-history)
  (GET "/api/history" [] handle-history)
  (GET "/api/sessions" [] handle-sessions)
  (GET "/api/subscribe" [] handle-sse)
  ;; Add compatibility endpoints for reactor/client.cljs
  (GET "/subscribe" [] handle-sse)
  (POST "/dispatch" [] handle-dispatch)
  (OPTIONS "/*" []
    {:status 200
     :headers {"Access-Control-Allow-Origin" "*"
               "Access-Control-Allow-Methods" "GET, POST, OPTIONS"
               "Access-Control-Allow-Headers" "Content-Type"}}))

(def app
  (-> app-routes
      (wrap-defaults api-defaults)
      wrap-cors))

;; Server Management
;; =================

(defonce server (atom nil))

(defn start-server [port]
  (when @server
    (@server))
  (println "\n" (str/join (repeat 60 "=")))
  (println "🚀 Reactor TODO Server (Session Edition)")
  (println (str/join (repeat 60 "=")))
  (println "Port:" port)
  (println "\nFeatures:")
  (println "  ✅ Session-isolated state (multiple users)")
  (println "  ✅ XTDB persistence")
  (println "  ✅ Time travel (undo per session)")
  (println "  ✅ Real-time updates via SSE")
  (println "  ✅ Re-frame style event handlers")
  (println "\nAPI Endpoints:")
  (println (str "  POST " server-url "/api/init - Create new session"))
  (println (str "  GET  " server-url "/api/state?session-id=XXX - Get state"))
  (println (str "  POST " server-url "/api/dispatch?session-id=XXX - Dispatch event"))
  (println (str "  POST " server-url "/api/undo?session-id=XXX - Undo last change"))
  (println (str "  GET  " server-url "/api/history?session-id=XXX - Get history"))
  (println (str "  GET  " server-url "/api/subscribe?session-id=XXX - SSE stream"))
  (println "\nTest with curl:")
  (println "  # Create session")
  (println (str "  curl -X POST http://localhost:" port "/api/init"))
  (println "  # Add todo (replace SESSION_ID)")
  (println (str "  curl -X POST http://localhost:" port "/api/dispatch?session-id=SESSION_ID \\"))
  (println "    -H 'Content-Type: application/json' \\")
  (println "    -d '[\"add-todo\", \"Learn Reactor\"]'")
  (println (str/join (repeat 60 "=")))
  (println)
  (reset! server (http/run-server app {:port port})))

(defn stop-server []
  (when @server
    (@server)
    (reset! server nil)))

(defn demo!
  "Create a demo session with sample data"
  []
  (let [session-id "demo"
        session (session/create-session! session-id)]
    (session/dispatch session-id [:initialize])
    (session/dispatch session-id [:add-todo "Learn Reactor"])
    (session/dispatch session-id [:add-todo "Build with XTDB"])
    (session/dispatch session-id [:add-todo "Master time travel"])
    (session/dispatch session-id [:toggle-todo 1])
    (println "Demo session created:" session-id)
    (println "State:" @session)
    session-id))

(defn -main [& args]
  (let [port (Integer/parseInt (or (first args) "9000"))]
    (start-server port)
    (demo!)
    (Thread/sleep Long/MAX_VALUE)))