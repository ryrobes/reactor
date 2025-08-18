(ns examples.todo-app.server-session
  "Session-aware TODO app server with XTDB backing.
   Each user gets their own isolated TODO list with time-travel."
  (:require [reactor.session :as session]
            [org.httpkit.server :as http]
            [compojure.core :refer [defroutes GET POST OPTIONS]]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]))

;; Initialize the session system
(session/init!)

;; Event Handlers
;; ==============
;; Re-frame style handlers that work with session state

(session/reg-event-db :initialize
  (fn [db _]
    {:todos {}
     :filter :all
     :next-id 1}))

(session/reg-event-db :add-todo
  (fn [db [text]]
    (let [id (:next-id db)]
      (-> db
          (assoc-in [:todos id] {:id id
                                 :text text
                                 :completed false})
          (update :next-id inc)))))

(session/reg-event-db :toggle-todo
  (fn [db [id]]
    (update-in db [:todos id :completed] not)))

(session/reg-event-db :delete-todo
  (fn [db [id]]
    (update db :todos dissoc id)))

(session/reg-event-db :edit-todo
  (fn [db [id text]]
    (assoc-in db [:todos id :text] text)))

(session/reg-event-db :complete-all
  (fn [db [completed?]]
    (update db :todos
            (fn [todos]
              (reduce-kv (fn [m k v]
                          (assoc m k (assoc v :completed completed?)))
                        {}
                        todos)))))

(session/reg-event-db :clear-completed
  (fn [db _]
    (update db :todos
            (fn [todos]
              (reduce-kv (fn [m k v]
                          (if (:completed v)
                            m
                            (assoc m k v)))
                        {}
                        todos)))))

(session/reg-event-db :set-filter
  (fn [db [filter]]
    (assoc db :filter filter)))

;; Subscription Handlers
;; =====================

(defn visible-todos
  "Compute visible todos based on filter"
  [todos filter]
  (case filter
    :active (remove :completed (vals todos))
    :completed (filter :completed (vals todos))
    :all (vals todos)
    (vals todos)))

;; SSE (Server-Sent Events) Support
;; =================================

(defonce sse-channels (atom {}))

(defn send-sse
  "Send data to SSE channel"
  [channel data]
  (when (and channel (http/on-close channel identity))
    (http/send! channel
                {:status 200
                 :headers {"Content-Type" "text/event-stream"
                          "Cache-Control" "no-cache"}
                 :body (str "data: " (json/generate-string data) "\n\n")}
                false)))

(defn close-sse
  "Close SSE channel"
  [session-id]
  (when-let [channel (get @sse-channels session-id)]
    (http/close channel)
    (swap! sse-channels dissoc session-id)))

(defn handle-sse
  "Establish SSE connection for real-time updates"
  [request]
  (let [session-id (get-in request [:params :session-id] "default")
        channel (http/as-channel request)]
    (if-not channel
      {:status 200 :body "Long polling not supported"}
      (do
        ;; Store channel
        (swap! sse-channels assoc session-id channel)
        
        ;; Watch session for changes
        (let [session (session/get-session session-id)]
          (add-watch session (keyword "sse" session-id)
            (fn [_ _ _ new-state]
              (send-sse channel {:type :state-update
                                :state new-state
                                :timestamp (System/currentTimeMillis)}))))
        
        ;; Send initial state
        (send-sse channel {:type :connected
                          :session-id session-id
                          :state @(session/get-session session-id)})
        
        ;; Handle channel close
        (http/on-close channel
          (fn [_]
            (let [session (session/get-session session-id)]
              (remove-watch session (keyword "sse" session-id)))
            (swap! sse-channels dissoc session-id)))
        
        channel))))

;; HTTP Handlers
;; =============

(defn handle-state
  "Get current session state"
  [request]
  (let [session-id (get-in request [:params :session-id] "default")
        session (session/get-session session-id)
        state @session]
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (pr-str state)}))

(defn handle-dispatch
  "Dispatch an event to session"
  [request]
  (let [session-id (get-in request [:params :session-id] "default")
        body (slurp (:body request))
        event (edn/read-string body)]
    (session/dispatch session-id event)
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (pr-str {:status :ok
                    :event event
                    :state @(session/get-session session-id)})}))

(defn handle-history
  "Get session history for time travel"
  [request]
  (let [session-id (get-in request [:params :session-id] "default")
        session (session/get-session session-id)
        history (session/get-history session)]
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (pr-str history)}))

(defn handle-undo
  "Undo last action in session"
  [request]
  (let [session-id (get-in request [:params :session-id] "default")]
    (session/undo! session-id)
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (pr-str {:status :ok
                    :state @(session/get-session session-id)})}))

(defn handle-time-travel
  "Jump to specific point in time"
  [request]
  (let [session-id (get-in request [:params :session-id] "default")
        body (slurp (:body request))
        {:keys [tx-time]} (edn/read-string body)]
    (session/time-travel! session-id tx-time)
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (pr-str {:status :ok
                    :state @(session/get-session session-id)})}))

(defn handle-subscribe
  "Create a subscription to a path in session state"
  [request]
  (let [session-id (get-in request [:params :session-id] "default")
        path (get-in request [:params :path] [])
        path (if (string? path)
               (map keyword (str/split path #"/"))
               path)]
    (handle-sse request)))

(defn handle-init
  "Initialize a new session"
  [request]
  (let [session-id (or (get-in request [:params :session-id])
                      (str "session-" (System/currentTimeMillis)))
        session (session/create-session! session-id)]
    ;; Initialize with default state
    (session/dispatch session-id [:initialize])
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (pr-str {:session-id session-id
                    :state @session})}))

;; Static File Serving
(defn handle-index [_]
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body (slurp "resources/public/index.html")})

;; Routes
;; ======

(defroutes app-routes
  ;; Static files
  (GET "/" [] handle-index)
  
  ;; Session management
  (POST "/api/init" [] handle-init)
  (GET "/api/state" [] handle-state)
  (POST "/api/dispatch" [] handle-dispatch)
  
  ;; Time travel
  (GET "/api/history" [] handle-history)
  (POST "/api/undo" [] handle-undo)
  (POST "/api/time-travel" [] handle-time-travel)
  
  ;; Real-time subscriptions
  (GET "/api/subscribe" [] handle-subscribe)
  
  ;; CORS preflight
  (OPTIONS "/*" []
    {:status 200
     :headers {"Access-Control-Allow-Origin" "*"
               "Access-Control-Allow-Methods" "GET, POST, OPTIONS"
               "Access-Control-Allow-Headers" "Content-Type"}}))

(defn wrap-cors-headers
  "Simple CORS middleware"
  [handler]
  (fn [request]
    (let [response (handler request)]
      (assoc-in response [:headers "Access-Control-Allow-Origin"] "*"))))

(def app
  (-> app-routes
      (wrap-defaults api-defaults)
      wrap-cors-headers))

;; Server Management
;; =================

(defonce server (atom nil))

(defn start-server
  [port]
  (when @server
    (@server))
  (println "Starting Session-aware TODO server on port" port)
  (println "Features:")
  (println "  • Session-isolated state")
  (println "  • Time travel per session")
  (println "  • Real-time subscriptions via SSE")
  (println "  • XTDB persistence")
  (println)
  (println "Try it:")
  (println (str "  curl -X POST http://localhost:" port "/api/init"))
  (println (str "  curl http://localhost:" port "/api/state?session-id=SESSION_ID"))
  (reset! server (http/run-server app {:port port})))

(defn stop-server []
  (when @server
    (@server)
    (reset! server nil)
    (println "Server stopped")))

(defn seed-session!
  "Create a demo session with sample todos"
  [session-id]
  (let [session (session/create-session! session-id)]
    (session/dispatch session-id [:initialize])
    (session/dispatch session-id [:add-todo "Learn Reactor"])
    (session/dispatch session-id [:add-todo "Build with XTDB"])
    (session/dispatch session-id [:add-todo "Master time travel"])
    (session/dispatch session-id [:toggle-todo 1])
    session-id))

(defn -main [& args]
  (let [port (Integer/parseInt (or (first args) "9000"))]
    (start-server port)
    ;; Create a demo session
    (let [demo-id (seed-session! "demo")]
      (println (str "Demo session created: " demo-id))
      (println (str "Open: http://localhost:" port "/?session=" demo-id)))
    ;; Keep the process running
    (Thread/sleep Long/MAX_VALUE)))