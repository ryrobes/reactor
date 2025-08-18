(ns reactor.server
  "Dead simple server setup - one function to rule them all"
  (:require [reactor.session_simple :as session]
            [org.httpkit.server :as http]
            [cheshire.core :as json]
            [xtdb.api :as xt]))

(defn wrap-cors [response]
  (-> response
      (assoc-in [:headers "Access-Control-Allow-Origin"] "*")
      (assoc-in [:headers "Access-Control-Allow-Methods"] "GET, POST, OPTIONS")
      (assoc-in [:headers "Access-Control-Allow-Headers"] "Content-Type")))

(defn create-handler
  "Create a Ring handler with all the Reactor endpoints"
  [& {:keys [session-id-fn]
      :or {session-id-fn (constantly "default")}}]
  (fn [req]
    (let [path (:uri req)
          method (:request-method req)
          session-id (session-id-fn req)
          session (session/get-session session-id)]
      (wrap-cors
        (cond
          ;; CORS preflight
          (= method :options)
          {:status 200 :headers {"Content-Type" "text/plain"}}
          
          ;; Regular routes
          :else
          (case path
            "/api/state"
            {:status 200 
             :headers {"Content-Type" "application/json"}
             :body (json/generate-string @session)}
            
            "/api/dispatch" 
            (let [raw-event (json/parse-string (slurp (:body req)) true)
                  event (vec (cons (keyword (first raw-event)) (rest raw-event)))]
              (session/dispatch session-id event)
              {:status 200 
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string @session)})
            
            "/api/undo" 
            (do (session/undo! session-id)
                {:status 200 
                 :headers {"Content-Type" "application/json"}
                 :body (json/generate-string @session)})
            
            "/api/redo" 
            (do (session/redo! session-id)
                {:status 200 
                 :headers {"Content-Type" "application/json"}
                 :body (json/generate-string @session)})
            
            "/api/query"
            (let [body (json/parse-string (slurp (:body req)) true)
                  query (:query body)
                  db (xt/db (:node session))
                  result (xt/q db query)]
              {:status 200
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string result)})
            
            "/api/subscribe"
            (http/with-channel req channel
              (http/send! channel {:status 200
                                  :headers {"Content-Type" "text/event-stream"
                                           "Cache-Control" "no-cache"
                                           "Access-Control-Allow-Origin" "*"}} false)
              (http/send! channel (str "data: " (json/generate-string @session) "\n\n") false)
              (add-watch session ::sse
                         (fn [_ _ _ new-state]
                           (http/send! channel (str "data: " (json/generate-string new-state) "\n\n") false))))
            
            ;; 404
            {:status 404 :body "Not found"}))))))

(defn start!
  "Start a Reactor server with your event handlers"
  [& {:keys [port handlers session-id-fn init-fn]
      :or {port 4000
           handlers {}
           session-id-fn (constantly "default")}}]
  
  ;; Initialize XTDB
  (session/init!)
  
  ;; Register all handlers
  (doseq [[event-id handler] handlers]
    (session/reg-event-db event-id handler))
  
  ;; Run any custom initialization
  (when init-fn (init-fn))
  
  ;; Start server
  (let [handler (create-handler :session-id-fn session-id-fn)]
    (http/run-server handler {:port port})
    (println (str "🚀 Reactor server running on http://localhost:" port))
    
    ;; Return server info
    {:port port
     :handler handler
     :handlers @session/event-handlers}))