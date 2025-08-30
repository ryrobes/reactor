(ns reactor.sse
  (:require [reactor.core :as r]
            [org.httpkit.server :as http]
            [ring.middleware.params :refer [wrap-params]]
            [compojure.core :refer [defroutes GET POST]]
            ;[compojure.route :as route]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]))

(def ^:private channels (atom {}))
(def ^:private channel-subs (atom {}))

(defn- parse-path [path-str]
  (when path-str
    (vec (map keyword (str/split path-str #"\.")))))

(defn- format-data [format-type data]
  (case format-type
    "json" (json/generate-string data)
    "edn" (pr-str data)
    (pr-str data)))

(defn- send-sse [channel data]
  (try
    (http/send! channel
                {:status 200
                 :headers {"Content-Type" "text/event-stream"
                           "Cache-Control" "no-cache"
                           "Connection" "keep-alive"}
                 :body (str "data: " data "\n\n")}
                false)
    (catch Exception e
      (println "Error sending SSE:" (.getMessage e)))))

(defn- handle-subscription [req ratom]
  (let [params (:params req)
        path-str (get params "path")
        path (parse-path path-str)
        format-type (get params "format" "edn")]
    (println "SSE subscription request - path:" path-str "format:" format-type)
    
    (http/with-channel req channel
      (println "SSE channel opened")
      (http/on-close channel
                     (fn [status]
                       (println "SSE channel closed with status:" status)
                       (when-let [sub-or-watch (get @channel-subs channel)]
                         (if (keyword? sub-or-watch)
                           (if (get-in @channels [channel :watch-key])
                             (remove-watch ratom sub-or-watch)
                             (r/unsubscribe! ratom sub-or-watch))
                           (r/unsubscribe! ratom sub-or-watch)))
                       (swap! channels dissoc channel)
                       (swap! channel-subs dissoc channel)))
      
      ;; Send SSE headers immediately
      (http/send! channel
                  {:status 200
                   :headers {"Content-Type" "text/event-stream"
                            "Cache-Control" "no-cache"
                            "Connection" "keep-alive"
                            "Access-Control-Allow-Origin" "*"}}
                  false)
      
      ;; Set up subscription
      (if path
        (let [sub-id (r/subscribe! ratom path
                                    (fn [_ new-val]
                                      (let [formatted (format-data format-type new-val)]
                                        (http/send! channel (str "data: " formatted "\n\n") false)))
                                    {:key (keyword (gensym "sse-sub-"))})]
          (swap! channels assoc channel {:path path :format format-type})
          (swap! channel-subs assoc channel sub-id)
          
          ;; Send initial data
          (let [initial-val (get-in @ratom path)
                formatted (format-data format-type initial-val)]
            (http/send! channel (str "data: " formatted "\n\n") false)))
        
        (let [watch-key (keyword (gensym "sse-watch-"))
              _ (add-watch ratom watch-key
                          (fn [_ _ _ new-val]
                            (let [formatted (format-data format-type new-val)]
                              (http/send! channel (str "data: " formatted "\n\n") false))))]
          (swap! channels assoc channel {:path nil :format format-type :watch-key watch-key})
          (swap! channel-subs assoc channel watch-key)
          
          ;; Send initial data
          (let [formatted (format-data format-type @ratom)]
            (http/send! channel (str "data: " formatted "\n\n") false)))))))

(defn- handle-update [req ratom]
  (let [body (slurp (:body req))
        content-type (get-in req [:headers "content-type"] "")]
    (try
      (let [data (cond
                   (str/includes? content-type "json")
                   (json/parse-string body true)
                   
                   (str/includes? content-type "edn")
                   (edn/read-string body)
                   
                   :else
                   (edn/read-string body))
            op (keyword (name (:op data)))
            path (if (vector? (:path data))
                   (vec (map keyword (:path data)))
                   (:path data))
            value (:value data)
            session-id (:session-id data)]
        
        (case op
          :assoc-in (swap! ratom assoc-in path value)
          :update-in (swap! ratom update-in path (eval value))
          :dissoc-in (swap! ratom update-in (butlast path) dissoc (last path))
          :swap (swap! ratom (eval value))
          :reset (reset! ratom value)
          ;; Time travel operations
          :undo (r/undo! ratom session-id)
          :redo (r/redo! ratom session-id)
          :jump-to (r/jump-to! ratom (:target data))
          :checkpoint (r/checkpoint! ratom (:name data))
          (throw (ex-info "Unknown operation" {:op op})))
        
        {:status 200
         :headers {"Content-Type" "application/edn"}
         :body (pr-str {:status :ok})})
      
      (catch Exception e
        {:status 400
         :headers {"Content-Type" "application/edn"}
         :body (pr-str {:status :error
                        :message (.getMessage e)})}))))

(defn- handle-time-travel [req ratom]
  (let [body (slurp (:body req))
        data (edn/read-string body)
        action (:action data)
        session-id (:session-id data)]
    (try
      (let [result (case action
                     :undo (r/undo! ratom session-id)
                     :redo (r/redo! ratom session-id)
                     :jump-to (r/jump-to! ratom (:target data))
                     :checkpoint (r/checkpoint! ratom (:name data))
                     :history (r/get-history ratom (:opts data))
                     (throw (ex-info "Unknown time-travel action" {:action action})))]
        {:status 200
         :headers {"Content-Type" "application/edn"}
         :body (pr-str {:status :ok :result result})})
      (catch Exception e
        {:status 400
         :headers {"Content-Type" "application/edn"}
         :body (pr-str {:status :error :message (.getMessage e)})}))))

(defn create-sse-handler [ratom]
  (defroutes sse-routes
    (GET "/subscribe" req (handle-subscription req ratom))
    (POST "/update" req (handle-update req ratom))
    (POST "/time-travel" req (handle-time-travel req ratom))))

(defn wrap-cors [handler]
  (fn [request]
    (let [response (handler request)]
      (-> response
          (assoc-in [:headers "Access-Control-Allow-Origin"] "*")
          (assoc-in [:headers "Access-Control-Allow-Methods"] "GET, POST, OPTIONS")
          (assoc-in [:headers "Access-Control-Allow-Headers"] "Content-Type")))))

(defn create-app [ratom]
  (-> (create-sse-handler ratom)
      wrap-params
      wrap-cors))

(defn sse-routes [ratom]
  (-> (create-sse-handler ratom)
      wrap-params))

(defn start-sse-server [ratom port]
  (let [app (create-app ratom)]
    (http/run-server app {:port port})))

(defn broadcast-to-all [ratom data]
  (doseq [[channel info] @channels]
    (let [formatted (format-data (:format info) data)]
      (send-sse channel formatted))))

(defn broadcast-to-path [ratom path data]
  (doseq [[channel info] @channels]
    (when (= (:path info) path)
      (let [formatted (format-data (:format info) data)]
        (send-sse channel formatted)))))