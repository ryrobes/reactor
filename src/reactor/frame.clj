(ns reactor.frame
  "Server-side re-frame style event handling and subscriptions"
  (:require [reactor.core :as r]))

;; Global registries
(def ^:private event-handlers (atom {}))
(def ^:private subscription-handlers (atom {}))
(def ^:private effect-handlers (atom {}))
(def ^:private coeffect-handlers (atom {}))
(def ^:private interceptors (atom {}))

;; Context for interceptor chain
(defrecord Context [coeffects effects queue stack])

(defn ->context
  ([event db]
   (->Context {:event event :db db} {} [] []))
  ([event db initial-coeffects]
   (->Context (merge {:event event :db db} initial-coeffects) {} [] [])))

;; Interceptor functions
(defn enqueue [context interceptors]
  (update context :queue into interceptors))

(defn- invoke-interceptor-fn
  [context interceptor direction]
  (if-let [f (get interceptor direction)]
    (f context)
    context))

(defn- invoke-interceptors [context direction]
  (loop [context context]
    (let [queue (if (= direction :before) :queue :stack)]
      (if-let [interceptor (peek (get context queue))]
        (let [context (update context queue pop)
              context (if (= direction :before)
                        (update context :stack conj interceptor)
                        context)]
          (recur (invoke-interceptor-fn context interceptor direction)))
        context))))

(defn execute [event-v interceptors]
  (-> (->context event-v nil)
      (enqueue interceptors)
      (invoke-interceptors :before)
      (invoke-interceptors :after)))

;; Built-in interceptors
(def do-fx
  {:id :do-fx
   :after (fn [context]
            (doseq [[effect-key effect-value] (:effects context)]
              (if-let [effect-fn (get @effect-handlers effect-key)]
                (effect-fn effect-value)
                (println "Warning: no handler registered for effect:" effect-key)))
            context)})

(def inject-db
  {:id :inject-db
   :before (fn [context]
             (assoc-in context [:coeffects :db] @(:app-db context)))})

(def debug
  {:id :debug
   :before (fn [context]
             (println "Handling event:" (get-in context [:coeffects :event]))
             context)
   :after (fn [context]
            (println "Event handled, new db:" (get-in context [:effects :db]))
            context)})

;; Registration functions
(defn reg-event-db
  "Register a pure event handler that receives db and returns new db"
  ([id handler-fn]
   (reg-event-db id nil handler-fn))
  ([id interceptors handler-fn]
   (swap! event-handlers assoc id
          {:interceptors (into [inject-db do-fx] interceptors)
           :handler-fn (fn [coeffects event-v]
                        {:db (handler-fn (:db coeffects) (rest event-v))})})))

(defn reg-event-fx
  "Register an effectful event handler that returns effects map"
  ([id handler-fn]
   (reg-event-fx id nil handler-fn))
  ([id interceptors handler-fn]
   (swap! event-handlers assoc id
          {:interceptors (into [inject-db do-fx] interceptors)
           :handler-fn (fn [coeffects event-v]
                        (handler-fn coeffects (rest event-v)))})))

(defn reg-sub
  "Register a subscription handler"
  ([id handler-fn]
   (reg-sub id nil handler-fn))
  ([id inputs-fn handler-fn]
   (swap! subscription-handlers assoc id
          {:inputs-fn inputs-fn
           :handler-fn handler-fn})))

(defn reg-fx
  "Register an effect handler"
  [id handler-fn]
  (swap! effect-handlers assoc id handler-fn))

(defn reg-cofx
  "Register a coeffect handler"
  [id handler-fn]
  (swap! coeffect-handlers assoc id handler-fn))

;; Forward declarations
(declare dispatch-with-db subscribe-with-db)

;; Built-in effects - will be initialized after dispatch-with-db is defined
(defn init-built-in-effects []
  (reg-fx :db
    (fn [value]
      (when-let [app-db (:app-db @effect-handlers)]
        (reset! app-db value))))
  
  (reg-fx :dispatch
    (fn [event-v]
      (if-let [app-db (:app-db @effect-handlers)]
        (dispatch-with-db app-db event-v)
        (throw (ex-info "No app-db found" {})))))
  
  (reg-fx :dispatch-n
    (fn [events]
      (if-let [app-db (:app-db @effect-handlers)]
        (doseq [event events]
          (dispatch-with-db app-db event))
        (throw (ex-info "No app-db found" {})))))
  
  (reg-fx :dispatch-later
    (fn [value]
      (if-let [app-db (:app-db @effect-handlers)]
        (doseq [{:keys [ms dispatch]} value]
          (future
            (Thread/sleep ms)
            (dispatch-with-db app-db dispatch)))
        (throw (ex-info "No app-db found" {}))))))

;; Application state management
(defn create-frame-app
  "Create a new re-frame style application with reactive atom"
  ([initial-db]
   (create-frame-app initial-db {}))
  ([initial-db opts]
   (let [app-db (r/ratom initial-db opts)]
     (swap! effect-handlers assoc :app-db app-db)
     {:app-db app-db
      :dispatch (fn [event-v] (dispatch-with-db app-db event-v))
      :subscribe (fn [query-v] (subscribe-with-db app-db query-v))
      :undo! (fn 
               ([] (r/undo! app-db))
               ([session-id] (r/undo! app-db session-id)))
      :redo! (fn 
               ([] (r/redo! app-db))
               ([session-id] (r/redo! app-db session-id)))
      :checkpoint! (fn [name] (r/checkpoint! app-db name))
      :jump-to! (fn [target] (r/jump-to! app-db target))
      :get-history (fn 
                     ([] (r/get-history app-db))
                     ([opts] (r/get-history app-db opts)))
      :get-future-count (fn []
                          (try
                            (let [tt (.-time-travel app-db)]
                              (if (and tt (:future tt))
                                (count @(:future tt))
                                0))
                            (catch Exception _ 0)))})))

(defn dispatch-with-db [app-db event-v]
  (let [event-id (first event-v)]
    (if-let [{:keys [interceptors handler-fn]} (get @event-handlers event-id)]
      (let [context (->context event-v @app-db)
            context (assoc context :app-db app-db)
            context (enqueue context interceptors)
            context (invoke-interceptors context :before)
            effects (handler-fn (:coeffects context) event-v)
            context (assoc context :effects effects)
            context (invoke-interceptors context :after)]
        (when-let [new-db (get-in context [:effects :db])]
          (reset! app-db new-db)))
      (println "Warning: no handler for event:" event-id))))

(defn dispatch [event-v]
  (if-let [app-db (:app-db @effect-handlers)]
    (dispatch-with-db app-db event-v)
    (throw (ex-info "No app-db found. Create app with create-frame-app first." {}))))

(defn subscribe-with-db [app-db query-v]
  (let [query-id (first query-v)
        args (rest query-v)]
    (if-let [{:keys [inputs-fn handler-fn]} (get @subscription-handlers query-id)]
      (let [inputs (when inputs-fn
                     (inputs-fn app-db args))
            result-atom (r/ratom nil)
            compute-fn (if inputs
                         #(apply handler-fn inputs args)
                         #(handler-fn @app-db args))]
        (r/subscribe! app-db
                      compute-fn
                      (fn [_ new-val]
                        (reset! result-atom new-val)))
        (compute-fn) ; Initial computation
        result-atom)
      (throw (ex-info (str "No subscription handler for: " query-id) {:query-id query-id})))))

(defn subscribe [query-v]
  (if-let [app-db (:app-db @effect-handlers)]
    (subscribe-with-db app-db query-v)
    (throw (ex-info "No app-db found. Create app with create-frame-app first." {}))))

;; Utility interceptors
(def path
  "Returns an interceptor that focuses on a path in app-db"
  (fn [p]
    {:id :path
     :before (fn [context]
               (update-in context [:coeffects :db] get-in p))
     :after (fn [context]
              (if-let [db (get-in context [:effects :db])]
                (assoc-in context [:effects :db]
                          (assoc-in (get-in context [:coeffects :original-db]) p db))
                context))}))

(def after
  "Interceptor that runs a function after event processing"
  (fn [f]
    {:id :after
     :after (fn [context]
              (f (get-in context [:effects :db])
                 (get-in context [:coeffects :event]))
              context)}))

(def enrich
  "Interceptor that enriches the :db effect"
  (fn [f]
    {:id :enrich
     :after (fn [context]
              (if-let [db (get-in context [:effects :db])]
                (assoc-in context [:effects :db] (f db (get-in context [:coeffects :event])))
                context))}))

;; Initialize built-in effects at the end of the file after all functions are defined

;; Helper for creating event chains
(defn fx-handler->db-handler
  "Convert an fx handler to a db handler"
  [fx-handler]
  (fn [db event-v]
    (let [effects (fx-handler {:db db} event-v)]
      (:db effects db))))

;; Initialize built-in effects now that all functions are defined
(init-built-in-effects)