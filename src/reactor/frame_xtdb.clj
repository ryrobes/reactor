(ns reactor.frame-xtdb
  "Re-frame style API backed by XTDB for persistence and time-travel"
  (:require [reactor.xtdb-store :as xts]
            [reactor.xtdb-query :as xtq]
            [xtdb.api :as xt]))

;; ===== Frame App with XTDB Backend =====

(defrecord XTDBFrameApp [node app-entity-id app-atom subscriptions event-handlers rules])

(defn create-xtdb-frame-app
  "Create a re-frame style app backed by XTDB
   Options:
   - :node - Existing XTDB node (or creates in-memory)
   - :app-id - Unique app identifier
   - :initial-state - Initial app state
   - :history - Enable time-travel (true by default with XTDB)"
  [initial-state & {:keys [node app-id history]
                    :or {app-id "app"
                         history true}}]
  (let [node (or node (xts/start-xtdb-node))
        app-entity-id (str "frame-app-" app-id)
        
        ;; Create the app atom wrapper with initial state
        ;; The atom will handle the entity ID conversion and initialization
        app-atom (xts/xtdb-atom node app-entity-id initial-state nil)]
    
    (->XTDBFrameApp node app-entity-id app-atom
                    (atom {})    ; subscriptions
                    (atom {})    ; event-handlers
                    (atom {}))))  ; rules

;; ===== Subscription Management =====

(defn reg-sub
  "Register a subscription handler
   Can be a simple path or a computation function"
  [app sub-id handler-or-path]
  (swap! (:subscriptions app) assoc sub-id handler-or-path))

(defn subscribe
  "Create a reactive subscription to app state
   Returns a reactive atom that updates when data changes"
  [app query-vector]
  (let [sub-id (first query-vector)
        params (rest query-vector)
        handler (get @(:subscriptions app) sub-id)]
    
    (cond
      ;; Simple path subscription
      (vector? handler)
      (let [path handler
            result-atom (atom nil)
            app-atom (:app-atom app)]
        ;; Set up watcher
        (add-watch app-atom (gensym "sub-")
                   (fn [_ _ _ new-state]
                     (reset! result-atom (get-in new-state path))))
        ;; Initialize value
        (reset! result-atom (get-in @app-atom path))
        result-atom)
      
      ;; Computed subscription
      (fn? handler)
      (let [result-atom (atom nil)
            app-atom (:app-atom app)]
        ;; Set up watcher with computation
        (add-watch app-atom (gensym "sub-")
                   (fn [_ _ _ new-state]
                     (reset! result-atom (handler new-state params))))
        ;; Initialize value
        (reset! result-atom (handler @app-atom params))
        result-atom)
      
      :else
      (throw (ex-info "Unknown subscription handler type" 
                      {:sub-id sub-id :handler handler})))))

;; ===== Event Handling =====

(defn reg-event-db
  "Register an event handler that receives and returns db"
  [app event-id handler]
  (swap! (:event-handlers app) assoc event-id
         {:type :db
          :handler handler}))

(defn reg-event-fx
  "Register an event handler that can produce effects"
  [app event-id handler]
  (swap! (:event-handlers app) assoc event-id
         {:type :fx
          :handler handler}))

(defn dispatch
  "Dispatch an event to be handled
   Events are processed synchronously with XTDB transactions"
  [app event-vector]
  (let [event-id (first event-vector)
        params (rest event-vector)
        handler-map (get @(:event-handlers app) event-id)
        app-atom (:app-atom app)]
    
    (when-not handler-map
      (throw (ex-info "No handler registered for event" {:event-id event-id})))
    
    (case (:type handler-map)
      :db
      (let [handler (:handler handler-map)
            current-db @app-atom
            new-db (handler current-db params)]
        (reset! app-atom new-db)
        new-db)
      
      :fx
      (let [handler (:handler handler-map)
            current-db @app-atom
            effects (handler {:db current-db} params)]
        ;; Handle :db effect
        (when-let [new-db (:db effects)]
          (reset! app-atom new-db))
        ;; Could handle other effects here
        effects))))

;; ===== Time Travel with XTDB =====

(defn undo!
  "Undo to previous state using XTDB's temporal features"
  [app]
  (let [node (:node app)
        ;; Get the actual entity ID from the atom
        entity-id (xts/global-key (:app-entity-id app))
        ;; Get transaction history
        history (xt/entity-history (xt/db node) entity-id :desc)
        ;; Skip current, get previous
        prev-tx (second history)]
    (when prev-tx
      (let [prev-db (xt/db node (:xtdb.api/tx-time prev-tx))
            prev-state (xt/entity prev-db entity-id)
            ;; Remove the :xt/id before resetting the atom
            clean-state (dissoc prev-state :xt/id)]
        ;; Reset the app atom to previous state
        (reset! (:app-atom app) clean-state)
        clean-state))))

(defn redo!
  "Redo to next state (if available)"
  [app]
  ;; XTDB doesn't have built-in redo, would need custom tracking
  ;; For now, return nil
  nil)

(defn get-history
  "Get transaction history from XTDB"
  [app & {:keys [limit] :or {limit 100}}]
  (let [node (:node app)
        ;; Get the actual entity ID from the atom
        entity-id (xts/global-key (:app-entity-id app))
        history (xt/entity-history (xt/db node) entity-id :desc)]
    (take limit history)))

(defn jump-to-time!
  "Jump to a specific point in time using XTDB"
  [app tx-time]
  (let [node (:node app)
        ;; Get the actual entity ID from the atom
        entity-id (xts/global-key (:app-entity-id app))
        historical-db (xt/db node tx-time)
        historical-state (xt/entity historical-db entity-id)]
    (when historical-state
      ;; Restore historical state as current
      (xts/put-entity node entity-id (dissoc historical-state :xt/id))
      (xt/sync node)
      historical-state)))

;; ===== Query Support =====

(defn query
  "Execute a query against the app state
   Supports keypaths, SQL, and HoneySQL"
  [app query & {:keys [format] :or {format :keypath}}]
  (let [node (:node app)]
    (case format
      :keypath (xtq/execute-query node query)
      :sql (xtq/execute-query node query)
      :honeysql (xtq/execute-query node query)
      (throw (ex-info "Unknown query format" {:format format})))))

;; ===== Helper Functions =====

(defn get-app-db
  "Get the current app database atom"
  [app]
  (:app-atom app))

(defn stop-app!
  "Cleanup and stop the XTDB node"
  [app]
  (xts/stop-xtdb-node (:node app)))

;; ===== Convenience Macros =====

(defmacro def-sub
  "Define a subscription at the namespace level"
  [app sub-id handler]
  `(reg-sub ~app ~sub-id ~handler))

(defmacro def-event
  "Define an event handler at the namespace level"
  [app event-id handler]
  `(reg-event-db ~app ~event-id ~handler))

;; ===== Migration Helper =====

(defn migrate-from-atom
  "Migrate an existing atom-based app to XTDB"
  [atom-app & {:keys [app-id] :or {app-id "migrated"}}]
  (let [current-state (if (satisfies? clojure.lang.IDeref atom-app)
                        @atom-app
                        atom-app)]
    (create-xtdb-frame-app current-state :app-id app-id)))