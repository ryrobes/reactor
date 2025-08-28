(ns examples.rabbit-demo.reactive-queries
  "Reactive query management for Rabbit Demo blocks"
  (:require [reactor.core :as r]
            [clojure.string :as cstr]
            [reagent.core :as reagent]))

;; Track which blocks have active subscriptions
(defonce block-subscriptions (atom {}))
;; Store query results separately from app state to avoid conflicts
(defonce block-results (reagent/atom {}))
;; Hook to be called when queries execute (for time travel refresh)
(defonce query-execution-hooks (atom {}))
;; Track the SQL for each block to prevent unnecessary re-execution
(defonce block-sql-cache (atom {}))

(defn get-block-results
  "Get the current results for a block"
  [block-id]
  (get @block-results (if (keyword? block-id) 
                        block-id 
                        (keyword (cstr/replace (str block-id) ":" ""))))) ;; ensure kw

(defn has-active-subscription?
  "Check if a block has an active subscription"
  [block-id]
  (contains? @block-subscriptions block-id))

(defn register-query-hook!
  "Register a hook to be called when a query executes"
  [block-id hook-fn]
  (swap! query-execution-hooks assoc block-id hook-fn))

(defn unregister-query-hook!
  "Unregister a query execution hook"
  [block-id]
  (swap! query-execution-hooks dissoc block-id))

(defn strunc [s & [chars]]
  (let [s (cstr/replace s #"[\r\n]+" "")
        chars (or chars 100)]
    (try
      (if (> (count s) chars) (str (subs (str s) 0 chars) "...") (str s))
      (catch :default _ s))))

(defn trigger-query-hooks!
  "Trigger hooks for a block after query execution"
  [block-id sql]
  (when-let [hook-fn (get @query-execution-hooks block-id)]
    (try
      (hook-fn block-id sql)
      (catch js/Error e
        (js/console.error "[REACTIVE-QUERIES] Error in query hook for block" block-id e)))))

(defn subscribe-block-query!
  "Subscribe a block to a SQL query with automatic updates"
  [block-id sql & [params as-of]]
  ;; Unsubscribe from previous query if exists - PROPERLY
  (when-let [old-sub (get @block-subscriptions block-id)]
    ;; Remove watcher first
    (remove-watch old-sub (keyword (str "block-" block-id)))
    ;; Note: We can't close the subscription directly because we don't have the ID
    ;; The subscription will be garbage collected when no longer referenced
    (swap! block-subscriptions dissoc block-id))
  
  ;; Create new subscription - use block-id as the stable subscription ID
  ;; This ensures the server can track subscriptions properly
  (let [result-atom (r/sql-subscribe-with-id! (str block-id) sql params as-of)]
    ;; Store the subscription
    (swap! block-subscriptions assoc block-id result-atom)
    
    ;; Watch the subscription for changes and update our local results atom
    (add-watch result-atom (keyword (str "block-" block-id))
               (fn [_ _ _ new-val]
                 (when-not (:loading new-val)
                   (js/console.log "🌖 [REACTIVE-QUERIES] Block" (str block-id) "got update:" (strunc (str (get new-val :data)) 100))
                   ;; Store results in our separate atom, not in app state
                   (swap! block-results assoc block-id 
                          (if (:error new-val)
                            {:error (:error new-val) 
                             :results nil
                             :executed-sql (:executed-sql new-val)}
                            {:results (:data new-val) 
                             :error nil
                             :executed-sql (:executed-sql new-val)}))
                   ;; Trigger any registered hooks (for time travel refresh)
                   (trigger-query-hooks! block-id sql))))
    
    ;; Return the result atom for initial value
    result-atom))

(defn execute-block-query!
  "Execute a SQL query for a block with reactive subscription"
  [block-id sql & [params as-of]]
  ;; ALWAYS check if we have an active subscription with the same query
  (let [cached-sql (get @block-sql-cache block-id)
        has-subscription (get @block-subscriptions block-id)
        would-have-been-blocked? (and cached-sql  (= cached-sql [sql params as-of]) has-subscription)]
    (if (and cached-sql false ;; temp
             (= cached-sql [sql params as-of])
             has-subscription)
      ;; Same query is already running, skip re-execution COMPLETELY
      (do
        (js/console.log "[REACTIVE-QUERIES] ✓ BLOCKED re-execution for block" (str block-id)
                        "- already subscribed to this exact query")
        ;; Return existing subscription
        has-subscription)
      ;; New or changed query, execute it
      (do
        (js/console.log "[REACTIVE-QUERIES] ✗ Executing query for block" (str block-id)
                        (when would-have-been-blocked? "🧦")
                        (if cached-sql
                          (str "- query changed from " cached-sql " to " [sql params as-of])
                          "- first execution"))
        ;; Update cache
        (swap! block-sql-cache assoc block-id [sql params as-of])
        ;; Clear old results and show loading
        (swap! block-results assoc block-id {:loading true})

        ;; IMPORTANT: Always create a new subscription when changing temporal state
        ;; This ensures that going back to NOW creates a fresh reactive subscription
        (let [result-atom (subscribe-block-query! block-id sql params as-of)]
          ;; The watcher will handle updating the results
          result-atom)))))

(defn unsubscribe-block!
  "Unsubscribe a block from its query"
  [block-id]
  (when-let [result-atom (get @block-subscriptions block-id)]
    ;; Remove the watcher
    (remove-watch result-atom (keyword (str "block-" block-id)))
    ;; Remove from subscriptions
    (swap! block-subscriptions dissoc block-id)
    ;; Clear results
    (swap! block-results dissoc block-id)
    ;; Clear SQL cache
    (swap! block-sql-cache dissoc block-id)))

(defn unsubscribe-all!
  "Unsubscribe all blocks"
  []
  (doseq [block-id (keys @block-subscriptions)]
    (unsubscribe-block! block-id)))