(ns examples.rabbit-demo.reactive-queries
  "Reactive query management for Rabbit Demo blocks"
  (:require [reactor.core :as r]
            [reagent.core :as reagent]))

;; Track which blocks have active subscriptions
(defonce block-subscriptions (atom {}))
;; Store query results separately from app state to avoid conflicts
(defonce block-results (reagent/atom {}))

(defn get-block-results
  "Get the current results for a block"
  [block-id]
  (get @block-results block-id))

(defn subscribe-block-query!
  "Subscribe a block to a SQL query with automatic updates"
  [block-id sql & [params as-of]]
  ;; Unsubscribe from previous query if exists
  (when-let [old-sub (get @block-subscriptions block-id)]
    (swap! block-subscriptions dissoc block-id))
  
  ;; Create new subscription
  (let [result-atom (r/sql-subscribe! sql params as-of)]
    ;; Store the subscription
    (swap! block-subscriptions assoc block-id result-atom)
    
    ;; Watch the subscription for changes and update our local results atom
    (add-watch result-atom (keyword (str "block-" block-id))
               (fn [_ _ _ new-val]
                 (when-not (:loading new-val)
                   (js/console.log "[REACTIVE-QUERIES] Block" block-id "got update:" (clj->js new-val))
                   ;; Store results in our separate atom, not in app state
                   (swap! block-results assoc block-id 
                          (if (:error new-val)
                            {:error (:error new-val) :results nil}
                            {:results (:data new-val) :error nil})))))
    
    ;; Return the result atom for initial value
    result-atom))

(defn execute-block-query!
  "Execute a SQL query for a block with reactive subscription"
  [block-id sql & [params as-of]]
  ;; Clear old results and show loading
  (swap! block-results assoc block-id {:loading true})
  
  ;; Subscribe to the query (this will trigger the watcher above)
  (let [result-atom (subscribe-block-query! block-id sql params as-of)]
    ;; The watcher will handle updating the results
    result-atom))

(defn unsubscribe-block!
  "Unsubscribe a block from its query"
  [block-id]
  (when-let [result-atom (get @block-subscriptions block-id)]
    ;; Remove the watcher
    (remove-watch result-atom (keyword (str "block-" block-id)))
    ;; Remove from subscriptions
    (swap! block-subscriptions dissoc block-id)
    ;; Clear results
    (swap! block-results dissoc block-id)))

(defn unsubscribe-all!
  "Unsubscribe all blocks"
  []
  (doseq [block-id (keys @block-subscriptions)]
    (unsubscribe-block! block-id)))