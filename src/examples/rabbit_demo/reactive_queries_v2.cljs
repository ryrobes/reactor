(ns examples.rabbit-demo.reactive-queries-v2
  "Improved reactive query management that properly handles subscription IDs"
  (:require [reactor.core :as r]
            [reagent.core :as reagent]))

;; Track block ID -> result atom mapping
(defonce block-results (reagent/atom {}))

;; Track block ID -> subscription info
(defonce block-subscriptions (atom {}))

(defn get-block-results
  "Get the current results for a block"
  [block-id]
  (get @block-results block-id))

(defn execute-block-query!
  "Execute a SQL query for a block with reactive subscription"
  [block-id sql & [params as-of]]
  (js/console.log "[RQ-V2] Executing query for block" block-id)
  
  ;; Check if we already have a subscription for this exact query
  (let [existing-sub (get @block-subscriptions block-id)]
    (when (and existing-sub 
               (= (:sql existing-sub) sql)
               (= (:params existing-sub) params)
               (= (:as-of existing-sub) as-of))
      (js/console.log "[RQ-V2] Reusing existing subscription for block" block-id)
      ;; Just trigger a refresh of the existing subscription
      (when-let [result-atom (:result-atom existing-sub)]
        ;; The subscription is already active, just return
        (return result-atom))))
  
  ;; Clear old subscription if it exists
  (when-let [old-sub (get @block-subscriptions block-id)]
    (js/console.log "[RQ-V2] Clearing old subscription for block" block-id)
    (when-let [result-atom (:result-atom old-sub)]
      (remove-watch result-atom (keyword (str "block-" block-id)))))
  
  ;; Show loading state
  (swap! block-results assoc block-id {:loading true})
  
  ;; Create new subscription
  (let [result-atom (r/sql-subscribe! sql params as-of)]
    (js/console.log "[RQ-V2] Created new subscription for block" block-id)
    
    ;; Store subscription info
    (swap! block-subscriptions assoc block-id 
           {:sql sql
            :params params
            :as-of as-of
            :result-atom result-atom})
    
    ;; Watch for changes
    (add-watch result-atom (keyword (str "block-" block-id))
               (fn [_ _ old-val new-val]
                 (when (not= old-val new-val)
                   (js/console.log "[RQ-V2] Block" block-id "result changed:" (clj->js new-val))
                   (swap! block-results assoc block-id
                          (if (:error new-val)
                            {:error (:error new-val) :loading false}
                            {:results (:data new-val) :loading false})))))
    
    ;; Return the result atom
    result-atom))

(defn unsubscribe-block!
  "Unsubscribe a block from its query"
  [block-id]
  (js/console.log "[RQ-V2] Unsubscribing block" block-id)
  (when-let [sub-info (get @block-subscriptions block-id)]
    (when-let [result-atom (:result-atom sub-info)]
      (remove-watch result-atom (keyword (str "block-" block-id))))
    (swap! block-subscriptions dissoc block-id)
    (swap! block-results dissoc block-id)))

(defn unsubscribe-all!
  "Unsubscribe all blocks"
  []
  (doseq [block-id (keys @block-subscriptions)]
    (unsubscribe-block! block-id)))