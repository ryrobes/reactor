(ns reactor.subscriptions.store
  "Single source of truth for subscription state.
   No caching for now - keep it simple and add later if needed."
  (:require [clojure.string :as str]))

;; ============================================================================
;; Core State
;; ============================================================================

;; The main subscription store - just ONE atom!
;; Structure: {subscription-id -> subscription-map}
(defonce subscriptions
  (atom {}))

;; Secondary indices for fast lookup.
;; Rebuilt from subscriptions as needed.
(defonce indices
  (atom {:by-table {}     ; table-name -> #{subscription-ids}
         :by-session {}   ; session-id -> #{subscription-ids}
         :by-block {}}))  ; block-id -> #{subscription-ids}

;; ============================================================================
;; Index Management
;; ============================================================================

(defn- update-indices-add!
  "Update indices when adding a subscription"
  [subscription]
  (let [sub-id (:id subscription)
        session-id (:session-id subscription)
        tables (:tables subscription [])
        block-id (:block-id subscription)]
    
    ;; Update by-table index (always lowercase)
    (doseq [table tables]
      (swap! indices update-in [:by-table (str/lower-case table)] 
             (fnil conj #{}) sub-id))
    
    ;; Update by-session index
    (when session-id
      (swap! indices update-in [:by-session session-id]
             (fnil conj #{}) sub-id))
    
    ;; Update by-block index
    (when block-id
      (swap! indices update-in [:by-block block-id]
             (fnil conj #{}) sub-id))))

(defn- update-indices-remove!
  "Update indices when removing a subscription"
  [subscription]
  (let [sub-id (:id subscription)
        session-id (:session-id subscription)
        tables (:tables subscription [])
        block-id (:block-id subscription)]
    
    ;; Remove from by-table index (always lowercase)
    (doseq [table tables]
      (let [table-key (str/lower-case table)]
        (swap! indices update-in [:by-table table-key] disj sub-id)
        ;; Clean up empty sets
        (when (empty? (get-in @indices [:by-table table-key]))
          (swap! indices update :by-table dissoc table-key))))
    
    ;; Remove from by-session index
    (when session-id
      (swap! indices update-in [:by-session session-id] disj sub-id)
      (when (empty? (get-in @indices [:by-session session-id]))
        (swap! indices update :by-session dissoc session-id)))
    
    ;; Remove from by-block index
    (when block-id
      (swap! indices update-in [:by-block block-id] disj sub-id)
      (when (empty? (get-in @indices [:by-block block-id]))
        (swap! indices update :by-block dissoc block-id)))))

(defn rebuild-indices!
  "Rebuild all indices from scratch.
   Useful for recovery or debugging."
  []
  (reset! indices {:by-table {}
                   :by-session {}
                   :by-block {}})
  (doseq [[_ subscription] @subscriptions]
    (update-indices-add! subscription)))

;; ============================================================================
;; Core Operations
;; ============================================================================

(defn add!
  "Add a subscription to the store.
   Returns the subscription with any generated fields."
  [subscription]
  (let [sub-id (:id subscription)
        ;; Add metadata if not present
        subscription (merge {:created-at (System/currentTimeMillis)
                            :status :active}
                           subscription)]
    (swap! subscriptions assoc sub-id subscription)
    (update-indices-add! subscription)
    subscription))

(defn get-subscription
  "Get a subscription by ID"
  [subscription-id]
  (get @subscriptions subscription-id))

(defn update!
  "Update a subscription.
   Updates is a map of fields to update."
  [subscription-id updates]
  (when-let [existing (get @subscriptions subscription-id)]
    ;; Remove old indices
    (update-indices-remove! existing)
    ;; Update subscription
    (let [updated (merge existing updates 
                        {:updated-at (System/currentTimeMillis)})]
      (swap! subscriptions assoc subscription-id updated)
      ;; Add new indices
      (update-indices-add! updated)
      updated)))

(defn delete!
  "Remove a subscription from the store"
  [subscription-id]
  (when-let [subscription (get @subscriptions subscription-id)]
    (update-indices-remove! subscription)
    (swap! subscriptions dissoc subscription-id)
    subscription))

;; ============================================================================
;; Query Operations
;; ============================================================================

(defn find-by-table
  "Find all subscriptions watching a specific table"
  [table-name]
  (let [sub-ids (get-in @indices [:by-table (str/lower-case table-name)] #{})]
    (map #(get @subscriptions %) sub-ids)))

(defn find-by-tables
  "Find all subscriptions watching any of the given tables"
  [table-names]
  (let [tables (map str/lower-case table-names)
        sub-ids (apply clojure.set/union
                      (map #(get-in @indices [:by-table %] #{}) tables))]
    (map #(get @subscriptions %) sub-ids)))

(defn find-by-session
  "Find all subscriptions for a session"
  [session-id]
  (let [sub-ids (get-in @indices [:by-session session-id] #{})]
    (map #(get @subscriptions %) sub-ids)))

(defn find-by-block
  "Find all subscriptions for a block"
  [block-id]
  (let [sub-ids (get-in @indices [:by-block block-id] #{})]
    (map #(get @subscriptions %) sub-ids)))

(defn find-active
  "Find all active subscriptions"
  []
  (filter #(= :active (:status %)) 
          (vals @subscriptions)))

(defn find-by-predicate
  "Find subscriptions matching a predicate function"
  [pred]
  (filter pred (vals @subscriptions)))

;; ============================================================================
;; Bulk Operations
;; ============================================================================

(defn delete-by-session!
  "Delete all subscriptions for a session"
  [session-id]
  (let [subs (find-by-session session-id)]
    (doseq [sub subs]
      (delete! (:id sub)))
    (count subs)))

(defn delete-inactive!
  "Delete subscriptions that haven't been accessed recently"
  [max-age-ms]
  (let [cutoff (- (System/currentTimeMillis) max-age-ms)
        old-subs (find-by-predicate 
                  #(< (or (:last-accessed %) 
                         (:created-at %) 
                         0) 
                     cutoff))]
    (doseq [sub old-subs]
      (delete! (:id sub)))
    (count old-subs)))

(defn pause!
  "Pause a subscription (won't react to changes)"
  [subscription-id]
  (update! subscription-id {:status :paused}))

(defn resume!
  "Resume a paused subscription"
  [subscription-id]
  (update! subscription-id {:status :active}))

;; ============================================================================
;; Statistics
;; ============================================================================

(defn stats
  "Get statistics about the subscription store"
  []
  {:total-subscriptions (count @subscriptions)
   :active-subscriptions (count (find-active))
   :tables-watched (count (get @indices :by-table))
   :sessions-with-subs (count (get @indices :by-session))
   :blocks-with-subs (count (get @indices :by-block))})

(defn clear!
  "Clear all subscriptions - useful for testing"
  []
  (reset! subscriptions {})
  (reset! indices {:by-table {}
                   :by-session {}
                   :by-block {}}))