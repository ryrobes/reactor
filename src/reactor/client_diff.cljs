(ns reactor.client-diff
  "Client-side diff reconstruction for Reactor
   Handles field-level and structural diffs including EDN fields"
  (:require [cljs.reader :as reader]
            [clojure.walk :as walk]))

;; ============================================================================
;; EDN Handling
;; ============================================================================

(defn safe-read-edn
  "Safely parse EDN string, returns original on failure"
  [s]
  (try
    (reader/read-string s)
    (catch :default _ s)))

(defn safe-pr-str
  "Safely convert to EDN string"
  [v]
  (try
    (pr-str v)
    (catch :default _ (str v))))

;; ============================================================================
;; Structural Diff Application
;; ============================================================================

(defn apply-structural-diff
  "Apply a structural diff to a value (mirrors server-side logic)"
  [current-val diff]
  (case (:type diff)
    ;; Simple value change
    :value-change 
    (:new diff)
    
    ;; EDN field diff - parse, apply changes, stringify
    :edn-diff 
    (let [parsed (if (string? current-val)
                   (safe-read-edn current-val)
                   current-val)
          ;; Deep merge for nested structures
          result (merge-with (fn [old new]
                              (if (and (map? old) (map? new))
                                (merge old new)
                                new))
                            (apply dissoc parsed (keys (:removed diff)))
                            (:added diff))]
      ;; Return as EDN string if original was string
      (if (string? current-val)
        (safe-pr-str result)
        result))
    
    ;; Map diff
    :map-diff
    (merge (apply dissoc current-val (keys (:removed diff)))
           (:added diff))
    
    ;; Sequence diff - apply indexed changes
    :seq-diff
    (let [result (vec (or current-val []))]
      (reduce (fn [v change]
                (case (:op change)
                  :add (if (< (:index change) (count v))
                        (assoc v (:index change) (:value change))
                        (conj v (:value change)))
                  :remove (vec (concat (subvec v 0 (:index change))
                                      (when (< (inc (:index change)) (count v))
                                        (subvec v (inc (:index change))))))
                  :update (assoc v (:index change) (:value change))))
              result
              (sort-by :index (:changes diff))))
    
    ;; Default - return current value unchanged
    current-val))

;; ============================================================================
;; Field-Level Diff Application
;; ============================================================================

(defn apply-field-changes
  "Apply field-level changes to a row/record"
  [row field-changes]
  (reduce-kv (fn [r field change]
              (case (:op change)
                :add (assoc r field (:value change))
                :update (assoc r field (:value change))
                :remove (dissoc r field)
                :structural-update 
                (assoc r field (apply-structural-diff (get r field) (:diff change)))
                ;; Default - no change
                r))
            row
            field-changes))

;; ============================================================================
;; Row-Level Diff Application
;; ============================================================================

(defn apply-row-diff
  "Apply a row-level diff to a result set"
  [current-results diff]
  (let [{:keys [type id-key added removed updated order]} diff
        ;; Convert to map for efficient lookup
        current-by-id (if id-key
                        (into {} (map (juxt id-key identity) current-results))
                        {})
        ;; Process changes
        result-map (as-> current-by-id m
                     ;; Remove deleted rows
                     (apply dissoc m removed)
                     ;; Add new rows
                     (reduce (fn [m row]
                              (assoc m (get row id-key) row))
                            m
                            added)
                     ;; Update changed rows
                     (reduce (fn [m update-entry]
                              (let [id (:id update-entry)
                                    current-row (get m id)]
                                (if (= type :field-diff)
                                  ;; Apply field-level changes
                                  (assoc m id (apply-field-changes current-row (:field-changes update-entry)))
                                  ;; Replace entire row
                                  (assoc m id (:new-values update-entry)))))
                            m
                            updated))
        ;; Convert back to vector
        all-rows (vals result-map)]
    ;; Apply ordering if provided
    (if order
      (let [order-map (into {} (map-indexed (fn [idx id] [id idx]) order))]
        (vec (sort-by #(get order-map (get % id-key) js/Number.MAX_SAFE_INTEGER) all-rows)))
      (vec all-rows))))

;; ============================================================================
;; Main Diff Application
;; ============================================================================

(defn apply-diff-update
  "Apply any type of diff update to current data"
  [current-data diff-message]
  (case (:type diff-message)
    ;; Full update - replace everything
    :full-update
    (get-in diff-message [:result :results])
    
    ;; Row-level diff
    :diff-update
    (apply-row-diff current-data (:diff diff-message))
    
    ;; Field-level diff (includes structural diffs)
    :field-diff-update
    (apply-row-diff current-data (:diff diff-message))
    
    ;; Query update (legacy format)
    :query-update
    (get-in diff-message [:result :results])
    
    ;; Default - return current data unchanged
    current-data))

;; ============================================================================
;; Cache Management
;; ============================================================================

(defonce result-cache (atom {}))

(defn cache-key
  "Generate cache key for a subscription"
  [subscription-id session-id]
  [session-id subscription-id])

(defn get-cached-results
  "Get cached results for a subscription"
  [subscription-id session-id]
  (get @result-cache (cache-key subscription-id session-id)))

(defn update-cache!
  "Update cached results after applying diff"
  [subscription-id session-id new-results]
  (swap! result-cache assoc (cache-key subscription-id session-id) new-results))

(defn process-subscription-update!
  "Process any subscription update message and return updated results"
  [message current-results]
  (let [{:keys [subscription-id session-id type]} message
        ;; Apply the diff
        new-results (apply-diff-update current-results message)]
    ;; Cache the results
    (when (and subscription-id session-id)
      (update-cache! subscription-id session-id new-results))
    ;; Return the new results
    new-results))

;; ============================================================================
;; Debug Helpers
;; ============================================================================

(defn log-diff-stats
  "Log statistics about a diff for debugging"
  [message]
  (when-let [diff (:diff message)]
    (js/console.log "Diff type:" (:type message))
    (js/console.log "Diff stats:"
                   (clj->js {:added (count (:added diff))
                            :removed (count (:removed diff))
                            :updated (count (:updated diff))
                            :has-order (boolean (:order diff))}))
    (when (= (:type diff) :field-diff)
      (let [field-changes (mapcat :field-changes (:updated diff))]
        (js/console.log "Field changes:"
                       (clj->js {:total (count field-changes)
                                :structural (count (filter #(= :structural-update (:op %)) field-changes))}))))))