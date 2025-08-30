(ns reactor.subscriptions.differ
  "Pure functions for calculating differences between result sets.
   No side effects, no dependencies on global state."
  (:require [clojure.set :as set]
            [clojure.data :as data]))

;; ============================================================================
;; Helpers
;; ============================================================================

(defn identify-by
  "Create a map indexed by the given key function"
  [key-fn coll]
  (into {} (map (juxt key-fn identity) coll)))

(defn get-id-key
  "Determine the ID key to use for diffing.
   Checks both old and new results to find the key."
  [old-results new-results]
  (let [sample (or (first new-results) (first old-results))]
    (when sample
      (let [keys (keys sample)]
        (cond
          (contains? sample :id) :id
          (contains? sample :_id) :_id
          (contains? sample :key) :key
          (= 1 (count keys)) (first keys)
          :else nil)))))

;; ============================================================================
;; Row-Level Diff
;; ============================================================================

(defn calculate-row-diff
  "Calculate differences at the row level.
   Returns added, removed, and updated rows."
  [old-results new-results & [id-key]]
  (let [id-key (or id-key (get-id-key old-results new-results) :id)
        old-by-id (identify-by id-key old-results)
        new-by-id (identify-by id-key new-results)
        old-ids (set (keys old-by-id))
        new-ids (set (keys new-by-id))
        
        added-ids (set/difference new-ids old-ids)
        removed-ids (set/difference old-ids new-ids)
        common-ids (set/intersection old-ids new-ids)
        
        ;; Check which common rows actually changed
        updated-ids (into #{}
                         (filter #(not= (old-by-id %)
                                       (new-by-id %)))
                         common-ids)]
    
    {:type :row-diff
     :id-key id-key
     :added (mapv new-by-id added-ids)
     :removed (mapv old-by-id removed-ids)  ; Return full rows, not just IDs
     :updated (mapv new-by-id updated-ids)}))

;; ============================================================================
;; Field-Level Diff
;; ============================================================================

(defn calculate-field-changes
  "Calculate which fields changed for a single row"
  [old-row new-row]
  (let [all-keys (set/union (set (keys old-row))
                           (set (keys new-row)))
        changes (reduce (fn [acc k]
                         (let [old-val (get old-row k)
                               new-val (get new-row k)]
                           (cond
                             ;; Field added
                             (and (nil? old-val) (some? new-val))
                             (assoc acc k {:op :add :value new-val})
                             
                             ;; Field removed
                             (and (some? old-val) (nil? new-val))
                             (assoc acc k {:op :remove})
                             
                             ;; Field changed
                             (not= old-val new-val)
                             (assoc acc k {:op :update :value new-val})
                             
                             ;; No change
                             :else acc)))
                       {}
                       all-keys)]
    (when (seq changes)
      changes)))

(defn calculate-field-diff
  "Calculate differences at the field level.
   Returns specific field changes for each updated row."
  [old-results new-results & [id-key]]
  (let [id-key (or id-key (get-id-key old-results new-results) :id)
        old-by-id (identify-by id-key old-results)
        new-by-id (identify-by id-key new-results)
        old-ids (set (keys old-by-id))
        new-ids (set (keys new-by-id))
        
        added-ids (set/difference new-ids old-ids)
        removed-ids (set/difference old-ids new-ids)
        common-ids (set/intersection old-ids new-ids)
        
        ;; Calculate field-level changes for common rows
        updated-entries (keep (fn [id]
                                (when-let [changes (calculate-field-changes
                                                   (old-by-id id)
                                                   (new-by-id id))]
                                  {:id id
                                   :changes changes}))
                             common-ids)]
    
    {:type :field-diff
     :id-key id-key
     :added (mapv new-by-id added-ids)
     :removed (mapv old-by-id removed-ids)  ; Return full rows, not just IDs
     :updated updated-entries}))

;; ============================================================================
;; Order Tracking
;; ============================================================================

(defn calculate-order-changes
  "Detect if the order of results changed"
  [old-results new-results id-key]
  (let [old-order (mapv id-key old-results)
        new-order (mapv id-key new-results)]
    (when (not= old-order new-order)
      {:old-order old-order
       :new-order new-order})))

;; ============================================================================
;; Diff Analysis
;; ============================================================================

(defn diff-size
  "Calculate the size of a diff (for compression ratio)"
  [diff]
  (case (:type diff)
    :row-diff
    (+ (count (:added diff))
       (count (:removed diff))
       (count (:updated diff)))
    
    :field-diff
    (+ (count (:added diff))
       (count (:removed diff))
       (reduce + 0 (map #(count (:changes %)) (:updated diff))))
    
    :full
    (count (:results diff))
    
    0))

(defn original-size
  "Calculate the size of the original result set"
  [results]
  (if (empty? results)
    0
    (* (count results)
       (count (keys (first results))))))

(defn compression-ratio
  "Calculate compression ratio (0.0 = perfect compression, 1.0 = no compression)"
  [diff original-results]
  (let [diff-sz (diff-size diff)
        ;; For compression ratio, we compare against total result count, not field count
        orig-sz (count original-results)]
    (if (zero? orig-sz)
      1.0
      (min 1.0 (/ (double diff-sz) (double orig-sz))))))

(defn should-use-diff?
  "Determine if diff should be used vs full results"
  [diff new-results & [threshold]]
  (let [threshold (or threshold 0.7)
        ;; If all rows are changing, the diff isn't more efficient
        ;; But we still want to send diffs for additions/removals
        ratio (compression-ratio diff new-results)
        has-changes? (pos? (diff-size diff))]
    ;; Always use diff if it has changes and is below threshold
    ;; Special case: empty old or new should always use diff
    (and has-changes?
         (< ratio threshold))))

;; ============================================================================
;; Main API
;; ============================================================================

(defn calculate-diff
  "Calculate the optimal diff between old and new results.
   Options:
   - :mode - :none, :row, :field (default: :field)
   - :id-key - Key to identify rows (auto-detected if not provided)
   - :threshold - Compression threshold (default: 0.7)"
  [old-results new-results & [options]]
  (let [{:keys [mode id-key threshold]
         :or {mode :field
              threshold 0.7}} options
        ;; Use provided id-key or auto-detect
        actual-id-key (or id-key (get-id-key old-results new-results))]
    
    (case mode
      :none
      {:type :full
       :results new-results}
      
      :row
      (let [diff (calculate-row-diff old-results new-results actual-id-key)]
        ;; Always use diff when old or new is empty (all additions or removals)
        (if (or (empty? old-results)
                (empty? new-results)
                (should-use-diff? diff new-results threshold))
          diff
          {:type :full :results new-results}))
      
      :field
      (let [diff (calculate-field-diff old-results new-results actual-id-key)]
        ;; Always use diff when old or new is empty (all additions or removals)
        (if (or (empty? old-results)
                (empty? new-results)
                (should-use-diff? diff new-results threshold))
          diff
          {:type :full :results new-results}))
      
      ;; Default to field diff
      (calculate-diff old-results new-results (assoc options :mode :field)))))

;; ============================================================================
;; Testing Helpers
;; ============================================================================

(defn summarize-diff
  "Create a human-readable summary of a diff"
  [diff]
  (case (:type diff)
    :full
    (str "Full update: " (count (:results diff)) " rows")
    
    :row-diff
    (str "Row diff: +" (count (:added diff))
         " -" (count (:removed diff))
         " ~" (count (:updated diff)))
    
    :field-diff
    (str "Field diff: +" (count (:added diff))
         " -" (count (:removed diff))
         " ~" (count (:updated diff))
         " (" (reduce + 0 (map #(count (:changes %)) (:updated diff)))
         " field changes)")
    
    "Unknown diff type"))