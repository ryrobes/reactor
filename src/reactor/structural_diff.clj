(ns reactor.structural-diff
  "Advanced structural diffing for EDN data and nested structures"
  (:require [clojure.data :as data]
            [clojure.edn :as edn]
            [clojure.walk :as walk]
            [clojure.string :as str]
            [clojure.set :as set]))

;; ============================================================================
;; EDN Diff Operations
;; ============================================================================

(defn edn-string?
  "Check if a value is an EDN string"
  [v]
  (and (string? v)
       (or (str/starts-with? v "{")
           (str/starts-with? v "[")
           (str/starts-with? v "(")
           (str/starts-with? v "#{"))))

(defn safe-read-edn
  "Safely read EDN string, returns original on failure"
  [s]
  (try
    (edn/read-string s)
    (catch Exception _ s)))

(defn compute-structural-diff
  "Compute deep structural diff between two values
   Returns nil if values are equal, otherwise returns diff operations"
  [old-val new-val]
  (cond
    ;; Values are equal
    (= old-val new-val) nil
    
    ;; Both are EDN strings - parse and diff
    (and (edn-string? old-val) (edn-string? new-val))
    (let [old-parsed (safe-read-edn old-val)
          new-parsed (safe-read-edn new-val)]
      (if (and (not= old-parsed old-val) ; Successfully parsed
               (not= new-parsed new-val))
        (let [[removed added _] (data/diff old-parsed new-parsed)]
          (when (or removed added)
            {:type :edn-diff
             :removed removed
             :added added
             :format :edn}))
        ;; Failed to parse, treat as regular value change
        {:type :value-change
         :old old-val
         :new new-val}))
    
    ;; Both are maps - compute field-level diff
    (and (map? old-val) (map? new-val))
    (let [[removed added _] (data/diff old-val new-val)]
      (when (or removed added)
        {:type :map-diff
         :removed removed
         :added added}))
    
    ;; Both are sequential - compute element diff
    (and (sequential? old-val) (sequential? new-val))
    (let [indexed-old (map-indexed vector old-val)
          indexed-new (map-indexed vector new-val)
          max-idx (max (count old-val) (count new-val))
          changes (for [i (range max-idx)
                       :let [old-elem (get (vec old-val) i ::not-found)
                             new-elem (get (vec new-val) i ::not-found)]
                       :when (not= old-elem new-elem)]
                   (cond
                     (= old-elem ::not-found) {:index i :op :add :value new-elem}
                     (= new-elem ::not-found) {:index i :op :remove}
                     :else {:index i :op :update :value new-elem}))]
      (when (seq changes)
        {:type :seq-diff
         :changes changes}))
    
    ;; Different types or simple values - just record the change
    :else
    {:type :value-change
     :old old-val
     :new new-val}))

;; ============================================================================
;; Field-based Diffing with Structural Support
;; ============================================================================

(defn compute-enhanced-field-diff
  "Enhanced field diff that handles structural data"
  [old-row new-row & {:keys [deep-diff? edn-fields debug?] 
                      :or {deep-diff? true 
                           edn-fields #{}
                           debug? false}}]
  (let [all-keys (set/union (set (keys old-row)) (set (keys new-row)))
        changed-fields (reduce 
                        (fn [acc k]
                          (let [old-val (get old-row k ::not-found)
                                new-val (get new-row k ::not-found)]
                            (cond
                              ;; Field added
                              (= old-val ::not-found)
                              (do
                                (when debug?
                                  (println "[STRUCT-DIFF] Field" k "added"))
                                (assoc acc k {:op :add :value new-val}))
                              
                              ;; Field removed
                              (= new-val ::not-found)
                              (do
                                (when debug?
                                  (println "[STRUCT-DIFF] Field" k "removed"))
                                (assoc acc k {:op :remove}))
                              
                              ;; Field unchanged
                              (= old-val new-val)
                              acc
                              
                              ;; Deep structural diff if enabled
                              (and deep-diff? 
                                   (or (contains? edn-fields k)
                                       (edn-string? old-val)
                                       (edn-string? new-val)
                                       (map? old-val)
                                       (map? new-val)
                                       (sequential? old-val)
                                       (sequential? new-val)))
                              (if-let [struct-diff (compute-structural-diff old-val new-val)]
                                (do
                                  (when debug?
                                    (println "[STRUCT-DIFF] Field" k "has structural changes:"
                                            "\n  Type:" (:type struct-diff)
                                            (case (:type struct-diff)
                                              :edn-diff (str "\n  EDN changes - removed:" (keys (:removed struct-diff))
                                                           " added:" (keys (:added struct-diff)))
                                              :map-diff (str "\n  Map changes - removed:" (keys (:removed struct-diff))
                                                           " added:" (keys (:added struct-diff)))
                                              :seq-diff (str "\n  Sequence changes:" (count (:changes struct-diff)) "items")
                                              "")))
                                  (assoc acc k {:op :structural-update 
                                              :diff struct-diff}))
                                acc)
                              
                              ;; Simple field change
                              :else
                              (do
                                (when debug?
                                  (println "[STRUCT-DIFF] Field" k "simple update"))
                                (assoc acc k {:op :update :value new-val})))))
                        {}
                        all-keys)]
    (when (seq changed-fields)
      changed-fields)))

;; ============================================================================
;; Diff Application
;; ============================================================================

(defn apply-structural-diff
  "Apply a structural diff to a value"
  [current-val diff]
  (case (:type diff)
    :value-change (:new diff)
    
    :edn-diff 
    (let [parsed (if (string? current-val)
                   (safe-read-edn current-val)
                   current-val)
          ;; Merge changes
          result (merge (apply dissoc parsed (keys (:removed diff)))
                       (:added diff))]
      ;; Return as EDN string if original was string
      (if (string? current-val)
        (pr-str result)
        result))
    
    :map-diff
    (merge (apply dissoc current-val (keys (:removed diff)))
           (:added diff))
    
    :seq-diff
    (let [result (vec current-val)]
      (reduce (fn [v change]
                (case (:op change)
                  :add (if (< (:index change) (count v))
                        (assoc v (:index change) (:value change))
                        (conj v (:value change)))
                  :remove (vec (concat (subvec v 0 (:index change))
                                      (subvec v (inc (:index change)))))
                  :update (assoc v (:index change) (:value change))))
              result
              (sort-by :index (:changes diff))))
    
    ;; Default - return original
    current-val))

(defn apply-enhanced-field-changes
  "Apply enhanced field changes including structural diffs"
  [row field-changes]
  (reduce-kv (fn [r field change]
              (case (:op change)
                :add (assoc r field (:value change))
                :update (assoc r field (:value change))
                :remove (dissoc r field)
                :structural-update 
                (assoc r field (apply-structural-diff (get r field) (:diff change)))
                r))
            row
            field-changes))

;; ============================================================================
;; Compression Analysis
;; ============================================================================

(defn calculate-diff-size
  "Calculate the size of a diff for compression ratio analysis"
  [diff]
  (walk/postwalk
   (fn [x]
     (cond
       (string? x) (count x)
       (keyword? x) 1
       (number? x) 8  ; Assume 8 bytes for numbers
       (boolean? x) 1
       (nil? x) 1
       (map? x) (reduce + (vals x))
       (sequential? x) (reduce + x)
       :else 1))
   diff))

(defn compression-ratio
  "Calculate compression ratio for enhanced diffs"
  [original-data diff-data]
  (let [original-size (calculate-diff-size original-data)
        diff-size (calculate-diff-size diff-data)]
    (if (zero? original-size)
      1.0
      (/ diff-size original-size))))