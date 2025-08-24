(ns reactor.structural-diff
  "Client-side structural diffing for EDN data and nested structures"
  (:require [clojure.data :as data]
            [cljs.reader :as reader]
            [clojure.walk :as walk]
            [clojure.string :as str]
            [clojure.set :as set]))

;; ============================================================================
;; EDN Diff Operations (ClojureScript version)
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
    (reader/read-string s)
    (catch :default _ s)))

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
;; Diff Application (ClojureScript version)
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
;; UI Helpers for Visualizing Diffs
;; ============================================================================

(defn diff->hiccup
  "Convert a diff to Hiccup for visualization"
  [diff]
  (case (:type diff)
    :value-change
    [:div.diff-value-change
     [:span.old {:style {:color "red" :text-decoration "line-through"}} 
      (str (:old diff))]
     " → "
     [:span.new {:style {:color "green"}} 
      (str (:new diff))]]
    
    :edn-diff
    [:div.diff-edn
     (when (:removed diff)
       [:div.removed {:style {:color "red"}}
        "Removed: " (pr-str (:removed diff))])
     (when (:added diff)
       [:div.added {:style {:color "green"}}
        "Added: " (pr-str (:added diff))])]
    
    :map-diff
    [:div.diff-map
     (when (:removed diff)
       [:div.removed {:style {:color "red"}}
        "Removed fields: " (str (keys (:removed diff)))])
     (when (:added diff)
       [:div.added {:style {:color "green"}}
        "Added fields: " (str (keys (:added diff)))])]
    
    :seq-diff
    [:div.diff-seq
     (for [change (:changes diff)]
       [:div {:key (:index change)}
        "Index " (:index change) ": "
        (case (:op change)
          :add [:span {:style {:color "green"}} "Added " (pr-str (:value change))]
          :remove [:span {:style {:color "red"}} "Removed"]
          :update [:span {:style {:color "orange"}} "Updated to " (pr-str (:value change))])])]
    
    [:div "Unknown diff type"]))