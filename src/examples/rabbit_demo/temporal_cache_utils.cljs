(ns examples.rabbit-demo.temporal-cache-utils
  "Browser-side local storage cache for temporal count queries"
  (:require [clojure.string :as str]))

;; Configuration
(def ^:const cache-prefix "reactor_temporal:")
(def ^:const max-cache-entries 5000)
(def ^:const cache-ttl-days 30)  ; Cache entries for 30 days
(def ^:const cache-version "v1")  ; Bump to invalidate old caches

(defn build-cache-key
  "Build a versioned cache key for temporal queries"
  [sql timestamp]
  (let [normalized-sql (-> sql
                           (str/replace #"\s+" " ")  ; Normalize whitespace
                           (str/trim)
                           (str/lower-case))
        ;; Include version to allow cache invalidation
        key-parts [cache-version normalized-sql timestamp]]
    (str cache-prefix (hash key-parts))))

(defn parse-cache-value
  "Parse a cached value with metadata"
  [stored-str]
  (try
    (let [parsed (js/JSON.parse stored-str)]
      {:count (.-count parsed)
       :timestamp (.-timestamp parsed)
       :version (.-version parsed)})
    (catch js/Error _ nil)))

(defn serialize-cache-value
  "Serialize a count with metadata for storage"
  [count]
  (js/JSON.stringify
   #js {:count count
        :timestamp (.getTime (js/Date.))
        :version cache-version}))

(defn is-cache-entry-valid?
  "Check if a cache entry is still valid"
  [{:keys [timestamp version]}]
  (and (= version cache-version)  ; Version matches
       timestamp  ; Has timestamp
       (< (- (.getTime (js/Date.)) timestamp)
          (* cache-ttl-days 24 60 60 1000))))  ; Within TTL

(declare manage-cache-size!)

(defn get-cached-temporal-count
  "Get a cached temporal count if valid"
  [sql timestamp]
  (try
    (when-let [stored (.getItem js/localStorage (build-cache-key sql timestamp))]
      (when-let [parsed (parse-cache-value stored)]
        (when (is-cache-entry-valid? parsed)
          (:count parsed))))
    (catch js/Error _ nil)))

(defn set-cached-temporal-count!
  "Cache a temporal count with metadata"
  [sql timestamp count]
  (try
    (.setItem js/localStorage
              (build-cache-key sql timestamp)
              (serialize-cache-value count))
    ;; Periodically check cache size
    (when (zero? (rand-int 100))  ; 1% chance
      (manage-cache-size!))
    true
    (catch js/Error _ false)))

(defn get-cache-entries
  "Get all temporal cache entries with metadata"
  []
  (try
    (let [storage js/localStorage
          entries (atom [])]
      (doseq [i (range (.-length storage))]
        (when-let [key (.key storage i)]
          (when (str/starts-with? key cache-prefix)
            (when-let [value (.getItem storage key)]
              (when-let [parsed (parse-cache-value value)]
                (swap! entries conj {:key key
                                    :data parsed}))))))
      @entries)
    (catch js/Error _ [])))

(defn manage-cache-size!
  "Remove old/invalid entries if cache is too large"
  []
  (try
    (let [entries (get-cache-entries)
          valid-entries (filter #(is-cache-entry-valid? (:data %)) entries)]
      ;; Remove invalid entries
      (doseq [{:keys [key data]} entries]
        (when-not (is-cache-entry-valid? data)
          (.removeItem js/localStorage key)))
      ;; If still too many, remove oldest
      (when (> (count valid-entries) max-cache-entries)
        (let [sorted (sort-by #(get-in % [:data :timestamp]) valid-entries)
              to-remove (take (- (count valid-entries) max-cache-entries) sorted)]
          (doseq [{:keys [key]} to-remove]
            (.removeItem js/localStorage key)))))
    (catch js/Error _ nil)))

(defn clear-temporal-cache!
  "Clear all temporal cache entries"
  []
  (try
    (let [storage js/localStorage
          keys-to-remove (atom [])]
      ;; Collect keys first (can't modify while iterating)
      (doseq [i (range (.-length storage))]
        (when-let [key (.key storage i)]
          (when (str/starts-with? key cache-prefix)
            (swap! keys-to-remove conj key))))
      ;; Remove them
      (doseq [key @keys-to-remove]
        (.removeItem storage key))
      (count @keys-to-remove))
    (catch js/Error _ 0)))

(defn get-cache-stats
  "Get statistics about the cache"
  []
  (let [entries (get-cache-entries)
        valid-entries (filter #(is-cache-entry-valid? (:data %)) entries)]
    {:total (count entries)
     :valid (count valid-entries)
     :invalid (- (count entries) (count valid-entries))
     :size-estimate (* (count entries) 200)}))  ; Rough estimate of bytes

;; Initialize - clean up on load
(when (zero? (rand-int 10))  ; 10% chance to clean on page load
  (manage-cache-size!))