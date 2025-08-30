(ns reactor.temporal-cache
  "LRU cache for temporal SQL queries - immutable results at specific timestamps"
  (:require [clojure.tools.logging :as log]))

;; ============================================================================
;; Configuration
;; ============================================================================

(def ^:dynamic *cache-enabled* true)  ; Can be overridden with binding
(def max-cache-size 500)              ; Maximum number of cached queries
(def cache-ttl-ms (* 60 60 1000))     ; 1 hour TTL (just in case)

;; ============================================================================
;; Cache Implementation
;; ============================================================================

(defonce temporal-cache 
  (atom {:entries {}        ; {cache-key -> {:results ... :timestamp ...}}
         :access-order []})) ; LRU tracking

(defn make-cache-key
  "Create a cache key from resolved SQL and timestamp"
  [resolved-sql as-of params]
  ;; Include params in key if they exist
  (str (hash [resolved-sql as-of params])))

(defn evict-lru!
  "Evict least recently used entries if cache is too large"
  []
  (let [{:keys [entries access-order]} @temporal-cache]
    (when (> (count entries) max-cache-size)
      (let [;; Take oldest 20% of entries to evict
            to-evict (take (/ max-cache-size 5) access-order)
            remaining (drop (/ max-cache-size 5) access-order)]
        (swap! temporal-cache
               (fn [cache]
                 {:entries (apply dissoc (:entries cache) to-evict)
                  :access-order (vec remaining)}))
        (log/debug "[TEMPORAL-CACHE] Evicted" (count to-evict) "LRU entries")))))

(defn update-access-order!
  "Update LRU access order for a cache key"
  [cache-key]
  (swap! temporal-cache
         (fn [cache]
           (let [;; Remove from current position and add to end
                 new-order (conj 
                           (filterv #(not= % cache-key) (:access-order cache))
                           cache-key)]
             (assoc cache :access-order new-order)))))

(defn get-cached
  "Get cached results for a temporal query"
  [cache-key]
  (when *cache-enabled*
    (when-let [entry (get-in @temporal-cache [:entries cache-key])]
      ;; Check TTL
      (let [age (- (System/currentTimeMillis) (:cached-at entry))]
        (if (< age cache-ttl-ms)
          (do
            (update-access-order! cache-key)
            (log/debug "[TEMPORAL-CACHE] Cache HIT for key:" cache-key)
            (:results entry))
          (do
            ;; Expired, remove it
            (swap! temporal-cache update :entries dissoc cache-key)
            (swap! temporal-cache update :access-order #(filterv (fn [k] (not= k cache-key)) %))
            (log/debug "[TEMPORAL-CACHE] Cache entry expired for key:" cache-key)
            nil))))))

(defn cache-results!
  "Cache results for a temporal query"
  [cache-key results]
  (when (and *cache-enabled* results)
    (swap! temporal-cache
           (fn [cache]
             (-> cache
                 (assoc-in [:entries cache-key] 
                          {:results results
                           :cached-at (System/currentTimeMillis)})
                 (update :access-order #(conj (filterv (fn [k] (not= k cache-key)) %) cache-key)))))
    (evict-lru!)  ; Check if we need to evict
    (log/debug "[TEMPORAL-CACHE] Cached results for key:" cache-key)))

(defn clear-cache!
  "Clear the entire cache"
  []
  (reset! temporal-cache {:entries {} :access-order []})
  (log/info "[TEMPORAL-CACHE] Cache cleared"))

(defn cache-stats
  "Get cache statistics"
  []
  (let [{:keys [entries access-order]} @temporal-cache]
    {:size (count entries)
     :max-size max-cache-size
     :enabled *cache-enabled*
     :oldest-key (first access-order)
     :newest-key (last access-order)}))

(defn set-cache-enabled!
  "Enable or disable the cache"
  [enabled]
  (alter-var-root #'*cache-enabled* (constantly enabled))
  (log/info "[TEMPORAL-CACHE] Cache" (if enabled "enabled" "disabled")))

;; ============================================================================
;; Pipeline Integration
;; ============================================================================

(defn check-temporal-cache
  "Check cache for temporal query results - pipeline stage"
  [ctx]
  (if (or (:error ctx)
          (not (:as-of ctx))        ; Only cache temporal queries
          (not *cache-enabled*)
          (:is-mutation? ctx))       ; Never cache mutations
    ctx
    (let [cache-key (make-cache-key (:resolved-sql ctx) 
                                   (:as-of ctx) 
                                   (:params ctx))]
      (if-let [cached-results (get-cached cache-key)]
        (do
          (log/info "[TEMPORAL-CACHE] Cache HIT - bypassing database"
                   "\n  SQL:" (if (> (count (:resolved-sql ctx)) 80)
                               (str (subs (:resolved-sql ctx) 0 80) "...")
                               (:resolved-sql ctx))
                   "\n  Timestamp:" (:as-of ctx))
          ;; Short-circuit with cached results
          (assoc ctx 
                 :results cached-results
                 :from-cache? true
                 :cache-key cache-key
                 :skip-remaining-stages? true))  ; Signal to skip DB execution
        ;; No cache hit, continue with pipeline
        (assoc ctx :cache-key cache-key)))))

(defn cache-temporal-results
  "Cache results if this was a temporal query - pipeline stage"
  [ctx]
  (if (and (not (:error ctx))
           (:as-of ctx)              ; Temporal query
           (:cache-key ctx)          ; Cache key was generated
           (:results ctx)            ; Have results to cache
           (not (:from-cache? ctx))  ; Not already from cache
           *cache-enabled*)
    (do
      (cache-results! (:cache-key ctx) (:results ctx))
      ctx)
    ctx))