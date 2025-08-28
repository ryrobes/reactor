(ns reactor.temporal-cache
  "Cache for immutable temporal queries, especially row counts"
  (:require [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [reactor.log :as log]
            [clojure.string :as str]))

;; In-memory cache for temporal queries
(defonce temporal-cache (atom {}))

;; Cache file location
(def cache-file "temporal_row_count_cache.nippy")

(defn is-temporal-count-query?
  "Check if a query is a temporal count query that can be cached forever"
  [sql]
  (when sql
    ;; Simple but reliable checks:
    ;; 1. Must start with SELECT COUNT(*) AS CNT FROM (
    ;; 2. Must contain FOR SYSTEM_TIME AS OF TIMESTAMP
    ;; 3. Must end with ) AS SUBQ
    (let [normalized-sql (-> sql str/trim str/upper-case)]
      (and 
        ;; Starts with SELECT COUNT(*) AS CNT FROM (
        (str/starts-with? normalized-sql "SELECT COUNT(*) AS CNT FROM (")
        ;; Contains temporal clause
        (str/includes? normalized-sql "FOR SYSTEM_TIME AS OF TIMESTAMP")
        ;; Ends with ) AS SUBQ
        (str/ends-with? normalized-sql ") AS SUBQ")))))

(defn extract-cache-key
  "Extract a stable cache key from a temporal count query"
  [sql]
  ;; Use the entire SQL as the key since it includes the timestamp and table
  ;; This ensures uniqueness for each specific temporal query
  (when (is-temporal-count-query? sql)
    ;; Normalize whitespace for consistent keys
    (-> sql
        (clojure.string/replace #"\s+" " ")
        (clojure.string/trim)
        (clojure.string/lower-case))))

(defn load-cache!
  "Load the cache from disk on startup"
  []
  (try
    (when (.exists (io/file cache-file))
      (let [data (nippy/thaw-from-file cache-file)]
        (reset! temporal-cache data)
        (log/info "[TEMPORAL-CACHE] Loaded" (count data) "cached queries from disk")))
    (catch Exception e
      (log/warn "[TEMPORAL-CACHE] Could not load cache file:" (.getMessage e))
      (reset! temporal-cache {}))))

(defn save-cache!
  "Persist the cache to disk"
  []
  (try
    (nippy/freeze-to-file cache-file @temporal-cache)
    (log/debug "[TEMPORAL-CACHE] Saved" (count @temporal-cache) "queries to disk")
    (catch Exception e
      (log/error "[TEMPORAL-CACHE] Failed to save cache:" (.getMessage e)))))

(defn get-cached
  "Get a cached result if it exists"
  [sql]
  (log/debug "[TEMPORAL-CACHE] Checking cache for query:" 
             (if (> (count (str sql)) 80) 
               (str (subs (str sql) 0 80) "...")
               sql))
  (if (is-temporal-count-query? sql)
    (when-let [cache-key (extract-cache-key sql)]
      (let [cached (get @temporal-cache cache-key)]
        (if cached
          (do
            (log/debug "[TEMPORAL-CACHE] ✅ CACHE HIT for key:" 
                      (if (> (count cache-key) 60)
                        (str (subs cache-key 0 60) "...")
                        cache-key))
            cached)
          (do
            (log/debug "[TEMPORAL-CACHE] ❌ CACHE MISS for key:"
                      (if (> (count cache-key) 60)
                        (str (subs cache-key 0 60) "...")
                        cache-key))
            nil))))
    (do
      (log/debug "[TEMPORAL-CACHE] Not a temporal count query, skipping cache")
      nil)))

(defn cache-result!
  "Cache a query result if it's a temporal count query"
  [sql result]
  (log/debug "[TEMPORAL-CACHE] Attempting to cache result for query:"
             (if (> (count (str sql)) 80)
               (str (subs (str sql) 0 80) "...")
               sql))
  (if (is-temporal-count-query? sql)
    (let [cache-key (extract-cache-key sql)]
      (swap! temporal-cache assoc cache-key result)
      ;; Save to disk periodically (every 10 new entries)
      (when (zero? (mod (count @temporal-cache) 10))
        (future (save-cache!)))
      (log/debug "[TEMPORAL-CACHE] 📦 CACHED temporal count query. Key:"
                (if (> (count cache-key) 60)
                  (str (subs cache-key 0 60) "...")
                  cache-key)
                "Total cached:" (count @temporal-cache)))
    (log/debug "[TEMPORAL-CACHE] Not a temporal count query, not caching"))
  result)

(defn clear-cache!
  "Clear the entire cache (useful for testing or maintenance)"
  []
  (reset! temporal-cache {})
  (save-cache!)
  (log/info "[TEMPORAL-CACHE] Cache cleared"))

;; Initialize cache on namespace load
(load-cache!)

;; Ensure cache is saved on shutdown
(.addShutdownHook (Runtime/getRuntime)
  (Thread. (fn []
            (log/info "[TEMPORAL-CACHE] Shutting down - saving cache...")
            (save-cache!))))