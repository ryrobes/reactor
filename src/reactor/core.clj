(ns reactor.core
  (:require [clojure.core.cache :as cache]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [reactor.time-travel-simple :as tt])
  (:import [java.util.concurrent ScheduledExecutorService Executors TimeUnit]
           [java.util.concurrent.locks ReentrantReadWriteLock]
           [java.lang.ref WeakReference]))

(defprotocol IReactive
  (add-watch! [this key f])
  (remove-watch! [this key])
  (notify-watches [this old-val new-val]))

(defprotocol ISubscribable
  (subscribe! [this path-or-fn callback opts])
  (unsubscribe! [this key]))

(defprotocol IRuleEngine
  (def-rule [this rule-key path-or-fn cond-fn action-fn])
  (enable-rule! [this key])
  (disable-rule! [this key])
  (get-rule [this key]))

(defprotocol IPersistable
  (persist! [this file-path])
  (rehydrate! [this file-path]))

(defprotocol ICursor
  (cursor [this path])
  (get-path [this]))

(def ^:dynamic *deps* nil)
(def ^:dynamic *current-reaction* nil)

(defn track-dep! [ratom path]
  (when *deps*
    (swap! *deps* conj {:ratom ratom :path path})))

(deftype Cursor [ratom path ^:volatile-mutable cached-val ^:volatile-mutable cached-state]
  clojure.lang.IDeref
  (deref [_]
    (let [current-state @ratom
          val (get-in current-state path)]
      (track-dep! ratom path)
      (when (not= current-state cached-state)
        (set! cached-val val)
        (set! cached-state current-state))
      cached-val))
  
  ICursor
  (get-path [_] path)
  
  clojure.lang.IRef
  (setValidator [_ vf] (throw (UnsupportedOperationException. "Cursor does not support validators")))
  (getValidator [_] nil)
  (getWatches [_] {})
  (addWatch [this key f]
    (add-watch! ratom (keyword (str "cursor-" (hash this) "-" (name key)))
                (fn [_ _ old-state new-state]
                  (let [old-val (get-in old-state path)
                        new-val (get-in new-state path)]
                    (when (not= old-val new-val)
                      (f key this old-val new-val))))))
  (removeWatch [this key]
    (remove-watch ratom (keyword (str "cursor-" (hash this) "-" (name key))))))

(deftype Subscription [ratom id path-or-fn callback deps ^:volatile-mutable last-val]
  clojure.lang.IFn
  (invoke [this]
    (let [result (if (fn? path-or-fn)
                   (binding [*deps* (atom #{})]
                     (let [res (path-or-fn)]
                       (reset! deps @*deps*)
                       res))
                   (get-in @ratom path-or-fn))]
      (when (not= result last-val)
        (let [old last-val]
          (set! last-val result)
          (callback old result)))
      result)))

(deftype Rule [id path-or-fn cond-fn action-fn enabled? deps]
  clojure.lang.IFn
  (invoke [this old-val new-val]
    (when @enabled?
      (when (or (nil? cond-fn) (cond-fn new-val))
        (action-fn old-val new-val)))))

(deftype RAtom [state 
                watches 
                subscriptions 
                rules 
                config 
                cache 
                ^ReentrantReadWriteLock lock
                path-trie
                time-travel]
  
  clojure.lang.IDeref
  (deref [this]
    (track-dep! this [])
    @state)
  
  clojure.lang.IAtom
  (swap [this f]
    (let [old-val @state
          new-val (swap! state f)]
      (when time-travel
        (tt/record-change! time-travel old-val new-val [:swap f]))
      (notify-watches this old-val new-val)
      new-val))
  (swap [this f arg]
    (let [old-val @state
          new-val (swap! state f arg)]
      (when time-travel
        (tt/record-change! time-travel old-val new-val [:swap f arg]))
      (notify-watches this old-val new-val)
      new-val))
  (swap [this f arg1 arg2]
    (let [old-val @state
          new-val (swap! state f arg1 arg2)]
      (when time-travel
        (tt/record-change! time-travel old-val new-val [:swap f arg1 arg2]))
      (notify-watches this old-val new-val)
      new-val))
  (swap [this f arg1 arg2 args]
    (let [old-val @state
          new-val (apply swap! state f arg1 arg2 args)]
      (when time-travel
        (tt/record-change! time-travel old-val new-val [:swap f arg1 arg2 args]))
      (notify-watches this old-val new-val)
      new-val))
  (compareAndSet [this old-val new-val]
    (let [current @state]
      (if (= current old-val)
        (let [result (compare-and-set! state current new-val)]
          (when result
            (notify-watches this old-val new-val))
          result)
        false)))
  (reset [this new-val]
    (let [old-val @state]
      (reset! state new-val)
      (when time-travel
        (tt/record-change! time-travel old-val new-val [:reset new-val]))
      (notify-watches this old-val new-val)
      new-val))
  
  clojure.lang.IRef
  (setValidator [_ vf] 
    (set-validator! state vf))
  (getValidator [_] 
    (get-validator state))
  (getWatches [_] 
    @watches)
  (addWatch [this key f]
    (add-watch! this key f))
  (removeWatch [this key]
    (remove-watch! this key))
  
  IReactive
  (add-watch! [this key f]
    (swap! watches assoc key f)
    this)
  
  (remove-watch! [this key]
    (swap! watches dissoc key)
    this)
  
  (notify-watches [this old-val new-val]
    (doseq [[key watch-fn] @watches]
      (try
        (watch-fn key this old-val new-val)
        (catch Exception e
          (println "Error in watch" key ":" (.getMessage e))))))
  
  ISubscribable
  (subscribe! [this path-or-fn callback opts]
    (let [sub-id (or (:key opts) (keyword (gensym "sub-")))
          sub (->Subscription this sub-id path-or-fn callback (atom #{}) nil)]
      (swap! subscriptions assoc sub-id sub)
      (add-watch! this sub-id
                  (fn [_ _ old-state new-state]
                    (if (fn? path-or-fn)
                      (sub)
                      (let [old-val (get-in old-state path-or-fn)
                            new-val (get-in new-state path-or-fn)]
                        (when (not= old-val new-val)
                          (callback old-val new-val))))))
      (when-not (:lazy opts)
        (sub))
      sub-id))
  
  (unsubscribe! [this key]
    (swap! subscriptions dissoc key)
    (remove-watch! this key))
  
  IRuleEngine
  (def-rule [this rule-key path-or-fn cond-fn action-fn]
    (let [rule (->Rule rule-key path-or-fn cond-fn action-fn (atom true) (atom #{}))]
      (swap! rules assoc rule-key rule)
      (subscribe! this path-or-fn 
                  (fn [old-val new-val]
                    (rule old-val new-val))
                  {:key (keyword (str "rule-" (name rule-key)))
                   :lazy true})
      rule-key))
  
  (enable-rule! [this key]
    (when-let [rule (get @rules key)]
      (reset! (.enabled? rule) true)))
  
  (disable-rule! [this key]
    (when-let [rule (get @rules key)]
      (reset! (.enabled? rule) false)))
  
  (get-rule [this key]
    (get @rules key))
  
  IPersistable
  (persist! [this file-path]
    (let [data @state]
      (with-open [writer (io/writer file-path)]
        (binding [*out* writer]
          (prn data))))
    file-path)
  
  (rehydrate! [this file-path]
    (when (.exists (io/file file-path))
      (with-open [reader (io/reader file-path)]
        (let [data (edn/read (java.io.PushbackReader. reader))]
          (reset! state data))))
    this)
  
  ICursor
  (cursor [this path]
    (->Cursor this path nil nil))
  
  clojure.lang.IFn
  (invoke [this]
    @this)
  (invoke [this path]
    (get-in @this path)))

(defn ratom
  ([initial-state]
   (ratom initial-state {}))
  ([initial-state config]
   (let [time-travel (when (:history config)
                       (tt/create-time-travel 
                        initial-state
                        :max-history (or (:max-history config) 100)))]
     (->RAtom (atom initial-state)
              (atom {})
              (atom {})
              (atom {})
              config
              (atom (cache/lru-cache-factory {} :threshold (or (:cache-size config) 1000)))
              (ReentrantReadWriteLock.)
              (atom {})
              time-travel))))

(defn cursor
  [ratom path]
  (.cursor ratom path))

;; Time Travel API
(defn undo! 
  "Undo last change to the ratom"
  ([ratom]
   (undo! ratom nil))
  ([ratom session-id]
   (when-let [tt (.-time-travel ratom)]
     (let [old-state @(.-state ratom)
           new-state (tt/undo! tt session-id)]
       (when new-state
         (reset! (.-state ratom) new-state)
         (.notify-watches ratom old-state new-state))
       new-state))))

(defn redo!
  "Redo previously undone change"
  ([ratom]
   (redo! ratom nil))
  ([ratom session-id]
   (when-let [tt (.-time-travel ratom)]
     (let [old-state @(.-state ratom)
           new-state (tt/redo! tt session-id)]
       (when new-state
         (reset! (.-state ratom) new-state)
         (.notify-watches ratom old-state new-state))
       new-state))))

(defn jump-to!
  "Jump to specific point in history"
  [ratom target]
  (when-let [tt (.-time-travel ratom)]
    (let [old-state @(.-state ratom)
          new-state (tt/jump-to! tt target)]
      (when new-state
        (reset! (.-state ratom) new-state)
        (.notify-watches ratom old-state new-state))
      new-state)))

(defn checkpoint!
  "Create named checkpoint in history"
  [ratom name]
  (when-let [tt (.-time-travel ratom)]
    (tt/checkpoint! tt name)))

(defn get-history
  "Get history of changes"
  ([ratom]
   (get-history ratom {}))
  ([ratom opts]
   (when-let [tt (.-time-travel ratom)]
     (tt/get-history tt opts))))

(defn subscribe!
  ([ratom path-or-fn callback]
   (subscribe! ratom path-or-fn callback {}))
  ([ratom path-or-fn callback opts]
   (.subscribe! ratom path-or-fn callback opts)))

(defn unsubscribe!
  [ratom key]
  (.unsubscribe! ratom key))

(defn def-rule
  ([ratom rule-key path-or-fn action-fn]
   (def-rule ratom rule-key path-or-fn nil action-fn))
  ([ratom rule-key path-or-fn cond-fn action-fn]
   (.def-rule ratom rule-key path-or-fn cond-fn action-fn)))

(defn enable-rule!
  [ratom key]
  (.enable-rule! ratom key))

(defn disable-rule!
  [ratom key]
  (.disable-rule! ratom key))

(defn persist!
  [ratom file-path]
  (.persist! ratom file-path))

(defn rehydrate!
  [ratom file-path]
  (.rehydrate! ratom file-path))

(defn cas!
  [ratom old-val new-val]
  (.compareAndSet ratom old-val new-val))

(deftype TimeRAtom [ratom scheduler interval-ms]
  clojure.lang.IDeref
  (deref [_]
    @ratom)
  
  clojure.lang.IRef
  (setValidator [_ vf] (set-validator! ratom vf))
  (getValidator [_] (get-validator ratom))
  (getWatches [_] (.getWatches ratom))
  (addWatch [_ key f] (add-watch ratom key f))
  (removeWatch [_ key] (remove-watch ratom key))
  
  ISubscribable
  (subscribe! [_ path-or-fn callback opts]
    (subscribe! ratom path-or-fn callback opts))
  (unsubscribe! [_ key]
    (unsubscribe! ratom key))
  
  java.io.Closeable
  (close [_]
    (.shutdown scheduler)))

(defn update-time-state [state interval]
  (let [now (System/currentTimeMillis)
        instant (java.time.Instant/ofEpochMilli now)
        zdt (java.time.ZonedDateTime/ofInstant instant (java.time.ZoneId/systemDefault))]
    (case interval
      :second {:now now
               :second (.getSecond zdt)
               :minute (.getMinute zdt)
               :hour (.getHour zdt)
               :day (.getDayOfMonth zdt)
               :month (.getMonthValue zdt)
               :year (.getYear zdt)}
      :minute {:now now
               :minute (.getMinute zdt)
               :hour (.getHour zdt)
               :day (.getDayOfMonth zdt)
               :month (.getMonthValue zdt)
               :year (.getYear zdt)}
      :hour {:now now
             :hour (.getHour zdt)
             :day (.getDayOfMonth zdt)
             :month (.getMonthValue zdt)
             :year (.getYear zdt)}
      {:now now})))

(defn time-ratom
  [config]
  (let [interval (or (:interval config) :minute)
        interval-ms (case interval
                      :second 1000
                      :minute 60000
                      :hour 3600000
                      60000)
        initial-state (update-time-state {} interval)
        r (ratom initial-state)
        scheduler (Executors/newScheduledThreadPool 1)]
    (.scheduleAtFixedRate scheduler
                          (fn []
                            (let [old-state @r
                                  new-state (update-time-state old-state interval)]
                              (when (not= old-state new-state)
                                (reset! r new-state))))
                          interval-ms
                          interval-ms
                          TimeUnit/MILLISECONDS)
    (->TimeRAtom r scheduler interval-ms)))

(defn reaction
  [f]
  (let [deps (atom #{})
        ratom (ratom nil)
        compute (fn []
                  (binding [*deps* deps]
                    (let [result (f)]
                      (reset! ratom result)
                      result)))]
    (compute)
    (doseq [{:keys [ratom path]} @deps]
      (add-watch ratom (keyword (gensym "reaction-"))
                 (fn [_ _ _ _]
                   (compute))))
    ratom))