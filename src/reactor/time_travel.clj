(ns reactor.time-travel
  "Time travel and undo/redo functionality for Reactor atoms"
  (:require [clojure.data :as data]))

;; Protocols

(defprotocol ITimeTravel
  "Protocol for time-travel capable atoms"
  (undo! [this] [this session-id] "Step back in history")
  (redo! [this] [this session-id] "Step forward in history")
  (jump-to! [this target] "Jump to timestamp or checkpoint")
  (checkpoint! [this name] "Create named checkpoint")
  (get-history [this] [this opts] "Get history with optional filters")
  (clear-future! [this] "Clear redo stack")
  (branch! [this session-id] "Create session branch")
  (merge-branch! [this session-id] "Merge session branch"))

(defprotocol IHistoryStore
  "Protocol for pluggable history storage"
  (store-entry [this entry] "Store history entry")
  (retrieve-range [this from to] "Get entries in time range")
  (retrieve-last [this n] "Get last n entries")
  (prune-before [this timestamp] "Remove old entries")
  (get-checkpoint [this name] "Get named checkpoint"))

;; Default in-memory store

(deftype MemoryHistoryStore [storage max-entries]
  IHistoryStore
  (store-entry [this entry]
    (swap! storage
           (fn [entries]
             (let [updated (conj entries entry)]
               (if (> (count updated) max-entries)
                 (vec (drop 1 updated))
                 updated)))))
  
  (retrieve-range [this from to]
    (filter #(and (>= (:timestamp %) from)
                  (<= (:timestamp %) to))
            @storage))
  
  (retrieve-last [this n]
    (take-last n @storage))
  
  (prune-before [this timestamp]
    (swap! storage
           (fn [entries]
             (vec (filter #(>= (:timestamp %) timestamp) entries)))))
  
  (get-checkpoint [this name]
    (first (filter #(= (:checkpoint-name %) name) @storage))))

(defn memory-store
  "Create in-memory history store"
  ([] (memory-store 100))
  ([max-entries]
   (->MemoryHistoryStore (atom []) max-entries)))

;; History entry creation

(defn- calculate-patch
  "Calculate minimal patch between two states"
  [old-state new-state]
  (let [[things-only-in-old things-only-in-new _] (data/diff old-state new-state)]
    {:removed things-only-in-old
     :added things-only-in-new}))

(defn create-history-entry
  "Create a history entry with metadata"
  [old-state new-state event metadata]
  (merge
   {:timestamp (System/currentTimeMillis)
    :event event
    :patch (calculate-patch old-state new-state)
    :state-hash (hash new-state)}
   metadata))

;; Time Travel Atom

(deftype TimeTravelAtom [state          ; Current state atom
                         history         ; Vector of past states
                         future          ; Vector of future states (for redo)
                         max-history     ; Max history entries
                         store           ; IHistoryStore implementation
                         sessions        ; Session branches
                         snapshot-every  ; Snapshot frequency
                         metadata]       ; Additional metadata
  
  clojure.lang.IDeref
  (deref [_] @state)
  
  clojure.lang.IAtom
  (reset [this new-value]
    (let [old-value @state
          entry (create-history-entry old-value new-value [:reset new-value] {})]
      (swap! history conj entry)
      (reset! future [])  ; Clear redo stack on new change
      (.store-entry store entry)
      (reset! state new-value)
      new-value))
  
  (swap [this f]
    (.reset this (f @state)))
  
  (swap [this f arg]
    (.reset this (f @state arg)))
  
  (swap [this f arg1 arg2]
    (.reset this (f @state arg1 arg2)))
  
  (swap [this f arg1 arg2 args]
    (.reset this (apply f @state arg1 arg2 args)))
  
  (compareAndSet [this old-value new-value]
    (if (= @state old-value)
      (do (.reset this new-value) true)
      false))
  
  ITimeTravel
  (undo! [this]
    (undo! this nil))
  
  (undo! [this session-id]
    (if (pos? (count @history))
      (let [entries (vec @history)
            last-entry (peek entries)
            current @state
            ;; Find the previous state - look for full snapshots
            prev-state (if (> (count entries) 1)
                         ;; Get state from previous entry or reconstruct
                         (loop [idx (- (count entries) 2)]
                           (if (>= idx 0)
                             (let [entry (nth entries idx)]
                               (if (:state entry)
                                 (:state entry)
                                 (recur (dec idx))))
                             ;; If no snapshot found, use initial state
                             (if-let [first-entry (first entries)]
                               (or (:initial-state first-entry) {})
                               {})))
                         ;; First undo - go to initial state
                         (or (:initial-state metadata) {}))]
        (swap! history pop)
        (swap! future conj (create-history-entry prev-state current [:undo] 
                                                  {:session-id session-id :state current}))
        (reset! state prev-state)
        prev-state)
      @state))
  
  (redo! [this]
    (redo! this nil))
  
  (redo! [this session-id]
    (if-let [next-entry (last @future)]
      (let [current @state
            next-state (if-let [patch (:patch next-entry)]
                         (merge (:added patch)
                                (apply dissoc current (keys (:removed patch))))
                         (:state next-entry))]
        (swap! future pop)
        (swap! history conj (create-history-entry current next-state [:redo]
                                                   {:session-id session-id}))
        (reset! state next-state)
        next-state)
      @state))
  
  (jump-to! [this target]
    (cond
      ;; Jump to timestamp
      (number? target)
      (when-let [entry (first (filter #(= (:timestamp %) target) @history))]
        (reset! state (:state entry))
        @state)
      
      ;; Jump to checkpoint
      (keyword? target)
      (when-let [entry (.get-checkpoint store (name target))]
        (reset! state (:state entry))
        @state)
      
      :else @state))
  
  (checkpoint! [this name]
    (let [entry (create-history-entry nil @state [:checkpoint name]
                                       {:checkpoint-name name
                                        :state @state})]  ; Store full state for checkpoints
      (.store-entry store entry)
      (swap! history conj entry)
      name))
  
  (get-history [this]
    (get-history this {}))
  
  (get-history [this opts]
    (let [entries @history]
      (cond
        (:session-id opts)
        (filter #(= (:session-id %) (:session-id opts)) entries)
        
        (:from opts)
        (filter #(>= (:timestamp %) (:from opts)) entries)
        
        (:last opts)
        (take-last (:last opts) entries)
        
        :else entries)))
  
  (clear-future! [this]
    (reset! future [])
    nil)
  
  (branch! [this session-id]
    (swap! sessions assoc session-id
           {:branched-at (System/currentTimeMillis)
            :base-state @state
            :history []
            :future []}))
  
  (merge-branch! [this session-id]
    (when-let [branch (get @sessions session-id)]
      ;; Apply branch history to main timeline
      (doseq [entry (:history branch)]
        (when-let [event (:event entry)]
          ;; Replay the event
          (case (first event)
            :reset (reset! state (second event))
            :swap (swap! state (second event))
            nil)))
      (swap! sessions dissoc session-id))))

;; Constructor

(defn time-travel-atom
  "Create a time-travel capable atom"
  [initial-value & {:keys [max-history store snapshot-every]
                     :or {max-history 100
                          store (memory-store)
                          snapshot-every 10}}]
  (->TimeTravelAtom
   (atom initial-value)
   (atom [])
   (atom [])
   max-history
   store
   (atom {})
   snapshot-every
   {}))

;; Helper functions

(defn record-change!
  "Record a change for an external atom"
  [time-travel-atom old-val new-val event]
  (when time-travel-atom
    (let [entry (create-history-entry old-val new-val event {})]
      (swap! (:history time-travel-atom) conj entry)
      (reset! (:future time-travel-atom) [])
      (when-let [store (:store time-travel-atom)]
        (.store-entry store entry)))))

(defn with-time-travel
  "Wrap existing atom with time-travel capabilities"
  [existing-atom & opts]
  (apply time-travel-atom @existing-atom opts))

(defn replay!
  "Replay history from one state to another"
  [tt-atom from to & {:keys [speed on-step]
                       :or {speed 1.0}}]
  (let [entries (get-history tt-atom {:from from :to to})]
    (doseq [entry entries]
      (when on-step (on-step entry))
      (Thread/sleep (long (/ 100 speed)))
      (when-let [event (:event entry)]
        (case (first event)
          :reset (reset! tt-atom (second event))
          :swap (swap! tt-atom (second event))
          nil)))))

(defn diff-states
  "Get differences between two timestamps"
  [tt-atom t1 t2]
  (let [history (get-history tt-atom)
        state1 (:state (first (filter #(= (:timestamp %) t1) history)))
        state2 (:state (first (filter #(= (:timestamp %) t2) history)))]
    (data/diff state1 state2)))

;; Macros for nicer syntax

(defmacro undoable
  "Mark a code block as undoable"
  [tt-atom & body]
  `(let [checkpoint# (gensym "undo-point")]
     (checkpoint! ~tt-atom checkpoint#)
     (try
       ~@body
       (catch Exception e#
         (jump-to! ~tt-atom checkpoint#)
         (throw e#)))))