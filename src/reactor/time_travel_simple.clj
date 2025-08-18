(ns reactor.time-travel-simple
  "Simplified time travel with full snapshots for reliability")

(defprotocol ITimeTravel
  (undo! [this] [this session-id])
  (redo! [this] [this session-id])
  (jump-to! [this target])
  (checkpoint! [this name])
  (get-history [this] [this opts])
  (record-change! [this old-val new-val event]))

(defrecord TimeTravelState [state history future checkpoints max-history])

(defn create-time-travel
  "Create a time travel state manager"
  [initial-state & {:keys [max-history] :or {max-history 100}}]
  (->TimeTravelState
   (atom initial-state)
   (atom [{:timestamp 0 :state initial-state :event [:init]}])
   (atom [])
   (atom {})
   max-history))

(extend-type TimeTravelState
  ITimeTravel
  
  (undo! 
    ([this] (undo! this nil))
    ([this session-id]
     (let [history-vec @(:history this)]
       (when (> (count history-vec) 1)
         ;; Keep at least one entry (initial state)
         (let [current-entry (last history-vec)
               prev-entry (nth history-vec (- (count history-vec) 2))]
           (swap! (:history this) pop)
           (swap! (:future this) conj current-entry)
           ;; Update internal state AND return the previous state
           (reset! (:state this) (:state prev-entry))
           (:state prev-entry))))))
  
  (redo!
    ([this] (redo! this nil))
    ([this session-id]
     (when-let [next-entry (last @(:future this))]
       (swap! (:future this) pop)
       (swap! (:history this) conj next-entry)
       ;; Update internal state AND return the next state
       (reset! (:state this) (:state next-entry))
       (:state next-entry))))
  
  (jump-to! [this target]
    (cond
      ;; Jump to checkpoint
      (keyword? target)
      (when-let [checkpoint (get @(:checkpoints this) target)]
        (reset! (:state this) (:state checkpoint))
        (:state checkpoint))
      
      ;; Jump to index
      (number? target)
      (let [history-vec @(:history this)
            future-vec @(:future this)
            history-count (count history-vec)
            current-index (dec history-count)]
        (cond
          ;; Target is in history
          (< target history-count)
          (let [moves-back (- current-index target)]
            ;; Move entries from history to future
            (dotimes [_ moves-back]
              (when (> (count @(:history this)) 1)
                (let [entry (last @(:history this))]
                  (swap! (:history this) pop)
                  (swap! (:future this) conj entry))))
            ;; Update and return the state at target position
            (let [target-state (:state (nth @(:history this) target))]
              (reset! (:state this) target-state)
              target-state))
          
          ;; Target is in future
          (>= target history-count)
          (let [future-index (- target history-count)
                moves-forward (inc future-index)]
            ;; Move entries from future to history
            (dotimes [_ moves-forward]
              (when-let [entry (last @(:future this))]
                (swap! (:future this) pop)
                (swap! (:history this) conj entry)))
            ;; Update and return the state at the new position
            (let [new-state (:state (last @(:history this)))]
              (reset! (:state this) new-state)
              new-state))
          
          :else nil))))
  
  (checkpoint! [this name]
    (let [current-state @(:state this)
          checkpoint {:timestamp (System/currentTimeMillis)
                      :state current-state
                      :name name}]
      (swap! (:checkpoints this) assoc name checkpoint)
      name))
  
  (get-history
    ([this] @(:history this))
    ([this opts]
     (let [history @(:history this)]
       (cond
         (:last opts) (take-last (:last opts) history)
         :else history))))
  
  (record-change! [this old-val new-val event]
    ;; Only record if actual data changed (not just time-travel metadata)
    (let [old-without-tt (dissoc old-val :time-travel)
          new-without-tt (dissoc new-val :time-travel)]
      (when (not= old-without-tt new-without-tt)
        (let [entry {:timestamp (System/currentTimeMillis)
                     :state new-val
                     :event event
                     :prev-state old-val}]
          ;; Clear future on new change
          (reset! (:future this) [])
          ;; Add to history with size limit
          (swap! (:history this) 
                 (fn [h]
                   (let [new-h (conj h entry)]
                     (if (> (count new-h) (:max-history this))
                       (vec (drop 1 new-h))
                       new-h)))))))))