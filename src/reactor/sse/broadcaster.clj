(ns reactor.sse.broadcaster
  "Clean SSE broadcasting - completely separate from subscription logic.
   Just handles sending messages to connected clients."
  (:require [org.httpkit.server :as http]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]))

;; ============================================================================
;; Channel Management
;; ============================================================================

;; Map of session-id -> #{channels}
(defonce channels (atom {}))

;; Track channel metadata for debugging
(defonce channel-metadata (atom {}))

(defn register-channel!
  "Register an SSE channel for a session"
  [session-id channel & [metadata]]
  (swap! channels update session-id (fnil conj #{}) channel)
  (when metadata
    (swap! channel-metadata assoc channel metadata))
  (log/info "[SSE] Registered channel for session" session-id
           "Total channels:" (count (get @channels session-id)))
  channel)

(defn unregister-channel!
  "Remove a channel from a session"
  [session-id channel]
  (swap! channels update session-id disj channel)
  (swap! channel-metadata dissoc channel)
  ;; Clean up empty session entries
  (when (empty? (get @channels session-id))
    (swap! channels dissoc session-id))
  (log/info "[SSE] Unregistered channel for session" session-id
           "Remaining channels:" (count (get @channels session-id [])))
  channel)

(defn get-channels
  "Get all channels for a session"
  [session-id]
  (get @channels session-id #{}))

(defn get-all-sessions
  "Get all sessions with active channels"
  []
  (keys @channels))

(defn channel-count
  "Get count of channels for a session"
  [session-id]
  (count (get-channels session-id)))

(defn total-channel-count
  "Get total number of active channels"
  []
  (reduce + 0 (map count (vals @channels))))

;; ============================================================================
;; Message Broadcasting
;; ============================================================================

(defn format-sse-message
  "Format data as SSE message"
  [data]
  (str "data: " (json/generate-string data) "\n\n"))

(defn send-to-channel!
  "Send message to a single channel"
  [channel message]
  (try
    (http/send! channel message false)
    true
    (catch Exception e
      (log/warn "[SSE] Failed to send to channel:" (.getMessage e))
      false)))

(defn broadcast-to-session!
  "Broadcast message to all channels for a session.
   Returns number of successful sends."
  [session-id data]
  (let [channels (get-channels session-id)
        message (format-sse-message data)]
    (if (empty? channels)
      (do
        (log/debug "[SSE] No channels for session" session-id)
        0)
      (let [results (doall
                     (for [channel channels]
                       (if (send-to-channel! channel message)
                         channel
                         nil)))
            successful (remove nil? results)
            failed (- (count channels) (count successful))]
        
        ;; Clean up failed channels
        (doseq [channel channels]
          (when-not (contains? (set successful) channel)
            (unregister-channel! session-id channel)))
        
        (log/info "[SSE] Broadcast to session" session-id
                 "- Success:" (count successful)
                 "Failed:" failed)
        (count successful)))))

(defn broadcast-to-sessions!
  "Broadcast message to multiple sessions"
  [session-ids data]
  (reduce (fn [total session-id]
           (+ total (broadcast-to-session! session-id data)))
          0
          session-ids))

(defn broadcast-to-all!
  "Broadcast message to all connected clients"
  [data]
  (broadcast-to-sessions! (get-all-sessions) data))

;; ============================================================================
;; Connection Management
;; ============================================================================

(defn establish-sse-connection!
  "Establish SSE connection and return channel"
  [request session-id & [metadata]]
  (http/with-channel request channel
    ;; Send SSE headers
    (http/send! channel
               {:status 200
                :headers {"Content-Type" "text/event-stream"
                         "Cache-Control" "no-cache"
                         "Connection" "keep-alive"
                         "Access-Control-Allow-Origin" "*"}}
               false)
    
    ;; Register channel
    (register-channel! session-id channel metadata)
    
    ;; Send initial connection message
    (send-to-channel! channel
                     (format-sse-message
                      {:type :connected
                       :session-id session-id
                       :timestamp (System/currentTimeMillis)}))
    
    ;; Set up cleanup on close
    (http/on-close channel
                  (fn [status]
                    (log/info "[SSE] Channel closed for session" session-id
                             "Status:" status)
                    (unregister-channel! session-id channel)))
    
    channel))

;; ============================================================================
;; Heartbeat / Keep-alive
;; ============================================================================

(defonce heartbeat-running? (atom false))
(defonce heartbeat-thread (atom nil))

(defn send-heartbeat!
  "Send heartbeat to all channels"
  []
  (let [message (format-sse-message
                {:type :heartbeat
                 :timestamp (System/currentTimeMillis)})]
    (doseq [[session-id session-channels] @channels]
      (doseq [channel session-channels]
        (when-not (send-to-channel! channel message)
          (unregister-channel! session-id channel))))))

(defn start-heartbeat!
  "Start heartbeat thread to keep connections alive"
  [interval-ms]
  (when-not @heartbeat-running?
    (reset! heartbeat-running? true)
    (reset! heartbeat-thread
           (future
             (log/info "[SSE] Starting heartbeat with interval" interval-ms "ms")
             (while @heartbeat-running?
               (try
                 (send-heartbeat!)
                 (catch Exception e
                   (log/error e "[SSE] Error in heartbeat")))
               (Thread/sleep interval-ms))
             (log/info "[SSE] Heartbeat stopped")))))

(defn stop-heartbeat!
  "Stop heartbeat thread"
  []
  (reset! heartbeat-running? false)
  (when-let [thread @heartbeat-thread]
    (future-cancel thread)
    (reset! heartbeat-thread nil)))

;; ============================================================================
;; Cleanup
;; ============================================================================

(defn cleanup-dead-channels!
  "Remove channels that are no longer alive"
  []
  (let [test-message (format-sse-message {:type :ping})]
    (doseq [[session-id session-channels] @channels]
      (doseq [channel session-channels]
        (when-not (send-to-channel! channel test-message)
          (unregister-channel! session-id channel))))))

(defn disconnect-session!
  "Disconnect all channels for a session"
  [session-id]
  (let [channels (get-channels session-id)]
    (doseq [channel channels]
      (try
        (http/close channel)
        (catch Exception e
          (log/warn "[SSE] Error closing channel:" e)))
      (unregister-channel! session-id channel))
    (count channels)))

(defn disconnect-all!
  "Disconnect all channels"
  []
  (let [all-sessions (get-all-sessions)]
    (doseq [session-id all-sessions]
      (disconnect-session! session-id))))

;; ============================================================================
;; Statistics
;; ============================================================================

(defn stats
  "Get SSE statistics"
  []
  {:total-sessions (count @channels)
   :total-channels (total-channel-count)
   :sessions (into {}
                   (map (fn [[session-id chans]]
                         [session-id (count chans)])
                       @channels))})