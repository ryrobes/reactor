(ns reactor.log
  "Enhanced logging with colored terminal output and async SQL persistence"
  (:require [io.aviso.ansi :as ansi]
            [reactor.xtdb-store :as xts]
            [reactor.session_simple :as session]
            [clojure.core.async :as async]
            [clojure.string :as str])
  (:import [java.time Instant]
           [java.util UUID]))

;; ============= Configuration =============

(def ^:dynamic *enable-colors* true)
(def ^:dynamic *enable-sql-logging* true)
(def ^:dynamic *log-level* :info)  ; Set to :info to see diff messages

(def log-levels {:trace 0 :debug 1 :info 2 :warn 3 :error 4 :fatal 5})

;; Color scheme for different subsystems
(def subsystem-colors
  {:kafka       ansi/blue
   :sql-rules   ansi/green
   :sql-stacks  ansi/cyan
   :xtdb        ansi/magenta
   :reactive    ansi/yellow
   :meta        ansi/white
   :session     ansi/blue-bg
   :server      ansi/bold-green
   :tap         ansi/bold-cyan
   :default     ansi/white})

;; Color scheme for log levels
(def level-colors
  {:trace ansi/white
   :debug ansi/cyan
   :info  ansi/green
   :warn  ansi/yellow
   :error ansi/red
   :fatal ansi/bold-red-bg})

;; ============= Async SQL Logger =============

;; Use dropping buffer to prevent blocking when channel is full
;; This prevents cascade failures when logging gets too verbose
(defonce log-channel (async/chan (async/dropping-buffer 10000)))
(defonce log-processor (atom nil))

(defn ensure-log-table!
  "Create reactor_logs table if it doesn't exist"
  [node]
  (try
    ;; Check if table exists
    (xts/execute-sql node "SELECT * FROM reactor_logs LIMIT 1")
    (catch Exception _
      ;; Create table - will be created on first insert in XTDB
      nil)))

(defn write-log-to-sql
  "Write a log entry to the database"
  [node log-entry]
  (try
    (let [log-id (str "log-" (UUID/randomUUID))]
      (xts/execute-sql node
        "INSERT INTO reactor_logs RECORDS 
         {_id: ?, timestamp: ?, level: ?, subsystem: ?, message: ?, thread: ?, context: ?}"
        log-id
        (str (:timestamp log-entry))
        (name (:level log-entry))
        (name (:subsystem log-entry))
        (:message log-entry)
        (:thread log-entry)
        (pr-str (:context log-entry))))
    (catch Exception e
      ;; Don't log logging errors to avoid infinite loop
      (println "Failed to write log to SQL:" (.getMessage e)))))

(defn start-log-processor!
  "Start async processor for SQL log writes"
  []
  (when-not @log-processor
    (reset! log-processor
      (async/go-loop []
        (when-let [log-entry (async/<! log-channel)]
          (try
            (when (and *enable-sql-logging* @session/default-node)
              (write-log-to-sql @session/default-node log-entry))
            (catch Exception e
              ;; Silently ignore SQL write errors to avoid console spam
              nil))
          (recur))))
    (println (ansi/green "✓ Async SQL logger started"))))

(defn stop-log-processor!
  "Stop the async log processor"
  []
  (when @log-processor
    (async/close! log-channel)
    (reset! log-processor nil)))

;; ============= Colored Terminal Output =============

(defn format-timestamp
  "Format timestamp for display"
  [ts]
  (subs (str ts) 11 23)) ; Just time portion HH:mm:ss.SSS

(defn colorize-message
  "Apply colors to log message based on subsystem and level"
  [subsystem level message]
  (if *enable-colors*
    (let [subsys-color (get subsystem-colors subsystem (:default subsystem-colors))
          level-color (get level-colors level ansi/white)]
      (str (level-color (str/upper-case (name level)))
           " "
           (subsys-color (format "[%-12s]" (name subsystem)))
           " "
           ansi/reset-font
           message))
    (format "%-5s [%-12s] %s" 
            (str/upper-case (name level))
            (name subsystem)
            message)))

(defn detect-subsystem
  "Auto-detect subsystem from namespace or thread name"
  [ns-str thread-name]
  (cond
    (str/includes? ns-str "kafka") :kafka
    (str/includes? ns-str "sql-rules") :sql-rules
    (str/includes? ns-str "sql-stacks") :sql-stacks
    (str/includes? ns-str "xtdb") :xtdb
    (str/includes? ns-str "reactive") :reactive
    (str/includes? ns-str "meta") :meta
    (str/includes? ns-str "session") :session
    (str/includes? ns-str "server") :server
    (str/includes? ns-str "tap") :tap
    (str/includes? thread-name "kafka") :kafka
    (str/includes? thread-name "agent") :reactive
    :else :default))

;; ============= Main Logging Function =============

(defn log!
  "Enhanced logging with colors and async SQL persistence
   
   Usage:
   (log! :info \"Message\")
   (log! :info :kafka \"Kafka message\")
   (log! :error :sql-rules \"Error\" {:context :data})"
  [level & args]
  (when (>= (get log-levels level 0) (get log-levels *log-level* 2))
    (let [[subsystem message context] (if (keyword? (first args))
                                        [(first args) (second args) (nth args 2 nil)]
                                        [:default (first args) (second args)])
          thread-name (.getName (Thread/currentThread))
          ns-str (str *ns*)
          subsystem (if (= subsystem :default)
                     (detect-subsystem ns-str thread-name)
                     subsystem)
          timestamp (Instant/now)
          
          ;; Format for terminal
          terminal-msg (str (ansi/white (format-timestamp timestamp))
                           ansi/reset-font
                           " "
                           (colorize-message subsystem level message))
          
          ;; Log entry for SQL
          log-entry {:timestamp timestamp
                    :level level
                    :subsystem subsystem
                    :message message
                    :thread thread-name
                    :context context}]
      
      ;; Print to terminal
      (println terminal-msg)
      
      ;; Send to async SQL logger - use offer! to never block
      (when *enable-sql-logging*
        (async/offer! log-channel log-entry))))
  nil)

;; ============= Convenience Functions =============

(defn trace [& args] (apply log! :trace args))
(defn debug [& args] (apply log! :debug args))
(defn info [& args] (apply log! :info args))
(defn warn [& args] (apply log! :warn args))
(defn error [& args] (apply log! :error args))
(defn fatal [& args] (apply log! :fatal args))

;; ============= Integration with tools.logging =============

(defn wrap-tools-logging
  "Wrap tools.logging to use our colored logger"
  []
  (alter-var-root #'clojure.tools.logging/log*
    (fn [original-log]
      (fn [logger level throwable message]
        (log! level message)
        ;; Still call original for file logging
        (original-log logger level throwable message)))))

;; ============= SQL Query Functions =============

(defn query-logs
  "Query logs from the database
   
   Examples:
   (query-logs :subsystem :kafka :limit 10)
   (query-logs :level :error :since \"2024-01-01\")
   (query-logs :search \"failed\" :subsystem :sql-rules)"
  [& {:keys [subsystem level since until search limit]
      :or {limit 100}}]
  (when-let [node @session/default-node]
    (let [conditions (cond-> []
                       subsystem (conj (str "subsystem = '" (name subsystem) "'"))
                       level (conj (str "level = '" (name level) "'"))
                       since (conj (str "timestamp >= '" since "'"))
                       until (conj (str "timestamp <= '" until "'"))
                       search (conj (str "message LIKE '%" search "%'")))
          where-clause (when (seq conditions)
                        (str " WHERE " (str/join " AND " conditions)))
          sql (str "SELECT * FROM reactor_logs"
                  where-clause
                  " ORDER BY timestamp DESC"
                  " LIMIT " limit)]
      (:results (xts/execute-sql node sql)))))

(defn tail-logs
  "Show recent logs, optionally filtered by subsystem"
  ([n] (tail-logs n nil))
  ([n subsystem]
   (let [logs (if subsystem
                (query-logs :subsystem subsystem :limit n)
                (query-logs :limit n))]
     (doseq [log (reverse logs)]
       (println (colorize-message 
                 (keyword (:subsystem log))
                 (keyword (:level log))
                 (str (format-timestamp (:timestamp log))
                      " "
                      (:message log))))))))

;; ============= Initialization =============

(defn init!
  "Initialize the colored logging system"
  []
  (start-log-processor!)
  (when-let [node @session/default-node]
    (ensure-log-table! node))
  (info :log "Colored logging system initialized"))