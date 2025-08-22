(ns reactor.rabbitize
  "Integration with rabbitize for visual testing and browser automation.
   Manages rabbitize process lifecycle and provides REST pass-through."
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [org.httpkit.client :as http]
            [reactor.xtdb-store :as xts])
  (:import [java.io BufferedReader InputStreamReader]))

;; ============================================================================
;; Process Management
;; ============================================================================

(defonce rabbitize-state 
  (atom {:process nil
         :port 3080
         :session nil
         :status :stopped}))

(defn installed?
  "Check if rabbitize is installed"
  []
  (try
    (let [{:keys [exit]} (shell/sh "which" "rabbitize")]
      (zero? exit))
    (catch Exception _ false)))

(defn install-rabbitize!
  "Install rabbitize and its dependencies"
  []
  (println "Installing rabbitize...")
  (let [npm-install (shell/sh "npm" "install" "-g" "rabbitize")
        playwright-deps (shell/sh "sudo" "npx" "playwright" "install-deps")]
    (if (and (zero? (:exit npm-install))
             (zero? (:exit playwright-deps)))
      (println "✅ Rabbitize installed successfully")
      (println "❌ Failed to install rabbitize:" 
               (:err npm-install) 
               (:err playwright-deps)))))

(defn start-rabbitize!
  "Start rabbitize process on specified port"
  [& {:keys [port] :or {port 3080}}]
  ;; First check if already running
  (if (= :running (:status @rabbitize-state))
    {:status :already-running :port (:port @rabbitize-state)}
    ;; Check for external instance - rabbitize doesn't have /health, just check if port responds
    (try
      (let [response @(http/get (str "http://localhost:" port "/")
                               {:timeout 1000
                                :follow-redirects false})] ;; Don't follow redirects
        (if (and (:status response) 
                 (or (= 200 (:status response))
                     (= 302 (:status response))  ;; Rabbitize returns 302 redirect
                     (= 301 (:status response)))) ;; Or 301
          (do
            (println "✅ Found existing rabbitize on port" port "(status:" (:status response) ")")
            (swap! rabbitize-state assoc 
                   :process nil
                   :port port
                   :status :running
                   :external true)
            {:status :already-running :port port :external true})
          ;; No response or wrong status, continue to try starting
          (throw (Exception. "No valid response"))))
      (catch Exception _
        ;; No external instance, start new one
        (println "Starting rabbitize on port" port "...")
        (try
          (let [pb (ProcessBuilder. (into-array String 
                                    ["npx" "rabbitize" "--port" (str port)]))
                _ (.redirectErrorStream pb true)
                process (.start pb)
                output-reader (future
                               (with-open [reader (io/reader (.getInputStream process))]
                                 (doseq [line (line-seq reader)]
                                   (println "[RABBITIZE]" line))))]
            (swap! rabbitize-state assoc 
                   :process process
                   :port port
                   :status :starting
                   :output-reader output-reader)
            ;; Wait and check - rabbitize doesn't have /health, just check if port responds
            (Thread/sleep 5000)
            (let [response @(http/get (str "http://localhost:" port "/")
                                      {:timeout 2000
                                       :follow-redirects false})]
              (if (and (:status response)
                       (or (= 200 (:status response))
                           (= 302 (:status response))
                           (= 301 (:status response)))) ;; Accept redirects
                (do
                  (swap! rabbitize-state assoc :status :running)
                  (println "✅ Rabbitize started on port" port)
                  {:status :started :port port})
                (throw (Exception. "Health check failed")))))
          (catch Exception e
            ;; Try to check if external instance started
            (try
              (let [response @(http/get (str "http://localhost:" port "/")
                                       {:timeout 1000
                                        :follow-redirects false})]
                (when (and (:status response)
                           (or (= 200 (:status response))
                               (= 302 (:status response))
                               (= 301 (:status response)))) ;; Accept redirects
                  (println "✅ Found rabbitize on port" port "(may be external)")
                  (swap! rabbitize-state assoc :status :running :port port :external true)
                  {:status :started :port port :external true}))
              (catch Exception _
                ;; Cleanup on failure
                (println "❌ Failed to start rabbitize:" (.getMessage e))
                (when-let [p (:process @rabbitize-state)]
                  (.destroy p))
                (swap! rabbitize-state assoc 
                       :process nil 
                       :status :stopped)
                {:status :failed :error (.getMessage e)}))))))))

(defn stop-rabbitize!
  "Stop rabbitize process"
  []
  (if-let [process (:process @rabbitize-state)]
    (do
      (println "Stopping rabbitize...")
      (.destroy process)
      (swap! rabbitize-state assoc 
             :process nil
             :session nil
             :status :stopped)
      {:status :stopped})
    {:status :not-running}))

;; ============================================================================
;; Session Management
;; ============================================================================

(defn create-session!
  "Create a new rabbitize session for a snapshot"
  [snapshot-id & {:keys [width height wait-seconds url base-url]
                  :or {width 1280 
                       height 720 
                       wait-seconds 5}}]
  (try
    (let [port (:port @rabbitize-state)
          client-id (str "reactor-snapshot-" snapshot-id)
          test-id (str (System/currentTimeMillis))
          ;; Allow direct URL or construct from base-url + snapshot
          final-url (or url 
                       (when base-url 
                         (str base-url "/?snapshot=" snapshot-id))
                       (throw (Exception. "Either 'url' or 'base-url' must be provided")))
          
          _ (println "[SESSION] Creating session for URL:" final-url)
          
          ;; Start session with initial URL
          start-response @(http/post (str "http://localhost:" port "/start")
                                     {:headers {"Content-Type" "application/json"}
                                      :body (json/generate-string
                                              {:clientId client-id
                                               :testId test-id
                                               :url final-url
                                               :width width
                                               :height height})
                                      :timeout 10000})]
      
      (println "[SESSION] Start response status:" (:status start-response))
      
      (if (= 200 (:status start-response))
        (let [session-data (json/parse-string (:body start-response) true)]
          ;; Store session info
          (swap! rabbitize-state assoc :session 
                 {:client-id client-id
                  :test-id test-id
                  :session-id (:sessionId session-data)
                  :snapshot-id snapshot-id
                  :start-time (System/currentTimeMillis)
                  :status :active})
          
          (println "[SESSION] Session created, waiting" wait-seconds "seconds for page load...")
          ;; Wait for page to load and render
          (Thread/sleep (* 1000 wait-seconds))
          
          {:status :created
           :session session-data
           :artifacts-path (str "rabbitize-runs/" client-id "/" test-id "/")})
        
        {:status :failed
         :error (str "Failed to start session. Status: " (:status start-response) 
                    " Body: " (:body start-response))}))
    (catch Exception e
      (println "[SESSION] Error creating session:" (.getMessage e))
      {:status :failed
       :error (.getMessage e)})))

(defn execute-command!
  "Execute a rabbitize command in the current session"
  [command]
  (if-let [session (:session @rabbitize-state)]
    (let [port (:port @rabbitize-state)
          response @(http/post (str "http://localhost:" port "/execute")
                              {:headers {"Content-Type" "application/json"}
                               :body (json/generate-string
                                       {:clientId (:client-id session)
                                        :testId (:test-id session)
                                        :command command})})]
      (if (= 200 (:status response))
        {:status :success
         :result (json/parse-string (:body response) true)}
        {:status :failed
         :error (:body response)}))
    {:status :no-session
     :error "No active session"}))

(defn end-session!
  "End the current session and save artifacts metadata"
  [& {:keys [save-to-db?] :or {save-to-db? true}}]
  (if-let [session (:session @rabbitize-state)]
    (let [port (:port @rabbitize-state)
          ;; Send finish command
          _ (execute-command! [:finish])
          
          ;; Wait for video processing
          _ (Thread/sleep 2000)
          
          ;; Get session artifacts path
          artifacts-dir (str "rabbitize-runs/" 
                           (:client-id session) "/" 
                           (:test-id session) "/")
          
          ;; Read session metadata if it exists
          metadata-file (io/file artifacts-dir 
                                (first (.list (io/file artifacts-dir) 
                                            (reify java.io.FilenameFilter
                                              (accept [_ _ name]
                                                (str/ends-with? name "session-metadata.json"))))))
          
          metadata (when (.exists metadata-file)
                    (json/parse-string (slurp metadata-file) true))]
      
      ;; Save to database if requested
      (when (and save-to-db? metadata)
        (let [node (xts/start-xtdb-node)]
          (xts/execute-sql node
            "INSERT INTO reactor_visual_tests 
             (_id, snapshot_id, session_id, artifacts_path, metadata, created_at)
             VALUES (?, ?, ?, ?, ?, ?)"
            (str "visual-test-" (System/currentTimeMillis))
            (:snapshot-id session)
            (:session-id metadata)
            artifacts-dir
            (pr-str metadata)
            (java.time.Instant/now))))
      
      ;; Clear session
      (swap! rabbitize-state assoc :session nil)
      
      {:status :ended
       :artifacts-path artifacts-dir
       :metadata metadata})
    
    {:status :no-session
     :error "No active session"}))

;; ============================================================================
;; App URL Configuration
;; ============================================================================

(def app-urls
  "Default URLs for different reactor apps"
  {:rabbit "http://localhost:8081/rabbit.html"
   :todo "http://localhost:3333"
   :magic "http://localhost:3000"})

(defn get-app-url
  "Get the URL for a specific app, with snapshot parameter if provided"
  [app-name snapshot-id]
  (let [base-url (get app-urls (keyword app-name))]
    (when base-url
      (if snapshot-id
        (str base-url "?snapshot=" snapshot-id)
        base-url))))

;; ============================================================================
;; Visual Testing
;; ============================================================================

(defn capture-snapshot!
  "Capture a visual snapshot of a reactor app state using batch mode"
  [snapshot-id & {:keys [width height wait-seconds url base-url app-name]
                  :or {width 1280 
                       height 720 
                       wait-seconds 5}}]
  (try
    (println "[CAPTURE] Starting batch capture for snapshot:" snapshot-id)
    
    ;; Determine URL
    (let [final-url (or url 
                       (when base-url 
                         (str base-url "?snapshot=" snapshot-id))
                       (when app-name
                         (get-app-url app-name snapshot-id))
                       (throw (Exception. "Must provide url, base-url, or app-name")))
          
          client-id (str "reactor-snapshot-" snapshot-id)
          test-id (str (System/currentTimeMillis))
          artifacts-dir (str "rabbitize-runs/" client-id "/" test-id "/")
          
          ;; Batch commands - just wait and capture
          batch-commands (json/generate-string
                          [[":wait" wait-seconds]])
          
          ;; Build command
          cmd ["npx" "rabbitize"
               "--stability-detection" "false"
               "--exit-on-end" "true"
               "--process-video" "true"
               "--client-id" client-id
               "--test-id" test-id
               "--width" (str width)
               "--height" (str height)
               "--batch-url" final-url
               "--batch-commands" batch-commands]
          
          _ (println "[CAPTURE] Running command:" (str/join " " cmd))
          
          ;; Execute rabbitize in batch mode
          process (apply shell/sh cmd)]
      
      (println "[CAPTURE] Batch execution completed with exit code:" (:exit process))
      
      (if (zero? (:exit process))
        (do
          (println "[CAPTURE] Successfully captured snapshot")
          ;; Save metadata to database
          (let [node (xts/start-xtdb-node)]
            (xts/execute-sql node
              "INSERT INTO reactor_visual_tests 
               (_id, snapshot_id, artifacts_path, created_at)
               VALUES (?, ?, ?, ?)"
              (str "visual-test-" (System/currentTimeMillis))
              snapshot-id
              artifacts-dir
              (java.time.Instant/now)))
          
          {:status :success
           :artifacts-path artifacts-dir
           :snapshot-id snapshot-id})
        
        (do
          (println "[CAPTURE] Failed with output:" (:out process))
          (println "[CAPTURE] Error output:" (:err process))
          {:status :error
           :message (str "Rabbitize failed with exit code " (:exit process))
           :output (:out process)
           :error (:err process)})))
    
    (catch Exception e
      (println "[CAPTURE] Error during capture:" (.getMessage e))
      {:status :error
       :message (.getMessage e)})))

(defn compare-snapshots
  "Compare two visual snapshots (phase 2 - placeholder for now)"
  [snapshot-id-1 snapshot-id-2]
  ;; TODO: Implement visual comparison
  ;; - Load screenshots from both sessions
  ;; - Calculate pixel diff
  ;; - Compare DOM structures
  ;; - Return similarity score
  {:status :not-implemented
   :message "Visual comparison coming in phase 2"})

;; ============================================================================
;; REST Endpoints
;; ============================================================================

(defn handle-rabbitize-request
  "Handle rabbitize-related HTTP requests"
  [req]
  (let [path (:uri req)
        method (:request-method req)
        body (when (:body req)
               (json/parse-string (slurp (:body req)) true))]
    
    (cond
      ;; Start rabbitize process
      (and (= path "/api/rabbitize/start") (= method :post))
      (let [result (start-rabbitize! :port (or (:port body) 3080))]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string result)})
      
      ;; Stop rabbitize process
      (and (= path "/api/rabbitize/stop") (= method :post))
      (let [result (stop-rabbitize!)]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string result)})
      
      ;; Get rabbitize status
      (and (= path "/api/rabbitize/status") (= method :get))
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string 
               {:status (:status @rabbitize-state)
                :port (:port @rabbitize-state)
                :session (:session @rabbitize-state)})}
      
      ;; Create new session for snapshot
      (and (= path "/api/rabbitize/session") (= method :post))
      (let [result (create-session! (:snapshot_id body)
                                   :width (or (:width body) 1280)
                                   :height (or (:height body) 720)
                                   :wait-seconds (or (:wait_seconds body) 5)
                                   :url (:url body)
                                   :base-url (:base_url body))]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string result)})
      
      ;; Execute command in session
      (and (= path "/api/rabbitize/execute") (= method :post))
      (let [result (execute-command! (:command body))]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string result)})
      
      ;; End session and get artifacts
      (and (= path "/api/rabbitize/end-session") (= method :post))
      (let [result (end-session! :save-to-db? (get body :save_to_db true))]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string result)})
      
      ;; Capture full snapshot (convenience)
      (and (= path "/api/rabbitize/capture") (= method :post))
      (let [result (capture-snapshot! (:snapshot_id body)
                                     :width (or (:width body) 1280)
                                     :height (or (:height body) 720)
                                     :wait-seconds (or (:wait_seconds body) 5)
                                     :url (:url body)
                                     :base-url (or (:base_url body) (:base-url body))
                                     :app-name (:app_name body))]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string result)})
      
      ;; Not found
      :else
      {:status 404
       :body "Rabbitize endpoint not found"})))