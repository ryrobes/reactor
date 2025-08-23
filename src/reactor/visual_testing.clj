(ns reactor.visual-testing
  "Visual regression testing for reactor apps.
   Captures screenshots and DOM snapshots, compares against baselines."
  (:require [reactor.xtdb-store :as xts]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [reactor.rabbitize :as rabbitize]
            [clojure.java.shell :as shell])
  (:import [java.nio.file Files Paths]
           [java.util Base64]
           [javax.imageio ImageIO]
           [java.awt.image BufferedImage]))

;; ============================================================================
;; Database Schema
;; ============================================================================

(defn create-visual-test-tables!
  "Initialize visual test tables by creating dummy records (XTDB creates tables on first insert)"
  []
  (let [node (xts/start-xtdb-node)]
    ;; Create baselines table by inserting and deleting a dummy record
    (try
      (xts/execute-sql node
        "INSERT INTO reactor_visual_baselines 
         (_id, app_name, test_name, step_index, screenshot_path, dom_json, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)"
        "dummy-baseline" "dummy" "dummy" 0 nil nil (java.time.Instant/now))
      (xts/execute-sql node
        "DELETE FROM reactor_visual_baselines WHERE _id = ?"
        "dummy-baseline")
      (catch Exception e
        (println "Baselines table initialization:" (.getMessage e))))
    
    ;; Create results table by inserting and deleting a dummy record  
    (try
      (xts/execute-sql node
        "INSERT INTO reactor_visual_results
         (_id, app_name, test_name, step_index, baseline_id,
          baseline_screenshot_path, current_screenshot_path, diff_image_path,
          image_similarity, dom_differences, status, artifacts_path, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        "dummy-result" "dummy" "dummy" 0 nil nil nil nil 0.0 nil "DUMMY" nil (java.time.Instant/now))
      (xts/execute-sql node
        "DELETE FROM reactor_visual_results WHERE _id = ?"
        "dummy-result")
      (catch Exception e
        (println "Results table initialization:" (.getMessage e))))
    
    (println "Visual test tables initialized")))

;; ============================================================================
;; Terminal Image Display
;; ============================================================================

(defn terminal-supports-images?
  "Check if terminal supports image display"
  []
  (let [term (System/getenv "TERM")
        term-program (System/getenv "TERM_PROGRAM")]
    (or (= "iTerm.app" term-program)
        (str/includes? (or term "") "kitty")
        (str/includes? (or term "") "wezterm")
        (str/includes? (or term "") "sixel"))))

(defn display-image-in-terminal
  "Display image in terminal using imgcat or other protocols"
  [image-path & {:keys [width height label]
                 :or {width 120 height 40}}]
  (try
    ;; Try imgcat first - it's widely compatible
    (let [result (shell/sh "imgcat" 
                          "-w" (str width)
                          image-path)]
      (when (zero? (:exit result))
        (when label
          (println (format "  %-40s" label)))
        (print (:out result))
        (flush)
        true))
    (catch Exception e
      nil))) ; Silently fail if image display not available

(defn generate-comparison-html
  "Generate an HTML file with side-by-side image comparison"
  [baseline-path current-path diff-path similarity & {:keys [output-dir] 
                                                       :or {output-dir "/tmp"}}]
  (let [html-path (str output-dir "/visual-test-comparison-" (System/currentTimeMillis) ".html")
        html-content (str 
                      "<!DOCTYPE html>
<html>
<head>
    <title>Visual Test Comparison</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #1e1e1e; color: #fff; }
        h1 { text-align: center; }
        .container { display: flex; justify-content: space-around; margin: 20px 0; }
        .image-box { text-align: center; }
        .image-box img { max-width: 500px; border: 2px solid #444; }
        .label { font-weight: bold; margin: 10px 0; font-size: 18px; }
        .similarity { text-align: center; font-size: 24px; margin: 20px; 
                     color: " (if (>= similarity 95.0) "#4CAF50" "#f44336") "; }
        .baseline { border-color: #2196F3 !important; }
        .current { border-color: #FF9800 !important; }
        .diff { border-color: #f44336 !important; }
    </style>
</head>
<body>
    <h1>Visual Test Comparison</h1>
    <div class='similarity'>Similarity: " (format "%.2f%%" similarity) "</div>
    <div class='container'>
        <div class='image-box'>
            <div class='label'>BASELINE</div>
            <img src='file://" (if (.isAbsolute (io/file baseline-path))
                                 baseline-path
                                 (.getAbsolutePath (io/file baseline-path))) "' class='baseline'/>
        </div>
        <div class='image-box'>
            <div class='label'>CURRENT</div>
            <img src='file://" (if (.isAbsolute (io/file current-path))
                                current-path
                                (.getAbsolutePath (io/file current-path))) "' class='current'/>
        </div>"
        (when diff-path
          (str "
        <div class='image-box'>
            <div class='label'>DIFFERENCE</div>
            <img src='file://" (if (.isAbsolute (io/file diff-path))
                                 diff-path
                                 (.getAbsolutePath (io/file diff-path))) "' class='diff'/>
        </div>"))
        "
    </div>
</body>
</html>")]
    (spit html-path html-content)
    html-path))

(defn display-images-side-by-side
  "Display two images side by side - generates HTML report and shows inline if possible"
  [baseline-path current-path & {:keys [similarity diff-path] :or {similarity 0.0}}]
  (println "\n📸 Visual Comparison:")
  (println (str (apply str (repeat 80 "─"))))
  
  ;; Try to display images inline with imgcat
  (let [imgcat-path (or (first (filter #(.exists (io/file %))
                                       ["/snap/bin/imgcat"
                                        "/usr/local/bin/imgcat"
                                        "/usr/bin/imgcat"]))
                        "imgcat")
        baseline-abs-path (if (.isAbsolute (io/file baseline-path))
                           baseline-path
                           (.getAbsolutePath (io/file baseline-path)))
        current-abs-path (if (.isAbsolute (io/file current-path))
                          current-path
                          (.getAbsolutePath (io/file current-path)))]
    
    (println "\n🖼️  VISUAL COMPARISON (inline preview):")
    (println (str (apply str (repeat 80 "─"))))
    
    ;; Display baseline
    (println "BASELINE:")
    (try
      (let [result (shell/sh "imgcat" baseline-abs-path)]
        (if (zero? (:exit result))
          (do 
            (print (:out result))
            (flush))
          (println "  (imgcat failed for baseline:" (:err result) ")")))
      (catch Exception e
        (println "  (Could not display baseline image:" (.getMessage e) ")")))
    
    ;; Display current  
    (println "\nCURRENT:")
    (try
      (let [result (shell/sh imgcat-path current-abs-path)]
        (if (zero? (:exit result))
          (do
            (print (:out result))
            (flush))
          (println "  (imgcat failed for current:" (:err result) ")")))
      (catch Exception e
        (println "  (Could not display current image:" (.getMessage e) ")")))
    
    (println)
    (println (str (apply str (repeat 80 "─"))))
    
    ;; Generate HTML comparison
    (let [html-path (generate-comparison-html baseline-path current-path diff-path similarity)]
    (println "\n🌐 Visual comparison report (full resolution):")
    (println (str "   file://" html-path))
    (println "\n📂 Image paths:")
    (println (str "   Baseline: " baseline-path))
    (println (str "   Current:  " current-path))
    (when diff-path
      (println (str "   Diff:     " diff-path)))
    
    ;; Try to open in browser automatically
    ;; (try
    ;;   (let [os-name (str/lower-case (System/getProperty "os.name"))]
    ;;     (cond
    ;;       (str/includes? os-name "linux") (shell/sh "xdg-open" html-path)
    ;;       (str/includes? os-name "mac") (shell/sh "open" html-path)
    ;;       (str/includes? os-name "windows") (shell/sh "cmd" "/c" "start" html-path)))
    ;;   (catch Exception e
    ;;     ; Silently fail if can't open browser
    ;;     ))
      ))
  
  (println (str (apply str (repeat 80 "─")))))

;; ============================================================================
;; Image Handling
;; ============================================================================

(defn image->bytes
  "Convert image file to byte array"
  [image-path]
  (Files/readAllBytes (Paths/get image-path (into-array String []))))

(defn bytes->image
  "Convert byte array back to BufferedImage"
  [bytes]
  (ImageIO/read (io/input-stream bytes)))

(defn save-image-to-db
  "Save image as binary blob in database"
  [node table-name id image-path]
  (let [bytes (image->bytes image-path)]
    (xts/execute-sql node
      (str "UPDATE " table-name " SET screenshot_blob = ? WHERE _id = ?")
      bytes id)))

;; ============================================================================
;; Image Comparison
;; ============================================================================

(defn compare-images-pixels
  "Fallback pixel-by-pixel image comparison in pure Clojure"
  [baseline-path test-path]
  (try
    (let [img1 (ImageIO/read (io/file baseline-path))
          img2 (ImageIO/read (io/file test-path))
          width (.getWidth img1)
          height (.getHeight img1)
          total-pixels (* width height)
          matching-pixels (atom 0)]
      
      ;; Compare each pixel
      (doseq [x (range width)
              y (range height)]
        (when (= (.getRGB img1 x y) (.getRGB img2 x y))
          (swap! matching-pixels inc)))
      
      {:similarity (* 100.0 (/ @matching-pixels total-pixels))})
    (catch Exception e
      {:similarity 0 :error (.getMessage e)})))

(defn compare-images
  "Compare two images and return similarity percentage and diff image.
   Uses ImageMagick's compare command if available."
  [baseline-path test-path]
  (try
    ;; Try using ImageMagick compare
    (let [diff-path (str/replace test-path #"\.jpg$" "-diff.jpg")
          result (shell/sh "compare" "-metric" "RMSE" 
                          baseline-path test-path diff-path)]
      (if (zero? (:exit result))
        (let [;; Parse RMSE value from stderr
              rmse-output (:err result)
              rmse-match (re-find #"(\d+\.?\d*)" rmse-output)
              rmse (if rmse-match (Double/parseDouble (second rmse-match)) 0)
              ;; Convert RMSE to similarity percentage (lower RMSE = higher similarity)
              ;; RMSE of 0 = 100% similar, normalize to 0-100%
              similarity (max 0 (- 100 (/ rmse 655.35)))] ; 65535 is max RMSE for 16-bit images
          {:similarity similarity
           :diff-image diff-path
           :rmse rmse})
        ;; Fallback to pixel-by-pixel comparison in Clojure
        (compare-images-pixels baseline-path test-path)))
    (catch Exception e
      (println "Error comparing images:" (.getMessage e))
      {:similarity 0 :error (.getMessage e)})))

;; ============================================================================
;; DOM Comparison
;; ============================================================================

(defn find-differences
  "Recursively find differences between two data structures"
  [baseline test path]
  (cond
    (= baseline test) []
    
    (and (map? baseline) (map? test))
    (let [all-keys (set (concat (keys baseline) (keys test)))]
      (mapcat (fn [k]
                (find-differences 
                  (get baseline k)
                  (get test k)
                  (conj path k)))
              all-keys))
    
    (and (sequential? baseline) (sequential? test))
    (if (= (count baseline) (count test))
      (mapcat (fn [idx]
                (find-differences
                  (nth baseline idx)
                  (nth test idx)
                  (conj path idx)))
              (range (count baseline)))
      [{:path path
        :baseline-count (count baseline)
        :test-count (count test)
        :type :count-mismatch}])
    
    :else
    [{:path path
      :baseline baseline
      :test test
      :type :value-difference}]))

(defn compare-dom
  "Compare two DOM structures (either JSON or markdown) and return differences"
  [baseline-content test-content]
  ;; For now, just do string comparison for markdown DOM
  ;; Could enhance this later to parse markdown tables
  (if (= baseline-content test-content)
    []  ;; No differences
    [{:type :content-mismatch
      :baseline-length (count baseline-content)
      :test-length (count test-content)}]))

;; ============================================================================
;; Capture & Store Baseline
;; ============================================================================

(defn capture-baseline!
  "Capture and store a visual baseline for a test"
  [app-name test-name snapshot-id & {:keys [wait-seconds steps]
                                     :or {wait-seconds 3
                                          steps 1}}]
  (let [node (xts/start-xtdb-node)
        ;; Use simpler folder structure
        test-id (str app-name "-" test-name "-" (System/currentTimeMillis))
        
        ;; Capture using rabbitize
        capture-result (rabbitize/capture-snapshot! 
                        snapshot-id
                        :app-name app-name
                        :wait-seconds wait-seconds)
        
        artifacts-path (:artifacts-path capture-result)]
    
    (when (= :success (:status capture-result))
      ;; Find the session folder - use the most recent one
      (let [session-dirs (when (.exists (io/file artifacts-path))
                          (.listFiles (io/file artifacts-path)))
            session-dir (when session-dirs
                         (last (sort-by #(.getName %) 
                                       (filter #(.isDirectory %) session-dirs))))
            screenshots-dir (when session-dir (io/file session-dir "screenshots"))
            dom-dir (when session-dir (io/file session-dir "dom_snapshots"))]
        
        ;; Store each step as a baseline
        (doseq [step (range steps)]
          (let [baseline-id (str "baseline-" app-name "-" test-name "-" step "-" 
                                (System/currentTimeMillis))
                screenshot-path (str screenshots-dir "/" step ".jpg")
                dom-path (str dom-dir "/dom_" step ".md")]
            
            ;; Insert baseline record with image path
            (xts/execute-sql node
              "INSERT INTO reactor_visual_baselines 
               (_id, app_name, test_name, step_index, screenshot_path, dom_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)"
              baseline-id app-name test-name step
              (when (.exists (io/file screenshot-path))
                screenshot-path)
              (when (.exists (io/file dom-path))
                (slurp dom-path))
              (java.time.Instant/now))
            
            (println "Stored baseline for" app-name "/" test-name "step" step)))))
    
    capture-result))

;; ============================================================================
;; Helper functions for visual testing
;; ============================================================================

(defn- create-baseline-from-capture
  "Create a baseline from the current capture"
  [node app-name test-name screenshots-dir dom-dir artifacts-path]
  (when (and screenshots-dir (.exists screenshots-dir))
    ;; Find the post-wait screenshot (the actual captured state)
    (let [post-wait-file (io/file screenshots-dir "0-post-wait.jpg")
          ;; Also check for plain "0.jpg" if post-wait doesn't exist
          screenshot-file (if (.exists post-wait-file)
                           post-wait-file
                           (io/file screenshots-dir "0.jpg"))]
      (when (.exists screenshot-file)
        (let [step 0  ;; For now, just handle single step
              baseline-id (str "baseline-" app-name "-" test-name "-" step "-" 
                              (System/currentTimeMillis))
              ;; Look for .md file not .json
              dom-filename "dom_0.md"
              dom-path (when dom-dir (io/file dom-dir dom-filename))]
          
          ;; Insert single baseline record
          (xts/execute-sql node
            "INSERT INTO reactor_visual_baselines 
             (_id, app_name, test_name, step_index, screenshot_path, dom_json, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)"
            baseline-id app-name test-name step
            (.getPath screenshot-file)
            (if (and dom-path (.exists dom-path))
              (slurp dom-path)
              "")
            (java.time.Instant/now))))))
  
  {:status "BASELINE_CREATED"
   :message "First run - baseline created for future comparisons"
   :artifacts-path artifacts-path})

(defn- compare-against-baseline
  "Compare current capture against existing baseline"
  [node app-name test-name baselines screenshots-dir dom-dir artifacts-path threshold]
  (let [baseline (first (:results baselines))
        test-results (atom [])]
    
    ;; Get all baselines for this test
    (let [all-baselines (xts/execute-sql node
                        "SELECT * FROM reactor_visual_baselines 
                         WHERE app_name = ? AND test_name = ?
                         LIMIT 10"
                        app-name test-name)]
      
      ;; Compare each step
      (doseq [baseline (:results all-baselines)]
        (let [step (:step_index baseline)
              ;; Try post-wait first, fall back to 0.jpg
              post-wait-path (str screenshots-dir "/0-post-wait.jpg")
              plain-path (str screenshots-dir "/0.jpg")
              screenshot-path (if (.exists (io/file post-wait-path))
                               post-wait-path
                               plain-path)
              ;; Look for .md file not .json
              dom-path (str dom-dir "/dom_0.md")
              
              ;; Compare image if available
              image-result (when (and (:screenshot_path baseline)
                                     (.exists (io/file screenshot-path))
                                     (.exists (io/file (:screenshot_path baseline))))
                            (compare-images (:screenshot_path baseline) screenshot-path))
              
              ;; Compare DOM if available
              dom-differences (when (and (:dom_json baseline)
                                        (.exists (io/file dom-path)))
                              (try
                                (compare-dom (:dom_json baseline) (slurp dom-path))
                                (catch Exception e
                                  (println "Error comparing DOM:" (.getMessage e))
                                  [{:type :error :message (.getMessage e)}])))
              
              ;; Determine pass/fail
              similarity (or (:similarity image-result) 100.0)
              status (if (and (>= similarity threshold)
                             (empty? dom-differences))
                      "PASS"
                      "FAIL")
              
              ;; ALWAYS show images for visual feedback
              _ (when (and (:screenshot_path baseline)
                          (.exists (io/file (:screenshot_path baseline)))
                          (.exists (io/file screenshot-path)))
                  ;; Always display the images
                  (display-images-side-by-side (:screenshot_path baseline) screenshot-path 
                                              :similarity similarity
                                              :diff-path (:diff-image image-result))
                  
                  ;; Show appropriate status message
                  (if (= status "PASS")
                    (println (format "\n✅ Visual test PASSED! Similarity: %.2f%% (threshold: %.2f%%)" 
                                    similarity threshold))
                    (do
                      (println (format "\n❌ Visual test FAILED! Similarity: %.2f%% (threshold: %.2f%%)" 
                                      similarity threshold))
                      (when (not (empty? dom-differences))
                        (println "📝 DOM differences found:" (count dom-differences) "changes")))))
              
              ;; Store result
              result-id (str "result-" app-name "-" test-name "-" step "-"
                            (System/currentTimeMillis))]
          
          (let [values [result-id app-name test-name step (:_id baseline)
                        (:screenshot_path baseline) screenshot-path (or (:diff-image image-result) "")
                        similarity (str dom-differences) status artifacts-path
                        (str (java.time.Instant/now))]]
            (apply xts/execute-sql 
                   node
                   "INSERT INTO reactor_visual_results
                    (_id, app_name, test_name, step_index, baseline_id,
                     baseline_screenshot_path, current_screenshot_path, diff_image_path,
                     image_similarity, dom_differences, status, artifacts_path, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                   values))
          
          (swap! test-results conj
                 {:step step
                  :status status
                  :similarity similarity
                  :dom-differences dom-differences
                  :diff-image (:diff-image image-result)})))
      
      ;; Update baseline if all tests pass
      (when (every? #(= "PASS" (:status %)) @test-results)
        ;; Delete old baselines
        (xts/execute-sql node
          "DELETE FROM reactor_visual_baselines 
           WHERE app_name = ? AND test_name = ?"
          app-name test-name)
        
        ;; Store current run as new baseline
        (let [step 0
              post-wait-path (str screenshots-dir "/0-post-wait.jpg")
              plain-path (str screenshots-dir "/0.jpg")
              screenshot-path (if (.exists (io/file post-wait-path))
                               post-wait-path
                               plain-path)
              dom-path (str dom-dir "/dom_0.md")
              baseline-id (str "baseline-" app-name "-" test-name "-" step "-"
                              (System/currentTimeMillis))]
          
          (xts/execute-sql node
            "INSERT INTO reactor_visual_baselines 
             (_id, app_name, test_name, step_index, screenshot_path, dom_json, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)"
            baseline-id app-name test-name step
            (when (.exists (io/file screenshot-path))
              screenshot-path)
            (when (.exists (io/file dom-path))
              (slurp dom-path))
            (java.time.Instant/now))))
      
      ;; Return overall results
      (let [all-pass? (every? #(= "PASS" (:status %)) @test-results)]
        {:status (if all-pass? "PASS" "FAIL")
         :results @test-results
         :artifacts-path artifacts-path}))))

;; ============================================================================
;; Run Visual Test
;; ============================================================================

(defn check-server-running
  "Check if server is running at the given URL"
  [url]
  (try
    (let [response (slurp url)]
      true)
    (catch Exception e
      false)))

(defn run-visual-test!
  "Run a visual test. If no baseline exists, creates one. Otherwise compares against previous run.
   REQUIRES: Server and UI must be running before test execution.
   Run with: lein run -m examples.<app>.server/-main
            shadow-cljs watch <app>"
  [app-name test-name snapshot-id & {:keys [wait-seconds threshold base-url]
                                     :or {wait-seconds 3
                                          threshold 95.0}}]
  ;; Check if server is reachable
  (when base-url
    (when-not (check-server-running base-url)
      (println (str "\n" (apply str (repeat 60 "="))
                   "\nERROR: Server not running at " base-url
                   "\n\nPlease start the server and UI before running visual tests:"
                   "\n  1. Start server: lein run -m examples." app-name ".server/-main"  
                   "\n  2. Start UI: shadow-cljs watch " app-name
                   "\n  3. Wait for both to be ready"
                   "\n  4. Run tests: lein test"
                   "\n" (apply str (repeat 60 "=")) "\n"))
      (throw (Exception. (str "Server not reachable at " base-url)))))
  
  (let [node (xts/start-xtdb-node)
        
        ;; Get most recent baseline for this test
        baselines (xts/execute-sql node
                    "SELECT * FROM reactor_visual_baselines 
                     WHERE app_name = ? AND test_name = ?
                     LIMIT 1"
                    app-name test-name)
        has-baseline? (not (empty? (:results baselines)))
        
        ;; Capture current state
        capture-result (rabbitize/capture-snapshot!
                        snapshot-id
                        :app-name app-name
                        :base-url base-url
                        :wait-seconds wait-seconds)
        
        artifacts-path (:artifacts-path capture-result)]
    
    (if-not (= :success (:status capture-result))
      ;; Capture failed
      (do
        {:status "ERROR"
         :message (str "Failed to capture: " (:message capture-result))
         :capture-result capture-result})
      ;; Capture succeeded
      (let [;; Find the session folder - use the most recent one
            session-dirs (when (.exists (io/file artifacts-path))
                          (.listFiles (io/file artifacts-path)))
            ;; Sort directories by name (they're timestamps) and take the last/newest one
            session-dir (when session-dirs
                         (last (sort-by #(.getName %) 
                                       (filter #(.isDirectory %) session-dirs))))
            screenshots-dir (when session-dir (io/file session-dir "screenshots"))
            dom-dir (when session-dir (io/file session-dir "dom_snapshots"))]
        
        (if (not has-baseline?)
          ;; No baseline exists - create it from this run
          (create-baseline-from-capture node app-name test-name screenshots-dir dom-dir artifacts-path)
          ;; Baseline exists - compare against it
          (compare-against-baseline node app-name test-name baselines screenshots-dir dom-dir artifacts-path threshold))))))

;; ============================================================================
;; Lein Test Integration
;; ============================================================================

(defn visual-test
  "Macro for defining visual tests that can run with lein test"
  [test-name app-name snapshot-id & opts]
  `(deftest ~(symbol (str "visual-test-" test-name))
     (let [result# (run-visual-test! ~app-name ~(name test-name) ~snapshot-id ~@opts)]
       (is (= "PASS" (:status result#))
           (str "Visual test failed: " (pr-str (:results result#)))))))

;; Example usage:
;; (visual-test home-page "rabbit" "snapshot-123" :threshold 98.0)