(ns examples.todo-app.build
  (:require [cljs.build.api :as b]
            [clojure.java.io :as io]))

(def source-dir "src")
(def output-dir "resources/public/js/todo-out")
(def output-file "resources/public/js/todo.js")

(defn clean []
  (println "Cleaning output directory...")
  (let [out-dir (io/file output-dir)]
    (when (.exists out-dir)
      (doseq [file (reverse (file-seq out-dir))]
        (.delete file)))))

(defn build-once []
  (println "Building ClojureScript...")
  (b/build source-dir
    {:main 'examples.todo-app.client
     :output-to output-file
     :output-dir output-dir
     :asset-path "/js/todo-out"
     :optimizations :none
     :source-map true
     :source-map-timestamp true
     :pretty-print true
     :foreign-libs []
     :externs []
     :closure-warnings {:global-this :off}}))

(defn watch []
  (println "Watching for changes...")
  (b/watch source-dir
    {:main 'examples.todo-app.client
     :output-to output-file
     :output-dir output-dir
     :asset-path "/js/todo-out"
     :optimizations :none
     :source-map true
     :source-map-timestamp true
     :pretty-print true
     :verbose true
     :watch-fn (fn [] 
                 (println (str "[" (java.util.Date.) "] Compilation complete")))
     :watch-error-fn (fn [e] 
                       (println "Build error:" (.getMessage e)))}))

(defn serve
  "Start a simple HTTP server for development"
  []
  (let [port 8083]
    (println (str "Starting HTTP server on port " port "..."))
    (println "Note: For production use, consider using nginx or a proper web server")
    (println (str "Serving files from: resources/public"))
    ;; Simple implementation - in practice you might use ring/jetty or similar
    (println "Please run: cd resources/public && python3 -m http.server 8083")
    (println "Or use any static file server of your choice")))

(defn auto
  "Run watcher - note: you'll need to run a separate HTTP server"
  []
  (println "Starting auto-build mode...")
  (println "Note: You'll need to serve files separately.")
  (println "Run this in another terminal: cd resources/public && python3 -m http.server 8083")
  (watch))

(defn -main [& args]
  (case (first args)
    "clean" (clean)
    "build" (build-once)
    "watch" (watch) 
    "serve" (serve)
    "auto" (auto)
    (do
      (println "Usage: lein run -m examples.todo-app.build [clean|build|watch|serve|auto]")
      (println "  clean - Remove compiled files")
      (println "  build - Build once")
      (println "  watch - Watch and rebuild on changes")
      (println "  serve - Start HTTP server")
      (println "  auto  - Start server and watch (recommended for dev)"))))