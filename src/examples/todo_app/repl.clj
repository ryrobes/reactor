(ns examples.todo-app.repl
  (:require [cljs.repl :as repl]
            [cljs.build.api :as b]
            [cljs.repl.browser :as browser]))

(def build-opts
  {:main 'examples.todo-app.client
   :output-to "resources/public/js/todo.js"
   :output-dir "resources/public/js/todo-out"
   :asset-path "/js/todo-out"
   :optimizations :none
   :source-map true
   :verbose true})

(defn start-repl
  "Start a browser-connected REPL for the TODO app"
  []
  (println "Building ClojureScript for REPL...")
  (b/build "src" build-opts)
  (println "Starting REPL server...")
  (println "Make sure to open http://localhost:8083/todo.html in your browser")
  (println "and that the client can connect back to the REPL on port 9000")
  (repl/repl (browser/repl-env :port 9000)
             :watch "src"
             :output-dir "resources/public/js/todo-out"))

(defn -main []
  (start-repl))