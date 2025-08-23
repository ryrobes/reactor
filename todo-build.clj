(require 'cljs.build.api)

(cljs.build.api/build "src"
  {:main 'examples.todo-app.client
   :output-to "resources/public/js/todo.js"
   :output-dir "resources/public/js/todo-out"
   :asset-path "/js/todo-out"
   :optimizations :none
   :source-map true
   :verbose true})