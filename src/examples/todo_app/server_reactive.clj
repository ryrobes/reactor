(ns examples.todo-app.server-reactive
  "TODO app server with SQL subscription support for diff optimization"
  (:require [reactor.reactive-server :as r]
            [reactor.session_simple :as session]
            [reactor.kafka-reactive :as kafka]
            [clojure.tools.logging :as log]))

(defn -main []
  (println "Starting TODO app reactive server on port 4000...")
  
  ;; Initialize Kafka for reactive subscriptions (optional but recommended)
  (try
    (kafka/init! {"bootstrap.servers" "localhost:9092"
                  "group.id" "todo-app"})
    (println "✅ Kafka reactive system initialized")
    (catch Exception e
      (println "⚠️  WARNING: Could not initialize Kafka:" (.getMessage e))
      (println "    Reactive updates will work but without Kafka optimization")))
  
  ;; Start the reactive server with SQL subscription support
  (r/start-reactive! 
    :port 4000
    :init-fn (fn []
              ;; Initialize session system with todo_sessions table
              (session/init! :todo "todo_sessions")
              (println "✅ TODO app reactive server started on port 4000")
              (println "✅ Using table: todo_sessions")
              (println "✅ SQL subscriptions enabled at /api/sql and /api/subscribe-sql")
              (println "✅ Diff optimization active for all subscriptions"))
    
    ;; Empty handlers - we're using SQL subscriptions directly
    :handlers {})
  
  (println "📊 Server ready! Open http://localhost:8084/todo-enhanced.html")
  (println "📊 Watch the browser console for diff statistics"))