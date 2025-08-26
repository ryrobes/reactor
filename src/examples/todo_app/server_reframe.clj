(ns examples.todo-app.server-reframe
  "Simplified TODO server for re-frame-style client with SQL-first approach"
  (:require 
   [reactor.reactive-server :as server]
   [reactor.kafka-reactive :as kafka]
   [reactor.xtdb-store :as store]
   [clojure.tools.logging :as log])
  (:gen-class))

;; ============================================================================
;; Configuration
;; ============================================================================

(def config
  {:server {:port 4000
            :host "0.0.0.0"}
   :xtdb {:node-url "jdbc:xtdb://localhost:5432/xtdb"}
   :kafka {:bootstrap-servers "localhost:9092"
           :group-id "todo-app-reframe"
           :topics ["xtdb-transaction-log"]}})

;; ============================================================================
;; Initialize TODO Sessions Table
;; ============================================================================

(defn ensure-todo-table! [node]
  "Ensure the todo_sessions table exists with proper schema"
  (try
    ;; In XTDB 2.0, tables are created implicitly on first insert
    ;; Create a dummy record to ensure table exists, then delete it
    (store/execute-sql node
      "INSERT INTO todo_sessions (_id, app_state) VALUES (?, ?)"
      "__init__" "{}")
    (store/execute-sql node
      "DELETE FROM todo_sessions WHERE _id = ?"
      "__init__")
    (log/info "Todo sessions table initialized")
    (catch Exception e
      (log/warn "Table initialization warning (may already exist):" (.getMessage e)))))

;; ============================================================================
;; Server Lifecycle
;; ============================================================================

(defn start-server []
  (log/info "Starting TODO app server (re-frame style)...")
  
  ;; Initialize XTDB connection
  (let [node (store/create-node (:xtdb config))]
    
    ;; Ensure table exists
    (ensure-todo-table! node)
    
    ;; Start Kafka consumer for reactive SQL subscriptions
    (kafka/start-consumer! 
      {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
       :group-id (get-in config [:kafka :group-id])
       :topics (get-in config [:kafka :topics])
       :node node})
    
    ;; Start HTTP server with simplified configuration
    ;; No need for complex event handlers - everything is SQL-based
    (server/start-server 
      {:port (get-in config [:server :port])
       :host (get-in config [:server :host])
       :node node
       ;; Enable CORS for development
       :cors {:allowed-origins ["http://localhost:8000"
                                "http://localhost:8080"
                                "http://localhost:3000"]
              :allowed-methods [:get :post :options]
              :allowed-headers ["Content-Type"]}
       ;; Simple app info
       :app-info {:name "todo-app-reframe"
                  :version "1.0.0"
                  :description "TODO MVC with re-frame-style SQL API"}})))

(defn -main [& args]
  (log/info "=====================================================")
  (log/info "TODO App Server (Re-frame Style)")
  (log/info "=====================================================")
  (log/info "Configuration:" config)
  
  ;; Start server
  (start-server)
  
  (log/info "Server started on port" (get-in config [:server :port]))
  (log/info "")
  (log/info "Client features:")
  (log/info "  - SQL-first subscriptions with transforms")
  (log/info "  - Declarative SQL event dispatching")
  (log/info "  - Automatic atom management")
  (log/info "  - Built-in time travel and sessions")
  (log/info "")
  (log/info "To use:")
  (log/info "  1. Start shadow-cljs: shadow-cljs watch todo-reframe")
  (log/info "  2. Open: http://localhost:8000")
  (log/info "")
  
  ;; Keep the server running
  (Thread/sleep Long/MAX_VALUE))