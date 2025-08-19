(ns TestRabbitSQL
  (:require [reactor.session-simple :as session]
            [reactor.xtdb-store :as xts]))

(defn -main [& args]
  (println "Initializing session...")
  (session/init!)
  
  (println "Getting node...")
  (def node @session/default-node) 
  (println "Node:" node)
  
  (println "Testing execute-sql-query...")
  (def result (session/execute-sql-query node "SELECT * FROM sales LIMIT 3"))
  (println "Result:" result)
  
  (System/exit 0))

(-main)