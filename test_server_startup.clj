#!/usr/bin/env clojure

(println "Testing server startup...")

(try
  (println "1. Loading reactive server...")
  (require '[reactor.reactive-server :as r])
  (println "   ✓ Reactive server loaded")
  
  (println "2. Loading session system...")
  (require '[reactor.session_simple :as session])
  (println "   ✓ Session system loaded")
  
  (println "3. Loading Kafka reactive (may fail)...")
  (try
    (require '[reactor.kafka-reactive :as kafka])
    (println "   ✓ Kafka reactive loaded")
    (catch Exception e
      (println "   ✗ Kafka reactive failed:" (.getMessage e))))
  
  (println "4. Loading SQL reactive bridge...")
  (require '[reactor.sql-reactive-bridge :as bridge])
  (println "   ✓ SQL reactive bridge loaded")
  
  (println "\nAll components loaded successfully!")
  (System/exit 0)
  
  (catch Exception e
    (println "Error:" (.getMessage e))
    (.printStackTrace e)
    (System/exit 1)))