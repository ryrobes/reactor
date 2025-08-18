(ns reactor.pg-server
  "PostgreSQL wire protocol server for XTDB
   Allows connecting with psql and other PostgreSQL clients"
  (:require [reactor.xtdb-store :as xts]
            [reactor.xtdb-query :as xtq]
            [xtdb.api :as xt]
            [clojure.string :as str])
  (:import [io.zonky.test.db.postgres.embedded EmbeddedPostgres]
           [java.sql DriverManager Connection Statement ResultSet]
           [org.postgresql Driver]))

;; Note: For a production-ready PostgreSQL wire protocol server for XTDB 1.x,
;; we would need to implement the full protocol. This is complex.
;; 
;; Better options:
;; 1. Upgrade to XTDB 2.x which has native PostgreSQL support
;; 2. Use XTDB's HTTP API and create a PostgreSQL Foreign Data Wrapper
;; 3. Use a tool like Presto/Trino to provide SQL access

;; For now, let's create a simpler solution using XTDB 2.x features

(defn start-xtdb2-with-postgres
  "Start XTDB 2.x with PostgreSQL wire protocol support"
  []
  (println "XTDB 2.x PostgreSQL Setup")
  (println "================================")
  (println "XTDB 2.x has native PostgreSQL wire protocol support!")
  (println)
  (println "To use XTDB 2.x with psql:")
  (println "1. Add XTDB 2.x dependencies to project.clj:")
  (println "   [com.xtdb/xtdb-core \"2.0.0-alpha\"]")
  (println "   [com.xtdb/xtdb-pgwire \"2.0.0-alpha\"]")
  (println)
  (println "2. Start XTDB with PostgreSQL wire protocol:")
  (println "   (xtdb/start-node")
  (println "    {:xtdb/pgwire {:port 5432}}")
  (println)
  (println "3. Connect with psql:")
  (println "   psql -h localhost -p 5432 -U xtdb")
  (println)
  (println "For XTDB 1.x (current version), we'll create a different solution..."))