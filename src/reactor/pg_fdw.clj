(ns reactor.pg-fdw
  "PostgreSQL Foreign Data Wrapper approach for XTDB
   Use PostgreSQL's FDW to query XTDB via HTTP API"
  (:require [reactor.xtdb-store :as xts]
            [reactor.sql-api :as sql-api]
            [xtdb.api :as xt]))

;; PostgreSQL Foreign Data Wrapper Setup
;; =====================================
;; This approach uses PostgreSQL's built-in FDW capabilities
;; to query XTDB through an HTTP API

(defn generate-fdw-setup []
  "Generate PostgreSQL FDW setup commands"
  (str "-- PostgreSQL Foreign Data Wrapper Setup for XTDB\n"
       "-- ================================================\n"
       "\n"
       "-- 1. Install the http extension (if not already installed)\n"
       "-- Run as superuser:\n"
       "CREATE EXTENSION IF NOT EXISTS http;\n"
       "\n"
       "-- 2. Create the foreign data wrapper\n"
       "CREATE SERVER xtdb_server\n"
       "  FOREIGN DATA WRAPPER http_fdw\n"
       "  OPTIONS (base_url 'http://localhost:8080');\n"
       "\n"
       "-- 3. Create user mapping (optional)\n"
       "CREATE USER MAPPING FOR CURRENT_USER\n"
       "  SERVER xtdb_server;\n"
       "\n"
       "-- 4. Create foreign tables\n"
       "CREATE FOREIGN TABLE todos (\n"
       "  id TEXT,\n"
       "  text TEXT,\n"
       "  completed BOOLEAN\n"
       ") SERVER xtdb_server\n"
       "  OPTIONS (\n"
       "    path '/sql',\n"
       "    method 'POST',\n"
       "    headers '{\"Content-Type\": \"application/json\"}',\n"
       "    body '{\"query\": \"SELECT * FROM todos\"}'\n"
       "  );\n"
       "\n"
       "-- Now you can query XTDB from psql:\n"
       "-- SELECT * FROM todos;\n"))

(defn start-http-sql-server
  "Start HTTP SQL API server for FDW access"
  [port]
  (println "Starting HTTP SQL API Server for PostgreSQL FDW...")
  (sql-api/start-sql-server port)
  (println "\nPostgreSQL Foreign Data Wrapper Setup:")
  (println "======================================")
  (println (generate-fdw-setup))
  (println "\nAlternatively, use direct HTTP queries:")
  (println (str "curl -X POST http://localhost:" port "/sql \\"))
  (println "  -H 'Content-Type: application/json' \\")
  (println "  -d '{\"query\": \"SELECT * FROM todos\"}'"))

(defn -main [& args]
  (let [port (Integer/parseInt (or (first args) "8080"))]
    (start-http-sql-server port)))