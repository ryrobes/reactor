(require '[reactor.sql-api :as sql])

;; Start the SQL API server
(sql/start-sql-server 8080)

;; Keep the process running
(Thread/sleep Long/MAX_VALUE)