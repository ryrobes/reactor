(ns reactor.examples.sql-demo
  "Demo showing SQL and HoneySQL query support with XTDB-backed Reactor"
  (:require [reactor.xtdb-store :as xts]
            [reactor.xtdb-query :as xtq]
            [reactor.sse-xtdb :as sse]
            [org.httpkit.client :as http]
            [clojure.edn :as edn]
            [cheshire.core :as json]))

(defn demo-sql-queries []
  (println "\n=== Reactor SQL/HoneySQL Query Demo ===\n")
  
  ;; Start XTDB node
  (let [node (xts/start-xtdb-node)
        port 9000
        base-url (str "http://localhost:" port)]
    
    (try
      ;; Start SSE server
      (println "Starting XTDB SSE server on port" port "...")
      (let [server (sse/start-xtdb-sse-server node port)]
        
        (Thread/sleep 500)
        
        ;; Create sample data
        (println "\n1. Creating sample data...")
        (doseq [[id data] [["user-1" {:name "Alice" :age 30 :role "developer" :active true}]
                           ["user-2" {:name "Bob" :age 25 :role "designer" :active true}]
                           ["user-3" {:name "Charlie" :age 35 :role "manager" :active false}]
                           ["task-1" {:title "Write tests" :assignee "Alice" :priority 1 :done false}]
                           ["task-2" {:title "Design UI" :assignee "Bob" :priority 2 :done false}]
                           ["task-3" {:title "Review PR" :assignee "Alice" :priority 1 :done true}]]]
          (let [resp @(http/post (str base-url "/update")
                                {:body (pr-str {:entity-id id :data data})
                                 :headers {"Content-Type" "application/edn"}
                                 :as :text})]
            (println (format "  Created %s: %s" id 
                           (if (= 200 (:status resp)) "✓" "✗")))))
        
        ;; Demonstrate different query formats
        (println "\n2. Query with Keypath:")
        (let [resp @(http/post (str base-url "/query")
                              {:body (pr-str {:query ["user-1"]})
                               :headers {"Content-Type" "application/edn"}
                               :as :text})
              result (edn/read-string (:body resp))]
          (println "  Query: [\"user-1\"]")
          (println "  Result:" (pr-str (:result result))))
        
        ;; Sync entities to tables for SQL queries
        (println "\n3. Syncing entities to SQL tables...")
        (doseq [id ["user-1" "user-2" "user-3" "task-1" "task-2" "task-3"]]
          (let [resp @(http/post (str base-url "/sync-table")
                                {:body (pr-str {:entity-id id})
                                 :headers {"Content-Type" "application/edn"}
                                 :as :text})]
            (println (format "  Synced %s to table: %s" id
                           (if (= 200 (:status resp)) "✓" "✗")))))
        
        ;; Build and execute SQL query
        (println "\n4. Building SQL with HoneySQL DSL:")
        (let [honeysql-map {:table "users"
                           :select ["name" "role"]
                           :where [:= :active true]
                           :order-by [[:name :asc]]}
              resp @(http/post (str base-url "/sql-builder")
                              {:body (json/generate-string honeysql-map)
                               :headers {"Content-Type" "application/json"}
                               :as :text})
              result (json/parse-string (:body resp) true)]
          (println "  HoneySQL:" (pr-str honeysql-map))
          (println "  Generated SQL:" (:sql result)))
        
        ;; Execute HoneySQL query
        (println "\n5. Executing HoneySQL query:")
        (let [query {:select [:title :assignee :priority]
                    :from :tasks
                    :where [:and 
                           [:= :done false]
                           [:< :priority 3]]
                    :order-by [[:priority :asc]]}
              resp @(http/post (str base-url "/query")
                              {:body (pr-str {:query query})
                               :headers {"Content-Type" "application/edn"}
                               :as :text})
              result (edn/read-string (:body resp))]
          (println "  Query:" (pr-str query))
          (println "  Result:" (pr-str (:result result))))
        
        ;; Execute raw SQL query
        (println "\n6. Executing raw SQL query:")
        (let [sql "SELECT name, age FROM users WHERE age > 25"
              resp @(http/post (str base-url "/query")
                              {:body (pr-str {:query sql})
                               :headers {"Content-Type" "application/edn"}
                               :as :text})
              result (edn/read-string (:body resp))]
          (println "  SQL:" sql)
          (println "  Result:" (pr-str (:result result))))
        
        ;; Demonstrate query subscription with polling
        (println "\n7. Setting up query subscription with polling:")
        (println "  Creating subscription for active users...")
        (println "  (In a real app, this would use EventSource API)")
        
        ;; Use curl to demonstrate SSE
        (let [sse-url (str base-url "/subscribe?"
                          "query=" (java.net.URLEncoder/encode "[\"user-1\"]" "UTF-8")
                          "&query-format=keypath"
                          "&format=edn"
                          "&poll-ms=1000")]
          (println "  SSE URL:" sse-url)
          (println "  Would receive real-time updates as data changes...")
          
          ;; Simulate an update
          (Thread/sleep 500)
          (println "\n  Updating user-1 age to 31...")
          @(http/post (str base-url "/update")
                     {:body (pr-str {:entity-id "user-1" 
                                   :data {:name "Alice" :age 31 :role "developer" :active true}})
                      :headers {"Content-Type" "application/edn"}
                      :as :text})
          (println "  Subscription would receive update notification"))
        
        ;; Demonstrate query builder DSL
        (println "\n8. Using Query Builder DSL:")
        (let [built-query (xtq/select :tasks [:title :priority]
                                     (xtq/where := :done false)
                                     (xtq/order-by :priority :asc)
                                     (xtq/limit 5))
              sql (xtq/honeysql->xtql built-query)]
          (println "  Built query:" (pr-str built-query))
          (println "  SQL:" sql))
        
        ;; Demonstrate entity flattening
        (println "\n9. Entity Flattening for SQL Tables:")
        (let [nested {:user {:profile {:name "David"
                                      :email "david@example.com"}
                            :preferences {:theme "dark"
                                        :notifications true}}
                     :metadata {:created "2024-01-01"}}
              flattened (xtq/flatten-entity nested)]
          (println "  Nested structure:" (pr-str nested))
          (println "  Flattened for SQL:" (pr-str flattened))
          (println "  Table columns would be:")
          (doseq [k (keys flattened)]
            (println (format "    - %s" k))))
        
        (println "\n=== Demo Complete ===")
        (println "The XTDB-backed Reactor now supports:")
        (println "  ✓ Traditional keypath queries")
        (println "  ✓ HoneySQL query building")
        (println "  ✓ Raw SQL queries")
        (println "  ✓ Real-time subscriptions with polling")
        (println "  ✓ Entity flattening for SQL compatibility")
        (println "  ✓ Session-based data isolation")
        
        ;; Cleanup
        (server))
      
      (finally
        (xts/stop-xtdb-node node)))))

;; Run the demo
(defn -main []
  (demo-sql-queries))

;; To run: lein run -m reactor.examples.sql-demo