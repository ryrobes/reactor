# Reactor - XTDB powered full stack Re-frame (**WIP!)

![alt text](http://textfiles.com/underconstruction/ViennaStrasse9671construction.gif "do not use, yet!")
![alt text](http://textfiles.com/underconstruction/ththe300exhavenunderconstruction.gif "do not use, yet!")
![alt text](http://textfiles.com/underconstruction/ViennaStrasse9671construction.gif "do not use, yet!")

```clj
[reactor "0.1.0"]
```

Write re-frame-style reactive apps that run on both client and server, with automatic persistence, time travel, reactive rules, and SQL.

## Why Reactor?

Re-frame is amazing for client-side state management, but you still need:
- REST/GraphQL APIs
- Database setup
- Server state sync
- Session management  
- Persistence layer

**Reactor gives you all of this in the same number of lines as a client-only re-frame app.**

## Quick Example

```clojure
;; server.clj - Your ENTIRE server!
(ns my-app.server
  (:require [reactor.server :as r]))

(r/start! 
  :port 4000
  :handlers {:inc (fn [db _] (update db :count (fnil inc 0)))
             :dec (fn [db _] (update db :count (fnil dec 0)))
             :set (fn [db [n]] (assoc db :count n))})

;; client.cljs - Familiar re-frame style
(ns my-app.client
  (:require [reactor.core :as r]
            [reagent.dom :as rdom]))

(defn counter []
  (let [count (r/subscribe [:get [:count]])]  ;; Just like re-frame!
    [:div
     [:h1 "Count: " @count]
     [:button {:on-click #(r/dispatch! [:inc])} "+"]
     [:button {:on-click #(r/dispatch! [:dec])} "-"]
     [:button {:on-click #(r/undo!)} "Undo"]      ;; Time travel!
     [:button {:on-click #(r/redo!)} "Redo"]]))

(defn init! []
  (r/init! {:server-url "http://localhost:4000"})
  (rdom/render [counter] (.getElementById js/document "app")))
```

**That's it!** You now have:
- ✅ Full-stack reactive state
- ✅ Automatic persistence (survives restarts)
- ✅ Time travel (undo/redo)
- ✅ Real-time sync via SSE
- ✅ Session isolation
- ✅ SQL queries over your state history

## Core API

### Client (`reactor.core`)

```clojure
;; Initialize connection
(r/init! {:server-url "http://localhost:4000"
          :session-id "user-123"})  ; Optional - for multi-user

;; Dispatch events (just like re-frame)
(r/dispatch! [:add-todo {:id 1 :text "Learn Reactor"}])
(r/dispatch! [:toggle-todo 1])

;; Subscribe to data (reactive atoms)
(def todos (r/subscribe [:get [:todos]]))
(def count (r/subscribe [:get [:stats :todo-count]]))

;; Time travel
(r/undo!)
(r/redo!)

;; SQL queries over your app history!
(r/q '{:find [?time ?todos]
       :where [[?e :state ?s ?time]
               [(get ?s :todos) ?todos]]})
```

### Server (`reactor.server`)

```clojure
;; One-function setup
(r/start!
  :port 4000
  :handlers {;; Event handlers (same as re-frame!)
             :add-todo (fn [db [todo]]
                        (assoc-in db [:todos (:id todo)] todo))
             
             :toggle-todo (fn [db [id]]
                           (update-in db [:todos id :done] not))
             
             :clear-completed (fn [db _]
                               (update db :todos 
                                 #(into {} (remove (fn [[_ v]] (:done v)) %))))}
  
  ;; Optional: Custom session ID extraction
  :session-id-fn (fn [request]
                   (get-in request [:session :user-id] "default")))
```

## Advanced Examples

### Custom Subscriptions

```clojure
;; Register computed subscriptions
(r/reg-sub :active-todos
  (fn [db _]
    (->> (:todos db)
         vals
         (filter (complement :done)))))

(r/reg-sub :todo-stats
  (fn [db _]
    (let [todos (vals (:todos db))]
      {:total (count todos)
       :active (count (filter (complement :done) todos))
       :completed (count (filter :done todos))})))

;; Use them
(def active (r/subscribe [:active-todos]))
(def stats (r/subscribe [:todo-stats]))
```

### SQL Queries for Analytics

```clojure
;; Query your app's history
(defn show-history []
  (-> (r/q '{:find [?time ?count]
             :where [[?e :state ?s ?time]
                     [(get-in ?s [:stats :todo-count]) ?count]]
             :order-by [[?time :desc]]
             :limit 10})
      (.then (fn [results]
               (doseq [[time count] results]
                 (println "At" time "had" count "todos"))))))

;; Find when a specific todo was added
(-> (r/q '{:find [?time]
           :where [[?e :state ?s ?time]
                   [(get-in ?s [:todos 42]) ?todo]
                   [(some? ?todo)]]
           :limit 1})
    (.then #(println "Todo 42 was added at" (first %))))
```

### Multi-User Sessions

```clojure
;; Server - Extract session from auth
(r/start!
  :handlers {...}
  :session-id-fn (fn [request]
                   (or (get-in request [:headers "x-session-id"])
                       (get-in request [:session :id])
                       "anonymous")))

;; Client - Pass session ID
(r/init! {:server-url "http://localhost:4000"
          :session-id current-user-id})
```

### Server-Side Subscriptions (Coming Soon)

```clojure
;; Define derived state on server
(r/reg-sub :todo-list-view
  (fn [db [filter-type]]
    (let [todos (vals (:todos db))]
      (case filter-type
        :active (filter (complement :done) todos)
        :completed (filter :done todos)
        todos))))

;; Client automatically syncs derived state
(def filtered (r/subscribe [:todo-list-view :active]))
```

## How It Works

1. **Events are re-frame handlers** - But they run on the server
2. **State is in XTDB** - A bitemporal database that stores everything
3. **Changes stream to clients** - Via Server-Sent Events (SSE)
4. **Time travel is free** - XTDB tracks all history automatically
5. **SQL queries work** - Because your state is in a real database

## Installation

```clojure
;; project.clj or deps.edn
[reactor "0.1.0-SNAPSHOT"]

;; Required dependencies (already included)
[xtdb "1.24.3"]        ; Bitemporal database
[reagent "1.2.0"]      ; React wrapper
[http-kit "2.7.0"]     ; WebServer
[cheshire "5.12.0"]    ; JSON
```

## Running the Examples

### Magic Counter
```bash
# Terminal 1 - Start server
lein run -m examples.magic-counter.server

# Terminal 2 - Start client
shadow-cljs watch magic

# Open http://localhost:8080/magic-counter.html
```

### TODO App with Sessions
```bash
# Terminal 1
lein run -m examples.todo-app.server-simple

# Terminal 2  
shadow-cljs watch todo-session

# Open http://localhost:8080/todo-session.html
```

## Comparison with Re-frame

| Feature | Re-frame | Reactor |
|---------|----------|---------|
| Reactive subscriptions | ✅ | ✅ |
| Event dispatch | ✅ | ✅ |
| Computed subscriptions | ✅ | ✅ |
| Dev tools | ✅ | ✅ (via XTDB) |
| Server state | ❌ (need API) | ✅ (automatic) |
| Persistence | ❌ (need backend) | ✅ (built-in) |
| Time travel | ✅ (client only) | ✅ (persisted) |
| Multi-user | ❌ (need backend) | ✅ (sessions) |
| SQL queries | ❌ | ✅ |
| WebSocket/SSE | ❌ (manual) | ✅ (automatic) |
| Lines of code | ~100 client + ~200 server | ~20 total |

## Philosophy

Reactor is built on the idea that **state management shouldn't require different mental models for client and server**. 

In traditional apps, you have:
- Client state (Re-frame, Redux, etc.)
- Server state (REST, GraphQL)  
- Database state (SQL, queries)
- Sync logic (WebSockets, polling)

In Reactor, you have:
- **State** (that happens to work everywhere)

## Status

Reactor is an experimental proof-of-concept exploring what "re-frame for the full stack" could look like. It's not production-ready but demonstrates that we can have the same elegant state management across client and server.

Ryan Robitaille - Copyright © 2025