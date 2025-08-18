(ns examples.one-file-demo
  "Complete Reactor demo in ONE file - just run it!"
  (:require [reactor.session_simple :as session]
            [xtdb.api :as xt]
            [clojure.pprint :as pp]))

;; Initialize Reactor
(session/init!)

;; Define your app logic (like re-frame, but simpler!)
(session/reg-event-db :add-user
  (fn [db [name]]
    (assoc-in db [:users name] {:name name 
                                 :score 0
                                 :joined (java.util.Date.)})))

(session/reg-event-db :score
  (fn [db [name points]]
    (update-in db [:users name :score] + points)))

(session/reg-event-db :remove-user
  (fn [db [name]]
    (update db :users dissoc name)))

;; Track events
(session/reg-event-db :track
  (fn [db [event-type data]]
    (update db :events conj {:type event-type
                             :data data
                             :timestamp (System/currentTimeMillis)})))

;; Navigation event
(session/reg-event-db :navigate
  (fn [db [page]]
    (assoc db :view page)))

;; Create our demo session
(def game (session/create-session! "game-1" {}))

(defn demo-basic []
  (println "\n=== 🎯 Basic Usage ===")
  
  ;; Dispatch events
  (session/dispatch "game-1" [:add-user "Alice"])
  (session/dispatch "game-1" [:add-user "Bob"])
  (session/dispatch "game-1" [:score "Alice" 10])
  (session/dispatch "game-1" [:score "Bob" 5])
  (session/dispatch "game-1" [:score "Alice" 7])
  
  (println "Current state:" @game))

(defn demo-time-travel []
  (println "\n=== ⏰ Time Travel ===")
  
  (println "Before undo:" @game)
  (session/undo! "game-1")
  (println "After 1 undo:" @game)
  (session/undo! "game-1") 
  (println "After 2 undos:" @game)
  (session/redo! "game-1")
  (println "After redo:" @game))

(defn demo-sql-queries []
  (println "\n=== 🔍 SQL Queries Over History ===")
  
  ;; Query: "Show me all score changes"
  (let [entity-id (keyword "session" "game-1")
        history (take 5 (xt/entity-history (xt/db (:node game)) entity-id :desc))]
    (println "\nState history (last 5 changes):")
    (doseq [entry history]
      (let [state (:state entry)
            alice-score (get-in state [:users "Alice" :score])]
        (when alice-score
          (println (str "  " (::xt/tx-time entry) ": Alice has " alice-score " points")))))
    
    (println "\nCurrent scores:")
    (let [users (:users @game)]
      (doseq [[name data] users]
        (println (str "  " name ": " (:score data) " points"))))))

(defn demo-multiple-sessions []
  (println "\n=== 👥 Multiple Sessions (Multiplayer) ===")
  
  ;; Each player gets their own session
  (def player1 (session/create-session! "player-1" {:view "home"}))
  (def player2 (session/create-session! "player-2" {:view "home"}))
  
  (session/dispatch "player-1" [:navigate "game"])
  (session/dispatch "player-2" [:navigate "lobby"])
  
  (println "Player 1 view:" @player1)
  (println "Player 2 view:" @player2)
  (println "Sessions are isolated!"))

(defn demo-analytics []
  (println "\n=== 📊 Analytics Example ===")
  
  ;; Create an analytics session
  (def analytics (session/create-session! "analytics" {:events []}))
  
  (session/dispatch "analytics" [:track :page-view "home"])
  (Thread/sleep 100)
  (session/dispatch "analytics" [:track :click "signup"])
  (Thread/sleep 100)
  (session/dispatch "analytics" [:track :page-view "dashboard"])
  
  ;; Query: "What's the user journey?"
  (let [events (:events @analytics)]
    (println "\nUser journey:")
    (doseq [e events]
      (println (str "  " (:type e) " -> " (:data e))))))

(defn -main []
  (println "")
  (println "🚀 REACTOR DEMO - Re-frame + Time Travel + SQL")
  (println "=" (apply str (repeat 50 "=")))
  
  (demo-basic)
  (demo-time-travel)
  (demo-sql-queries)
  (demo-multiple-sessions)
  (demo-analytics)
  
  (println "\n✨ That's Reactor! Same API as re-frame, but with:")
  (println "  • Automatic persistence (XTDB)")
  (println "  • Time travel (undo/redo/jump)")
  (println "  • SQL queries over history")
  (println "  • Multi-session support")
  (println "  • Works on server AND client")
  (println "\n🎯 All in ~20 lines for basic apps!"))

;; Run with: lein run -m examples.one-file-demo