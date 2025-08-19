(require '[reactor.session_simple :as session])
(require '[reactor.xtdb-store :as xts])

(println "Testing time travel functionality...")

;; Initialize
(session/init!)

;; Create a test session
(def sess (session/create-session! "time-test" {:todos {}}))

(println "\n1. Initial state:" @sess)

;; Add first TODO
(swap! sess assoc-in [:todos "t1"] {:id "t1" :text "First" :completed false})
(Thread/sleep 100)
(println "\n2. After adding first TODO:" @sess)

;; Add second TODO  
(swap! sess assoc-in [:todos "t2"] {:id "t2" :text "Second" :completed false})
(Thread/sleep 100)
(println "\n3. After adding second TODO:" @sess)

;; Toggle first TODO
(swap! sess update-in [:todos "t1" :completed] not)
(Thread/sleep 100)
(println "\n4. After toggling first TODO:" @sess)

;; Check history
(let [history-info (session/get-history-info "time-test")]
  (println "\n5. History info:")
  (println "   Total states:" (:total-states history-info))
  (println "   Current index:" (:current-index history-info))
  (println "   Can undo:" (:can-undo history-info)))

;; Try undo
(println "\n6. Performing undo...")
(session/undo! "time-test")
(println "   State after undo:" @sess)

;; Try another undo
(println "\n7. Performing another undo...")
(session/undo! "time-test")
(println "   State after second undo:" @sess)

;; Try redo
(println "\n8. Performing redo...")
(session/redo! "time-test")
(println "   State after redo:" @sess)

(println "\nTest complete!")
(System/exit 0)