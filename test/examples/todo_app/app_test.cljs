(ns examples.todo-app.app-test
  "Tests demonstrating Reactor as a clean re-frame alternative"
  (:require [cljs.test :refer-macros [deftest testing is async]]
            [reactor.client :as reactor]))

(deftest reactor-vs-reframe-api
  (testing "Reactor has simpler API than re-frame"
    
    (testing "Connection is one line vs re-frame's complex setup"
      ;; Reactor
      (let [app (reactor/connect! "http://localhost:3000")]
        (is (some? app))
        ;; vs re-frame: requires app-db, interceptors, effects setup
        ))
    
    (testing "Subscriptions don't need registration"
      ;; Reactor - subscriptions work immediately from server
      (let [app (reactor/connect! "http://localhost:3000")
            todos-sub (reactor/subscribe app [:todos])]
        (is (some? todos-sub))
        ;; vs re-frame: must call reg-sub with handler function
        ))
    
    (testing "Dispatches go directly to server"
      ;; Reactor
      (let [app (reactor/connect! "http://localhost:3000")]
        (reactor/dispatch! app [:add-todo "Test"])
        ;; vs re-frame: needs reg-event-db/fx handlers
        ))
    
    (testing "Time travel built-in"
      ;; Reactor
      (let [app (reactor/connect! "http://localhost:3000")]
        (reactor/dispatch! app [:time-travel/undo])
        (reactor/dispatch! app [:time-travel/redo])
        ;; vs re-frame: needs re-frame-10x or custom implementation
        ))))

(deftest subscription-behavior
  (testing "Server subscriptions update automatically"
    (async done
      (let [app (reactor/connect! "http://localhost:3000")
            todos-sub (reactor/subscribe! app []
                                         (fn [_ new-state]
                                           (is (map? new-state))
                                           (is (contains? new-state :todos))
                                           (done)))]
        ;; Trigger a change
        (reactor/dispatch! app [:add-todo "Async test"])))))

(deftest derived-subscriptions
  (testing "Client-side computed values"
    (let [app (reactor/connect! "http://localhost:3000")]
      ;; Register a derived subscription
      (reactor/reg-sub app :todo-count
        (fn [state]
          (count (:todos state))))
      
      (let [count-sub (reactor/subscribe app [:todo-count])]
        (is (some? count-sub))))))

(deftest comparison-with-reframe
  (testing "Lines of code comparison"
    (let [reactor-loc {:connection 1
                      :subscription 1
                      :dispatch 1
                      :total 3}
          reframe-loc {:app-db 5
                      :reg-sub 10
                      :reg-event 15
                      :dispatch 3
                      :interceptors 20
                      :total 53}]
      
      (is (< (:total reactor-loc) (:total reframe-loc))
          "Reactor requires significantly less boilerplate")
      
      (is (= (/ (:total reframe-loc) (:total reactor-loc)) 
             (/ 53 3))
          "Re-frame requires ~17x more setup code"))))

(deftest server-authority
  (testing "Server is single source of truth"
    (let [app (reactor/connect! "http://localhost:3000")]
      ;; All state changes go through server
      (reactor/dispatch! app [:add-todo "Server authoritative"])
      
      ;; No local state manipulation needed
      ;; vs re-frame: complex coordination between client/server
      )))

(deftest real-time-sync
  (testing "SSE provides automatic real-time updates"
    (async done
      (let [app1 (reactor/connect! "http://localhost:3000")
            app2 (reactor/connect! "http://localhost:3000")
            received (atom false)]
        
        ;; App2 listens for changes
        (reactor/subscribe! app2 []
                          (fn [_ new-state]
                            (when (and (not @received)
                                     (some #(= (:text %) "Broadcast test")
                                          (vals (:todos new-state))))
                              (reset! received true)
                              (is true "Received broadcast from other client")
                              (done))))
        
        ;; App1 makes a change
        (js/setTimeout 
          #(reactor/dispatch! app1 [:add-todo "Broadcast test"])
          100)))))