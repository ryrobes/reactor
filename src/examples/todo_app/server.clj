(ns examples.todo-app.server
  "TODO app server with the new clean Reactor API"
  (:require [reactor.server :as r]))

(defn compute-filtered-todos
  "Compute filtered todos based on current filter"
  [db]
  (let [todos (vals (:todos db {}))
        filter-type (:filter db :all)
        filtered (case filter-type
                  :active (vec (filter (complement :completed) todos))
                  :completed (vec (filter :completed todos))
                  :all (vec todos)
                  (vec todos))]
    (println "Computing filtered todos - filter:" filter-type 
             "total:" (count todos) 
             "filtered:" (count filtered))
    (assoc db :filtered-todos filtered)))

(defn wrap-handler
  "Wrap handler to compute filtered todos after each state change"
  [handler]
  (fn [db args]
    (-> (handler db args)
        compute-filtered-todos)))

(defn -main []
  (r/start! 
    :port 4000
    :init-fn (fn []
              ;; Set up initial state computation for new sessions
              (println "TODO app server started on port 4000"))
    :handlers {;; Initialize new session with computed filtered todos
               :init-session (fn [db _]
                              (compute-filtered-todos 
                               (or db {:todos {} :filter :all})))
               
               ;; Todo management
               :add-todo (wrap-handler 
                          (fn [db [todo]]
                            (assoc-in db [:todos (:id todo)] todo)))
               
               :toggle-todo (wrap-handler
                             (fn [db [id]]
                               (update-in db [:todos id :completed] not)))
               
               :delete-todo (wrap-handler
                             (fn [db [id]]
                               (update db :todos dissoc id)))
               
               :edit-todo (wrap-handler
                           (fn [db [id text]]
                             (assoc-in db [:todos id :text] text)))
               
               :clear-completed (wrap-handler
                                 (fn [db _]
                                   (update db :todos 
                                     (fn [todos]
                                       (into {} (remove (fn [[_ todo]] (:completed todo)) todos))))))
               
               :toggle-all (wrap-handler
                            (fn [db [completed?]]
                              (update db :todos
                                (fn [todos]
                                  (into {} (map (fn [[id todo]]
                                                 [id (assoc todo :completed completed?)])
                                               todos))))))
               
               ;; Filter management - also wraps to compute filtered todos
               :set-filter (wrap-handler
                            (fn [db [filter-type]]
                              (println "Setting filter to:" filter-type)
                              (assoc db :filter filter-type)))}))