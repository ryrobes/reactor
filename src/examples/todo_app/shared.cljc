(ns examples.todo-app.shared
  "Shared data structures and validation between client and server")

(defn valid-todo? [todo]
  (and (map? todo)
       (contains? todo :id)
       (contains? todo :text)
       (contains? todo :completed)
       (string? (:text todo))
       (boolean? (:completed todo))))

(defn valid-filter? [filter]
  (#{:all :active :completed} filter))

(def initial-db
  {:todos {}
   :filter :all
   :next-id 1
   :users {}})

;; Event schemas for validation
(def event-schemas
  {:add-todo [:text]
   :toggle-todo [:id]
   :delete-todo [:id]
   :update-todo-text [:id :text]
   :set-filter [:filter]
   :clear-completed []
   :toggle-all []})

(defn validate-event [event]
  (let [[event-type & args] event
        schema (get event-schemas event-type)]
    (when schema
      (= (count args) (count schema)))))