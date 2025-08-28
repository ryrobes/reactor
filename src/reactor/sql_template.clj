(ns reactor.sql-template
  "SQL template resolution for dynamic query references"
  (:require [clojure.string :as str]
            [reactor.log :as log]))

(defn extract-template-refs
  "Extract all {{blockId.sql}} references from SQL"
  [sql]
  (let [pattern #"\{\{([^}]+)\.sql\}\}"
        matches (re-seq pattern sql)]
    (map second matches)))

(defn get-block-sql
  "Get the current SQL for a block ID from the session state"
  [session-state block-id]
  ;; Try both string and keyword forms since blocks might be stored either way
  (or (get-in session-state [:canvas :blocks block-id :sql])
      (get-in session-state [:canvas :blocks (keyword block-id) :sql])))

(defn resolve-templates
  "Recursively resolve all template references in SQL"
  [sql session-state resolved-blocks]
  (log/info {:message "Resolving SQL templates"
            :sql sql
            :resolved-count (count resolved-blocks)})
  
  (let [refs (extract-template-refs sql)]
    (if (empty? refs)
      ;; No more templates to resolve
      sql
      ;; Resolve each reference
      (reduce
        (fn [current-sql block-id]
          ;; Check for circular references
          (if (contains? resolved-blocks block-id)
            (do
              (log/warn {:message "Circular reference detected"
                        :block-id block-id
                        :resolved-blocks resolved-blocks})
              (throw (ex-info "Circular block reference detected" 
                             {:block-id block-id
                              :resolved-blocks resolved-blocks})))
            ;; Get the block's SQL
            (if-let [block-sql (get-block-sql session-state block-id)]
              (let [_ (log/info {:message "Template resolution: Found parent block SQL"
                                :block-id block-id
                                :sql-length (count block-sql)
                                :sql-preview (if (> (count block-sql) 100)
                                               (str (subs block-sql 0 100) "...")
                                               block-sql)})
                    ;; Remove any LIMIT from the referenced SQL
                    clean-sql (str/replace block-sql #"(?i)\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?" "")
                    ;; Recursively resolve any templates in the referenced SQL
                    resolved-sql (resolve-templates clean-sql 
                                                   session-state 
                                                   (conj resolved-blocks block-id))
                    ;; Wrap as subquery
                    wrapped-sql (str "(" resolved-sql ")")]
                (log/info {:message "Resolved template reference"
                          :block-id block-id
                          :original-sql block-sql
                          :wrapped-sql wrapped-sql})
                ;; Replace the template reference with the resolved SQL
                (str/replace current-sql 
                           (str "{{" block-id ".sql}}")
                           wrapped-sql))
              (do
                (log/warn {:message "Block not found for template reference"
                          :block-id block-id
                          :available-blocks (keys (:blocks (:canvas session-state)))})
                ;; Return SQL unchanged if block not found
                current-sql))))
        sql
        refs))))

(defn find-dependent-blocks
  "Find all blocks that reference a given block ID in their SQL templates"
  [session-state block-id]
  (log/info {:message "Finding dependent blocks"
            :block-id block-id})
  (let [blocks (get-in session-state [:canvas :blocks])
        block-id-str (if (keyword? block-id)
                       (name block-id)
                       (str block-id))
        pattern (re-pattern (str "\\{\\{" (java.util.regex.Pattern/quote block-id-str) "\\.sql\\}\\}"))]
    (reduce
      (fn [deps [id block]]
        (if (and (:sql block)
                 (re-find pattern (:sql block)))
          (conj deps (if (keyword? id) (name id) (str id)))
          deps))
      []
      blocks)))

(defn find-dependent-subscriptions
  "Find all active subscriptions that reference a given block ID in their SQL templates.
   This searches through the raw SQL strings of active subscriptions."
  [active-subscriptions block-id]
  (log/info {:message "Finding dependent subscriptions"
            :block-id block-id
            :total-subscriptions (count active-subscriptions)})
  (let [;; Strip colon if block-id is a keyword, handle both forms
        block-id-str (cond
                       (keyword? block-id) (name block-id)
                       (string? block-id) (if (str/starts-with? block-id ":")
                                           (subs block-id 1)
                                           block-id)
                       :else (str block-id))
        ;; Create pattern to match {{block-id.sql}} in SQL strings
        pattern (re-pattern (str "\\{\\{" (java.util.regex.Pattern/quote block-id-str) "\\.sql\\}\\}"))]
    (reduce
      (fn [deps [sub-id subscription]]
        (if (and (:query subscription)
                 (re-find pattern (:query subscription)))
          (do
            (log/info {:message "Found dependent subscription"
                      :sub-id sub-id
                      :block-id block-id-str
                      :sql-snippet (when (:query subscription)
                                    (subs (:query subscription) 0 
                                          (min 100 (count (:query subscription)))))})
            (conj deps sub-id))
          deps))
      []
      active-subscriptions)))

(defn get-cascade-chain
  "Recursively find all blocks that depend on the given block, directly or indirectly"
  [session-state block-id & [visited depth]]
  (let [visited (or visited #{})
        depth (or depth 0)
        max-depth 10  ; Maximum cascade depth to prevent runaway cascades
        block-id-str (if (keyword? block-id)
                       (name block-id)
                       (str block-id))]
    (cond
      ;; Circular dependency detected
      (contains? visited block-id-str) 
      []
      
      ;; Max depth reached
      (>= depth max-depth)
      (do
        (log/warn "[CASCADE] Maximum cascade depth reached:" max-depth "for block" block-id-str)
        [])
      
      ;; Normal case - find dependencies
      :else
      (let [direct-deps (find-dependent-blocks session-state block-id-str)
            visited (conj visited block-id-str)]
        (concat direct-deps
                (mapcat #(get-cascade-chain session-state % visited (inc depth)) direct-deps))))))

(defn resolve-sql-templates
  "Main entry point for SQL template resolution"
  [sql session-state]
  (try
    (resolve-templates sql session-state #{})
    (catch Exception e
      (log/error {:message "Failed to resolve SQL templates"
                 :error (.getMessage e)
                 :sql sql})
      ;; Return original SQL if resolution fails
      sql)))

(defn resolve-sql-templates-with-deps
  "Resolve SQL template references and return both the resolved SQL and the block dependencies"
  [sql session-state]
  (let [refs (extract-template-refs sql)]
    (if (empty? refs)
      {:sql sql :dependencies []}
      (let [resolved (resolve-templates sql session-state #{})
            ;; Extract just the block IDs without .sql suffix
            deps (mapv #(str/replace % #"\.sql$" "") refs)]
        {:sql resolved :dependencies deps}))))