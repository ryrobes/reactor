(ns reactor.sql-pipeline
  "Centralized SQL execution pipeline - simplified and testable.
   All SQL requests flow through this single pipeline.
   Both manual requests and reactions use the same flow!"
  (:require [reactor.sql-template :as template]
            [reactor.sql-resolver :as resolver]
            [reactor.sql-parser :as parser]
            [reactor.session_simple :as session]
            [reactor.kafka-reactive :as kafka]
            [reactor.xtdb-store :as xts]
            [reactor.meta-tracking :as meta]
            ;[reactor.time-travel-sql :as time-travel]
            [reactor.subscriptions.store :as sub-store]
            [reactor.subscriptions.differ :as differ]
            [reactor.log :as log]
            [clojure.string :as str]
            [clojure.core.async :as async]))

;; ============================================================================
;; Pipeline Context Structure
;; ============================================================================

(defn create-context
  "Create initial pipeline context from request parameters"
  [{:keys [sql params session-id block-id as-of subscription-id client-id 
           is-reaction? previous-results]}]
  {:request-id      (str (java.util.UUID/randomUUID))
   :sql             sql
   :params          params
   :session-id      (or session-id "default")
   :block-id        block-id
   :as-of           as-of
   :subscription-id subscription-id
   :client-id       client-id
   :timestamp       (System/currentTimeMillis)
   :is-reaction?    (boolean is-reaction?)  ; Is this a reaction to a change?
   :previous-results previous-results       ; For diff calculation
   
   ;; These will be populated by pipeline stages
   :session-state   nil
   :resolved-sql    nil
   :has-templates?  false
   :dependencies    []
   :is-temporal?    false
   :tables          []
   :should-cascade? false
   :cascade-targets []
   :result          nil
   :diff            nil      ; Diff between previous and current results
   :subscription    nil      ; The subscription object from store
   :error           nil})

;; ============================================================================
;; Stage 1: Validation
;; ============================================================================

(defn validate-request
  "Validate the request context has required fields"
  [ctx]
  (cond
    (str/blank? (:sql ctx))
    (assoc ctx :error {:type :validation :message "SQL query is required"})
    
    (and (:params ctx) (not (sequential? (:params ctx))))
    (assoc ctx :error {:type :validation :message "Parameters must be a sequence"})
    
    :else ctx))

;; ============================================================================
;; Stage 2: Session State Loading (Read-only)
;; ============================================================================

(defn load-session-state
  "Load session state without mutations - creates immutable snapshot"
  [ctx]
  (if (:error ctx)
    ctx
    (try
      (let [session-id (:session-id ctx)
            session-obj (when session-id (session/get-session session-id))
            session-state (when session-obj (session/get-state session-obj))]
        (assoc ctx :session-state (or session-state {})))
      (catch Exception e
        (log/warn "Failed to load session state:" (.getMessage e))
        (assoc ctx :session-state {})))))

;; ============================================================================
;; Stage 3: Template Resolution (Pure Function)
;; ============================================================================

(defn resolve-templates
  "Resolve SQL templates - pure function that doesn't mutate state"
  [ctx]
  (if (:error ctx)
    ctx
    (let [sql (:sql ctx)
          session-state (:session-state ctx)
          has-templates? (resolver/has-templates? sql)
          as-of (:as-of ctx)]
      
      (if has-templates?
        (try
          ;; Use temporal template resolver if we have an as-of timestamp
          (let [result (if as-of
                        ;; Temporal template resolution - adds FOR SYSTEM_TIME to inner queries
                        (do
                          (log/info "[PIPELINE] Using temporal template resolution for as-of:" as-of)
                          (require '[reactor.sql-template-temporal :as temp-template])
                          (let [temp-resolver (ns-resolve 'reactor.sql-template-temporal 
                                                         'resolve-sql-templates-with-deps-and-temporal)]
                            (temp-resolver sql session-state as-of)))
                        ;; Regular template resolution
                        (template/resolve-sql-templates-with-deps sql session-state))
                resolved-sql (:sql result)
                dependencies (:dependencies result)]
            
            (log/info "[PIPELINE] Resolved templates"
                     "\n  Temporal:" (boolean as-of)
                     "\n  Dependencies:" dependencies
                     "\n  Original length:" (count sql)
                     "\n  Resolved length:" (count resolved-sql))
            
            (assoc ctx
                   :resolved-sql resolved-sql
                   :has-templates? true
                   :dependencies dependencies))
          (catch Exception e
            (log/error "Template resolution failed:" e)
            (assoc ctx :error {:type :template-resolution 
                               :message (.getMessage e)})))
        ;; No templates, use original SQL
        (assoc ctx
               :resolved-sql sql
               :has-templates? false
               :dependencies [])))))

;; ============================================================================
;; Stage 4: Temporal Clause Addition (Pure Function)
;; ============================================================================

(defn add-temporal-clause
  "Add temporal clause if as-of timestamp provided"
  [ctx]
  (if (or (:error ctx) (not (:as-of ctx)))
    ctx
    (let [resolved-sql (:resolved-sql ctx)
          as-of (:as-of ctx)
          has-templates? (:has-templates? ctx)]
      (cond
        ;; Already has temporal clause (from template resolution or manual)
        (str/includes? resolved-sql "FOR SYSTEM_TIME")
        (do
          (log/debug "[PIPELINE] SQL already has temporal clause, skipping addition")
          (assoc ctx :is-temporal? true))
        
        ;; If we had templates, temporal clauses were already added during template resolution
        has-templates?
        (do
          (log/debug "[PIPELINE] Templates were resolved with temporal support, skipping clause addition")
          (assoc ctx :is-temporal? true))
        
        ;; No templates, add temporal clause normally
        :else
        (let [temporal-sql (parser/add-as-of-clause resolved-sql as-of)]
          (log/debug "[PIPELINE] Added temporal clause"
                    "\n  Timestamp:" as-of)
          (assoc ctx
                 :resolved-sql temporal-sql
                 :is-temporal? true))))))

;; ============================================================================
;; Stage 5: Extract Metadata (Pure Function)
;; ============================================================================

(defn extract-metadata
  "Extract tables and other metadata from SQL"
  [ctx]
  (if (or (:error ctx) (nil? (:resolved-sql ctx)))
    ctx
    (let [sql (:resolved-sql ctx)
          tables (if (str/blank? sql)
                  []
                  (kafka/extract-tables-from-sql sql))
          is-query? (and sql 
                        (str/starts-with? (str/lower-case (str/trim sql)) "select"))
          is-mutation? (and sql (not is-query?))]
      (assoc ctx
             :tables tables
             :is-query? is-query?
             :is-mutation? is-mutation?))))

;; ============================================================================
;; Stage 6: Generate Subscription ID
;; ============================================================================

(defn generate-subscription-id
  "Generate or validate subscription ID"
  [ctx]
  (if (or (:error ctx) (:subscription-id ctx))
    ctx
    (let [sub-id (cond
                   ;; Block-based subscription
                   (:block-id ctx)
                   (str "block-" (:block-id ctx))
                   
                   ;; Temporal query subscription
                   (:is-temporal? ctx)
                   (str "temporal-" (hash [(:resolved-sql ctx) (:as-of ctx)]))
                   
                   ;; Default: UUID-based
                   :else
                   (str "sql-" (java.util.UUID/randomUUID)))]
      (assoc ctx :subscription-id sub-id))))

;; ============================================================================
;; Stage 7: Register Subscription (Side Effect)
;; ============================================================================

(defn register-subscription
  "Register subscription with Kafka reactive system - only for initial queries, not reactions"
  [ctx]
  (if (or (:error ctx) 
          (:is-mutation? ctx)  ; Don't subscribe to mutations
          (:is-reaction? ctx)  ; Don't re-register during reactions
          (not (:is-query? ctx))
          (= (:session-id ctx) "session-query"))
    ctx
    (try
      (let [sub-id (:subscription-id ctx)
            ;; Use ORIGINAL SQL for registration, not resolved
            ;; Templates need to be re-resolved on each execution
            sql (:sql ctx)
            params (:params ctx)
            session-id (:session-id ctx)
            client-id (:client-id ctx)
            is-temporal? (:is-temporal? ctx)
            parent-blocks (:dependencies ctx)
            ;; Pass the tables extracted from resolved SQL
            tables (:tables ctx)
            
            ;; Create callback function that pushes results via SSE
            ;; This callback is called when the subscription is re-executed
            callback (kafka/create-subscription-callback session-id)]
        
        (kafka/register-query-subscription!
         sub-id sql params callback session-id client-id is-temporal? parent-blocks tables)
        
        (log/info "[PIPELINE] Registered subscription"
                 "\n  ID:" sub-id
                 "\n  Tables:" (:tables ctx)
                 "\n  Temporal:" is-temporal?)
        
        ctx)
      (catch Exception e
        (log/error "Failed to register subscription:" e)
        ctx))))

;; ============================================================================
;; Stage 8: Execute Query (Side Effect)
;; ============================================================================

(defn execute-query
  "Execute the SQL query against the database"
  [ctx]
  (if (:error ctx)
    ctx
    (try
      (let [node @session/default-node
            sql (:resolved-sql ctx)
            params (:params ctx)
            start-time (System/currentTimeMillis)
            
            result (if params
                    (xts/execute-sql node sql params)
                    (xts/execute-sql node sql))
            
            execution-time (- (System/currentTimeMillis) start-time)]
        
        (log/debug "[PIPELINE] Query executed"
                  "\n  Time:" execution-time "ms"
                  "\n  Row count:" (count (:results result [])))
        
        ;; Track metrics
        (when (:subscription-id ctx)
          (meta/track-event! "sql-execution" "query"
                           {:sql sql
                            :execution-time execution-time
                            :row-count (count (:results result []))}
                           (:session-id ctx)))
        
        (assoc ctx
               :result result
               :execution-time execution-time))
      (catch Exception e
        (log/error "Query execution failed:" e)
        (assoc ctx :error {:type :execution
                           :message (.getMessage e)})))))

;; ============================================================================ 
;; Stage 9: Trigger Reactive Updates for Mutations (Side Effect)
;; ============================================================================

(defn trigger-reactive-updates
  "For mutations, trigger reactive updates on affected tables"
  [ctx]
  ;; DISABLED: Kafka consumer already monitors the transaction log and triggers reactions
  ;; This was causing duplicate reactions. The Kafka consumer is the authoritative source.
  #_(if (or (:error ctx) 
          (not (:is-mutation? ctx))
          (empty? (:tables ctx)))
    ctx
    (do
      ;; Trigger updates asynchronously
      (future
        (Thread/sleep 100) ; Small delay to ensure transaction commits
        (doseq [table (:tables ctx)]
          (log/info "[PIPELINE] Triggering reactive updates for table:" table)
          (try
            ;; Find and re-execute affected subscriptions
            (let [affected-subs (kafka/find-affected-subscriptions [table])]
              (log/debug "[PIPELINE] Found" (count affected-subs) 
                        "subscriptions watching table" table)
              (doseq [sub-id affected-subs]
                (kafka/request-re-execution! sub-id)))
            (catch Exception e
              (log/error e "Failed to trigger reactive updates for table" table)))))
      ctx))
  ;; Just pass through the context unchanged
  ctx)

;; ============================================================================
;; Stage 10: Identify Cascade Targets (Pure Function)
;; ============================================================================

(defn identify-cascade-targets
  "Identify blocks that should be cascaded to"
  [ctx]
  (if (or (:error ctx)
          (not (:block-id ctx))
          (not (:is-mutation? ctx)))
    ctx
    (let [session-state (:session-state ctx)
          block-id (:block-id ctx)
          ;; Find blocks that depend on this one
          dependent-blocks (template/find-dependent-blocks session-state block-id)]
      
      (if (seq dependent-blocks)
        (do
          (log/info "[PIPELINE] Found cascade targets"
                   "\n  Source block:" block-id
                   "\n  Targets:" dependent-blocks)
          (assoc ctx
                 :should-cascade? true
                 :cascade-targets dependent-blocks))
        ctx))))

;; ============================================================================
;; Forward Declarations
;; ============================================================================

(declare execute-pipeline)

;; ============================================================================
;; Stage 10: Trigger Cascades (Async Side Effect)
;; ============================================================================

(defn trigger-cascades
  "Trigger cascade execution asynchronously"
  [ctx]
  (if (or (:error ctx) (not (:should-cascade? ctx)))
    ctx
    (let [targets (:cascade-targets ctx)
          session-id (:session-id ctx)]
      
      ;; Fire and forget - don't wait for cascade completion
      (async/go
        (doseq [target-block targets]
          (try
            (log/info "[PIPELINE] Triggering cascade for block:" target-block)
            ;; Execute cascade by creating new pipeline context
            (let [block-sql (get-in (:session-state ctx) 
                                   [:canvas :blocks (keyword target-block) :sql])
                  cascade-ctx (create-context
                              {:sql block-sql
                               :session-id session-id
                               :block-id target-block})]
              ;; Recursively execute pipeline for cascade
              (execute-pipeline cascade-ctx))
            (catch Exception e
              (log/error "Cascade execution failed for block" target-block e)))))
      
      ctx)))

;; ============================================================================
;; Subscription Management Stages
;; ============================================================================

(defn load-or-create-subscription
  "Load existing subscription or create new one"
  [ctx]
  (if (or (:error ctx) 
          (:is-mutation? ctx))
    ctx
    (let [sub-id (:subscription-id ctx)
          current-sql (:sql ctx)
          current-tables (:tables ctx)
          current-as-of (:as-of ctx)]
      (if-let [existing (sub-store/get-subscription sub-id)]
        ;; Check if SQL or temporal state has changed (for block-based subscriptions)
        (let [sql-changed? (and current-sql
                                (not= current-sql (:sql existing)))
              as-of-changed? (not= current-as-of (:as-of existing))
              updates (cond-> {:last-accessed (System/currentTimeMillis)}
                       sql-changed? (assoc :sql current-sql
                                          :tables current-tables)
                       as-of-changed? (assoc :as-of current-as-of))]
          (when sql-changed?
            (log/info "[PIPELINE] Updating subscription SQL for" sub-id
                     "\n  Old SQL:" (subs (:sql existing) 0 (min 50 (count (:sql existing))))
                     "\n  New SQL:" (subs current-sql 0 (min 50 (count current-sql)))))
          (when as-of-changed?
            (log/info "[PIPELINE] Updating subscription temporal state for" sub-id
                     "\n  Old as-of:" (:as-of existing)
                     "\n  New as-of:" current-as-of))
          (sub-store/update! sub-id updates)
          (assoc ctx 
                 :subscription (sub-store/get-subscription sub-id)
                 :previous-results (:last-results existing)))
        ;; Create new subscription
        (let [new-sub {:id sub-id
                      :sql (:sql ctx)
                      :resolved-sql (:resolved-sql ctx)
                      :params (:params ctx)
                      :tables (:tables ctx)
                      :session-id (:session-id ctx)
                      :client-id (:client-id ctx)
                      :block-id (:block-id ctx)
                      :dependencies (:dependencies ctx)
                      :is-temporal? (:is-temporal? ctx)
                      :as-of (:as-of ctx)  ; Store the temporal timestamp
                      :created-at (System/currentTimeMillis)}]
          (sub-store/add! new-sub)
          (assoc ctx :subscription new-sub))))))

(defn calculate-diff
  "Calculate diff between previous and current results"
  [ctx]
  (if (or (:error ctx)
          (:is-mutation? ctx)
          (not (:is-query? ctx)))
    ctx
    (let [previous (:previous-results ctx)
          current (get-in ctx [:result :results] [])
          ;; Use field-level diff by default
          diff (when previous
                (differ/calculate-diff previous current {:mode :field}))]
      (assoc ctx :diff diff))))

(defn update-subscription-state
  "Update subscription with latest results"
  [ctx]
  (if (or (:error ctx)
          (:is-mutation? ctx)
          (not (:subscription ctx)))
    ctx
    (let [sub-id (get-in ctx [:subscription :id])
          results (get-in ctx [:result :results] [])]
      (sub-store/update! sub-id 
                        {:last-results results
                         :last-executed (System/currentTimeMillis)
                         :last-row-count (count results)})
      ctx)))

;; ============================================================================
;; Main Pipeline Execution
;; ============================================================================

(defn execute-pipeline
  "Execute the complete SQL pipeline"
  [initial-context]
  (-> initial-context
      validate-request
      load-session-state
      resolve-templates
      add-temporal-clause
      extract-metadata
      generate-subscription-id
      load-or-create-subscription   ; NEW: Manage subscription lifecycle
      register-subscription         ; Register with Kafka (if needed)
      execute-query
      calculate-diff                ; NEW: Calculate diff for subscriptions
      update-subscription-state     ; NEW: Update subscription state
      trigger-reactive-updates      ; For mutations
      identify-cascade-targets      ; For block mutations
      trigger-cascades))            ; Execute cascades

;; ============================================================================
;; Public API
;; ============================================================================

(defn execute-sql
  "Main entry point for SQL execution through the pipeline"
  [{:keys [sql params session-id block-id as-of subscription-id client-id] :as request}]
  (when as-of
    (log/info "[PIPELINE] Received temporal query with as-of:" as-of
             "\n  SQL:" (if (> (count sql) 100) 
                           (str (subs sql 0 100) "...") 
                           sql)))
  (let [ctx (create-context request)
        result (execute-pipeline ctx)]
    
    (if (:error result)
      {:success false
       :error (:error result)}
      {:success true
       :subscription-id (:subscription-id result)
       :results (get-in result [:result :results] [])
       :diff (:diff result)  ; Include diff if available
       :execution-time (:execution-time result)
       :has-templates? (:has-templates? result)
       :dependencies (:dependencies result)
       :tables (:tables result)})))

(defn execute-reaction
  "Execute a reaction to a data change - uses the SAME pipeline!
   This is called when Kafka detects a change."
  [subscription-id]
  (if-let [subscription (sub-store/get-subscription subscription-id)]
    (let [;; Build context from stored subscription
          ctx (create-context
               {:sql (:sql subscription)
                :params (:params subscription)
                :session-id (:session-id subscription)
                :block-id (:block-id subscription)
                :as-of (:as-of subscription)  ; Preserve temporal timestamp for reactions
                :subscription-id subscription-id
                :client-id (:client-id subscription)
                :is-reaction? true
                :previous-results (:last-results subscription)})
          ;; Execute through the SAME pipeline
          result (execute-pipeline ctx)]
      
      (if (:error result)
        {:success false
         :error (:error result)
         :subscription-id subscription-id}
        {:success true
         :subscription-id subscription-id
         :results (get-in result [:result :results] [])
         :diff (:diff result)
         :is-reaction true}))
    {:success false
     :error {:type :not-found
             :message (str "Subscription not found: " subscription-id)}}))

;; ============================================================================
;; Testing Helpers - Pure Functions for Easy Testing
;; ============================================================================

(defn test-template-resolution
  "Test helper for template resolution"
  [sql session-state]
  (let [ctx {:sql sql :session-state session-state}]
    (resolve-templates ctx)))

(defn test-temporal-clause
  "Test helper for temporal clause addition"
  [sql as-of]
  (let [ctx {:resolved-sql sql :as-of as-of}]
    (add-temporal-clause ctx)))

(defn test-cascade-detection
  "Test helper for cascade detection"
  [block-id session-state]
  (let [ctx {:block-id block-id 
             :session-state session-state
             :is-mutation? true}]
    (identify-cascade-targets ctx)))