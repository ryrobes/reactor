(ns reactor.sql-pipeline-adapter
  "Adapter to integrate the new SQL pipeline with existing endpoints.
   Provides backward compatibility while migrating to the new system."
  (:require [reactor.sql-pipeline :as pipeline]
            [reactor.kafka-reactive :as kafka]
            [reactor.reactive.coordinator :as coordinator]
            [reactor.sse.broadcaster :as broadcaster]
            [reactor.xtdb-store :as store]
            [reactor.session_simple :as session]
            [reactor.meta-tracking :as meta]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

;; Forward declarations
(declare handle-sql-new-pipeline)
(declare handle-sql-legacy)

;; ============================================================================
;; Configuration
;; ============================================================================

(def ^:dynamic *use-new-pipeline* 
  "Feature flag to enable/disable new pipeline"
  (atom true))

(defn enable-new-pipeline! []
  (reset! *use-new-pipeline* true)
  (log/info "[ADAPTER] New SQL pipeline ENABLED"))

(defn disable-new-pipeline! []
  (reset! *use-new-pipeline* false)
  (log/info "[ADAPTER] New SQL pipeline DISABLED - using legacy system"))

;; ============================================================================
;; Request Transformation
;; ============================================================================

(defn transform-http-request
  "Transform HTTP request to pipeline context format"
  [req body]
  (let [session-id (or (get-in req [:query-params "session_id"])
                      (get-in req [:headers "x-session-id"])
                      "default")
        block-id (or (:block_id body)
                    (get-in req [:query-params "block_id"]))
        subscription-id (or (:subscription_id body)
                          (get-in req [:query-params "subscription_id"]))
        client-id (get-in req [:query-params "client_id"])]
    
    {:sql             (:sql body)
     :params          (:params body)
     :session-id      session-id
     :block-id        block-id
     :as-of           (:as_of body)
     :subscription-id subscription-id
     :client-id       client-id}))

;; ============================================================================
;; Response Transformation
;; ============================================================================

(defn transform-pipeline-response
  "Transform pipeline result to legacy response format"
  [result]
  (if (:success result)
    ;; Success response
    {:status 200
     :headers {"Content-Type" "application/json"
              "Access-Control-Allow-Origin" "*"
              "Access-Control-Allow-Methods" "GET, POST, OPTIONS"
              "Access-Control-Allow-Headers" "Content-Type, x-session-id"}
     :body (json/generate-string
            {:results (:results result)
             :subscription_id (:subscription-id result)
             :has_templates (:has-templates? result)
             :dependencies (:dependencies result)
             :execution_time (:execution-time result)
             :tables (:tables result)})}
    ;; Error response
    {:status 400
     :headers {"Content-Type" "application/json"
              "Access-Control-Allow-Origin" "*"}
     :body (json/generate-string
            {:error (get-in result [:error :message])
             :type (get-in result [:error :type])})}))

;; ============================================================================
;; Main Adapter Functions
;; ============================================================================

(defn handle-sql-request
  "Main entry point for SQL requests - routes to new or old pipeline"
  [req]
  (if @*use-new-pipeline*
    (handle-sql-new-pipeline req)
    (handle-sql-legacy req)))

(defn handle-sql-new-pipeline
  "Handle SQL request using the new pipeline"
  [req]
  (try
    (let [body (json/parse-string (slurp (:body req)) true)
          ;; Transform request to pipeline format
          pipeline-request (transform-http-request req body)
          
          _ (log/info "[ADAPTER] Processing SQL request through NEW pipeline"
                     "\n  SQL:" (str (take 100 (:sql pipeline-request)))
                     "\n  Session:" (:session-id pipeline-request)
                     "\n  Block:" (:block-id pipeline-request))
          
          ;; Execute through new pipeline
          result (pipeline/execute-sql pipeline-request)
          
          ;; Transform response to legacy format
          response (transform-pipeline-response result)]
      
      (log/info "[ADAPTER] New pipeline completed"
               "\n  Success:" (:success result)
               "\n  Row count:" (count (:results result []))
               "\n  Subscription:" (:subscription-id result))
      
      response)
    
    (catch Exception e
      (log/error e "[ADAPTER] Error in new pipeline")
      {:status 500
       :headers {"Content-Type" "application/json"
                "Access-Control-Allow-Origin" "*"}
       :body (json/generate-string
              {:error (.getMessage e)
               :type "pipeline-error"})})))

(defn handle-sql-legacy
  "Fallback to legacy SQL handling (would call existing code)"
  [req]
  ;; This would call the existing implementation
  ;; For now, just return a placeholder
  (log/warn "[ADAPTER] Legacy pipeline not implemented in adapter - should call original handler")
  {:status 501
   :headers {"Content-Type" "application/json"
            "Access-Control-Allow-Origin" "*"}
   :body (json/generate-string
          {:error "Legacy pipeline not available through adapter"})})

;; ============================================================================
;; Mutation Support
;; ============================================================================

(defn handle-sql-mutation
  "Handle SQL mutations (INSERT/UPDATE/DELETE) through the pipeline"
  [req]
  (try
    (let [body (json/parse-string (slurp (:body req)) true)
          pipeline-request (transform-http-request req body)
          
          _ (log/info "[ADAPTER] Processing SQL mutation"
                     "\n  SQL:" (:sql pipeline-request))
          
          ;; Execute mutation through pipeline
          ;; The pipeline will handle cascade detection for mutations
          result (pipeline/execute-sql pipeline-request)]
      
      (if (:success result)
        (do
          ;; For mutations, also trigger reactive updates
          (when-let [tables (:tables result)]
            (doseq [table tables]
              (log/info "[ADAPTER] Notifying table change:" table)
              ;; Trigger reactive updates for affected tables via coordinator
              (future
                (Thread/sleep 100)
                (coordinator/handle-table-change table))))
          
          (transform-pipeline-response result))
        (transform-pipeline-response result)))
    
    (catch Exception e
      (log/error e "[ADAPTER] Error in mutation")
      {:status 500
       :headers {"Content-Type" "application/json"
                "Access-Control-Allow-Origin" "*"}
       :body (json/generate-string
              {:error (.getMessage e)
               :type "mutation-error"})})))

;; ============================================================================
;; SSE Support for Pipeline
;; ============================================================================

(defn register-sse-subscription
  "Register an SSE subscription using the new pipeline"
  [session-id channel sql params]
  (let [pipeline-request {:sql sql
                          :params params
                          :session-id session-id}
        ;; Generate subscription through pipeline context
        ctx (-> (pipeline/create-context pipeline-request)
               pipeline/validate-request
               pipeline/load-session-state
               pipeline/resolve-templates
               pipeline/extract-metadata
               pipeline/generate-subscription-id)
        
        subscription-id (:subscription-id ctx)
        resolved-sql (:resolved-sql ctx)
        tables (:tables ctx)]
    
    ;; Register with Kafka system
    (kafka/register-query-subscription!
     subscription-id
     resolved-sql
     params
     (fn [result]
       ;; Callback to send results through SSE
       (when session-id
         (broadcaster/broadcast-to-session! 
           session-id
           {:type :update
            :subscription-id subscription-id
            :results result
            :timestamp (System/currentTimeMillis)})))
     session-id
     nil  ; client-id
     false  ; is-temporal
     (:dependencies ctx))
    
    ;; Register SSE channel
    (swap! kafka/sse-channels update session-id (fnil conj #{}) channel)
    
    (log/info "[ADAPTER] Registered SSE subscription"
             "\n  ID:" subscription-id
             "\n  Tables:" tables)
    
    subscription-id))

;; ============================================================================
;; Legacy API Compatibility
;; ============================================================================

(defn execute-sql
  "Legacy execute-sql function for backward compatibility.
   Transforms to new pipeline format and back."
  ([node sql] (execute-sql node sql [] {}))
  ([node sql params] (execute-sql node sql params {}))
  ([node sql params options]
   (let [context (merge
                  {:node node
                   :sql sql
                   :params (or params [])
                   :session-id (if (string? options) 
                                options  ; Legacy format: session-id as third param
                                (:session-id options))}
                  (when (map? options) options))
         result (pipeline/execute-pipeline context)]
     
     ;; Transform back to legacy format (just results array)
     (if (:error result)
       []  ; Legacy returned empty vector on error
       (:results result [])))))

(defn resolve-and-execute
  "Legacy template resolution and execution"
  [node template-name params options]
  (let [template (store/get-entity node :sql-templates template-name)
        resolved-sql (:sql template)
        resolved-params (if (:params template)
                         (map #(get params %) (:params template))
                         [])]
    (execute-sql node resolved-sql resolved-params options)))

(defn handle-reactive-sql
  "Legacy reactive SQL handler"
  [node session-id request]
  (let [context {:node node
                 :session-id session-id
                 :sql (:sql request)
                 :params (:params request [])
                 :subscribe true}
        result (pipeline/execute-pipeline context)]
    
    ;; Send results via SSE using broadcaster
    (when (:results result)
      (broadcaster/broadcast-to-session! 
        session-id
        {:type :update
         :results (:results result)
         :subscription-id (:subscription-id result)}))
    
    result))

;; ============================================================================
;; Testing & Debugging
;; ============================================================================

(defn test-pipeline-compatibility
  "Test that new pipeline produces same results as old system"
  [sql params session-id]
  (let [request {:sql sql
                :params params
                :session-id session-id}
        
        ;; Run through new pipeline
        new-result (pipeline/execute-sql request)
        
        ;; Would compare with old system result here
        ;; old-result (execute-sql-legacy sql params session-id)
        ]
    
    {:new-result new-result
     ;; :old-result old-result
     ;; :match? (= (:results new-result) (:results old-result))
     }))

(defn debug-pipeline-execution
  "Execute SQL through pipeline with detailed logging at each stage"
  [sql params session-id]
  (let [ctx (pipeline/create-context
             {:sql sql
              :params params
              :session-id session-id})
        
        log-stage (fn [stage-name ctx]
                    (log/info (str "[DEBUG-PIPELINE] Stage: " stage-name)
                             "\n  Error:" (:error ctx)
                             "\n  Has templates:" (:has-templates? ctx)
                             "\n  Resolved SQL:" (when (:resolved-sql ctx)
                                                (take 100 (:resolved-sql ctx)))
                             "\n  Tables:" (:tables ctx))
                    ctx)]
    
    (-> ctx
        (log-stage "Initial")
        pipeline/validate-request
        (log-stage "After validation")
        pipeline/load-session-state
        (log-stage "After session load")
        pipeline/resolve-templates
        (log-stage "After template resolution")
        pipeline/add-temporal-clause
        (log-stage "After temporal clause")
        pipeline/extract-metadata
        (log-stage "After metadata extraction")
        pipeline/generate-subscription-id
        (log-stage "After subscription ID")
        ;; Skip actual execution for debugging
        )))

;; ============================================================================
;; Gradual Migration Support
;; ============================================================================

(def migration-config
  "Configuration for gradual migration"
  (atom {:enabled-for-blocks true      ; Use new pipeline for block queries
         :enabled-for-temporal true    ; Use new pipeline for temporal queries
         :enabled-for-mutations false  ; Use new pipeline for mutations
         :enabled-for-regular true      ; Use new pipeline for regular queries
         :percentage 100}))             ; Percentage of requests to route to new pipeline

(defn should-use-new-pipeline?
  "Determine if a request should use the new pipeline based on migration config"
  [request]
  (let [config @migration-config
        sql (:sql request)
        is-block-query? (some? (:block-id request))
        is-temporal? (some? (:as-of request))
        is-mutation? (and sql (re-find #"(?i)^(INSERT|UPDATE|DELETE)" sql))
        random-percent (rand-int 100)]
    
    (and @*use-new-pipeline*
         (or (and is-block-query? (:enabled-for-blocks config))
             (and is-temporal? (:enabled-for-temporal config))
             (and is-mutation? (:enabled-for-mutations config))
             (and (not (or is-block-query? is-temporal? is-mutation?))
                  (:enabled-for-regular config)))
         (< random-percent (:percentage config)))))

(defn set-migration-percentage!
  "Set the percentage of requests that should use the new pipeline"
  [percentage]
  (swap! migration-config assoc :percentage percentage)
  (log/info "[ADAPTER] Migration percentage set to" percentage "%"))

(defn enable-for-query-type!
  "Enable new pipeline for specific query types"
  [query-type enabled?]
  (let [key (case query-type
             :blocks :enabled-for-blocks
             :temporal :enabled-for-temporal
             :mutations :enabled-for-mutations
             :regular :enabled-for-regular)]
    (swap! migration-config assoc key enabled?)
    (log/info "[ADAPTER] New pipeline" (if enabled? "ENABLED" "DISABLED") 
             "for" query-type "queries")))