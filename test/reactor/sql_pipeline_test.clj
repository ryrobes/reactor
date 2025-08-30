(ns reactor.sql-pipeline-test
  "Comprehensive tests for the SQL pipeline"
  (:require [clojure.test :refer :all]
            [reactor.sql-pipeline :as pipeline]))

;; ============================================================================
;; Test Data
;; ============================================================================

(def test-session-state
  {:canvas {:blocks {:block1 {:sql "SELECT * FROM users WHERE active = true"}
                      :block2 {:sql "SELECT * FROM orders WHERE user_id IN ({{block1.sql}})"}
                      :block3 {:sql "SELECT COUNT(*) FROM ({{block2.sql}}) as orders"}
                      :circular1 {:sql "SELECT * FROM ({{circular2.sql}})"}
                      :circular2 {:sql "SELECT * FROM ({{circular1.sql}})"}}}})

;; ============================================================================
;; Context Creation Tests
;; ============================================================================

(deftest test-create-context
  (testing "Creates context with all provided fields"
    (let [ctx (pipeline/create-context
               {:sql "SELECT * FROM users"
                :params [1 2 3]
                :session-id "test-session"
                :block-id "block123"
                :as-of "2024-01-01T00:00:00Z"
                :subscription-id "sub-456"
                :client-id "client-789"})]
      
      (is (= "SELECT * FROM users" (:sql ctx)))
      (is (= [1 2 3] (:params ctx)))
      (is (= "test-session" (:session-id ctx)))
      (is (= "block123" (:block-id ctx)))
      (is (= "2024-01-01T00:00:00Z" (:as-of ctx)))
      (is (= "sub-456" (:subscription-id ctx)))
      (is (= "client-789" (:client-id ctx)))
      (is (string? (:request-id ctx)))
      (is (number? (:timestamp ctx)))))
  
  (testing "Provides defaults for missing fields"
    (let [ctx (pipeline/create-context {:sql "SELECT 1"})]
      (is (= "default" (:session-id ctx)))
      (is (nil? (:block-id ctx)))
      (is (nil? (:as-of ctx))))))

;; ============================================================================
;; Validation Tests
;; ============================================================================

(deftest test-validate-request
  (testing "Valid request passes through"
    (let [ctx {:sql "SELECT * FROM users" :params [1 2]}
          result (pipeline/validate-request ctx)]
      (is (nil? (:error result)))
      (is (= ctx result))))
  
  (testing "Empty SQL is rejected"
    (let [ctx {:sql "" :params []}
          result (pipeline/validate-request ctx)]
      (is (= :validation (get-in result [:error :type])))
      (is (string? (get-in result [:error :message])))))
  
  (testing "Nil SQL is rejected"
    (let [ctx {:sql nil}
          result (pipeline/validate-request ctx)]
      (is (= :validation (get-in result [:error :type])))))
  
  (testing "Non-sequential params are rejected"
    (let [ctx {:sql "SELECT ?" :params "not-a-sequence"}
          result (pipeline/validate-request ctx)]
      (is (= :validation (get-in result [:error :type]))))))

;; ============================================================================
;; Template Resolution Tests
;; ============================================================================

(deftest test-resolve-templates
  (testing "SQL without templates passes through unchanged"
    (let [ctx {:sql "SELECT * FROM users"
               :session-state test-session-state}
          result (pipeline/resolve-templates ctx)]
      (is (= "SELECT * FROM users" (:resolved-sql result)))
      (is (false? (:has-templates? result)))
      (is (empty? (:dependencies result)))))
  
  (testing "Simple template reference is resolved"
    (let [ctx {:sql "SELECT * FROM ({{block1.sql}}) as subq"
               :session-state test-session-state}
          result (pipeline/resolve-templates ctx)]
      (is (string? (:resolved-sql result)))
      (is (true? (:has-templates? result)))
      (is (= ["block1"] (:dependencies result)))
      ;; The resolved SQL should contain the actual query, not the template
      (is (not (.contains (:resolved-sql result) "{{"))))) 
  
  (testing "Nested template references are resolved recursively"
    (let [ctx {:sql "{{block3.sql}}"
               :session-state test-session-state}
          result (pipeline/resolve-templates ctx)]
      (is (true? (:has-templates? result)))
      ;; Should have all dependencies in chain
      (is (seq (:dependencies result)))
      (is (not (.contains (:resolved-sql result) "{{")))))
  
  (testing "Missing block reference returns error"
    (let [ctx {:sql "SELECT * FROM ({{nonexistent.sql}})"
               :session-state test-session-state}
          result (pipeline/resolve-templates ctx)]
      ;; Depending on implementation, this might return unchanged SQL or error
      (is (or (:error result)
              (.contains (:resolved-sql result) "{{nonexistent.sql}}")))))
  
  (testing "Circular references are detected"
    (let [ctx {:sql "{{circular1.sql}}"
               :session-state test-session-state}
          result (pipeline/resolve-templates ctx)]
      ;; Should either have an error or throw exception (caught)
      (is (or (:error result)
              (= "{{circular1.sql}}" (:resolved-sql result)))))))

;; ============================================================================
;; Temporal Clause Tests
;; ============================================================================

(deftest test-add-temporal-clause
  (testing "No temporal clause when as-of is nil"
    (let [ctx {:resolved-sql "SELECT * FROM users"
               :as-of nil}
          result (pipeline/add-temporal-clause ctx)]
      (is (= "SELECT * FROM users" (:resolved-sql result)))
      (is (false? (:is-temporal? result false)))))
  
  (testing "Temporal clause is added when as-of provided"
    (let [ctx {:resolved-sql "SELECT * FROM users"
               :as-of "2024-01-01T00:00:00Z"}
          result (pipeline/add-temporal-clause ctx)]
      (is (.contains (:resolved-sql result) "FOR SYSTEM_TIME AS OF"))
      (is (.contains (:resolved-sql result) "2024-01-01T00:00:00Z"))
      (is (true? (:is-temporal? result)))))
  
  (testing "Existing temporal clause is not duplicated"
    (let [ctx {:resolved-sql "SELECT * FROM users FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z'"
               :as-of "2024-02-01T00:00:00Z"}
          result (pipeline/add-temporal-clause ctx)]
      ;; Should not add another temporal clause
      (is (= 1 (count (re-seq #"FOR SYSTEM_TIME" (:resolved-sql result)))))
      (is (true? (:is-temporal? result))))))

;; ============================================================================
;; Metadata Extraction Tests
;; ============================================================================

(deftest test-extract-metadata
  (testing "Extract tables from SELECT query"
    (let [ctx {:resolved-sql "SELECT * FROM users JOIN orders ON users.id = orders.user_id"}
          result (pipeline/extract-metadata ctx)]
      (is (contains? (set (:tables result)) "users"))
      (is (contains? (set (:tables result)) "orders"))
      (is (true? (:is-query? result)))
      (is (false? (:is-mutation? result)))))
  
  (testing "Extract table from INSERT statement"
    (let [ctx {:resolved-sql "INSERT INTO users (name, email) VALUES (?, ?)"}
          result (pipeline/extract-metadata ctx)]
      (is (contains? (set (:tables result)) "users"))
      (is (false? (:is-query? result)))
      (is (true? (:is-mutation? result)))))
  
  (testing "Extract table from UPDATE statement"
    (let [ctx {:resolved-sql "UPDATE users SET active = false WHERE id = ?"}
          result (pipeline/extract-metadata ctx)]
      (is (contains? (set (:tables result)) "users"))
      (is (false? (:is-query? result)))
      (is (true? (:is-mutation? result)))))
  
  (testing "Extract table from DELETE statement"
    (let [ctx {:resolved-sql "DELETE FROM users WHERE inactive = true"}
          result (pipeline/extract-metadata ctx)]
      (is (contains? (set (:tables result)) "users"))
      (is (false? (:is-query? result)))
      (is (true? (:is-mutation? result))))))

;; ============================================================================
;; Subscription ID Generation Tests
;; ============================================================================

(deftest test-generate-subscription-id
  (testing "Uses existing subscription ID if provided"
    (let [ctx {:subscription-id "existing-sub-123"}
          result (pipeline/generate-subscription-id ctx)]
      (is (= "existing-sub-123" (:subscription-id result)))))
  
  (testing "Generates block-based ID for block queries"
    (let [ctx {:block-id "myblock"}
          result (pipeline/generate-subscription-id ctx)]
      (is (= "block-myblock" (:subscription-id result)))))
  
  (testing "Generates temporal ID for temporal queries"
    (let [ctx {:is-temporal? true
               :resolved-sql "SELECT * FROM users FOR SYSTEM_TIME AS OF '2024'"
               :as-of "2024-01-01T00:00:00Z"}
          result (pipeline/generate-subscription-id ctx)]
      (is (.startsWith (:subscription-id result) "temporal-"))))
  
  (testing "Generates UUID-based ID for regular queries"
    (let [ctx {:resolved-sql "SELECT * FROM users"}
          result (pipeline/generate-subscription-id ctx)]
      (is (.startsWith (:subscription-id result) "sql-")))))

;; ============================================================================
;; Cascade Detection Tests
;; ============================================================================

(deftest test-identify-cascade-targets
  (testing "No cascade for queries"
    (let [ctx {:block-id "block1"
               :is-mutation? false
               :session-state test-session-state}
          result (pipeline/identify-cascade-targets ctx)]
      (is (false? (:should-cascade? result false)))
      (is (empty? (:cascade-targets result [])))))
  
  (testing "Identifies dependent blocks for mutations"
    (let [ctx {:block-id "block1"
               :is-mutation? true
               :session-state test-session-state}
          result (pipeline/identify-cascade-targets ctx)]
      ;; block2 depends on block1
      (is (true? (:should-cascade? result)))
      (is (contains? (set (:cascade-targets result)) "block2"))))
  
  (testing "No cascade when no dependents"
    (let [ctx {:block-id "block3"  ; Nothing depends on block3
               :is-mutation? true
               :session-state test-session-state}
          result (pipeline/identify-cascade-targets ctx)]
      (is (false? (:should-cascade? result false)))
      (is (empty? (:cascade-targets result []))))))

;; ============================================================================
;; Pipeline Error Propagation Tests
;; ============================================================================

(deftest test-error-propagation
  (testing "Error in early stage prevents later stages from running"
    (let [ctx {:sql ""  ; Invalid SQL
               :session-state test-session-state}
          result (-> ctx
                    pipeline/validate-request
                    pipeline/resolve-templates
                    pipeline/add-temporal-clause)]
      ;; Error from validation should be present
      (is (= :validation (get-in result [:error :type])))
      ;; Later stages should not have modified the context
      (is (nil? (:resolved-sql result)))
      (is (nil? (:has-templates? result))))))

;; ============================================================================
;; Testing Helper Functions
;; ============================================================================

(deftest test-helper-functions
  (testing "test-template-resolution helper"
    (let [result (pipeline/test-template-resolution
                  "SELECT * FROM ({{block1.sql}})"
                  test-session-state)]
      (is (string? (:resolved-sql result)))
      (is (true? (:has-templates? result)))))
  
  (testing "test-temporal-clause helper"
    (let [result (pipeline/test-temporal-clause
                  "SELECT * FROM users"
                  "2024-01-01T00:00:00Z")]
      (is (.contains (:resolved-sql result) "FOR SYSTEM_TIME"))))
  
  (testing "test-cascade-detection helper"
    (let [result (pipeline/test-cascade-detection
                  "block1"
                  test-session-state)]
      (is (seq (:cascade-targets result))))))

;; ============================================================================
;; Integration Tests
;; ============================================================================

(deftest test-full-pipeline-flow
  (testing "Simple query flows through pipeline"
    (let [ctx (pipeline/create-context
               {:sql "SELECT * FROM users"
                :session-id "test"})
          ;; Run through non-side-effect stages
          result (-> ctx
                    pipeline/validate-request
                    pipeline/load-session-state
                    pipeline/resolve-templates
                    pipeline/add-temporal-clause
                    pipeline/extract-metadata
                    pipeline/generate-subscription-id)]
      
      (is (nil? (:error result)))
      (is (= "SELECT * FROM users" (:resolved-sql result)))
      (is (false? (:has-templates? result)))
      (is (contains? (set (:tables result)) "users"))
      (is (true? (:is-query? result)))
      (is (string? (:subscription-id result)))))
  
  (testing "Template query with temporal clause"
    (let [ctx (pipeline/create-context
               {:sql "SELECT * FROM ({{block1.sql}}) WHERE id = ?"
                :params [123]
                :as-of "2024-01-01T00:00:00Z"
                :session-id "test"})
          ;; Run through non-side-effect stages  
          result (-> ctx
                    pipeline/validate-request
                    (assoc :session-state test-session-state)
                    pipeline/resolve-templates
                    pipeline/add-temporal-clause
                    pipeline/extract-metadata
                    pipeline/generate-subscription-id)]
      
      (is (nil? (:error result)))
      (is (true? (:has-templates? result)))
      (is (true? (:is-temporal? result)))
      (is (= ["block1"] (:dependencies result)))
      (is (.contains (:resolved-sql result) "FOR SYSTEM_TIME")))))

;; ============================================================================
;; Mutation Tests
;; ============================================================================

(deftest test-mutation-handling
  (testing "INSERT mutation doesn't create subscription"
    (let [ctx (pipeline/create-context
               {:sql "INSERT INTO users (name, email) VALUES (?, ?)"
                :params ["John" "john@example.com"]
                :session-id "test"})
          result (-> ctx
                    pipeline/validate-request
                    pipeline/resolve-templates  ; This sets resolved-sql
                    pipeline/extract-metadata
                    pipeline/register-subscription)]
      (is (true? (:is-mutation? result)))
      (is (false? (:is-query? result)))
      ;; Should not have registered subscription (mutations don't subscribe)
      (is (nil? (:subscription-registered result)))))
  
  (testing "UPDATE mutation identifies affected table"
    (let [ctx (pipeline/create-context
               {:sql "UPDATE orders SET status = ? WHERE id = ?"
                :params ["shipped" 123]})
          result (-> ctx
                    pipeline/resolve-templates  ; This sets resolved-sql
                    pipeline/extract-metadata)]
      (is (true? (:is-mutation? result)))
      (is (contains? (set (:tables result)) "orders"))))
  
  (testing "DELETE mutation with template resolution"
    (let [ctx (pipeline/create-context
               {:sql "DELETE FROM orders WHERE user_id IN ({{block1.sql}})"
                :session-id "test"})
          result (-> ctx
                    (assoc :session-state test-session-state)
                    pipeline/resolve-templates
                    pipeline/extract-metadata)]
      (is (true? (:has-templates? result)))
      (is (true? (:is-mutation? result)))
      (is (contains? (set (:tables result)) "orders"))
      (is (= ["block1"] (:dependencies result)))))
  
  (testing "Block mutation triggers cascade detection"
    (let [ctx (pipeline/create-context
               {:sql "INSERT INTO users (name) VALUES (?)"
                :params ["Test"]
                :block-id "block1"
                :session-id "test"})
          result (-> ctx
                    (assoc :session-state test-session-state)
                    pipeline/resolve-templates  ; Sets resolved-sql from sql
                    pipeline/extract-metadata   ; Now can detect mutation
                    pipeline/identify-cascade-targets)]
      (is (true? (:is-mutation? result)))
      ;; block2 depends on block1, so should cascade
      (is (true? (:should-cascade? result)))
      (is (contains? (set (:cascade-targets result)) "block2")))))

;; ============================================================================
;; Complex Pipeline Scenarios
;; ============================================================================

(deftest test-complex-scenarios
  (testing "Temporal query with templates and parameters"
    (let [ctx (pipeline/create-context
               {:sql "SELECT * FROM ({{block1.sql}}) WHERE created_at > ?"
                :params ["2024-01-01"]
                :as-of "2024-06-01T00:00:00Z"
                :session-id "test"
                :block-id "complex-block"})
          result (-> ctx
                    pipeline/validate-request
                    (assoc :session-state test-session-state)
                    pipeline/resolve-templates
                    pipeline/add-temporal-clause
                    pipeline/extract-metadata
                    pipeline/generate-subscription-id)]
      
      (is (nil? (:error result)))
      (is (true? (:has-templates? result)))
      (is (true? (:is-temporal? result)))
      (is (= ["block1"] (:dependencies result)))
      (is (= "block-complex-block" (:subscription-id result)))
      (is (.contains (:resolved-sql result) "FOR SYSTEM_TIME"))))
  
  (testing "Mutation with cascade chain"
    (let [;; Create session state where block3 depends on block2 depends on block1
          chained-state {:canvas {:blocks {:block1 {:sql "SELECT * FROM base"}
                                          :block2 {:sql "SELECT * FROM ({{block1.sql}}) WHERE x = 1"}
                                          :block3 {:sql "SELECT COUNT(*) FROM ({{block2.sql}})"}}}}
          ctx (pipeline/create-context
               {:sql "UPDATE base SET value = ?"
                :params [42]
                :block-id "block1"
                :session-id "test"})
          result (-> ctx
                    (assoc :session-state chained-state)
                    pipeline/resolve-templates  ; This sets resolved-sql
                    pipeline/extract-metadata
                    pipeline/identify-cascade-targets)]
      
      (is (true? (:is-mutation? result)))
      (is (true? (:should-cascade? result)))
      ;; Should identify block2 as direct dependent
      (is (contains? (set (:cascade-targets result)) "block2"))
      ;; Note: block3 would be triggered when block2 cascades
      )))

;; ============================================================================
;; Run Tests
;; ============================================================================

(defn run-tests []
  (clojure.test/run-tests 'reactor.sql-pipeline-test))