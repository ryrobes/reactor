(ns test-field-diff
  "Test script to verify field-based diffing"
  (:require [reactor.kafka-reactive :as kafka]
            [reactor.structural-diff :as sdiff]))

(defn test-basic-field-diff []
  (println "\n=== Testing Basic Field Diff ===")
  (let [old-row {:id 1 :name "Alice" :age 30 :city "NYC"}
        new-row {:id 1 :name "Alice" :age 31 :city "Boston"}
        diff (kafka/compute-field-diff old-row new-row)]
    (println "Old row:" old-row)
    (println "New row:" new-row)
    (println "Field diff:" diff)
    (println "Expected: Changes to :age and :city fields only")
    diff))

(defn test-field-addition []
  (println "\n=== Testing Field Addition ===")
  (let [old-row {:id 2 :name "Bob"}
        new-row {:id 2 :name "Bob" :email "bob@test.com"}
        diff (kafka/compute-field-diff old-row new-row)]
    (println "Old row:" old-row)
    (println "New row:" new-row)
    (println "Field diff:" diff)
    (println "Expected: :email field added")
    diff))

(defn test-field-removal []
  (println "\n=== Testing Field Removal ===")
  (let [old-row {:id 3 :name "Charlie" :temp "temp-value"}
        new-row {:id 3 :name "Charlie"}
        diff (kafka/compute-field-diff old-row new-row)]
    (println "Old row:" old-row)
    (println "New row:" new-row)
    (println "Field diff:" diff)
    (println "Expected: :temp field removed")
    diff))

(defn test-edn-field-diff []
  (println "\n=== Testing EDN Field Diff ===")
  (let [old-row {:id 4 :state (pr-str {:todos [{:id 1 :text "Buy milk" :done false}] 
                                        :filter :all})}
        new-row {:id 4 :state (pr-str {:todos [{:id 1 :text "Buy milk" :done true}
                                               {:id 2 :text "Walk dog" :done false}] 
                                        :filter :active})}
        diff (kafka/compute-field-diff old-row new-row 
                                       :structural-diff? true
                                       :edn-fields #{:state})]
    (println "Old row:" old-row)
    (println "New row:" new-row)
    (println "Field diff:" diff)
    (println "Expected: Structural diff on :state field")
    diff))

(defn test-row-diff-with-field-mode []
  (println "\n=== Testing Row Diff with Field Mode ===")
  (let [old-results [{:id 1 :name "Alice" :age 30 :city "NYC"}
                     {:id 2 :name "Bob" :age 25 :city "LA"}
                     {:id 3 :name "Charlie" :age 35 :city "Chicago"}]
        new-results [{:id 1 :name "Alice" :age 31 :city "NYC"}  ; age changed
                     {:id 2 :name "Bob" :age 25 :city "SF"}      ; city changed
                     {:id 3 :name "Charles" :age 35 :city "Chicago"}] ; name changed
        diff (kafka/compute-row-diff old-results new-results :field-based? true)]
    (println "Old results:" (count old-results) "rows")
    (println "New results:" (count new-results) "rows")
    (println "Diff type:" (:type diff))
    (println "Updated entries:")
    (doseq [entry (:updated diff)]
      (println "  ID" (:id entry) "changed fields:" (keys (:field-changes entry))))
    (println "Expected: 3 updated rows with 1 field change each")
    diff))

(defn check-diff-config []
  (println "\n=== Current Diff Configuration ===")
  (println @kafka/diff-config)
  (println "\nDiff stats:" (kafka/get-diff-stats)))

(defn run-all-tests []
  (check-diff-config)
  (test-basic-field-diff)
  (test-field-addition)
  (test-field-removal)
  (test-edn-field-diff)
  (test-row-diff-with-field-mode)
  (println "\n=== Test Summary ===")
  (println "Field-based diffing is" (if (:field-based? @kafka/diff-config) "ENABLED" "DISABLED"))
  (println "Structural diffing is" (if (:structural-diff? @kafka/diff-config) "ENABLED" "DISABLED"))
  (println "\nTo change diff mode, use: (kafka/set-diff-mode! :field)")
  (println "Options: :none, :row, :field, :structural"))

;; Run the tests
(run-all-tests)