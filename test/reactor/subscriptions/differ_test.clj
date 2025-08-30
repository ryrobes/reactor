(ns reactor.subscriptions.differ-test
  (:require [clojure.test :refer :all]
            [reactor.subscriptions.differ :as differ]))

;; ============================================================================
;; Test Data
;; ============================================================================

(def sample-old
  [{:id 1 :name "Alice" :age 30 :city "NYC"}
   {:id 2 :name "Bob" :age 25 :city "LA"}
   {:id 3 :name "Charlie" :age 35 :city "Chicago"}])

(def sample-new-added
  [{:id 1 :name "Alice" :age 30 :city "NYC"}
   {:id 2 :name "Bob" :age 25 :city "LA"}
   {:id 3 :name "Charlie" :age 35 :city "Chicago"}
   {:id 4 :name "Diana" :age 28 :city "Boston"}])

(def sample-new-removed
  [{:id 1 :name "Alice" :age 30 :city "NYC"}
   {:id 3 :name "Charlie" :age 35 :city "Chicago"}])

(def sample-new-updated
  [{:id 1 :name "Alice" :age 31 :city "NYC"}      ; Age changed
   {:id 2 :name "Bob" :age 25 :city "SF"}         ; City changed
   {:id 3 :name "Charlie" :age 35 :city "Chicago"}]) ; No change

;; ============================================================================
;; Helper Tests
;; ============================================================================

(deftest test-identify-by
  (testing "Creates map indexed by key function"
    (let [result (differ/identify-by :id sample-old)]
      (is (= 3 (count result)))
      (is (= {:id 1 :name "Alice" :age 30 :city "NYC"}
             (get result 1))))))

(deftest test-get-id-key
  (testing "Auto-detects :id key"
    (is (= :id (differ/get-id-key sample-old []))))
  
  (testing "Falls back to :_id"
    (is (= :_id (differ/get-id-key [{:_id 1 :name "Test"}] []))))
  
  (testing "Returns nil when no clear ID"
    (is (nil? (differ/get-id-key [{:x 1 :y 2}] []))))
  
  (testing "Checks new results when old is empty"
    (is (= :id (differ/get-id-key [] sample-old)))))

;; ============================================================================
;; Row Diff Tests
;; ============================================================================

(deftest test-row-diff-additions
  (testing "Detects added rows"
    (let [diff (differ/calculate-row-diff sample-old sample-new-added)]
      (is (= :row-diff (:type diff)))
      (is (= 1 (count (:added diff))))
      (is (= {:id 4 :name "Diana" :age 28 :city "Boston"}
             (first (:added diff))))
      (is (empty? (:removed diff)))
      (is (empty? (:updated diff))))))

(deftest test-row-diff-removals
  (testing "Detects removed rows"
    (let [diff (differ/calculate-row-diff sample-old sample-new-removed)]
      (is (= 1 (count (:removed diff))))
      (is (= {:id 2 :name "Bob" :age 25 :city "LA"} 
             (first (:removed diff))))
      (is (empty? (:added diff))))))

(deftest test-row-diff-updates
  (testing "Detects updated rows"
    (let [diff (differ/calculate-row-diff sample-old sample-new-updated)]
      (is (= 2 (count (:updated diff))))
      (is (empty? (:added diff)))
      (is (empty? (:removed diff))))))

(deftest test-row-diff-no-changes
  (testing "Handles identical data"
    (let [diff (differ/calculate-row-diff sample-old sample-old)]
      (is (empty? (:added diff)))
      (is (empty? (:removed diff)))
      (is (empty? (:updated diff))))))

;; ============================================================================
;; Field Diff Tests
;; ============================================================================

(deftest test-field-changes
  (testing "Detects individual field changes"
    (let [old-row {:id 1 :name "Alice" :age 30 :city "NYC"}
          new-row {:id 1 :name "Alice" :age 31 :city "NYC" :country "USA"}
          changes (differ/calculate-field-changes old-row new-row)]
      
      (is (= {:op :update :value 31} (:age changes)))
      (is (= {:op :add :value "USA"} (:country changes)))
      (is (nil? (:name changes)))  ; Unchanged
      (is (nil? (:city changes))))))  ; Unchanged

(deftest test-field-diff-updates
  (testing "Provides field-level changes for updates"
    (let [diff (differ/calculate-field-diff sample-old sample-new-updated)]
      (is (= :field-diff (:type diff)))
      (is (= 2 (count (:updated diff))))
      
      (let [alice-update (first (filter #(= 1 (:id %)) (:updated diff)))]
        (is (= 1 (:id alice-update)))
        (is (= {:op :update :value 31} (get-in alice-update [:changes :age]))))
      
      (let [bob-update (first (filter #(= 2 (:id %)) (:updated diff)))]
        (is (= 2 (:id bob-update)))
        (is (= {:op :update :value "SF"} (get-in bob-update [:changes :city])))))))

(deftest test-field-diff-additions
  (testing "Handles row additions in field diff"
    (let [diff (differ/calculate-field-diff sample-old sample-new-added)]
      (is (= 1 (count (:added diff))))
      (is (= {:id 4 :name "Diana" :age 28 :city "Boston"}
             (first (:added diff)))))))

;; ============================================================================
;; Compression Tests
;; ============================================================================

(deftest test-diff-size
  (testing "Calculates row diff size"
    (let [diff {:type :row-diff
                :added [{:id 4}]
                :removed [2]
                :updated [{:id 1}]}]
      (is (= 3 (differ/diff-size diff)))))
  
  (testing "Calculates field diff size"
    (let [diff {:type :field-diff
                :added []
                :removed []
                :updated [{:id 1 :changes {:age 31}}
                         {:id 2 :changes {:city "SF" :state "CA"}}]}]
      (is (= 3 (differ/diff-size diff))))))  ; 1 + 2 field changes

(deftest test-compression-ratio
  (testing "Calculates compression ratio"
    (let [diff {:type :row-diff :added [] :removed [] :updated [{:id 1}]}
          results [{:id 1 :a 1 :b 2} {:id 2 :a 3 :b 4}]]
      ;; 1 change out of 2 rows = 0.5
      (is (= 0.5 (differ/compression-ratio diff results)))))
  
  (testing "Handles empty results"
    (let [diff {:type :row-diff :added [] :removed [] :updated []}]
      (is (= 1.0 (differ/compression-ratio diff []))))))

(deftest test-should-use-diff
  (testing "Uses diff when efficient"
    (let [diff {:type :row-diff :added [] :removed [] :updated [{:id 1}]}
          results (repeat 10 {:id 1 :a 1 :b 2 :c 3})]
      (is (true? (differ/should-use-diff? diff results 0.5)))))
  
  (testing "Skips diff when inefficient"
    (let [diff {:type :row-diff :added (range 10) :removed [] :updated []}
          results [{:id 1}]]
      (is (false? (differ/should-use-diff? diff results 0.5))))))

;; ============================================================================
;; Main API Tests
;; ============================================================================

(deftest test-calculate-diff-modes
  (testing "Returns full results when mode is :none"
    (let [diff (differ/calculate-diff sample-old sample-new-updated {:mode :none})]
      (is (= :full (:type diff)))
      (is (= sample-new-updated (:results diff)))))
  
  (testing "Calculates row diff when mode is :row"
    (let [diff (differ/calculate-diff sample-old sample-new-updated {:mode :row})]
      (is (= :row-diff (:type diff)))
      (is (= 2 (count (:updated diff))))))
  
  (testing "Calculates field diff by default"
    (let [diff (differ/calculate-diff sample-old sample-new-updated)]
      (is (= :field-diff (:type diff)))
      (is (= 2 (count (:updated diff)))))))

(deftest test-calculate-diff-threshold
  (testing "Falls back to full when diff exceeds threshold"
    ;; Use a case where we have changes, not just additions
    (let [old (map #(hash-map :id % :data "old") (range 50))
          new (map #(hash-map :id % :data "new") (range 100))
          diff (differ/calculate-diff old new {:threshold 0.1})]
      ;; 50 additions + 50 updates = 100 changes, exceeds threshold
      ;; Since old is not empty, it won't force a diff
      (is (= :full (:type diff)))
      (is (= new (:results diff)))))
  
  (testing "Empty old always uses diff regardless of threshold"
    (let [old []
          new (map #(hash-map :id % :data "lots") (range 100))
          diff (differ/calculate-diff old new {:threshold 0.1})]
      ;; Even though it exceeds threshold, empty old forces diff
      (is (= :field-diff (:type diff)))
      (is (= 100 (count (:added diff)))))))

;; ============================================================================
;; Edge Cases
;; ============================================================================

(deftest test-edge-cases
  (testing "Handles empty old results"
    (let [diff (differ/calculate-diff [] sample-old)]
      (is (= 3 (count (:added diff))))
      (is (empty? (:removed diff)))))
  
  (testing "Handles empty new results"
    (let [diff (differ/calculate-diff sample-old [])]
      (is (empty? (:added diff)))
      (is (= 3 (count (:removed diff))))))
  
  (testing "Handles both empty"
    (let [diff (differ/calculate-diff [] [])]
      (is (empty? (:added diff)))
      (is (empty? (:removed diff)))
      (is (empty? (:updated diff)))))
  
  (testing "Handles custom ID key"
    (let [old [{:x 1 :y 2} {:x 2 :y 5} {:x 3 :y 8}]
          new [{:x 1 :y 3} {:x 2 :y 5} {:x 3 :y 8}]  ; Only first row changes
          diff (differ/calculate-diff old new {:id-key :x})]
      ;; With 3 rows and only 1 change, compression ratio is 1/3 = 0.33
      (is (= :field-diff (:type diff)))
      (is (= :x (:id-key diff)))
      (is (= 1 (count (:updated diff)))))))

;; ============================================================================
;; Summary Tests
;; ============================================================================

(deftest test-summarize-diff
  (testing "Summarizes full update"
    (is (= "Full update: 3 rows"
           (differ/summarize-diff {:type :full :results sample-old}))))
  
  (testing "Summarizes row diff"
    (is (= "Row diff: +1 -1 ~2"
           (differ/summarize-diff {:type :row-diff
                                   :added [1]
                                   :removed [2]
                                   :updated [3 4]}))))
  
  (testing "Summarizes field diff"
    (is (= "Field diff: +0 -0 ~2 (3 field changes)"
           (differ/summarize-diff {:type :field-diff
                                   :added []
                                   :removed []
                                   :updated [{:id 1 :changes {:a 1}}
                                            {:id 2 :changes {:b 2 :c 3}}]})))))