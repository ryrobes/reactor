(ns reactor.subscriptions.store-test
  (:require [clojure.test :refer :all]
            [reactor.subscriptions.store :as store]))

;; ============================================================================
;; Test Helpers
;; ============================================================================

(defn fresh-store!
  "Clear the store before each test"
  []
  (store/clear!))

(use-fixtures :each (fn [f] (fresh-store!) (f)))

(defn create-test-subscription
  "Create a test subscription with defaults"
  [& [overrides]]
  (merge {:id (str "test-" (rand-int 10000))
          :sql "SELECT * FROM users"
          :tables ["users"]
          :session-id "test-session"
          :status :active}
         overrides))

;; ============================================================================
;; Basic Operations
;; ============================================================================

(deftest test-add-and-get
  (testing "Can add and retrieve subscription"
    (let [sub (create-test-subscription {:id "sub-1"})
          added (store/add! sub)]
      (is (= "sub-1" (:id added)))
      (is (number? (:created-at added)))
      (is (= :active (:status added)))
      (is (= added (store/get-subscription "sub-1")))))
  
  (testing "Returns nil for non-existent subscription"
    (is (nil? (store/get-subscription "does-not-exist")))))

(deftest test-update
  (testing "Can update existing subscription"
    (let [sub (create-test-subscription {:id "sub-1"})
          _ (store/add! sub)
          updated (store/update! "sub-1" {:status :paused :sql "SELECT * FROM orders"})]
      (is (= :paused (:status updated)))
      (is (= "SELECT * FROM orders" (:sql updated)))
      (is (number? (:updated-at updated)))
      ;; Original fields preserved
      (is (= "test-session" (:session-id updated)))))
  
  (testing "Returns nil when updating non-existent"
    (is (nil? (store/update! "does-not-exist" {:status :paused})))))

(deftest test-delete
  (testing "Can delete subscription"
    (let [sub (create-test-subscription {:id "sub-1"})
          _ (store/add! sub)
          deleted (store/delete! "sub-1")]
      (is (= "sub-1" (:id deleted)))
      (is (nil? (store/get-subscription "sub-1")))))
  
  (testing "Returns nil when deleting non-existent"
    (is (nil? (store/delete! "does-not-exist")))))

;; ============================================================================
;; Index Tests
;; ============================================================================

(deftest test-find-by-table
  (testing "Can find subscriptions by table"
    (store/add! (create-test-subscription {:id "sub-1" :tables ["users"]}))
    (store/add! (create-test-subscription {:id "sub-2" :tables ["orders"]}))
    (store/add! (create-test-subscription {:id "sub-3" :tables ["users" "orders"]}))
    
    (let [user-subs (store/find-by-table "users")]
      (is (= 2 (count user-subs)))
      (is (contains? (set (map :id user-subs)) "sub-1"))
      (is (contains? (set (map :id user-subs)) "sub-3")))
    
    (let [order-subs (store/find-by-table "orders")]
      (is (= 2 (count order-subs)))
      (is (contains? (set (map :id order-subs)) "sub-2"))
      (is (contains? (set (map :id order-subs)) "sub-3")))))

(deftest test-find-by-tables
  (testing "Can find subscriptions by multiple tables"
    (store/add! (create-test-subscription {:id "sub-1" :tables ["users"]}))
    (store/add! (create-test-subscription {:id "sub-2" :tables ["orders"]}))
    (store/add! (create-test-subscription {:id "sub-3" :tables ["products"]}))
    
    (let [subs (store/find-by-tables ["users" "orders"])]
      (is (= 2 (count subs)))
      (is (contains? (set (map :id subs)) "sub-1"))
      (is (contains? (set (map :id subs)) "sub-2")))))

(deftest test-find-by-session
  (testing "Can find subscriptions by session"
    (store/add! (create-test-subscription {:id "sub-1" :session-id "session-1"}))
    (store/add! (create-test-subscription {:id "sub-2" :session-id "session-2"}))
    (store/add! (create-test-subscription {:id "sub-3" :session-id "session-1"}))
    
    (let [subs (store/find-by-session "session-1")]
      (is (= 2 (count subs)))
      (is (contains? (set (map :id subs)) "sub-1"))
      (is (contains? (set (map :id subs)) "sub-3")))))

(deftest test-find-by-block
  (testing "Can find subscriptions by block"
    (store/add! (create-test-subscription {:id "sub-1" :block-id "block-1"}))
    (store/add! (create-test-subscription {:id "sub-2" :block-id "block-2"}))
    (store/add! (create-test-subscription {:id "sub-3" :block-id "block-1"}))
    
    (let [subs (store/find-by-block "block-1")]
      (is (= 2 (count subs)))
      (is (contains? (set (map :id subs)) "sub-1"))
      (is (contains? (set (map :id subs)) "sub-3")))))

;; ============================================================================
;; Index Update Tests
;; ============================================================================

(deftest test-index-updates-on-change
  (testing "Indices update when subscription changes"
    (store/add! (create-test-subscription {:id "sub-1" :tables ["users"]}))
    
    ;; Initially in users index
    (is (= 1 (count (store/find-by-table "users"))))
    (is (= 0 (count (store/find-by-table "orders"))))
    
    ;; Update to different table
    (store/update! "sub-1" {:tables ["orders"]})
    
    ;; Now in orders index, not users
    (is (= 0 (count (store/find-by-table "users"))))
    (is (= 1 (count (store/find-by-table "orders"))))))

(deftest test-index-cleanup-on-delete
  (testing "Indices cleaned up on delete"
    (store/add! (create-test-subscription {:id "sub-1" :tables ["users"]}))
    (store/add! (create-test-subscription {:id "sub-2" :tables ["users"]}))
    
    (is (= 2 (count (store/find-by-table "users"))))
    
    (store/delete! "sub-1")
    (is (= 1 (count (store/find-by-table "users"))))
    
    (store/delete! "sub-2")
    (is (= 0 (count (store/find-by-table "users"))))))

;; ============================================================================
;; Bulk Operations
;; ============================================================================

(deftest test-delete-by-session
  (testing "Can delete all subscriptions for a session"
    (store/add! (create-test-subscription {:id "sub-1" :session-id "session-1"}))
    (store/add! (create-test-subscription {:id "sub-2" :session-id "session-2"}))
    (store/add! (create-test-subscription {:id "sub-3" :session-id "session-1"}))
    
    (let [deleted (store/delete-by-session! "session-1")]
      (is (= 2 deleted))
      (is (nil? (store/get-subscription "sub-1")))
      (is (nil? (store/get-subscription "sub-3")))
      (is (some? (store/get-subscription "sub-2"))))))

(deftest test-delete-inactive
  (testing "Can delete old inactive subscriptions"
    ;; Add some subscriptions with different ages
    (store/add! (assoc (create-test-subscription {:id "sub-old"})
                       :created-at (- (System/currentTimeMillis) 10000)))
    (store/add! (assoc (create-test-subscription {:id "sub-new"})
                       :created-at (System/currentTimeMillis)))
    
    ;; Delete older than 5 seconds
    (let [deleted (store/delete-inactive! 5000)]
      (is (= 1 deleted))
      (is (nil? (store/get-subscription "sub-old")))
      (is (some? (store/get-subscription "sub-new"))))))

;; ============================================================================
;; Status Management
;; ============================================================================

(deftest test-pause-and-resume
  (testing "Can pause and resume subscriptions"
    (store/add! (create-test-subscription {:id "sub-1"}))
    
    (is (= :active (:status (store/get-subscription "sub-1"))))
    
    (store/pause! "sub-1")
    (is (= :paused (:status (store/get-subscription "sub-1"))))
    
    (store/resume! "sub-1")
    (is (= :active (:status (store/get-subscription "sub-1"))))))

(deftest test-find-active
  (testing "Can find only active subscriptions"
    (store/add! (create-test-subscription {:id "sub-1" :status :active}))
    (store/add! (create-test-subscription {:id "sub-2" :status :paused}))
    (store/add! (create-test-subscription {:id "sub-3" :status :active}))
    
    (let [active (store/find-active)]
      (is (= 2 (count active)))
      (is (every? #(= :active (:status %)) active)))))

;; ============================================================================
;; Statistics
;; ============================================================================

(deftest test-stats
  (testing "Returns correct statistics"
    (store/add! (create-test-subscription {:id "sub-1" :tables ["users"]}))
    (store/add! (create-test-subscription {:id "sub-2" :tables ["orders"] :status :paused}))
    (store/add! (create-test-subscription {:id "sub-3" :tables ["users" "products"]}))
    
    (let [stats (store/stats)]
      (is (= 3 (:total-subscriptions stats)))
      (is (= 2 (:active-subscriptions stats)))
      (is (= 3 (:tables-watched stats)))  ; users, orders, products
      (is (= 1 (:sessions-with-subs stats))))))  ; All use test-session

;; ============================================================================
;; Edge Cases
;; ============================================================================

(deftest test-case-insensitive-tables
  (testing "Table names are case-insensitive"
    (store/add! (create-test-subscription {:id "sub-1" :tables ["Users"]}))
    (store/add! (create-test-subscription {:id "sub-2" :tables ["USERS"]}))
    
    (let [subs (store/find-by-table "users")]
      (is (= 2 (count subs))))))

(deftest test-rebuild-indices
  (testing "Can rebuild indices from scratch"
    (store/add! (create-test-subscription {:id "sub-1" :tables ["users"]}))
    (store/add! (create-test-subscription {:id "sub-2" :tables ["orders"]}))
    
    ;; Corrupt indices (simulating a bug)
    (reset! store/indices {})
    
    ;; Indices should be empty
    (is (= 0 (count (store/find-by-table "users"))))
    
    ;; Rebuild
    (store/rebuild-indices!)
    
    ;; Indices restored
    (is (= 1 (count (store/find-by-table "users"))))
    (is (= 1 (count (store/find-by-table "orders"))))))