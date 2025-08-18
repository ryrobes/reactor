(ns reactor.core-test
  (:require [clojure.test :refer :all]
            [reactor.core :as r]
            [clojure.java.io :as io])
  (:import [java.io File]))

(deftest ratom-basic-operations
  (testing "Basic ratom creation and deref"
    (let [ra (r/ratom {:a 1 :b 2})]
      (is (= {:a 1 :b 2} @ra))
      (is (= {:a 1 :b 2} (ra)))))
  
  (testing "ratom swap!"
    (let [ra (r/ratom {:count 0})]
      (swap! ra update :count inc)
      (is (= {:count 1} @ra))
      (swap! ra assoc :name "test")
      (is (= {:count 1 :name "test"} @ra))))
  
  (testing "ratom reset!"
    (let [ra (r/ratom {:old "value"})]
      (reset! ra {:new "value"})
      (is (= {:new "value"} @ra))))
  
  (testing "ratom with path access"
    (let [ra (r/ratom {:users {:alice {:age 30}
                               :bob {:age 25}}})]
      (is (= {:age 30} (ra [:users :alice])))
      (is (= 25 (ra [:users :bob :age]))))))

(deftest ratom-watches
  (testing "Add and trigger watch"
    (let [ra (r/ratom {:value 0})
          watched (atom nil)]
      (add-watch ra :test-watch
                 (fn [_ _ old-val new-val]
                   (reset! watched {:old old-val :new new-val})))
      (swap! ra update :value inc)
      (is (= {:old {:value 0} :new {:value 1}} @watched))))
  
  (testing "Remove watch"
    (let [ra (r/ratom {:value 0})
          watch-count (atom 0)]
      (add-watch ra :test-watch
                 (fn [_ _ _ _]
                   (swap! watch-count inc)))
      (swap! ra update :value inc)
      (is (= 1 @watch-count))
      (remove-watch ra :test-watch)
      (swap! ra update :value inc)
      (is (= 1 @watch-count)))))

(deftest cursor-operations
  (testing "Cursor creation and deref"
    (let [ra (r/ratom {:users {:alice {:age 30 :name "Alice"}
                               :bob {:age 25 :name "Bob"}}})
          alice-cursor (r/cursor ra [:users :alice])]
      (is (= {:age 30 :name "Alice"} @alice-cursor))))
  
  (testing "Cursor updates when ratom changes"
    (let [ra (r/ratom {:users {:alice {:age 30}}})
          alice-cursor (r/cursor ra [:users :alice])]
      (is (= {:age 30} @alice-cursor))
      (swap! ra assoc-in [:users :alice :age] 31)
      (is (= {:age 31} @alice-cursor))))
  
  (testing "Cursor watches"
    (let [ra (r/ratom {:users {:alice {:age 30}}})
          alice-cursor (r/cursor ra [:users :alice])
          watched (atom nil)]
      (add-watch alice-cursor :cursor-watch
                 (fn [_ _ old-val new-val]
                   (reset! watched {:old old-val :new new-val})))
      (swap! ra assoc-in [:users :alice :age] 31)
      (is (= {:old {:age 30} :new {:age 31}} @watched)))))

(deftest subscription-tests
  (testing "Path subscription"
    (let [ra (r/ratom {:users {:count 0}})
          callback-vals (atom [])]
      (r/subscribe! ra [:users :count]
                    (fn [old-val new-val]
                      (swap! callback-vals conj {:old old-val :new new-val})))
      (swap! ra assoc-in [:users :count] 5)
      (swap! ra assoc-in [:users :count] 10)
      (is (= [{:old nil :new 0}
              {:old 0 :new 5}
              {:old 5 :new 10}]
             @callback-vals))))
  
  (testing "Function subscription with dependency tracking"
    (let [ra (r/ratom {:a 1 :b 2})
          callback-vals (atom [])]
      (r/subscribe! ra
                    (fn [] (+ (:a @ra) (:b @ra)))
                    (fn [old-val new-val]
                      (swap! callback-vals conj {:old old-val :new new-val})))
      (swap! ra assoc :a 5)
      (is (= [{:old nil :new 3}
              {:old 3 :new 7}]
             @callback-vals))))
  
  (testing "Unsubscribe"
    (let [ra (r/ratom {:value 0})
          callback-count (atom 0)
          sub-id (r/subscribe! ra [:value]
                               (fn [_ _]
                                 (swap! callback-count inc)))]
      (swap! ra assoc :value 1)
      (is (= 2 @callback-count))
      (r/unsubscribe! ra sub-id)
      (swap! ra assoc :value 2)
      (is (= 2 @callback-count))))
  
  (testing "Lazy subscription"
    (let [ra (r/ratom {:value 0})
          callback-vals (atom [])]
      (r/subscribe! ra [:value]
                    (fn [old-val new-val]
                      (swap! callback-vals conj {:old old-val :new new-val}))
                    {:lazy true})
      (is (empty? @callback-vals))
      (swap! ra assoc :value 1)
      (is (= [{:old 0 :new 1}] @callback-vals)))))

(deftest rule-system-tests
  (testing "Basic rule definition and execution"
    (let [ra (r/ratom {:status :inactive :counter 0})]
      (r/def-rule ra :increment-on-active [:status]
                  (fn [val] (= val :active))
                  (fn [_ _]
                    (swap! ra update :counter inc)))
      (swap! ra assoc :status :active)
      (is (= 1 (:counter @ra)))
      (swap! ra assoc :status :inactive)
      (is (= 1 (:counter @ra)))
      (swap! ra assoc :status :active)
      (is (= 2 (:counter @ra)))))
  
  (testing "Rule without condition"
    (let [ra (r/ratom {:value 0 :double 0})]
      (r/def-rule ra :doubler [:value]
                  (fn [_ new-val]
                    (swap! ra assoc :double (* 2 new-val))))
      (swap! ra assoc :value 5)
      (is (= 10 (:double @ra)))))
  
  (testing "Enable and disable rule"
    (let [ra (r/ratom {:trigger 0 :counter 0})]
      (r/def-rule ra :counter-rule [:trigger]
                  (fn [_ _]
                    (swap! ra update :counter inc)))
      (swap! ra update :trigger inc)
      (is (= 1 (:counter @ra)))
      (r/disable-rule! ra :counter-rule)
      (swap! ra update :trigger inc)
      (is (= 1 (:counter @ra)))
      (r/enable-rule! ra :counter-rule)
      (swap! ra update :trigger inc)
      (is (= 2 (:counter @ra)))))
  
  (testing "Cascading rules"
    (let [ra (r/ratom {:a 0 :b 0 :c 0})]
      (r/def-rule ra :rule-a-to-b [:a]
                  (fn [_ new-val]
                    (swap! ra assoc :b (* 2 new-val))))
      (r/def-rule ra :rule-b-to-c [:b]
                  (fn [_ new-val]
                    (swap! ra assoc :c (* 3 new-val))))
      (swap! ra assoc :a 5)
      (Thread/sleep 10)
      (is (= {:a 5 :b 10 :c 30} @ra)))))

(deftest persistence-tests
  (testing "Persist and rehydrate"
    (let [temp-file (File/createTempFile "ratom-test" ".edn")
          file-path (.getAbsolutePath temp-file)]
      (try
        (let [ra1 (r/ratom {:data "test" :number 42})]
          (r/persist! ra1 file-path)
          (let [ra2 (r/ratom {})]
            (r/rehydrate! ra2 file-path)
            (is (= {:data "test" :number 42} @ra2))))
        (finally
          (.delete temp-file)))))
  
  (testing "Rehydrate non-existent file"
    (let [ra (r/ratom {:original "data"})]
      (r/rehydrate! ra "/non/existent/file.edn")
      (is (= {:original "data"} @ra)))))

(deftest time-ratom-tests
  (testing "Time ratom creation"
    (let [tr (r/time-ratom {:interval :second})]
      (try
        (is (contains? @tr :now))
        (is (contains? @tr :second))
        (is (contains? @tr :minute))
        (is (number? (:now @tr)))
        (finally
          (.close tr)))))
  
  (testing "Time ratom updates"
    (let [tr (r/time-ratom {:interval :second})
          update-count (atom 0)]
      (try
        (add-watch tr :time-watch
                   (fn [_ _ _ _]
                     (swap! update-count inc)))
        (Thread/sleep 2100)
        (is (>= @update-count 1))
        (finally
          (.close tr)))))
  
  (testing "Time ratom subscription"
    (let [tr (r/time-ratom {:interval :second})
          seconds (atom [])]
      (try
        (r/subscribe! tr [:second]
                      (fn [_ new-val]
                        (swap! seconds conj new-val)))
        (Thread/sleep 3100)
        (is (>= (count @seconds) 2))
        (finally
          (.close tr))))))

(deftest reaction-tests
  (testing "Basic reaction"
    (let [ra (r/ratom {:a 1 :b 2})
          sum-reaction (r/reaction #(+ (:a @ra) (:b @ra)))]
      (is (= 3 @sum-reaction))
      (swap! ra assoc :a 5)
      (Thread/sleep 10)
      (is (= 7 @sum-reaction))))
  
  (testing "Reaction with multiple dependencies"
    (let [ra1 (r/ratom {:value 10})
          ra2 (r/ratom {:value 20})
          product-reaction (r/reaction #(* (:value @ra1) (:value @ra2)))]
      (is (= 200 @product-reaction))
      (swap! ra1 assoc :value 5)
      (Thread/sleep 10)
      (is (= 100 @product-reaction))
      (swap! ra2 assoc :value 10)
      (Thread/sleep 10)
      (is (= 50 @product-reaction)))))

(deftest validator-tests
  (testing "Validator on ratom"
    (let [ra (r/ratom {:age 25})]
      (set-validator! ra (fn [state]
                           (> (:age state) 0)))
      (is (thrown? Exception
                   (swap! ra assoc :age -5)))
      (is (= 25 (:age @ra)))
      (swap! ra assoc :age 30)
      (is (= 30 (:age @ra))))))

(deftest concurrent-access-tests
  (testing "Concurrent swaps"
    (let [ra (r/ratom {:counter 0})
          threads 10
          increments-per-thread 100]
      (let [futures (doall
                     (for [_ (range threads)]
                       (future
                         (dotimes [_ increments-per-thread]
                           (swap! ra update :counter inc)))))]
        (doseq [f futures] @f)
        (is (= (* threads increments-per-thread)
               (:counter @ra))))))
  
  (testing "Concurrent subscriptions"
    (let [ra (r/ratom {:value 0})
          callback-counts (atom {})]
      (dotimes [i 5]
        (let [key (keyword (str "sub-" i))]
          (swap! callback-counts assoc key 0)
          (r/subscribe! ra [:value]
                        (fn [_ _]
                          (swap! callback-counts update key inc))
                        {:key key})))
      (swap! ra assoc :value 1)
      (Thread/sleep 10)
      (is (every? #(= 2 %) (vals @callback-counts))))))

(deftest compare-and-set-test
  (testing "Compare and set operation"
    (let [ra (r/ratom {:value 1})]
      (is (true? (r/cas! ra {:value 1} {:value 2})))
      (is (= {:value 2} @ra))
      (is (false? (r/cas! ra {:value 1} {:value 3})))
      (is (= {:value 2} @ra)))))