(ns examples.rabbit-demo.tap-handler
  "TAP handler for capturing and displaying tap> entries in Rabbit Demo"
  (:require [reagent.core :as reagent]
            [reactor.tap :as rtap]
            [reactor.core :as r]
            [cljs.reader :as reader]))

;; Store tap entries with timestamps
(defonce tap-entries (reagent/atom []))

;; Maximum number of entries to keep (prevent memory issues)
(def max-entries 100)

;; Tap handler function - now receives structured tap entry
(defn tap-handler [tap-entry]
  (let [entry (merge tap-entry
                    {:id (random-uuid)
                     :timestamp (or (:timestamp tap-entry) (js/Date.))
                     :expanded? false})]
    (swap! tap-entries (fn [entries]
                        (let [new-entries (conj entries entry)]
                          ;; Keep only the last max-entries
                          (if (> (count new-entries) max-entries)
                            (vec (drop (- (count new-entries) max-entries) new-entries))
                            new-entries))))))

;; Initialize tap handler
(defn init-tap! []
  ;; Register our handler with the reactor tap system
  (rtap/register-handler! tap-handler)
  ;; Also poll server for any backend taps
  (start-server-tap-polling!)
  (js/console.log "[TAP] Tap handler initialized"))

;; Poll server for backend tap entries
(defn start-server-tap-polling! []
  (js/setInterval
   (fn []
     (-> (js/fetch (str (:server-url @r/config) "/api/tap-entries?limit=50")
                   #js {:method "GET"
                        :headers #js {"Content-Type" "application/json"}})
         (.then #(.json %))
         (.then (fn [data]
                 (let [entries (js->clj data :keywordize-keys true)]
                   ;; Process new entries from server
                   (doseq [entry entries]
                     (when (not (some #(= (:_id entry) (:_id %)) @tap-entries))
                       ;; Parse EDN string value if from server
                       (let [parsed-entry (if (string? (:value entry))
                                           (update entry :value reader/read-string)
                                           entry)]
                         (tap-handler parsed-entry)))))))
         (.catch (fn [err]
                  (js/console.debug "Failed to fetch tap entries:" err)))))
   5000)) ; Poll every 5 seconds

;; Clear all tap entries
(defn clear-tap-entries! []
  (reset! tap-entries []))

;; Remove tap handler
(defn remove-tap! []
  (rtap/unregister-handler! tap-handler))

;; Format value for display
(defn format-tap-value [value]
  (cond
    (string? value) value
    (number? value) (str value)
    (boolean? value) (str value)
    (nil? value) "nil"
    (keyword? value) (str value)
    :else (with-out-str (prn value))))

;; Get entries for display (newest first)
(defn get-tap-entries []
  (reverse @tap-entries))

;; Toggle entry expansion
(defn toggle-entry-expansion! [entry-id]
  (swap! tap-entries
         (fn [entries]
           (mapv (fn [entry]
                  (if (= (:id entry) entry-id)
                    (update entry :expanded? not)
                    entry))
                entries))))

;; Export entries as EDN
(defn export-entries-as-edn []
  (with-out-str 
    (prn (mapv #(select-keys % [:timestamp :value]) @tap-entries))))

;; Filter entries by search term
(defn filter-entries [search-term]
  (if (empty? search-term)
    (get-tap-entries)
    (filter (fn [entry]
              (let [formatted (format-tap-value (:value entry))]
                (clojure.string/includes? 
                 (clojure.string/lower-case formatted)
                 (clojure.string/lower-case search-term))))
            (get-tap-entries))))