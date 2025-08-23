(ns reactor.tap
  "Simplified tap system that inserts directly into XTDB"
  (:require [cljs.reader :as reader]
            [reactor.core :as r])
  (:refer-clojure :exclude [tap>]))

(defn tap>
  "Send a value to the tap system for debugging. 
   Usage: 
     (tap> value)
     (tap> value \"caller-name\")
     (tap> value \"caller-name\" \"platform\")
   
   Values are sent to server and stored in reactor_taps table."
  ([value]
   (tap> value nil))
  ([value caller]
   (tap> value caller nil))
  ([value caller platform]
   ;; Defer ALL work to avoid blocking
   (js/setTimeout
    (fn []
      (try
        ;; Get session and server info
        (let [session-id (or (:session-id @r/config)
                           js/window.sessionId 
                           (.-sessionId js/window) 
                           "default")
              server-url (or (:server-url @r/config) "http://localhost:5000")
              full-url (str server-url "/api/tap")]
          ;; Send to server for storage in XTDB
          (-> (js/fetch full-url
                        #js {:method "POST"
                             :headers #js {"Content-Type" "application/json"}
                             :body (js/JSON.stringify 
                                    (clj->js {:value (cond
                                                       ;; For JS strings, use lightweight JSON stringify
                                                       (and (= platform "JS") (string? value))
                                                       (js/JSON.stringify value)
                                                       ;; For small simple values, use pr-str
                                                       (or (string? value) (number? value) (boolean? value) (nil? value))
                                                       (pr-str value)
                                                       ;; For complex objects, truncate to avoid slowness
                                                       :else
                                                       (let [s (pr-str value)]
                                                         (if (> (count s) 1000)
                                                           (str (subs s 0 1000) "...")
                                                           s)))
                                             :caller (or caller "anonymous")
                                             :platform (or platform "CLJS")
                                             :session-id session-id}))})
              (.catch (fn [err]
                       ;; Silently ignore errors to prevent console spam
                       nil))))
        (catch :default e
          ;; Silently ignore any errors
          nil)))
    0)
   
   ;; Return the value immediately (for threading)
   value))