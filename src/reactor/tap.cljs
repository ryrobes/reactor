(ns reactor.tap
  "Simplified tap system that inserts directly into XTDB"
  (:require [cljs.reader :as reader]))

(defn tap>
  "Send a value to the tap system for debugging. 
   Usage: 
     (tap> value)
     (tap> value \"caller-name\")
   
   Values are sent to server and stored in reactor_taps table."
  ([value]
   (tap> value nil))
  ([value caller]
   ;; Send to server for storage in XTDB
   (-> (js/fetch "http://localhost:5000/api/tap"
                 #js {:method "POST"
                      :headers #js {"Content-Type" "application/json"}
                      :body (js/JSON.stringify 
                             (clj->js {:value (pr-str value)  ; EDN string for server
                                      :caller (or caller "anonymous")
                                      :platform "CLJS"
                                      :session-id (or (.-sessionId js/window) "browser")}))})
       (.catch (fn [err]
                (js/console.error "Failed to send tap to server:" err))))
   
   ;; Log to console for debugging
   (js/console.log "TAP>" (or caller "anonymous") "-" value)
   
   ;; Return the value (for threading)
   value))