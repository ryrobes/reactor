(ns reactor.client
  "ClojureScript client for Reactor - unified reactive state management.
   Provides seamless integration with Reagent for reactive UI updates."
  (:require [cljs.reader :as reader]
            [clojure.string :as str]
            [reagent.core :as r]
            [reagent.ratom :as ratom]))

;; ===== SSE Data Parsing =====

(defn- parse-sse-data [data format]
  (case format
    :json (js->clj (js/JSON.parse data) :keywordize-keys true)
    :edn (reader/read-string data)
    data))

(defn- path->string [path]
  (str/join "." (map name path)))

;; ===== Protocol for Reactive Client =====

(defprotocol IReactiveClient
  (-subscribe! [this path callback opts])
  (-unsubscribe! [this key])
  (-dispatch! [this event]))

;; ===== Reactor Atom - Integrates with Reagent =====

(deftype ReactorAtom [^:mutable state
                      base-url
                      format
                      subscriptions
                      event-source]
  IDeref
  (-deref [_]
    state)
  
  ratom/IReactiveAtom
  
  IWatchable
  (-add-watch [this key f]
    (add-watch state key f)
    this)
  
  (-remove-watch [this key]
    (remove-watch state key)
    this)
  
  (-notify-watches [this oldval newval]
    ;; Delegate to the Reagent atom's notify
    nil)
  
  IReactiveClient
  (-subscribe! [this path callback opts]
    ;; For the main subscription, we handle it via SSE which is set up in constructor
    ;; Additional subscriptions can be derived locally
    (let [sub-id (or (:key opts) (keyword (gensym "sub-")))]
      (if path
        ;; Path subscription - derive from main state
        (let [cursor (r/cursor state path)]
          (swap! subscriptions assoc sub-id
                 {:type :cursor
                  :cursor cursor
                  :callback callback})
          ;; Watch the cursor for changes
          (add-watch cursor sub-id
                     (fn [_ _ old-val new-val]
                       (when (not= old-val new-val)
                         (callback old-val new-val))))
          sub-id)
        ;; Full state subscription - watch the main atom
        (do
          (swap! subscriptions assoc sub-id
                 {:type :full
                  :callback callback})
          (add-watch state sub-id
                     (fn [_ _ old-val new-val]
                       (when (not= old-val new-val)
                         (callback old-val new-val))))
          sub-id))))
  
  (-unsubscribe! [this key]
    (when-let [sub (get @subscriptions key)]
      (case (:type sub)
        :cursor (remove-watch (:cursor sub) key)
        :full (remove-watch state key)
        nil)
      (swap! subscriptions dissoc key)))
  
  (-dispatch! [this event]
    ;; Send event to server
    (-> (js/fetch (str base-url "/api/dispatch")
                  #js {:method "POST"
                       :headers #js {"Content-Type" (case format
                                                      :json "application/json"
                                                      :edn "application/edn"
                                                      "application/edn")}
                       :body (case format
                               :json (js/JSON.stringify (clj->js event))
                               :edn (pr-str event)
                               (pr-str event))})
        (.then (fn [response]
                 (when-not (.-ok response)
                   (js/console.error "Dispatch failed:" (.-status response)))))
        (.catch (fn [error]
                  (js/console.error "Dispatch error:" error))))))

;; ===== Constructor Functions =====

(defn- setup-sse! 
  "Set up SSE connection to receive server state updates"
  [reactor-atom]
  (let [base-url (.-base-url reactor-atom)
        format (.-format reactor-atom)
        state (.-state reactor-atom)
        url (str base-url "/subscribe?format=" (name format))]
    
    (try
      (let [event-source (js/EventSource. url)
            last-state (atom nil)]
        
        ;; Message handler
        (set! (.-onmessage event-source)
              (fn [event]
                (try
                  (let [data (parse-sse-data (.-data event) format)]
                    ;; Only update if state actually changed
                    (when (not= @last-state data)
                      (reset! last-state data)
                      ;; Update the Reagent atom - this triggers re-renders!
                      (reset! state data)))
                  (catch js/Error e
                    (js/console.error "Error parsing SSE data:" e)))))
        
        ;; Connection opened
        (set! (.-onopen event-source)
              (fn [_]
                (js/console.log "✓ Reactor SSE connected")))
        
        ;; Error handler
        (set! (.-onerror event-source)
              (fn [event]
                (js/console.error "SSE error - will retry:" event)))
        
        ;; Store event source for cleanup
        (set! (.-event-source reactor-atom) event-source)
        
        event-source)
      
      (catch js/Error e
        (js/console.error "Failed to create SSE connection:" e)
        nil))))

(defn create-reactor-atom
  "Create a ReactorAtom that syncs with server state via SSE"
  [base-url {:keys [format initial-state]
             :or {format :edn
                  initial-state {}}}]
  (let [;; Use a Reagent atom for reactive state
        state (r/atom initial-state)
        subscriptions (atom {})
        reactor-atom (->ReactorAtom state
                                    base-url
                                    format
                                    subscriptions
                                    nil)]
    ;; Set up SSE connection
    (setup-sse! reactor-atom)
    reactor-atom))

;; ===== Public API =====

(defn connect!
  "Connect to a Reactor server and return a reactive atom.
   The returned atom integrates with Reagent for automatic UI updates."
  ([url]
   (connect! url {}))
  ([url opts]
   (create-reactor-atom url opts)))

(defn subscribe!
  "Subscribe to state changes. 
   Path can be nil for full state or a vector path like [:todos]"
  [reactor-atom path callback & [opts]]
  (-subscribe! reactor-atom path callback (or opts {})))

(defn unsubscribe!
  "Unsubscribe from state changes"
  [reactor-atom key]
  (-unsubscribe! reactor-atom key))

(defn dispatch!
  "Send an event to the server for processing"
  [reactor-atom event]
  (-dispatch! reactor-atom event))

;; ===== Reagent Integration Helpers =====

(defn cursor
  "Get a reactive cursor to a path in the reactor atom.
   Works just like reagent.core/cursor"
  [reactor-atom path]
  (r/cursor (.-state reactor-atom) path))

(defn subscribe
  "Subscribe to a path and return a reactive value.
   Similar to re-frame subscriptions but simpler."
  [reactor-atom path]
  (if path
    (cursor reactor-atom path)
    (.-state reactor-atom)))

(defn <sub
  "Convenience function to deref a subscription.
   Usage: (<sub conn [:todos])"
  [reactor-atom path]
  @(subscribe reactor-atom path))