(ns reactor.console-tap
  "Hijack console methods to also send output to tap>"
  (:require [reactor.tap :as tap]
            [clojure.string :as str]))

(defonce original-console (atom {}))
(defonce hijacked? (atom false))
(defonce ^:private in-console-tap? (atom false))

(defn should-tap?
  "Check if we should tap this console message"
  [args-vec]
  (not (some (fn [arg]
               ;; Skip internal ClojureScript protocol objects
               (and (object? arg)
                    (or (.-cljs$lang$protocol_mask$partition0$ arg)
                        (.-cljs$lang$protocol_mask$partition1$ arg)
                        ;; Skip if it looks like an internal iterator
                        (and (.-arr ^js arg) (.-i ^js arg) (number? (.-i ^js arg))))))
             args-vec)))

(defn console->tap!
  "Convert console arguments to tap format"
  [method args]
  ;; Prevent recursion - if we're already in console-tap, don't tap
  (when-not @in-console-tap?
    (try
      (let [;; args is already a JS array
            args-vec (vec args)]
        ;; Only tap if it's not an internal ClojureScript object
        (when (should-tap? args-vec)
          (reset! in-console-tap? true)
          (try
            (let [;; Build value for tap - for JS console, use simple string conversion
                  value (cond
                         ;; Single string argument - send as plain string
                         (and (= (count args-vec) 1)
                              (string? (first args-vec)))
                         (first args-vec)
                         
                         ;; Multiple arguments - join them as simple strings
                         (> (count args-vec) 1)
                         (str/join " " (map str args-vec))
                         
                         ;; Single argument - convert to simple string
                         (= (count args-vec) 1)
                         (str (first args-vec))
                         
                         ;; Fallback - shouldn't happen but just in case
                         :else
                         (str args-vec))
                  ;; Add metadata about the console method used
                  caller (str "console." (name method))]
              ;; Send with JS platform and console method as caller
              (tap/tap> value caller "JS"))
            (finally
              (reset! in-console-tap? false)))))
      (catch :default e
        ;; Silently ignore errors to prevent breaking console
        nil))))

(defn hijack-console!
  "Hijack console methods to also send to tap>
   Options:
   - :methods - vector of methods to hijack (default: [:log :warn :error :info :debug])
   - :preserve-original? - whether to still call original console (default: true)
   - :tap-enabled? - whether to send to tap (default: true)"
  [& [{:keys [methods preserve-original? tap-enabled?]
       :or {methods [:log :warn :error :info :debug]
            preserve-original? true
            tap-enabled? true}}]]
  (when false ;-not @hijacked?
    (doseq [method methods]
      (let [method-name (name method)
            original-fn (aget js/console method-name)]
        ;; Store original function
        (swap! original-console assoc method original-fn)
        ;; Store reference on console object for tap.cljs to use
        (aset js/console (str "original" (str/capitalize method-name)) original-fn)
        ;; Replace with wrapped version
        (aset js/console method-name
              (fn []
                (let [args (js/Array.prototype.slice.call (js-arguments))]
                  ;; Call original immediately - don't block
                  (when preserve-original?
                    (.apply original-fn js/console args))
                  ;; Send to tap async - don't block console
                  (when tap-enabled?
                    (js/setTimeout
                     (fn []
                       (try
                         (console->tap! method args)
                         (catch :default _
                           nil)))
                     0)))))))
    (reset! hijacked? true)
    ;; Log that we've hijacked console (using original to avoid recursion)
    (when-let [orig-log (:log @original-console)]
      (.call orig-log js/console "[CONSOLE-TAP] Console methods hijacked and sending to tap>"))
    true))

(defn restore-console!
  "Restore original console methods"
  []
  (when @hijacked?
    (doseq [[method original-fn] @original-console]
      (aset js/console (name method) original-fn)
      ;; Remove the stored original reference
      (js-delete js/console (str "original" (str/capitalize (name method)))))
    (reset! hijacked? false)
    (reset! original-console {})
    ;; Use restored console to log
    (.log js/console "[CONSOLE-TAP] Console methods restored")
    true))

(defn toggle-console-tap!
  "Toggle console hijacking on/off"
  []
  (if @hijacked?
    (restore-console!)
    (hijack-console!)))

;; Convenience functions for selective hijacking
(defn hijack-errors-only!
  "Only hijack console.error and console.warn"
  []
  (hijack-console! {:methods [:error :warn]}))

(defn hijack-all!
  "Hijack all common console methods"
  []
  (hijack-console! {:methods [:log :warn :error :info :debug :trace]}))

;; Advanced: Create custom console that only goes to tap
(defn create-tap-console
  "Create a console object that only sends to tap (doesn't print to browser console)"
  []
  (let [console-obj #js {}]
    (doseq [method [:log :warn :error :info :debug]]
      (aset console-obj (name method)
            (fn [& args]
              (console->tap! method (js-arguments)))))
    console-obj))

;; Allow filtering what gets tapped
(defonce tap-filter (atom nil))

(defn set-tap-filter!
  "Set a filter function that determines what console messages get tapped.
   Filter receives [method args-vec] and should return truthy to tap."
  [filter-fn]
  (reset! tap-filter filter-fn))

(defn hijack-console-filtered!
  "Hijack console with filtering support"
  [& [opts]]
  (hijack-console!
   (assoc opts
          :tap-enabled?
          (fn [method args]
            (if-let [filter-fn @tap-filter]
              (filter-fn method (vec (array-seq args)))
              true)))))