(ns reactor.utils
  (:require [puget.printer :as puget])
  (:import [jline TerminalFactory]))

;; ============================================================================
;; Utils
;; ============================================================================

(def console-lock (Object.))

(defonce terminal-width (atom nil))
(def terminal (TerminalFactory/get))
(defn get-terminal-width [] (or @terminal-width
                                (try (.getWidth terminal) (catch Throwable _ 85))))

(defn ppln [x] (puget/with-options {:width 330} (puget/cprint x)))

(defn safe-cprint [x & [opts-map]]
  (locking console-lock
    (puget/with-options (merge
                         {:width (get-terminal-width)}
                         opts-map) (puget/cprint x))))

(defn pp [x & [opts-map]]
    (safe-cprint x opts-map) ((fn [& _]) x))
