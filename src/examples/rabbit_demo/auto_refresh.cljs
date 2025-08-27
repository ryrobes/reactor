(ns examples.rabbit-demo.auto-refresh
  "Auto-refresh queries when blocks are loaded from persistence"
  (:require [reactor.core :as r]
            [examples.rabbit-demo.reactive-queries :as rq]))

(defn refresh-block-queries!
  "Refresh all query blocks (auto-execute on load)"
  []
  (let [blocks @(r/subscribe [:blocks])]
    (js/console.log "[AUTO-REFRESH] Auto-executing all query blocks...")
    (doseq [[block-id block] blocks]
      (when (and (= (:type block) "query")
                 (:sql block))
        (js/console.log "[AUTO-REFRESH] Executing query for block" (str block-id) "SQL:" (:sql block))
        (rq/execute-block-query! block-id (:sql block) nil (:as-of block))))))

(defn init-auto-refresh!
  "Initialize auto-refresh on app startup"
  []
  ;; Give the app a moment to load, then refresh queries
  (js/setTimeout refresh-block-queries! 1000))