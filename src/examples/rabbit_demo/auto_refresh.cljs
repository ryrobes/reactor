(ns examples.rabbit-demo.auto-refresh
  "Auto-refresh queries when blocks are loaded from persistence"
  (:require [reactor.core :as r]
            [examples.rabbit-demo.reactive-queries :as rq]))

(defn refresh-block-queries!
  "Refresh all query blocks that don't have results (e.g., after loading from persistence)"
  []
  (let [blocks @(r/subscribe [:blocks])]
    (js/console.log "[AUTO-REFRESH] Checking blocks for missing results...")
    (doseq [[block-id block] blocks]
      (when (and (= (:type block) "query")
                 (:sql block)
                 (not (:results block))
                 (not (:loading block)))
        (js/console.log "[AUTO-REFRESH] Refreshing block" block-id "with SQL:" (:sql block))
        (rq/execute-block-query! block-id (:sql block) nil (:as-of block))))))

(defn init-auto-refresh!
  "Initialize auto-refresh on app startup"
  []
  ;; Give the app a moment to load, then refresh queries
  (js/setTimeout refresh-block-queries! 1000))