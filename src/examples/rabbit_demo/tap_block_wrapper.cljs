(ns examples.rabbit-demo.tap-block-wrapper
  "TAP block wrapper with standard block chrome"
  (:require [reagent.core :as reagent]
            [reactor.core :as r]
            [examples.rabbit-demo.tap-block :as tap-block]))

(defn tap-block-component [{:keys [id position size] :as block}]
  (let [;; Use local position only while dragging
        drag-state (atom nil) ; This should reference the global drag state
        resize-state (atom nil) ; This should reference the global resize state
        local-positions (atom {})
        local-sizes (atom {})
        
        is-dragging? false ; (= id (:block-id @drag-state))
        is-resizing? false ; (= id (:block-id @resize-state))
        
        actual-pos (if (or is-dragging? (get @local-positions id))
                     (get @local-positions id position)
                     position)
        actual-size (if (or is-resizing? (get @local-sizes id))
                      (get @local-sizes id size)
                      size)]
    [:div.block
     {:style {:position "absolute"
              :left (:x actual-pos)
              :top (:y actual-pos)
              :width (:width actual-size)
              :height (:height actual-size)
              :background "linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 100%)"
              :border "1px solid #ff4f99"
              :border-radius "4px"
              :box-shadow "0 4px 20px rgba(255,79,153,0.3)"
              :display "flex"
              :flex-direction "column"
              :transition (when-not (or is-dragging? is-resizing?)
                           "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)")}}
     ;; Header
     [:div.block-header
      {:style {:padding "10px"
               :background "rgba(255,79,153,0.1)"
               :border-bottom "1px solid rgba(255,79,153,0.3)"
               :cursor "move"
               :display "flex"
               :justify-content "space-between"
               :align-items "center"}}
      [:span {:style {:color "#ff4f99"
                      :font-family "'JetBrains Mono', monospace"
                      :text-transform "uppercase"
                      :font-size "11px"
                      :letter-spacing "1px"}} "TAP"]
      [:button {:on-click #(r/dispatch! [:delete-block id])
                :style {:background "transparent"
                        :border "none"
                        :color "#ff4f99"
                        :cursor "pointer"
                        :font-size "16px"
                        :padding "0 5px"}}
       "×"]]
     ;; Content
     [:div {:style {:flex 1
                    :overflow "hidden"
                    :display "flex"}}
      [tap-block/tap-block-content block]]
     ;; Resize handle
     [:div.resize-handle
      {:style {:position "absolute"
               :bottom 0
               :right 0
               :width "15px"
               :height "15px"
               :cursor "nwse-resize"
               :background "radial-gradient(circle at center, rgba(255,79,153,0.5) 0%, transparent 70%)"}}]]))