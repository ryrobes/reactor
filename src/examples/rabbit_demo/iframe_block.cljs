(ns examples.rabbit-demo.iframe-block
  "Iframe block component for embedding external content with zoom controls and template support"
  (:require [reactor.core :as r]
            [reagent.core :as reagent]
            [examples.rabbit-demo.template-resolver :as resolver]))

(defn iframe-block 
  "Iframe content component - just the inner content, wrapper is handled by client.cljs"
  [{:keys [id url zoom blocks] :as block}]
  (let [;; Local state for URL input and zoom level
        local-url (reagent/atom (or url "http://localhost:8080"))
        local-zoom (reagent/atom (or zoom 100))
        editing-url? (reagent/atom false)
        temp-url (reagent/atom "")
        ;; Template resolution
        has-templates? (reagent/atom false)
        resolved-url (reagent/atom "")
        template-errors (reagent/atom nil)]
    
    ;; Effect to resolve templates when URL or blocks change
    (reagent/create-class
     {:component-did-mount
      (fn []
        (when @local-url
          (let [refs (resolver/parse-template-refs @local-url)]
            (reset! has-templates? (seq refs))
            (when (seq refs)
              (resolver/update-dependencies! id @local-url)
              ;; Initial resolution
              (let [blocks (or (:blocks block) {})
                    resolved (resolver/resolve-template @local-url blocks)]
                (reset! resolved-url resolved))))))
      
      :component-will-unmount
      (fn []
        (resolver/clear-dependencies! id))
      
      :component-did-update
      (fn [this [_ old-props]]
        (let [new-props (second (reagent/argv this))
              new-url (:url new-props)
              new-blocks (:blocks new-props)]
          ;; Only update if URL or blocks actually changed
          (when (or (not= new-url (:url old-props))
                    (not= new-blocks (:blocks old-props)))
            (when @local-url
              (let [refs (resolver/parse-template-refs @local-url)]
                (when (seq refs)
                  (resolver/update-dependencies! id @local-url)
                  (let [resolved (resolver/resolve-template @local-url new-blocks)]
                    (reset! resolved-url resolved))))))))
      
      :reagent-render
      (fn [{:keys [id url zoom blocks] :as block}]
      [:div.iframe-block-content
       {:style {:width "100%"
                :height "100%"
                :display "flex"
                :flex-direction "column"
                :padding "10px"}}
       
       ;; URL Input Bar
       [:div.url-bar
        {:style {:padding "8px"
                 :background "rgba(0,0,0,0.5)"
                 :border "1px solid rgba(138,43,226,0.2)"
                 :border-radius "4px"
                 :margin-bottom "8px"
                 :display "flex"
                 :align-items "center"
                 :gap "8px"}}
        
        ;; URL display/input
        (if @editing-url?
          [:input
           {:type "text"
            :value @temp-url
            :placeholder "Enter URL... (use {blockId.field} for templates)"
            :style {:flex 1
                    :background "rgba(0,0,0,0.7)"
                    :border "1px solid #8a2be2"
                    :color "#8a2be2"
                    :padding "4px 8px"
                    :border-radius "4px"
                    :font-family "monospace"
                    :font-size "12px"}
            :on-change #(reset! temp-url (.. % -target -value))
            :on-key-down (fn [e]
                          (case (.-key e)
                            "Enter" (do
                                     (.preventDefault e)
                                     (reset! local-url @temp-url)
                                     (reset! editing-url? false)
                                     ;; Update block state
                                     (r/dispatch! [:update-block id {:url @temp-url}]))
                            "Escape" (do
                                      (.preventDefault e)
                                      (reset! editing-url? false))
                            nil))
            :auto-focus true}]
          [:div
           {:style {:flex 1
                    :display "flex"
                    :flex-direction "column"
                    :gap "2px"}}
           ;; Template URL display
           [:div
            {:style {:color (if @has-templates? "#00ff9f" "#8a2be2")
                     :font-family "monospace"
                     :font-size "11px"
                     :padding "4px 8px"
                     :background (if @has-templates? 
                                  "rgba(0,255,159,0.1)" 
                                  "rgba(138,43,226,0.1)")
                     :border-radius "4px"
                     :cursor "pointer"
                     :white-space "nowrap"
                     :overflow "hidden"
                     :text-overflow "ellipsis"}
             :on-click (fn []
                        (reset! temp-url @local-url)
                        (reset! editing-url? true))}
            @local-url]
           ;; Show resolved URL if templates exist
           (when @has-templates?
             [:div
              {:style {:color "#666"
                       :font-family "monospace"
                       :font-size "10px"
                       :padding "2px 8px"
                       :white-space "nowrap"
                       :overflow "hidden"
                       :text-overflow "ellipsis"}}
              "→ " @resolved-url])])
        
        ;; Zoom controls
        [:div.zoom-controls
         {:style {:display "flex"
                  :align-items "center"
                  :gap "4px"}}
         
         ;; Zoom out button
         [:button
          {:style {:background "rgba(138,43,226,0.1)"
                   :border "1px solid rgba(138,43,226,0.3)"
                   :color "#8a2be2"
                   :width "24px"
                   :height "24px"
                   :border-radius "4px"
                   :cursor "pointer"
                   :display "flex"
                   :align-items "center"
                   :justify-content "center"
                   :font-size "16px"
                   :font-weight "bold"}
           :on-click (fn []
                      (let [new-zoom (max 25 (- @local-zoom 10))]
                        (reset! local-zoom new-zoom)
                        (r/dispatch! [:update-block id {:zoom new-zoom}])))}
          "−"]
         
         ;; Zoom percentage display
         [:span
          {:style {:color "#8a2be2"
                   :font-family "monospace"
                   :font-size "11px"
                   :min-width "45px"
                   :text-align "center"}}
          (str @local-zoom "%")]
         
         ;; Zoom in button
         [:button
          {:style {:background "rgba(138,43,226,0.1)"
                   :border "1px solid rgba(138,43,226,0.3)"
                   :color "#8a2be2"
                   :width "24px"
                   :height "24px"
                   :border-radius "4px"
                   :cursor "pointer"
                   :display "flex"
                   :align-items "center"
                   :justify-content "center"
                   :font-size "16px"
                   :font-weight "bold"}
           :on-click (fn []
                      (let [new-zoom (min 200 (+ @local-zoom 10))]
                        (reset! local-zoom new-zoom)
                        (r/dispatch! [:update-block id {:zoom new-zoom}])))}
          "+"]
         
         ;; Reset zoom button
         [:button
          {:style {:background "rgba(255,79,153,0.1)"
                   :border "1px solid rgba(255,79,153,0.3)"
                   :color "#ff4f99"
                   :padding "2px 6px"
                   :border-radius "4px"
                   :cursor "pointer"
                   :font-family "monospace"
                   :font-size "10px"
                   :margin-left "4px"}
           :on-click (fn []
                      (reset! local-zoom 100)
                      (r/dispatch! [:update-block id {:zoom 100}]))}
          "100%"]]]
       
       ;; Iframe container
       [:div.iframe-container
        {:style {:flex 1
                 :position "relative"
                 :overflow "auto"
                 :background "rgba(0,0,0,0.2)"
                 :border "1px solid rgba(138,43,226,0.1)"
                 :border-radius "4px"}}
        
        ;; The actual iframe
        [:iframe
         {:src (if @has-templates? @resolved-url @local-url)
          :style {:width (str @local-zoom "%")
                  :height (str @local-zoom "%")
                  :border "none"
                  :transform-origin "top left"
                  :transform (str "scale(" (/ 100 @local-zoom) ")")
                  :position "absolute"
                  :top 0
                  :left 0}
          ;; Security settings - adjust as needed
          :sandbox "allow-same-origin allow-scripts allow-forms allow-popups"
          :key (str id "-" (:refresh-at block))}]]])})))