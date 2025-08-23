(ns examples.rabbit-demo.iframe-block
  "Iframe block component for embedding external content with zoom controls and template support"
  (:require [reactor.core :as r]
            [reagent.core :as reagent]
            [reagent.ratom :as ratom]
            [examples.rabbit-demo.template-resolver :as resolver]
            [examples.rabbit-demo.reactive-queries :as rq]))

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
        template-errors (reagent/atom nil)
        ;; Track iframe element and last sent message
        iframe-ref (atom nil)
        last-sent-hash (atom nil)
        ;; Simple atom for resolved URL - we'll update it manually
        resolved-url (reagent/atom (or url "http://localhost:8080"))
        ;; Store blocks in an atom so we can access the latest version
        current-blocks (atom blocks)
        ;; Manual update function that only runs when needed
        update-resolved-url! (fn []
                               (let [url-str @local-url
                                     refs (resolver/parse-template-refs url-str)]
                                 (if (seq refs)
                                   (let [_ (js/console.log "[IFRAME] Current blocks:" (clj->js @current-blocks))
                                         _ (js/console.log "[IFRAME] Template refs found:" (clj->js refs))
                                         _ (js/console.log "[IFRAME] Blocks keys:" (clj->js (keys @current-blocks)))
                                         new-resolved (resolver/resolve-template url-str @current-blocks)]
                                     (js/console.log "[IFRAME] Resolving template. Old:" @resolved-url "New:" new-resolved)
                                     (when (not= @resolved-url new-resolved)
                                       (js/console.log "[IFRAME] URL changed! Updating...")
                                       (reset! resolved-url new-resolved)
                                       ;; Send update via postMessage (cross-origin safe)
                                       (when-let [iframe @iframe-ref]
                                         (js/console.log "[IFRAME] Iframe element exists, sending update via postMessage")
                                         (when-let [content-window (.-contentWindow iframe)]
                                           (let [url (js/URL. new-resolved "http://localhost:8083")
                                                 hash-part (.-hash url)]
                                             (js/console.log "[IFRAME] Sending hash update:" hash-part)
                                             (when (and hash-part 
                                                       (> (.-length hash-part) 1)
                                                       (not= hash-part @last-sent-hash))
                                               (reset! last-sent-hash hash-part)
                                               ;; Use postMessage for cross-origin communication
                                               (try
                                                 (.postMessage content-window
                                                              #js {:type "reactor-state-update"
                                                                   :hash hash-part}
                                                              "*")
                                                 (js/console.log "[IFRAME] PostMessage sent successfully!")
                                                 (catch js/Error e
                                                   (js/console.error "[IFRAME] Error sending postMessage:" e))))))))))
                                   (reset! resolved-url url-str)))
        ;; Track last known results to detect changes
        last-results (atom nil)
        ;; Periodic check for changes (instead of watcher to avoid feedback loops)
        check-interval (atom nil)]
    
    ;; Create component with lifecycle methods
    (reagent/create-class
     {:component-did-mount
      (fn [this]
        (let [props (reagent/props this)]
          (js/console.log "[IFRAME] Component mounted with props:" (clj->js props))
          ;; Initialize current-blocks with the blocks from props
          (reset! current-blocks (:blocks props))
          (js/console.log "[IFRAME] Initial blocks set:" (clj->js @current-blocks))
          (update-resolved-url!))
        ;; Set up periodic check for template changes (every 2 seconds)
        (let [url-str @local-url
              refs (resolver/parse-template-refs url-str)]
          (when (seq refs)
            (reset! check-interval
                    (js/setInterval
                      (fn []
                        ;; Get current results for referenced blocks
                        (let [current-results (into {}
                                                (for [{:keys [block-id]} refs]
                                                  [block-id (rq/get-block-results block-id)]))]
                          ;; Only update if results actually changed
                          (when (not= @last-results current-results)
                            (js/console.log "[IFRAME] Block results changed at" (.toISOString (js/Date.)))
                            (reset! last-results current-results)
                            ;; Update the resolved URL which will trigger postMessage
                            (update-resolved-url!))))
                      500)))))  ;; Reduced from 2000ms to 500ms for faster updates
      
      :component-will-unmount
      (fn []
        ;; Clean up interval
        (when @check-interval
          (js/clearInterval @check-interval)))
      
      :component-did-update
      (fn [this [_ old-props]]
        (let [new-props (reagent/props this)]
          ;; Update current-blocks atom with latest blocks
          (reset! current-blocks (:blocks new-props))
          ;; Update if blocks changed (for manual template resolution)
          (when (not= (:blocks old-props) (:blocks new-props))
            (js/console.log "[IFRAME] Blocks prop changed, updating resolved URL")
            (update-resolved-url!))))
      
      :reagent-render
      (fn [{:keys [id url zoom blocks] :as block}]
        (let [url-str @local-url
              refs (resolver/parse-template-refs url-str)
              has-templates (seq refs)
              current-resolved-url @resolved-url]
          (reset! has-templates? has-templates)
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
              "→ " current-resolved-url])])
        
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
        
        ;; The actual iframe with postMessage communication
        [:iframe
         {:ref (fn [iframe-element]
                 (when iframe-element
                   (reset! iframe-ref iframe-element)
                   ;; Send initial hash params once iframe loads
                   (when has-templates
                     (set! (.-onload iframe-element)
                           (fn []
                             (let [url (js/URL. current-resolved-url "http://localhost:8083")
                                   hash-part (.-hash url)]
                               (when (and hash-part (> (.-length hash-part) 1))
                                 (js/console.log "[IFRAME] Iframe loaded, sending initial hash:" hash-part)
                                 (try
                                   (.postMessage (.-contentWindow iframe-element)
                                                #js {:type "reactor-state-update"
                                                     :hash hash-part}
                                                "*")
                                   (js/console.log "[IFRAME] Initial postMessage sent!")
                                   (catch js/Error e
                                     (js/console.error "[IFRAME] Error sending initial postMessage:" e)))))))))
                 ;; React ref callbacks must return undefined
                 nil)
          :src (if has-templates
                 ;; For templates, use base URL without hash to avoid reloads
                 ;; We'll send hash params via postMessage instead
                 (let [url (js/URL. current-resolved-url "http://localhost:8083")]
                   (set! (.-hash url) "")
                   (.toString url))
                 url-str)
          :style {:width (str @local-zoom "%")
                  :height (str @local-zoom "%")
                  :border "none"
                  :transform-origin "top left"
                  :transform (str "scale(" (/ 100 @local-zoom) ")")
                  :position "absolute"
                  :top 0
                  :left 0}
          ;; Security settings - allow-same-origin is crucial for postMessage
          ;; Commenting out sandbox for now to ensure postMessage works
          ;; :sandbox "allow-same-origin allow-scripts allow-forms allow-popups"
          ;; Only change key if the base URL changes, not hash params
          :key (str id "-" (if has-templates
                             (let [url (js/URL. current-resolved-url "http://localhost:8083")]
                               (set! (.-hash url) "")
                               (set! (.-search url) "")
                               (.toString url))
                             url-str))}]]]))})))