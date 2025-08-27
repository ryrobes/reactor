(ns examples.rabbit-demo.themes
  "Dynamic theming system for Rabbit Demo using EDN theme files"
  (:require [reagent.core :as r]
            [cljs.reader :as reader]
            [reactor.core :as reactor]
            [clojure.set]))

;; Current active theme atom
(defonce current-theme (r/atom nil))

;; Available themes cache
(defonce available-themes (r/atom nil))

;; Forward declarations
(declare apply-monaco-theme!)
(declare get-font-family)
(declare inject-theme-styles!)

;; Default theme with cyberpunk green aesthetic
(def default-theme
  {:canvas-background-css {:background "radial-gradient(circle at 20% 50%, #1a1a2e 0%, #0a0a0a 100%)"}
   :base-block-style {:background "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
                      :border "1px solid rgba(0,255,159,0.3)"
                      :border-radius "4px"}
   :base-block-color "rgba(26,26,46,0.8)"
   :base-block-color-selected "rgba(26,26,46,0.95)"
   :block-title-font-color "#00ff9f"
   :block-title-selected-font-color "#00ff9f"
   :editor-rim-color "#00ff9f"
   :editor-outer-rim-color "#00ff9f"
   :pop-1 "#00ff9f"  ; Primary accent (green)
   :pop-2 "#ff006e"   ; Secondary accent (pink)
   :pop-3 "#00ffd4"   ; Tertiary accent (cyan)
   :grid-font-color "#8ff0a4"
   :grid-selected-font-color "#050510"
   :editor-font-color "#8ff0a4"
   :editor-background-color "rgba(10,10,10,0.9)"
   :editor-grid-font-color "#8ff0a4"
   :monospaced-font "JetBrains Mono"
   :base-font "Plus Jakarta Sans"})

(defn fetch-available-themes!
  "Fetch list of available theme files from the server"
  []
  (-> (js/fetch (str (:server-url @reactor/config) "/api/themes"))
      (.then #(.json %))
      (.then (fn [data]
               (let [theme-list (js->clj data :keywordize-keys true)]
                 (reset! available-themes theme-list)
                 ;; (js/console.log "[THEMES] Available themes:" (clj->js theme-list))
                 ))
      ;; (.catch (fn [err]
      ;;           (js/console.error "[THEMES] Failed to fetch theme list:" err)))
             )))

(defn load-theme!
  "Load a specific theme by filename"
  [theme-name]
  (when theme-name
    ;; (js/console.log "[THEMES] Loading theme:" theme-name)
    (-> (js/fetch (str (:server-url @reactor/config) "/api/themes/" theme-name))
        (.then #(.text %))
        (.then (fn [edn-text]
                 (try
                   (let [theme-data (reader/read-string edn-text)]
                     (reset! current-theme (merge default-theme theme-data))
                     ;; (js/console.log "[THEMES] Theme loaded successfully:" theme-name)
                     ;; Apply Monaco theme if available
                     (apply-monaco-theme!)
                     ;; Inject dynamic CSS to override hardcoded colors
                     (inject-theme-styles!))
                   (catch js/Error e
                     (js/console.error "[THEMES] Failed to parse theme:" e)
                     (reset! current-theme default-theme)
                     (inject-theme-styles!)))))
        (.catch (fn [err]
                  (js/console.error "[THEMES] Failed to load theme:" err)
                  (reset! current-theme default-theme)
                  (inject-theme-styles!))))))

(defn get-theme-property
  "Get a property from the current theme with fallback to default"
  [property]
  (or (get @current-theme property)
      (get default-theme property)))

(defn get-primary-color
  "Get the primary accent color - used uniformly across all UI elements"
  []
  (or ;(get-theme-property :pop-1)
      (get-theme-property :editor-rim-color)
      (get-theme-property :editor-outer-rim-color)
      (when-let [tree (get @current-theme :generated-tree)]
        (or (:most-contrasting-light tree)
            (:brightest tree)
            (:most-contrasting tree)))
      "#00ff9f"))

(defn get-secondary-color
  "Get a lighter variant of the primary color for hover states"
  []
  (or (get-theme-property :pop-2)
      (when-let [tree (get @current-theme :generated-tree)]
        (or (:second-brightest tree)
            (:contrast-with-dominant tree)))
      (get-theme-property :grid-font-color)
      "#8ff0a4"))

(defn get-tertiary-color
  "Get an even lighter variant for subtle accents"
  []
  (or (get-theme-property :pop-3)
      (when-let [tree (get @current-theme :generated-tree)]
        (:brightest tree))
      "#00ffd4"))

(defn get-generated-colors
  "Get colors from the generated-tree palette if available"
  []
  (when-let [tree (get @current-theme :generated-tree)]
    {:primary (or (:most-contrasting-light tree)
                  (:brightest tree)
                  (:most-contrasting tree))
     :secondary (or (:contrast-with-dominant tree)
                   (:second-brightest tree))
     :background (or (:darkest tree)
                    (:dominant tree))
     :surface (or (:second-darkest tree)
                 (:most-frequent tree))
     :colors (:colors tree [])}))

(defn apply-block-style
  "Apply UNIFORM theme styles to ALL blocks"
  [base-style block-type]
  (let [theme @current-theme
        theme-base-style (get theme :base-block-style {})
        primary-color (get-primary-color)
        bg-color (or (get theme :base-block-color)
                    (get theme :editor-background-color))
        generated (get-generated-colors)
        
        ;; ALL blocks get the same color scheme for uniformity
        themed-style (merge
                      ;; Keep original positioning and sizing
                      (select-keys base-style [:position :left :top :width :height :display 
                                              :flex-direction :z-index :overflow :flex :min-height
                                              :padding :cursor :transition])
                      ;; Apply theme base styles (shadows, borders, etc)
                      (dissoc theme-base-style :background) ; We'll set this separately
                      ;; Uniform background for all blocks
                      {:background (or bg-color 
                                      (:background theme-base-style)
                                      "rgba(26,26,46,0.8)")}
                      ;; Uniform border color
                      {:border (str "1px solid " primary-color)}
                      ;; Uniform box shadow with primary color
                      {:box-shadow (str "0 0 20px " primary-color "30, "
                                       "inset 0 0 10px " primary-color "10")}
                      ;; Font family from theme
                      {:font-family (get-font-family :base)})]
    themed-style))

(defn apply-canvas-style
  "Apply theme styles to the canvas background"
  [base-style]
  (let [theme @current-theme
        canvas-style (get theme :canvas-background-css {})
        generated (get-generated-colors)
        primary (get-primary-color)
        ;; Clean up conflicting CSS properties to avoid React warnings
        ;; Convert background-color to backgroundColor for React
        cleaned-style (-> canvas-style
                          (clojure.set/rename-keys {:background-color :backgroundColor
                                                    :background-image :backgroundImage
                                                    :background-blend-mode :backgroundBlendMode}))
        ;; Check if theme has any background specified
        has-background? (or (:background cleaned-style)
                           (:backgroundImage cleaned-style)
                           (:backgroundColor cleaned-style))
        ;; Build complete canvas style
        themed-style (merge 
                      base-style
                      (if has-background?
                        cleaned-style
                        ;; Create a grid pattern with dimmed primary color
                        {:backgroundColor "#000000"
                         :backgroundImage (str "linear-gradient(" primary "11 1px, transparent 1px), "
                                             "linear-gradient(90deg, " primary "11 1px, transparent 1px)")
                         :backgroundSize "20px 20px"
                         :backgroundPosition "0 0, 0 0"}))]
    themed-style))

(defn get-font-color
  "Get appropriate font color for a given context - all using theme colors"
  [context]
  (let [theme @current-theme
        primary (get-primary-color)
        secondary (get-secondary-color)
        generated (get-generated-colors)]
    (case context
      :block-title (or (get theme :block-title-font-color)
                      primary)
      :block-title-secondary (or (get theme :grid-font-color)
                                (get theme :editor-grid-font-color)
                                secondary)
      :grid (or (get theme :grid-font-color)
               (get theme :editor-grid-font-color)
               secondary)
      :grid-selected (or (get theme :grid-selected-font-color)
                        (get theme :editor-grid-selected-font-color)
                        "#050510")
      :editor (or (get theme :editor-font-color)
                 (get theme :grid-font-color)
                 secondary)
      :button primary
      :button-hover secondary
      :text-primary (or (get theme :editor-font-color)
                       (get theme :grid-font-color)
                       primary)
      :text-secondary (or (get theme :grid-font-color) 
                         secondary)
      :success primary  ; Use primary for success instead of hardcoded green
      :error (or (get theme :pop-2) "#ff006e")
      primary))) ; Default to primary color

(defn get-accent-color
  "Get accent colors from theme - all unified"
  [index]
  ;; Always return primary for consistency
  (get-primary-color))

(defn get-font-family
  "Get font family for a given context - actually apply the fonts!"
  [context]
  (let [theme @current-theme
        mono-font (get theme :monospaced-font)
        base-font (get theme :base-font)
        result (case context
                 :monospace (if mono-font
                             (str "'" mono-font "', 'JetBrains Mono', 'Cascadia Code', monospace")
                             "'JetBrains Mono', 'Cascadia Code', monospace")
                 :base (if base-font
                        (str "'" base-font "', 'Inter', sans-serif")
                        "'Plus Jakarta Sans', 'Inter', sans-serif")
                 ;; Default to monospace for code-heavy UI
                 (if mono-font
                   (str "'" mono-font "', 'JetBrains Mono', monospace")
                   "'JetBrains Mono', monospace"))]
    ;; (js/console.log "[THEMES] Font for" (name context) ":" result "mono-font:" mono-font)
    result))

(defn create-monaco-theme
  "Create a Monaco editor theme from the current theme"
  []
  (let [theme @current-theme
        primary (get-primary-color)
        secondary (get-secondary-color)
        tertiary (get-tertiary-color)
        generated (get-generated-colors)
        bg-color (or (get theme :editor-background-color)
                    (get theme :editor-param-background-color)
                    (:darkest generated)
                    "#050510")
        text-color (or (get theme :editor-font-color)
                      (get theme :grid-font-color)
                      primary)
        ;; Get data colors for syntax highlighting
        data-colors (get theme :data-colors {})
        keyword-color (or (get data-colors "keyword") primary)
        string-color (or (get data-colors "string") secondary)
        number-color (or (get data-colors "integer") tertiary)
        comment-color (or (:most-frequent generated) "#666666")]
    
    {:base (if (< (js/parseInt (subs bg-color 1 3) 16) 128) "vs-dark" "vs")
     :inherit true
     :rules [{:token "keyword" :foreground keyword-color}
             {:token "string" :foreground string-color}
             {:token "number" :foreground number-color}
             {:token "comment" :foreground comment-color :fontStyle "italic"}
             {:token "function" :foreground primary}
             {:token "variable" :foreground text-color}
             {:token "constant" :foreground tertiary}
             {:token "type" :foreground secondary}]
     :colors {"editor.background" bg-color
             "editor.foreground" text-color
             "editor.lineHighlightBackground" (str bg-color "22")
             "editor.selectionBackground" (str primary "33")
             "editorCursor.foreground" primary
             "editorLineNumber.foreground" (str text-color "66")
             "editorLineNumber.activeForeground" text-color
             "editor.border" (str primary "33")
             "editorIndentGuide.background" (str text-color "11")
             "editorIndentGuide.activeBackground" (str text-color "33")}}))

(defn generate-theme-css
  "Generate CSS styles based on current theme to override hardcoded colors"
  []
  (let [primary (get-primary-color)
        secondary (get-secondary-color)
        tertiary (get-tertiary-color)
        bg-color (or (get-theme-property :editor-background-color) "#0a0a0a")
        mono-font (get-font-family :monospace)
        base-font (get-font-family :base)]
    (str "
      /* Override hardcoded colors from rabbit.html */
      body {
        color: " primary " !important;
        font-family: " mono-font " !important;
      }
      
      /* Scrollbar styling */
      ::-webkit-scrollbar-track {
        background: rgba(0, 0, 0, 0.5) !important;
        border: 1px solid " primary "22 !important;
      }
      
      ::-webkit-scrollbar-thumb {
        background: linear-gradient(180deg, " primary ", " secondary ") !important;
      }
      
      ::-webkit-scrollbar-thumb:hover {
        background: " primary " !important;
        box-shadow: 0 0 10px " primary " !important;
      }
      
      /* Range input styling */
      input[type='range']::-webkit-slider-thumb {
        background: " primary " !important;
        box-shadow: 0 0 15px " primary "cc, 0 0 5px " primary " !important;
      }
      
      input[type='range']::-webkit-slider-thumb:hover {
        box-shadow: 0 0 20px " primary ", 0 0 10px " primary " !important;
      }
      
      input[type='range']::-moz-range-thumb {
        background: " primary " !important;
        box-shadow: 0 0 15px " primary "cc, 0 0 5px " primary " !important;
      }
      
      input[type='range']::-moz-range-thumb:hover {
        box-shadow: 0 0 20px " primary ", 0 0 10px " primary " !important;
      }
      
      /* Loading animation */
      .loading::before {
        background: linear-gradient(90deg, transparent, " primary ", transparent) !important;
      }
      
      .loading h2 {
        color: " primary " !important;
        text-shadow: 0 0 20px " primary "80 !important;
      }
      
      /* Button hover effects */
      button:hover {
        box-shadow: 0 5px 15px " primary "4d !important;
      }
      
      /* Resize handle */
      .resize-handle {
        background: linear-gradient(135deg, transparent 50%, " primary " 50%) !important;
      }
      
      /* Any remaining green overrides */
      [style*='#00ff9f'] {
        color: " primary " !important;
      }
      
      [style*='rgba(0, 255, 159'] {
        color: " primary " !important;
      }
      
      /* Monaco Editor overrides */
      .monaco-editor, .monaco-editor-background, .monaco-editor .inputarea.ime-input {
        background-color: " bg-color " !important;
        font-family: " mono-font " !important;
      }
      
      .monaco-editor .view-lines {
        font-family: " mono-font " !important;
      }
      
      .monaco-editor .line-numbers {
        color: " primary "66 !important;
        font-family: " mono-font " !important;
      }
      
      .monaco-editor .current-line ~ .line-numbers {
        color: " primary " !important;
      }
      
      /* Monaco syntax highlighting */
      .monaco-editor .mtk1 { color: " secondary " !important; }  /* Default text */
      .monaco-editor .mtk4 { color: " primary " !important; }    /* Keywords */
      .monaco-editor .mtk5 { color: " tertiary " !important; }   /* Strings */
      .monaco-editor .mtk6 { color: " primary " !important; }    /* Numbers */
      .monaco-editor .mtk7 { color: " secondary "99 !important; } /* Comments */
      .monaco-editor .mtk8 { color: " primary " !important; }    /* Types */
      .monaco-editor .mtk9 { color: " tertiary " !important; }   /* Functions */
      .monaco-editor .mtk10 { color: " secondary " !important; }  /* Variables */
      
      /* Monaco selections and cursor */
      .monaco-editor .selected-text {
        background-color: " primary "33 !important;
      }
      
      .monaco-editor .selectionHighlight {
        background-color: " primary "22 !important;
      }
      
      .monaco-editor .cursor {
        background-color: " primary " !important;
        color: " primary " !important;
      }
      
      .monaco-editor .cursors-layer .cursor {
        background: " primary " !important;
      }
      
      /* Monaco minimap */
      .monaco-editor .minimap-slider {
        background: " primary "22 !important;
      }
      
      .monaco-editor .minimap-slider:hover {
        background: " primary "44 !important;
      }
      
      /* Monaco scrollbars */
      .monaco-editor .scrollbar .slider {
        background: " primary "33 !important;
      }
      
      .monaco-editor .scrollbar .slider:hover {
        background: " primary "55 !important;
      }
      
      /* Monaco editor border */
      .monaco-editor {
        border: 1px solid " primary "33 !important;
      }
      
      /* Force font family on all Monaco text content */
      .monaco-editor * {
        font-family: " mono-font " !important;
      }
      
      /* Specific targeting for the actual code text */
      .monaco-editor .view-line span {
        font-family: " mono-font " !important;
      }
      
      /* SQL keyword coloring */
      .monaco-editor .token.keyword.sql { color: " primary " !important; }
      .monaco-editor .token.string.sql { color: " tertiary " !important; }
      .monaco-editor .token.number.sql { color: " secondary " !important; }
      .monaco-editor .token.comment.sql { color: " primary "66 !important; }
      .monaco-editor .token.operator.sql { color: " primary " !important; }
      .monaco-editor .token.identifier.sql { color: " secondary " !important; }
    ")))

(defn inject-theme-styles!
  "Inject or update theme CSS styles into the DOM"
  []
  (let [style-id "rabbit-theme-styles"
        existing-style (.getElementById js/document style-id)
        css-text (generate-theme-css)]
    (if existing-style
      ;; Update existing style element
      (set! (.-textContent existing-style) css-text)
      ;; Create new style element
      (let [style-element (.createElement js/document "style")]
        (set! (.-id style-element) style-id)
        (set! (.-type style-element) "text/css")
        (set! (.-textContent style-element) css-text)
        (.appendChild (.-head js/document) style-element)))
    ;; Force Monaco font updates with a slight delay to catch React-created instances
    (js/setTimeout
     (fn []
       (let [mono-font (get-font-family :monospace)]
         ;; Try to update all Monaco editor instances via their API
         (when js/window.monaco
           (try
             ;; Get all editor instances and force font update
             (when-let [editors (js/window.monaco.editor.getEditors)]
               (doseq [editor editors]
                 (try
                   (.updateOptions ^js editor #js {:fontFamily mono-font
                                                   :fontSize 13})
                   (catch js/Error e nil))))
             (catch js/Error e nil)))
         ;; Also force it via direct DOM manipulation as a fallback
         (let [monaco-containers (.querySelectorAll js/document ".monaco-editor")]
           (doseq [container monaco-containers]
             (set! (.-fontFamily (.-style container)) mono-font)
             ;; Find all view-line spans inside this container
             (let [view-lines (.querySelectorAll container ".view-line span")]
               (doseq [line view-lines]
                 (set! (.-fontFamily (.-style line)) mono-font)))))))
     100)
    ;; Also update Mermaid theme if it's loaded
    (when js/window.mermaid
      (let [primary (get-primary-color)
            secondary (get-secondary-color)
            tertiary (get-tertiary-color)
            bg-color (or (get-theme-property :base-block-color) "#1a1a1a")]
        (try
          (.initialize js/window.mermaid
            #js {:startOnLoad false
                 :theme "dark"
                 :themeVariables #js {:primaryColor bg-color
                                     :primaryTextColor primary
                                     :primaryBorderColor primary
                                     :lineColor primary
                                     :secondaryColor secondary
                                     :tertiaryColor "#333"
                                     :background "#0a0a0a"
                                     :mainBkg bg-color
                                     :secondBkg "#2a2a2a"
                                     :tertiaryBkg "#333"
                                     :textColor tertiary
                                     :edgeLabelBackground secondary
                                     :nodeBkg bg-color
                                     :nodeBorder primary
                                     :clusterBkg "#2a2a2a"
                                     :clusterBorder primary
                                     :defaultLinkColor primary}
                 :flowchart #js {:htmlLabels true
                                :curve "linear"
                                :nodeSpacing 50
                                :rankSpacing 100}})
          (catch js/Error e
            ;; Mermaid might not support re-initialization, that's ok
            nil))))))

(defn apply-monaco-theme!
  "Apply the current theme to all Monaco editors"
  []
  (when (and js/window.monaco @current-theme)
    (let [primary (get-primary-color)
          secondary (get-secondary-color)
          tertiary (get-tertiary-color)
          bg-color (or (get-theme-property :editor-background-color) "#050510")
          text-color (or (get-theme-property :editor-font-color) primary)
          font-family (get-font-family :monospace)
          theme-name "rabbit-dynamic-theme"]
      (try
        ;; Create theme with explicit JS object construction to avoid interop issues
        (let [theme-obj #js {:base "vs-dark"
                            :inherit true
                            :rules #js [#js {:token "keyword" :foreground primary}
                                       #js {:token "string" :foreground tertiary}
                                       #js {:token "number" :foreground secondary}
                                       #js {:token "comment" :foreground (str primary "99") :fontStyle "italic"}
                                       #js {:token "function" :foreground primary}
                                       #js {:token "variable" :foreground text-color}
                                       #js {:token "constant" :foreground tertiary}
                                       #js {:token "type" :foreground secondary}]
                            :colors #js {"editor.background" bg-color
                                       "editor.foreground" text-color
                                       "editor.lineHighlightBackground" (str bg-color "22")
                                       "editor.selectionBackground" (str primary "33")
                                       "editorCursor.foreground" primary
                                       "editorLineNumber.foreground" (str text-color "66")
                                       "editorLineNumber.activeForeground" text-color
                                       "editor.border" (str primary "33")
                                       "editorIndentGuide.background" (str text-color "11")
                                       "editorIndentGuide.activeBackground" (str text-color "33")}}]
          (.defineTheme js/window.monaco.editor theme-name theme-obj)
          (.setTheme js/window.monaco.editor theme-name))
        (catch js/Error e
          ;; Theme definition might fail silently
          nil))
      ;; Force update all editor instances with explicit options
      (try
        (when js/window.monaco.editor.getEditors
          (let [editors (.getEditors js/window.monaco.editor)
                options #js {:fontFamily font-family
                           :fontSize 13
                           :theme theme-name}]
            (doseq [editor editors]
              (try
                (.updateOptions ^js editor options)
                (catch js/Error e nil)))))
        (catch js/Error e
          ;; getEditors might not exist
          nil)))))

(defn setup-monaco-observer!
  "Set up a MutationObserver to catch new Monaco instances and apply theme"
  []
  (when (exists? js/MutationObserver)
    (let [observer (js/MutationObserver.
                    (fn [mutations]
                      ;; Check if any Monaco editors were added
                      (doseq [mutation mutations]
                        (doseq [node (.-addedNodes mutation)]
                          (when (and node (.-classList node))
                            (when (or (.contains (.-classList node) "monaco-editor")
                                     (.querySelector node ".monaco-editor"))
                              ;; Re-inject styles when new Monaco editor detected
                              (js/setTimeout inject-theme-styles! 50)))))))]
      ;; Start observing
      (.observe observer js/document.body
                #js {:childList true
                     :subtree true}))))

(defn init!
  "Initialize the theming system"
  []
  ;; Load available themes
  (fetch-available-themes!)
  ;; Load default theme or saved preference
  (let [saved-theme (js/localStorage.getItem "rabbit-demo-theme")]
    (if saved-theme
      (load-theme! saved-theme)
      (do
        (reset! current-theme default-theme)
        ;; Inject default theme styles
        (inject-theme-styles!))))
  ;; Set up observer for new Monaco instances
  (setup-monaco-observer!)
  ;; Set up Monaco theme application when Monaco loads
  (js/setInterval
   (fn []
     (when js/window.monaco
       ;; Check if theme has changed or Monaco was never initialized
       (let [current-theme-hash (hash @current-theme)]
         (when (or (not js/window.monacoThemeApplied)
                   (not= js/window.monacoThemeHash current-theme-hash))
           (apply-monaco-theme!)
           (set! js/window.monacoThemeApplied true)
           (set! js/window.monacoThemeHash current-theme-hash)))))
   500))

(defn set-theme!
  "Set and persist theme selection"
  [theme-name]
  (if theme-name
    (do
      (js/localStorage.setItem "rabbit-demo-theme" theme-name)
      (load-theme! theme-name)
      ;; Clear the Monaco theme hash to force re-application
      (set! js/window.monacoThemeHash nil))
    (do
      (js/localStorage.removeItem "rabbit-demo-theme")
      (reset! current-theme default-theme)
      (inject-theme-styles!)
      ;; Clear the Monaco theme hash to force re-application
      (set! js/window.monacoThemeHash nil))))