(ns examples.rabbit-demo.monaco
  "Monaco Editor wrapper for ClojureScript"
  (:require [reagent.core :as r]
            [examples.rabbit-demo.themes :as themes]
            ["@monaco-editor/react" :default MonacoEditor :refer [loader]]))

;; Define custom theme for Rabbit Demo
(def rabbit-theme
  #js {:base "vs-dark"
       :inherit true
       ;; :font-family (themes/get-font-family :monospace)
       ;; :font (themes/get-font-family :monospace)
       :rules #js [;; Keywords - bright pink
                   #js {:token "keyword" :foreground "FF006E"}
                   ;; Strings - cyan/aqua
                   #js {:token "string" :foreground "00FFD4"}
                   ;; Numbers - yellow/gold
                   #js {:token "number" :foreground "FFB700"}
                   ;; Comments - muted pink
                   #js {:token "comment" :foreground "FF4F9999"}
                   ;; Operators
                   #js {:token "delimiter.bracket" :foreground "FF4F99"}
                   #js {:token "delimiter.parenthesis" :foreground "FF4F99"}
                   ;; Default text
                   #js {:token "" :foreground "FFFFFF88"}]
       :colors #js {;; Editor background - semi-transparent to show gradient
                    "editor.background" "#00000088"
                    ;; Current line highlight
                    "editor.lineHighlightBackground" "#FF006E11"
                    ;; Selection
                    "editor.selectionBackground" "#FF006E33"
                    ;; Cursor
                    "editorCursor.foreground" "#FF006E"
                    ;; Line numbers
                    "editorLineNumber.foreground" "#FF4F9966"
                    "editorLineNumber.activeForeground" "#FF006E"
                    ;; Indent guides
                    "editorIndentGuide.background" "#FF006E22"
                    "editorIndentGuide.activeBackground" "#FF006E44"
                    ;; Brackets
                    "editorBracketMatch.background" "#FF006E33"
                    "editorBracketMatch.border" "#FF006E"
                    ;; Scrollbar
                    "scrollbarSlider.background" "#FF006E33"
                    "scrollbarSlider.hoverBackground" "#FF006E55"
                    "scrollbarSlider.activeBackground" "#FF006E77"}})

;; Initialize the theme
(defonce theme-initialized
  (-> (.init loader)
      (.then (fn [monaco]
               (when monaco
                 (let [editor (.-editor ^js monaco)]
                   (when editor
                     (.defineTheme ^js editor "rabbit-theme" rabbit-theme)
                     ;; (js/console.log "Rabbit theme initialized")
                     )))))))

(defn sql-editor [{:keys [value on-change height width theme read-only? font-size editor-key]}]
  (let [font-family (themes/get-font-family :monospace)
        #_ (js/console.log "Monaco SQL editor rendering with font-size:" font-size)
        ;; Use provided editor-key if available, otherwise use theme-based key
        component-key (or editor-key (str "monaco-" (hash @themes/current-theme)))]
    ^{:key component-key} ; Force re-mount when key changes
    [:> MonacoEditor
     {:height (or height "100px")
      :width (or width "100%")
      :language "sql"
      :theme (or theme "rabbit-dynamic-theme")
      :value value
      :onChange (when on-change
                  (fn [new-value]
                    (on-change new-value)))
      :options {:minimap {:enabled false}
                :fontSize (or font-size 16)
                :fontFamily font-family
                :lineNumbers "on"
                :glyphMargin false
                :folding false
                :lineDecorationsWidth 10  ; Add padding between line numbers and content
                :lineNumbersMinChars 3
                :renderLineHighlight "none"
                :scrollBeyondLastLine false
                :readOnly (boolean read-only?)
                :automaticLayout true
                :wordWrap "on"
                :padding {:top 5 :bottom 5 :left 5}}}]))  ; Also add left padding

(defn text-editor [{:keys [value on-change height width theme read-only? language]}]
  (let [font-family (themes/get-font-family :monospace)
        theme-key (str "monaco-text-" (hash @themes/current-theme))]
    ^{:key theme-key}
    [:> MonacoEditor
     {:height (or height "100px")
      :width (or width "100%")
      :language (or language "plaintext")
      :theme (or theme "rabbit-dynamic-theme")
      :value value
      :onChange (when on-change
                  (fn [new-value]
                    (on-change new-value)))
      :options {:minimap {:enabled false}
                :fontSize 12
                :fontFamily font-family
                :lineNumbers "off"
                :glyphMargin false
                :folding false
                :lineDecorationsWidth 0
                :renderLineHighlight "none"
                :scrollBeyondLastLine false
                :readOnly (boolean read-only?)
                :automaticLayout true
                :wordWrap "on"
                :padding {:top 5 :bottom 5}}}]))

(defn edn-editor [{:keys [value on-change height width theme read-only?]}]
  (let [font-family (themes/get-font-family :monospace)
        theme-key (str "monaco-edn-" (hash @themes/current-theme))]
    ^{:key theme-key}
    [:> MonacoEditor
     {:height (or height "100px")
      :width (or width "100%")
      :language "clojure"  ; Use Clojure syntax highlighting for EDN
      :theme (or theme "rabbit-dynamic-theme")  ; Use our dynamic theme by default
      :value value
      :onChange (when on-change
                  (fn [new-value]
                    (on-change new-value)))
      :options {:minimap {:enabled false}
                :fontSize 11
                :fontFamily font-family
              :lineNumbers "on"
              :glyphMargin false
              :folding true
              :lineDecorationsWidth 10
              :lineNumbersMinChars 3
              :renderLineHighlight "line"
              :scrollBeyondLastLine false
              :readOnly (boolean read-only?)
              :automaticLayout true
              :wordWrap "on"
              :padding {:top 10 :bottom 10 :left 10}
              :scrollbar {:vertical "auto"
                         :horizontal "auto"
                         :verticalScrollbarSize 10
                         :horizontalScrollbarSize 10}}}]))