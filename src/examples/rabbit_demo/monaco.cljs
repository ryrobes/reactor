(ns examples.rabbit-demo.monaco
  "Monaco Editor wrapper for ClojureScript"
  (:require [reagent.core :as r]
            ["@monaco-editor/react" :default MonacoEditor :refer [loader]]))

;; Define custom theme for Rabbit Demo
(def rabbit-theme
  #js {:base "vs-dark"
       :inherit true
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
                    "editor.background" "#1a1a2e00"
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
                     (js/console.log "Rabbit theme initialized"))))))))

(defn sql-editor [{:keys [value on-change height width theme read-only?]}]
  [:> MonacoEditor
   {:height (or height "100px")
    :width (or width "100%")
    :language "sql"
    :theme (or theme "vs-dark")
    :value value
    :onChange (when on-change
                (fn [new-value]
                  (on-change new-value)))
    :options {:minimap {:enabled false}
              :fontSize 12
              :fontFamily "'JetBrains Mono', 'Courier New', monospace"
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
              :padding {:top 5 :bottom 5 :left 5}}}])  ; Also add left padding

(defn text-editor [{:keys [value on-change height width theme read-only? language]}]
  [:> MonacoEditor
   {:height (or height "100px")
    :width (or width "100%")
    :language (or language "plaintext")
    :theme (or theme "vs-dark")
    :value value
    :onChange (when on-change
                (fn [new-value]
                  (on-change new-value)))
    :options {:minimap {:enabled false}
              :fontSize 12
              :fontFamily "'JetBrains Mono', 'Courier New', monospace"
              :lineNumbers "off"
              :glyphMargin false
              :folding false
              :lineDecorationsWidth 0
              :renderLineHighlight "none"
              :scrollBeyondLastLine false
              :readOnly (boolean read-only?)
              :automaticLayout true
              :wordWrap "on"
              :padding {:top 5 :bottom 5}}}])

(defn edn-editor [{:keys [value on-change height width theme read-only?]}]
  [:> MonacoEditor
   {:height (or height "100px")
    :width (or width "100%")
    :language "clojure"  ; Use Clojure syntax highlighting for EDN
    :theme (or theme "rabbit-theme")  ; Use our custom theme by default
    :value value
    :onChange (when on-change
                (fn [new-value]
                  (on-change new-value)))
    :options {:minimap {:enabled false}
              :fontSize 11
              :fontFamily "'JetBrains Mono', 'Courier New', monospace"
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
                         :horizontalScrollbarSize 10}}}])