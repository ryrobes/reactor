(ns examples.rabbit-demo.monaco
  "Monaco Editor wrapper for ClojureScript"
  (:require [reagent.core :as r]
            ["@monaco-editor/react" :default MonacoEditor]))

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