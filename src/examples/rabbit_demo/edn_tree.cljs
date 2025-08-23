(ns examples.rabbit-demo.edn-tree
  "Tree view component for EDN data visualization"
  (:require [reagent.core :as reagent]
            [clojure.string :as str]))

;; ============= Tree Node Component =============

(defn get-type-color [value]
  "Return color based on EDN type"
  (cond
    (map? value) "#00ffd4"      ; Cyan for maps
    (vector? value) "#00ff9f"   ; Green for vectors
    (set? value) "#ff006e"       ; Pink for sets
    (list? value) "#ff4f99"      ; Light pink for lists
    (keyword? value) "#ffb700"   ; Gold for keywords
    (string? value) "#8ff0a4"    ; Light green for strings
    (number? value) "#ffd700"    ; Yellow for numbers
    (boolean? value) "#9b59b6"   ; Purple for booleans
    (nil? value) "#666"          ; Gray for nil
    :else "#fff"))               ; White for unknown

(defn get-type-bg-color [value]
  "Return background color based on EDN type - more visible versions"
  (cond
    (map? value) "rgba(0,255,212,0.15)"      ; Cyan bg for maps
    (vector? value) "rgba(0,255,159,0.15)"   ; Green bg for vectors
    (set? value) "rgba(255,0,110,0.15)"      ; Pink bg for sets
    (list? value) "rgba(255,79,153,0.15)"    ; Light pink bg for lists
    (keyword? value) "rgba(255,183,0,0.2)"   ; Gold bg for keywords
    (string? value) "rgba(143,240,164,0.15)" ; Light green bg for strings
    (number? value) "rgba(255,215,0,0.2)"    ; Yellow bg for numbers
    (boolean? value) "rgba(155,89,182,0.2)"  ; Purple bg for booleans
    (nil? value) "rgba(102,102,102,0.25)"    ; Gray bg for nil
    :else "transparent"))                      ; Transparent for unknown

(defn get-type-label [value]
  "Return a label for collection types"
  (cond
    (map? value) (str "{" (count value) "}")
    (vector? value) (str "[" (count value) "]")
    (set? value) (str "#{" (count value) "}")
    (list? value) (str "(" (count value) ")")
    :else nil))

(defn format-value [value]
  "Format a leaf value for display"
  (cond
    (string? value) (pr-str value)
    (keyword? value) (str value)
    (nil? value) "nil"
    :else (str value)))

(declare tree-node) ; Forward declaration for recursion

(defn tree-collection-node
  "Render a collection node with expand/collapse"
  [{:keys [path label value expanded-paths on-toggle on-select selected-path search-term]}]
  (let [expanded? (contains? expanded-paths path)
        children (cond
                   (map? value) (map (fn [[k v]] [k v]) value)
                   (vector? value) (map-indexed (fn [idx item] [idx item]) value)
                   (set? value) (map-indexed (fn [idx item] [(str "#{" idx "}") item]) value)
                   (list? value) (map-indexed (fn [idx item] [(str "(" idx ")") item]) value)
                   :else [])
        type-label (get-type-label value)
        is-selected? (= path selected-path)
        matches-search? (and search-term
                             (or (str/includes? (str/lower-case (str label)) 
                                              (str/lower-case search-term))
                                 (str/includes? (str/lower-case (pr-str value))
                                              (str/lower-case search-term))))]
    [:div {:style {:margin-left (if (empty? path) "0" "20px")}}
     [:div {:style {:display "flex"
                    :align-items "center"
                    :padding "2px 4px"
                    :cursor "pointer"
                    :background (cond
                                 is-selected? "rgba(0,255,212,0.2)"
                                 matches-search? "rgba(255,183,0,0.1)"
                                 :else "transparent")
                    :border-radius "2px"
                    :margin "1px 0"}
            :on-click (fn [e]
                       (.stopPropagation e)
                       (on-toggle path)
                       (when on-select (on-select path value)))}
      ;; Expand/collapse arrow
      [:span {:style {:display "inline-block"
                      :width "12px"
                      :margin-right "4px"
                      :color "rgba(255,255,255,0.4)"
                      :transform (if expanded? "rotate(90deg)" "rotate(0deg)")
                      :transition "transform 0.2s"
                      :font-size "10px"}}
       (when (seq children) "▶")]
      ;; Key/index label
      (when label
        [:span {:style {:color (if (keyword? label) "#ffb700" "#00ffd4")
                        :background (when (keyword? label) "rgba(255,183,0,0.2)")
                        :padding "1px 3px"
                        :border-radius "2px"
                        :margin-right "6px"
                        :font-family "'JetBrains Mono', monospace"
                        :font-size "11px"}}
         (str label)])
      ;; Type label
      [:span {:style {:color (get-type-color value)
                      :background (get-type-bg-color value)
                      :padding "1px 4px"
                      :border-radius "2px"
                      :opacity 0.9
                      :font-family "'JetBrains Mono', monospace"
                      :font-size "10px"}}
       type-label]
      ;; Item count
      (when (> (count children) 0)
        [:span {:style {:margin-left "6px"
                        :color "rgba(255,255,255,0.3)"
                        :font-size "9px"
                        :font-family "'JetBrains Mono', monospace"}}
         (str (count children) " items")])]
     ;; Children (when expanded)
     (when expanded?
       [:div {:style {:margin-left "4px"
                      :border-left "1px solid rgba(0,255,212,0.1)"}}
        (for [[k v] children]
          ^{:key (str path "/" k)}
          [tree-node {:path (conj path k)
                     :label k
                     :value v
                     :expanded-paths expanded-paths
                     :on-toggle on-toggle
                     :on-select on-select
                     :selected-path selected-path
                     :search-term search-term}])])]))

(defn tree-leaf-node
  "Render a leaf value node"
  [{:keys [path label value on-select selected-path search-term]}]
  (let [is-selected? (= path selected-path)
        formatted (format-value value)
        matches-search? (and search-term
                             (or (and label (str/includes? (str/lower-case (str label))
                                                          (str/lower-case search-term)))
                                 (str/includes? (str/lower-case formatted)
                                              (str/lower-case search-term))))]
    [:div {:style {:margin-left (if (empty? path) "0" "20px")
                   :display "flex"
                   :align-items "center"
                   :padding "2px 4px"
                   :cursor "pointer"
                   :background (cond
                               is-selected? "rgba(0,255,212,0.2)"
                               matches-search? "rgba(255,183,0,0.1)"
                               :else "transparent")
                   :border-radius "2px"
                   :margin "1px 0"}
           :on-click (fn [e]
                      (.stopPropagation e)
                      (when on-select (on-select path value)))}
     ;; Spacer for alignment with collection nodes
     [:span {:style {:display "inline-block"
                     :width "16px"}}]
     ;; Key/index label
     (when label
       [:span {:style {:color (if (keyword? label) "#ffb700" "#00ffd4")
                       :background (when (keyword? label) "rgba(255,183,0,0.2)")
                       :padding "1px 3px"
                       :border-radius "2px"
                       :margin-right "6px"
                       :font-family "'JetBrains Mono', monospace"
                       :font-size "11px"}}
        (str label)])
     ;; Value
     [:span {:style {:color (get-type-color value)
                     :background (get-type-bg-color value)
                     :padding "1px 4px"
                     :border-radius "2px"
                     :font-family "'JetBrains Mono', monospace"
                     :font-size "11px"
                     :max-width "300px"
                     :overflow "hidden"
                     :text-overflow "ellipsis"
                     :white-space "nowrap"}}
      formatted]]))

(defn tree-node
  "Main tree node component that delegates to collection or leaf"
  [{:keys [value] :as props}]
  (if (coll? value)
    [tree-collection-node props]
    [tree-leaf-node props]))

;; ============= Main Tree View Component =============

;; Global state for tree expansion - persists across all component re-renders
(defonce tree-expanded-paths (reagent/atom #{[]}))
(defonce tree-selected-path (reagent/atom nil))
(defonce tree-initialized? (atom false))

(defn edn-tree-view
  "Main tree view component for EDN data"
  [{:keys [data on-select search-term initial-depth]
    :or {initial-depth 2}}]
  ;; Initialize expansion on first use only
  (when-not @tree-initialized?
    (reset! tree-initialized? true)
    (reset! tree-expanded-paths #{[]})
    ;; Expand to initial depth
    (letfn [(expand-fn [value path depth]
              (when (and (coll? value) (< depth initial-depth))
                (swap! tree-expanded-paths conj path)
                (doseq [[k v] (cond
                               (map? value) value
                               (sequential? value) (map-indexed vector value)
                               :else [])]
                  (expand-fn v (conj path k) (inc depth)))))]
      (expand-fn data [] 0)))
  
  [:div {:class "edn-tree-container"
         :style {:font-family "'JetBrains Mono', monospace"
                 :font-size "12px"
                 :color "#fff"
                 :padding "10px"
                 :overflow-y "auto"
                 :overflow-x "auto"
                 :height "100%"
                 :background "rgba(0,0,0,0.2)"
                 :scrollbar-width "thin"
                 :scrollbar-color "rgba(0,255,212,0.3) transparent"}}
   [tree-node {:path []
               :label nil
               :value data
               :expanded-paths @tree-expanded-paths
               :on-toggle (fn [path]
                           (if (contains? @tree-expanded-paths path)
                             (swap! tree-expanded-paths disj path)
                             (swap! tree-expanded-paths conj path)))
               :on-select (fn [path value]
                           (reset! tree-selected-path path)
                           (when on-select (on-select path value)))
               :selected-path @tree-selected-path
               :search-term search-term}]])

;; ============= Tree Controls Component =============

(defn tree-controls
  "Controls for the tree view (expand all, collapse all, search)"
  [{:keys [on-expand-all on-collapse-all on-search]}]
  [:div {:style {:display "flex"
                 :gap "10px"
                 :padding "5px"
                 :border-bottom "1px solid rgba(0,255,212,0.2)"
                 :align-items "center"}}
   [:button {:style {:padding "2px 6px"
                     :background "rgba(0,255,212,0.1)"
                     :color "#00ffd4"
                     :border "1px solid rgba(0,255,212,0.3)"
                     :border-radius "2px"
                     :cursor "pointer"
                     :font-size "9px"
                     :font-family "'JetBrains Mono', monospace"}
             :on-click on-expand-all}
    "EXPAND ALL"]
   [:button {:style {:padding "2px 6px"
                     :background "rgba(0,255,212,0.1)"
                     :color "#00ffd4"
                     :border "1px solid rgba(0,255,212,0.3)"
                     :border-radius "2px"
                     :cursor "pointer"
                     :font-size "9px"
                     :font-family "'JetBrains Mono', monospace"}
             :on-click on-collapse-all}
    "COLLAPSE"]
   [:input {:type "text"
            :placeholder "Search..."
            :style {:flex 1
                    :background "rgba(0,255,212,0.05)"
                    :color "#00ffd4"
                    :border "1px solid rgba(0,255,212,0.2)"
                    :border-radius "2px"
                    :padding "2px 5px"
                    :font-family "'JetBrains Mono', monospace"
                    :font-size "10px"
                    :outline "none"}
            :on-change (fn [e]
                        (when on-search
                          (on-search (.. e -target -value))))}]])