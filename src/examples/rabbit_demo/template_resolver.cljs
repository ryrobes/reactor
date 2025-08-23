(ns examples.rabbit-demo.template-resolver
  "Dynamic template resolution system for referencing data across blocks
   
   Template Syntax Examples:
   - {blockId.results.0.session_id} - Get session_id from first result row
   - {blockId.*timestamp} - Get the as-of timestamp from a query block
   - {blockId.*session_id} - Get session_id (from results or block level)
   - {blockId.results.0.value} - Access nested field in query results
   - {blockId.sql} - Get the SQL query string from a query block
   
   When a query block updates, all blocks with templates referencing it
   will automatically refresh. Visual connections show these dependencies
   as dashed pink lines."
  (:require [clojure.string :as str]
            [reagent.core :as reagent]
            [clojure.edn :as edn]
            [clojure.string :as cstr]
            [examples.rabbit-demo.reactive-queries :as rq]))

;; =============================================================================
;; Template Parsing
;; =============================================================================

(defn parse-template-refs
  "Parse a template string and extract all {blockId.path} references
   Returns a vector of {:block-id :path :full-ref} maps"
  [template-str]
  (when template-str
    (let [pattern #"\{([^}]+)\}"
          matches (re-seq pattern template-str)]
      (vec
        (for [[full-match ref-str] matches]
          (let [parts (str/split ref-str #"\.")
                block-id (first parts)
                path (vec (rest parts))]
            {:block-id block-id
             :path path
             :full-ref full-match
             :ref-str ref-str}))))))

(defn extract-special-refs
  "Extract special references like *timestamp, *session_id, etc."
  [path]
  (filter #(str/starts-with? % "*") path))

(defn resolve-path
  "Resolve a path in a nested data structure
   Handles both keyword and index access"
  [data path]
  (reduce
    (fn [current-data path-part]
      (cond
                     ;; Nil check - stop traversing if we hit nil
                     (nil? current-data)
                     nil
                     
                     ;; Special timestamp reference - get as-of from block
                     (= path-part "*timestamp")
                     (:as-of current-data)
                     
                     ;; Special session reference - extract from results  
                     (= path-part "*session_id")
                     (or
                       ;; Try to get session_id from first result row
                       (when-let [results (:results current-data)]
                         (when (seq results)
                           (:session_id (first results))))
                       ;; Fallback to block-level session-id
                       (:session-id current-data)
                       ;; For query blocks, check if we're at the block level and have results
                       (when (and (nil? (:session-id current-data)) (:results current-data))
                         (when (seq (:results current-data))
                           (:session_id (first (:results current-data))))))
                     
                     ;; Numeric index for arrays/vectors
                     (re-matches #"\d+" path-part)
                     (let [idx (js/parseInt path-part)]
                       (cond
                         (vector? current-data) (nth current-data idx nil)
                         (sequential? current-data) (nth (vec current-data) idx nil)
                         :else nil))
                     
                     ;; Keyword access for maps
                     :else
                     (get current-data (keyword path-part))))
    data
    path))

;; =============================================================================
;; Block Data Resolution
;; =============================================================================

(defn get-block-data
  "Get data from a block by ID - looks for various data fields"
  [blocks block-id]
  ;; Get the base block data
  (let [block (or (get blocks block-id)
                  (get blocks (keyword block-id))
                  (get blocks (name block-id)))]
    ;; For query blocks, get results directly from reactive-queries store
    (if (and block (= (:type block) :query))
      (let [;; Get the actual results - the rq function handles ID normalization
            query-results (rq/get-block-results (:id block))
            ;; Merge the base block with query results
            merged (merge block query-results)]
        merged)
      block)))

(defn resolve-template-ref
  "Resolve a single template reference against current blocks"
  [blocks {:keys [block-id path] :as ref}]
  (let [block-data (get-block-data blocks block-id)
        data (rq/get-block-results block-id)
        block-id-kw (if (keyword? block-id) block-id (keyword (cstr/replace (str block-id) ":" ""))) ;; ensure kw 
        path2 (vec (for [p path]
                     (if (try (number? (edn/read-string p)) (catch :default _ false))
                       (edn/read-string p)
                       (keyword (cstr/replace (str p) ":" "")))))
        path2 (if (= (get path2 0) :*timestamp) [:*timestamp block-id-kw] path2)
        result (when data (if (= path2 [:*timestamp block-id-kw])
                            (get-in @rq/block-results path2)
                            (get-in data path2)))]
    (when (nil? result)
      (js/console.log "Template resolution failed:"
                      (clj->js {:ref ref
                                :block-found? (boolean block-data)
                                :block-keys (keys blocks)
                                :block-data block-data})))
    result))

(defn resolve-template
  "Resolve all template references in a string
   Returns the string with all {refs} replaced with actual values
   Special handling: if a URL parameter value is null/empty, remove the entire parameter"
  [template-str blocks]
  (if-not template-str
    ""
    (let [_ (js/console.log "[TEMPLATE] Input template:" template-str)
          refs (parse-template-refs template-str)
          ;; First pass - resolve all template values
          resolved (reduce
                     (fn [result-str {:keys [full-ref] :as ref}]
                       (let [value (resolve-template-ref blocks ref)
                             new-str (str/replace result-str full-ref (if (nil? value) "" (str value)))]
                         (js/console.log "[TEMPLATE] Replacing" full-ref "with" value "=>" new-str)
                         new-str))
                     template-str
                     refs)
          _ (js/console.log "[TEMPLATE] After replacements:" resolved)]
      ;; Second pass - clean up URL parameters
      (let [;; Match timestamps with or without milliseconds and Z
            with-at-fix (str/replace resolved 
                                     #"&(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z?)" 
                                     "&at=$1")]
        (when (not= resolved with-at-fix)
          (js/console.log "[TEMPLATE] Fixed missing at= parameter:" resolved "->" with-at-fix))
        (-> with-at-fix
          ;; Remove empty parameters that come after other parameters
          ;; Match &param= when followed by & or end of string
          (str/replace #"&[^=&?]+=(?=&|$)" "")
          ;; Remove empty parameters that are first (after ?)
          (str/replace #"\?[^=&?]+=(?=&|$)" "")
          ;; If first parameter was removed and there's another, fix the ?
          (str/replace #"^\?&" "?")
          ;; Clean up any double & that might result
          (str/replace #"&&+" "&")
          ;; Clean up trailing & or ?
          (str/replace #"[&?]$" ""))))))

;; =============================================================================
;; Dependency Tracking
;; =============================================================================

(defonce block-dependencies (reagent/atom {}))

(defn extract-dependencies
  "Extract all block IDs that a template depends on"
  [template-str]
  (let [refs (parse-template-refs template-str)]
    (set (map :block-id refs))))

(defn update-dependencies!
  "Update the dependency graph for a block"
  [block-id template-str]
  (let [deps (extract-dependencies template-str)]
    (swap! block-dependencies assoc block-id deps)))

(defn clear-dependencies!
  "Clear dependencies for a block"
  [block-id]
  (swap! block-dependencies dissoc block-id))

(defn get-dependent-blocks
  "Get all blocks that depend on a given block"
  [block-id]
  (set
    (for [[dependent-id deps] @block-dependencies
          :when (contains? deps block-id)]
      dependent-id)))

;; =============================================================================
;; Change Detection & Propagation
;; =============================================================================

(defn trigger-dependent-updates!
  "Trigger updates for all blocks that depend on the changed block"
  [changed-block-id blocks]
  (let [dependents (get-dependent-blocks changed-block-id)]
    (doseq [dependent-id dependents]
      ;; For iframe blocks, trigger a refresh by updating refresh-at timestamp
      (when-let [dependent-block (get blocks dependent-id)]
        (when (= (:type dependent-block) :iframe)
          ;; Trigger iframe refresh by updating a timestamp
          (reactor.core/dispatch! [:update-block dependent-id 
                                  {:refresh-at (.toISOString (js/Date.))
                                   :_refresh-reason (str "Query " changed-block-id " updated")}]))))))

;; =============================================================================
;; Visual Connection Helpers
;; =============================================================================

(defn get-implicit-connections
  "Get all implicit connections (template references) for visualization"
  []
  (vec
    (for [[target-id deps] @block-dependencies
          source-id deps]
      {:from source-id
       :to target-id
       :type :implicit  ;; Mark as implicit/template connection
       :style :dashed})))

;; =============================================================================
;; Integration Helpers
;; =============================================================================

(defn create-reactive-template
  "Create a reactive template that auto-updates when dependencies change
   Returns a reagent atom that updates when referenced blocks change"
  [template-str blocks-atom]
  (let [result (reagent/atom "")]
    ;; Initial resolution
    (reset! result (resolve-template template-str @blocks-atom))
    
    ;; Watch for changes
    (add-watch blocks-atom (keyword (str "template-" (random-uuid)))
      (fn [_ _ _ new-blocks]
        (reset! result (resolve-template template-str new-blocks))))
    
    result))

;; =============================================================================
;; Template Builder UI Helper
;; =============================================================================

(defn template-hint-text
  "Generate hint text for template syntax"
  []
  "Use {blockId.field} to reference other blocks. Special fields: *timestamp, *session_id")

(defn validate-template
  "Validate template syntax and return errors if any"
  [template-str blocks]
  (try
    (let [refs (parse-template-refs template-str)
          missing-blocks (filter 
                          (fn [{:keys [block-id]}]
                            (not (contains? blocks block-id)))
                          refs)]
      (when (seq missing-blocks)
        {:valid false
         :errors (map (fn [{:keys [block-id]}]
                       (str "Block not found: " block-id))
                     missing-blocks)}))
    (catch js/Error e
      {:valid false
       :errors [(str "Invalid template syntax: " (.-message e))]})))