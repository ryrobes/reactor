(ns examples.rabbit-demo.reactive-blocks
  "Reactive SQL blocks that auto-update via Kafka"
  (:require [reactor.core :as r]
            [reagent.core :as reagent]
            [clojure.string :as str]))

(defonce block-subscriptions (atom {}))
(defonce block-results (reagent/atom {}))

(defn subscribe-block-query!
  "Subscribe a block to a SQL query for automatic updates"
  [block-id sql]
  ;; Unsubscribe from previous query if exists
  (when-let [old-source (get @block-subscriptions block-id)]
    (.close old-source)
    (swap! block-subscriptions dissoc block-id))
  
  ;; Create new subscription
  (let [server-url (:server-url @r/config)
        session-id (:session-id @r/config)
        event-source (js/EventSource. 
                      (str server-url "/api/subscribe-sql?session=" session-id))]
    
    ;; Set up event handlers
    (set! (.-onopen event-source)
          (fn [e]
            (js/console.log "Block" block-id "subscription connected")))
    
    (set! (.-onmessage event-source)
          (fn [e]
            (let [data (js/JSON.parse (.-data e))
                  data-clj (js->clj data :keywordize-keys true)]
              (case (:type data-clj)
                :subscription-created
                (js/console.log "Block" block-id "subscription created:" (:subscription-id data-clj))
                
                :query-update
                (do
                  (js/console.log "Block" block-id "received update")
                  ;; Update block results
                  (swap! block-results assoc block-id (:result data-clj))
                  ;; Also update the block in app state
                  (r/dispatch! [:update-block block-id 
                               {:results (:results (:result data-clj))
                                :error (:error (:result data-clj))}]))
                
                (js/console.warn "Unknown message type:" (:type data-clj))))))
    
    (set! (.-onerror event-source)
          (fn [e]
            (js/console.error "Block" block-id "subscription error:" e)
            (r/dispatch! [:update-block block-id {:error "Connection error"}])))
    
    ;; Store event source
    (swap! block-subscriptions assoc block-id event-source)
    
    ;; Send subscription request with SQL
    (-> (js/fetch (str server-url "/api/subscribe-sql?session=" session-id)
                 #js {:method "POST"
                      :headers #js {"Content-Type" "application/json"}
                      :body (js/JSON.stringify 
                             (clj->js {:sql sql}))})
        (.then (fn [response]
                 (js/console.log "Block" block-id "subscription request sent")))
        (.catch (fn [error]
                  (js/console.error "Block" block-id "subscription request failed:" error))))))

(defn unsubscribe-block!
  "Unsubscribe a block from its query"
  [block-id]
  (when-let [event-source (get @block-subscriptions block-id)]
    (.close event-source)
    (swap! block-subscriptions dissoc block-id)
    (swap! block-results dissoc block-id)))

(defn execute-block-query!
  "Execute a SQL query for a block (one-time, not subscription)"
  [block-id sql & [as-of]]
  (r/dispatch! [:update-block block-id {:loading true}])
  (-> (r/sql-query! sql nil as-of)
      (.then (fn [result]
               (r/dispatch! [:update-block block-id 
                           {:results (:results result)
                            :error (:error result)
                            :loading false}])))
      (.catch (fn [error]
                (r/dispatch! [:update-block block-id 
                           {:error (str error)
                            :loading false}])))))

(defn reactive-query-block
  "Enhanced query block with reactive subscriptions"
  [{:keys [id position size sql] :as block}]
  (let [is-dragging (reagent/atom false)
        is-resizing (reagent/atom false)
        drag-offset (reagent/atom {:x 0 :y 0})
        initial-sql (reagent/atom sql)
        subscription-active (reagent/atom false)
        ;; Get reactive results from the separate atom
        block-result (rq/get-block-results id)
        {:keys [results error loading executed-sql]} block-result]
    
    ;; Set up subscription when SQL changes
    (reagent/create-class
     {:component-did-mount
      (fn []
        (when sql
          (subscribe-block-query! id sql)
          (reset! subscription-active true)))
      
      :component-will-unmount
      (fn []
        (unsubscribe-block! id))
      
      :component-did-update
      (fn [this [_ old-props]]
        (let [new-props (second (reagent/argv this))]
          ;; Re-subscribe if SQL changed
          (when (and (:sql new-props)
                    (not= (:sql old-props) (:sql new-props)))
            (subscribe-block-query! (:id new-props) (:sql new-props))
            (reset! subscription-active true))))
      
      :reagent-render
      (fn [{:keys [id position size sql results as-of error loading] :as block}]
        [:div.block.query-block
         {:style {:left (:x position)
                 :top (:y position)
                 :width (:width size)
                 :height (:height size)}
          :class (when @is-dragging "dragging")}
         
         ;; Header with subscription indicator
         [:div.block-header
          [:div.block-title 
           "SQL Query"
           (when @subscription-active
             [:span.subscription-indicator 
              {:style {:margin-left "10px"
                      :color "#4CAF50"
                      :font-size "12px"}}
              "● LIVE"])]
          [:button.close-btn
           {:on-click #(r/dispatch! [:delete-block id])}
           "×"]]
         
         ;; SQL Editor
         [:div.sql-editor
          [:textarea
           {:value (or executed-sql sql "")
            :placeholder "Enter SQL query..."
            :style (when executed-sql
                     {:background "rgba(0,255,159,0.05)"
                      :border-color "#00ff9f"})
            :on-change (fn [e]
                        (let [new-sql (.. e -target -value)]
                          (r/dispatch! [:update-block id {:sql new-sql}])))}]]
         
         ;; Execute button
         [:button.execute-btn
          {:on-click #(subscribe-block-query! id sql)}
          "↻ Re-subscribe"]
         
         ;; Show time travel indicator when active
         (when (and executed-sql (not= executed-sql sql))
           [:div.time-travel-indicator {:style {:background "rgba(0,255,159,0.1)"
                                                :border "1px solid rgba(0,255,159,0.3)"
                                                :padding "4px 8px"
                                                :margin "5px 0"
                                                :font-size "10px"
                                                :font-family "monospace"
                                                :color "#00ff9f"
                                                :display "flex"
                                                :align-items "center"
                                                :gap "5px"}}
            [:span "⏰"]
            [:span "TIME TRAVEL MODE - Query is showing historical data"]])
         
         ;; Results
         [:div.results
          (cond
            loading [:div.loading "Executing..."]
            error [:div.error error]
            results [:div.table-container
                    [:table
                     [:thead
                      [:tr
                       (for [col (keys (first results))]
                         ^{:key col} [:th (name col)])]]
                     [:tbody
                      (for [row results]
                        ^{:key (hash row)}
                        [:tr
                         (for [[k v] row]
                           ^{:key k} [:td (str v)])])]]]
            :else [:div.no-results "No results"])]])})))

(defn handle-sql-exec!
  "Handle SQL execution (INSERT/UPDATE/DELETE) and notify about table changes"
  [block-id sql]
  (r/dispatch! [:update-block block-id {:loading true}])
  (-> (r/sql-exec! sql)
      (.then (fn [result]
               (r/dispatch! [:update-block block-id 
                           {:result (:result result)
                            :error (:error result)
                            :loading false}])
               ;; After successful execution, the Kafka system should
               ;; automatically trigger updates to affected query blocks
               (when-not (:error result)
                 (js/console.log "SQL executed, reactive updates should trigger"))))
      (.catch (fn [error]
                (r/dispatch! [:update-block block-id 
                           {:error (str error)
                            :loading false}])))))

(defn reactive-sql-exec-block
  "SQL execution block that triggers reactive updates"
  [{:keys [id position size sql error result loading] :as block}]
  [:div.block.sql-exec-block
   {:style {:left (:x position)
           :top (:y position)
           :width (:width size)
           :height (:height size)}}
   
   [:div.block-header
    [:div.block-title "SQL Execute"]
    [:button.close-btn
     {:on-click #(r/dispatch! [:delete-block id])}
     "×"]]
   
   [:div.sql-editor
    [:textarea
     {:value (or sql "")
      :placeholder "Enter SQL statement (INSERT/UPDATE/DELETE)..."
      :on-change (fn [e]
                  (let [new-sql (.. e -target -value)]
                    (r/dispatch! [:update-block id {:sql new-sql}])))}]]
   
   [:button.execute-btn
    {:on-click #(handle-sql-exec! id sql)
     :disabled loading}
    (if loading "Executing..." "Execute")]
   
   [:div.results
    (cond
      error [:div.error error]
      result [:div.success result]
      :else [:div.info "Ready to execute"])]])