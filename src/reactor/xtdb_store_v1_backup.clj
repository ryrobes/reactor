(ns reactor.xtdb-store
  "XTDB-backed storage for Reactor with atom-like interface"
  (:require [xtdb.api :as xt]
            [clojure.java.io :as io])
  (:import [java.time Duration]))

(defn start-xtdb-node
  "Start an in-memory XTDB node with SQL support and optional file persistence"
  ([]
   (start-xtdb-node nil))
  ([data-dir]
   (start-xtdb-node data-dir 1501))
  ([data-dir sql-port]
   (let [base-config (if data-dir
                      ;; With persistence using RocksDB
                      {:xtdb/tx-log {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                                                :db-dir (io/file data-dir "tx-log")
                                                :sync? true}}
                       :xtdb/document-store {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                                                        :db-dir (io/file data-dir "doc-store")}}
                       :xtdb/index-store {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                                                     :db-dir (io/file data-dir "index-store")}}}
                      ;; Pure in-memory
                      {})
         ;; Add SQL server configuration
         sql-config (if sql-port
                     (assoc base-config :xtdb.calcite/server {:port sql-port})
                     base-config)]
     (xt/start-node sql-config))))

(defn stop-xtdb-node [node]
  (.close node))

;; Helper functions for working with XTDB

(defn put-entity
  "Store an entity in XTDB"
  [node entity-id data]
  (xt/submit-tx node [[::xt/put (assoc data :xt/id entity-id)]]))

(defn get-entity
  "Retrieve an entity from XTDB"
  ([node entity-id]
   (get-entity node entity-id nil))
  ([node entity-id as-of]
   (let [db (if as-of
              (xt/db node as-of)
              (xt/db node))]
     (xt/entity db entity-id))))

(defn delete-entity
  "Delete an entity from XTDB"
  [node entity-id]
  (xt/submit-tx node [[::xt/delete entity-id]]))

(defn entity-history
  "Get the full history of an entity"
  [node entity-id]
  (xt/entity-history (xt/db node) entity-id :asc))

;; Session management helpers

(defn session-key
  "Create a namespaced key for session-specific data"
  [session-id & path]
  (keyword (str "session." session-id) 
           (clojure.string/join "." (map name path))))

(defn global-key
  "Create a namespaced key for global data"
  [& path]
  (keyword "global" (clojure.string/join "." (map name path))))

;; Atom-like wrapper for XTDB

(defprotocol IXTDBAtom
  (entity-id [this])
  (session-id [this])
  (sync! [this] "Wait for all transactions to be indexed")
  (history [this] "Get full history of this atom's entity"))

(deftype XTDBAtom [node entity-id* session-id* cache watchers]
  clojure.lang.IDeref
  (deref [this]
    (or @cache
        (let [entity (get-entity node @entity-id*)]
          (reset! cache (dissoc entity :xt/id))
          @cache)))
  
  clojure.lang.IAtom
  (swap [this f]
    (let [old-val @this
          new-val (f old-val)
          tx (put-entity node @entity-id* (assoc new-val :xt/id @entity-id*))]
      (xt/await-tx node tx (Duration/ofSeconds 5))
      (reset! cache new-val)
      ;; Notify watchers
      (doseq [[key watcher-fn] @watchers]
        (watcher-fn key this old-val new-val))
      new-val))
  (swap [this f arg]
    (let [old-val @this
          new-val (f old-val arg)
          tx (put-entity node @entity-id* (assoc new-val :xt/id @entity-id*))]
      (xt/await-tx node tx (Duration/ofSeconds 5))
      (reset! cache new-val)
      ;; Notify watchers
      (doseq [[key watcher-fn] @watchers]
        (watcher-fn key this old-val new-val))
      new-val))
  (swap [this f arg1 arg2]
    (let [old-val @this
          new-val (f old-val arg1 arg2)
          tx (put-entity node @entity-id* (assoc new-val :xt/id @entity-id*))]
      (xt/await-tx node tx (Duration/ofSeconds 5))
      (reset! cache new-val)
      ;; Notify watchers
      (doseq [[key watcher-fn] @watchers]
        (watcher-fn key this old-val new-val))
      new-val))
  (swap [this f arg1 arg2 args]
    (let [old-val @this
          new-val (apply f old-val arg1 arg2 args)
          tx (put-entity node @entity-id* (assoc new-val :xt/id @entity-id*))]
      (xt/await-tx node tx (Duration/ofSeconds 5))
      (reset! cache new-val)
      ;; Notify watchers
      (doseq [[key watcher-fn] @watchers]
        (watcher-fn key this old-val new-val))
      new-val))
  
  (reset [this new-val]
    (let [old-val @this
          tx (put-entity node @entity-id* (assoc new-val :xt/id @entity-id*))]
      (xt/await-tx node tx (Duration/ofSeconds 5))
      (reset! cache new-val)
      ;; Notify watchers
      (doseq [[key watcher-fn] @watchers]
        (watcher-fn key this old-val new-val))
      new-val))
  
  clojure.lang.IRef
  (addWatch [this key callback]
    (swap! watchers assoc key callback)
    this)
  
  (removeWatch [this key]
    (swap! watchers dissoc key)
    this)
  
  IXTDBAtom
  (entity-id [_] @entity-id*)
  (session-id [_] @session-id*)
  (sync! [_] 
    (xt/sync node (Duration/ofSeconds 10)))
  (history [_]
    (entity-history node @entity-id*)))

(defn xtdb-atom
  "Create an atom-like wrapper around XTDB storage"
  ([node entity-key]
   (xtdb-atom node entity-key nil nil))
  ([node entity-key initial-value]
   (xtdb-atom node entity-key initial-value nil))
  ([node entity-key initial-value session-id]
   (let [entity-id (if session-id
                     (session-key session-id entity-key)
                     (global-key entity-key))
         existing (get-entity node entity-id)]
     ;; Initialize if needed
     (when (and initial-value (not existing))
       (let [tx (put-entity node entity-id (assoc initial-value :xt/id entity-id))]
         (xt/await-tx node tx (Duration/ofSeconds 5))))
     (->XTDBAtom node (atom entity-id) (atom session-id) 
                 (atom (dissoc (or existing initial-value) :xt/id))
                 (atom {})))))