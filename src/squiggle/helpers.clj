(ns squiggle.helpers
  (:require [clojure.java.jdbc :as j]
            [squiggle.core :as sq]))

(defn schema-columns [column-schema]
  "Given a column-schema, returns a list of columns in the schema."
  (map first column-schema))

(defn primary-key [schema table]
  (->> schema table :primary-key name (j/quoted \")))

(defn in-or-= [column id]
  "Given a column name and an id or a coll of ids returns an equal
   expression or a in expression."
  (if (coll? id)
    [:in column (vec id)]
    [:= column id]))

(defn create-table [{:keys [db db-spec schema]} table]
  "Given a sql-fn and a schema, creates the table described in the schema.
   The schema needs at least valid values for :table and :column-schema."
  (let [{:keys [option column-schema]} (table schema)]
    (sq/sql! db db-spec
             {:command :create-table
              :table table
              :column-schema column-schema
              :option (:create-table option)})))

(defn drop-table [{:keys [db db-spec schema]} table]
  "Given a sql-fn and a schema, drops the table in the schema
  The schema needs at least a valid :table value."
  (let [{:keys [option]} (table schema)]
    (sq/sql! db db-spec
             {:command :drop-table
              :table table
              :option (:drop-table option)})))

(defn create-index [sql-fn {:keys [table index]}]
  (sql-fn
   {:command :create-index
    :table table
    :index index}))

(defn drop-index [sql-fn {:keys [table index]}]
  (sql-fn
   {:command :drop-index
    :table table
    :index index}))

(defn delete! [{:keys [db-spec schema]} table id]
  "Given a sql-fn, a schema and an id, delete the record in the table
   with the primary key equal id. The schema needs at least valid values
   for :table and :primary-key"
  (let [column (primary-key schema table)]
    (j/delete! db-spec table
               [(str column " = ?") id]
               :entities (j/quoted \") )))

(defn find-ids [{:keys [db-spec schema db]} table id & {:keys [column]}]
  "Given a sql-fn, a schema and an id, returns a seq of results with
   primary keys equal the id. id may be a coll of ids or a single id.
   It accepts and optional column argument in the form
   \":column :name-of-column\". In this case, returns the results where
   the column is equal the id."
  (let [{:keys [primary-key order]} (table schema)]
    (sq/sql! db db-spec
             {:command :select
              :table table
              :where (in-or-= (or column primary-key) id)
              :order order})))

(defn find-all
  [{:keys [db-spec schema db]} table
   & {:keys [limit offset search search-column parent-map]}]
  (let [{:keys [order primary-key parent]} (table schema)
        where (when search
                [:like (or search-column primary-key)
                       search])
        where-parent (when-let [[parent-schema parent-id] (first parent-map)]
                       [:= (parent-schema parent) parent-id])
        where (if (and where where-parent)
                [:and where where-parent]
                (or where where-parent))
        command-map {:command :select
                     :table table
                     :where where
                     :order order
                     :limit limit
                     :offset offset}]
    (vary-meta (sq/sql! db db-spec command-map)
      merge {:command-map command-map})))

(defn add-count-records [{:keys [db-spec schema db]} results]
  (let [command-map (:command-map (meta results))]
    (vary-meta results
      merge {:records (-> command-map
                          (assoc
                            :order nil
                            :limit nil
                            :offset nil
                            :column [:count :*])
                          (#(sq/sql! db db-spec %))
                          ffirst
                          second)})))

(defn insert! [{:keys [db-spec schema]} table params]
  (j/insert! db-spec table params :entities (j/quoted \")))

(defn update! [{:keys [db-spec schema]} table id params]
 (let [column (primary-key schema table)]
    (j/update! db-spec table
           params
           [(str column " = ?") id]
           :entities (j/quoted \"))))

(defn put! [full-schema table id params]
  (if id
    (update! full-schema table id params)
    (insert! full-schema table params)))

(defn find-child
  "Returns a seq of vectors. Each vector has the form [:schema seq]."
  [{:keys [schema] :as full-schema} table s & {:keys [only]}]
  (when (and (seq s) (:child (table schema)))
    (let [{:keys [primary-key foreign-column child]} (table schema)
          ids (map primary-key s)]
      (for [c (or only child)]
        [c (find-ids full-schema c ids :column foreign-column)]))))

(defn add-children [{:keys [schema] :as full-schema} table s & {:keys [only]}]
  (if (and (seq s) (:child (table schema)))
    (let [{:keys [primary-key foreign-column child]} (table schema)
          ids (map primary-key s)
          children (if only
                     (find-child full-schema table s :only only)
                     (find-child full-schema table s))
          children (map (fn [[tbl chi]] chi) children)
          children (map #(group-by foreign-column %) children)
          ordered-children (for [p ids]
                             (map (fn [chi fk] (hash-map fk (get chi p)))
                                  children (or only child)))]
      (map #(apply merge %1 %2) s ordered-children))
    s))

(defn add-all-children [{:keys [schema] :as full-schema} table s]
  (if (and (seq s) (:child (table schema)))
    (let [{:keys [primary-key foreign-column child]} (table schema)
          ids (map primary-key s)
          children (find-child full-schema table s)
          children (map (fn [[tbl chi]]
                          (add-all-children full-schema tbl chi))
                        children)
          children (map #(group-by foreign-column %) children)
          ordered-children (for [p ids]
                             (map (fn [chi fk] (hash-map fk (get chi p)))
                                  children child))]
      (map #(apply merge %1 %2) s ordered-children))
    s))

(defn find-parent [{:keys [schema] :as full-schema} table s]
  (when (and (seq s) (:parent (table schema)))
    (let [{:keys [primary-key parent]} (table schema)]
      (for [[parent-schema column] parent
            :let [ids (distinct (map column s))]]
        [parent-schema
         (find-ids full-schema parent-schema ids)]))))

(defn add-parents [{:keys [schema] :as full-schema} table s & {:keys [only]}]
  (if (and (seq s) (:parent (table schema)))
    (let [{:keys [primary-key parent]} (table schema)
          parents (if only
                     (find-parent full-schema table s :only only)
                     (find-parent full-schema table s))
          parents (map (fn [[tbl chi]] (group-by (:primary-key (tbl schema)) chi)) parents)
          keys-fn (apply juxt (keys parent))
          ids (map keys-fn s)
          ordered-parents (for [p ids]
                            (map (fn [id chi [_ fk]] (hash-map fk (first (get chi id))))
                                  p parents (or only parent)))]
      (map #(apply merge %1 %2) s ordered-parents))
    s))

(defn add-all-parents [{:keys [schema] :as full-schema} table s]
  (if (and (seq s) (:parent (table schema)))
    (let [{:keys [primary-key parent]} (table schema)
          parents (find-parent full-schema table s)
          parents (map (fn [[tbl par]]
                         [tbl (add-all-parents full-schema tbl par)])
                        parents)
          parents (map (fn [[tbl par]] (group-by (:primary-key (tbl schema)) par)) parents)
          keys-fn (apply juxt (keys parent))
          ids (map keys-fn s)
          ordered-parents (for [p ids]
                             (map (fn [id par [_ fk]] (hash-map fk (get par id)))
                                  p parents parent))]
      (map #(apply merge %1 %2) s ordered-parents))
    s))
