(ns squiggle.helpers)

(defn column [column-schema]
  "Given a column-schema, returns a list of columns in the schema."
  (map first column-schema))

(defn in-or-= [column id]
  "Given a column name and an id or a coll of ids returns an equal
   expression or a in expression."
  (if (coll? id)
    [:in column (vec id)]
    [:= column id]))

(defn create-table [sql-fn {:keys [table option column-schema]}]
  "Given a sql-fn and a schema, creates the table described in the schema.
   The schema needs at least valid values for :table and :column-schema."
  (sql-fn
   {:command :create-table
    :table table
    :column-schema column-schema
    :option (:create-table option)}))

(defn drop-table [sql-fn {:keys [table option]}]
  "Given a sql-fn and a schema, drops the table in the schema
  The schema needs at least a valid :table value."
  (sql-fn
   {:command :drop-table
    :table table
    :option (:drop-table option)}))

(defn delete [sql-fn {:keys [table primary-key]} id]
  "Given a sql-fn, a schema and an id, delete the record in the table
   with the primary key equal id. The schema needs at least valid values
   for :table and :primary-key"
  (sql-fn
   {:command :delete
    :table table
    :where (in-or-= primary-key id)}))

(defn find-ids [sql-fn {:keys [table primary-key order]} id & {:keys [column]}]
  "Given a sql-fn, a schema and an id, returns a seq of results with
   primary keys equal the id. id may be a coll of ids or a single id.
   It accepts and optional column argument in the form
   \":column :name-of-column\". In this case, returns the results where
   the column is equal the id."
  (sql-fn
   {:command :select
    :table table
    :where (in-or-= (or column primary-key) id)
    :order order}))

(defn find-all [sql-fn {:keys [table order]} & {:keys [limit offset]}]
  "Given a sql-fn and a schema returns all the results in the table."
  (let [command-map {:command :select
                     :table table
                     :order order
                     :limit limit
                     :offset offset}]
    {:command-map command-map
     :results (sql-fn command-map)}))

(defn find-like
  [sql-fn {:keys [table order primary-key]} term & {:keys [limit offset column]}]
  (let [command-map {:command :select
                     :table table
                     :where [:like (or column primary-key) term]
                     :order order
                     :limit limit
                     :offset offset}]
    {:command-map command-map
     :results (sql-fn command-map)}))

(defn add-count [sql-fn {:keys [command-map] :as results}]
  (assoc results
    :count (second (ffirst (sql-fn
                            (assoc command-map
                              :order nil
                              :limit nil
                              :offset nil
                              :column [:count :*]))))))

(defn insert [{:keys [column-schema table]} params]
  (let [c (column column-schema)
        value (map params c)]
    {:command :insert
     :table table
     :column c
     :value [value]}))

(defn insert! [sql-fn schema params]
  (sql-fn (insert schema params)))

(defn update [{:keys [column-schema pk table]} id params]
  (let [c (column column-schema)
        value (map params c)
        s (partition 2 (interleave c value))
        s (remove #(nil? (second %)) s)]
    {:command :update
     :table table
     :where [:= pk id]
     :set (vec (map (fn [x] [:= (first x) (second x)]) s))}))

(defn update! [sql-fn schema id params]
  (sql-fn (update schema id params)))

(defn put! [sql-fn schema id params]
  (if id
    (update! sql-fn schema id params)
    (insert! sql-fn schema params)))

(defn find-child
  "Returns a seq of vectors. Each vector has the form [:schema seq]."
  [sql-fn full-schema schema s & {:keys [only]}]
  (when (and (seq s) (:child (schema full-schema)))
    (let [{:keys [primary-key foreign-column child]} (schema full-schema)
          ids (map primary-key s)]
      (for [c (or only child)]
        [c (find-ids sql-fn (full-schema c) ids :column foreign-column)]))))

(defn add-children [sql-fn full-schema schema s & {:keys [only]}]
  (if (and (seq s) (:child (schema full-schema)))
    (let [{:keys [primary-key foreign-column child]} (schema full-schema)
          ids (map primary-key s)
          children (if only
                     (find-child sql-fn full-schema schema s :only only)
                     (find-child sql-fn full-schema schema s))
          children (map (fn [[sch chi]] chi) children)
          children (map #(group-by foreign-column %) children)
          ordered-children (for [p ids]
                             (map (fn [chi fk] (hash-map fk (get chi p)))
                                  children (or only child)))
          _ (println ordered-children)]
      (map #(apply merge %1 %2) s ordered-children))
    s))

(defn add-all-children [sql-fn full-schema schema s]
  (if (and (seq s) (:child (schema full-schema)))
    (let [{:keys [primary-key foreign-column child]} (schema full-schema)
          ids (map primary-key s)
          children (find-child sql-fn full-schema schema s)
          children (map (fn [[sch chi]]
                          (add-all-children sql-fn full-schema sch chi))
                        children)
          children (map #(group-by foreign-column %) children)
          ordered-children (for [p ids]
                             (map (fn [chi fk] (hash-map fk (get chi p)))
                                  children child))]
      (map #(apply merge %1 %2) s ordered-children))
    s))

(defn find-parent [sql-fn full-schema schema s]
  (when (and (seq s) (:parent (schema full-schema)))
    (let [{:keys [primary-key parent]} (schema full-schema)]
      (for [[parent-schema column] parent
            :let [ids (map column s)]]
        [parent-schema
         (find-ids sql-fn (full-schema parent-schema) ids)]))))

(defn add-parents [sql-fn full-schema schema s & {:keys [only]}]
  (if (and (seq s) (:parent (schema full-schema)))
    (let [{:keys [primary-key parent]} (schema full-schema)
          parents (if only
                     (find-parent sql-fn full-schema schema s :only only)
                     (find-parent sql-fn full-schema schema s))
          parents (map (fn [[sch chi]] (group-by (:primary-key (sch full-schema)) chi)) parents)
          keys-fn (apply juxt (keys parent))
          ids (map keys-fn s)
          ordered-parents (for [p ids]
                             (map (fn [id chi [_ fk]] (hash-map fk (get chi id)))
                                  p parents (or only parent)))
          ]
      (map #(apply merge %1 %2) s ordered-parents))
    s))

(defn add-all-parents [sql-fn full-schema schema s]
  (if (and (seq s) (:parent (schema full-schema)))
    (let [{:keys [primary-key parent]} (schema full-schema)
          parents (find-parent sql-fn full-schema schema s)
          parents (map (fn [[sch par]]
                         [sch (add-all-parents sql-fn full-schema sch par)])
                        parents)
          parents (map (fn [[sch par]] (group-by (:primary-key (sch full-schema)) par)) parents)
          keys-fn (apply juxt (keys parent))
          ids (map keys-fn s)
          ordered-parents (for [p ids]
                             (map (fn [id par [_ fk]] (hash-map fk (get par id)))
                                  p parents parent))
          ]
      (map #(apply merge %1 %2) s ordered-parents))
    s))
