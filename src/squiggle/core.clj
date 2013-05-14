(ns squiggle.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.walk :as walk]
            [clojure.string :as str]))

(defn- not-unitary?
  [c]
  (not= 1 (count c)))

(defn- select-string?
  [f]
  (when (and (string? f)
             (= "select" (str/lower-case (subs f 0 (min (count f) 6)))))
    true))

(defn- select-vector?
  [f]
  (when (and (vector? f) (select-string? (first f)))
    true))

(defn- identifier->str
  "Given a db and a identifier converts the identifier to a string."
  [db t]
  (if (= (name t) "*")
    "*"
    (case db
      :mysql (str "`" (name t) "`")
      :mssql (str "[" (name t) "]")
      (str "\"" (name t) "\""))))

(defn- table-string
  "Given a db and a table or a vector of tables, returns a string for
   a table or list of tables."
  [db t]
  (cond
   (string? t) t
   (keyword? t)
   (let [[tn dbn] (reverse (str/split (name t) #"\."))
         dbn (if dbn (str (identifier->str db dbn) "."))]
     (str dbn (identifier->str db tn)))

   (map? t)
   (if (not-unitary? t)
     (throw (IllegalArgumentException.
             "A table map can only have one key value pair."))
     (let [[tname talias] (first t)]
       (str (table-string db tname) " AS " (table-string db talias))))

   (select-vector? t)
   (str "(" (first t) ")")

   (vector? t)
   (str/join ", " (map (partial table-string db) t))

   :else
   (throw (IllegalArgumentException.
           "Invalid value for :table key"))))

(defn- option->set
  [option]
  (cond (keyword? option) #{option}
        :else (set option)))

(defn- sql-drop
  "Given a db and a command map generates a drop table sql code."
  [db {:keys [table option]}]
  (let [option (option->set option)]
    (if (and (:cascade option)
             (:restrict option))
      (throw (IllegalArgumentException.
              "Can't use both :cascade and :restrict at the same time."))
      [(str "DROP TABLE "
            (if (:if-exists option)
              "IF EXISTS ")
            (table-string db table)
            (if (:cascade option)
              " CASCADE")
            (if (:restrict option)
              " RESTRICT"))])))

(def ^{:private true
       :doc "Map of options for create-table used to convert keywords to
       strings."}
  create-table-options
  {:primary-key "PRIMARY KEY"
   :unique      "UNIQUE"
   :null        "NULL"
   :not-null    "NOT NULL"})

(defn- convert-options
  "Convert the options of each column for the create table command."
  [opt]
  (cond
    (string? opt)  opt
    (keyword? opt) (opt create-table-options)
    (vector? opt)  (str/join " " (map convert-options opt))
    :else (throw (IllegalArgumentException. "Incorrect option format."))))

(defn- ct-columns
  "Given a vector of column vectors returns a string to create this columns in
  a create table command."
  [db c]
  (str/join ", "
   (for [[cname type options] c]
     (str/join
     [(if (string? cname) cname (identifier->str db cname)) " "
      (str (name type))
      (when  options (str " " (convert-options options)))]))))

(defn- pre-table-options
  [db option]
  (cond
    (and (:memory option) (:cached option))
    (throw (IllegalArgumentException.
      (str "Incompatible options: :memory :cached."
           "Chose only one.")))
    (> (count (option #{:temp :temporary:global-temporary
                         :local-temporary})) 1)
    (throw (IllegalArgumentException.
      (str "Incompatible options: :temp :temporary :global-temporary "
           ":local-temporary. Chose only one.")))

    :else
    (str
      (when (:memory option) "MEMORY ")
      (when (:cached option) "CACHED ")
      (when (:temporary option) "TEMPORARY ")
      (when (:global-temporary option) "GLOBAL TEMPORARY ")
      (when (:local-temporary option) "LOCAL TEMPORARY ")
      (when (:temp option) "TEMP "))))

(defn- post-table-options
  [db option]
  (if (:if-not-exists option) "IF NOT EXISTS "))

(defn- sql-create
  "Given a db and a command map generates a create table sql code."
  [db {:keys [table column option]}]
  (let [option (option->set option)]
    [(str "CREATE "
          (pre-table-options db option)
          "TABLE "
          (post-table-options db option)
          (table-string db table)
          " (" (ct-columns db column) ")")]))

(defn- sql-insert
  "Given a db and a command map return the insert sql code."
  [db {:keys [table column value select]}]
  (let [column (if (and column (not (coll? column))) [column] column)
        c (count column)
        v (count value)]
    (cond
      (and value select)
      (throw (IllegalArgumentException.
               (str "Can't insert select and value at the same time "
                    "with squiggle. Not sure if it's possible at all.")))

      select
      (into [(str "INSERT INTO " (identifier->str db table)
                  (str " " (first select)))] (rest select))

      value
      (cond
        (= :not-sure db)
        (into
          [(str "INSERT INTO " (identifier->str db table) " ("
                (str/join ", " (map (partial identifier->str db) column))
                ") VALUES ("
                (str/join ", " (repeat c "?")) ")")]
          value)

       :else
       (into
        [(str "INSERT INTO " (identifier->str db table) " ("
              (str/join ", " (map (partial identifier->str db)
                                  column))
              ") VALUES "
              (str/join ", "
                        (repeat v (str "(" (str/join ", "
                                                     (repeat c "?")) ")"))))]
        (flatten value))))))

(def ^{:private true
       :doc "Map of operators used to convert the operator keyword to a
       string."}
  operators->str
  {:=        "="
   :>        ">"
   :>=       ">="
   :<        "<"
   :<=       "<="
   :!=       "<>"
   :<>       "<>"
   :not=     "<>"
   :like     "LIKE"
   :in       "IN"
   :not-in   "NOT IN"
   :between  "BETWEEN"
   :not      "NOT"
   :is-null  "IS NULL"
   :not-null "NULL"
   :and      "AND"
   :or       "OR"})

(def ^{:private true
       :doc "Map of aggregators used to convert the aggregator keyword to
       a string"}
  aggregators->str
  {:count "COUNT"
   :sum   "SUM"
   :avg   "AVG"
   :stdev "STDEV"
   :min   "MIN"
   :max   "MAX"
   :first "FIRST"
   :last  "LAST"})

(def ^{:private true
       :doc "Set of infix operators used verify if a keyword is a infix
       operator."}
  infix-operators
  #{:> :< := :>= :<= :!= :<> :like :not= :in :not-in :between :is-null
    :not-null})

#_(defn- table-alias
  "Given a table return a map of table as key and alias as val.
  Given a vector of tables, return a seq of maps."
  [t]
  (cond
   (map? t)
   (if (not= 1 (count t))
     (throw (IllegalArgumentException.
             "A table map can only have one key value pair."))
     t)

   (vector? t)
   (reduce merge (map table-alias t))

   (or (keyword? t) (string? t))
   {t nil}

   :else
   (throw (IllegalArgumentException.
           "Invalid value for :table key"))))

(defn- process-operators
  "Given a where or having expression returns a pre-processed expression in
  the correct order for string generation."
  [f]
  (if (vector? f)
    (let [[o & r] f]
       (if (operators->str o)
         (cond
          (infix-operators o) (let [[le re] r]
                         (list (process-operators le) o (process-operators re)))
          (= :and o) (if (= 1 (count r))
                         (process-operators (first r))
                         (interpose o (map process-operators r)))
          (= :or o) (if (= 1 (count r))
                         (process-operators (first r))
                         (vec (interpose o (map process-operators r)))))
         (vec (map process-operators f))))
    f))

(defn- escaped-string?
  [f]
  (when (and (string? f) (= (first f) \!))
    true))

(defn- sanitize*
  "Given a form, returns a string \"?\" if the form isn't a collection, a
  keyword or a escaped string."
  [f]
  (if (or (coll? f) (keyword? f) (escaped-string? f) (select-string? f))
    f
    "?"))

(defn- sanitize
  "Given an expression, returns a sanitized expression with \"?\" replacing
  arguments."
  [ex]
  (walk/postwalk sanitize* ex))

(defn- arguments
  "Given an expression returns a seq of all arguments"
  [ex]
  (seq (remove #(when (or (escaped-string? %) (select-string? %)) true)
               (remove keyword? (flatten ex)))))

(defn- fix-subselects*
  [f]
  (if (select-vector? f)
    (str "(" (first f) ")")
    f))

(defn- fix-subselects
  [ex]
  (walk/postwalk fix-subselects* ex))

(defn- add-operators
  "Given an expression returns a expression with operator strings replacing
  operator keywords."
  [ex]
  (walk/postwalk-replace operators->str ex))

(defn- aggregator-vector?
  [f]
  (when (and (vector? f) (= 2 (count f)) (aggregators->str (first f)))
    true))

(defn- add-columns*
  "Given a db and a column returns a string for that column."
  [db f]
  (cond
   (keyword? f)
   (let [[cn & r] (reverse (str/split (name f) #"\."))
         r (if r (str (str/join "." (map (partial identifier->str db)
                                         (reverse r)))
                      "."))]
     (str r (identifier->str db cn)))

   (aggregator-vector? f)
   (str (aggregators->str (first f)) "(" (add-columns* db (second f)) ")")

   :else f))

(defn- add-columns
  "Given an alias map and an expression returns a expression with column
  strings replacing column keywords."
  [db ex]
  (walk/prewalk (partial add-columns* db) ex))

(defn- remove-literal-mark*
  "Given a string, if the string begins with \"!\" returns the string
   without \"!\"."
  [f]
  (if (escaped-string? f)
    (str/replace-first f #"^!" "")
    f))

(defn- remove-literal-mark
  "Given an expression remove the literal marks of all strings."
  [ex]
  (walk/postwalk remove-literal-mark* ex))

(defn- column-string
  "Given a db and a column, or vector of columns, return a string to
   be used in the select statement."
  [db c]
  (cond
   (nil? c) "*"
   (string? c) c
   (keyword? c) (add-columns* db c)

   (aggregator-vector? c)
   (add-columns* db c)

   (select-vector? c)
   (str "(" (first c) ")")

   (map? c)
   (if (not-unitary? c)
     (throw (IllegalArgumentException.
             "A column map can only have one key value pair."))
     (let [[cname calias] (first c)]
       (str (column-string db cname) " AS " (add-columns* db calias))))

   (vector? c)
   (str/join ", " (map (partial column-string db) c))

   :else
   (throw (IllegalArgumentException.
           "Invalid value for :column key"))))

(defn- fix-in-vector*
  "Given a list, if it is a \"IN\" list return the last list as a comma
   interposed vector."
  [f]
  (if (coll? f)
    (if (and (= (second f) "IN") (not (or (map? (last f)) (string? (last f)))))
      (list (first f) (second f) (vec (interpose ", " (last f))))
      f)
    f))

(defn- fix-in-vector
  "Given an expression fix all \"IN\" lists in this expression."
  [ex]
  (walk/postwalk fix-in-vector* ex))

(defn- parentesis*
  "Given a vector, returns a string of the vector contents enclosed in
   parentesis."
  [f]
  (if (vector? f)
    (if (every? string? f)
      (str "(" (str/join f) ")")
      (str "(" (str/join " " (flatten f)) ")"))
    f))

(defn- parentesis
  "Given an expression, returns a new expression with strings enclosed in
   parentesis replacing vectors."
  [ex]
  (walk/postwalk parentesis* ex))

(defn- add-space
  "Given an expression adds spaces between forms in the expression."
  [ex]
  (interpose " " ex))

(defn- add-expression
  "Given a expression, the expression type and the db returns the
  string for the expression."
  [e ty db]
  (let [exp (some->> e
                     process-operators
                     sanitize
                     fix-subselects
                     add-operators
                     remove-literal-mark
                     fix-in-vector
                     (add-columns db)
                     parentesis
                     flatten
                     add-space)]
    (cond
     (nil? exp) nil
     (= ty :where) (apply str " WHERE " exp)
     (= ty :having) (apply str " HAVING " exp)
     (= ty :on) (apply str " ON " exp)
     :else (throw (IllegalArgumentException.
                   "Only accepts type :where, :having or :on.")))))

(defn- set-expression
  [db e]
  (let [exp (some->> e
                     process-operators
                     sanitize
                     fix-subselects
                     add-operators
                     remove-literal-mark
                     (add-columns db))
        exp (if (coll? (first exp))
              (map (partial str/join " ") exp)
              (str/join " " exp))]
    (str " SET " (if (coll? exp) (str/join ", " exp) exp))))

(def ^{:private true
       :doc "Map of aggregators used to convert the aggregator keyword to
       a string"}
  modifiers->str
  {:distinct    "DISTINCT"
   :distinct-on "DISTINCT ON"
   :top         "TOP"})

(defn- add-modifier
  "Given a db and a list of modifiers returns the string of modifiers
   for a select query."
  [db m]
  (if (nil? m)
    m
    (cond
      (map? m)
      (if (not-unitary? m)
        (throw (IllegalArgumentException.
                "A identifier map can only have one key value pair."))
        (let [[mname margs] (first m)
              margs (if (coll? margs)
                      (str "("
                           (str/join ", " (map (partial add-columns* db) margs))
                           ")")
                      margs)]
          (str (or (modifiers->str mname) (name mname)) " " margs)))

      (vector? m)
      (str/join " " (map (partial add-modifier db) m))

      :else (or (modifiers->str m) (name m)))))

(defn- by-columns
  "Given a db and a list of columns returns a order/group by string."
  [db b]
  (if (nil? b)
    b
    (if (vector? b)
      (str/join ", " (map (partial add-columns* db) b))
      (add-columns* db b))))

(defn- add-group-by
  "Given a db and a list of columns returns a group by string."
  [db gb]
  (if-let [gb (by-columns db gb)]
    (str " GROUP BY " gb)))

(defn- add-order-by
  "Given a db and a list of columns returns a order by string."
  [db ob]
  (if-let [ob (by-columns db ob)]
    (str " ORDER BY " ob)))

(defn- add-limit
  "Given a db and a integer returns a limit string."
  [db i]
  (if i
    (str " LIMIT " i)))

(defn- add-offset
  "Given a db and a integer returns a offset string."
  [db i]
  (if i
    (str " OFFSET " i)))

(def
  join->str
  {:left "LEFT JOIN"
   :right "RIGHT JOIN"
   :inner "INNER JOIN"
   :cross "CROSS JOIN"
   :full  "FULL JOIN"})

(defn- single-join
  "Given a db and a join map returns a join string"
  [db {:keys [type table on]}]
  (str (or (join->str type) (name type)) " "
       (table-string db table)
       (add-expression on :on db)))

(defn- add-join
  "Given a db and the contents of the join keyword returns a join string"
  [db j]
  (cond
   (nil? j) j
   (vector? j) (str/join " " (map (partial single-join db) j))
   (map? j) (single-join db j)))

(defn- join-args
  "Given a join expression returns a seq of all arguments"
  [j]
  (cond
   (nil? j) j
   (vector? j) (arguments (concat (map :on j)))
   :else (arguments (:on j))))

(declare sql-select)

(defn- process-subselects*
  [db f]
  (if (and (map? f) (= :select (:command f)))
    (sql-select db f)
    f))

(defn- process-subselects
  [db cm]
  (walk/prewalk (partial process-subselects* db) cm))

(defn- column-args
  [c]
  (cond
   (nil? c) nil
   (string? c) nil
   (keyword? c) nil

   (map? c)
   (if (not-unitary? c)
     (throw (IllegalArgumentException.
             "A column map can only have one key value pair."))
     (let [[cname calias] (first c)]
       (column-args cname)))

   (aggregator-vector? c)
   nil

   (select-vector? c)
   (rest c)

   (vector? c)
   (mapcat column-args c)

   :else
   (throw (IllegalArgumentException.
           "Invalid value for :column key"))))

(defn- table-args
  "Given a table or a vector of tables, returns the args inside subselects."
  [t]
  (cond
   (string? t) nil
   (keyword? t) nil

   (map? t)
   (if (not-unitary? t)
     (throw (IllegalArgumentException.
             "A table map can only have one key value pair."))
     (let [[tname talias] (first t)]
       (table-args tname)))

   (select-vector? t)
   (rest t)

   (vector? t)
   (mapcat table-args t)

   :else
   (throw (IllegalArgumentException.
           "Invalid value for :table key"))))

(defn- sql-select
  "Given a database and a select command map, returns a select query
  vector."
  [db cm]
  (let [q (str "SELECT "
               (if (:modifier cm) (str (add-modifier db (:modifier cm)) " "))
               (column-string db (:column cm))
               " FROM "
               (table-string db (:table cm))
               (add-join db (:join cm))
               (add-expression (:where cm) :where db)
               (add-group-by db (:group cm))
               (add-expression (:having cm) :having db)
               (add-order-by db (:order cm))
               (add-limit db (:limit cm))
               (add-offset db (:offset cm)))
        a (concat (column-args (:column cm)) (table-args (:table cm))
                  (join-args (:join cm)) (arguments (:where cm))
                  (arguments (:having cm)))]
    (into [q] a)))

(defn- sql-delete
  "Given a database and a delete command map, returns a delete
  query vector."
  [db {:keys [table where limit]}]
  (into [(str "DELETE FROM " (identifier->str db table)
              (add-expression where :where db)
              (add-limit db limit))]
        (arguments where)))

(defn- sql-update
  "Given a database and a update command map, returns a update
  query vector."
  [db {:keys [table where set]}]
  (into [(str "UPDATE " (table-string db table)
              (set-expression db set)
              (add-expression where :where db))]
        (concat (arguments set) (arguments where))))

(defn- sql-create-index
  "Given a database and a create-index command map returns a create
  index query vector."
  [db {:keys [table column option name]}]
  (let [option (set option)]
    [(str "CREATE "
         (when (:unique option) "UNIQUE ")
         (when (:hash option) "HASH ")
         "INDEX "
         (when (:if-not-exists option) "IF NOT EXISTS ")
         (when name (str (identifier->str name) " "))
         "ON "
         (table-string db table)
         " ("
         (column-string db column)
         ")")]))

(defn sql-gen
  "Given a database and a command map return the sql vector."
  [db cm]
  (let [command (:command cm)
        cm (process-subselects db (assoc cm :command nil))
        cm (assoc cm :command command)]
    (case (:command cm)
      :select (sql-select db cm)

      :insert (sql-insert db cm)

      :delete (sql-delete db cm)

      :update (sql-update db cm)

      :drop (sql-drop db cm)

      :create (sql-create db cm)

      :create-index (sql-create-index db cm)

      (throw (IllegalArgumentException.
              "Incorrect :command value format.")))))

(defn sql-exec!
  "Given a database, a connection map and a command map, execute the
  command."
  [db c cm]
  (case (:command cm)
    :select
    (jdbc/query c (sql-gen db cm))

    (if (and (= :insert (:command cm))
             (= :not-sure db))
      (apply jdbc/insert! c (:table cm) (:column cm) (:value cm))
      (jdbc/execute! c (sql-gen db cm)))))
