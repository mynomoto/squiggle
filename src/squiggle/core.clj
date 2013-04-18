(ns squiggle.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.walk :as walk]
            [clojure.string :as str]))

(def ^{:private true
       :doc "Map of operators used to convert the operator keyword to a
       string."}
  operators->str
  {:=        "="
   :>        ">"
   :>=       ">="
   :<        "<"
   :<=       "<="
   :!=       "!="
   :<>       "<>"
   :not=     "!="
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
  #{:> :< := :>= :<= :!= :<> :like :not= :in :not-in :between :is-null :not-null})

(def ^{:private true
       :doc "Map of options for create-table used to convert keywords to
       strings."}
  create-table-options
  {:primary-key "PRIMARY KEY"
   :unique      "UNIQUE"
   :null        "NULL"
   :not-null    "NOT NULL"})

(defn- table-alias
  [t]
  (if (coll? t)
    (let [[tname talias] t]
      {:string (str (name tname) " AS " (name talias))
       :alias {tname talias}})
    {:string (name t)}))

(defn- process-tables
  [t]
  (if (coll? t)
    (let [am (map table-alias t)
          a (map :alias am)]
      {:string (str/join ", " (map :string am))
       :alias (reduce merge a)})
    {:string (name t)
     :alias {t t}}))

(defn- convert-options
  [opt]
  (cond
    (string? opt)  opt
    (coll? opt)    (str/join " " (replace create-table-options opt))
    (keyword? opt) (opt create-table-options)
    :else          (throw (IllegalArgumentException. "Incorrect option format."))))

(defn- prefix-column
  "Given a table alias map, a column alias map and a column returns the
  prefixed column string. If the column is a column alias, returns the
  original column. Always apply the column alias."
  [ta ca c]
  (if (coll? c)
    (if (and ca (ca c))
      {:string (ca c)}
      (if (coll? (first c))
        (if (= 1 (count (first c)))
          {:string (str (ffirst c) " AS " (name (last c)))
           :alias {(last c) (ffirst c)}}
          (let [[f a] (first c)
                f (if (keyword? f) (or (f aggregators->str) (name f)) f)
                a (if (keyword? a) (if (= a :*) "*" (:string (prefix-column ta ca a))) (name a))]
            {:string (str f "(" a ")" " AS " (name (last c)))
             :alias {(first c) (str f "(" a ")")}}))
        (let [pcolumn (:string (prefix-column ta ca (first c)))]
          {:string (str pcolumn " AS " (name (last c)))
           :alias {(last c) (keyword pcolumn)}})))

    (let [ra (c ca)
          default-table (if (= 1 (count ta)) (key (first ta)))
          [cn tn & r] (reverse (str/split (name c) #"\."))
          tn (or tn (if default-table (name default-table)))
          tb-nm (if-let [tb-nm (or ((keyword tn) ta) tn)] tb-nm)
          tb-col (if tb-nm (str (name tb-nm) "." cn) cn)]
      {:string
       (str (if r (str (str/join "." (reverse r)) "."))
            (if ra
              (name ra)
              tb-col))})))

(defn- prefix-list-columns
  "Given columns and a table alias map returns the prefixed columns string."
  [cs ta]
  (let [cs (or cs [:*])
        pc (map (partial prefix-column ta nil) cs)
        a (map :alias pc)]
    {:string (str/join ", " (map :string pc))
     :alias (reduce merge a)}))

(defn- pre-process-exp*
  "Given a form, return a pre-processed form in the correct order for
  string generation."
  [f]
  (if (coll? f)
    (let [[o & r] f]
       (if (operators->str o)
         (cond
          (infix-operators o) (let [[le re] r]
                         (list le o re))
          (= :and o) (if (= 1 (count r))
                         r
                         (interpose o r))
          (= :or o) (if (= 1 (count r))
                         r
                         (vec (interpose o r))))
         f))
    f))

(defn- pre-process-exp
  "Given a where or having expression returns a pre-processed expression in
  the correct order for string generation."
  [ex]
  (walk/postwalk pre-process-exp* ex))

(defn- sanitize*
  "Given a form, returns a string ? if the form isn't a collection or a
  keyword."
  [f]
  (if (or (coll? f) (keyword? f) (and (string? f) (= (first f) \!)))
    f
    "?"))

(defn- sanitize
  "Given an expression, returns a sanitized expression with ? replacing
  arguments."
  [ex]
  (walk/postwalk sanitize* ex))

(defn- arguments
  "Given an expression returns a lazy seq of all arguments"
  [ex]
  (remove #(if (and (string? %) (= (first %) \!)) true)
          (remove keyword? (flatten ex))))

(defn- gen-arguments
  [ex]
  (when-let [a (seq (arguments (pre-process-exp ex)))]
    a))

(defn- add-operators
  "Given an expression returns a expression with operator strings replacing
  operator keywords."
  [ex]
  (walk/postwalk-replace operators->str ex))

(defn- add-columns*
  [ta ca f]
  (if (or (keyword? f) (and (vector? f) (= 2 (count f))))
    (:string (prefix-column ta ca f))
    f))

(defn- add-columns
  "Given an alias map and an expression returns a expression with column
  strings replacing column keywords."
  [ta ca ex]
  (walk/prewalk (partial add-columns* ta ca) ex))

(defn- remove-literal-mark*
  [f]
  (if (and (string? f) (= (first f) \!))
    (str/replace-first f #"^!" "")
    f))

(defn- remove-literal-mark
  [ex]
  (walk/postwalk remove-literal-mark* ex))

(defn- fix-in-vector*
  "We need the vector following \"IN\" comma separated."
  [f]
  (if (coll? f)
    (if (= (second f) "IN")
      (list (first f) (second f) (vec (interpose ", " (last f))))
      f)
    f))

(defn- fix-in-vector
  [ex]
  (walk/postwalk fix-in-vector* ex))

(defn- parentesis*
  [f]
  (if (vector? f)
    (list "(" (seq f) ")")
    f))

(defn- parentesis
  [ex]
  (walk/postwalk parentesis* ex))

(defn- add-space*
  [f]
  (if (coll? f)
    (interpose " " f)
    f))

(defn- add-space
  [ex]
  (walk/postwalk add-space* ex))

(defn- exp-gen
  "Given a expression, a alias map and the expression type, returns the
  string for the expression."
  [e ta ca type]
  (let [exp (some->> e
                     pre-process-exp
                     sanitize
                     add-operators
                     remove-literal-mark
                     fix-in-vector
                     (add-columns ta ca)
                     parentesis
                     add-space
                     flatten)]
    (cond
     (nil? exp) nil
     (= type :where) (apply str "WHERE " exp)
     (= type :having) (apply str "HAVING " exp)
     :else (throw (IllegalArgumentException.
                   "Only accepts type :where or :having.")))))

(defn sql-drop
  "Given an entity generate a command to drop the entity table."
  [t opts]
  (if (and (:cascade opts)
           (:restrict opts))
    (throw (IllegalArgumentException.
            "Can't use both :cascade and :restrict at the same time."))
    [(str "DROP TABLE "(if (:if-exists opts) "IF EXISTS ") (:string (process-tables t))
          (if (:cascade opts) " CASCADE") (if (:restrict opts) " RESTRICT"))]))

(defn- ct-columns
  "Given a vector of column maps returns a string to create this columns in
  a create-table command."
  [c]
  (str/join ", "
   (for [[type c-name options] c]
     (str/join
     [(name c-name) " "
      (str (name type))
      (if options (str " " (convert-options options)))]))))

(defn sql-create
  "Given a table, a vector of columns and a map of options, generates a
  command to create a table and columns with options."
  [t c opts]
  [(str "CREATE "
        (if (or (:temp opts)
                (:temporary opts)) "TEMPORARY ")
        "TABLE "
        (if (:if-not-exists opts) "IF NOT EXISTS ")
        (:string (process-tables t))
        " ("
        (ct-columns c)
        ")")])

(defn add-expression
  "Given an select expression, a where or having expression and an alias
  map, returns the select expression with the where/having expression."
  [ex we ta ca ty]
  (if-let [w (exp-gen we ta ca ty)]
    (conj ex w)
    ex))

(defn add-group-by
  [ex gb ta]
  (if gb
    (conj ex (str "GROUP BY " (:string (prefix-list-columns gb ta))))
    ex))

(defn add-modifier
  [m]
  (cond
   (nil? m) m
   (coll? m) (if (integer? (second m))
               (str " " (name (first m)) " " (second m))
               (apply str (map add-modifier m)))
   :else (str " " (name m))))

(defn order-by
  [ob]
  (cond
   (keyword? ob) (name ob)
   (string? ob) ob
   (some coll? ob) (map order-by ob)
   :else (str/join " " (map name ob))))

(defn add-order-by
  [ex ob]
  (if ob
    (let [ob (order-by ob)]
      (conj ex (str "ORDER BY " (if (coll? ob) (str/join ", " ob) ob))))
    ex))

(defn add-limit
  [ex i]
  (if i
    (conj ex (str "LIMIT " i))
    ex))

(defn add-offset
  [ex i]
  (if i
    (conj ex (str "OFFSET " i))
    ex))

(defn sql-select
  "Given a select command map, returns a select query vector."
  [{:keys [table columns where group-by having order-by limit offset modifier]}]
   (let [pt (process-tables table)
         pc (prefix-list-columns columns (:alias pt))
         qv [(-> [(str "SELECT"
                  (add-modifier modifier))
                  (:string pc)
                  "FROM"
                  (:string pt)]
                 (add-expression where (:alias pt) (:alias pc) :where)
                 (add-group-by group-by (:alias pt))
                 (add-expression having (:alias pt) (:alias pc) :having)
                 (add-order-by order-by)
                 (add-limit limit)
                 (add-offset offset)
                 add-space
                 flatten
                 str/join)]]
      (if-let [arguments (concat (gen-arguments where)
                                 (gen-arguments having))]
        (vec (concat qv arguments))
        qv)))

(defn sql-insert
  "Given a command map return the insert sql code"
  [{:keys [table columns data]}]
  (let [c (count columns)]
    (concat [(str "INSERT INTO " (name table) " ("
                  (str/join ", "(map name columns)) ") VALUES ("
                  (str/join ", " (repeat c "?")) ")")]
            data)))

(defn sql-gen
  "Given a command map return the sql code."
  [db cm]
  (case (:command cm)
    :select
    (sql-select cm)

    :insert
    (sql-insert cm)

    :drop-table
    (sql-drop (:table cm) (:opts cm))

    :create-table
    (sql-create (:table cm) (:columns cm) (:opts cm))

    (throw (IllegalArgumentException. "Incorrect :command value format."))))

(defn sql-exec!
  "Given a database, a command map and a connection map, execute the
  command."
  [db c cm]
  (case (:command cm)
    :select
    (jdbc/query c (sql-gen db cm))

    (jdbc/execute! c (sql-gen db cm))))

(defn select
  "Given an entity returns a select query for the entity."
  [e]
  {:command :select
   :modifier nil
   :target e
   :where nil
   :group-by nil
   :having nil
   :offset nil
   :limit nil})
