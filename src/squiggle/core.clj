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
  aggregators
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
  #{:> :< := :>= :<= :!= :<> :like :not= :in :not-in :between})

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
  "Given a table alias map and a column return a string of prefixed column."
  [am c]
  (let [default-table (if (= 1 (count am)) (key (first am)))
        [cn tn & r] (reverse (str/split (name c) #"\."))
        tn (or tn (if default-table (name default-table)))]
    (str
      (if r (str (str/join "." (reverse r)) "."))
      (if (or ((keyword tn) am)
                tn)
        (str (name (or ((keyword tn) am)
                tn)) "."))
      cn)))

(defn- prefix-list-columns
  "Given columns and a process table map return a string of prefixed columns."
  [cs am]
  (let [cs (or cs
              [:*])]
    (str/join ", "
              (map (partial prefix-column am) cs))))

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
  (if (or (coll? f) (keyword? f))
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
  (remove keyword? (flatten ex)))

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
  [m f]
  (if (keyword? f)
    (prefix-column m f)
    f))

(defn- add-columns
  "Given an alias map and an expression returns a expression with column
  strings replacing column keywords."
  [m ex]
  (walk/postwalk (partial add-columns* m) ex))

(defn- fix-in-vector*
  "We need the vector following \"IN\" comma separated."
  [f]
  (if (coll? f)
    (if (= (second f) "IN")
      (list (first f) (second f) (vec (interpose ", " (last f))))
      f)
    f))

(defn- fix-in-vector
  [e]
  (walk/postwalk fix-in-vector* e))

(defn- parentesis*
  [f]
  (if (vector? f)
    (list "(" (seq f) ")")
    f))

(defn- parentesis
  [e]
  (walk/postwalk parentesis* e))

(defn- add-space*
  [f]
  (if (coll? f)
    (interpose " " f)
    f))

(defn- add-space
  [e]
  (walk/postwalk add-space* e))

(defn- exp-gen
  "Given a expression, a alias map and the expression type, returns the
  string for the expression."
  [e m type]
  (let [exp (some->> e
                     pre-process-exp
                     sanitize
                     add-operators
                     fix-in-vector
                     (add-columns m)
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
  [(str "CREATE TABLE "
        (if (or (:temp opts)
                (:temporary opts)) "TEMPORARY ")
        (:string (process-tables t))
        (if (:if-not-exists opts) " IF NOT EXISTS")
        " ("
        (ct-columns c)
        ")")])

(defn add-expression
  "Given an select expression, a where or having expression and an alias
  map, returns the select expression with the where/having expression."
  [ex we m ty]
  (if-let [w (exp-gen we m ty)]
    (conj ex w)
    ex))

(defn sql-select
  "Given a select command map, returns a select query vector."
  [cm]
   (let [pt (process-tables (:table cm))
         qv [(-> ["SELECT"
                  (prefix-list-columns (:columns cm) (:alias pt))
                  "FROM"
                  (:string pt)]
                 (add-expression (:where cm) (:alias pt) :where)
                 (add-expression (:having cm) (:alias pt) :having)
                 add-space
                 flatten
                 str/join)]]
      (if-let [arguments (gen-arguments (:where cm))]
        (vec (concat qv arguments))
        qv)))

(defn sql
  "Given a command map return the sql code."
  [cm]
  (case (:command cm)
    :select
    (sql-select cm)

    :drop-table
    (sql-drop (:table cm) (:opts cm))

    :create-table
    (sql-create (:table cm) (:columns cm) (:opts cm))

    (throw (IllegalArgumentException. "Incorrect :command key format."))))


(defn select
  "Given an entity returns a select query for the entity."
  [e]
  {:command :select
   :modifier nil
   :target e
   :where [:and [:in :username ["mynomoto" "m" "myn"]] [:like :roles "%us%"] [:or [:< :id 1000] [:> :id 0]]]
   :group-by nil
   :having nil
   :offset nil
   :limit nil})

(defn query [db q]
  (jdbc/query db (sql q)))

(defmulti auto
  "Given a db and a options map returns a column vector."
  (fn [db _] db))

(defmethod auto :h2
  [_ opts]
  (case (:type opts)
    :pk [:identity (or (:name opts) :id) :primary-key]
    :belongs (auto :default opts)
    :erro))

(defmethod auto :default
  [_ opts]
  (case (:type opts)
    :pk [:integer (or (:name opts) :id) :primary-key]
    :belongs [:integer (or (:name opts) (keyword (str (name (:relationship opts)) "_id")))]
    :erro))
