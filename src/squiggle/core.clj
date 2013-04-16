(ns squiggle.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.walk :as walk]
            [clojure.string :as str]))

(def ^{:private true
       :doc "Map of operators used to convert the operator keyword to a
       string."}
  operators->str
  {:=       "="
   :>       ">"
   :>=      ">="
   :<       "<"
   :<=      "<="
   :!=      "!="
   :<>      "<>"
   :not=    "!="
   :like    "LIKE"
   :in      "IN"
   :not-in  "NOT IN"
   :between "BETWEEN"
   :and     "AND"
   :or      "OR"})

(def ^{:private true
       :doc "Map of aggregators used to convert the aggregator keyword to
       a string"}
  aggregators
  {:count "COUNT"
   :sum "SUM"
   :avg "AVG"
   :stdev "STDEV"
   :min "MIN"
   :max "MAX"
   :first "FIRST"
   :last "LAST"})

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

(defn- table-name
  "Given a entity return the table name string"
  [e]
  (name (:table e)))

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

(defn- prefix-columns
  "Given columns and a process table map return a string of prefixed columns."
  [cs m]
  (let [cs (or cs
              [:*])
        alias (:alias m)
        default-table (if (= 1 (count alias)) (key (first alias)))]
    (str/join ", "
                (for [c cs
                      :let [[cn tn & r] (reverse (str/split (name c) #"\."))
                            tn (or tn (name default-table))]]
                  (str (str/join "." (reverse r))
                       (name
                       (or ((keyword tn) (:alias m))
                           tn))
                       "."
                       cn)))))

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
  (when-let [e (seq (arguments (pre-process-exp ex)))]
    e))

(defn- add-operators
  "Given an expression returns a expression with operator strings replacing
  operator keywords."
  [ex]
  (walk/postwalk-replace operators->str ex))

(defn- add-columns* [q f]
  (if (keyword? f)
    (let [cn (name f)
          s (set cn)]
      (if (s \.)
        cn
       (str (table-name q) "." cn)))
    f))

(defn- add-columns
  "Given an expression returns a expression with column strings replacing
  column keywords."
  [q ex]
  (walk/postwalk (partial add-columns* q) ex))

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

(defn- where-gen
  "Given a query return the processed where clause."
  [q]
  (some->> (:where q)
           pre-process-exp
           sanitize
           add-operators
           fix-in-vector
           (add-columns q)
           parentesis
           add-space
           flatten
           (apply str "WHERE ")))

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

(defn sql-select
  "Given a select command map, returns a select query vector."
  [cm]
   (let [pt (process-tables (:table cm))
         qv [(-> ["SELECT" (prefix-columns (:columns cm) pt) ["FROM"] (:string pt)]
                 ((fn [x] (if (where-gen cm) (conj x (where-gen cm)) x)))
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
