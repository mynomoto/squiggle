(ns squiggle.test.h2
  (:use clojure.test
        squiggle.core))

(def db {:classname   "org.h2.Driver"
         :subprotocol "h2"
         :subname     "resources/db/test-squiggle-h2.db"})

(def sql! (partial sql-exec! :h2 db))

(def ct
  {:command :create-table
   :table :user
   :columns [[:identity :id [:primary-key]]
             [:varchar :username [:not-null :unique]]
             [:varchar :password [:not-null]]
             [:varchar :email [:not-null]]
             [:varchar :roles [:not-null]]
             [:timestamp :created_at]
             [:timestamp :updated_at]]
   :opts {:if-not-exists true
          :temp false
          :temporary false}})

(def dt
  {:command :drop-table
   :opts {:if-exists true
          :cascade false
          :restrict false}
   :table [:user]})

(def sl
  {:command :select
   :table :user})

(defn- drop-tables [f]
  (sql! dt)
  (f))

(use-fixtures
  :each drop-tables)

(deftest create-table
  (sql! ct) 
  (is (= () (sql! sl))))

(deftest drop-table
  (sql! ct) 
  (sql! dt)
  (is (thrown? org.h2.jdbc.JdbcSQLException
               (sql! sl))))   
