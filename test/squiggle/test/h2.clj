(ns squiggle.test.h2
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        squiggle.core))

(def db {:classname   "org.h2.Driver"
         :subprotocol "h2"
         :subname     "resources/db/test-squiggle-h2.db"})

(def sql! (partial sql-exec! :h2 db))

(def ct-user
  {:command :create
   :table :user
   :column [[:id :identity [:primary-key]]
            [:username :varchar [:not-null :unique]]
            [:password :varchar [:not-null]]
            [:email :varchar [:not-null]]
            [:roles :varchar [:not-null]]
            [:created_at :timestamp]
            [:updated_at :timestamp]]})

(def ct-email
  {:command :create
   :table :email
   :column [[:id :identity [:primary-key]]
            [:user_id :integer [:not-null]]
            [:subject :varchar]
            [:content :text]]})

(def dt-email
  {:command :drop
   :opts [:if-exists]
   :table :email})

(def dt-user
  {:command :drop
   :opts [:if-exists]
   :table :user})

(def sl-user
  {:command :select
   :table :user})

(def ins-user
  {:command :insert
   :table :user
   :column [:username :password :email :roles]
   :values [["m" "mistery" "user@user.com" "user"]
            ["a" "passwd" "admin@admin.com" "admin"]
            ["s" "111111" "super@super.com" "super"]]})

(def ins-email
  {:command :insert
   :table :email
   :column [:user_id :subject :content]
   :values [[1 "Email to user m" "loren1"]
            [2 "Email to user a" "loren2"]
            [1 "Email2 to user m" "loren3"]
            [4 "Email to user 4" "loren4"]]})

(def sl-left-join
  {:command :select
   :table {:user :u}
   :column [:u.username :e.subject]
   :join {:type :left
          :table {:email :e}
          :on [:= :u.id :e.user_id]}})

(def sl-right-join
  (assoc-in sl-left-join [:join :type] :right))

(def sl-inner-join
  (assoc-in sl-left-join [:join :type] :inner))

(def del-user
  {:command :delete
   :table :user
   :where [:= :username "m"]})

(defn- drop-tables [f]
  (sql! dt-user)
  (sql! dt-email)
  (f))

(use-fixtures
  :each drop-tables)

(deftest create-table
  (sql! ct-user)
  (is (= [] (sql! sl-user))))

(deftest drop-table
  (sql! ct-user)
  (sql! dt-user)
  (is (thrown? org.h2.jdbc.JdbcSQLException
               (sql! sl-user))))

(deftest insert-users
  (is (= [0] (sql! ct-user)))
  (is (= [3] (sql! ins-user)))
  (is (= [{:updated_at nil, :created_at nil, :roles "user",
           :email "user@user.com", :password "mistery", :username "m", :id 1}
          {:updated_at nil, :created_at nil, :roles "admin",
           :email "admin@admin.com", :password "passwd", :username "a", :id 2}
          {:updated_at nil, :created_at nil, :roles "super", :email "super@super.com",
           :password "111111", :username "s", :id 3}]
         (sql! sl-user)))
  (is (= [0] (sql! ct-email)))
  (is (= [4] (sql! ins-email)))
  (is (= #{{:subject nil, :username "s"}
           {:subject "Email to user m", :username "m"}
           {:subject "Email2 to user m", :username "m"}
           {:subject "Email to user a", :username "a"}}
         (set (sql! sl-left-join))))
  (is (= #{{:subject "Email to user m", :username "m"}
           {:subject "Email2 to user m", :username "m"}
           {:subject "Email to user a", :username "a"}
           {:subject "Email to user 4" :username nil}}
         (set (sql! sl-right-join))))
  (is (= #{{:subject "Email to user m", :username "m"}
           {:subject "Email2 to user m", :username "m"}
           {:subject "Email to user a", :username "a"}}
         (set (sql! sl-inner-join))))
  (is (= [{(keyword "count(*)") 3}] (sql! (assoc sl-user :column [:count :*]))))
  (is (= [{(keyword "max(\"id\")") 3}] (sql! (assoc sl-user :column [:max :id]))))
  (is (= [1] (sql! del-user)))
  (is (= #{{:updated_at nil, :created_at nil, :roles "admin",
            :email "admin@admin.com", :password "passwd", :username "a", :id 2}
           {:updated_at nil, :created_at nil, :roles "super", :email "super@super.com",
            :password "111111", :username "s", :id 3}}
         (set (sql! sl-user))))
  )
