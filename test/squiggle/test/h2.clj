(ns squiggle.test.h2
  (:use clojure.test)
  (:require [squiggle.core :as sq]
            [squiggle.helpers :as h]))

(def db {:subprotocol "h2"
         :subname     "mem:h2db;DB_CLOSE_DELAY=-1"})

(def sql! (partial sq/sql! :h2 db))

(def ct-user
  {:command :create-table
   :table :user
   :column-schema
     [[:id :identity [:primary-key]]
      [:username :varchar [:not-null :unique]]
      [:password :varchar [:not-null]]
      [:email :varchar [:not-null]]
      [:roles :varchar [:not-null]]
      [:created_at :timestamp]
      [:updated_at :timestamp]]})

(def ct-email
  {:command :create-table
   :table :email
   :column-schema
     [[:id :identity [:primary-key]]
      [:user_id :integer [:not-null]]
      [:subject :varchar]
      [:content :text]]})

(def dt-user
  {:command :drop-table
   :option [:if-exists]
   :table :user})

(def dt-all
  {:command :drop-table
   :option [:if-exists]
   :table [:email :user]})

(def ins-user
  {:command :insert
   :table :user
   :column [:username :password :email :roles]
   :value [["m" "mistery" "user@user.com" "user"]
           ["a" "passwd" "admin@admin.com" "admin"]
           ["s" "111111" "super@super.com" "user"]]})

(def ins-email
  {:command :insert
   :table :email
   :column [:user_id :subject :content]
   :value [[1 "Email to user m" "loren1"]
           [2 "Email to user a" "loren2"]
           [1 "Email2 to user m" "loren3"]
           [4 "Email to user 4" "loren4"]]})

(def sl-user
  {:command :select
   :table :user})

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

(def sl-full-join
  (assoc-in sl-left-join [:join :type] :full))

(def sl-cross-join
  (-> sl-left-join
      (assoc-in [:join :type] :cross)
      (assoc-in [:join :on] nil)))

(def del-email
  {:command :delete
   :table :email
   :where [:= :user_id 1]})

(def del-user
  {:command :delete
   :table :user
   :where [:like :roles "%dm%"]})

(defn- drop-tables [f]
  (sql! dt-all)
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

(deftest basic-operations
  (testing "create table users"
    (is (= [0] (sql! ct-user))))
  (testing "insert users"
    (is (= [3] (sql! ins-user))))
  (testing "select all users"
    (is (= [{:updated_at nil, :created_at nil, :roles "user",
             :email "user@user.com", :password "mistery", :username "m", :id 1}
            {:updated_at nil, :created_at nil, :roles "admin",
             :email "admin@admin.com", :password "passwd", :username "a", :id 2}
            {:updated_at nil, :created_at nil, :roles "user", :email "super@super.com",
             :password "111111", :username "s", :id 3}]
              (sql! sl-user))))
  (testing "create table email"
    (is (= [0] (sql! ct-email))))
  (testing "insert emails"
    (is (= [4] (sql! ins-email))))
  (testing "left join users email"
    (is (= #{{:subject nil, :username "s"}
             {:subject "Email to user m", :username "m"}
             {:subject "Email2 to user m", :username "m"}
             {:subject "Email to user a", :username "a"}}
              (set (sql! sl-left-join)))))
  (testing "right join users email"
    (is (= #{{:subject "Email to user m", :username "m"}
             {:subject "Email2 to user m", :username "m"}
             {:subject "Email to user a", :username "a"}
             {:subject "Email to user 4" :username nil}}
              (set (sql! sl-right-join)))))
  (testing "inner join users email"
    (is (= #{{:subject "Email to user m", :username "m"}
             {:subject "Email2 to user m", :username "m"}
             {:subject "Email to user a", :username "a"}}
           (set (sql! sl-inner-join)))))
  (testing "count users"
    (is (= [{(keyword "count(*)") 3}] (sql! (assoc sl-user :column [:count :*])))))
  (testing "max users id"
    (is (= [{(keyword "max(\"id\")") 3}] (sql! (assoc sl-user :column [:max :id])))))
  (testing "delete user"
    (is (= [1] (sql! del-user))))
  (testing "delete emails"
    (is (= [2] (sql! del-email))))
  (testing "full join"
    (is (thrown? org.h2.jdbc.JdbcSQLException
               (sql! sl-full-join))))
  (testing "cross join users email"
    (is (= #{{:subject "Email to user 4", :username "s"}
             {:subject "Email to user a", :username "m"}
             {:subject "Email to user a", :username "s"}
             {:subject "Email to user 4", :username "m"}}
              (set (sql! sl-cross-join))))))
