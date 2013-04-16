(ns squiggle.test.core
  (:use clojure.test
        squiggle.core))

(def db {:classname   "org.h2.Driver"
         :subprotocol "h2"
         :subname     "resources/db/squiggle.db"})

(def ent {:table :user
          :default-select nil
          :pk nil
          :relationships {:letter {:type :has-many
                                   :fk nil
                                   :join-table nil
                                   :pk-jt nil
                                   :fk-jt nil}}})

(def ct
  {:command :create-table
   :table :user
   :columns [[:identity :id [:primary-key]]]
   :opts {:if-not-exists true
          :temp false
          :temporary false}})

(deftest test-create-table
  (testing "default"
    (is (= (sql ct)
           ["CREATE TABLE user IF NOT EXISTS (id identity PRIMARY KEY)"])))
  (testing "without :if-not-exists"
    (is (= (sql (assoc-in ct [:opts :if-not-exists] false))
           ["CREATE TABLE user (id identity PRIMARY KEY)"])))
  (testing "with :temp"
    (is (= (sql (assoc-in ct [:opts :temp] true))
           ["CREATE TABLE TEMPORARY user IF NOT EXISTS (id identity PRIMARY KEY)"])))
  (testing "with :temporary"
    (is (= (sql (assoc-in ct [:opts :temporary] true))
           ["CREATE TABLE TEMPORARY user IF NOT EXISTS (id identity PRIMARY KEY)"]))))

(def dt
  {:command :drop-table
   :opts {:if-exists true
          :cascade false
          :restrict false}
   :table [:user]})

(deftest test-drop-table
  (testing "default"
    (is (= (sql dt)
           ["DROP TABLE IF EXISTS user"])))
  (testing "without :if-exists"
    (is (= (sql (assoc-in dt [:opts :if-exists] false))
           ["DROP TABLE user"])))
  (testing "with :cascade"
    (is (= (sql (assoc-in dt [:opts :cascade] true)))
           ["DROP TABLE IF EXISTS user CASCADE"]))
  (testing "with :restrict"
    (is (= (sql (assoc-in dt [:opts :restrict] true))
           ["DROP TABLE IF EXISTS user RESTRICT"])))
  (testing "with :restrict and :cascade"
    (is (thrown? IllegalArgumentException
                 (sql (assoc dt :opts {:restrict true :cascade true}))))))

(def sl
  {:command :select
   :table :user})

(deftest test-select-query
  (testing "default"
    (is (= (sql sl)
           ["SELECT user.* FROM user"])))
  (testing "with table alias"
    (is (= (sql (assoc sl :table [[:user :u]]))
           ["SELECT u.* FROM user AS u"])))
  (testing "with table columns"
    (is (= (sql (assoc sl :columns [:username :role]))
           ["SELECT user.username, user.role FROM user"])))
  (testing "with table alias and columns"
    (is (= (sql (assoc sl :table [[:user :u]] :columns [:username :role]))
           ["SELECT u.username, u.role FROM user AS u"])))
  (testing "with several tables and columns"
    (is (= (sql (assoc sl :table [[:user :u] [:email :e] :address]
                          :columns [:user.username :user.role
                                    :email.title :address.zip]))
           [(str "SELECT u.username, u.role, e.title, address.zip "
                 "FROM user AS u, email AS e, address")])))
  (testing "with a simple where clause"
    (is (= (sql (assoc sl :where [:and [:in :username ["user" "u" "admin"]]
                                       [:like :roles "%us%"]
                                       [:or [:< :id 1000]
                                       [:> :id 0]]]))
           [(str "SELECT user.* FROM user "
                 "WHERE user.username IN ( ? ,  ? ,  ? ) AND "
                 "user.roles LIKE ? AND ( user.id < ? OR user.id > ? )")
            "user" "u" "admin" "%us%" 1000 0]))))

{:command :select
 :table :user
 :modifier nil
 :where [:and [:in :username ["user" "u" "admin"]]
              [:like :roles "%us%"]
              [:or [:< :id 1000]
                   [:> :id 0]]]
 :group-by nil
 :having nil
 :offset nil
 :limit nil}

(defmacro with-private-fns [[ns fns] & tests]
  "Refers private fns from ns and runs tests in context."
  `(let ~(reduce #(conj %1 %2 `(ns-resolve '~ns '~%2)) [] fns)
     ~@tests))

(with-private-fns [squiggle.core [ct-columns]]
  (deftest private-functions
    (testing "string-to-create-columns"
      (is (= (ct-columns [[:identity :id [:primary-key]]
                          ["varchar(100)" :username [:not-null :unique]]
                          ["varchar(100)" :password [:not-null]]
                          [:varchar :roles [:not-null]]
                          [:timestamp :created_at]
                          [:timestamp :updated_at]])
             (str "id identity PRIMARY KEY, "
                  "username varchar(100) NOT NULL UNIQUE, "
                  "password varchar(100) NOT NULL, "
                  "roles varchar NOT NULL, "
                  "created_at timestamp, "
                  "updated_at timestamp"))))))

;(query db (gen-select ent))
;(jdbc/execute! db ["insert into user set username = ?, password = ?, roles = ?" "mynomoto" "password" "user"])
;(jdbc/execute! db ["insert into user set username = ?, password = ?, roles = ?" "mynomoto" "password" "user"])
;[:and [:and [:> :a 3] [:<= :d 3] [:between :d [:and 2 5]]] [:or [:> :d :a] [:< :a :d]] [:in :x [3 4 9]]]
;[:and [:in :username ["marcelo" "m" "myn"]] [:like :roles "%adm%"] [:or [:< :id 1000] [:> :id 0]]]
;(jdbc/execute! db ["update user set username = ? where id IN ( ?, ? )" "bla" 1 2])
;(jdbc/query db ["SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"])
;(jdbc/query db ["SELECT TABLE_NAME, SQL FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'TABLE'"])
;(jdbc/query db ["SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'SYSTEM TABLE' "])
;(jdbc/query db ["SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS"])
