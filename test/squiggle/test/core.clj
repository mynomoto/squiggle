(ns squiggle.test.core
  (:use clojure.test
        squiggle.core))

(defmacro with-private-fns [[ns fns] & tests]
  "Refers private fns from ns and runs tests in context."
  `(let ~(reduce #(conj %1 %2 `(ns-resolve '~ns '~%2)) [] fns)
     ~@tests))

(with-private-fns [squiggle.core
                   [add-modifier table-string ct-columns table-alias
                    pre-process-exp* sanitize* arguments add-columns*
                    remove-literal-mark* column-string fix-in-vector*
                    parentesis* add-join join-args]]
  (deftest private-functions
    (testing "add-modifier"
      (is (= "DISTINCT" (add-modifier :h2 :distinct)))
      (is (= "TOP 10" (add-modifier :h2 {:top 10})))
      (is (= "DISTINCT ON (\"username\", \"role\")" (add-modifier :h2 [{:distinct-on [:username :role]} ])))
      (is (= "TOP 10 DISTINCT" (add-modifier :h2 [{:top 10} :distinct]))))

    (testing "table-string"
      (is (= "\"user\"" (table-string :h2 :user)))
      (is (= "\"user\" AS \"u\"" (table-string :h2 {:user :u})))
      (is (= "\"user\" AS \"u\"" (table-string :h2 [{:user :u}])))
      (is (= "\"user\" AS \"u\", \"email\"" (table-string :h2 [{:user :u} :email])))
      (is (= "\"user\" AS \"u\", \"email\"" (table-string :h2 [{:user :u} :email])))
      (is (= "UsEr AS \"u\", \"email\"" (table-string :h2 [{"UsEr" :u} :email]))))

    (testing "column-string-for-create-columns"
      (is (= (ct-columns :h2
                         [[:id :identity [:primary-key]]
                          ["LoGiN" "VARchar(30)" ["NOT NULL" :unique "primary key"]]
                          [:username "varchar(100)" [:not-null :unique]]
                          [:password "varchar(100)" [:not-null]]
                          [:roles :varchar [:not-null]]
                          [:created_at :timestamp]
                          [:updated_at :timestamp]])
             (str "\"id\" identity PRIMARY KEY, "
                  "LoGiN VARchar(30) NOT NULL UNIQUE primary key, "
                  "\"username\" varchar(100) NOT NULL UNIQUE, "
                  "\"password\" varchar(100) NOT NULL, "
                  "\"roles\" varchar NOT NULL, "
                  "\"created_at\" timestamp, "
                  "\"updated_at\" timestamp"))))

    (testing "pre-process-exp"
      (is (= ["!UserName" "!LiKe" "joh%"]
             (pre-process-exp* ["!UserName" "!LiKe" "joh%"])))
      (is (= [:id :> 1000]
             (pre-process-exp* [:> :id 1000])))
      (is (= [:username :like "joh%"]
             (pre-process-exp* [:like :username "joh%"])))
      (is (= [:id :between [:and 2 5]]
             (pre-process-exp* [:between :id [:and 2 5]])))
      (is (= [:id :in [2 5]]
             (pre-process-exp* [:in :id [2 5]])))
      ; AND
      (is (= '([:like :username "joh%"])
             (pre-process-exp* [:and [:like :username "joh%"]])))
      (is (= [[:> :id 1000] :and [:like :username "joh%"]]
             (pre-process-exp* [:and [:> :id 1000]
                                [:like :username "joh%"]])))
      (is (= [[:> :id 1000] :and [:like :username "joh%"]
              :and [:= :id 300]]
             (pre-process-exp* [:and [:> :id 1000]
                                [:like :username "joh%"]
                                [:= :id 300]])))
      (is (not (vector? (pre-process-exp*
                    [:and [:> :id 1000] [:like :username "joh%"]]))))
      ; OR
      (is (= '([:like :username "joh%"])
             (pre-process-exp* [:or [:like :username "joh%"]])))
      (is (= [[:> :id 1000] :or [:like :username "joh%"]]
             (pre-process-exp* [:or [:> :id 1000]
                                [:like :username "joh%"]])))
      (is (= [[:> :id 1000] :or [:like :username "joh%"]
              :or [:= :id 300]]
             (pre-process-exp* [:or [:> :id 1000]
                                [:like :username "joh%"]
                                [:= :id 300]])))
      (is (vector? (pre-process-exp*
                    [:or [:> :id 1000] [:like :username "joh%"]]))))

    #_(testing "table-alias"
      (is (= {:user nil} (table-alias :user)))
      (is (= {:user :u} (table-alias {:user :u})))
      (is (= {:user :u} (table-alias [{:user :u}])))
      (is (= {:user :u, :email nil} (table-alias [{:user :u} :email])))
      (is (= {"UsEr" :u, :email nil} (table-alias [{"UsEr" :u} :email]))))

    (testing "sanitize"
      (is (= :user (sanitize* :user)))
      (is (= "!escaped" (sanitize* "!escaped")))
      (is (= [:a :b] (sanitize* [:a :b])))
      (is (= "?" (sanitize* 10)))
      (is (= "?" (sanitize* "not escaped"))))

    (testing "arguments"
      (is (= ["user" "u" "admin" "%us%" 1000 0 "joh%"]
             (arguments [:and [:in :username ["user" "u" "admin"]]
                         [:like :roles "%us%"]
                         [:or [:< :id 1000] [:> :id 0]]
                         ["!UserName" "!LiKe" "joh%"]]))))

    (testing "add-columns"
      (is (= "\"user\".\"username\"" (add-columns* :h2 :user.username)))
      (is (= "\"username\"" (add-columns* :h2 :username)))
      (is (= "\"info\".\"user\".\"username\"" (add-columns* :h2 :info.user.username)))
      (is (= "COUNT(*)" (add-columns* :h2 [:count :*])))
      (is (= "MAX(\"id\")" (add-columns* :h2 [:max :id]))))

    (testing "remove-literal-mark*"
      (is (= "user" (remove-literal-mark* "!user")))
      (is (= "user" (remove-literal-mark* "user"))))

    (testing "column-string"
      (is (= "*" (column-string :db nil)))
      (is (= "Us!#er" (column-string :db "Us!#er")))
      (is (= "Us!#er, MAX(\"id\") AS \"max\", \"user\".\"username\" AS \"u\""
             (column-string :h2 ["Us!#er" {[:max :id] :max} {:user.username :u}])))
      (is (= "COUNT(*)" (column-string :h2 [:count :*]))))

    (testing "fix-in-vector*"
      (is (= ["user" "IN" ["?" ", " "?" ", " "?"]]
             (fix-in-vector* ["user" "IN" ["?" "?" "?"]])))
      (is (not (vector? (fix-in-vector* ["user" "IN" ["?" "?" "?"]]))))
      (is (vector? (last (fix-in-vector* ["user" "IN" ["?" "?" "?"]]))))
      (is (= ["user" "LIKE" ["?" "?" "?"]]
             (fix-in-vector* ["user" "LIKE" ["?" "?" "?"]]))))

    (testing "parentesis"
      (is (= "(?, ?, ?)" (parentesis* ["?" ", " "?" ", " "?"])))
      (is (= "(id < ? OR id > ?)" (parentesis* [["id" "<" "?"] "OR" ["id" ">" "?"]]))))

    (testing "add-join"
      (is (= "INNER JOIN \"email\" ON \"user\".\"id\" = \"email\".\"user_id\""
             (add-join :h2 {:type :inner
                            :table :email
                            :on [:= :user.id :email.user_id]})))
      (is (= (str "FULL JOIN friends ON user.id = friends.user_id "
                  "LEFT JOIN email ON user.id = email.user_id"
             (add-join :h2 [{:type :full
                             :table :friends
                             :on [:= :user.id :friends.user_id]}
                            {:type :left
                             :table :email
                             :on [:= :user.id :email.user_id]}]))))

    (testing "join-args"
      (is (= nil
             (join-args {:type :inner
                         :table :email
                         :on [:= :user.id :email.user_id]})))
      (is (= nil
             (join-args [{:type :full
                             :table :friends
                             :on [:= :user.id :friends.user_id]}
                            {:type :left
                             :table :email
                             :on [:= :user.id :email.user_id]}])))
      (is (= [2 3 9 7]
             (join-args [{:type :full
                             :table :friends
                             :on [:in :user.id [2 3 9]]}
                            {:type :left
                             :table :email
                             :on [:= :user.id 7]}])))))))
(def ent {:table :user
          :default-select nil
          :pk nil
          :relationships {:letter {:type :has-many
                                   :fk nil
                                   :join-table nil
                                   :pk-jt nil
                                   :fk-jt nil}}})

(def ct
  {:command :create
   :table :user
   :column [[:id :identity [:primary-key]]]
   :options [:if-not-exists]})

(def sql (partial sql-gen :h2))
(def sql-default (partial sql-gen :not-sure))

(deftest test-create-table
  (testing "default"
    (is (= ["CREATE TABLE IF NOT EXISTS \"user\" (\"id\" identity PRIMARY KEY)"]
           (sql ct))))
  (testing "without :if-not-exists"
    (is (= ["CREATE TABLE \"user\" (\"id\" identity PRIMARY KEY)"]
           (sql (assoc ct :options [])))))
  (testing "with :temp"
    (is (= ["CREATE TEMP TABLE IF NOT EXISTS \"user\" (\"id\" identity PRIMARY KEY)"]
           (sql (assoc ct :options [:temp :if-not-exists])))))
  (testing "with :temporary"
    (is (= ["CREATE TEMPORARY TABLE IF NOT EXISTS \"user\" (\"id\" identity PRIMARY KEY)"]
           (sql (assoc ct :options [:temporary :if-not-exists]))))))

(def dt
  {:command :drop
   :options [:if-exists]
   :table [:user]})

(deftest test-drop-table
  (testing "default"
    (is (= ["DROP TABLE IF EXISTS \"user\""]
           (sql dt))))
  (testing "without :if-exists"
    (is (= ["DROP TABLE \"user\""]
           (sql (assoc dt :options [])))))
  (testing "with :cascade"
    (is (= ["DROP TABLE IF EXISTS \"user\" CASCADE"]
           (sql (assoc dt :options [:cascade :if-exists])))))
  (testing "with :restrict"
    (is (= ["DROP TABLE IF EXISTS \"user\" RESTRICT"]
           (sql (assoc dt :options [:restrict :if-exists])))))
  (testing "with :restrict and :cascade"
    (is (thrown? IllegalArgumentException
                 (sql (assoc dt :options [:restrict :cascade]))))))

(def sl
  {:command :select
   :table :user})

(deftest test-select-query
  (testing "default"
    (is (= ["SELECT * FROM \"user\""]
           (sql sl))))

  (testing "with table alias"
    (is (= ["SELECT * FROM \"user\" AS \"u\""]
           (sql (assoc sl :table [{:user :u}])))))

  (testing "with one column"
    (is (= ["SELECT \"username\" FROM \"user\""]
           (sql (assoc sl :column [:username])))))

  (testing "with one column and a modifier"
    (is (= ["SELECT DISTINCT \"username\" FROM \"user\""]
           (sql (assoc sl :column [:username]
                          :modifier :distinct)))))

  (testing "with one column, a modifier and a order by clause"
    (is (= ["SELECT TOP 10 \"username\" FROM \"user\" ORDER BY \"username\""]
           (sql (assoc sl :column [:username]
                          :modifier {:top 10}
                          :order :username)))))

  (testing "with one column, modifiers and a order by clause"
    (is (= ["SELECT DISTINCT TOP 10 \"username\" FROM \"user\" ORDER BY \"username\""]
           (sql (assoc sl :column [:username]
                          :modifier [:distinct {:top 10}]
                          :order [:username])))))

  (testing "with columns"
    (is (= ["SELECT \"username\", \"roles\" FROM \"user\""]
           (sql (assoc sl :column [:username :roles])))))

  (testing "with functions as columns"
    (is (= ["SELECT count(*) AS \"count\" FROM \"user\""]
           (sql (assoc sl :column [{"count(*)" :count}])))))

  (testing "with functions keywords as columns"
    (is (= ["SELECT COUNT(*) AS \"count\" FROM \"user\""]
           (sql (assoc sl :column [{[:count :*] :count}])))))

  (testing "with one column alias"
    (is (= ["SELECT \"username\" AS \"login\" FROM \"user\""]
           (sql (assoc sl :column [{:username :login}])))))

  (testing "with columns aliases"
    (is (= ["SELECT \"username\" AS \"login\", \"roles\" AS \"perfil\" FROM \"user\""]
           (sql (assoc sl :column [{:username :login} {:roles :perfil}])))))

  (testing "with table alias and columns"
    (is (= ["SELECT \"username\", \"roles\" FROM \"user\" AS \"u\""]
           (sql (assoc sl :table [{:user :u}] :column [:username :roles])))))

  (testing "with table and columns aliases"
    (is (= ["SELECT \"username\" AS \"login\", \"roles\" AS \"perfil\" FROM \"user\" AS \"u\""]
           (sql (assoc sl :table [{:user :u}]
                          :column [{:username :login} {:roles :perfil}])))))

  (testing "with several tables and columns"
    (is (= [(str "SELECT \"u\".\"username\", \"u\".\"roles\", \"e\".\"title\", \"address\".\"zip\" "
                 "FROM \"user\" AS \"u\", \"email\" AS \"e\", \"address\"")]
           (sql (assoc sl :table [{:user :u} {:email :e} :address]
                          :column [:u.username :u.roles
                                    :e.title :address.zip])))))

  (testing "with several tables and columns with alias"
    (is (= [(str "SELECT \"u\".\"username\" AS \"login\", \"u\".\"roles\", \"e\".\"title\", "
                 "\"address\".\"zip\" AS \"code\" "
                 "FROM \"user\" AS \"u\", \"email\" AS \"e\", \"address\"")]
           (sql (assoc sl :table [{:user :u} {:email :e} :address]
                          :column [{:u.username :login} :u.roles
                                    :e.title {:address.zip :code}])))))

  (testing "with a simple where clause"
    (is (= [(str "SELECT * FROM \"user\" "
                 "WHERE \"username\" = ?")
            "user"]
           (sql (assoc sl :where [:and [:= :username "user"]])))))

  (testing "with a simple where clause and aliases"
    (is (= [(str "SELECT \"u\".\"username\" AS \"login\" FROM \"user\" AS \"u\" "
                 "WHERE \"username\" = ?")
            "user"]
           (sql (assoc sl :where [:and [:= :username "user"]]
                          :table [{:user :u}]
                          :column [{:u.username :login}])))))

  (testing "replaces column alias with columns in a where clause"
    (is (= [(str "SELECT \"username\" AS \"login\" FROM \"user\" AS \"u\" "
                 "WHERE \"username\" = ?")
            "user"]
           (sql (assoc sl :where [:and [:= :username "user"]]
                          :table [{:user :u}]
                          :column [{:username :login}])))))

  (testing "with a complex where clause"
    (is (= [(str "SELECT * FROM \"user\" "
                 "WHERE \"username\" IN (?, ?, ?) AND "
                 "\"roles\" LIKE ? AND (\"id\" < ? OR \"id\" > ?)")
            "user" "u" "admin" "%us%" 1000 0]
           (sql (assoc sl :where [:and [:in :username ["user" "u" "admin"]]
                                       [:like :roles "%us%"]
                                       [:or [:< :id 1000]
                                            [:> :id 0]]])))))

  (testing "with a group by clause and an aggregator"
    (is (= ["SELECT COUNT(*) AS \"count\" FROM \"user\" GROUP BY \"roles\""]
           (sql (assoc sl :column [{[:count :*] :count}]
                          :group [:roles])))))

  (testing "with a group by clause, an aggregator and a having clause"
    (is (= ["SELECT COUNT(*) AS \"count\" FROM \"user\" GROUP BY \"roles\" HAVING COUNT(*) > ?" 2]
           (sql (assoc sl :column [{[:count :*] :count}]
                          :having [:> [:count :*] 2]
                          :group [:roles])))))

  (testing "with a group by clause, an aggregator and a having clause literal"
    (is (= ["SELECT COUNT(*) AS \"count\" FROM \"user\" GROUP BY \"roles\" HAVING count(*) > ?" 2]
           (sql (assoc sl :column [{[:count :*] :count}]
                          :having [:> "!count(*)" 2]
                          :group [:roles])))))

  (testing "simple limit clause"
    (is (= ["SELECT * FROM \"user\" LIMIT 10"]
           (sql (assoc sl :limit 10)))))

  (testing "simple offset clause"
    (is (= ["SELECT * FROM \"user\" OFFSET 10"]
           (sql (assoc sl :offset 10)))))
  )

(def ins
  {:command :insert
   :table :user
   :column [:username :password :email :roles]
   :values [["m" "mistery" "user@user.com" "user"]
            ["a" "passwd" "admin@admin.com" "admin"]]})

(def ins-subselect
  {:command :insert
   :table :user
   :select {:command :select
            :column [:id :username :status]
            :table :old_user
            :where [:= :status "active"]}})

(deftest insert
  (testing "h2"
    (is (= [(str "INSERT INTO \"user\" (\"username\", \"password\", \"email\", \"roles\") "
                 "VALUES (?, ?, ?, ?), (?, ?, ?, ?)") "m" "mistery"
                 "user@user.com" "user" "a" "passwd" "admin@admin.com"
                 "admin"]
           (sql ins))))
  (testing "default"
    (is (= ["INSERT INTO \"user\" (\"username\", \"password\", \"email\", \"roles\") VALUES (?, ?, ?, ?)"
            ["m" "mistery" "user@user.com" "user"] ["a" "passwd" "admin@admin.com" "admin"]]
           (sql-default ins))))
  (testing "subselect"
    (is (= ["INSERT INTO \"user\" SELECT \"id\", \"username\", \"status\" FROM \"old_user\" WHERE \"status\" = ?" "active"]
           (sql ins-subselect)))))

(def x {:table [:user {:email :e}]
        :column [{"count(*)" :count}]
        :where [:and [:like :email.title "%error%"] [:< :user.id 7]]
        :group ["user.roles" :email.title]
        :having [:> [:count :*] 1]
        :limit 4
        :offset 10
        :order :username})

(def subselect-where
  {:command :select
   :table :user
   :column :username
   :where [:and [:= :roles "admin"]
                [:in :username {:command :select
                                :table :billing
                                :column :username
                                :where [:like :status "%paid%"]}]]})

(def subselect-column
  {:command :select
   :table :user
   :column [:user.username
            :user.friends
            {:command :select
                      :table :friendship
                      :column [:count :*]
                      :where [:and [:= :user.username
                                       :friendship.username]
                                   [:= :friendship.status
                                       "active"]]}]
   :where [:= :user.roles "admin"]})

(def subselect-table
  {:command :select
   :table {{:command :select
            :table :user
            :column [:roles {[:count :*] :cnt}]
            :group :roles
            :having [:> [:count :*] 2]} :summary}
   :where [:= :roles "admin"]})

(deftest test-sub-select-query
  (testing "simple where case"
    (is (= [(str "SELECT \"username\" FROM \"user\" WHERE \"roles\" = ? AND "
                 "\"username\" IN (SELECT \"username\" FROM \"billing\" WHERE "
                 "\"status\" LIKE ?)") "admin" "%paid%"]
           (sql subselect-where))))
  (testing "simple column case"
    (is (= [(str "SELECT \"user\".\"username\", \"user\".\"friends\", "
                 "(SELECT COUNT(*) FROM \"friendship\" WHERE \"user\".\"username\" "
                 "= \"friendship\".\"username\" AND \"friendship\".\"status\" = ?) FROM \"user\" "
                 "WHERE \"user\".\"roles\" = ?") "active" "admin"]
           (sql subselect-column))))
  (testing "simple table case"
    (is (= [(str "SELECT * FROM (SELECT \"roles\", COUNT(*) AS \"cnt\" FROM \"user\" "
                 "GROUP BY \"roles\" HAVING COUNT(*) > ?) AS \"summary\" WHERE "
                 "\"roles\" = ?") 2 "admin"]
           (sql subselect-table)))))

(def del
  {:command :delete
   :table :user
   :where [:= :id 2]})

(def del-select
  {:command :delete
   :table :user
   :where [:in :id {:command :select
                    :table :old_user
                    :column :id}]})

(deftest test-delete
  (testing "simple case"
    (is (= ["DELETE FROM \"user\" WHERE \"id\" = ?" 2]
           (sql del))))
  (testing "simple case with subselect"
    (is (= ["DELETE FROM \"user\" WHERE \"id\" IN (SELECT \"id\" FROM \"old_user\")"]
           (sql del-select)))))

(def up
  {:command :update
   :table :user
   :set [[:= :username "user1000"]
         [:= :role "admin"]]
   :where [:= :id 1]})

(def up-subselect
  {:command :update
   :table {:user :u}
   :set [:= :status {:command :select
                     :column :status
                     :table {:old_user :ou}
                     :where [:= :u.id :ou.id]}]
   :where [:in :u.id {:command :select
                      :column :id
                      :table :old_user}]})

(deftest test-update
  (testing "simple case"
    (is (= ["UPDATE \"user\" SET \"username\" = ?, \"role\" = ? WHERE \"id\" = ?" "user1000" "admin" 1]
           (sql up))))
  (testing "subselect case"
    (is (= [(str "UPDATE \"user\" AS \"u\" SET \"status\" = (SELECT \"status\" FROM "
                 "\"old_user\" AS \"ou\" WHERE \"u\".\"id\" = \"ou\".\"id\") WHERE \"u\".\"id\" IN "
                 "(SELECT \"id\" FROM \"old_user\")")]
           (sql up-subselect)))))

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
