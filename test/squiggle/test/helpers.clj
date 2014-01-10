(ns squiggle.test.helpers
  (:use clojure.test)
  (:require [squiggle.helpers :as h]
            [squiggle.core :as sq]
            [clojure.java.jdbc :as j]))

(def db-spec {:subprotocol "h2"
              :subname     "mem:helpers_db;DB_CLOSE_DELAY=-1"})

(def schema
  {:user
   {:option {:create-table [:if-not-exists]
             :drop-table [:if-exists]}
    :column-schema
    [[:username :varchar [:not-null :primary-key]]
     [:password :varchar [:not-null]]]
    :primary-key :username
    :order [:username]
    :child [:email :account]
    :foreign-column :user}

   :bank
   {:option {:create-table [:if-not-exists]
             :drop-table [:if-exists]}
    :column-schema
    [[:bank :varchar [:not-null :primary-key]]
     [:id :integer [:not-null]]]
    :primary-key :bank
    :child [:account]
    :foreign-column :bank}

   :account
   {:option {:create-table [:if-not-exists]
             :drop-table [:if-exists]}
    :column-schema
    [[:number :integer [:primary-key :not-null]]
     [:user :varchar [:not-null]]
     [:bank :varchar [:not-null]]]
    :primary-key :number
    :parent {:user :user :bank :bank}
    :child [:history]
    :foreign-column :account}

   :history
   {:option {:create-table [:if-not-exists]
             :drop-table [:if-exists]}
    :column-schema
    [[:transaction :integer [:primary-key :not-null]]
     [:account :integer [:not-null]]]
    :parent {:account :account}
    :primary-key :transaction}

   :email
   {:option {:create-table [:if-not-exists]
             :drop-table [:if-exists]}
    :column-schema
    [[:id :identity [:primary-key]]
     [:user :varchar [:not-null]]
     [:subject :varchar]]
    :order [:subject]
    :parent {:user :user}
    :primary-key :id
    :index [[{:user :user_index} [:if-not-exists :if-exists]]]}})

(def full-schema
  {:schema schema
   :db :h2
   :db-spec db-spec})

(def sql! (partial sq/sql! :h2 db-spec))
(def sql (partial sq/sql :h2))

(defn drop-tables [f]
  (doseq [s (keys schema)]
    (h/drop-table full-schema s))
  (f))

(use-fixtures
 :each drop-tables)

(def iden (keyword "scope_identity()"))

(deftest basic-operations
  (testing "create-all-tables"
    (is (= [[0] [0] [0] [0] [0]]
           (h/create-all-tables full-schema)))
    (is (empty? (h/find-all full-schema :user))))
  (testing "index"
    (is (= [[0]]
           (h/create-index full-schema :email)))
    (is (nil? (h/create-index full-schema :user))))
  (testing "drop-table"
    (is (= [0]
           (h/drop-table full-schema :history)))
    (is (thrown? org.h2.jdbc.JdbcSQLException (h/find-all full-schema :history)))
    (is (= [0]
           (h/create-table full-schema :history))))
  (testing "insert users"
    (is (= [[nil] [nil] [nil]]
           (map #(h/put! full-schema :user nil %)
                [{:username "a" :password "z"}
                 {:username "b" :password "y"}
                 {:username "c" :password "x"}]))))
  (testing "insert emails"
    (is (= [[{iden 1}] [{iden 2}]
            [{iden 3}] [{iden 4}]]
           (map #(h/put! full-schema :email nil %)
                [{:user "a" :subject "s1"}
                 {:user "a" :subject "s2"}
                 {:user "a" :subject "s3"}
                 {:user "c" :subject "s4"}]))))
  (testing "insert banks"
    (is (= [[nil] [nil]]
           (map #(h/put! full-schema :bank nil %)
                [{:bank "k" :id 1}
                 {:bank "l" :id 2}]))))
  (testing "insert accounts"
    (is (= [[nil] [nil] [nil]]
           (map #(h/put! full-schema :account nil %)
                [{:user "b" :number 1 :bank "k"}
                 {:user "b" :number 2 :bank "l"}
                 {:user "c" :number 3 :bank "k"}]))))
  (testing "insert history"
    (is (= [[nil] [nil] [nil]]
           (map #(h/put! full-schema :history nil %)
                [{:account 1 :transaction 1}
                 {:account 3 :transaction 2}
                 {:account 3 :transaction 3}]))))
  (testing "find functions"
    (is (= (h/find-ids full-schema :user "a")
           [{:username "a" :password "z"}]))
    (is (= (h/find-ids full-schema :user ["a" "b"])
           [{:username "a" :password "z"}
            {:username "b" :password "y"}]))
    (is (= (h/find-all full-schema :email)
           [{:user "a" :subject "s1" :id 1}
            {:user "a" :subject "s2" :id 2}
            {:user "a" :subject "s3" :id 3}
            {:user "c" :subject "s4" :id 4}]))
    (is (= (h/find-all full-schema :email
                       :search "%1"
                       :search-column :subject)
           [{:user "a" :subject "s1" :id 1}])))
  (testing "add count"
    (is (= (:records (meta (h/add-count-records full-schema (h/find-all full-schema :email))))
           4)))
  (testing "find child"
    (is (= (h/find-child full-schema :account (h/find-all full-schema :account))
           [[:history [{:account 1, :transaction 1} {:account 3, :transaction 2} {:account 3, :transaction 3}]]])))
  (testing "add children"
    (is (= (h/add-children full-schema :user (h/find-all full-schema :user))
           [{:account nil
             :email [{:subject "s1", :user "a", :id 1}
                     {:subject "s2", :user "a", :id 2}
                     {:subject "s3", :user "a", :id 3}]
             :password "z", :username "a"}
            {:account [{:user "b", :number 1 :bank "k"}
                       {:user "b", :number 2 :bank "l"}]
             :email nil
             :password "y", :username "b"}
            {:account [{:user "c", :number 3 :bank "k"}]
             :email [{:subject "s4", :user "c", :id 4}]
             :password "x", :username "c"}]))
    (is (= (h/add-children full-schema :user (h/find-all full-schema :user) :only [:account])
           [{:account nil
             :password "z" :username "a"}
            {:account [{:user "b", :number 1 :bank "k"}
                       {:user "b", :number 2 :bank "l"}]
             :password "y" :username "b"}
            {:account [{:user "c", :number 3 :bank "k"}]
             :password "x" :username "c"}])))

  (testing "add all children"
    (is (= (h/add-all-children full-schema :user (h/find-all full-schema :user))
           [{:account nil
             :email [{:subject "s1", :user "a", :id 1}
                     {:subject "s2", :user "a", :id 2}
                     {:subject "s3", :user "a", :id 3}]
             :password "z", :username "a"}
            {:account [{:history [{:account 1, :transaction 1}]
                        :user "b", :number 1 :bank "k"}
                       {:history nil, :user "b", :number 2 :bank "l"}]
             :email nil
             :password "y", :username "b"}
            {:account [{:history [{:account 3, :transaction 2}
                                  {:account 3, :transaction 3}]
                        :user "c", :number 3 :bank "k"}]
             :email [{:subject "s4", :user "c", :id 4}]
             :password "x", :username "c"}])))
  (testing "find parent"
    (is (= (h/find-parent full-schema :history (h/find-all full-schema :history))
           [[:account [{:user "b", :number 1 :bank "k"}
                       {:user "c", :number 3 :bank "k"}]]])))
  (testing "add parents"
    (is (= (h/add-parents full-schema :account (h/find-all full-schema :account))
           [{:bank {:id 1, :bank "k"}
             :user {:password "y", :username "b"}
             :number 1}
            {:bank {:id 2, :bank "l"}
             :user {:password "y", :username "b"}
             :number 2}
            {:bank {:id 1, :bank "k"}
             :user {:password "x", :username "c"}
             :number 3}])))
  (testing "add all parents"
    (is (= (h/add-all-parents full-schema :history (h/find-all full-schema :history))
           [{:account {:bank {:id 1, :bank "k"}
                       :user {:password "y", :username "b"}
                       :number 1}
             :transaction 1}
            {:account {:bank {:id 1, :bank "k"}
                        :user {:password "x", :username "c"}
                        :number 3}
             :transaction 2}
            {:account {:bank {:id 1, :bank "k"}
                        :user {:password "x", :username "c"}
                        :number 3}
             :transaction 3}])))
  (testing "delete"
    (is (= (h/delete! full-schema :user "c")
           [1]))))
