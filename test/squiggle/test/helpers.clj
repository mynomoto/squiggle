(ns squiggle.test.helpers
  (:use clojure.test)
  (:require [squiggle.helpers :as h]
            [squiggle.core :as sq]))

(def db {:subprotocol "h2"
         :subname     "mem:helpers_db;DB_CLOSE_DELAY=-1"})

(def sql! (partial sq/sql! :h2 db))
(def sql (partial sq/sql :h2))

(def schema
  {:user
   {:table :user
    :option {:create-table [:if-not-exists]
             :drop-table [:if-exists]}
    :column-schema
    [[:username :varchar [:not-null :primary-key]]
     [:password :varchar [:not-null]]]
    :primary-key :username
    :order [:username]
    :child [:email :account]
    :foreign-column :user}

   :bank
   {:table :bank
    :option {:create-table [:if-not-exists]
             :drop-table [:if-exists]}
    :column-schema
    [[:bank :varchar [:not-null :primary-key]]
     [:id :integer [:not-null]]]
    :primary-key :bank
    :child [:account]
    :foreign-column :bank}

   :account
   {:table :account
    :option {:create-table [:if-not-exists]
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
   {:table :history
    :option {:create-table [:if-not-exists]
             :drop-table [:if-exists]}
    :column-schema
    [[:transaction :integer [:primary-key :not-null]]
     [:account :integer [:not-null]]]
    :parent {:account :account}
    :primary-key :transaction}

   :email
   {:table :email
    :option {:create-table [:if-not-exists]
             :drop-table [:if-exists]}
    :column-schema
    [[:id :identity [:primary-key]]
     [:user :varchar [:not-null]]
     [:subject :varchar]]
    :order [:subject]
    :parent {:user :user}
    :primary-key :id}})

(defn drop-tables [f]
  (doseq [s (vals schema)]
    (h/drop-table sql! s))
  (f))

(use-fixtures
 :each drop-tables)

(deftest basic-operations
  (testing "create-tables"
    (is (= [[0] [0] [0] [0] [0]] (map #(h/create-table sql! %) (vals schema))))
    (is (empty? (h/find-all sql! (:user schema)))))
  (testing "insert users"
    (is (= [[1] [1] [1]]
           (map #(h/put! sql! (:user schema) nil %)
                [{:username "a" :password "z"}
                 {:username "b" :password "y"}
                 {:username "c" :password "x"}]))))
  (testing "insert emails"
    (is (= [[1] [1] [1] [1]]
           (map #(h/put! sql! (:email schema) nil %)
                [{:user "a" :subject "s1"}
                 {:user "a" :subject "s2"}
                 {:user "a" :subject "s3"}
                 {:user "c" :subject "s4"}]))))
  (testing "insert banks"
    (is (= [[1] [1]]
           (map #(h/put! sql! (:bank schema) nil %)
                [{:bank "k" :id 1}
                 {:bank "l" :id 2}]))))
  (testing "insert accounts"
    (is (= [[1] [1] [1]]
           (map #(h/put! sql! (:account schema) nil %)
                [{:user "b" :number 1 :bank "k"}
                 {:user "b" :number 2 :bank "l"}
                 {:user "c" :number 3 :bank "k"}]))))
  (testing "insert history"
    (is (= [[1] [1] [1]]
           (map #(h/put! sql! (:history schema) nil %)
                [{:account 1 :transaction 1}
                 {:account 3 :transaction 2}
                 {:account 3 :transaction 3}]))))
  (testing "find functions"
    (is (= (h/find-ids sql! (:user schema) "a")
           [{:username "a" :password "z"}]))
    (is (= (h/find-ids sql! (:user schema) ["a" "b"])
           [{:username "a" :password "z"}
            {:username "b" :password "y"}]))
    (is (= (h/find-all sql! (:email schema))
           [{:user "a" :subject "s1" :id 1}
            {:user "a" :subject "s2" :id 2}
            {:user "a" :subject "s3" :id 3}
            {:user "c" :subject "s4" :id 4}]))
    (is (= (h/find-like sql! (:email schema) "%1" :column :subject)
           [{:user "a" :subject "s1" :id 1}])))
  (testing "add count"
    (is (= (:count (meta (h/add-count sql! (h/find-all sql! (:email schema)))))
           4)))
  (testing "find child"
    (is (= (h/find-child sql! schema :account (h/find-all sql! (:account schema)))
           [[:history [{:account 1, :transaction 1} {:account 3, :transaction 2} {:account 3, :transaction 3}]]])))
  (testing "add children"
    (is (= (h/add-children sql! schema :user (h/find-all sql! (:user schema)))
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
    (is (= (h/add-children sql! schema :user (h/find-all sql! (:user schema)) :only [:account])
           [{:account nil
             :password "z" :username "a"}
            {:account [{:user "b", :number 1 :bank "k"}
                       {:user "b", :number 2 :bank "l"}]
             :password "y" :username "b"}
            {:account [{:user "c", :number 3 :bank "k"}]
             :password "x" :username "c"}])))

  (testing "add all children"
    (is (= (h/add-all-children sql! schema :user (h/find-all sql! (:user schema)))
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
    (is (= (h/find-parent sql! schema :history (h/find-all sql! (:history schema)))
           [[:account [{:user "b", :number 1 :bank "k"}
                       {:user "c", :number 3 :bank "k"}]]])))
  (testing "add parents"
    (is (= (h/add-parents sql! schema :account (h/find-all sql! (:account schema)))
           [{:bank [{:id 1, :bank "k"}]
             :user [{:password "y", :username "b"}]
             :number 1}
            {:bank [{:id 2, :bank "l"}]
             :user [{:password "y", :username "b"}]
             :number 2}
            {:bank [{:id 1, :bank "k"}]
             :user [{:password "x", :username "c"}]
             :number 3}])))
  (testing "add all parents"
    (is (= (h/add-all-parents sql! schema :history (h/find-all sql! (:history schema)))
           [{:account [{:bank [{:id 1, :bank "k"}]
                        :user [{:password "y", :username "b"}]
                        :number 1}]
             :transaction 1}
            {:account [{:bank [{:id 1, :bank "k"}]
                        :user [{:password "x", :username "c"}]
                        :number 3}]
             :transaction 2}
            {:account [{:bank [{:id 1, :bank "k"}]
                        :user [{:password "x", :username "c"}]
                        :number 3}]
             :transaction 3}])))
  )
