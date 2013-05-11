(ns your.namespace
  (:require [squiggle.core :as sq]))

; db is a jdbc connection map.
(def db {:classname   "org.h2.Driver"
         :subprotocol "h2"
         :subname     "resources/db/squiggle.db"})

; the most simple command map for select
(def select-users
  {:command :select
   :table :user})

; this gives you the query vector. The first argument is
; the db and the second is the command map.
(sq/sql-gen :h2 select-users)

; and this executes the query vector. First arg is the db,
; second is the connection map and the third is the command
; map
(sq/sql-exec! :h2 db select-users)

; you can create a table with this command map
(def create-user-table
  {:command :create
   :table :user
   :column [[:id :identity :primary-key]
            [:username :varchar :not-null]
            [:password :varchar :not-null]]})

; you can drop a table
(def drop-user-table
  {:command :drop
   :table :user})

; you can create an index
(def create-index
  {:command :create-index
   :table :user
   :column :username})

; insert
(def insert-user
  {:command :insert
   :table :user
   :column [:username :password]
   :value [["admin" "secret"]]})

; delete
(def delete-user
  {:command :delete
   :table :user
   :where [:= :username "admin"]})

; update
(def update-user
  {:command :update
   :table :user
   :set [[:= :username "user1000"]]
   :where [:= :id 1]})
