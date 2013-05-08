# Squiggle

A Clojure library designed to transform maps in well crafted SQL code.
It has a spec.

## Why???

I like to see what I'm doing. It's just a plain map so it's harder for
anything to hide from you.

Squiggle uses the new clojure.java.jdbc API.

So this is not Korma. It's more like honeysql but I only found that later
and Squiggle already works for create and drop tables, insert, update and
delete statements.

And finally, the idea is have something that can generate sql optimized
for each db. Now it only tested in H2 but should partially work with Derby,
HSQL, Mysql, Postgresql. I will need help from people that use other dbs to
add specific characteristics.

## Usage

Add to your dependencies `[squiggle "0.1.0-SNAPSHOT"]`

Require the namespace `squiggle.core`.

There are only two public functions. `sql-gen` and `sql-exec!`.

```clojure
(ns your.db.config
  (:require [squiggle.core :as sq]))

; db is a jdbc connection map.
(def db {:classname   "org.h2.Driver"
         :subprotocol "h2"
         :subname     "resources/db/squiggle.db"})

; the most simple command map for select is:
(def select-users
  {:command :select
   :table :user})

; this gives you the query vector
(sq/sql-gen :h2 select-users)

; and this executes the query vector
(sq/sql-exec! :h2 db select-users)

; you can create a table with this command map
(def create-user-table
  {:command :create
   :table :user
   :column [[:id :identity :primary-key]
            [:username :varchar :not-null]
            [:password :varchar :not-null]]}

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
   :values [["admin" "secret"]]})

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
```

## License

Copyright © 2013 Marcelo Nomoto

Licensed under the EPL, the same as Clojure (see the file epl-v10.html).
