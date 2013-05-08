# Squiggle Spec 0.1
Squiggle is defined in terms of command maps.

## Command Map
A command map is a Clojure map containing at least the following keys and
corresponding values.

:command
  (Required, Keyword)
  The command. Must be one of :select, :insert, :update, :delete, :create,
  :drop. Still not implemented: :union, :intersection, :union-all, :alter.

:table
  (Required, {String, Keyword, Map, Vector of Strings, Keywords or Maps})
  A single table: keyword or string of the table name
  :user
  "USER"
  A single table with alias: map with table name as key and alias as value.
  {:user :u}
  Several tables: Vector of single tables.
  [:user {:email :e}]

### :command :create
:create accepts only one table without alias.

:options
  (Optional, Vector of keywords)
  Accepted keywords: :memory, :cached, :temporary, :global-temporary,
  :local-temporary, :temp, :if-not-exists

:column
  (Required, Vector of column definitions)
  A column definition vector is a Clojure vector in the format
  [name type options]. E.g.:
  [:id :integer :primary-key]

  name
  (Required, {String, Keyword})
  The name of the column.

  type
  (Required, {Keyword, String})
  The datatype of the column. E.g. :varchar, "varchar(100)", :identity.

  options
  (Optional, {String, Keyword, Vector of keywords and/or strings})
  Options for the column. It can be:
  * String: "NOT-NULL UNIQUE"
  * Keyword: Accepted keywords: :primary-key, :unique, :null, :not-null
  * Vector of keywords and/or strings: [:not-null "unique"]

### :command :drop
:drop accepts one or more tables without alias.

:options
  (Optional, Vector of keywords)
  Accepted keyworks: :cascade, :restrict, :if-exists

### :command :insert
:insert accepts only one table


### :delete

### :update

### :select
A select command map can also have the following keys and values.

:column


:modifier
  (Optional, {String, Keyword, Modifier map, Vector of modifiers})
  Modifiers to apply to the query. Accepts the following keywords: :distinct,
  :distinct-on and :top. Keyworks that accepts options must be used as a
  key in a map with options as values.
  :distinct, {:top 10}, {:distinct-on [:username :role]}

:where
  (Optional, Vector of Expressions)
  Vector of where expressions. Operators are used in keyword form and vectors
  delimit operations like parentesis in Clojure. E.g.: [:and [:and [:> :a 3]
  [:<= :d 3] [:between :d [:and 2 5]]] [:or [:> :d :a] [:< :a :d]]
  [:in :x [3 4 9]]

:group-by
  (Optional, Vector of Keywords, Keyword, String)
  Columns for grouping.

:having
  (Optional, Vector of Expressions)
  Vector of having expressions. Operators are used in keyword form and vectors
  delimit operations like parentesis in Clojure. E.g.: [:and [:and [:> :a 3]
  [:<= :d 3] [:between :d [:and 2 5]]] [:or [:> :d :a] [:< :a :d]]
  [:in :x [3 4 9]]

:offset
  (Optional, Integer)
  The number of rows to offset.

:limit
  (Optional, Integer)
  The number of rows to limit.

:order-by
  (Optional, Vector of Keywords)
  Columns used to order the result.


## Entity Map
A entity map is a Clojure map containing at least the following keys and
corresponding values.

:table
  (Required, Keyword)
  The name of the table representing the entity.

:columns
  (Optional, Vector)
  A vector of columns maps representing the columns of the entity table.
  Necessary to create the entity table.

:default-select
  (Optional, Select Map)
  A map representing the default select query of the entity.

:pk
  (Optional, Keyword)
  The name of the column that is the primary key of the table.

:relationships
  (Optional, Relationship Map)
  Map containing a relationship keys and values. The key represents
  the name of the entity and the value is a map of relationship
  caracteristics.

## Relationship Characteristics Map
A relationship caracteristics map is a Clojure map containing at least the
following keys and corresponding values.

:type
  (Required, Keyword)
  The type of relationship. Must be one of :has-one, :has-many, :belongs-to
  or :many-to-many.

:fk
  (Optional, Keyword)
  The name of the column that is the foreign key of the another entity table.

:join-table
  (Optional, Keyword)
  The name of the join table used in :many-to-many relationships.

:pk-jt
  (Optional, Keyword)
  The name of the column that corresponds to the primary key in the join table.

:fk-jt
  (Optional, Keyword)
  The name of the column that corresponds to the foreign key in the join table.

