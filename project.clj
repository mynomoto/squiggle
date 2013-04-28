(defproject squiggle "0.1.0-SNAPSHOT"
  :description "Generates SQL from clojure maps."
  :url "https://github.com/mynomoto/squiggle"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/java.jdbc "0.3.0-alpha1"]]
  :profiles {:dev {:dependencies [[com.h2database/h2 "1.3.170"]
                                  [org.hsqldb/hsqldb "2.2.9"]
                                  [org.xerial/sqlite-jdbc "3.7.2"]
                                  [org.apache.derby/derby "10.9.1.0"]]}})
