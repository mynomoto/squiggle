(defproject squiggle "0.1.0"
  :description "Generates SQL from clojure maps."
  :url "https://github.com/mynomoto/squiggle"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/java.jdbc "0.3.4"]]
  :test-refresh  {:notify-command  ["lein-test"]}
  :profiles {:dev {:dependencies [[com.h2database/h2 "1.4.179"]]}})
