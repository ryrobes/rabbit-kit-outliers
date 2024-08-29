(defproject rabbit-kit-outliers "0.1.0-SNAPSHOT"
  :description "An example RVBBIT 'kit' for SQL dimensional outliers"
  :url "http://github/ryrobes/rabbit-kit-outliers"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"] ;""1.9.0"] "1.10.3"
                 [clj-time "0.15.2"]
                 ;;[mvxcvi/puget "1.3.2"]
                 [com.ryrobes/puget "1.3.5-SNAPSHOT"]
                 [com.github.seancorfield/honeysql "2.2.868"]
                 [org.xerial/sqlite-jdbc "3.36.0.3"]
                 [org.hsqldb/hsqldb "2.7.1"]
                 [com.h2database/h2 "2.1.214"]
                 [ru.yandex.clickhouse/clickhouse-jdbc "0.3.1"]
                 [com.cognitect/transit-clj "1.0.333"]
                 [org.duckdb/duckdb_jdbc "1.0.0"] 
                 ;[duratom "0.5.8"]
                 [durable-atom "0.0.3"]
               ;  [com.ryrobes/flowmaps "0.31-SNAPSHOT"]
                 [hikari-cp "3.0.1"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [org.clojure/math.combinatorics "0.1.6"]
                 [org.clojure/tools.nrepl "0.2.13"]
                 [nrepl/nrepl "0.9.0"]
                 ;[incanter/incanter-core "1.9.3"]
                 ;[incanter/incanter-svg "1.9.3"]
                 ;[incanter/incanter-charts "1.9.3"]
                 ]
  :main ^:skip-aot rabbit-kit-outliers.core
  :repl-options {:port 45999
                 :nrepl-middleware [nrepl.middleware.session/add-stdin]
                 :init-ns rabbit-kit-outliers.core}
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
