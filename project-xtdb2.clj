(defproject reactor-xtdb2 "0.1.0-SNAPSHOT"
  :description "XTDB 2.0 migration test project"
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [com.xtdb/xtdb-api "2.0.0"]
                 [com.xtdb/xtdb-core "2.0.0"]
                 ;; Keep non-XTDB dependencies
                 [http-kit "2.7.0"]
                 [ring/ring-core "1.10.0"]
                 [ring/ring-defaults "0.4.0"]
                 [compojure "1.7.0"]
                 [cheshire "5.12.0"]
                 [honeysql "1.0.461"]
                 [org.clojure/tools.logging "1.2.4"]
                 [ch.qos.logback/logback-classic "1.4.11"]]
  :jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED"
             "-Dio.netty.tryReflectionSetAccessible=true"]
  :main ^:skip-aot reactor.xtdb-v2-test
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})