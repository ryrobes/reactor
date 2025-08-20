(defproject reactor "0.1.0-SNAPSHOT"
  :description "A unified reactive state management library for Clojure"
  :url "https://github.com/ryrobes/reactor"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.12.0"] ; Required for XTDB 2.0
                 [org.clojure/core.cache "1.0.225"]
                 [http-kit "2.7.0"]
                 [ring/ring-core "1.10.0"]
                 [ring/ring-defaults "0.4.0"]
                 [compojure "1.7.0"]
                 [cheshire "5.12.0"]
                 ;; XTDB 2.0 dependencies
                 [com.xtdb/xtdb-api "2.0.0"]
                 [com.xtdb/xtdb-core "2.0.0"]
                 [com.github.seancorfield/next.jdbc "1.3.939"] ; Required by XTDB 2.0
                 ;; SQL query support
                 [honeysql "1.0.461"]
                 ;; Logging
                 [org.clojure/tools.logging "1.2.4"]
                 [ch.qos.logback/logback-classic "1.4.11"]
                 ;; Kafka integration for XTDB transaction log
                 [fundingcircle/jackdaw "0.9.12"] ;;  :exclusions [org.apache.kafka/kafka-streams-test-utils]
                 [com.taoensso/nippy "3.3.0"]
                 ;; Async for tests
                 [org.clojure/core.async "1.6.681"]
                 ;; https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams-test-utils
                 [org.apache.kafka/kafka-streams-test-utils "4.0.0"]
                 ;;[pjstadig/humane-test-output "0.11.0"]
                 ;; SQL parsing and manipulation
                 [com.github.jsqlparser/jsqlparser "4.9"]]

  :jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED"
             "-Dio.netty.tryReflectionSetAccessible=true"]
  :plugins [[lein-cloverage "1.0.13"]
            [lein-shell "0.5.0"]
            [lein-eftest "0.6.0"]
            [lein-ancient "0.6.15"]
            [lein-changelog "0.3.2"]]
  :profiles {:dev {:dependencies [[nrepl "1.0.0"]]}}
  :deploy-repositories [["releases" :clojars]]
  :aliases {"update-readme-version" ["shell" "sed" "-i" "s/\\\\[reactor \"[0-9.]*\"\\\\]/[reactor \"${:version}\"]/" "README.md"]}
  :release-tasks [["shell" "git" "diff" "--exit-code"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["changelog" "release"]
                  ["update-readme-version"]
                  ["vcs" "commit"]
                  ["vcs" "tag"]
                  ["deploy"]
                  ["vcs" "push"]])
