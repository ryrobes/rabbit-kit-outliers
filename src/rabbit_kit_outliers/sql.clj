(ns rabbit-kit-outliers.sql
  (:require [puget.printer :as puget]
            [hikari-cp.core :as hik]
            [clojure.string :as cstr]
            [clojure.walk :as walk]
            [honey.sql :as honey]
            [clojure.java.jdbc :as jdbc]))

(def num-cores (.availableProcessors (Runtime/getRuntime)))

(def theme-data-type-colors {:float "#bdbae5"
                             :universal-pop-color "#c8a9a4"
                             :boolean "#c8a4a4"
                             :map "#9ecc96"
                             :list "#6b5d9c"
                             :string "#f8f5f3"
                             :any "#b56365"
                             :vector "#9193c6"
                             :keyword "#f2e7ec"
                             :rabbit-code "#60b7bd"
                             :datetime "#3e30a5"
                             :integer "#ccaeae"
                             :unknown "#752c64"
                             :date "#10290a"})
(def color-map
  (assoc (walk/postwalk-replace
          {:integer :number
           :universal-pop-color :delimiter
           :rabbit-code :function-symbol}
          (into {} (for [[k v] theme-data-type-colors]
                     {k (if (or (= k :universal-pop-color)
                                (= k :keyword)
                                (= k :nil))
                          [:bold v]
                          [v])}))) :tag [(get theme-data-type-colors :universal-pop-color)]))

(defn pp [x & [opts-map]]
  (puget/with-options 
    (merge {:width 60 
            :color-scheme color-map} opts-map)
    (puget/cprint x)))

(defn deep-flatten [x]
  (if (coll? x)
    (mapcat deep-flatten x)
    [x]))

(defn unkeyword [x]
  (if (keyword? x)
    (-> (str x)
        (cstr/replace #":" "")
        (cstr/replace #"-" "_"))
    (str x)))

(defn pool-create [jdbc-body name]
  (delay (let [base {;:jdbc-url "jdbc:sqlite:db/system.db"
                     ;:minimum-idle 10 ;; min iodle connections to keep open?
                     ;:cache "shared"
                     :idle-timeout       600000
                     :max-lifetime       1800000
                     :pool-name name ;"system-db-pool"
                     ;:metric-registry instrument/metric-registry
                     :maximum-pool-size (* 1 num-cores) ;8 ;100
                     }
               bbase (merge base jdbc-body)]
           (pp [:creating-conn-pool name])
           (hik/make-datasource bbase))))

;; (def system-db {:datasource @(pool-create {:jdbc-url "jdbc:sqlite:file:systemdb22?mode=memory&cache=shared&transaction_mode=IMMEDIATE&journal_mode=WAL" ; "jdbc:sqlite:db/system.db"
;;                                            ;:idle-timeout        600000  ;;;; LAST KNOW GOOD SQLITE 10/25/23  - fastest?
;;                                            ;:max-lifetime       1800000
;;                                            ;:minimum-idle 12
;;                                            ;:maximum-pool-size 200
;;                                            :transaction_mode "IMMEDIATE"
;;                                            :journal_mode "WAL"
;;                                            :cache "shared"} "outliers-db")})

(def system-db {:datasource @(pool-create
                              {:jdbc-url "jdbc:duckdb::memory:outliers-db"
                               :idle-timeout      600000
                               :maximum-pool-size 200
                               :minimum-idle      12
                               :max-lifetime      1800000}
                              "outliers-db")})

;; (def system-db {:datasource @(pool-create {:jdbc-url "jdbc:h2:mem:cachedb;DATABASE_TO_UPPER=FALSE" ; h2 
;;                                            ;:jdbc-url "jdbc:hsqldb:mem:cachedb" ; hypersql  ;;"jdbc:duckdb:db/cache.duck" IGNORECASE=TRUE; 
;;                                            :idle-timeout       600000
;;                                            :max-lifetime       1800000} "outliers-db")})

;; (def system-db {:datasource @(pool-create {:jdbc-url "jdbc:clickhouse://10.174.1.150:8123/rabbit_cache"
;;                                           :username "rabbit_cache"
;;                                           :password "notofox"
;;                                           :driver-class-name "ru.yandex.clickhouse.ClickHouseDriver"
;;                                           :connection-timeout 5000
;;                                           ;:maximum-pool-size 20
;;                                           :max-lifetime 300000} "cache-db-pool")})

(defn sql-exec [db-spec query & [extra]]
  (jdbc/with-db-transaction
    [t-con db-spec {:isolation :read-uncommitted}]
    (try
      (jdbc/db-do-commands t-con query)
      (catch Exception e
        (do (pp (merge {:sql-exec-fn-error e :db-spec db-spec :query query}
                       (if extra {:ex extra} {})))
            ;(insert-error-row! db-spec query e)
            )))))

(defn to-sql [honey-map & [key-vector system-db]]
  (let [sql-dialect (when key-vector
                      (cond (cstr/includes? (cstr/lower-case (first key-vector)) "microsoft") :sqlserver
                            (cstr/includes? (cstr/lower-case (first key-vector)) "mysql") :mysql
                            (cstr/includes? (cstr/lower-case (first key-vector)) "oracle") :oracle
                            :else nil))
       ;; key-farm (deep-flatten honey-map)
        ;value-keys? (true? (some #(or (cstr/starts-with? (str %) ":test-value/") 
        ;                       (cstr/starts-with? (str %) ":attribute-value/")) key-farm))
        ;; value-keys (filter #(or (cstr/starts-with? (str %) ":tests/")
        ;;                         (cstr/starts-with? (str %) ":sample/")
        ;;                         (cstr/starts-with? (str %) ":attributes/")) key-farm)
        ;; value-keys? #_{:clj-kondo/ignore [:not-empty?]}
        ;; (and (not (empty? value-keys))
        ;;      (not (nil? key-vector))
        ;;      (not (nil? system-db)))
        value-mapping {} ;; removed
        hm (walk/postwalk-replace value-mapping honey-map) ; (walk/postwalk {} honey-map)
        fixed-honey-map (cond (and (= sql-dialect :sqlserver) (get hm :limit))
                              (let [limit (get hm :limit)
                                    select-distinct (get hm :select-distinct)
                                    select (get hm :select)]
                                (cond select-distinct
                                      (dissoc (dissoc (assoc hm
                                                             :select-distinct-top (vec (cons [limit] select-distinct)))
                                                      :select-distinct) :limit)
                                      select
                                      (dissoc (dissoc (assoc hm
                                                             :select-top (vec (cons [limit] select)))
                                                      :select) :limit)
                                      :else ["sqlserver top issue"]))

                              (and (= sql-dialect :oracle) (get hm :limit))

                              (let [limit (get hm :limit)]
                                (dissoc (assoc hm :fetch limit) :limit))

                              :else hm)]
    ;(when (cstr/includes? (str fixed-honey-map) "derived") (ut/pp fixed-honey-map))
    (try (-> fixed-honey-map (honey/format (merge {:pretty true :inline true}
                                                  (if (nil? sql-dialect) {}
                                                      {:dialect sql-dialect}))))
         (catch Exception e
           (do (pp [:honey-format-err (str e) :input honey-map])
               ;(insert-error-row! :honey-format honey-map e)
               ;[{:sql-parse-error (str e)}]
               ;;(str "select '" "' as error, 'sql parse error' as type ")
               (to-sql {:union-all
                        [{:select [[(str (.getMessage e)) :query_error]]}
                         {:select [["(from HoneySQL formatter)" :query_error]]}]}))))))

(defn create-attribute-sample [sample-table-name-str rowset meta-map]
  (try
    (let [field-types (cstr/join ""
                                 (for [[k v] (first rowset)]
                                   (let [type (cond
                                                ;;(and (string? v) (< (get-in meta-map [:fields k :distinct]) 100)) "enum"
                                                (string? v) "varchar" ;"text"
                                                (integer? v) "integer" ;;"bigint" ; "integer" ;; "bigint" ;;integer"
                                                (float? v) "float"
                                                (cstr/includes? (str (type v)) "DateTime") "timestamp"
                                                (cstr/includes? (str (type v)) "Date") "date"
                                                :else "varchar")
                                         kk (unkeyword k)]
                                     (str kk " " type " NULL,"))))]
      ;(ut/pp [:debug field-types (keys (first rowset))])
      (str " -- drop table if exists " sample-table-name-str ";
           create table if not exists " sample-table-name-str "
  (" (cstr/join "" (drop-last field-types)) ") ;")) ;;h2 opt: ENGINE \"COLUMNAR \"
    (catch Exception e (pp [:error-creating-ddl-for-sample-based-on-rowset sample-table-name-str
                            :error (str e)
                            :rowset rowset]))))

(defn insert-rowset [rowset table-name meta-map & columns-vec]
  (if (not (empty? rowset))
    (try
      (let [rowset-type (cond (and (map? (first rowset)) (vector? rowset)) :rowset
                              (and (not (map? (first rowset))) (vector? rowset)) :vectors)
            columns-vec-arg (first columns-vec)
            db-conn system-db
            rowset-fixed (if (= rowset-type :vectors)
                           ;(vec 
                           (for [r rowset]
                             (zipmap columns-vec-arg r))
                           ; )
                           rowset)
            columns (keys (first rowset-fixed))

            table-name-str (unkeyword table-name)
            ddl-str (create-attribute-sample table-name-str rowset-fixed meta-map)

            extra [ddl-str columns-vec-arg table-name table-name-str]]
        (sql-exec db-conn (str "drop table if exists " table-name-str " ; ") extra)
        (sql-exec db-conn ddl-str extra)
        (doseq [batch (partition-all 100 rowset-fixed)
                :let [values (vec (for [r batch] (vals r)))
                      insert-sql (to-sql {:insert-into [table-name]
                                          :columns columns
                                          :values values})]]
          (sql-exec db-conn insert-sql extra))

        (pp [:INSERTED-SUCCESS! (count rowset) :into table-name-str])
        {:sql-cache-table table-name :rows (count rowset)})
      (catch Exception e (pp [:INSERT-ERROR! (str e) table-name])))
    (pp [:cowardly-wont-insert-empty-rowset table-name :puttem-up-puttem-up!])))

(defn- asort [m order]
  (let [order-map (apply hash-map (interleave order (range)))]
    (conj
     (sorted-map-by #(compare (order-map %1) (order-map %2))) ; empty map with the desired ordering
     (select-keys m order))))

(defonce map-orders (atom {}))

(defn- wrap-maps [query arrays]
  (let [headers (first arrays)
        mm (vec (doall (for [r (rest arrays)]
                         (asort
                          (into {} ;(sorted-map)
                                (for [i (map vector headers r)]
                                  {(first i) (last i)}))
                          headers))))]
    (swap! map-orders assoc query headers)
    mm))

(defn sql-query [db-spec query & extra]
  (jdbc/with-db-connection ;; for connection hygiene - atomic query
    [t-con db-spec]
    ;(pp [query db-spec])
    (try
      ;(vec 
      (wrap-maps query
                 (jdbc/query t-con query {:identifiers keyword
                                          :as-arrays? true
                                    ;:row-fn #(zipmap (first %) (rest %))
                                          :result-set-fn doall}))
     ;  )
      (catch Exception e
        (do (pp {:sql-query-fn-error e :query query :extra (when extra extra)})
            ;(insert-error-row! db-spec query e)
            [;{:query_error (str (.getType e))}
             {:query_error (str (.getMessage e))}
             {:query_error "(from database connection)"}])))))