(ns rabbit-kit-outliers.core
  (:require
   [cognitect.transit  :as transit]
   [clojure.string :as cstr]
   [clojure.set :as cset]
   [clojure.walk :as walk]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [nrepl.server :as nrepl-server]
   [rabbit-kit-outliers.sql :refer [sql-query sql-exec system-db unkeyword
                                    to-sql insert-rowset num-cores pool-create] :as sql]
   [clojure.math.combinatorics :as combo]
   [clojure.core.reducers :as r])
  (:import ;java.text.SimpleDateFormat
           ;java.util.Date
   [java.nio.file Paths]
   [java.util.concurrent Executors Callable])
  (:gen-class))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; RabbitSQL:: woodshed quality 'Multivariate Outlier Detection' - plug-in KIT example ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; december 8 2023, v0

(def min-threads num-cores)  ; Minimum number of threads
(def max-threads (* 24 num-cores))  ; Maximum number of threads
;;(def table-name (keyword (str "table" (rand-int 12345))))

(defn pmap+ [f coll] ;;; more threads than pmap, 12 out of 10 dentists agree!
  (let [n-threads (max min-threads (min max-threads (count (vec coll)))) ; (count (vec coll)) ;(min 4 (count (vec coll)))
        _ (sql/pp (str "Exec Pool using " n-threads " threads (" num-cores  " cores / " (count (vec coll)) " items)"))
        executor (Executors/newFixedThreadPool n-threads)
        tasks (map (fn [item]
                     (reify Callable
                       (call [_] (f item)))) coll)]
    (try
      (mapv (fn [future] (.get future)) (.invokeAll executor tasks))
      (finally
        (.shutdown executor)))))

(defn nf [num]
  (cstr/trim
   (if (float? num)
     (str (format "%,12.2f" num))
     (str (format "%,12d" num)))))

(defn fraction? [n]
  (and (rational? n) (not (integer? n))))

(defn rnd [number scale]
  (let [number (if (fraction? number) (double number) number) ; Convert fractions to decimal
        rounding-mode java.math.RoundingMode/HALF_UP
        big-decimal (BigDecimal. (str number))
        rounded (.setScale big-decimal scale rounding-mode)
        rounded-value (if (= 0 scale)
                        (.longValue rounded)
                        (.doubleValue rounded))]
    (if (integer? number)
      number
      (if (and (not (integer? number)) (zero? (mod rounded-value 1.0)))
        (.intValue rounded)
        rounded-value))))

;; (defn parse-date [date-string]
;;   (let [format (SimpleDateFormat. "yyyy-MM-dd")]
;;     (.parse format date-string)))

(defn read-transit-data
  [file-path]
  (let [abs-filepath (.toString (.toAbsolutePath (Paths/get file-path (into-array String []))))]
    (if (.exists (io/file abs-filepath))
      (with-open [in (io/input-stream abs-filepath)]
        (transit/read (transit/reader in :msgpack)))
      (throw (ex-info "File not found" {:file abs-filepath})))))

;parallel versions w r/fold ? 
(defn mean [values]
  (let [sum (r/fold + values)
        cnt (count values)]
    (/ sum cnt)))

(defn standard-deviation [values]
  (let [cnt (count values)
        avg (mean values)
        sum-sq (r/fold + (map #(Math/pow (- % avg) 2) values))]
    (Math/sqrt (/ sum-sq cnt))))

(defn detect-outliers [values] ;; old
  (try ;; issues with receiving a single value. doesnt matter cause we cant calc outliers on a single value. lol
    (let [mean-val (mean values)
          sd (standard-deviation values)
          lower-bound (- mean-val (* 2 sd))
          upper-bound (+ mean-val (* 2 sd))]
      {:low (filter #(< % lower-bound) values)
       :sd sd
       :mean mean-val
       :sample-size (count values)
       :high (filter #(> % upper-bound) values)})
    (catch Exception e
      (println "Error during sd outlier detection for values:" values "Error:" e)
      nil)))

(defn detect-outliers2 [values] ;; z-score fn 
  (let [avg (mean values)
        sd (standard-deviation values)]
    ;(sql/pp [avg sd])
    {:low (filter #(< % (- avg (* 2 sd))) values)
     :sd sd :mean avg :sample-size (count values)
     :high (filter #(> % (+ avg (* 2 sd))) values)}))

(defn percentile [p values]
  (try 
    (let [sorted-values (sort values)
        n (dec (count sorted-values))
        pos (+ (* p n) 1)
        fpos (Math/floor pos)
        intpos (int fpos)
        has-remainder (not (zero? (- pos fpos)))]
    (if has-remainder
      (+ (nth sorted-values (dec intpos))
         (* (- pos fpos)
            (- (nth sorted-values intpos)
               (nth sorted-values (dec intpos)))))
      (nth sorted-values (dec intpos))))
    (catch Exception e
      (println "Error during percentile calculation for values:" values "Error:" e)
      nil)))

(defn detect-outliers3 [values] ;; percentile
  (try
    (let [lp 0.01
          hp 0.99
          lower-bound (percentile lp values)
          upper-bound (percentile hp values)
          avg (mean values)
          sd (standard-deviation values)]
      {:low (filter #(< % lower-bound) values)
       :sd sd :mean avg :sample-size (count values)
       :percentiles [lp hp]
       :high (filter #(> % upper-bound) values)})
    (catch Exception e
      (println "Error during pctl outlier detection for values:" values "Error:" e)
      nil)))

(defn detect-outliers-multi [values]
  (if true ;(> (count values) 50)
    (detect-outliers3 values)  ;; Use percentile method for large datasets only?
    ;; (let [avg (mean values)
    ;;       sd (standard-deviation values)]
    ;;   {:low (filter #(< % (- avg (* 3 sd))) values)  ;; Use 3 standard deviations for small datasets
    ;;    :high (filter #(> % (+ avg (* 3 sd))) values)})
    nil))

(defn analyze-field [dataset field table-name]
  (sql/pp [:working-on-agg field])
  (let [values (pmap :v (sql-query system-db
                                   (to-sql {:select [[field :v]]
                                            :from [table-name]})))
        outliers (detect-outliers-multi values)]
    (when (or (seq (get outliers :low))
              (seq (get outliers :high)))
      {:fields [field]
       :sd (get outliers :sd)
       :mean (get outliers :mean)
       :sample-size (get outliers :sample-size)
       :percentiles (get outliers :percentiles "N/A")
       :low-outliers (get outliers :low)
       :high-outliers (get outliers :high)})))

(defn analyze-combined-fields [dataset fields agg-field table-name]
  (sql/pp [:building-group-by (vec fields) :agg agg-field])
  (try
    (let [values (pmap :v (sql-query system-db
                                     (to-sql {:select [[[:sum agg-field] :v]]
                                              :from [table-name] :group-by fields})))
          outliers (try (detect-outliers-multi values) (catch Exception e (do (println "*" e) {})))]
      (when (try (or
                  (seq (get outliers :low))
                  (seq (get outliers :high)))
                 (catch Exception _ false))
        {:fields fields
         :sd (get outliers :sd)
         :mean (get outliers :mean)
         :sample-size (get outliers :sample-size)
         :percentiles (get outliers :percentiles "N/A")
         :low-outliers (get outliers :low)
         :high-outliers (get outliers :high)}))
    (catch Exception e (do ; (println (str "Error during analysis for fields:" fields "Error:" e))
                         {:err (str e)
                          :fields fields}))))

(defn group-and-aggregate [data fields agg-field table-name]
  (let [out (pmap (fn [r] [(get r :v) (hash r) (select-keys r fields)])
                  (sql-query system-db (to-sql {:select (into (vec fields) [[[:sum agg-field] :v]])
                                                :from [table-name] :group-by fields})))]
    (when (= fields [agg-field]) (println "BASE outlier found**"))
    out))

(defn analyze-dataset [dataset fields agg-field rowset max-combos table-name & [_]]
  (let [old-max-combos (inc (count fields))
        field-combos (apply concat (map #(combo/combinations fields %) (range 1 max-combos)))
        field-combos (vec (map sort field-combos))
        output {agg-field (vec (remove nil?
                                       (concat
                                        (analyze-field dataset agg-field table-name)
                                        (pmap+ #(analyze-combined-fields dataset % agg-field table-name)
                                               field-combos))))}
        outlier-vec (get output agg-field)
        cnter (atom 1)
        enhanced-map (pmap (fn [{:keys [fields high-outliers sd mean percentiles sample-size low-outliers]}]
                             (when (not (nil? mean))
                               (let [ttl (count outlier-vec)
                                     _ (sql/pp [:analyzing-group-by (vec fields)
                                                [@cnter :of ttl]
                                                {:sample-size sample-size :mean mean :sd sd}])
                                     _ (swap! cnter inc)
                                     grouped (group-and-aggregate rowset (vec fields) agg-field table-name)
                                     field-values-high
                                     (into {} (for [[k v]
                                                    (group-by first
                                                              (for [[k h v] grouped
                                                                    :when (some #(= % k) high-outliers)] [k h v]))]
                                                {k (vec
                                                    (map last v))}))
                                     field-values-low
                                     (into {} (for [[k v]
                                                    (group-by first
                                                              (for [[k h v] grouped
                                                                    :when (some #(= % k) low-outliers)] [k h v]))]
                                                {k (vec
                                                    (map last v))}))]
                                 {:fields (vec fields)
                                  :mean (float mean)
                                  :sd sd
                                  :percentiles percentiles
                                  :sample-size sample-size
                                  :low-outliers (vec (distinct low-outliers))
                                  :high-outliers (vec (distinct high-outliers))
                                  :low-outliers-num (count low-outliers)
                                  :high-outliers-num (count high-outliers)
                                  :outlier-details-high field-values-high
                                  :outlier-details-low field-values-low})))
                           outlier-vec)]
    {agg-field (vec (remove nil? enhanced-map))}))


(defn merge-and-sort-outliers [outliers]
  (let [_ (sql/pp [:running-merge-and-sort-outliers])
        merge-high-low (fn [outlier]
                         (let [high (get outlier :outlier-details-high)
                               low (get outlier :outlier-details-low)
                               mean (get outlier :mean)]
                           (merge-with concat high low)))
        calculate-diff (fn [[abs-val details] mean]
                         [[abs-val (Math/abs (- mean abs-val))] details])
        sort-by-diff (fn [outlier]
                       (let [mean (get outlier :mean)
                             details (merge-high-low outlier)]
                         (into (sorted-map-by (fn [[k1 _] [k2 _]] (compare k1 k2)))
                               (map #(calculate-diff % mean) details))))]
    (apply concat (into [] (map sort-by-diff outliers)))))

(defn create-top-x-output [top-x agg-field outliers panel-name query-ref-name query-name query-meta]
  (let [xx 30
        top-xx (vec (reverse (take-last xx top-x)))
        xx-d (count top-xx) ;; in case it is lower...
        top-key (keyword (str "top-outliers"))
        connection-id (get query-meta :connection-id "system-db")
        outliers-source-map (into {} (group-by :fields (get outliers agg-field)))
        ;story-key :storykey0 ;(keyword (str "storykey" (rand-int 1234)))
        _ (sql/pp [:creating-kit-story-map-for-top xx])
        story-desc (str "Top " xx-d " Outlier by Severity")]
    ;(sql/pp outliers-source-map)

    {top-key ;; a "tab" in the kit panel essentially
     {:description story-desc
      :options {:pages? true :search? false :actions? false}
      :parameters {} ;; base params that mutate on exec, not render. step mutations insert on render
      :mutates {} ;; a base mutation and will take place before any UI can get in the way... dangerous, but needed for calliope etc
      :data (vec (for [idx (range (count top-xx))
                       :let [[[val abs-diff fields] wheres] (get top-xx idx)
                             diff-val-str (nf (rnd (Math/floor abs-diff) 2)) ;; (cstr/replace (str (Math/floor abs-diff)) ".0" "")
                             tmean (get-in outliers-source-map [fields 0 :mean])
                             hash-wheres (hash wheres)
                             query-id-base (str "kit-query-" hash-wheres)
                             stats-map {:mean tmean
                                        :standard-deviation (get-in outliers-source-map [fields 0 :sd])
                                        :sample-size (get-in outliers-source-map [fields 0 :sample-size])
                                        :fields fields
                                        :calc-used [:percentiles (get-in outliers-source-map [fields 0 :percentiles])]}]]
                   {:name (let [std-devs-away (/ abs-diff (get-in outliers-source-map [fields 0 :sd]))
                                percentile-used (first (get-in outliers-source-map [fields 0 :percentiles]))]
                            (str (+ 1 idx) ". When grouping by " (cstr/join ", " (map #(str (cstr/lower-case (name %))) fields))
                                 " and aggregating by " agg-field ", there are values that are significantly "
                                 (if (> val tmean) "ABOVE" "BELOW") " the mean. "
                                 "These values deviate by " (format "%.2f" std-devs-away) " standard deviations, "
                                 "with an absolute difference of " diff-val-str " from the mean. "
                                 "This outlier was identified using the " (format "%.2f" (* 100 percentile-used)) "th percentile threshold."))
                    ;; :name (str (+ 1 idx) ". " "When grouping by " (cstr/join ", " (map #(str (cstr/lower-case (name %))) fields)) " and aggregating by "
                    ;;            agg-field " there are values that are " 
                    ;;            (if (> val tmean) "HIGHER" "LOWER") ;" " (str abs-diff)
                    ;;            " than the mean "
                    ;;            ;"(" (cstr/replace (str (Math/floor tmean)) ".0" "")
                    ;;            ;" for this particular aggregation)" 
                    ;;            " by " diff-val-str ".")

                    :highlight-columns {query-name (vec fields)}
                    :parameters {}
                    :step-mutates {}
                    :ask-mutates {"Highlight these in your source query?"
                                  {[:panels panel-name :queries query-name :style-rules [:* (keyword (str "higlight" hash-wheres))]]
                                   {:logic (let [cc (for [w wheres] (into [:and] (vec (for [[kk vv] w] [:= kk vv]))))]
                                             (if (> (count wheres) 1) (into [:and] cc) (first cc)))
                                    :style {:background-color "#008b8b66"
                                            :border "1px solid #00000000"}}}}
                    :content (vec (into [[:v-box
                                          :size "auto"
                                          :width "490px"
                                          :style {:font-size "13px"
                                                  :opacity 0.33}
                                          :children (vec (for [[k v] stats-map] [:h-box
                                                                                 :size "auto"
                                                                                 :justify :between
                                                                                 :children [[:box :child (str k)]
                                                                                            [:box :child (str v) :style {:font-weight 400}]]]))]]

                                        (apply concat (for [w wheres
                                                            :let [query-id (str query-id-base (try (.indexOf wheres w) (catch Exception _ 0)))]]
                                                        [[:v-box
                                                          :size "auto" :width "490px" :style {:font-size "16px"}
                                                          :children
                                                          (vec (for [[k v] w] [:h-box
                                                                               :size "auto"
                                                                               :justify :between
                                                                               :children [[:box :child (str k)]
                                                                                          [:box :child (pr-str v) :style {:font-weight 400}]]]))]

                                                        ; (str (nf (get-in outliers-source-map [fields 0 :mean])))
                                                        ; (if (> val tmean) "▲" "▼")

                                                         "as a wider aggregate:"
                                                         {:select (if (= agg-field :rows)
                                                                    (into (vec fields) [[[:count 1] :rows]])
                                                                    (into (vec fields) [[[:sum agg-field] :sum_agg]]))
                                                          :from [query-ref-name]
                                                          :_query-id (keyword (str query-id "-agg"))
                                                          :_h 4
                                                          :connection-id connection-id
                                                          :group-by (vec fields)
                                                          :style-rules {[:* :highlight-group]
                                                                        {:logic (into [:and] (vec (for [[kk vv] w] [:= kk vv])))
                                                                         :style {:background-color
                                                                                 "#008b8b66"
                                                                                 :border
                                                                                 "1px solid #00000000"}}}
                                                          :order-by [[(+ (count fields) 1) (if (> val tmean) :desc :asc)]]}

                                                         "All rows in this group:"
                                                         {:select [:*]
                                                          :from [query-ref-name]
                                                          :_query-id (keyword (str query-id "-detail"))
                                                          :_h 8
                                                          :connection-id connection-id
                                                          :where [:*if (keyword (str query-id "-agg/*.clicked"))
                                                                  [:*all= (keyword (str query-id "-agg/*.clicked"))
                                                                   (vec fields)]
                                                                  ;[:= 1 1]
                                                                  (into [:and] (vec (for [[kk vv] w] [:= kk vv])))]
                                                          ;:where (into [:and] (vec (for [[kk vv] w] [:= kk vv])))
                                                          }]))))
                    :order idx}))}}))

(defn date-string?
  "Check if the given string matches the format 'YYYY-MM-DD HH:mm:ss'"
  [s]
  (boolean (re-matches #"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}" s)))

(defn parse-date-string
  "Parse a date string in the format 'YYYY-MM-DD HH:mm:ss' into a map of date parts"
  [s]
  (if (date-string? s)
    (let [[date time] (cstr/split s #" ")
          [year month day] (cstr/split date #"-")
          [hour minute second] (cstr/split time #":")]
      {:calc_year year
       :calc_month month
       :calc_day day
       :calc_hour hour
       :calc_minute minute
       :calc_second second})
    (throw (ex-info "Invalid date string format" {:input s}))))

(defn get-outliers [query-name panel-name data-file-path meta-file-path opts-map & [split-dates?]]
  (let [;results (durable-atom (str "./" (hash query) ".edn"))
        query-ref-name (keyword (str "query/" (cstr/replace (str query-name) #":" "")))]
    ;(if false; (not (empty? @results)) ;; disable caching for now
    ;  (do
    ;    (sql/pp @results)
    ;    @results)
    ;;;(sql/pp {:query-name query-name :panel-name panel-name :data-file-path data-file-path :meta-file-path meta-file-path})
    (sql/pp [:opts-map opts-map])
    (let [table-name (keyword (str "table" (rand-int 12345)))
          query-meta (read-transit-data meta-file-path)
         ; _ (sql/pp query-meta)
          date-fields (vec (for [[fname {:keys [commons]}]
                                 (get query-meta :fields)
                                 :when (and (string? (first (keys commons)))
                                            (date-string? (first (keys commons))))] fname))
          _ (sql/pp [:date-fields date-fields])
          gdata      (read-transit-data data-file-path)
          gdata      (vec (for [row gdata]
                            (merge
                             (when split-dates? (parse-date-string (get row (first date-fields))))
                             (assoc row :rows 1)
                             row))) ;; need an agg field for now
            ;;gdata (walk/postwalk-replace {nil "NULL"} gdata) ;; materialized NULLs for better reporting

            ;;_ (println gdata)
            ;; gdata (if data (edn/read-string data)
            ;;           (edn/read-string (slurp data-file-path))) ;(durable-atom "./datom.edn")

          _ (sql/pp ["loading-data... " (nf (count gdata)) " rows"])
          sales-data gdata
          _ (insert-rowset sales-data table-name query-meta)
          table-name-str (unkeyword table-name)
          _ (sql-exec system-db (str "analyze " table-name-str " ; "))
          _ (sql-exec system-db (str "set threads to 32 ; "))
          sales-dataset [] ;;(incanter/to-dataset sales-data)
          _ (sql/pp ["analyze-results... "])
          _ (sql/pp query-meta)
          string-dims (try (vec (for [[k v] (first sales-data)
                                      :when (and (not (= (get-in query-meta [:fields k :distinct]) 1)) ;; no super low cardinality for now
                                                 (not (= (get-in query-meta [:fields k :distinct]) 2)) ;; error trying to do > ? clojure.Numbers.java ?
                                                 (true? (get-in query-meta [:fields k :group-by?] true)) ;; just in case
                                                 (or (= (cstr/lower-case (str k)) ":year")
                                                     (= (cstr/lower-case (str k)) ":hour")
                                                     (= (cstr/lower-case (str k)) ":month")
                                                     (string? v)))] k))
                           (catch Exception e (do (println e) [])))
          _ (sql/pp [:string-dims string-dims])
          agg-fields (vec (filter (fn [[_ v]] (false? (get v :group-by?)))
                                  (get query-meta :fields)))
          _ (sql/pp [:pre-agg-fields agg-fields])
          agg-fields? (not (empty? agg-fields))
          ;; gdata (if (not agg-fields?)
          ;;         (vec (for [row gdata]
          ;;                (merge row {:rows 1})))
          ;;         gdata)
          agg-fields (if agg-fields? agg-fields [:rows])
          _ (sql/pp [:agg-fields agg-fields])
          agg-field (if (empty? agg-fields) :rows (try (last agg-fields) (catch Exception _ agg-fields))) ;; :sales_amount
          agg-field (if (vector? agg-field) (first agg-field) agg-field)
          _ (sql/pp [:agg-field agg-field])
          dim-opts (get opts-map :dimensions-to-scan)
          dim-fields (if (empty? dim-opts)
                       (vec (sort (remove #(= % agg-field) string-dims)))
                       dim-opts)
          ;;_ (sql/pp [:groupings dim-fields :aggs agg-fields :using agg-field {:row-count (count gdata)}])
          max-combos (get opts-map :max-combos 4)
          _ (sql/pp {:dim-fields dim-fields
                     :aggs agg-fields
                     :using-agg agg-field
                     :row-count (count gdata)
                     :max-dim-combos max-combos
                     :percentile-targets [0.01 0.99]})
          _ (Thread/sleep 1500)

          outliers (analyze-dataset sales-dataset dim-fields agg-field sales-data max-combos table-name)
          ;;_ (sql/pp [:outliers outliers])
          sorted-outliers (merge-and-sort-outliers (vec (get outliers agg-field)))
          ;;_ (sql/pp [:sorted-outliers sorted-outliers])
          merged-sorted (for [[[vv dd] mm] sorted-outliers]
                          [[vv dd (vec (sort (keys (first mm))))] mm])
          ;;_ (sql/pp [:merged-sorted merged-sorted])
          by-abs-diff (sort-by #(second (first %)) merged-sorted)
          ;;combos2 (vec (sort (distinct (map #(last (first %)) by-abs-diff))))
          totals (apply + (for [[_ vv] outliers] (apply + (for [v vv]
                                                            (+ (get v :high-outliers-num)
                                                               (get v :low-outliers-num))))))
          [top-limit group-limit] [100 10]
          top-x (take-last top-limit by-abs-diff)
          ;;_ (sql/pp [:top-x top-x :by-abs-diff by-abs-diff])
          ;; top-x-by-group (into {} 
          ;;                      (for [c combos2]
          ;;                        {c (vec (map (fn [[[vv dd _] b]] [vv dd b])
          ;;                                     (take-last group-limit (filter #(= (last (first %)) c) by-abs-diff))))}))
          ;;_ (sql/pp [:top-x-by-group top-x-by-group])
          kit-body (create-top-x-output top-x agg-field outliers panel-name query-ref-name query-name query-meta)
          kit-body (if (empty? (get-in kit-body [(ffirst kit-body) :data]))
                     {:top-outliers
                      {:description "No outliers found"
                       :options {:pages? true :search? false :actions? false}
                       :parameters {} ;; base params that mutate on exec, not render. step mutations insert on render
                       :mutates {} ;; a base mutation and will take place before any UI can get in the way... dangerous, but needed for calliope etc
                       :data [{:name (str (nf (count gdata)) " rows in dataset")
                               :content [:box :child "No outliers found"]}]}}
                     kit-body)
          ;; example of optional client alerts being sent to the client
          ;; kit-body (assoc kit-body :push-alerts [{:body [:box :child "YO 1!"] :seconds 12}
          ;;                                        {:body [:box :child "YO 2!"] :seconds 10}
          ;;                                        {:body [:box :child "YO 3!"] :seconds 16}])
          ;; example of optional raw client mutations being sent to the client 
          ;; (opt-in mutations can still be sent through the kit UI map, but this is a forced assoc-in)
          ;; kit-body (assoc kit-body :push-assocs {[:panels :block-44jj] {:h 3
          ;;                                                               :w 8
          ;;                                                               :root [6 2]
          ;;                                                               :tab "cubic panda"
          ;;                                                               :selected-view :hare-vw-45
          ;;                                                               :name "mind yo business!"
          ;;                                                               :views
          ;;                                                               {:hare-vw-45
          ;;                                                                [:box :align :center :justify :center
          ;;                                                                 :style
          ;;                                                                 {:font-size "33px"
          ;;                                                                  :font-weight 700
          ;;                                                                  :padding-top "6px"
          ;;                                                                  :padding-left "14px"
          ;;                                                                  :margin-top "-8px"
          ;;                                                                  :color :theme/editor-outer-rim-color
          ;;                                                                  :font-family :theme/base-font} :child
          ;;                                                                 "LETS GO!"]}
          ;;                                                               :queries {}}})
          ]
        ;(reset! results kit-body) ;; save raw pre-processed data to cache
        ;(sql/pp kit-body)
        ;(sql/pp [:outliers outliers])
      (sql/pp [:showing-top 30 :out-of totals])
      (sql/pp [:done])
      (Thread/sleep 500)
      kit-body)));)


  ;; (time (get-outliers
  ;;      :DISTRICT-drag-22
  ;;      :block-984
  ;;      "/home/ryanr/rvbbit/backend/transit-data/worthy-bronze-grasshopper-42/DISTRICT_drag_22.transit"
  ;;      "/home/ryanr/rvbbit/backend/transit-data/worthy-bronze-grasshopper-42/DISTRICT_drag_22.meta.transit" {}))

  ;; (time (get-outliers
  ;;      :offenses_drag_18
  ;;      :block-984
  ;;      "/home/ryanr/rvbbit/backend/transit-data/worthy-bronze-grasshopper-42/offenses_drag_18.transit" ;; 70MB, 320k rows
  ;;      "/home/ryanr/rvbbit/backend/transit-data/worthy-bronze-grasshopper-42/offenses_drag_18.meta.transit" {}))
  

(defn -main
  [& args]
  (nrepl-server/start-server :port 45999)
  (sql/pp ["Rabbit Kit Outliers - nREPL server started on port 45999"])
  @(promise) 
  ;; (time (get-outliers
  ;;        :DISTRICT-drag-22
  ;;        :block-984
  ;;        "/home/ryanr/rvbbit/backend/transit-data/worthy-bronze-grasshopper-42/DISTRICT_drag_22.meta/transit"
  ;;        "/home/ryanr/rvbbit/backend/transit-data/worthy-bronze-grasshopper-42/DISTRICT_drag_22.transit"
  ;;        ))
  ;(time (run-flow))
  ;; (System/exit 0)
  ;(sql/pp :happy-birthday!)
  )
