(require '[clojure.data.json :as json]
         '[clojure.java.io :as io]
         '[clojure.java.shell :as shell]
         '[clojure.string :as str]
         '[xtdb.api :as xt]
         '[xtdb.node :as xtn]
         '[xtdb.protocols :as xtp]
         '[xtdb.serde :as serde])
(import '[java.io BufferedWriter FileOutputStream OutputStreamWriter]
        '[java.lang AutoCloseable]
        '[java.math BigDecimal BigInteger]
        '[java.nio.charset StandardCharsets]
        '[java.nio.file Files Path Paths StandardOpenOption]
        '[java.security MessageDigest]
        '[java.time Instant]
        '[java.util HexFormat])

(def root (.normalize (.toAbsolutePath (Paths/get "/var/tmp/sirix-bitemporal" (make-array String 0)))))
(def day-zero (Instant/parse "2024-01-01T00:00:00Z"))
(def horizon 366)
(def publications 25)
(def engine-cap-bytes (* 6 1024 1024 1024))
(def campaign-cap-bytes (* 12 1024 1024 1024))
(def minimum-free-bytes (* 20 1024 1024 1024))

(def tiers {"development" {:contracts 2000 :products 200 :suppliers 100 :events 4776}
            "t25k" {:contracts 25000 :products 2500 :suppliers 500 :events 58724}
            "t100k" {:contracts 100000 :products 10000 :suppliers 2000 :events 234884}})

(defn campaign-path [value label]
  (let [path (.normalize (.toAbsolutePath (Paths/get value (make-array String 0))))]
    (when-not (.startsWith path root)
      (throw (IllegalArgumentException. (str label " must be under " root ": " path))))
    path))

(defn day [number] (.plusSeconds day-zero (* 86400 (long number))))
(defn system-time [epoch] (day (* 15 epoch)))

(defn du-bytes [path & options]
  (let [result (apply shell/sh "du" "-B1" "-s" (concat options [(str path)]))]
    (when-not (zero? (:exit result))
      (throw (ex-info "du failed" {:path (str path) :exit (:exit result) :err (:err result)})))
    (Long/parseLong (first (str/split (str/trim (:out result)) #"\s+")))))

(defn capacity-facts [database epoch]
  (let [allocated (du-bytes database)
        logical (du-bytes database "--apparent-size")
        campaign-allocated (du-bytes root)
        free (.getUsableSpace (Files/getFileStore root))]
    (when (> allocated engine-cap-bytes)
      (throw (ex-info "XTDB allocated footprint exceeds 6 GiB cap"
                      {:epoch epoch :allocated allocated :cap engine-cap-bytes})))
    (when (> campaign-allocated campaign-cap-bytes)
      (throw (ex-info "SH1 campaign footprint exceeds 12 GiB cap"
                      {:epoch epoch :allocated campaign-allocated :cap campaign-cap-bytes})))
    (when (< free minimum-free-bytes)
      (throw (ex-info "free space fell below 20 GiB"
                      {:epoch epoch :free free :minimum minimum-free-bytes})))
    {:logical-bytes logical :allocated-bytes allocated
     :campaign-allocated-bytes campaign-allocated :free-bytes free}))

(defn config [database]
  {:log [:local {:path (str (.resolve database "log"))}]
   :storage [:local {:path (str (.resolve database "storage"))}]
   :default-tz "UTC"
   :indexer {:rows-per-block 10000}
   :compactor {:threads 1}})

(defn read-events [path tier]
  (with-open [reader (io/reader (.toFile path))]
    (let [events (mapv #(json/read-str % :key-fn keyword) (line-seq reader))
          expected (:events tier)]
      (when-not (= expected (count events))
        (throw (ex-info "event count mismatch" {:actual (count events) :expected expected})))
      (doseq [[prior current] (partition 2 1 events)]
        (let [prior-key [(:epoch prior) (:table prior) (:id prior)]
              current-key [(:epoch current) (:table current) (:id current)]]
          (when-not (neg? (compare prior-key current-key))
            (throw (ex-info "event stream is not strictly ordered" {:prior prior-key :current current-key})))))
      events)))

(defn event-doc [event]
  (case (:table event)
    "contracts" {:xt/id (:id event) :pid (:pid event) :sid (:sid event) :cost (:cost event)
                 :qty (:qty event) :grade (:grade event)}
    "products" {:xt/id (:id event) :category (:category event) :retail (:retail event)}
    "suppliers" {:xt/id (:id event) :region (:region event) :tier (:tier event)}))

(defn base-ops [events]
  (let [by-table (group-by :table events)
        opts (fn [table] {:into (keyword table) :valid-from (day 0) :valid-to (day horizon)})
        epochs (mapv (fn [epoch] {:xt/id epoch :epoch epoch :ts (system-time epoch)})
                     (range publications))
        days (mapv (fn [number] {:xt/id number :day-no number :ts (day number)})
                   (range horizon))]
    [(into [:put-docs (opts "contracts")] (map event-doc (get by-table "contracts")))
     (into [:put-docs (opts "products")] (map event-doc (get by-table "products")))
     (into [:put-docs (opts "suppliers")] (map event-doc (get by-table "suppliers")))
     (into [:put-docs {:into :epochs :valid-from (day 0)}] epochs)
     (into [:put-docs {:into :days :valid-from (day 0)}] days)]))

(defn event-op [event]
  (let [table (keyword (:table event))
        options {(if (= "put" (:op event)) :into :from) table
                 :valid-from (day (:a event))
                 :valid-to (day (:b event))}]
    (if (= "put" (:op event))
      [:put-docs options (event-doc event)]
      [:delete-docs options (:id event)])))

(defn check-transactions [node]
  (let [rows (xt/q node "SELECT _id, committed, system_time FROM xt.txs ORDER BY _id")]
    (when-not (= publications (count rows))
      (throw (ex-info "wrong transaction count" {:actual (count rows) :expected publications})))
    (doseq [row rows]
      (when-not (:committed row)
        (throw (ex-info "uncommitted XTDB transaction" row))))
    rows))

(defn await-replay [node]
  (let [deadline (+ (System/nanoTime) (* 120 1000000000))]
    (loop []
      (let [rows (xt/q node "SELECT _id, committed, system_time FROM xt.txs ORDER BY _id")]
        (if (= publications (count rows))
          (do
            (doseq [row rows]
              (when-not (:committed row)
                (throw (ex-info "uncommitted replayed transaction" row))))
            (println (json/write-str {:phase "xtdb-ready" :transactions (count rows)}))
            (flush))
          (if (< (System/nanoTime) deadline)
            (do (Thread/sleep 100) (recur))
            (throw (ex-info "timed out waiting for XTDB log replay"
                            {:transactions (count rows) :expected publications}))))))))

(defn call-with-close-suppressed [resource action]
  (let [primary-failure (volatile! nil)]
    (try
      (action resource)
      (catch Throwable failure
        (vreset! primary-failure failure)
        (throw failure))
      (finally
        (try
          (.close ^AutoCloseable resource)
          (catch Throwable close-failure
            (if-some [failure @primary-failure]
              (.addSuppressed ^Throwable failure close-failure)
              (throw close-failure))))))))

(defn load-database [tier-name event-file database]
  (let [tier (get tiers tier-name)]
    (when-not tier
      (throw (IllegalArgumentException. (str "unknown tier: " tier-name))))
    (when (Files/exists database (make-array java.nio.file.LinkOption 0))
      (throw (IllegalArgumentException. (str "refusing to overwrite existing XTDB database: " database))))
    (Files/createDirectories database (make-array java.nio.file.attribute.FileAttribute 0))
    (let [events (read-events event-file tier)
          grouped (group-by :epoch events)
          started (System/nanoTime)]
      (call-with-close-suppressed
       (xtn/start-node (config database))
       (fn [node]
         (doseq [epoch (range publications)]
           (let [epoch-events (get grouped epoch)
                 ops (if (zero? epoch) (base-ops epoch-events) (mapv event-op epoch-events))
                 result (xt/execute-tx node ops {:system-time (system-time epoch)})
                 footprint (capacity-facts database epoch)]
             (println (json/write-str (merge {:phase "xtdb-publication" :epoch epoch
                                              :events (count epoch-events) :tx (pr-str result)}
                                             footprint)))
             (flush)))
         (check-transactions node)))
      (let [sync-result (shell/sh "sync" "-f" (str database))]
        (when-not (zero? (:exit sync-result))
          (throw (ex-info "sync failed" {:exit (:exit sync-result) :err (:err sync-result)}))))
      (println (json/write-str (merge {:phase "xtdb-load" :tier tier-name
                                       :seconds (/ (- (System/nanoTime) started) 1e9)
                                       :database (str database)}
                                      (capacity-facts database (dec publications))))))))

(def query-specs
  [{:q 1 :columns [:id :cost :qty :grade] :keys 1
    :sql "SELECT c._id AS id,c.cost,c.qty,c.grade FROM contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-12-26 00:00:00+00:00' FOR VALID_TIME AS OF TIMESTAMP '2024-06-15 00:00:00+00:00' AS c WHERE c._id=1 ORDER BY id"}
   {:q 2 :columns [:id :cost :qty :grade] :keys 1
    :sql "SELECT c._id AS id,c.cost,c.qty,c.grade FROM contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-29 00:00:00+00:00' FOR VALID_TIME AS OF TIMESTAMP '2024-06-15 00:00:00+00:00' AS c WHERE c._id=1 ORDER BY id"}
   {:q 3 :columns [:min-cost :max-cost :prices] :keys 0
    :sql "SELECT MIN(c.cost) AS min_cost,MAX(c.cost) AS max_cost,COUNT(DISTINCT c.cost) AS prices FROM contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-12-26 00:00:00+00:00' FOR VALID_TIME FROM TIMESTAMP '2024-05-30 00:00:00+00:00' TO TIMESTAMP '2024-07-29 00:00:00+00:00' AS c WHERE c._id=1"}
   {:q 4 :columns [:id :old-cost :new-cost :old-qty :new-qty] :keys 1
    :sql "SELECT a._id AS id,a.cost AS old_cost,b.cost AS new_cost,a.qty AS old_qty,b.qty AS new_qty FROM contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-29 00:00:00+00:00' FOR VALID_TIME AS OF TIMESTAMP '2024-06-15 00:00:00+00:00' AS a JOIN contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-12-26 00:00:00+00:00' FOR VALID_TIME AS OF TIMESTAMP '2024-06-15 00:00:00+00:00' AS b ON a._id=b._id WHERE a.cost<>b.cost OR a.qty<>b.qty ORDER BY id"}
   {:q 5 :columns [:epoch :cost :qty] :keys 1
    :sql "SELECT e.epoch,c.cost,c.qty FROM epochs AS e JOIN contracts FOR ALL SYSTEM_TIME FOR VALID_TIME AS OF TIMESTAMP '2024-06-15 00:00:00+00:00' AS c ON c._system_from<=e.ts AND (c._system_to IS NULL OR e.ts<c._system_to) WHERE c._id=1 ORDER BY e.epoch"}
   {:q 6 :columns [:epoch :grade :n :qty-sum] :keys 2
    :sql "SELECT e.epoch,c.grade,COUNT(*) AS n,SUM(c.qty) AS qty_sum FROM epochs AS e JOIN contracts FOR ALL SYSTEM_TIME FOR VALID_TIME AS OF TIMESTAMP '2024-06-15 00:00:00+00:00' AS c ON c._system_from<=e.ts AND (c._system_to IS NULL OR e.ts<c._system_to) GROUP BY e.epoch,c.grade ORDER BY e.epoch,c.grade"}
   {:q 7 :columns [:grade :n :qty-sum :exposure] :keys 1
    :sql "SELECT c.grade,COUNT(*) AS n,SUM(c.qty) AS qty_sum,SUM(c.cost*c.qty) AS exposure FROM contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-12-26 00:00:00+00:00' FOR VALID_TIME AS OF TIMESTAMP '2024-06-15 00:00:00+00:00' AS c GROUP BY c.grade ORDER BY c.grade"}
   {:q 8 :columns [:sid :grade :n :min-cost :max-cost] :keys 2
    :sql "SELECT c.sid,c.grade,COUNT(*) AS n,MIN(c.cost) AS min_cost,MAX(c.cost) AS max_cost FROM contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-29 00:00:00+00:00' FOR VALID_TIME AS OF TIMESTAMP '2024-06-15 00:00:00+00:00' AS c GROUP BY c.sid,c.grade ORDER BY c.sid,c.grade"}
   {:q 9 :columns [:region :grade :n :exposure] :keys 2
    :sql "SELECT s.region,c.grade,COUNT(*) AS n,SUM(c.cost*c.qty) AS exposure FROM contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-12-26 00:00:00+00:00' FOR VALID_TIME AS OF TIMESTAMP '2024-06-15 00:00:00+00:00' AS c JOIN suppliers FOR SYSTEM_TIME AS OF TIMESTAMP '2024-12-26 00:00:00+00:00' FOR VALID_TIME AS OF TIMESTAMP '2024-06-15 00:00:00+00:00' AS s ON c.sid=s._id GROUP BY s.region,c.grade ORDER BY s.region,c.grade"}
   {:q 10 :columns [:category :contracts :min-margin :max-margin] :keys 1
    :sql "SELECT p.category,COUNT(DISTINCT c._id) AS contracts,MIN(p.retail-c.cost) AS min_margin,MAX(p.retail-c.cost) AS max_margin FROM contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-12-26 00:00:00+00:00' FOR VALID_TIME FROM TIMESTAMP '2024-05-30 00:00:00+00:00' TO TIMESTAMP '2024-07-29 00:00:00+00:00' AS c JOIN products FOR SYSTEM_TIME AS OF TIMESTAMP '2024-12-26 00:00:00+00:00' FOR VALID_TIME FROM TIMESTAMP '2024-05-30 00:00:00+00:00' TO TIMESTAMP '2024-07-29 00:00:00+00:00' AS p ON c.pid=p._id AND c._valid_from<p._valid_to AND p._valid_from<c._valid_to GROUP BY p.category ORDER BY p.category"}
   {:q 11 :columns [:day-no :grade :n :exposure] :keys 2
    :sql "SELECT d.day_no,c.grade,COUNT(*) AS n,SUM(c.cost*c.qty) AS exposure FROM days AS d JOIN contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-12-26 00:00:00+00:00' FOR ALL VALID_TIME AS c ON c._valid_from<=d.ts AND d.ts<c._valid_to WHERE d.day_no>=150 AND d.day_no<210 GROUP BY d.day_no,c.grade ORDER BY d.day_no,c.grade"}
   {:q 12 :columns [:grade :n :old-exposure] :keys 1
    :sql "SELECT a.grade,COUNT(*) AS n,SUM(a.cost*a.qty) AS old_exposure FROM contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-29 00:00:00+00:00' FOR VALID_TIME AS OF TIMESTAMP '2024-06-15 00:00:00+00:00' AS a WHERE NOT EXISTS (SELECT 1 FROM contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-09-27 00:00:00+00:00' FOR VALID_TIME AS OF TIMESTAMP '2024-06-15 00:00:00+00:00' AS b WHERE b._id=a._id) GROUP BY a.grade ORDER BY a.grade"}])

(defn exact-long [value query column]
  (cond
    (instance? BigDecimal value) (.longValueExact ^BigDecimal value)
    (instance? BigInteger value) (.longValueExact ^BigInteger value)
    (instance? clojure.lang.BigInt value)
    (.longValueExact (.toBigInteger ^clojure.lang.BigInt value))
    (integer? value) (long value)
    :else (throw (ex-info "non-integral query result" {:q query :column column :value value
                                                       :class (class value)}))))

(defn validate-key [prior current key-count query]
  (when prior
    (let [comparison (compare (subvec prior 0 key-count) (subvec current 0 key-count))]
      (when-not (neg? comparison)
        (throw (ex-info "unordered or duplicate query key" {:q query :prior prior :current current})))))
  current)

(defn write-canonical [spec rows output]
  (let [target (.resolve output (str "q" (:q spec) ".tsv"))
        digest (MessageDigest/getInstance "SHA-256")]
    (with-open [writer (BufferedWriter. (OutputStreamWriter.
                                         (FileOutputStream. (.toFile target)) StandardCharsets/UTF_8))]
      (loop [remaining rows prior nil row-count 0]
        (if-let [row (first remaining)]
          (let [raw-values (mapv #(get row %) (:columns spec))]
            (when (some nil? raw-values)
              (throw (ex-info "missing/null query column" {:q (:q spec) :row row})))
            (when-not (= (count row) (count (:columns spec)))
              (throw (ex-info "query schema width mismatch" {:q (:q spec) :row row})))
            (let [values (mapv #(exact-long % (:q spec) %2) raw-values (:columns spec))]
              (validate-key prior values (:keys spec) (:q spec))
              (let [line (str (str/join "\t" values) "\n")
                  bytes (.getBytes line StandardCharsets/UTF_8)]
                (.write writer line)
                (.update digest bytes)
                (recur (next remaining) (when (pos? (:keys spec)) values) (inc row-count)))))
          {:rows row-count :sha256 (.formatHex (HexFormat/of) (.digest digest))})))))

(defn query-database [database output]
  (Files/createDirectories output (make-array java.nio.file.attribute.FileAttribute 0))
  (call-with-close-suppressed
   (xtn/start-node (config database))
   (fn [node]
     (await-replay node)
     (let [entries
           (mapv (fn [spec]
                   (let [started (System/nanoTime)
                         rows (with-open [stream (xtp/open-sql-query
                                                 node (:sql spec)
                                                 {:key-fn (serde/read-key-fn :kebab-case-keyword)})]
                                (vec (.toList stream)))
                         seconds (/ (- (System/nanoTime) started) 1e9)
                         result (write-canonical spec rows output)
                         query-sha (let [digest (MessageDigest/getInstance "SHA-256")]
                                     (.formatHex (HexFormat/of)
                                                 (.digest digest (.getBytes (:sql spec) StandardCharsets/UTF_8))))]
                     (spit (.toFile (.resolve output (str "q" (:q spec) ".raw.edn"))) (str (pr-str rows) "\n"))
                     (println (json/write-str (merge {:phase "xtdb-query" :q (:q spec)
                                                      :seconds seconds :query-sha256 query-sha}
                                                     result)))
                     (flush)
                     (merge {:q (:q spec) :query_sha256 query-sha} result)))
                 query-specs)]
       (spit (.toFile (.resolve output "manifest.json"))
             (str (json/write-str {:engine "xtdb-2.1.0" :queries entries}) "\n"))))))

(defn probe-database [database]
  (call-with-close-suppressed
   (xtn/start-node (config database))
   (fn [node]
     (await-replay node)
     (doseq [sql ["SELECT _id,cost,qty,_valid_from,_valid_to,_system_from,_system_to FROM contracts FOR ALL SYSTEM_TIME FOR ALL VALID_TIME WHERE _id=1 ORDER BY _system_from,_valid_from"
                  "SELECT _id,cost,qty,_valid_from,_valid_to FROM contracts FOR SYSTEM_TIME AS OF TIMESTAMP '2024-12-26 00:00:00+00:00' FOR ALL VALID_TIME WHERE _id=1 ORDER BY _valid_from"
                  "SELECT _id,committed,system_time,error FROM xt.txs ORDER BY _id"]]
       (prn {:phase "probe" :sql sql :rows (xt/q node sql)})))))

(let [[mode & args] *command-line-args*]
  (case mode
    "load" (let [[tier events database] args]
             (when-not (= 3 (count args))
               (throw (IllegalArgumentException. "load needs <tier> <events.jsonl> <database-dir>")))
             (load-database tier (campaign-path events "event stream")
                            (campaign-path database "database directory")))
    "run" (let [[database output] args]
            (when-not (= 2 (count args))
              (throw (IllegalArgumentException. "run needs <database-dir> <output-dir>")))
            (query-database (campaign-path database "database directory")
                            (campaign-path output "output directory")))
    "probe" (let [[database] args]
              (when-not (= 1 (count args))
                (throw (IllegalArgumentException. "probe needs <database-dir>")))
              (probe-database (campaign-path database "database directory")))
    (throw (IllegalArgumentException. (str "expected load, run or probe, got " mode)))))

(shutdown-agents)
